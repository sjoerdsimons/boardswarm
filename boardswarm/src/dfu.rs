use std::{collections::HashMap, time::Duration};

use anyhow::bail;
use bytes::Bytes;
use dfu_nusb::{DfuASync, DfuNusb};
use futures::StreamExt;
use nusb::descriptors::language_id::US_ENGLISH;
use tokio::sync::{
    mpsc::{self, Receiver},
    oneshot,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{compat::TokioAsyncReadCompatExt, io::StreamReader};
use tracing::instrument;
use tracing::{info, warn};

use crate::{
    provider::Provider, udev::DeviceEvent, Volume, VolumeError, VolumeTarget, VolumeTargetInfo,
};
pub const PROVIDER: &str = "dfu";

#[instrument(skip(provider))]
pub async fn start_provider(provider: Provider) {
    let mut registrations = HashMap::new();
    let mut devices = crate::udev::DeviceStream::new("usb").unwrap();
    while let Some(d) = devices.next().await {
        match d {
            DeviceEvent::Add { device, .. } => {
                let Some(interface) = device.property("ID_USB_INTERFACES") else {
                    continue;
                };
                if interface != ":fe0102:" || device.devnode().is_none() {
                    continue;
                }
                let Some(busnum) = device
                    .property_u64("BUSNUM", 10)
                    .and_then(|v| v.try_into().ok())
                else {
                    continue;
                };
                let Some(devnum) = device
                    .property_u64("DEVNUM", 10)
                    .and_then(|v| v.try_into().ok())
                else {
                    continue;
                };

                // Match on ID_MODEL here; the device may advertise the optional idModel property.
                let name = if let Some(model) = device.property("ID_MODEL") {
                    format!("{}/{} {}", busnum, devnum, model)
                } else {
                    format!("{}/{}", devnum, devnum)
                };

                let dfu = Dfu::new(busnum, devnum).await;
                let properties = device.properties(name);

                let id = provider.register_volume(properties, dfu);
                registrations.insert(device.syspath().to_path_buf(), id);
            }
            DeviceEvent::Remove(device) => {
                if let Some(id) = registrations.remove(device.syspath()) {
                    provider.unregister_volume(id)
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Dfu {
    device: DfuDevice,
    targets: Vec<VolumeTargetInfo>,
}

impl Dfu {
    pub async fn new(busnum: u8, address: u8) -> Self {
        let device = DfuDevice::new(busnum, address);
        let targets = device
            .targets()
            .await
            .into_iter()
            .map(|name| VolumeTargetInfo {
                name,
                readable: false,
                writable: true,
                seekable: false,
                size: None,
                blocksize: None,
            })
            .collect();
        Self { device, targets }
    }
}

#[async_trait::async_trait]
impl Volume for Dfu {
    fn targets(&self) -> (&[VolumeTargetInfo], bool) {
        (self.targets.as_slice(), true)
    }

    async fn open(
        &self,
        target: &str,
        length: Option<u64>,
    ) -> Result<(VolumeTargetInfo, Box<dyn VolumeTarget>), VolumeError> {
        if let Some(volume_target) = self.targets.iter().find(|t| t.name == target) {
            let tx = self
                .device
                .start_download(target.to_owned(), length.unwrap_or_default() as u32)
                .await;
            Ok((volume_target.clone(), Box::new(DfuTarget { tx })))
        } else {
            warn!("Unknown target requested");
            Err(VolumeError::UnknownTargetRequested)
        }
    }

    async fn commit(&self) -> Result<(), VolumeError> {
        self.device.reset().await;
        Ok(())
    }
}

struct DfuTarget {
    tx: mpsc::Sender<Bytes>,
}

#[async_trait::async_trait]
impl VolumeTarget for DfuTarget {
    async fn write(&mut self, data: Bytes, _offset: u64, completion: crate::WriteCompletion) {
        let len = data.len();
        let _ = self.tx.send(data).await;
        completion.complete(Ok(len as u64));
    }
}

#[derive(Debug)]
enum DfuCommand {
    Targets(oneshot::Sender<Vec<String>>),
    Download {
        target: String,
        length: u32,
        data: Receiver<Bytes>,
    },
    Reset(oneshot::Sender<()>),
}

#[derive(Clone, Debug)]
struct DfuDevice(mpsc::Sender<DfuCommand>);
impl DfuDevice {
    fn new(bus: u8, dev: u8) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(dfu_task(bus, dev, rx));
        Self(tx)
    }

    async fn targets(&self) -> Vec<String> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.send(DfuCommand::Targets(tx)).await;
        rx.await.unwrap_or_default()
    }

    async fn start_download(&self, target: String, length: u32) -> mpsc::Sender<Bytes> {
        let (tx, data) = tokio::sync::mpsc::channel(1);
        let command = DfuCommand::Download {
            target,
            length,
            data,
        };

        self.0.send(command).await.unwrap();
        tx
    }

    async fn reset(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.send(DfuCommand::Reset(tx)).await;
        let _ = rx.await;
    }
}

struct DfuData {
    device: nusb::Device,
    interfaces: HashMap<String, DfuInterface>,
}

impl DfuData {
    fn open(&self, interface: &DfuInterface) -> anyhow::Result<DfuASync> {
        let i = self.device.claim_interface(interface.interface)?;
        let dfu = DfuNusb::open(self.device.clone(), i, interface.alt)?;
        Ok(dfu.into_async_dfu())
    }
}

#[derive(Debug, Clone)]
struct DfuInterface {
    interface: u8,
    alt: u8,
}

fn setup_device(bus: u8, dev: u8) -> anyhow::Result<DfuData> {
    let Some(info) = crate::utils::nusb_info_from_bus_dev(bus, dev) else {
        bail!("Couldn't find device")
    };
    let device = info.open()?;
    let mut interfaces = HashMap::new();

    for c in device.configurations() {
        for i in c.interfaces() {
            for alt in i.alt_settings() {
                if alt.class() == 0xfe && alt.subclass() == 0x1 {
                    info!(
                        "Bus {bus} Device {dev}: ID {} {}",
                        info.vendor_id(),
                        info.product_id()
                    );
                    info!(" interface: {}", i.interface_number());
                    info!(" alt: {}", alt.alternate_setting());
                    match alt.protocol() {
                        0x1 => {
                            info!("     Application mode");
                            continue;
                        }
                        0x2 => info!("     DFU mode"),
                        m => {
                            info!("     Unknown mode: {}", m);
                            continue;
                        }
                    }
                    if let Some(s_i) = alt.string_index() {
                        let lang = device
                            .get_string_descriptor_supported_languages(Duration::from_millis(250))?
                            .next()
                            .unwrap_or(US_ENGLISH);
                        let descriptor =
                            device.get_string_descriptor(s_i, lang, Duration::from_millis(250))?;
                        info!("  descriptor: {}", descriptor);
                        interfaces.insert(
                            descriptor,
                            DfuInterface {
                                interface: i.interface_number(),
                                alt: alt.alternate_setting(),
                            },
                        );
                    }
                }
            }
        }
    }

    Ok(DfuData { device, interfaces })
}

async fn dfu_download(
    device: &DfuData,
    interface: &DfuInterface,
    length: u32,
    data: Receiver<Bytes>,
) -> anyhow::Result<()> {
    let mut dfu = device.open(interface)?;
    let stream = ReceiverStream::new(data);
    let reader = StreamReader::new(stream.map(Ok::<_, std::io::Error>));
    let reader = TokioAsyncReadCompatExt::compat(reader);
    dfu.download(reader, length).await?;
    Ok(())
}

async fn dfu_reset(device: &DfuData, interface: &DfuInterface) -> anyhow::Result<()> {
    let dfu = device.open(interface)?;
    let _ = dfu.detach().await;
    dfu.usb_reset().await?;
    Ok(())
}

async fn dfu_task(bus: u8, dev: u8, mut control: mpsc::Receiver<DfuCommand>) {
    let device = match setup_device(bus, dev) {
        Ok(d) => d,
        Err(e) => {
            warn!("Failed to setup device: {}", e);
            return;
        }
    };

    while let Some(command) = control.recv().await {
        match command {
            DfuCommand::Targets(tx) => {
                let _ = tx.send(device.interfaces.keys().cloned().collect());
            }
            DfuCommand::Download {
                target,
                length,
                data,
            } => {
                if let Some(interface) = device.interfaces.get(&target) {
                    if let Err(e) = dfu_download(&device, interface, length, data).await {
                        warn!("Download failed: {}", e);
                    }
                } else {
                    warn!("Target not found: {}", target);
                }
            }
            DfuCommand::Reset(tx) => {
                // Shouldn't really matter what interface gets reset;  on the first interface
                if let Some(interface) = device.interfaces.values().next() {
                    if let Err(e) = dfu_reset(&device, interface).await {
                        warn!("Reset failed: {}", e);
                    }
                } else {
                    warn!("No interfaces");
                }
                let _ = tx.send(());
            }
        }
    }
}
