use std::{collections::HashMap, io::SeekFrom};

use bytes::{Bytes, BytesMut};
use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, StreamExt};
use nusb::DeviceInfo;
use rockusb::nusb::Device;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::instrument;
use tracing::{info, warn};

use crate::provider::Provider;
use crate::{udev::DeviceEvent, Volume, VolumeError, VolumeTarget, VolumeTargetInfo};

pub const PROVIDER: &str = "rockusb";

#[instrument(skip_all)]
pub async fn start_provider(provider: Provider) {
    let mut registrations = HashMap::new();
    let mut devices = crate::udev::DeviceStream::new("usb").unwrap();
    while let Some(d) = devices.next().await {
        match d {
            DeviceEvent::Add { device, .. } => {
                let Some(vendor) = device.property_u64("ID_VENDOR_ID", 16) else {
                    continue;
                };

                if vendor != 0x2207 || device.devnode().is_none() {
                    continue;
                }

                let Some(busnum): Option<u8> = device
                    .property_u64("BUSNUM", 10)
                    .and_then(|v| v.try_into().ok())
                else {
                    continue;
                };
                let Some(devnum): Option<u8> = device
                    .property_u64("DEVNUM", 10)
                    .and_then(|v| v.try_into().ok())
                else {
                    continue;
                };

                let name = if let Some(model) = device.property("ID_MODEL_ID") {
                    format!("{}/{} {}", busnum, devnum, model)
                } else {
                    format!("{}/{}", devnum, devnum)
                };
                info!("New rockusb volume: {name}");

                match Rockusb::new(busnum, devnum).await {
                    Ok(rockusb) => {
                        let properties = device.properties(name);
                        let id = provider.register_volume(properties, rockusb);
                        registrations.insert(device.syspath().to_path_buf(), id);
                    }
                    Err(e) => {
                        warn!("Rockusb device setup failure: {}", e);
                    }
                }
            }
            DeviceEvent::Remove(device) => {
                if let Some(id) = registrations.remove(device.syspath()) {
                    provider.unregister_volume(id)
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum RockUsbError {
    #[error("Failed to send command to rockusb thread")]
    CommandsSend,
    #[error("Failed to getting reply from rockusb thread: {0}")]
    CommandsRecv(oneshot::error::RecvError),
    #[error("Rockusb usb error: {0}")]
    RockUsb(#[from] rockusb::device::Error<rockusb::nusb::TransferError>),
    #[error("Rockusb device not available: {0}")]
    RockDeviceUnavailable(#[from] rockusb::nusb::DeviceUnavalable),
}

impl From<RockUsbError> for tonic::Status {
    fn from(e: RockUsbError) -> Self {
        match e {
            RockUsbError::CommandsSend | RockUsbError::CommandsRecv(_) => {
                tonic::Status::internal(e.to_string())
            }
            RockUsbError::RockUsb(_) => tonic::Status::aborted(e.to_string()),
            RockUsbError::RockDeviceUnavailable(_) => tonic::Status::unavailable(e.to_string()),
        }
    }
}

impl From<RockUsbError> for VolumeError {
    fn from(e: RockUsbError) -> Self {
        match e {
            RockUsbError::CommandsSend => VolumeError::Internal(e.to_string()),
            RockUsbError::CommandsRecv(_) => VolumeError::Internal(e.to_string()),
            RockUsbError::RockDeviceUnavailable(_) | RockUsbError::RockUsb(_) => {
                VolumeError::Failure(e.to_string())
            }
        }
    }
}

#[derive(Debug)]
enum RockUsbMode {
    MaskRom,
    Loader((String, u64)),
}

#[derive(Debug)]
pub struct Rockusb {
    commands: mpsc::Sender<RockUsbCommand>,
    targets: Vec<VolumeTargetInfo>,
    mode: RockUsbMode,
}

impl Rockusb {
    pub async fn new(bus: u8, dev: u8) -> Result<Self, RockUsbError> {
        let (commands, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(rockusb_task(bus, dev, rx));

        let mode = Self::determine_mode(&commands).await?;

        let targets = match &mode {
            RockUsbMode::MaskRom => vec![
                VolumeTargetInfo {
                    name: "471".to_string(),
                    readable: false,
                    writable: true,
                    seekable: false,
                    size: None,
                    blocksize: None,
                },
                VolumeTargetInfo {
                    name: "472".to_string(),
                    readable: false,
                    writable: true,
                    seekable: false,
                    size: None,
                    blocksize: None,
                },
            ],
            RockUsbMode::Loader(l) => vec![VolumeTargetInfo {
                name: l.0.clone(),
                readable: true,
                writable: true,
                seekable: true,
                size: Some(l.1),
                blocksize: Some(512),
            }],
        };

        Ok(Self {
            commands,
            mode,
            targets,
        })
    }

    async fn determine_mode(
        commands: &mpsc::Sender<RockUsbCommand>,
    ) -> Result<RockUsbMode, RockUsbError> {
        let (mode_tx, mode_rx) = oneshot::channel();

        commands
            .send(RockUsbCommand::Mode(mode_tx))
            .await
            .map_err(|_e| RockUsbError::CommandsSend)?;
        mode_rx
            .await
            .unwrap_or_else(|e| Err(RockUsbError::CommandsRecv(e)))
    }
}

#[async_trait::async_trait]
impl Volume for Rockusb {
    fn targets(&self) -> (&[VolumeTargetInfo], bool) {
        (&self.targets, true)
    }

    async fn open(
        &self,
        target: &str,
        _length: Option<u64>,
    ) -> Result<(VolumeTargetInfo, Box<dyn VolumeTarget>), VolumeError> {
        let Some(info) = self.targets.iter().find(|t| t.name == target) else {
            return Err(VolumeError::UnknownTargetRequested);
        };
        let target: Box<dyn VolumeTarget> = match &self.mode {
            RockUsbMode::MaskRom => {
                let area = match target {
                    "471" => 0x471,
                    "472" => 0x472,
                    _ => Err(VolumeError::UnknownTargetRequested)?,
                };
                Box::new(RockUsbMaskromTarget::new(area, self.commands.clone()))
            }
            RockUsbMode::Loader((name, _size)) if name == target => {
                Box::new(RockUsbTarget::new(self.commands.clone()).await?)
            }
            _ => return Err(VolumeError::UnknownTargetRequested)?,
        };
        Ok((info.clone(), target))
    }

    async fn commit(&self) -> Result<(), VolumeError> {
        Ok(())
    }
}

struct RockUsbMaskromTarget {
    area: u16,
    commands: mpsc::Sender<RockUsbCommand>,
    data: BytesMut,
}

impl RockUsbMaskromTarget {
    fn new(area: u16, commands: mpsc::Sender<RockUsbCommand>) -> Self {
        // Expect 1MB as the maximum size for the blobs loaded in maskrom mode
        let data = BytesMut::with_capacity(1024 * 1024);
        Self {
            area,
            commands,
            data,
        }
    }

    async fn shutdown(&mut self) -> Result<(), RockUsbError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RockUsbCommand::WriteMaskromArea((
                tx,
                self.area,
                self.data.split().into(),
            )))
            .await
            .map_err(|_e| RockUsbError::CommandsSend)?;
        self.data = BytesMut::new();
        rx.await
            .unwrap_or_else(|e| Err(RockUsbError::CommandsRecv(e)))
    }
}

#[async_trait::async_trait]
impl VolumeTarget for RockUsbMaskromTarget {
    async fn write(&mut self, data: Bytes, offset: u64, completion: crate::WriteCompletion) {
        if offset as usize != self.data.len() {
            completion.complete(Err(tonic::Status::out_of_range("Invalid offset")));
        } else if data.len() + self.data.len() > self.data.capacity() {
            completion.complete(Err(tonic::Status::out_of_range("No more capacity")));
        } else {
            self.data.extend_from_slice(&data);
            completion.complete(Ok(data.len() as u64));
        }
    }

    async fn shutdown(&mut self, completion: crate::ShutdownCompletion) {
        let r = self.shutdown().await;
        completion.complete(r.map_err(Into::into));
    }
}

struct RockUsbTarget {
    operations: mpsc::Sender<RockUsbIO>,
}

impl RockUsbTarget {
    async fn new(commands: mpsc::Sender<RockUsbCommand>) -> Result<Self, VolumeError> {
        let (tx, rx) = oneshot::channel();
        commands
            .send(RockUsbCommand::Open(tx))
            .await
            .map_err(|_e| RockUsbError::CommandsSend)?;
        let operations = rx
            .await
            .unwrap_or_else(|e| Err(RockUsbError::CommandsRecv(e)))?;

        Ok(Self { operations })
    }

    async fn do_read(&mut self, length: u64, offset: u64) -> Result<Bytes, tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.operations
            .send(RockUsbIO::Read(length, offset, tx))
            .await
            .map_err(|_e| RockUsbError::CommandsSend)?;
        rx.await
            .map_err(RockUsbError::CommandsRecv)?
            .map_err(|e| tonic::Status::aborted(format!("{:?}", e)))
    }

    async fn do_write(&mut self, data: Bytes, offset: u64) -> Result<u64, tonic::Status> {
        let len = data.len() as u64;
        let (tx, rx) = oneshot::channel();
        self.operations
            .send(RockUsbIO::Write(data, offset, tx))
            .await
            .map_err(|_e| RockUsbError::CommandsSend)?;
        rx.await
            .map_err(RockUsbError::CommandsRecv)?
            .map_err(|e| tonic::Status::aborted(format!("{:?}", e)))?;
        Ok(len)
    }

    async fn do_flush(&mut self) -> Result<(), tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.operations
            .send(RockUsbIO::Flush(tx))
            .await
            .map_err(|_e| RockUsbError::CommandsSend)?;
        rx.await
            .map_err(RockUsbError::CommandsRecv)?
            .map_err(|e| tonic::Status::aborted(format!("{:?}", e)))
    }
}

#[async_trait::async_trait]
impl VolumeTarget for RockUsbTarget {
    async fn read(&mut self, length: u64, offset: u64, completion: crate::ReadCompletion) {
        completion.complete(self.do_read(length, offset).await);
    }

    async fn write(&mut self, data: Bytes, offset: u64, completion: crate::WriteCompletion) {
        completion.complete(self.do_write(data, offset).await);
    }

    async fn flush(&mut self, completion: crate::FlushCompletion) {
        completion.complete(self.do_flush().await);
    }
}

#[derive(Debug)]
enum RockUsbCommand {
    Mode(oneshot::Sender<Result<RockUsbMode, RockUsbError>>),
    WriteMaskromArea((oneshot::Sender<Result<(), RockUsbError>>, u16, Bytes)),
    Open(oneshot::Sender<Result<mpsc::Sender<RockUsbIO>, RockUsbError>>),
}

#[derive(Debug)]
enum RockUsbIO {
    Write(Bytes, u64, oneshot::Sender<Result<(), std::io::Error>>),
    Read(u64, u64, oneshot::Sender<Result<Bytes, std::io::Error>>),
    Flush(oneshot::Sender<Result<(), std::io::Error>>),
}

fn open_device(info: DeviceInfo) -> Result<Device, RockUsbError> {
    Ok(Device::from_usb_device_info(info)?)
}

async fn handle_io(
    info: DeviceInfo,
    tx: oneshot::Sender<Result<mpsc::Sender<RockUsbIO>, RockUsbError>>,
) {
    let transport = match open_device(info) {
        Ok(t) => t,
        Err(e) => {
            let _ = tx.send(Err(e));
            return;
        }
    };
    let mut io = match transport.into_io().await {
        Ok(io) => io,
        Err(e) => {
            let _ = tx.send(Err(e.into()));
            return;
        }
    };

    let (operations_tx, mut operations) = mpsc::channel(1);
    let _ = tx.send(Ok(operations_tx));

    while let Some(op) = operations.recv().await {
        match op {
            RockUsbIO::Write(data, offset, tx) => {
                let r = match io.seek(SeekFrom::Start(offset)).await {
                    Ok(_) => io.write_all(&data).await,
                    Err(e) => Err(e),
                };
                let _ = tx.send(r);
            }
            RockUsbIO::Read(length, offset, tx) => {
                let mut data = BytesMut::zeroed(length as usize);
                let r = match io.seek(SeekFrom::Start(offset)).await {
                    Ok(_) => io.read(&mut data).await,
                    Err(e) => Err(e),
                }
                .map(|read| {
                    data.truncate(read);
                    data.into()
                });
                let _ = tx.send(r);
            }
            RockUsbIO::Flush(tx) => {
                let r = io.flush().await;
                let _ = tx.send(r);
            }
        }
    }
}

async fn determine_mode(device: DeviceInfo) -> Result<RockUsbMode, RockUsbError> {
    let mut transport = open_device(device)?;
    match transport.flash_info().await {
        Ok(info) => {
            let name = transport
                .flash_id()
                .await
                .map(|f| f.to_str().trim_end().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            Ok(RockUsbMode::Loader((name, info.size())))
        }
        Err(_) => Ok(RockUsbMode::MaskRom),
    }
}

async fn write_maskrom_area(
    device: DeviceInfo,
    area: u16,
    data: Bytes,
) -> Result<(), RockUsbError> {
    let mut transport = open_device(device)?;
    transport.write_maskrom_area(area, &data).await?;
    Ok(())
}

async fn rockusb_task(bus: u8, dev: u8, mut commands: mpsc::Receiver<RockUsbCommand>) {
    let Some(info) = crate::utils::nusb_info_from_bus_dev(bus, dev) else {
        warn!(
            "Failed to start task for usb device {}/{}, not found",
            bus, dev
        );
        return;
    };

    while let Some(command) = commands.recv().await {
        match command {
            RockUsbCommand::Mode(tx) => {
                let _ = tx.send(determine_mode(info.clone()).await);
            }
            RockUsbCommand::WriteMaskromArea((tx, area, data)) => {
                let _ = tx.send(write_maskrom_area(info.clone(), area, data).await);
            }
            RockUsbCommand::Open(tx) => handle_io(info.clone(), tx).await,
        }
    }
}
