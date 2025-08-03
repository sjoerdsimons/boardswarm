use bytes::Bytes;
use proc_mounts::{MountInfo, MountIter};
use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::OsStr,
    io::SeekFrom,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_stream::StreamExt;
use tracing::{debug, instrument, trace, warn};

use crate::{
    provider::Provider,
    registry::Properties,
    udev::{DeviceEvent, DeviceRegistrations, PreRegistration},
    Volume, VolumeError, VolumeTarget, VolumeTargetInfo,
};

pub const PROVIDER: &str = "eswin-eic7700-storage";

const MOUNT_TIMEOUT: Duration = Duration::from_secs(10);
const MOUNT_INTERVAL: Duration = Duration::from_secs(1);

const TARGET_BOOTLOADER: (&str, &str) = ("bootloader", "bootloader_ddr5_secboot.bin");

#[derive(Deserialize, Debug, Default)]
struct EswinEic7700StorageParameters {
    #[serde(rename = "match")]
    match_: HashMap<String, String>,
    mountpoint: String,
}

#[instrument(skip_all)]
pub async fn start_provider(provider: Provider) {
    let mut devices = crate::udev::DeviceStream::new("block").unwrap();
    let parameters: EswinEic7700StorageParameters = if let Some(parameters) = provider.parameters()
    {
        serde_yaml::from_value(parameters.clone()).unwrap()
    } else {
        Default::default()
    };
    let registrations = DeviceRegistrations::new(provider);
    while let Some(d) = devices.next().await {
        match d {
            DeviceEvent::Add { device, seqnum } => {
                let name = device
                    .devnode()
                    .and_then(|n| n.file_name())
                    .map(OsStr::to_string_lossy)
                    .unwrap_or(Cow::from("unknown"));
                let properties = device.properties(name);
                if !properties.matches(&parameters.match_) {
                    debug!(
                        "Ignoring device {} - {:?}",
                        device.syspath().display(),
                        properties,
                    );
                    continue;
                }
                let prereg = registrations.pre_register(&device, seqnum);
                let mountpoint = parameters.mountpoint.clone();
                tokio::spawn(async move {
                    if let Err(e) = setup_volume(prereg, Path::new(&mountpoint), properties).await {
                        warn!("Failed to setup volume: {e}");
                    }
                });
            }
            DeviceEvent::Remove(device) => registrations.remove(&device),
        }
    }
}

#[instrument(skip(r, properties))]
async fn setup_volume(
    r: PreRegistration,
    mountpoint: &Path,
    properties: Properties,
) -> anyhow::Result<()> {
    let devname = properties
        .get("udev.DEVNAME")
        .ok_or_else(|| anyhow::anyhow!("Failed to read udev property DEVNAME"))?;
    let id = properties
        .get("udev.ID_PATH")
        .ok_or_else(|| anyhow::anyhow!("Failed to read udev property ID_PATH"))?;
    let path = tokio::fs::canonicalize(mountpoint).await?.join(id);
    tokio::time::timeout(
        MOUNT_TIMEOUT,
        wait_mount(
            Path::new(devname),
            &path,
            tokio::time::interval(MOUNT_INTERVAL),
        ),
    )
    .await??;
    r.register_volume(properties, EswinEic7700StorageVolume::new(path));
    Ok(())
}

#[instrument(skip(interval_))]
async fn wait_mount(
    source: &Path,
    dest: &Path,
    mut interval_: tokio::time::Interval,
) -> anyhow::Result<MountInfo> {
    loop {
        trace!("Waiting mount point");
        for mount in MountIter::new()? {
            let mount = mount?;
            if mount.source == source && mount.dest == dest {
                trace!("Found mount point: {:?}", mount);
                return Ok(mount);
            }
        }
        interval_.tick().await;
    }
}

#[derive(Debug)]
pub struct EswinEic7700StorageVolume {
    path: PathBuf,
    targets: Vec<VolumeTargetInfo>,
}

impl EswinEic7700StorageVolume {
    fn new(path: PathBuf) -> Self {
        let (name, _) = TARGET_BOOTLOADER;
        let target = VolumeTargetInfo {
            name: name.to_string(),
            readable: false,
            writable: true,
            seekable: true,
            size: None,
            blocksize: None,
        };
        Self {
            path,
            targets: vec![target],
        }
    }
}

impl From<std::io::Error> for crate::VolumeError {
    fn from(e: std::io::Error) -> Self {
        crate::VolumeError::Internal(e.to_string())
    }
}

#[async_trait::async_trait]
impl Volume for EswinEic7700StorageVolume {
    fn targets(&self) -> (&[VolumeTargetInfo], bool) {
        (self.targets.as_slice(), true)
    }

    async fn open(
        &self,
        target: &str,
        _length: Option<u64>,
    ) -> Result<(VolumeTargetInfo, Box<dyn VolumeTarget>), VolumeError> {
        if let Some(volume_target) = self.targets.iter().find(|t| t.name == target) {
            let (_, filename) = TARGET_BOOTLOADER;
            let path = self.path.join(filename);
            let file = tokio::fs::File::create(path).await?;
            Ok((
                volume_target.clone(),
                Box::new(EswinEic7700StorageTarget { file }),
            ))
        } else {
            warn!("Unknown target requested");
            Err(VolumeError::UnknownTargetRequested)
        }
    }

    async fn commit(&self) -> Result<(), VolumeError> {
        // Once the file is written to the filesystem, the board runs the
        // bootstrapping sequence (loads bootloader firmware file in memory,
        // unregisters usb mass-storage device and jumps into the loaded
        // bootloader). This is guaranteed to happen by the flush/shutdown calls
        // on the VolumeTarget. There's no further action to be run on commit,
        // e.g. sync or umount calls won't make the board to reboot.
        Ok(())
    }
}

struct EswinEic7700StorageTarget {
    file: tokio::fs::File,
}

impl std::fmt::Debug for EswinEic7700StorageTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO make more meaningful
        f.debug_struct("EswinEic7700StorageTarget")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl VolumeTarget for EswinEic7700StorageTarget {
    async fn write(&mut self, data: Bytes, offset: u64, completion: crate::WriteCompletion) {
        if let Err(e) = self.file.seek(SeekFrom::Start(offset)).await {
            completion.complete(Err(tonic::Status::internal(e.to_string())));
            return;
        };
        completion.complete(
            self.file
                .write_all(&data)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))
                .and(Ok(data.len() as u64)),
        );
    }

    async fn flush(&mut self, completion: crate::FlushCompletion) {
        completion.complete(
            self.file
                .flush()
                .await
                .map_err(|e| tonic::Status::internal(e.to_string())),
        );
    }

    async fn shutdown(&mut self, completion: crate::ShutdownCompletion) {
        completion.complete(
            self.file
                .shutdown()
                .await
                .map_err(|e| tonic::Status::internal(e.to_string())),
        );
    }
}
