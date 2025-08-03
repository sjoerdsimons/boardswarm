use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use boardswarm_client::client::BoardswarmBuilder;
use boardswarm_client::client::{Boardswarm, ItemEvent};
use boardswarm_protocol::ItemType;
use futures::{pin_mut, TryStreamExt};
use serde::Deserialize;
use tokio::join;
use tokio::sync::broadcast;
use tonic::transport::Uri;
use tracing::info;
use tracing::warn;

use crate::provider::Provider;
use crate::registry::Properties;
use crate::ActuatorId;
use crate::ConsoleId;
use crate::DeviceId;
use crate::VolumeId;

use self::actuator::BoardswarmActuator;
use self::console::BoardswarmConsole;
use self::device::BoardswarmDevice;
use self::volume::BoardswarmVolume;

mod actuator;
mod console;
mod device;
mod volume;

pub const PROVIDER: &str = "boardswarm";

#[derive(Deserialize, Debug)]
struct BoardswarmParameters {
    uri: String,
    token: PathBuf,
}

pub struct BsProvider {
    /* Remote to local mappings */
    actuators: Mutex<HashMap<u64, ActuatorId>>,
    consoles: Mutex<HashMap<u64, ConsoleId>>,
    devices: Mutex<HashMap<u64, DeviceId>>,
    volumes: Mutex<HashMap<u64, VolumeId>>,
    notifier: broadcast::Sender<()>,
}

impl BsProvider {
    fn new() -> Self {
        let actuators = Mutex::new(HashMap::new());
        let consoles = Mutex::new(HashMap::new());
        let volumes = Mutex::new(HashMap::new());
        let devices = Mutex::new(HashMap::new());
        Self {
            actuators,
            consoles,
            volumes,
            devices,
            notifier: broadcast::channel(1).0,
        }
    }

    pub fn console_id(&self, remote: u64) -> Option<ConsoleId> {
        self.consoles.lock().unwrap().get(&remote).copied()
    }

    #[allow(dead_code)]
    pub fn actuator_id(&self, remote: u64) -> Option<ActuatorId> {
        self.actuators.lock().unwrap().get(&remote).copied()
    }

    pub fn volume_id(&self, remote: u64) -> Option<VolumeId> {
        self.volumes.lock().unwrap().get(&remote).copied()
    }

    pub fn watch(&self) -> broadcast::Receiver<()> {
        self.notifier.subscribe()
    }
}

async fn add_item(
    bsprovider: Arc<BsProvider>,
    type_: ItemType,
    provider: &Provider,
    mut remote: Boardswarm,
    id: u64,
    instance: &str,
) {
    let properties = remote.properties(type_, id).await.unwrap();
    let mut properties: Properties = properties.into();
    properties.insert(crate::registry::INSTANCE, instance);

    match type_ {
        ItemType::Console => {
            let local = provider.register_console(properties, BoardswarmConsole::new(id, remote));
            bsprovider.consoles.lock().unwrap().insert(id, local);
        }
        ItemType::Actuator => {
            let local = provider.register_actuator(properties, BoardswarmActuator::new(id, remote));
            bsprovider.actuators.lock().unwrap().insert(id, local);
        }
        ItemType::Device => match BoardswarmDevice::new(id, remote, bsprovider.clone()).await {
            Ok(d) => {
                let local = provider.register_device(properties, d);
                bsprovider.devices.lock().unwrap().insert(id, local);
            }
            Err(e) => warn!("Failed to montior remote device: {e}"),
        },
        ItemType::Volume => match BoardswarmVolume::new(id, remote.clone()).await {
            Ok(v) => {
                let local = provider.register_volume(properties, v);
                bsprovider.volumes.lock().unwrap().insert(id, local);
            }
            Err(e) => warn!("Failed to setup remote volume: {e}"),
        },
    }
    let _ = provider.notifier.send(());
}

fn remove_item(bsprovider: &BsProvider, type_: ItemType, provider: &Provider, id: u64) {
    match type_ {
        ItemType::Console => {
            let mut consoles = bsprovider.consoles.lock().unwrap();
            if let Some(local) = consoles.remove(&id) {
                provider.unregister_console(local)
            }
        }
        ItemType::Actuator => {
            let mut actuators = bsprovider.actuators.lock().unwrap();
            if let Some(local) = actuators.remove(&id) {
                provider.unregister_actuator(local)
            }
        }
        ItemType::Device => {
            let mut devices = bsprovider.devices.lock().unwrap();
            if let Some(local) = devices.remove(&id) {
                provider.unregister_device(local)
            }
        }
        ItemType::Volume => {
            let mut volumes = bsprovider.volumes.lock().unwrap();
            if let Some(local) = volumes.remove(&id) {
                provider.unregister_volume(local)
            }
        }
    }
    let _ = bsprovider.notifier.send(());
}

async fn monitor_items(
    bsprovider: Arc<BsProvider>,
    type_: ItemType,
    mut remote: Boardswarm,
    provider: Provider,
) {
    let monitor = remote.monitor(type_).await.unwrap();
    pin_mut!(monitor);
    while let Ok(Some(event)) = monitor.try_next().await {
        match event {
            ItemEvent::Added(items) => {
                for i in items {
                    add_item(
                        bsprovider.clone(),
                        type_,
                        &provider,
                        remote.clone(),
                        i.id,
                        provider.name(),
                    )
                    .await
                }
            }
            ItemEvent::Removed(removed) => {
                remove_item(&bsprovider, type_, &provider, removed);
            }
        }
    }

    // If the connection breaks; drop all registrations
    match type_ {
        ItemType::Console => {
            for (_remote, local) in bsprovider.consoles.lock().unwrap().drain() {
                provider.unregister_console(local);
            }
        }
        ItemType::Actuator => {
            for (_remote, local) in bsprovider.actuators.lock().unwrap().drain() {
                provider.unregister_actuator(local);
            }
        }
        ItemType::Device => {
            for (_remote, local) in bsprovider.devices.lock().unwrap().drain() {
                provider.unregister_device(local);
            }
        }
        ItemType::Volume => {
            for (_remote, local) in bsprovider.volumes.lock().unwrap().drain() {
                provider.unregister_volume(local);
            }
        }
    }
}

pub fn start_provider(provider: Provider) {
    let bsprovider = Arc::new(BsProvider::new());
    let parameters: BoardswarmParameters =
        serde_yaml::from_value(provider.parameters().cloned().unwrap()).unwrap();
    let uri: Uri = parameters.uri.parse().unwrap();

    tokio::spawn(async move {
        let token_path = provider.server().config_dir().join(parameters.token);
        let token = match tokio::fs::read_to_string(token_path).await {
            Ok(token) => token.trim_end().to_string(),
            Err(e) => {
                warn!("Failed to read token for {}: {}", provider.name(), e);
                return;
            }
        };
        loop {
            let _span = tracing::span!(tracing::Level::INFO, "boardswarm", "{}", provider.name());
            let mut boardswarm = BoardswarmBuilder::new(uri.clone());
            boardswarm.auth_static(&token);
            if let Ok(remote) = boardswarm.connect().await {
                info!("Connected to {}", provider.name());
                let consoles = monitor_items(
                    bsprovider.clone(),
                    ItemType::Console,
                    remote.clone(),
                    provider.clone(),
                );
                let actuators = monitor_items(
                    bsprovider.clone(),
                    ItemType::Actuator,
                    remote.clone(),
                    provider.clone(),
                );
                let devices = monitor_items(
                    bsprovider.clone(),
                    ItemType::Device,
                    remote.clone(),
                    provider.clone(),
                );
                let volumes = monitor_items(
                    bsprovider.clone(),
                    ItemType::Volume,
                    remote,
                    provider.clone(),
                );

                join!(consoles, actuators, devices, volumes);
                info!("Connection to {} failed", provider.name());
            }
            // TODO move to exponential backoff
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}
