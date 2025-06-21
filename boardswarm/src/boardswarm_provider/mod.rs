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

use crate::ActuatorId;
use crate::ConsoleId;
use crate::DeviceId;
use crate::VolumeId;
use crate::{registry::Properties, Server};

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

pub struct Provider {
    /* Remote to local mappings */
    actuators: Mutex<HashMap<u64, ActuatorId>>,
    consoles: Mutex<HashMap<u64, ConsoleId>>,
    devices: Mutex<HashMap<u64, DeviceId>>,
    volumes: Mutex<HashMap<u64, VolumeId>>,
    notifier: broadcast::Sender<()>,
}

/*
impl serde::Serialize for Provider {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <Mutex<HashMap<u64, ConsoleId>> as serde::Serialize>::serialize(&self.consoles, serializer)
    }
}
*/

impl Provider {
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
    provider: Arc<Provider>,
    type_: ItemType,
    server: &Server,
    mut remote: Boardswarm,
    id: u64,
    instance: &str,
) {
    let properties = remote.properties(type_, id).await.unwrap();
    let mut properties: Properties = properties.into();
    properties.insert(crate::registry::INSTANCE, instance);

    match type_ {
        ItemType::Console => {
            let local = server.register_console(properties, BoardswarmConsole::new(id, remote));
            provider.consoles.lock().unwrap().insert(id, local);
        }
        ItemType::Actuator => {
            let local = server.register_actuator(properties, BoardswarmActuator::new(id, remote));
            provider.actuators.lock().unwrap().insert(id, local);
        }
        ItemType::Device => match BoardswarmDevice::new(id, remote, provider.clone()).await {
            Ok(d) => {
                let local = server.register_device(properties, d);
                provider.devices.lock().unwrap().insert(id, local);
            }
            Err(e) => warn!("Failed to montior remote device: {e}"),
        },
        ItemType::Volume => match BoardswarmVolume::new(id, remote.clone()).await {
            Ok(v) => {
                let local = server.register_volume(properties, v);
                provider.volumes.lock().unwrap().insert(id, local);
            }
            Err(e) => warn!("Failed to setup remote volume: {e}"),
        },
    }
    let _ = provider.notifier.send(());
}

fn remove_item(provider: &Provider, type_: ItemType, server: &Server, id: u64) {
    match type_ {
        ItemType::Console => {
            let mut consoles = provider.consoles.lock().unwrap();
            if let Some(local) = consoles.remove(&id) {
                server.unregister_console(local)
            }
        }
        ItemType::Actuator => {
            let mut actuators = provider.actuators.lock().unwrap();
            if let Some(local) = actuators.remove(&id) {
                server.unregister_actuator(local)
            }
        }
        ItemType::Device => {
            let mut devices = provider.devices.lock().unwrap();
            if let Some(local) = devices.remove(&id) {
                server.unregister_device(local)
            }
        }
        ItemType::Volume => {
            let mut volumes = provider.volumes.lock().unwrap();
            if let Some(local) = volumes.remove(&id) {
                server.unregister_volume(local)
            }
        }
    }
    let _ = provider.notifier.send(());
}

async fn monitor_items(
    provider: Arc<Provider>,
    type_: ItemType,
    mut remote: Boardswarm,
    server: Server,
    instance: &str,
) {
    let monitor = remote.monitor(type_).await.unwrap();
    pin_mut!(monitor);
    while let Ok(Some(event)) = monitor.try_next().await {
        match event {
            ItemEvent::Added(items) => {
                for i in items {
                    add_item(
                        provider.clone(),
                        type_,
                        &server,
                        remote.clone(),
                        i.id,
                        instance,
                    )
                    .await
                }
            }
            ItemEvent::Removed(removed) => {
                remove_item(&provider, type_, &server, removed);
            }
        }
    }

    // If the connection breaks; drop all registrations
    match type_ {
        ItemType::Console => {
            for (_remote, local) in provider.consoles.lock().unwrap().drain() {
                server.unregister_console(local);
            }
        }
        ItemType::Actuator => {
            for (_remote, local) in provider.actuators.lock().unwrap().drain() {
                server.unregister_actuator(local);
            }
        }
        ItemType::Device => {
            for (_remote, local) in provider.devices.lock().unwrap().drain() {
                server.unregister_device(local);
            }
        }
        ItemType::Volume => {
            for (_remote, local) in provider.volumes.lock().unwrap().drain() {
                server.unregister_volume(local);
            }
        }
    }
}

pub fn start_provider(name: String, parameters: serde_yaml::Value, server: Server) {
    let provider = Arc::new(Provider::new());
    let parameters: BoardswarmParameters = serde_yaml::from_value(parameters).unwrap();
    let uri: Uri = parameters.uri.parse().unwrap();

    tokio::spawn(async move {
        let token_path = server.config_dir().join(parameters.token);
        let token = match tokio::fs::read_to_string(token_path).await {
            Ok(token) => token.trim_end().to_string(),
            Err(e) => {
                warn!("Failed to read token for {name}: {e}");
                return;
            }
        };
        loop {
            let _span = tracing::span!(tracing::Level::INFO, "boardswarm", name);
            let mut boardswarm = BoardswarmBuilder::new(uri.clone());
            boardswarm.auth_static(&token);
            if let Ok(remote) = boardswarm.connect().await {
                info!("Connected to {}", name);
                let consoles = monitor_items(
                    provider.clone(),
                    ItemType::Console,
                    remote.clone(),
                    server.clone(),
                    &name,
                );
                let actuators = monitor_items(
                    provider.clone(),
                    ItemType::Actuator,
                    remote.clone(),
                    server.clone(),
                    &name,
                );
                let devices = monitor_items(
                    provider.clone(),
                    ItemType::Device,
                    remote.clone(),
                    server.clone(),
                    &name,
                );
                let volumes = monitor_items(
                    provider.clone(),
                    ItemType::Volume,
                    remote,
                    server.clone(),
                    &name,
                );

                join!(consoles, actuators, devices, volumes);
                info!("Connection to {} failed", name);
            }
            // TODO move to exponential backoff
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}
