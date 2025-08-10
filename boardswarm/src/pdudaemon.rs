use std::fmt::Display;

use pdudaemon_client::PduDaemon;
use serde::Deserialize;
use tracing::instrument;

use crate::{provider::Provider, registry::Properties, ActuatorError};

pub const PROVIDER: &str = "pdudaemon";

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum Ports {
    Num(u16),
    Ports(Vec<String>),
}

#[derive(Deserialize, Debug)]
struct Pdu {
    name: String,
    ports: Ports,
}

#[derive(Deserialize, Debug)]
struct PduDaemonParameters {
    uri: String,
    pdus: Vec<Pdu>,
}

fn setup_actuator<D: Display>(provider: &Provider, daemon: &PduDaemon, pdu_name: &str, port: D) {
    let name = format!("{}.{}.port-{}", provider.name(), pdu_name, port);
    let port_name = port.to_string();

    let mut properties = Properties::new(name);
    properties.insert("pdudaemon.pdu", pdu_name);
    properties.insert("pdudaemon.port", port_name.clone());

    let actuator = PduDaemonActuator::new(daemon.clone(), pdu_name.to_string(), port_name);
    provider.register_actuator(properties, actuator);
}

#[instrument(skip_all)]
pub fn start_provider(provider: Provider) {
    let parameters: PduDaemonParameters =
        serde_yaml::from_value(provider.parameters().cloned().unwrap()).unwrap();

    let daemon = PduDaemon::new(&parameters.uri).unwrap();
    for pdu in parameters.pdus {
        match pdu.ports {
            Ports::Num(ports) => {
                for i in 1..=ports {
                    setup_actuator(&provider, &daemon, &pdu.name, i);
                }
            }
            Ports::Ports(ports) => {
                for i in ports {
                    setup_actuator(&provider, &daemon, &pdu.name, i);
                }
            }
        }
    }
}

#[derive(Debug)]
struct PduDaemonActuator {
    daemon: PduDaemon,
    hostname: String,
    port: String,
}

impl PduDaemonActuator {
    fn new(daemon: PduDaemon, hostname: String, port: String) -> Self {
        Self {
            daemon,
            hostname,
            port,
        }
    }
}

#[async_trait::async_trait]
impl crate::Actuator for PduDaemonActuator {
    async fn set_mode(
        &self,
        parameters: Box<dyn erased_serde::Deserializer<'static> + Send>,
    ) -> Result<(), ActuatorError> {
        #[derive(Deserialize)]
        struct ModeParameters {
            mode: String,
        }
        let parameters = ModeParameters::deserialize(parameters).unwrap();
        match parameters.mode.as_str() {
            "on" => self.daemon.on(&self.hostname, &self.port).await,
            "off" => self.daemon.off(&self.hostname, &self.port).await,
            _ => todo!(),
        }
        .map_err(|_e| ActuatorError {})
    }
}
