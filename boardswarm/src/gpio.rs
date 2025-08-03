use std::{collections::HashMap, path::PathBuf};

use futures::StreamExt;
use serde::Deserialize;
use tokio_gpiod::{Chip, Lines};
use tracing::instrument;
use tracing::{debug, warn};

use crate::provider::Provider;
use crate::ActuatorId;
use crate::{
    registry::{self, Properties},
    udev::DeviceEvent,
};

pub const PROVIDER: &str = "gpio";

#[derive(Deserialize, Debug)]
struct Line {
    line_name: Option<String>,
    line_number: Option<tokio_gpiod::LineId>,
    name: String,
}

#[derive(Deserialize, Debug)]
struct GpioParameters {
    #[serde(rename = "match")]
    match_: HashMap<String, String>,
    lines: Vec<Line>,
}

impl GpioParameters {
    fn lines_by_number(&self) -> impl Iterator<Item = &Line> {
        self.lines.iter().filter(|l| l.line_number.is_some())
    }

    fn lines_by_name(&self) -> impl Iterator<Item = &Line> {
        self.lines.iter().filter(|l| l.line_name.is_some())
    }
}

#[instrument(skip_all, level = "error")]
pub async fn start_provider(provider: Provider) {
    let parameters: GpioParameters =
        serde_yaml::from_value(provider.parameters().cloned().unwrap()).unwrap();
    if parameters.match_.is_empty() {
        warn!("matches is empty - will match any gpio device");
    }

    let mut registration = None;
    let mut devices = crate::udev::DeviceStream::new("gpio").unwrap();
    while let Some(d) = devices.next().await {
        match d {
            DeviceEvent::Add { device, .. } => {
                if registration.is_some() {
                    continue;
                }
                if let Some(path) = device.devnode() {
                    if let Some(name) = path.file_name() {
                        let name = name.to_string_lossy().into_owned();
                        let mut properties = device.properties(name);

                        if !properties.matches(&parameters.match_) {
                            debug!(
                                "Ignoring gpio device {} - {:?}",
                                device.syspath().display(),
                                properties,
                            );

                            continue;
                        }

                        if let Some(ids) =
                            setup_gpio_chip(path.to_path_buf(), &parameters, properties, &provider)
                                .await
                        {
                            registration = Some((device.syspath().to_owned(), ids));
                        }
                    }
                }
            }
            DeviceEvent::Remove(device) => {
                if let Some((p, ids)) = registration.as_ref() {
                    if device.syspath() == p {
                        for i in ids {
                            provider.unregister_actuator(*i);
                        }
                        registration = None
                    }
                }
            }
        }
    }
}

async fn setup_gpio_chip(
    path: PathBuf,
    parameters: &GpioParameters,
    mut properties: Properties,
    provider: &Provider,
) -> Option<Vec<ActuatorId>> {
    let chip = match Chip::new(&path).await {
        Ok(chip) => chip,
        Err(e) => {
            warn!("Failed to open gpio chip {}: {}", path.display(), e);
            return None;
        }
    };
    let mut ids = Vec::new();
    let label = chip.label();
    properties.insert("gpio.chip_label", label);

    for line in parameters.lines_by_number() {
        let line_number = line.line_number.unwrap();
        let id =
            setup_gpio_line(&chip, line_number, &line.name, properties.clone(), provider).await;
        ids.push(id);
    }

    if parameters.lines_by_name().next().is_some() {
        for i in 1..chip.num_lines() {
            let info = chip.line_info(i).await.unwrap();
            if let Some(line) = parameters
                .lines_by_name()
                .find(|l| l.line_name.as_deref().unwrap() == info.name)
            {
                let id = setup_gpio_line(&chip, i, &line.name, properties.clone(), provider).await;
                ids.push(id);
            }
        }
    }
    Some(ids)
}

async fn setup_gpio_line(
    chip: &Chip,
    line: tokio_gpiod::LineId,
    name: &str,
    mut properties: Properties,
    provider: &Provider,
) -> ActuatorId {
    let info = chip.line_info(line).await.unwrap();
    properties.insert(registry::NAME, name);
    if !info.name.is_empty() {
        properties.insert("gpio.line_name", info.name);
    }
    properties.insert("gpio.line_number", format!("{}", line));
    let opts = tokio_gpiod::Options::output([line])
        .drive(tokio_gpiod::Drive::PushPull)
        .consumer("boardswarm");
    let line = chip.request_lines(opts).await.unwrap();
    provider.register_actuator(properties, GpioLine { line })
}

struct GpioLine {
    line: Lines<tokio_gpiod::Output>,
}

impl std::fmt::Debug for GpioLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO make more meaningful
        f.debug_struct("GpioLine").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl crate::Actuator for GpioLine {
    async fn set_mode(
        &self,
        parameters: Box<dyn erased_serde::Deserializer<'static> + Send>,
    ) -> Result<(), crate::ActuatorError> {
        #[derive(Deserialize)]
        struct ModeParameters {
            value: bool,
        }
        let parameters = ModeParameters::deserialize(parameters).unwrap();
        self.line.set_values([parameters.value]).await.unwrap();
        Ok(())
    }
}
