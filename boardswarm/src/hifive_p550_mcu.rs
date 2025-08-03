use regex::Regex;
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf};
use strum::{EnumIter, IntoEnumIterator};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::provider::Provider;
use crate::udev::{DeviceRegistrations, PreRegistration};
use crate::{get_bit, ActuatorError};
use crate::{
    registry::{self, Properties},
    serial::SerialProvider,
    udev::Device,
};

pub const PROVIDER: &str = "hifive-p550-mcu";
const MCU_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const MCU_PROMPT: &[u8] = b"#cmd:";

#[derive(Error, Debug)]
enum McuError {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Timeout error")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Unexpected EOF error")]
    UnexpectedEof,
    #[error("Response parse error")]
    ResponseParse,
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, strum_macros::Display, strum_macros::AsRefStr, EnumIter,
)]
#[strum(serialize_all = "kebab-case")]
enum McuCommand {
    HifiveP550McuSomPower,
    HifiveP550McuBootSel,
}

#[derive(Debug, Eq, PartialEq, Deserialize)]
struct SomPowerParameters {
    mode: SomPowerMode,
}

#[derive(Debug, Eq, PartialEq, Deserialize, strum_macros::Display, strum_macros::AsRefStr)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
enum SomPowerMode {
    Off,
    On,
}

impl SomPowerMode {
    fn to_status(&self) -> (u8, String) {
        match self {
            SomPowerMode::Off => (0, "STOP_POWER".into()),
            SomPowerMode::On => (1, "POWERON".into()),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize)]
struct BootSelParameters {
    mode: BootSelMode,
}

#[derive(Debug, Eq, PartialEq, Deserialize, strum_macros::Display)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
enum BootSelMode {
    Hardware,
    Usb,
}

impl BootSelMode {
    fn to_status(&self) -> (String, u8, Option<String>) {
        match self {
            BootSelMode::Hardware => ("HW".into(), 0x0, None),
            BootSelMode::Usb => {
                let value = 0x3;
                ("SW".into(), value, Some(Self::format_bits(&value)))
            }
        }
    }

    fn format_bits(value: &u8) -> String {
        format!(
            "{} {} {} {}",
            get_bit!(value, 3),
            get_bit!(value, 2),
            get_bit!(value, 1),
            get_bit!(value, 0),
        )
    }
}

#[derive(Deserialize, Clone, Debug, Default)]
struct HifiveP550MCUParameters {
    #[serde(rename = "match", default)]
    match_: HashMap<String, String>,
}

pub struct HifiveP550MCUProvider {
    parameters: HifiveP550MCUParameters,
    registrations: DeviceRegistrations,
}

impl HifiveP550MCUProvider {
    pub fn new(provider: Provider) -> Self {
        let parameters: HifiveP550MCUParameters =
            serde_yaml::from_value(provider.parameters().cloned().unwrap_or_default()).unwrap();
        Self {
            parameters,
            registrations: DeviceRegistrations::new(provider),
        }
    }
}

impl SerialProvider for HifiveP550MCUProvider {
    fn handle(&mut self, device: &crate::udev::Device, seqnum: u64) -> bool {
        if device.property_u64("ID_VENDOR_ID", 16) != Some(0x0403) {
            return false;
        };
        if device.property_u64("ID_MODEL_ID", 16) != Some(0x6011) {
            return false;
        };
        if let Some(node) = device.devnode() {
            if let Some(name) = node.file_name() {
                let properties = device.properties(name.to_string_lossy());
                if !properties.matches(&self.parameters.match_) {
                    debug!(
                        "Ignoring device {} - {:?}",
                        device.syspath().display(),
                        properties,
                    );
                    return false;
                }
                let prereg = self.registrations.pre_register(device, seqnum);
                tokio::spawn(setup_hifive_p550_mcu(
                    prereg,
                    node.to_path_buf(),
                    properties,
                ));
                return true;
            }
        }
        false
    }

    fn remove(&mut self, device: &Device) {
        self.registrations.remove(device);
    }
}

async fn setup_hifive_p550_mcu(prereg: PreRegistration, node: PathBuf, properties: Properties) {
    info!(
        "Setting up Hifive P550 MCU serial connection for {}",
        node.display()
    );
    let port = match tokio_serial::new(node.to_string_lossy(), 115200).open_native_async() {
        Ok(port) => port,
        Err(e) => {
            warn!("Failed to open serial port: {e}");
            return;
        }
    };

    let (sender, exec) = tokio::sync::mpsc::channel(16);
    tokio::spawn(process(port, exec));

    prereg.register_batch(|r| {
        for command in McuCommand::iter() {
            let mut properties = properties.clone();
            properties.insert(registry::NAME, command.as_ref());
            let actuator = HifiveP550MCUActuator {
                command,
                sender: sender.clone(),
            };
            r.register_actuator(properties, actuator);
        }
    });
}

enum ChannelMessage {
    SomPower(SomPowerParameters),
    BootSel(BootSelParameters),
}

impl McuCommand {
    fn to_channel_message(
        self,
        parameters: Box<dyn erased_serde::Deserializer<'static> + Send>,
    ) -> Result<ChannelMessage, erased_serde::Error> {
        let message = match self {
            McuCommand::HifiveP550McuSomPower => {
                let parameters = SomPowerParameters::deserialize(parameters)?;
                ChannelMessage::SomPower(parameters)
            }
            McuCommand::HifiveP550McuBootSel => {
                let parameters = BootSelParameters::deserialize(parameters)?;
                ChannelMessage::BootSel(parameters)
            }
        };
        Ok(message)
    }
}

async fn process_som_power(
    reader: &mut BufReader<ReadHalf<SerialStream>>,
    write: &mut WriteHalf<SerialStream>,
    parameters: &SomPowerParameters,
) -> Result<(), McuError> {
    let (value, message) = parameters.mode.to_status();

    mcu_wait_prompt(reader, write).await?;
    mcu_send(write, "sompower-g\n").await?;

    let response = Regex::new(r"^Som Power Status: (?<mode>.*)$").unwrap();
    let ret = mcu_wait_response(reader, &response, MCU_RESPONSE_TIMEOUT).await?;
    let caps = response.captures(&ret).ok_or(McuError::ResponseParse)?;
    if caps["mode"].to_lowercase() == parameters.mode.as_ref() {
        return Ok(());
    }

    let request = format!("sompower-s {}\n", value);
    mcu_send(write, &request).await?;
    let response = Regex::new(&message).unwrap();
    mcu_wait_response(reader, &response, MCU_RESPONSE_TIMEOUT).await?;

    Ok(())
}

async fn process_boot_sel(
    reader: &mut BufReader<ReadHalf<SerialStream>>,
    write: &mut WriteHalf<SerialStream>,
    parameters: &BootSelParameters,
) -> Result<(), McuError> {
    let (mode, value, bits) = parameters.mode.to_status();

    mcu_wait_prompt(reader, write).await?;
    mcu_send(write, "bootsel-g\n").await?;

    let response =
        Regex::new(r"^Get: Bootsel Controlled by: (?<mode>.*), bootsel\[3 2 1 0\]:(?<bits>.*)$")
            .unwrap();
    let ret = mcu_wait_response(reader, &response, MCU_RESPONSE_TIMEOUT).await?;
    let caps = response.captures(&ret).ok_or(McuError::ResponseParse)?;
    if caps["mode"] == mode && bits.as_ref().is_none_or(|b| caps["bits"] == *b) {
        return Ok(());
    }

    let request = format!("bootsel-s {} 0x{:x}\n", mode, value);
    mcu_send(write, &request).await?;

    let response =
        Regex::new(r"^Set: Bootsel Controlled by: (?<mode>.*), bootsel\[3 2 1 0\]:(?<bits>.*)$")
            .unwrap();
    let ret = mcu_wait_response(reader, &response, MCU_RESPONSE_TIMEOUT).await?;
    let caps = response.captures(&ret).ok_or(McuError::ResponseParse)?;
    if caps["mode"] == mode && bits.as_ref().is_none_or(|b| caps["bits"] == *b) {
        Ok(())
    } else {
        Err(McuError::ResponseParse)
    }
}

impl ChannelMessage {
    async fn process(
        &self,
        reader: &mut BufReader<ReadHalf<SerialStream>>,
        write: &mut WriteHalf<SerialStream>,
    ) -> Result<(), McuError> {
        match &self {
            ChannelMessage::SomPower(parameters) => {
                process_som_power(reader, write, parameters).await
            }
            ChannelMessage::BootSel(parameters) => {
                process_boot_sel(reader, write, parameters).await
            }
        }
    }
}

#[instrument(skip(write), err)]
async fn mcu_send(write: &mut WriteHalf<SerialStream>, request: &str) -> Result<(), McuError> {
    trace!("request: {:?}", &request);
    write.write_all(request.as_bytes()).await?;
    Ok(())
}

#[instrument(skip(reader), err)]
async fn mcu_wait_response(
    reader: &mut BufReader<ReadHalf<SerialStream>>,
    response: &Regex,
    duration: Duration,
) -> Result<String, McuError> {
    let mut lines = reader.lines();
    loop {
        let line = timeout(duration, lines.next_line())
            .await??
            .ok_or(McuError::UnexpectedEof)?;
        trace!("response: {:?}", &line);
        if response.is_match(&line) {
            return Ok(line);
        }
    }
}

#[instrument(skip(reader, write), err)]
async fn mcu_wait_prompt(
    reader: &mut BufReader<ReadHalf<SerialStream>>,
    write: &mut WriteHalf<SerialStream>,
) -> Result<(), McuError> {
    let mut buf = Vec::new();

    mcu_send(write, "\n").await?;
    trace!("waiting: {:?}", &MCU_PROMPT[0]);
    timeout(
        MCU_RESPONSE_TIMEOUT,
        reader.read_until(MCU_PROMPT[0], &mut buf),
    )
    .await??;

    let mut buf = [0; MCU_PROMPT.len() - 1];
    trace!("waiting: {:?}", &MCU_PROMPT[1..]);
    timeout(MCU_RESPONSE_TIMEOUT, reader.read_exact(&mut buf)).await??;
    trace!("received: {:?}", &buf);

    if buf == MCU_PROMPT[1..] {
        Ok(())
    } else {
        Err(McuError::ResponseParse)
    }
}

async fn process(
    port: tokio_serial::SerialStream,
    mut exec: mpsc::Receiver<(ChannelMessage, oneshot::Sender<Result<(), McuError>>)>,
) {
    let (read, mut write) = tokio::io::split(port);
    let mut reader = BufReader::new(read);

    while let Some((message, resp)) = exec.recv().await {
        let _ = resp.send(message.process(&mut reader, &mut write).await);
    }
}

struct HifiveP550MCUActuator {
    command: McuCommand,
    sender: mpsc::Sender<(ChannelMessage, oneshot::Sender<Result<(), McuError>>)>,
}

impl std::fmt::Debug for HifiveP550MCUActuator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO make more meaningful
        f.debug_struct("HifiveP550MCUActuator")
            .finish_non_exhaustive()
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for crate::ActuatorError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        crate::ActuatorError {}
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for crate::ActuatorError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        crate::ActuatorError {}
    }
}

impl From<erased_serde::Error> for crate::ActuatorError {
    fn from(_: erased_serde::Error) -> Self {
        crate::ActuatorError {}
    }
}

#[async_trait::async_trait]
impl crate::Actuator for HifiveP550MCUActuator {
    async fn set_mode(
        &self,
        parameters: Box<dyn erased_serde::Deserializer<'static> + Send>,
    ) -> Result<(), crate::ActuatorError> {
        let message = self.command.to_channel_message(parameters)?;
        let (tx, rx) = oneshot::channel();
        self.sender.send((message, tx)).await?;
        if let Err(e) = rx.await? {
            error!("Actuator {} failed: {:?}", &self.command, e);
            return Err(ActuatorError());
        }
        Ok(())
    }
}
