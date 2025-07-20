#![allow(dead_code)]
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Result;
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: Server,
    pub providers: Vec<Provider>,
    pub devices: Vec<Device>,
}

#[derive(Default, Debug, Deserialize)]
pub struct Server {
    pub listen: Option<String>,
    pub certificate: Option<Certificate>,
    pub authentication: Vec<Authentication>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Authentication {
    #[serde(rename = "oidc")]
    Oidc {
        description: String,
        uri: String,
        client: String,
        audience: Vec<String>,
    },
    #[serde(rename = "jwks")]
    Jwks {
        path: PathBuf,
        identifier: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
pub struct Certificate {
    pub chain: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Provider {
    pub name: String,
    pub provider: String,
    pub parameters: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct Device {
    pub name: String,
    pub consoles: Vec<Console>,
    pub modes: Vec<Mode>,
    #[serde(default)]
    pub volumes: Vec<Volume>,
}

#[derive(Debug, Deserialize)]
pub struct Console {
    pub name: String,
    #[serde(default)]
    pub default: bool,
    pub parameters: serde_yaml::Value,
    #[serde(rename = "match")]
    pub match_: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct Volume {
    pub name: String,
    #[serde(rename = "match")]
    pub match_: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct Mode {
    pub name: String,
    pub depends: Option<String>,
    pub sequence: Vec<ModeStep>,
}

#[derive(Debug, Deserialize)]
pub struct ModeStep {
    #[serde(rename = "match")]
    pub match_: HashMap<String, String>,
    pub parameters: serde_yaml::Value,
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub stabilisation: Option<Duration>,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config> {
        info!("Loading configuration file {}", path.as_ref().display());
        let file = std::fs::File::open(path)?;
        Ok(serde_yaml::from_reader(file)?)
    }
}
