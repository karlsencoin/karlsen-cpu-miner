//! Saved miner settings (`karlsen-cpu-miner.json`, next to the executable).

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::dataset::DatasetMode;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Connection {
    /// Public pool over stratum.
    Pool { name: String, url: String },
    /// Solo mining through a karlsen-stratum-bridge.
    SoloBridge { url: String },
    /// Solo mining directly against karlsend over gRPC (mass-preserving submission).
    SoloNode { url: String },
    /// Any other stratum endpoint.
    CustomStratum { url: String },
}

impl Connection {
    pub fn describe(&self) -> String {
        match self {
            Connection::Pool { name, url } => format!("Pool {name} ({url})"),
            Connection::SoloBridge { url } => format!("Solo through karlsen-stratum-bridge ({url})"),
            Connection::SoloNode { url } => format!("Solo directly to karlsend ({url})"),
            Connection::CustomStratum { url } => format!("Custom stratum ({url})"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinerConfig {
    pub mining_address: String,
    pub connection: Connection,
    pub worker_name: String,
    /// `None` = all logical CPUs.
    pub threads: Option<usize>,
    #[serde(with = "dataset_mode_serde")]
    pub dataset_mode: DatasetMode,
    /// `None` = default cache location.
    pub dataset_file: Option<PathBuf>,
}

pub fn config_path() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("karlsen-cpu-miner.json")
}

pub fn load() -> Result<Option<MinerConfig>> {
    let path = config_path();
    if !path.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?))
}

pub fn save(config: &MinerConfig) -> Result<PathBuf> {
    let path = config_path();
    std::fs::write(&path, serde_json::to_string_pretty(config)? + "\n").with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

mod dataset_mode_serde {
    use super::DatasetMode;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(mode: &DatasetMode, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(match mode {
            DatasetMode::Memory => "memory",
            DatasetMode::File => "file",
            DatasetMode::Light => "light",
        })
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DatasetMode, D::Error> {
        match String::deserialize(d)?.as_str() {
            "memory" => Ok(DatasetMode::Memory),
            "file" => Ok(DatasetMode::File),
            "light" => Ok(DatasetMode::Light),
            other => Err(serde::de::Error::custom(format!("unknown dataset mode '{other}'"))),
        }
    }
}
