use std::path::PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    pub http: HttpConfig,
    pub storage: StorageConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    pub bind: String,
    pub port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// default zstd level for sealed segments (higher = smaller, slower)
    pub zstd_level: i32,
    /// max memtable records before forcing a flush
    pub memtable_max_records: usize,
    /// max bytes buffered in WAL before forcing a flush
    pub memtable_max_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            http: HttpConfig { bind: "0.0.0.0".into(), port: 9200 },
            storage: StorageConfig {
                zstd_level: 3,
                memtable_max_records: 50_000,
                memtable_max_bytes: 64 * 1024 * 1024,
            },
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> crate::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        toml::from_str(&text).map_err(|e| crate::Error::BadRequest(format!("config: {e}")))
    }
}
