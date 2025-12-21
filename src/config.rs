use {
    richat_client::grpc::ConfigGrpcClient,
    richat_shared::tracing::ConfigTracing,
    rocksdb::DBCompressionType,
    serde::Deserialize,
    std::{
        fs::read_to_string as read_to_string_sync,
        path::{Path, PathBuf},
        time::Duration,
    },
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub logs: ConfigTracing,
    pub state_init: ConfigStateInit,
    pub storage: ConfigStorage,
    pub source: ConfigSource,
    pub bank: ConfigBank,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(file: P) -> anyhow::Result<Self> {
        let config = read_to_string_sync(&file)?;
        if matches!(
            file.as_ref().extension().and_then(|e| e.to_str()),
            Some("yml") | Some("yaml")
        ) {
            serde_yaml::from_str(&config).map_err(Into::into)
        } else {
            json5::from_str(&config).map_err(Into::into)
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStateInit {
    pub endpoint: String,
    #[serde(default = "ConfigStateInit::default_segments")]
    pub segments: u8,
    pub path: PathBuf,
    #[serde(default)]
    pub compression: ConfigStorageRocksdbCompression,
}

impl ConfigStateInit {
    const fn default_segments() -> u8 {
        16
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum ConfigStorageRocksdbCompression {
    #[default]
    None,
    Snappy,
    Zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
}

impl From<ConfigStorageRocksdbCompression> for DBCompressionType {
    fn from(value: ConfigStorageRocksdbCompression) -> Self {
        match value {
            ConfigStorageRocksdbCompression::None => Self::None,
            ConfigStorageRocksdbCompression::Snappy => Self::Snappy,
            ConfigStorageRocksdbCompression::Zlib => Self::Zlib,
            ConfigStorageRocksdbCompression::Bz2 => Self::Bz2,
            ConfigStorageRocksdbCompression::Lz4 => Self::Lz4,
            ConfigStorageRocksdbCompression::Lz4hc => Self::Lz4hc,
            ConfigStorageRocksdbCompression::Zstd => Self::Zstd,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorage {
    pub path: PathBuf,
    #[serde(default)]
    pub compression: ConfigStorageRocksdbCompression,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigSource {
    #[serde(default)]
    pub reconnect: Option<ConfigSourceReconnect>,
    #[serde(flatten)]
    pub config: ConfigGrpcClient,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceReconnect {
    #[serde(with = "humantime_serde")]
    pub backoff_init: Duration,
    #[serde(with = "humantime_serde")]
    pub backoff_max: Duration,
}

impl Default for ConfigSourceReconnect {
    fn default() -> Self {
        Self {
            backoff_init: Duration::from_millis(100),
            backoff_max: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigBank {
    #[serde(default = "ConfigBank::updates_channel_size_default")]
    pub updates_channel_size: usize,
}

impl ConfigBank {
    const fn updates_channel_size_default() -> usize {
        32_768
    }
}
