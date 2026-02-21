use {
    ahash::HashMap,
    human_size::Size,
    hyper::{
        HeaderMap,
        header::{HeaderName, HeaderValue},
    },
    richat_client::grpc::ConfigGrpcClient,
    richat_shared::config::{ConfigTokio, deserialize_num_str},
    rocksdb::DBCompressionType,
    serde::{
        Deserialize,
        de::{self, Deserializer},
    },
    std::{net::SocketAddr, path::PathBuf, str::FromStr, time::Duration},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub monitoring: ConfigMonitoring,
    pub source: ConfigSource,
    pub storage: ConfigStorage,
    pub rpc: ConfigRpc,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigMonitoring {
    pub logs_json: bool,
    pub otlp_endpoint: Option<String>,
    pub prometheus_endpoint: SocketAddr,
}

impl Default for ConfigMonitoring {
    fn default() -> Self {
        Self {
            logs_json: false,
            otlp_endpoint: None,
            prometheus_endpoint: SocketAddr::from(([127, 0, 0, 1], 9001)),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSource {
    /// Tokio runtime for Source
    pub tokio: ConfigTokio,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorage {
    pub path: PathBuf,
    #[serde(default)]
    pub compression: ConfigStorageRocksdbCompression,
    pub init: ConfigStorageInit,
    pub blocks: ConfigBlocks,
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
pub struct ConfigStorageInit {
    pub endpoint: String,
    #[serde(default = "ConfigStorageInit::default_segments")]
    pub segments: u8,
}

impl ConfigStorageInit {
    const fn default_segments() -> u8 {
        16
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigBlocks {
    pub updates_channel_size: usize,
    /// Max number of read requests in the queue
    #[serde(deserialize_with = "deserialize_num_str")]
    pub request_channel_capacity: usize,
    /// Number of read workers
    #[serde(deserialize_with = "deserialize_num_str")]
    pub read_workers: usize,
}

impl Default for ConfigBlocks {
    fn default() -> Self {
        Self {
            updates_channel_size: 512,
            request_channel_capacity: 128 * 1024,
            read_workers: num_cpus::get(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigRpc {
    /// Endpoint of RPC service
    pub endpoint: SocketAddr,
    /// Tokio runtime for RPC
    pub tokio: ConfigTokio,
    /// Max body size limit in bytes
    #[serde(deserialize_with = "ConfigRpc::deserialize_humansize_usize")]
    pub body_limit: usize,
    /// Extra headers added to response
    #[serde(deserialize_with = "ConfigRpc::deserialize_extra_headers")]
    pub extra_headers: HeaderMap,
    /// Request timeout
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
}

impl Default for ConfigRpc {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::from(([127, 0, 0, 1], 9000)),
            tokio: Default::default(),
            body_limit: 10 * 1024,
            extra_headers: Default::default(),
            request_timeout: Duration::from_secs(60),
        }
    }
}

impl ConfigRpc {
    fn deserialize_humansize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let size: &str = Deserialize::deserialize(deserializer)?;

        Size::from_str(size)
            .map(|size| size.to_bytes())
            .map_err(|error| de::Error::custom(format!("failed to parse size {size:?}: {error}")))
    }

    fn deserialize_humansize_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize_humansize(deserializer).map(|value| value as usize)
    }

    fn deserialize_extra_headers<'de, D>(deserializer: D) -> Result<HeaderMap, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = HeaderMap::new();
        for (key, value) in HashMap::<String, String>::deserialize(deserializer)? {
            map.insert(
                HeaderName::try_from(&key)
                    .map_err(|_| de::Error::custom("failed to parse header key: {key}"))?,
                HeaderValue::try_from(&value)
                    .map_err(|_| de::Error::custom("failed to parse header value: {value}"))?,
            );
        }
        Ok(map)
    }
}
