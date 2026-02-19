use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::{BitdexError, Result};
use crate::filter::FilterFieldType;

/// Top-level Bitdex V2 configuration.
///
/// Loaded from TOML or YAML files. Designed for future hot-reloadability:
/// all config sections are cheaply cloneable and can be swapped atomically
/// behind an `Arc<ArcSwap<Config>>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Filter field definitions.
    #[serde(default)]
    pub filter_fields: Vec<FilterFieldConfig>,

    /// Sort field definitions.
    #[serde(default)]
    pub sort_fields: Vec<SortFieldConfig>,

    /// Maximum results per query (hard cap).
    #[serde(default = "default_max_page_size")]
    pub max_page_size: usize,

    /// Trie cache settings.
    #[serde(default)]
    pub cache: CacheConfig,

    /// Autovac interval in seconds.
    #[serde(default = "default_autovac_interval")]
    pub autovac_interval_secs: u64,

    /// Snapshot interval in seconds.
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_secs: u64,

    /// WAL flush strategy: "fsync" or "async".
    #[serde(default = "default_wal_flush_strategy")]
    pub wal_flush_strategy: String,

    /// Prometheus metrics port.
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,

    /// Flush interval for the concurrent engine's background flush thread, in microseconds.
    #[serde(default = "default_flush_interval_us")]
    pub flush_interval_us: u64,

    /// Bounded channel capacity for the write coalescer.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

fn default_max_page_size() -> usize {
    100
}
fn default_autovac_interval() -> u64 {
    3600
}
fn default_snapshot_interval() -> u64 {
    300
}
fn default_wal_flush_strategy() -> String {
    "fsync".to_string()
}
fn default_prometheus_port() -> u16 {
    9090
}
fn default_flush_interval_us() -> u64 {
    100
}
fn default_channel_capacity() -> usize {
    100_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            filter_fields: Vec::new(),
            sort_fields: Vec::new(),
            max_page_size: default_max_page_size(),
            cache: CacheConfig::default(),
            autovac_interval_secs: default_autovac_interval(),
            snapshot_interval_secs: default_snapshot_interval(),
            wal_flush_strategy: default_wal_flush_strategy(),
            prometheus_port: default_prometheus_port(),
            flush_interval_us: default_flush_interval_us(),
            channel_capacity: default_channel_capacity(),
        }
    }
}

impl Config {
    /// Load configuration from a file. Format is detected from the file extension.
    ///
    /// Supported extensions: `.toml`, `.yaml`, `.yml`
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| BitdexError::Config(format!("failed to read {}: {e}", path.display())))?;

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        match ext {
            "toml" => Self::from_toml(&content),
            "yaml" | "yml" => Self::from_yaml(&content),
            other => Err(BitdexError::Config(format!(
                "unsupported config file format: '{other}'"
            ))),
        }
    }

    /// Load configuration from a TOML string.
    pub fn from_toml(toml_str: &str) -> Result<Self> {
        let config: Config =
            toml::from_str(toml_str).map_err(|e| BitdexError::Config(format!("TOML parse error: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let config: Config =
            serde_yaml::from_str(yaml).map_err(|e| BitdexError::Config(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        let config: Config =
            serde_json::from_str(json).map_err(|e| BitdexError::Config(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.max_page_size == 0 {
            return Err(BitdexError::Config(
                "max_page_size must be > 0".to_string(),
            ));
        }

        // Validate cache settings
        if self.cache.decay_rate <= 0.0 || self.cache.decay_rate > 1.0 {
            return Err(BitdexError::Config(
                "cache.decay_rate must be in (0.0, 1.0]".to_string(),
            ));
        }

        // Validate WAL flush strategy
        match self.wal_flush_strategy.as_str() {
            "fsync" | "async" => {}
            other => {
                return Err(BitdexError::Config(format!(
                    "wal_flush_strategy must be 'fsync' or 'async', got '{other}'"
                )));
            }
        }

        // Check for duplicate filter field names
        let mut filter_names = std::collections::HashSet::new();
        for f in &self.filter_fields {
            if f.name.is_empty() {
                return Err(BitdexError::Config(
                    "filter field name must not be empty".to_string(),
                ));
            }
            if !filter_names.insert(&f.name) {
                return Err(BitdexError::Config(format!(
                    "duplicate filter field: {}",
                    f.name
                )));
            }
        }

        // Check for duplicate sort field names and validate bits
        let mut sort_names = std::collections::HashSet::new();
        for s in &self.sort_fields {
            if s.name.is_empty() {
                return Err(BitdexError::Config(
                    "sort field name must not be empty".to_string(),
                ));
            }
            if !sort_names.insert(&s.name) {
                return Err(BitdexError::Config(format!(
                    "duplicate sort field: {}",
                    s.name
                )));
            }
            if s.bits == 0 || s.bits > 64 {
                return Err(BitdexError::Config(format!(
                    "sort field '{}': bits must be 1-64, got {}",
                    s.name, s.bits
                )));
            }
        }

        Ok(())
    }
}

/// Trie cache configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum number of cached entries.
    #[serde(default = "default_cache_max_entries")]
    pub max_entries: usize,
    /// Exponential decay rate for hit stats (0.0, 1.0].
    #[serde(default = "default_cache_decay_rate")]
    pub decay_rate: f64,
}

fn default_cache_max_entries() -> usize {
    10_000
}
fn default_cache_decay_rate() -> f64 {
    0.95
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: default_cache_max_entries(),
            decay_rate: default_cache_decay_rate(),
        }
    }
}

/// Configuration for a single filter field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterFieldConfig {
    pub name: String,
    pub field_type: FilterFieldType,
}

/// Configuration for a single sort field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortFieldConfig {
    pub name: String,
    /// Source type (e.g., "uint32", "int64").
    #[serde(default = "default_source_type")]
    pub source_type: String,
    /// Encoding: "linear" or "log" (log is future work).
    #[serde(default = "default_encoding")]
    pub encoding: String,
    /// Number of bitmap layers. Defaults to 32 for uint32.
    #[serde(default = "default_bits")]
    pub bits: u8,
}

fn default_source_type() -> String {
    "uint32".to_string()
}
fn default_encoding() -> String {
    "linear".to_string()
}
fn default_bits() -> u8 {
    32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.max_page_size, 100);
        assert_eq!(config.cache.max_entries, 10_000);
        assert_eq!(config.cache.decay_rate, 0.95);
        assert_eq!(config.autovac_interval_secs, 3600);
        assert_eq!(config.snapshot_interval_secs, 300);
        assert_eq!(config.wal_flush_strategy, "fsync");
        assert_eq!(config.prometheus_port, 9090);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_toml_parsing() {
        let toml_str = r#"
max_page_size = 50
autovac_interval_secs = 7200
snapshot_interval_secs = 600
wal_flush_strategy = "async"
prometheus_port = 9191

[cache]
max_entries = 5000
decay_rate = 0.9

[[filter_fields]]
name = "nsfwLevel"
field_type = "single_value"

[[filter_fields]]
name = "tagIds"
field_type = "multi_value"

[[filter_fields]]
name = "onSite"
field_type = "boolean"

[[sort_fields]]
name = "reactionCount"
source_type = "uint32"
encoding = "linear"
bits = 32

[[sort_fields]]
name = "sortAt"
source_type = "uint32"
bits = 32
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert_eq!(config.max_page_size, 50);
        assert_eq!(config.cache.max_entries, 5000);
        assert_eq!(config.cache.decay_rate, 0.9);
        assert_eq!(config.autovac_interval_secs, 7200);
        assert_eq!(config.snapshot_interval_secs, 600);
        assert_eq!(config.wal_flush_strategy, "async");
        assert_eq!(config.prometheus_port, 9191);
        assert_eq!(config.filter_fields.len(), 3);
        assert_eq!(config.sort_fields.len(), 2);
        assert_eq!(config.filter_fields[0].name, "nsfwLevel");
        assert_eq!(config.filter_fields[0].field_type, FilterFieldType::SingleValue);
        assert_eq!(config.filter_fields[1].field_type, FilterFieldType::MultiValue);
        assert_eq!(config.filter_fields[2].field_type, FilterFieldType::Boolean);
        assert_eq!(config.sort_fields[0].bits, 32);
    }

    #[test]
    fn test_yaml_parsing() {
        let yaml = r#"
max_page_size: 50
filter_fields:
  - name: nsfwLevel
    field_type: single_value
  - name: tagIds
    field_type: multi_value
  - name: onSite
    field_type: boolean
sort_fields:
  - name: reactionCount
    source_type: uint32
    encoding: linear
    bits: 32
  - name: sortAt
    source_type: uint32
    bits: 32
"#;
        let config = Config::from_yaml(yaml).unwrap();
        assert_eq!(config.max_page_size, 50);
        assert_eq!(config.filter_fields.len(), 3);
        assert_eq!(config.sort_fields.len(), 2);
        assert_eq!(config.filter_fields[0].name, "nsfwLevel");
        assert_eq!(
            config.filter_fields[0].field_type,
            FilterFieldType::SingleValue
        );
        assert_eq!(config.filter_fields[1].field_type, FilterFieldType::MultiValue);
        assert_eq!(config.filter_fields[2].field_type, FilterFieldType::Boolean);
        assert_eq!(config.sort_fields[0].bits, 32);
    }

    #[test]
    fn test_json_parsing() {
        let json = r#"{
            "max_page_size": 200,
            "filter_fields": [
                {"name": "userId", "field_type": "single_value"}
            ],
            "sort_fields": [
                {"name": "id", "source_type": "uint32", "bits": 32}
            ]
        }"#;
        let config = Config::from_json(json).unwrap();
        assert_eq!(config.max_page_size, 200);
        assert_eq!(config.filter_fields.len(), 1);
    }

    #[test]
    fn test_from_file_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
max_page_size = 42

[cache]
max_entries = 999
"#,
        )
        .unwrap();
        let config = Config::from_file(&path).unwrap();
        assert_eq!(config.max_page_size, 42);
        assert_eq!(config.cache.max_entries, 999);
    }

    #[test]
    fn test_from_file_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            r#"
max_page_size: 77
cache:
  decay_rate: 0.8
"#,
        )
        .unwrap();
        let config = Config::from_file(&path).unwrap();
        assert_eq!(config.max_page_size, 77);
        assert_eq!(config.cache.decay_rate, 0.8);
    }

    #[test]
    fn test_from_file_unsupported_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.xml");
        std::fs::write(&path, "<config/>").unwrap();
        let err = Config::from_file(&path);
        assert!(err.is_err());
    }

    #[test]
    fn test_from_file_not_found() {
        let path = PathBuf::from("/nonexistent/config.toml");
        assert!(Config::from_file(&path).is_err());
    }

    #[test]
    fn test_validation_rejects_zero_page_size() {
        let config = Config {
            max_page_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_invalid_decay_rate_zero() {
        let mut config = Config::default();
        config.cache.decay_rate = 0.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_invalid_decay_rate_over_one() {
        let mut config = Config::default();
        config.cache.decay_rate = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_invalid_wal_strategy() {
        let config = Config {
            wal_flush_strategy: "never".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_accepts_valid_wal_strategies() {
        for strategy in &["fsync", "async"] {
            let config = Config {
                wal_flush_strategy: strategy.to_string(),
                ..Default::default()
            };
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn test_validation_rejects_duplicate_filter_fields() {
        let config = Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "status".to_string(),
                    field_type: FilterFieldType::SingleValue,
                },
                FilterFieldConfig {
                    name: "status".to_string(),
                    field_type: FilterFieldType::SingleValue,
                },
            ],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_duplicate_sort_fields() {
        let config = Config {
            sort_fields: vec![
                SortFieldConfig {
                    name: "x".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                },
                SortFieldConfig {
                    name: "x".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                },
            ],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_empty_field_names() {
        let config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "".to_string(),
                field_type: FilterFieldType::SingleValue,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_invalid_bits() {
        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "test".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 0,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "test".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 65,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_civitai_config_toml() {
        let toml_str = r#"
max_page_size = 100
autovac_interval_secs = 3600
snapshot_interval_secs = 300
wal_flush_strategy = "fsync"
prometheus_port = 9090

[cache]
max_entries = 10000
decay_rate = 0.95

[[filter_fields]]
name = "nsfwLevel"
field_type = "single_value"

[[filter_fields]]
name = "tagIds"
field_type = "multi_value"

[[filter_fields]]
name = "userId"
field_type = "single_value"

[[filter_fields]]
name = "modelVersionIds"
field_type = "multi_value"

[[filter_fields]]
name = "onSite"
field_type = "boolean"

[[filter_fields]]
name = "hasMeta"
field_type = "boolean"

[[filter_fields]]
name = "type"
field_type = "single_value"

[[sort_fields]]
name = "reactionCount"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "sortAt"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "commentCount"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "collectedCount"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "id"
source_type = "uint32"
bits = 32
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert_eq!(config.filter_fields.len(), 7);
        assert_eq!(config.sort_fields.len(), 5);
        assert_eq!(config.cache.max_entries, 10_000);
        assert_eq!(config.cache.decay_rate, 0.95);
    }

    #[test]
    fn test_civitai_config_yaml() {
        let yaml = r#"
max_page_size: 100
autovac_interval_secs: 3600
snapshot_interval_secs: 300
prometheus_port: 9090

filter_fields:
  - name: nsfwLevel
    field_type: single_value
  - name: tagIds
    field_type: multi_value
  - name: userId
    field_type: single_value
  - name: modelVersionIds
    field_type: multi_value
  - name: onSite
    field_type: boolean
  - name: hasMeta
    field_type: boolean
  - name: type
    field_type: single_value

sort_fields:
  - name: reactionCount
    source_type: uint32
    bits: 32
  - name: sortAt
    source_type: uint32
    bits: 32
  - name: commentCount
    source_type: uint32
    bits: 32
  - name: collectedCount
    source_type: uint32
    bits: 32
  - name: id
    source_type: uint32
    bits: 32
"#;
        let config = Config::from_yaml(yaml).unwrap();
        assert_eq!(config.filter_fields.len(), 7);
        assert_eq!(config.sort_fields.len(), 5);
    }

    #[test]
    fn test_invalid_toml() {
        assert!(Config::from_toml("{{{{not valid").is_err());
    }

    #[test]
    fn test_invalid_yaml() {
        assert!(Config::from_yaml(":\n  :\n    :invalid:").is_err());
    }

    #[test]
    fn test_serde_roundtrip_toml() {
        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "score".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
            }],
            filter_fields: vec![FilterFieldConfig {
                name: "status".into(),
                field_type: FilterFieldType::SingleValue,
            }],
            ..Config::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let roundtrip = Config::from_toml(&toml_str).unwrap();
        assert_eq!(roundtrip.sort_fields.len(), 1);
        assert_eq!(roundtrip.sort_fields[0].name, "score");
        assert_eq!(roundtrip.filter_fields[0].field_type, FilterFieldType::SingleValue);
    }
}
