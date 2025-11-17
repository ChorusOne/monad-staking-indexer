use config::{Config as ConfigBuilder, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub rpc_url: String,
    pub database_url: String,
    pub backfill_chunk_size: u64,
    pub gap_check_interval_secs: u64,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub bind_address: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config_path = "config.toml";

        let mut builder = ConfigBuilder::builder()
            .set_default("backfill_chunk_size", 100)?
            .set_default("gap_check_interval_secs", 300)?
            .set_default("metrics.bind_address", "127.0.0.1")?
            .set_default("metrics.port", 9090)?
            .set_default("logging.level", "info")?;

        if Path::new(config_path).exists() {
            builder = builder.add_source(File::with_name(config_path));
        }

        builder = builder.add_source(Environment::default().separator("__").prefix("INDEXER"));

        let config = builder.build()?;
        config.try_deserialize()
    }

    pub fn parse_log_level(&self) -> log::LevelFilter {
        match self.logging.level.to_lowercase().as_str() {
            "error" => log::LevelFilter::Error,
            "warn" => log::LevelFilter::Warn,
            "info" => log::LevelFilter::Info,
            "debug" => log::LevelFilter::Debug,
            "trace" => log::LevelFilter::Trace,
            _ => {
                panic!(
                    "Invalid log level '{}', try error, warn, info, debug, trace",
                    self.logging.level
                );
            }
        }
    }

    pub fn metrics_bind_addr(&self) -> String {
        format!("{}:{}", self.metrics.bind_address, self.metrics.port)
    }
}
