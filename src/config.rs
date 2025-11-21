use config::{Config as ConfigBuilder, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;
use tokio::fs;
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub rpc_url: String,
    pub db_host: String,
    pub db_port: u16,
    pub db_name: String,
    #[serde(flatten)]
    pub db_auth: DbAuth,
    pub backfill_chunk_size: u64,
    pub gap_check_interval_secs: u64,
    pub db_batch_size: usize,
    pub db_operation_timeout_secs: u64,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbCredentials {
    user: String,
    password: String,
}

fn default_k8s_mount() -> String {
    "kubernetes".to_string()
}

fn default_jwt_path() -> String {
    "/var/run/secrets/kubernetes.io/serviceaccount/token".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct TokenConfig {
    token_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KubernetesConfig {
    role: String,
    #[serde(default = "default_k8s_mount")]
    mount: String,
    #[serde(default = "default_jwt_path")]
    jwt_path: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum VaultAuthMethod {
    Token { token_config: TokenConfig },
    Kubernetes { kubernetes_config: KubernetesConfig },
}

#[derive(Debug, Deserialize, Clone)]
pub struct VaultConfig {
    address: String,
    db_secret_path: String,
    #[serde(flatten)]
    auth: VaultAuthMethod,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum DbAuth {
    Direct { db_credentials: DbCredentials },
    Vault { vault: VaultConfig },
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
            .set_default("db_batch_size", 10)?
            .set_default("db_operation_timeout_secs", 10)?
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

    pub async fn connection_string(&self) -> Result<String, Box<dyn std::error::Error>> {
        let creds = match &self.db_auth {
            DbAuth::Direct { db_credentials } => db_credentials.clone(),
            DbAuth::Vault { vault } => {
                let token = match &vault.auth {
                    VaultAuthMethod::Token { token_config } => {
                        fs::read_to_string(&token_config.token_path)
                            .await
                            .expect("Can't read plain token file")
                            .trim()
                            .to_string()
                    }
                    VaultAuthMethod::Kubernetes { kubernetes_config } => {
                        let jwt = fs::read_to_string(&kubernetes_config.jwt_path)
                            .await
                            .expect("Can't read k8s jwt file")
                            .trim()
                            .to_string();

                        let client = VaultClient::new(
                            VaultClientSettingsBuilder::default()
                                .address(&vault.address)
                                .build()?,
                        )?;

                        let auth_info = vaultrs::auth::kubernetes::login(
                            &client,
                            &kubernetes_config.mount,
                            &kubernetes_config.role,
                            &jwt,
                        )
                        .await
                        .expect("Can't log in to vault via k8s jwt token");

                        auth_info.client_token
                    }
                };

                let client = VaultClient::new(
                    VaultClientSettingsBuilder::default()
                        .address(&vault.address)
                        .token(token)
                        .build()?,
                )?;

                let mount = "secret";
                let creds: DbCredentials =
                    vaultrs::kv2::read(&client, mount, &vault.db_secret_path).await?;

                creds
            }
        };

        Ok(format!(
            "postgres://{}:{}@{}:{}/{}",
            creds.user, creds.password, self.db_host, self.db_port, self.db_name
        ))
    }
}
