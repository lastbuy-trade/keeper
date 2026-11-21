use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::{
    env, fs,
    path::{Path, PathBuf},
};
use thiserror::Error;
use url::Url;

#[derive(Debug, Parser)]
#[command(version, about = "LastBuy migration keeper")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Debug, Subcommand)]
pub enum Command {
    Run(Options),
    Check(Options),
    Profit(Options),
    Init,
}
#[derive(Debug, clap::Args)]
pub struct Options {
    #[arg(long, default_value = "keeper.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub log: Option<String>,
}
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read configuration file: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid configuration: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rpc_url: String,
    pub grpc_url: String,
    pub keypair_path: PathBuf,
    pub state_dir: PathBuf,
    pub log: String,
    pub log_format: String,
    pub priority_fee_lamports: u64,
    pub max_priority_fee_lamports: u64,
    pub min_estimated_profit_lamports: i64,
    pub gpa_interval_seconds: u64,
    pub max_curve_failures: u8,
    #[serde(skip)]
    pub grpc_x_token: Option<String>,
    #[serde(skip)]
    pub secrets_file: PathBuf,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rpc_url", &"[secret]")
            .field("grpc_url", &"[secret]")
            .field("keypair_path", &self.keypair_path)
            .field("state_dir", &self.state_dir)
            .field("log", &self.log)
            .field("log_format", &self.log_format)
            .field("priority_fee_lamports", &self.priority_fee_lamports)
            .field("max_priority_fee_lamports", &self.max_priority_fee_lamports)
            .field(
                "min_estimated_profit_lamports",
                &self.min_estimated_profit_lamports,
            )
            .field("gpa_interval_seconds", &self.gpa_interval_seconds)
            .field("max_curve_failures", &self.max_curve_failures)
            .field(
                "grpc_x_token",
                &self.grpc_x_token.as_ref().map(|_| "[secret]"),
            )
            .finish()
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            rpc_url: String::new(),
            grpc_url: String::new(),
            keypair_path: "~/.config/solana/id.json".into(),
            state_dir: "~/.local/state/lastbuy-keeper".into(),
            log: "info".into(),
            log_format: "text".into(),
            priority_fee_lamports: 1000,
            max_priority_fee_lamports: 10000,
            min_estimated_profit_lamports: 0,
            gpa_interval_seconds: 300,
            max_curve_failures: 3,
            grpc_x_token: None,
            secrets_file: "keeper.secrets.toml".into(),
        }
    }
}
impl Config {
    pub fn load(path: &Path, log_override: Option<&str>) -> Result<Self, ConfigError> {
        Self::load_for(path, log_override, false)
    }
    /// Load only the local settings needed to read the ledger.
    pub fn load_profit(path: &Path, log_override: Option<&str>) -> Result<Self, ConfigError> {
        Self::load_for(path, log_override, true)
    }
    fn load_for(
        path: &Path,
        log_override: Option<&str>,
        profit: bool,
    ) -> Result<Self, ConfigError> {
        let mut config: Self = toml::from_str(&fs::read_to_string(path)?)?;
        config.secrets_file = path.with_file_name("keeper.secrets.toml");
        if !profit && config.secrets_file.exists() {
            #[derive(Deserialize, Default)]
            #[serde(default, deny_unknown_fields)]
            struct Secrets {
                rpc_url: Option<String>,
                grpc_url: Option<String>,
                grpc_x_token: Option<String>,
            }
            let secrets: Secrets = toml::from_str(&fs::read_to_string(&config.secrets_file)?)?;
            config.rpc_url = secrets.rpc_url.unwrap_or_default();
            config.grpc_url = secrets.grpc_url.unwrap_or_default();
            config.grpc_x_token = secrets.grpc_x_token;
        }
        macro_rules! string {
            ($name:literal, $field:ident) => {
                if let Some(value) = env::var_os($name) {
                    config.$field = value
                        .into_string()
                        .map_err(|_| ConfigError::Invalid(format!("{} is not UTF-8", $name)))?
                        .into();
                }
            };
        }
        macro_rules! number {
            ($name:literal, $field:ident) => {
                if let Some(value) = env::var_os($name) {
                    config.$field = value
                        .to_str()
                        .and_then(|v| v.parse().ok())
                        .ok_or_else(|| ConfigError::Invalid(format!("invalid {}", $name)))?;
                }
            };
        }
        string!("KEEPER_STATE_DIR", state_dir);
        string!("KEEPER_LOG", log);
        string!("KEEPER_LOG_FORMAT", log_format);
        if !profit {
            if let Some(value) = env::var_os("KEEPER_RPC_URL") {
                config.rpc_url = value
                    .into_string()
                    .map_err(|_| ConfigError::Invalid("KEEPER_RPC_URL is not UTF-8".into()))?;
            }
            if let Some(value) = env::var_os("KEEPER_GRPC_URL") {
                config.grpc_url = value
                    .into_string()
                    .map_err(|_| ConfigError::Invalid("KEEPER_GRPC_URL is not UTF-8".into()))?;
            }
            string!("KEEPER_KEYPAIR_PATH", keypair_path);
            number!("KEEPER_PRIORITY_FEE_LAMPORTS", priority_fee_lamports);
            number!(
                "KEEPER_MAX_PRIORITY_FEE_LAMPORTS",
                max_priority_fee_lamports
            );
            number!(
                "KEEPER_MIN_ESTIMATED_PROFIT_LAMPORTS",
                min_estimated_profit_lamports
            );
            number!("KEEPER_GPA_INTERVAL_SECONDS", gpa_interval_seconds);
            number!("KEEPER_MAX_CURVE_FAILURES", max_curve_failures);
            if let Ok(token) = env::var("KEEPER_GRPC_X_TOKEN") {
                config.grpc_x_token = Some(token);
            }
            config.keypair_path = expand_home(&config.keypair_path)?;
        }
        if let Some(log) = log_override {
            config.log = log.to_owned();
        }
        config.state_dir = expand_home(&config.state_dir)?;
        if profit {
            config.validate_local()?;
        } else {
            config.validate()?;
        }
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_local()?;
        for (name, value) in [("rpc_url", &self.rpc_url), ("grpc_url", &self.grpc_url)] {
            if value.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "{name} is required; configure keeper.secrets.toml or KEEPER_{}",
                    name.to_ascii_uppercase()
                )));
            }
            let value = value.as_str();
            let url = Url::parse(value)
                .map_err(|_| ConfigError::Invalid(format!("{name} must be a valid HTTP(S) URL")))?;
            if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
                return Err(ConfigError::Invalid(format!(
                    "{name} must be a valid HTTP(S) URL"
                )));
            }
        }
        if self.priority_fee_lamports > self.max_priority_fee_lamports {
            return Err(ConfigError::Invalid(
                "priority fee exceeds configured cap".into(),
            ));
        }
        if self.gpa_interval_seconds == 0 {
            return Err(ConfigError::Invalid(
                "gpa_interval_seconds must be positive".into(),
            ));
        }
        if self.max_curve_failures == 0 {
            return Err(ConfigError::Invalid(
                "max_curve_failures must be positive".into(),
            ));
        }
        Ok(())
    }
    fn validate_local(&self) -> Result<(), ConfigError> {
        if !matches!(self.log_format.as_str(), "text" | "json") {
            return Err(ConfigError::Invalid(
                "log_format must be text or json".into(),
            ));
        }
        Ok(())
    }
}
fn expand_home(path: &Path) -> Result<PathBuf, ConfigError> {
    if path == Path::new("~") {
        return env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| ConfigError::Invalid("HOME is required for ~ expansion".into()));
    }
    if let Ok(rest) = path.strip_prefix("~") {
        return env::var_os("HOME")
            .map(|home| PathBuf::from(home).join(rest))
            .ok_or_else(|| ConfigError::Invalid("HOME is required for ~ expansion".into()));
    }
    Ok(path.to_owned())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_fee_and_redaction() {
        let mut c = Config {
            rpc_url: "https://user:secret@example.com".into(),
            grpc_url: "https://example.com".into(),
            ..Default::default()
        };
        c.grpc_x_token = Some("token-secret".into());
        let debug = format!("{c:?}");
        assert!(!debug.contains("secret"));
        assert!(c.validate().is_ok());
        c.priority_fee_lamports = 10001;
        assert!(c.validate().is_err());
    }
}

#[cfg(test)]
mod profit_tests {
    use super::*;
    #[test]
    fn profit_loads_local_only_config_but_run_requires_endpoints() {
        let path = env::temp_dir().join(format!(
            "keeper-profit-config-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, "state_dir = '/tmp/local-state'\n").unwrap();
        let config = Config::load_profit(&path, None).unwrap();
        assert_eq!(config.state_dir, Path::new("/tmp/local-state"));
        assert!(Config::load(&path, None).is_err());
        fs::remove_file(path).unwrap();
    }
}
