use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::{
    env, fs,
    path::{Path, PathBuf},
};
use thiserror::Error;
use url::Url;

const MAX_DURATION_SECONDS: u64 = 31_536_000;

#[derive(Debug, Parser)]
#[command(version, about = "LastBuy migration keeper")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(about = "Run the migration keeper")]
    Run(Options),
    #[command(about = "Check RPC, program, and gRPC connectivity")]
    Check(Options),
    #[command(about = "Show realized profit and unresolved attempts")]
    Profit(Options),
    #[command(about = "Permanently delete the active ledger and its accounting history")]
    ResetLedger(LocalOptions),
    #[command(about = "Create default configuration files")]
    Init,
}
#[derive(Debug, clap::Args)]
pub struct Options {
    #[arg(long, default_value = "keeper.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub log: Option<String>,
}
#[derive(Debug, clap::Args)]
pub struct LocalOptions {
    #[arg(long, default_value = "keeper.toml")]
    pub config: PathBuf,
}
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read configuration file: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid configuration: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid configuration: {0} is not UTF-8")]
    EnvironmentNotUtf8(&'static str),
    #[error("invalid configuration: invalid {0}")]
    InvalidEnvironmentNumber(&'static str),
    #[error(
        "invalid configuration: {field} is required; configure keeper.secrets.toml or {environment}"
    )]
    MissingServiceUrl {
        field: &'static str,
        environment: &'static str,
    },
    #[error("invalid configuration: {0} must be a valid HTTP(S) URL")]
    InvalidServiceUrl(&'static str),
    #[error("invalid configuration: priority fee exceeds configured cap")]
    PriorityFeeExceedsCap,
    #[error("invalid configuration: gpa_interval_seconds must be between 1 and 31536000")]
    InvalidGpaInterval,
    #[error("invalid configuration: max_curve_failures must be positive")]
    InvalidMaxCurveFailures,
    #[error("invalid configuration: log_format must be text or json")]
    InvalidLogFormat,
    #[error("invalid configuration: HOME is required for ~ expansion")]
    HomeRequired,
    #[error("cannot read advanced configuration file {path}: {source}")]
    AdvancedRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid advanced configuration file {path} at line {line}; check its values and keys")]
    AdvancedToml { path: PathBuf, line: usize },
    #[error(
        "invalid advanced configuration at {path}: {field} must be positive and no more than one year"
    )]
    InvalidAdvancedDuration { path: PathBuf, field: &'static str },
    #[error(
        "invalid advanced configuration at {path}: gpa_request_spacing_ms must be between 0 and {max} milliseconds"
    )]
    InvalidAdvancedSpacing { path: PathBuf, max: u64 },
    #[error(
        "invalid advanced configuration at {path}: scheduler_retry_initial_seconds must not exceed scheduler_retry_max_seconds"
    )]
    InvalidRetryRange { path: PathBuf },
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
    pub advanced: AdvancedConfig,
    #[serde(skip)]
    pub grpc_x_token: Option<String>,
    #[serde(skip)]
    pub secrets_file: PathBuf,
}
#[derive(Clone, Debug)]
pub struct AdvancedConfig {
    pub rpc_timeout_seconds: u64,
    pub settlement_timeout_seconds: u64,
    pub pending_recovery_poll_seconds: u64,
    pub gpa_request_spacing_ms: u64,
    pub grpc_slot_idle_timeout_seconds: u64,
    pub scheduler_retry_initial_seconds: u64,
    pub scheduler_retry_max_seconds: u64,
}
impl Default for AdvancedConfig {
    fn default() -> Self {
        Self {
            rpc_timeout_seconds: 900,
            settlement_timeout_seconds: 150,
            pending_recovery_poll_seconds: 10,
            gpa_request_spacing_ms: 100,
            grpc_slot_idle_timeout_seconds: 30,
            scheduler_retry_initial_seconds: 2,
            scheduler_retry_max_seconds: 60,
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AdvancedOverrides {
    rpc_timeout_seconds: Option<u64>,
    settlement_timeout_seconds: Option<u64>,
    pending_recovery_poll_seconds: Option<u64>,
    gpa_request_spacing_ms: Option<u64>,
    grpc_slot_idle_timeout_seconds: Option<u64>,
    scheduler_retry_initial_seconds: Option<u64>,
    scheduler_retry_max_seconds: Option<u64>,
    priority_fee_lamports: Option<u64>,
    max_priority_fee_lamports: Option<u64>,
    min_estimated_profit_lamports: Option<i64>,
    gpa_interval_seconds: Option<u64>,
    max_curve_failures: Option<u8>,
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
            .field("advanced", &self.advanced)
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
            advanced: AdvancedConfig::default(),
            grpc_x_token: None,
            secrets_file: "keeper.secrets.toml".into(),
        }
    }
}
impl Config {
    pub fn load(path: &Path, log_override: Option<&str>) -> Result<Self, ConfigError> {
        Self::load_for(path, log_override, false)
    }
    /// Load local settings without reading secrets or requiring service configuration.
    pub fn load_local(path: &Path, log_override: Option<&str>) -> Result<Self, ConfigError> {
        Self::load_for(path, log_override, true)
    }
    pub fn load_profit(path: &Path, log_override: Option<&str>) -> Result<Self, ConfigError> {
        Self::load_local(path, log_override)
    }
    fn load_for(
        path: &Path,
        log_override: Option<&str>,
        profit: bool,
    ) -> Result<Self, ConfigError> {
        let mut config: Self = toml::from_str(&fs::read_to_string(path)?)?;
        config.secrets_file = path.with_file_name("keeper.secrets.toml");
        let advanced_path = path.with_file_name("keeper.advanced.toml");
        if !profit {
            load_advanced(&advanced_path, &mut config)?;
        }
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
                        .map_err(|_| ConfigError::EnvironmentNotUtf8($name))?
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
                        .ok_or(ConfigError::InvalidEnvironmentNumber($name))?;
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
                    .map_err(|_| ConfigError::EnvironmentNotUtf8("KEEPER_RPC_URL"))?;
            }
            if let Some(value) = env::var_os("KEEPER_GRPC_URL") {
                config.grpc_url = value
                    .into_string()
                    .map_err(|_| ConfigError::EnvironmentNotUtf8("KEEPER_GRPC_URL"))?;
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
        for (name, environment, value) in [
            ("rpc_url", "KEEPER_RPC_URL", &self.rpc_url),
            ("grpc_url", "KEEPER_GRPC_URL", &self.grpc_url),
        ] {
            if value.is_empty() {
                return Err(ConfigError::MissingServiceUrl {
                    field: name,
                    environment,
                });
            }
            let value = value.as_str();
            let url = Url::parse(value).map_err(|_| ConfigError::InvalidServiceUrl(name))?;
            if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
                return Err(ConfigError::InvalidServiceUrl(name));
            }
        }
        if self.priority_fee_lamports > self.max_priority_fee_lamports {
            return Err(ConfigError::PriorityFeeExceedsCap);
        }
        if !(1..=MAX_DURATION_SECONDS).contains(&self.gpa_interval_seconds) {
            return Err(ConfigError::InvalidGpaInterval);
        }
        if self.max_curve_failures == 0 {
            return Err(ConfigError::InvalidMaxCurveFailures);
        }
        validate_advanced(
            &self,
            &self.secrets_file.with_file_name("keeper.advanced.toml"),
        )?;
        Ok(())
    }
    fn validate_local(&self) -> Result<(), ConfigError> {
        if !matches!(self.log_format.as_str(), "text" | "json") {
            return Err(ConfigError::InvalidLogFormat);
        }
        Ok(())
    }
}
fn load_advanced(path: &Path, config: &mut Config) -> Result<(), ConfigError> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ConfigError::AdvancedRead {
                path: path.to_owned(),
                source,
            });
        }
    };
    let overrides: AdvancedOverrides =
        toml::from_str(&contents).map_err(|error| ConfigError::AdvancedToml {
            path: path.to_owned(),
            line: error
                .span()
                .map(|span| {
                    contents
                        .as_bytes()
                        .get(..span.start.min(contents.len()))
                        .unwrap_or_default()
                        .iter()
                        .filter(|b| **b == b'\n')
                        .count()
                        + 1
                })
                .unwrap_or(1),
        })?;
    macro_rules! apply_overrides {
        ($destination:ident, $($field:ident),+ $(,)?) => {
            $(
                $destination.$field = overrides.$field.unwrap_or($destination.$field);
            )+
        };
    }
    let advanced = &mut config.advanced;
    apply_overrides!(
        advanced,
        rpc_timeout_seconds,
        settlement_timeout_seconds,
        pending_recovery_poll_seconds,
        gpa_request_spacing_ms,
        grpc_slot_idle_timeout_seconds,
        scheduler_retry_initial_seconds,
        scheduler_retry_max_seconds,
    );
    apply_overrides!(
        config,
        priority_fee_lamports,
        max_priority_fee_lamports,
        min_estimated_profit_lamports,
        gpa_interval_seconds,
        max_curve_failures,
    );
    Ok(())
}

fn validate_advanced(config: &Config, path: &Path) -> Result<(), ConfigError> {
    let values = [
        ("rpc_timeout_seconds", config.advanced.rpc_timeout_seconds),
        (
            "settlement_timeout_seconds",
            config.advanced.settlement_timeout_seconds,
        ),
        (
            "pending_recovery_poll_seconds",
            config.advanced.pending_recovery_poll_seconds,
        ),
        (
            "grpc_slot_idle_timeout_seconds",
            config.advanced.grpc_slot_idle_timeout_seconds,
        ),
        (
            "scheduler_retry_initial_seconds",
            config.advanced.scheduler_retry_initial_seconds,
        ),
        (
            "scheduler_retry_max_seconds",
            config.advanced.scheduler_retry_max_seconds,
        ),
    ];
    for (field, value) in values {
        if value == 0 || value > MAX_DURATION_SECONDS {
            return Err(ConfigError::InvalidAdvancedDuration {
                path: path.to_owned(),
                field,
            });
        }
    }
    if config.advanced.gpa_request_spacing_ms > MAX_DURATION_SECONDS * 1000 {
        return Err(ConfigError::InvalidAdvancedSpacing {
            path: path.to_owned(),
            max: MAX_DURATION_SECONDS * 1000,
        });
    }
    if config.advanced.scheduler_retry_initial_seconds > config.advanced.scheduler_retry_max_seconds
    {
        return Err(ConfigError::InvalidRetryRange {
            path: path.to_owned(),
        });
    }
    Ok(())
}
fn expand_home(path: &Path) -> Result<PathBuf, ConfigError> {
    if path == Path::new("~") {
        return env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or(ConfigError::HomeRequired);
    }
    if let Ok(rest) = path.strip_prefix("~") {
        return env::var_os("HOME")
            .map(|home| PathBuf::from(home).join(rest))
            .ok_or(ConfigError::HomeRequired);
    }
    Ok(path.to_owned())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_fee_and_redaction() {
        let mut c = Config {
            rpc_url: "https://rpc-user-unique:rpc-password-unique@example.com".into(),
            grpc_url: "https://grpc.example.com?api_key=grpc-key-unique".into(),
            ..Default::default()
        };
        c.grpc_x_token = Some("grpc-token-unique".into());
        let debug = format!("{c:?}");
        assert!(debug.contains("[secret]"));
        assert!(!debug.contains("rpc-user-unique"));
        assert!(!debug.contains("rpc-password-unique"));
        assert!(!debug.contains("grpc-key-unique"));
        assert!(!debug.contains("grpc-token-unique"));
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
