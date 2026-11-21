use clap::Parser;
use keeper::{
    config::{Cli, Command, Config},
    ledger,
};
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let (action, options) = match cli.command {
        Command::Init => return init_config(),
        Command::ResetLedger(options) => {
            let config = Config::load_local(&options.config, None)?;
            let removed = ledger::reset(&config.state_dir)?;
            if removed {
                println!(
                    "Removed active ledger at {}. The next run will initialize a fresh ledger.",
                    config.state_dir.join("ledger.jsonl").display()
                );
            } else {
                println!(
                    "No active ledger found at {}; nothing was removed.",
                    config.state_dir.join("ledger.jsonl").display()
                );
            }
            return Ok(());
        }
        Command::Run(options) => ("run", options),
        Command::Check(options) => ("check", options),
        Command::Profit(options) => ("profit", options),
    };
    let config = if action == "profit" {
        Config::load_profit(&options.config, options.log.as_deref())?
    } else {
        Config::load(&options.config, options.log.as_deref())?
    };
    if action == "profit" {
        let report = ledger::read_summary(&config.state_dir.join("ledger.jsonl"))?;
        println!(
            "Successful migrations: {}",
            report.totals.successful_migrations
        );
        println!(
            "Successful migration net (lamports): {}",
            report.totals.successful_net_lamports
        );
        println!(
            "Failed attempt net (lamports): {}",
            report.totals.failed_net_lamports
        );
        println!(
            "Total realized net: {} SOL ({} lamports)",
            format_sol(report.totals.realized_net_lamports),
            report.totals.realized_net_lamports
        );
        println!(
            "Unresolved attempts: {}",
            report.totals.unresolved_settlements
        );
        return Ok(());
    }
    if action == "check" {
        let payer = solana_keypair::read_keypair_file(&config.keypair_path)
            .map_err(|_| CheckError::Keypair)?;
        let rpc =
            solana_rpc_client::nonblocking::rpc_client::RpcClient::new(config.rpc_url.clone());
        let genesis =
            tokio::time::timeout(std::time::Duration::from_secs(10), rpc.get_genesis_hash())
                .await
                .map_err(|_| CheckError::Rpc)?
                .map_err(|_| CheckError::Rpc)?;
        let program = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            rpc.get_account(&lastbuy_sdk::PROGRAM_ID),
        )
        .await
        .map_err(|_| CheckError::Program)?
        .map_err(|_| CheckError::Program)?;
        if !program.executable {
            return Err(CheckError::Program.into());
        }
        let global = lastbuy_sdk::pda::derive_global_config().0;
        let account =
            tokio::time::timeout(std::time::Duration::from_secs(10), rpc.get_account(&global))
                .await
                .map_err(|_| CheckError::Global)?
                .map_err(|_| CheckError::Global)?;
        if account.owner != lastbuy_sdk::PROGRAM_ID {
            return Err(CheckError::Global.into());
        }
        let builder =
            yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(config.grpc_url.clone())
                .and_then(|b| b.x_token(config.grpc_x_token.clone()))
                .and_then(|b| {
                    b.tls_config(
                        yellowstone_grpc_client::ClientTlsConfig::new().with_enabled_roots(),
                    )
                })
                .map_err(|_| CheckError::Grpc)?;
        tokio::time::timeout(std::time::Duration::from_secs(10), builder.connect())
            .await
            .map_err(|_| CheckError::Grpc)?
            .map_err(|_| CheckError::Grpc)?;
        use solana_signer::Signer;
        println!("RPC genesis: {genesis}; payer: {}", payer.pubkey());
        println!(
            "RPC program/config and gRPC connection checked; v1 execution and provider history have not been verified"
        );
        return Ok(());
    }
    let mut logger = env_logger::Builder::new();
    logger.parse_filters(&config.log);
    if config.log_format == "json" {
        logger.format(|buf, record| {
            use std::io::Write;
            writeln!(buf, "{}", serde_json::json!({"level": record.level().to_string(), "message": record.args().to_string()}))
        });
    }
    logger.try_init()?;
    keeper::runtime::run(config).await?;
    Ok(())
}

fn init_config() -> Result<(), Box<dyn std::error::Error>> {
    use std::{fs::OpenOptions, io::Write};
    let config = std::path::Path::new("keeper.toml");
    if !config.exists() {
        std::fs::write(config, include_str!("../keeper.example.toml"))?;
    }
    let secrets = std::path::Path::new("keeper.secrets.toml");
    if !secrets.exists() {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(secrets)?;
        file.write_all(include_bytes!("../keeper.secrets.example.toml"))?;
    }
    println!(
        "Created keeper.toml and keeper.secrets.toml (private permissions). Edit keeper.secrets.toml, then run `keeper check`."
    );
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum CheckError {
    #[error("could not read the configured Solana keypair")]
    Keypair,
    #[error("RPC connection or genesis lookup failed")]
    Rpc,
    #[error("LastBuy executable program was not found on RPC")]
    Program,
    #[error("LastBuy global configuration was not found or has the wrong owner")]
    Global,
    #[error("Yellowstone gRPC connection failed")]
    Grpc,
}

fn format_sol(lamports: i128) -> String {
    let amount = lamports.unsigned_abs();
    let sign = if lamports < 0 { "-" } else { "" };
    format!(
        "{sign}{}.{:09}",
        amount / 1_000_000_000,
        amount % 1_000_000_000
    )
}
