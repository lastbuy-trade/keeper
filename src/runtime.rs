//! Discovery and supervised serial execution runtime.
use crate::{
    chain,
    config::Config,
    coordinator::{Coordinator, StepResult},
    ledger::Ledger,
    monitor::{self, MonitorConfig, MonitorEvent},
    pipeline::ValidatedPolicy,
    reconcile,
    scheduler::{Candidate, EnqueueResult, Scheduler},
    transaction::ResourcePolicy,
};
use lastbuy_sdk::bonding_curve::CurveState;
use solana_pubkey::Pubkey;
use solana_rpc_client::{
    api::{client_error::Error as RpcError, config::CommitmentConfig},
    nonblocking::rpc_client::RpcClient,
};
use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

enum ExecutionCommand {
    Candidate(Candidate),
    Exhausted {
        curve: String,
        failures: u8,
        persisted: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

const CAPACITY: usize = 1024;
// Six HTTP attempts at 30 seconds each plus five provider cooldowns below 120 seconds.
// Keep the outer deadline above the client's complete retry window.
const RPC_TIMEOUT: Duration = Duration::from_secs(900);
// Permit a provider cooldown below 120 seconds without blocking later settlements for 15 minutes.
const SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(150);

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("could not read the configured Solana keypair")]
    Keypair,
    #[error("RPC genesis lookup timed out")]
    GenesisTimeout,
    #[error("RPC genesis lookup failed")]
    GenesisRpc,
    #[error("could not open ledger: {0}")]
    Ledger(#[from] crate::ledger::LedgerError),
    #[error("signal handling failed: {0}")]
    Signal(#[from] std::io::Error),
    #[error("monitor task stopped unexpectedly")]
    MonitorStopped,
    #[error("monitor connection state closed")]
    MonitorConnectionClosed,
    #[error("execution task stopped unexpectedly")]
    ExecutionStopped,
    #[error("execution results closed")]
    ExecutionResultsClosed,
    #[error("RPC worker stopped unexpectedly")]
    RpcWorkerStopped,
}

/// Starts confirmed discovery and a serial execution/recovery lane.
pub async fn run(config: Config) -> Result<(), RuntimeError> {
    let mut terminate = termination_signal()?;
    let rpc = Arc::new(RpcClient::new_with_commitment(
        config.rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));
    use solana_signer::Signer;
    let payer = solana_keypair::read_keypair_file(&config.keypair_path)
        .map_err(|_| RuntimeError::Keypair)?;
    let genesis = tokio::time::timeout(RPC_TIMEOUT, rpc.get_genesis_hash())
        .await
        .map_err(|_| RuntimeError::GenesisTimeout)?
        .map_err(|_| RuntimeError::GenesisRpc)?;
    let mut ledger = Ledger::open(
        &config.state_dir,
        &genesis.to_string(),
        &payer.pubkey().to_string(),
    )?;
    let restored_exhausted = ledger
        .replay
        .exhausted_curves
        .iter()
        .filter_map(|curve| Pubkey::from_str(curve).ok())
        .collect::<Vec<_>>();
    let restored_successful = ledger
        .replay
        .successful_curves()
        .filter_map(|curve| Pubkey::from_str(curve).ok())
        .collect::<Vec<_>>();
    // The probe uses protocol resource ceilings; simulation sizes the final message.
    let policy = ValidatedPolicy {
        resources: ResourcePolicy {
            compute_unit_limit: 1_400_000,
            loaded_accounts_data_size_limit: 64 * 1024 * 1024,
            priority_fee_lamports: config.priority_fee_lamports,
            max_priority_fee_lamports: config.max_priority_fee_lamports,
        },
        min_profit_lamports: i128::from(config.min_estimated_profit_lamports),
        max_compute_units: 1_400_000,
        max_loaded_accounts_data_size: 64 * 1024 * 1024,
        max_total_fee_lamports: u64::MAX,
        min_block_heights_remaining: 1,
    };
    let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
    let (commands, mut command_rx) = mpsc::channel::<ExecutionCommand>(1);
    let (results, mut result_rx) = mpsc::channel(1);
    let (terminal_tx, mut terminal_rx) = mpsc::unbounded_channel::<Pubkey>();
    let execution_rpc = rpc.clone();
    let mut execution = tokio::spawn(async move {
        let mut coordinator = Coordinator::new(
            &execution_rpc,
            &mut ledger,
            &payer,
            Some(policy),
            RPC_TIMEOUT,
        )?;
        let (settlement_tx, mut settlement_rx) = mpsc::channel(1);
        let mut settlement_task: Option<tokio::task::JoinHandle<()>> = None;
        let mut recovery = tokio::time::interval(Duration::from_secs(10));
        recovery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *cancel_rx.borrow() {
                break;
            }
            tokio::select! {
                changed = cancel_rx.changed() => { if changed.is_err() || *cancel_rx.borrow() { break; } }
                _ = recovery.tick() => {
                    if settlement_task.as_ref().is_some_and(|task| task.is_finished())
                        && settlement_rx.is_empty()
                        && let Some(task) = settlement_task.take()
                        && task.await.is_err()
                    {
                        log::warn!("finalized accounting worker stopped unexpectedly");
                    }
                    if settlement_task.is_none()
                        && let Some(job) = coordinator.next_settlement_job()
                    {
                        let rpc = execution_rpc.clone();
                        let tx = settlement_tx.clone();
                        settlement_task = Some(tokio::spawn(async move {
                            let response = crate::submission::fetch_settlement(&rpc, job, SETTLEMENT_TIMEOUT).await;
                            let _ = tx.send(response).await;
                        }));
                    }
                    match coordinator.recover_pending(&mut cancel_rx).await {
                        Ok(Some((signature, _))) => {
                            if let Some(curve) = coordinator.successful_curve_for_signature(&signature) {
                                let _ = terminal_tx.send(curve);
                            }
                        }
                        Ok(None) => {}
                        Err(error) => log::warn!("pending attempt recovery failed: {}", error_class(&error)),
                    }
                }
                Some(response) = settlement_rx.recv() => {
                    if let Some(task) = settlement_task.take() { let _ = task.await; }
                    let signature = response.job.attempt.signature.clone();
                    if let Err(error) = coordinator.apply_settlement(response) {
                        log::warn!("finalized accounting recovery failed: {}", error_class(&error));
                    }
                    if let Some(curve) = coordinator.successful_curve_for_signature(&signature) {
                        let _ = terminal_tx.send(curve);
                    }
                }
                item = command_rx.recv() => {
                    let Some(item) = item else { break };
                    match item {
                        ExecutionCommand::Candidate(item) => {
                            let outcome = coordinator.step(item, &mut cancel_rx).await;
                            tokio::select! {
                                _ = cancel_rx.changed() => break,
                                sent = results.send((item, outcome)) => if sent.is_err() { break; },
                            }
                        }
                        ExecutionCommand::Exhausted { curve, failures, persisted } => {
                            let result = coordinator.mark_curve_exhausted(curve, failures).map_err(|error| error.to_string());
                            let _ = persisted.send(result);
                        }
                    }
                }
            }
        }
        if let Some(task) = settlement_task {
            task.abort();
            let _ = task.await;
        }
        Ok::<(), crate::coordinator::CoordinatorError>(())
    });
    let (tx, mut rx) = mpsc::channel(CAPACITY);
    let (connection, mut connection_rx) = tokio::sync::watch::channel(false);
    let needed = Arc::new(AtomicBool::new(true));
    let mut monitor = tokio::spawn(monitor::run(
        MonitorConfig {
            endpoint: config.grpc_url.clone(),
            token: config.grpc_x_token.clone(),
            global_config: lastbuy_sdk::pda::derive_global_config().0.to_string(),
            idle_timeout: Duration::from_secs(30),
            replay_overlap: 32,
        },
        tx,
        connection,
        needed.clone(),
    ));
    let mut scheduler = Scheduler::new_with_failure_limit(
        CAPACITY,
        Duration::from_secs(2),
        Duration::from_secs(60),
        config.max_curve_failures,
    );
    scheduler.restore_exhausted(restored_exhausted);
    for curve in restored_successful {
        scheduler.finalize_success(curve);
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut next_periodic = Instant::now() + Duration::from_secs(config.gpa_interval_seconds);
    let mut next_scan = Instant::now();
    let mut work: Option<tokio::task::JoinHandle<WorkResult>> = None;
    let mut next_rpc = Instant::now();
    let mut failures = 0u32;
    let mut subscribed = false;
    let mut last_slot = 0;
    let mut monitor_finished = false;
    let mut execution_finished = false;
    let outcome: Result<(), RuntimeError> = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { break signal.map_err(Into::into); }
            _ = termination(&mut terminate) => { break Ok(()); }
            _ = &mut monitor => { monitor_finished = true; break Err(RuntimeError::MonitorStopped); }
            _ = &mut execution => { execution_finished = true; break Err(RuntimeError::ExecutionStopped); }
            result = result_rx.recv() => {
                let Some(result) = result else { break Err(RuntimeError::ExecutionResultsClosed); };
                let (item, outcome) = result;
                match outcome {
                    Ok(StepResult::FinalizedSuccess) => {
                        log::info!("curve={} already has finalized successful migration", item.curve);
                        scheduler.finalize_success(item.curve);
                        scheduler.complete(item.curve, Instant::now());
                    }
                    Ok(StepResult::ObservedMigrated { slot }) => {
                        log::info!("curve={} already migrated in confirmed snapshot slot={slot}", item.curve);
                        scheduler.complete(item.curve, Instant::now());
                    }
                    Ok(StepResult::Prepared { result, successful_curve }) => {
                        if let Some(curve) = successful_curve {
                            scheduler.finalize_success(curve);
                        }
                        log::info!("curve={} execution result={result:?}", item.curve);
                        if matches!(result, crate::submission::RecoveryResult::Settled) {
                            scheduler.complete(item.curve, Instant::now());
                        } else {
                            scheduler.defer(item.curve, Instant::now());
                        }
                    }
                    Ok(StepResult::Recovery { result, successful_curve }) => {
                        log::info!("pending attempt recovery result={result:?}");
                        if let Some(curve) = successful_curve {
                            scheduler.finalize_success(curve);
                        }
                        scheduler.defer(item.curve, Instant::now());
                    }
                    Err(error) if should_defer(&error) => {
                        log::warn!("curve={} execution deferred: {}", item.curve, error_class(&error));
                        scheduler.defer(item.curve, Instant::now());
                    }
                    Err(error) => {
                        log::warn!("curve={} execution failed: {}", item.curve, error_class(&error));
                        retry_candidate(&mut scheduler, &commands, item.curve, Instant::now(), config.max_curve_failures).await;
                    }
                }
            }
            Some(curve) = terminal_rx.recv() => {
                scheduler.finalize_success(curve);
            }
            changed = connection_rx.changed() => {
                if changed.is_err() { break Err(RuntimeError::MonitorConnectionClosed); }
                subscribed = *connection_rx.borrow_and_update();
                needed.store(true, Ordering::Release);
            }
            event = rx.recv() => match event {
                Some(MonitorEvent::Slot(slot)) => last_slot = last_slot.max(slot),
                Some(MonitorEvent::Account(update)) => {
                    last_slot = last_slot.max(update.slot);
                    if let Some(account) = update.account
                        && let (Ok(key), Ok(owner)) = (Pubkey::try_from(account.pubkey.as_slice()), Pubkey::try_from(account.owner.as_slice())) {
                            if key != lastbuy_sdk::pda::derive_global_config().0
                                && let Ok(curve) = chain::decode_curve(&key, &owner, &account.data)
                                && chain::eligible(&curve) {
                                    admit_stream(&mut scheduler, Candidate { curve: key, mint: curve.mint, slot: update.slot, write_version: Some(account.write_version) }, work.is_some(), &needed);
                            }
                    }
                }
                None => break Err(RuntimeError::MonitorStopped),
            },
            result = async { match &mut work { Some(task) => Some(task.await), None => std::future::pending().await } } => {
                work = None;
                match result {
                    Some(Ok(WorkResult::Scan(token, Ok(scans)))) => {
                        failures = 0;
                        // Account streams may repeatedly update the global config. Coalesce
                        // those hints so they cannot trigger an RPC scan on every message.
                        next_scan = Instant::now() + Duration::from_secs(config.gpa_interval_seconds);
                        next_rpc = next_scan;
                        let mut candidates: Vec<_> = scans.into_iter().flat_map(|scan| scan.accounts.into_iter().filter_map(move |(key, curve)| candidate(key, &curve, scan.slot))).collect();
                        candidates.sort_by_key(|c| c.curve);
                        scheduler.scan_begin();
                        let mut exhausted = true;
                        for item in candidates {
                            if scheduler.scan_enqueue(item, Instant::now()) == EnqueueResult::Overflow { exhausted = false; break; }
                        }
                        scheduler.scan_finished(token, exhausted);
                        log::info!("confirmed reconciliation completed; candidates={}, complete={exhausted}", scheduler.len());
                    }
                    Some(Ok(WorkResult::Scan(_, Err(error)))) => {
                        failures = failures.saturating_add(1);
                        next_scan = Instant::now() + backoff(failures);
                        next_rpc = next_scan;
                        needed.store(true, Ordering::Release);
                        log::warn!("confirmed reconciliation failed; class={:?}; retrying", error.class());
                    }
                    _ => break Err(RuntimeError::RpcWorkerStopped),
                }
            }
            _ = interval.tick() => { if subscribed && Instant::now() >= next_periodic { needed.store(true, Ordering::Release); next_periodic = Instant::now() + Duration::from_secs(config.gpa_interval_seconds); } }
        }
        if scheduler.reconciliation_needed() && Instant::now() >= next_scan {
            needed.store(true, Ordering::Release);
        }
        // Dispatch queued candidates even while reconciliation is running or backing off.
        if let Some(item) = scheduler.next(Instant::now())
            && commands
                .try_send(ExecutionCommand::Candidate(item))
                .is_err()
        {
            scheduler.defer(item.curve, Instant::now());
        }
        // Scan as soon as there is room, even if new candidates keep arriving.
        if work.is_none()
            && Instant::now() >= next_rpc
            && subscribed
            && Instant::now() >= next_scan
            && scan_has_capacity(scheduler.len())
            && needed.swap(false, Ordering::AcqRel)
        {
            let token = scheduler.reconciliation_token();
            let rpc = rpc.clone();
            let slot = last_slot;
            work = Some(tokio::spawn(async move {
                WorkResult::Scan(token, reconcile::scan(&rpc, Some(slot), RPC_TIMEOUT).await)
            }));
        }
    };
    let _ = cancel_tx.send(true);
    if let Some(task) = work {
        task.abort();
        let _ = task.await;
    }
    if !monitor_finished {
        monitor.abort();
        let _ = monitor.await;
    }
    if !execution_finished
        && tokio::time::timeout(Duration::from_secs(5), &mut execution)
            .await
            .is_err()
    {
        execution.abort();
        let _ = execution.await;
    }
    outcome
}

async fn retry_candidate(
    scheduler: &mut Scheduler,
    commands: &mpsc::Sender<ExecutionCommand>,
    curve: Pubkey,
    now: Instant,
    max_failures: u8,
) {
    scheduler.retry(curve, now);
    if scheduler.exhausted(&curve) {
        log::warn!(
            "curve={curve} reached max_curve_failures={max_failures}; ignoring until manually removed from ledger"
        );
        let (persisted, result) = tokio::sync::oneshot::channel();
        let command = ExecutionCommand::Exhausted {
            curve: curve.to_string(),
            failures: max_failures,
            persisted,
        };
        if commands.send(command).await.is_err() {
            log::error!("could not queue exhausted curve ledger record for {curve}");
            return;
        }
        match result.await {
            Ok(Ok(())) => {
                log::info!("curve={curve} durably excluded after {max_failures} failures")
            }
            Ok(Err(_)) | Err(_) => log::error!("could not durably exclude exhausted curve={curve}"),
        }
    }
}

fn admit_stream(
    scheduler: &mut Scheduler,
    candidate: Candidate,
    scan_running: bool,
    needed: &AtomicBool,
) {
    let reserve =
        scan_running || needed.load(Ordering::Acquire) || scheduler.reconciliation_needed();
    if reserve && scheduler.len() >= CAPACITY - 1 && !scheduler.contains(&candidate.curve) {
        needed.store(true, Ordering::Release);
        return;
    }
    if matches!(
        scheduler.enqueue(candidate, Instant::now()),
        EnqueueResult::Deferred | EnqueueResult::Overflow
    ) {
        needed.store(true, Ordering::Release);
    }
}

fn scan_has_capacity(queued: usize) -> bool {
    queued < CAPACITY
}

fn should_defer(error: &crate::coordinator::CoordinatorError) -> bool {
    use crate::{
        coordinator::CoordinatorError::{Preparation, Recovery},
        executor::SimulationError,
        pipeline::PipelineError,
        submission::SubmissionError,
    };
    match error {
        Preparation(PipelineError::Rpc(error))
        | Recovery(SubmissionError::Rpc(error))
        | Preparation(PipelineError::Simulation(SimulationError::Rpc(error)))
        | Recovery(SubmissionError::Simulation(SimulationError::Rpc(error))) => {
            error.get_transaction_error().is_none_or(|error| {
                error == solana_rpc_client::api::response::TransactionError::BlockhashNotFound
            })
        }
        Preparation(
            PipelineError::Timeout
            | PipelineError::StaleContext
            | PipelineError::NearExpiry
            | PipelineError::MissingAccount(_)
            | PipelineError::InvalidAccountData(_),
        )
        | Recovery(SubmissionError::Timeout | SubmissionError::PendingAttempt)
        | Preparation(PipelineError::Simulation(
            SimulationError::Timeout | SimulationError::StaleContext,
        ))
        | Recovery(SubmissionError::Simulation(
            SimulationError::Timeout | SimulationError::StaleContext,
        )) => true,
        Preparation(PipelineError::Simulation(SimulationError::Rejected { error, .. }))
        | Recovery(SubmissionError::Simulation(SimulationError::Rejected { error, .. })) => {
            solana_rpc_client::api::response::TransactionError::from(error.clone())
                == solana_rpc_client::api::response::TransactionError::BlockhashNotFound
        }
        _ => false,
    }
}

fn rpc_diagnostic(error: &RpcError) -> String {
    use solana_rpc_client::api::{client_error::ErrorKind, request::RpcError};
    let (method, class) = match error.kind() {
        ErrorKind::Custom(message) if message == "Invalid blockhash" => {
            return "RPC getFeeForMessage: blockhash unavailable".into();
        }
        _ => (error.request(), reconcile::classify_rpc(error)),
    };
    let mut diagnostic = match method {
        Some(method) => format!("RPC {method} {class:?}"),
        None => format!("RPC {class:?}"),
    };
    if let Some(transaction_error) = error.get_transaction_error() {
        return simulation_rejection_class(&diagnostic, &transaction_error.into());
    }
    match error.kind() {
        ErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) => {
            diagnostic.push_str(&format!(" (JSON-RPC code {code})"));
        }
        ErrorKind::Reqwest(error) => {
            if let Some(status) = error.status() {
                diagnostic.push_str(&format!(" (HTTP {})", status.as_u16()));
            }
        }
        _ => {}
    }
    diagnostic
}

fn error_class(error: &crate::coordinator::CoordinatorError) -> String {
    use crate::{coordinator::CoordinatorError, pipeline::PipelineError};
    match error {
        CoordinatorError::MissingPolicy => "missing execution policy".into(),
        CoordinatorError::Cancelled => "cancelled".into(),
        CoordinatorError::Preparation(PipelineError::Rpc(error))
        | CoordinatorError::Preparation(PipelineError::Simulation(
            crate::executor::SimulationError::Rpc(error),
        )) => rpc_diagnostic(error),
        CoordinatorError::Preparation(PipelineError::Persistence(error))
        | CoordinatorError::Recovery(error) => submission_error_class(error),
        CoordinatorError::Preparation(PipelineError::Simulation(
            crate::executor::SimulationError::Rejected { error, .. },
        )) => simulation_rejection_class("simulation", error),
        CoordinatorError::Preparation(error) => match error {
            PipelineError::Timeout => "preparation RPC timeout",
            PipelineError::WrongCluster => "ledger and RPC cluster mismatch",
            PipelineError::WrongPayer => "ledger and payer mismatch",
            PipelineError::MissingAccount(account) => {
                return format!("required {account} account missing");
            }
            PipelineError::InvalidAccountData(account) => {
                return format!("invalid RPC account data: {account}");
            }
            PipelineError::StaleContext => "RPC context is stale",
            PipelineError::AlreadyMigrated(_) => "curve already migrated",
            PipelineError::FeeBounds => "quoted fee exceeds policy",
            PipelineError::NearExpiry => "blockhash near expiry",
            PipelineError::ResourceBounds => "simulation exceeds resource policy",
            PipelineError::Simulation(crate::executor::SimulationError::BelowMinimum {
                ..
            }) => "simulated profit below minimum",
            PipelineError::Simulation(crate::executor::SimulationError::MissingBalances) => {
                "simulation balance data unavailable"
            }
            PipelineError::Simulation(_) => "simulation failed",
            PipelineError::Curve(_) => "invalid curve",
            PipelineError::GlobalConfig => "invalid global configuration",
            PipelineError::MissingPolicy | PipelineError::InvalidPolicy => {
                "invalid execution policy"
            }
            PipelineError::Build(_) | PipelineError::Serialize => "transaction construction failed",
            PipelineError::Rpc(_) | PipelineError::Persistence(_) => unreachable!(),
        }
        .into(),
    }
}

fn submission_error_class(error: &crate::submission::SubmissionError) -> String {
    use crate::submission::SubmissionError;
    if let SubmissionError::Simulation(crate::executor::SimulationError::Rejected {
        error, ..
    }) = error
    {
        return simulation_rejection_class("send-time simulation", error);
    }
    match error {
        SubmissionError::Ledger(_) => "ledger operation failed",
        SubmissionError::Rpc(error)
        | SubmissionError::Simulation(crate::executor::SimulationError::Rpc(error)) => {
            return rpc_diagnostic(error);
        }
        SubmissionError::Timeout => "recovery RPC timeout",
        SubmissionError::InvalidAttempt => "invalid journaled attempt",
        SubmissionError::WrongCluster => "ledger and RPC cluster mismatch",
        SubmissionError::PendingAttempt => "another attempt remains unresolved",
        SubmissionError::CurvePendingOrComplete => "curve pending settlement or already complete",
        SubmissionError::Accounting(crate::accounting::AccountingError::RpcUnavailable) => {
            "finalized transaction RPC unavailable"
        }
        SubmissionError::Accounting(crate::accounting::AccountingError::InvalidEvidence) => {
            "finalized transaction evidence invalid"
        }
        SubmissionError::Accounting(_) => "finalized accounting failed",
        SubmissionError::Simulation(crate::executor::SimulationError::BelowMinimum { .. }) => {
            "send-time profit below minimum"
        }
        SubmissionError::Simulation(crate::executor::SimulationError::MissingBalances) => {
            "simulation balance data unavailable"
        }
        SubmissionError::Simulation(_) => "send-time simulation failed",
        SubmissionError::ResourceBounds => "send-time resources exceed policy",
        SubmissionError::FeeBounds => "send-time fee exceeds policy",
    }
    .into()
}

// Only emit enum identifiers and numeric custom codes, never untrusted logs or text.
fn simulation_rejection_class(
    phase: &str,
    error: &solana_rpc_client::api::response::UiTransactionError,
) -> String {
    use solana_rpc_client::api::response::TransactionError;
    let error = TransactionError::from(error.clone());
    match error {
        TransactionError::InstructionError(index, error) => {
            let code = format!("{error:?}");
            let safe = code.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                || code
                    .strip_prefix("Custom(")
                    .and_then(|s| s.strip_suffix(')'))
                    .is_some_and(|n| !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()));
            if safe {
                format!("{phase} rejected: instruction {index}, {code}")
            } else {
                format!("{phase} rejected: instruction {index} failed")
            }
        }
        error => {
            let code = format!("{error:?}");
            if code.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
                format!("{phase} rejected: {code}")
            } else {
                format!("{phase} rejected: transaction error")
            }
        }
    }
}

enum WorkResult {
    Scan(
        u64,
        Result<[reconcile::Snapshot<CurveState>; 2], reconcile::ScanError>,
    ),
}

fn backoff(failures: u32) -> Duration {
    let base = 2u64.saturating_pow(failures.min(5)).min(30);
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_millis()) % 1000)
        .unwrap_or(0);
    Duration::from_secs(base) + Duration::from_millis(jitter)
}

fn candidate(key: Pubkey, curve: &CurveState, slot: u64) -> Option<Candidate> {
    chain::eligible(curve).then_some(Candidate {
        curve: key,
        mint: curve.mint,
        slot,
        write_version: None,
    })
}

#[cfg(unix)]
fn termination_signal() -> std::io::Result<tokio::signal::unix::Signal> {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
}

#[cfg(unix)]
async fn termination(signal: &mut tokio::signal::unix::Signal) {
    signal.recv().await;
}

#[cfg(not(unix))]
fn termination_signal() -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
async fn termination(_: &mut ()) {
    std::future::pending::<()>().await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn simulation_rejection_reports_safe_codes_without_program_logs() {
        use crate::{
            coordinator::CoordinatorError, executor::SimulationError, pipeline::PipelineError,
            submission::SubmissionError,
        };
        let error: solana_rpc_client::api::response::UiTransactionError =
            serde_json::from_str(r#"{"InstructionError":[2,{"Custom":6001}]}"#).unwrap();
        let rejection = || SimulationError::Rejected {
            error: error.clone(),
            logs: vec!["secret program output".into()],
        };
        let preparation = CoordinatorError::Preparation(PipelineError::Simulation(rejection()));
        let recovery = CoordinatorError::Recovery(SubmissionError::Simulation(rejection()));
        for diagnostic in [error_class(&preparation), error_class(&recovery)] {
            assert!(diagnostic.contains("instruction 2, Custom(6001)"));
            assert!(!diagnostic.contains("secret"));
        }
        let missing = solana_rpc_client::api::response::TransactionError::AccountNotFound.into();
        assert!(simulation_rejection_class("simulation", &missing).contains("AccountNotFound"));
    }

    #[test]
    fn reconciliation_can_resume_with_queued_candidates() {
        assert!(scan_has_capacity(1));
        assert!(scan_has_capacity(CAPACITY - 1));
        assert!(!scan_has_capacity(CAPACITY));
    }

    #[test]
    fn stream_saturation_reserves_space_for_scan_and_requests_rescan() {
        let mut scheduler =
            Scheduler::new(CAPACITY, Duration::from_secs(2), Duration::from_secs(60));
        let needed = AtomicBool::new(false);
        let make_candidate = || Candidate {
            curve: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            slot: 1,
            write_version: Some(1),
        };
        for _ in 0..CAPACITY - 1 {
            admit_stream(&mut scheduler, make_candidate(), false, &needed);
        }
        admit_stream(&mut scheduler, make_candidate(), true, &needed);
        assert_eq!(scheduler.len(), CAPACITY - 1);
        assert!(needed.load(Ordering::Acquire));
        scheduler.scan_begin();
        assert_eq!(
            scheduler.scan_enqueue(make_candidate(), Instant::now()),
            EnqueueResult::Added
        );
        assert_eq!(scheduler.len(), CAPACITY);
    }

    #[test]
    fn reserved_stream_admission_does_not_evict_retrying_candidate() {
        let mut scheduler =
            Scheduler::new(CAPACITY, Duration::from_secs(2), Duration::from_secs(60));
        let needed = AtomicBool::new(false);
        let make = || Candidate {
            curve: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            slot: 1,
            write_version: Some(1),
        };
        let first = make();
        assert_eq!(
            scheduler.enqueue(first, Instant::now()),
            EnqueueResult::Added
        );
        for _ in 1..CAPACITY {
            assert_eq!(
                scheduler.enqueue(make(), Instant::now()),
                EnqueueResult::Added
            );
        }
        assert_eq!(scheduler.next(Instant::now()), Some(first));
        scheduler.retry(first.curve, Instant::now());
        let incoming = make();
        needed.store(true, Ordering::Release);
        admit_stream(&mut scheduler, incoming, false, &needed);
        assert!(scheduler.contains(&first.curve));
        assert!(!scheduler.contains(&incoming.curve));
        assert_eq!(scheduler.len(), CAPACITY);
    }

    #[test]
    fn reservation_persists_between_pages_and_coalesces_active_updates() {
        let mut scheduler =
            Scheduler::new(CAPACITY, Duration::from_secs(2), Duration::from_secs(60));
        let needed = AtomicBool::new(false);
        let make_candidate = || Candidate {
            curve: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            slot: 1,
            write_version: Some(1),
        };
        let first = make_candidate();
        admit_stream(&mut scheduler, first, false, &needed);
        for _ in 1..CAPACITY - 1 {
            admit_stream(&mut scheduler, make_candidate(), false, &needed);
        }
        assert_eq!(scheduler.next(Instant::now()), Some(first));
        needed.store(true, Ordering::Release); // Partial scan has ended; the next page is pending.
        let mut updated = first;
        updated.slot = 2;
        admit_stream(&mut scheduler, updated, false, &needed);
        admit_stream(&mut scheduler, make_candidate(), false, &needed);
        assert_eq!(scheduler.len(), CAPACITY - 1);
        assert!(scheduler.complete(first.curve, Instant::now()));
        assert_eq!(scheduler.len(), CAPACITY - 1); // Active update remains dirty and queued.
        scheduler.scan_begin();
        assert_eq!(
            scheduler.scan_enqueue(make_candidate(), Instant::now()),
            EnqueueResult::Added
        );
        assert_eq!(scheduler.len(), CAPACITY);
    }

    #[test]
    fn backoff_is_bounded_and_increases() {
        assert!(backoff(1) >= Duration::from_secs(2));
        assert!(backoff(4) >= Duration::from_secs(16));
        assert!(backoff(u32::MAX) < Duration::from_secs(31));
    }
}
