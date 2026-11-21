//! Transaction simulation and balance-delta estimation.

/// Compute the payer delta from a single simulation response; missing fields fail closed.
pub fn simulated_payer_delta(
    pre: Option<&[u64]>,
    post: Option<&[u64]>,
    payer_index: usize,
    account_key_count: usize,
) -> Option<i128> {
    let (pre, post) = (pre?, post?);
    if pre.len() != account_key_count || post.len() != account_key_count {
        return None;
    }
    Some(i128::from(*post.get(payer_index)?) - i128::from(*pre.get(payer_index)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_or_mismatched_simulation_balances_are_unknown() {
        assert_eq!(
            simulated_payer_delta(Some(&[100]), Some(&[90]), 0, 1),
            Some(-10)
        );
        assert_eq!(simulated_payer_delta(Some(&[100]), None, 0, 1), None);
        assert_eq!(
            simulated_payer_delta(Some(&[100]), Some(&[90, 1]), 0, 1),
            None
        );
        assert_eq!(simulated_payer_delta(Some(&[100]), Some(&[90]), 0, 2), None);
        assert_eq!(
            simulated_payer_delta(Some(&[100, 4]), Some(&[90, 4]), 0, 2),
            Some(-10)
        );
    }
    #[test]
    fn rejection_preserves_error_and_bounded_logs_without_displaying_logs() {
        use solana_rpc_client::api::response::TransactionError;
        let error = simulation_rejection(
            TransactionError::AccountNotFound.into(),
            Some(vec!["diagnostic".into(), "x".repeat(600)]),
        );
        assert!(!error.to_string().contains("diagnostic"));
        match error {
            SimulationError::Rejected { error, logs } => {
                assert_eq!(
                    TransactionError::from(error),
                    TransactionError::AccountNotFound
                );
                assert_eq!(logs[0], "diagnostic");
                assert_eq!(logs[1].len(), 512);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("simulation RPC failed")]
    Rpc(#[from] solana_rpc_client::api::client_error::Error),
    #[error("simulation request timed out")]
    Timeout,
    #[error("simulation rejected the transaction: {error}")]
    Rejected {
        error: solana_rpc_client::api::response::UiTransactionError,
        // Untrusted program output; kept for explicit diagnostics, not included in Display.
        logs: Vec<String>,
    },
    #[error("only v1 transactions are supported for simulation")]
    UnsupportedVersion,
    #[error("provider did not return complete simulation payer balances")]
    MissingBalances,
    #[error("simulation response context is older than the required slot")]
    StaleContext,
    #[error("simulation returned insufficient resource measurements")]
    MissingResources,
    #[error("simulated payer delta {estimated} is below minimum {minimum}")]
    BelowMinimum { estimated: i128, minimum: i128 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimulationEstimate {
    pub payer_delta_lamports: i128,
    pub units_consumed: u64,
    pub loaded_accounts_data_size: u32,
}

/// Simulate the exact signed transaction. A missing payer delta or resource measurement fails closed.
pub async fn simulate_final_transaction(
    rpc: &solana_rpc_client::nonblocking::rpc_client::RpcClient,
    transaction: &solana_transaction::versioned::VersionedTransaction,
    min_profit_lamports: i128,
    min_context_slot: u64,
    timeout: std::time::Duration,
) -> Result<SimulationEstimate, SimulationError> {
    use solana_rpc_client::api::config::{CommitmentConfig, RpcSimulateTransactionConfig};
    let solana_message::VersionedMessage::V1(message) = &transaction.message else {
        return Err(SimulationError::UnsupportedVersion);
    };
    let account_key_count = message.account_keys.len();
    let config = RpcSimulateTransactionConfig {
        sig_verify: true,
        min_context_slot: Some(min_context_slot),
        commitment: Some(CommitmentConfig::confirmed()),
        ..Default::default()
    };
    let result = tokio::time::timeout(
        timeout,
        rpc.simulate_transaction_with_config(transaction, config),
    )
    .await
    .map_err(|_| SimulationError::Timeout)??;
    if result.context.slot < min_context_slot {
        return Err(SimulationError::StaleContext);
    }
    let result = result.value;
    if let Some(error) = result.err {
        return Err(simulation_rejection(error, result.logs));
    }
    let payer_delta_lamports = simulated_payer_delta(
        result.pre_balances.as_deref(),
        result.post_balances.as_deref(),
        0,
        account_key_count,
    )
    .ok_or(SimulationError::MissingBalances)?;
    let units_consumed = result
        .units_consumed
        .ok_or(SimulationError::MissingResources)?;
    let loaded_accounts_data_size = result
        .loaded_accounts_data_size
        .ok_or(SimulationError::MissingResources)?;
    if payer_delta_lamports < min_profit_lamports {
        return Err(SimulationError::BelowMinimum {
            estimated: payer_delta_lamports,
            minimum: min_profit_lamports,
        });
    }
    Ok(SimulationEstimate {
        payer_delta_lamports,
        units_consumed,
        loaded_accounts_data_size,
    })
}

// Bound untrusted program output before retaining it; never include it in error Display.
fn simulation_rejection(
    error: solana_rpc_client::api::response::UiTransactionError,
    logs: Option<Vec<String>>,
) -> SimulationError {
    let logs = logs
        .unwrap_or_default()
        .into_iter()
        .take(32)
        .map(|line| line.chars().take(512).collect())
        .collect();
    SimulationError::Rejected { error, logs }
}
