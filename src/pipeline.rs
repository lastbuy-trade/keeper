//! Preparation only: no transaction is sent by this module.
//! Policy values must come from an independently validated measurement of this migration.
use crate::{
    chain,
    executor::{self, SimulationEstimate},
    ledger::{ExecutionEvidence, Ledger, SignedAttempt},
    submission,
    transaction::{self, ResourcePolicy},
};
use borsh::BorshDeserialize;
use lastbuy_sdk::{
    accounts::GlobalConfig,
    bonding_curve::constants::{ANCHOR_DISCRIMINATOR_SIZE, GLOBAL_CONFIG_DISCRIMINATOR, PROGRAM_ID},
    pda::derive_global_config,
};
use solana_keypair::{Keypair, Signer};
use solana_pubkey::Pubkey;
use solana_rpc_client::{
    api::config::{CommitmentConfig, RpcAccountInfoConfig},
    nonblocking::rpc_client::RpcClient,
};
use std::time::Duration;

/// Resource, fee, profit and expiry limits supplied by the caller.
#[derive(Debug, Clone, Copy)]
pub struct ValidatedPolicy {
    pub resources: ResourcePolicy,
    pub min_profit_lamports: i128,
    pub max_compute_units: u64,
    pub max_loaded_accounts_data_size: u32,
    /// Maximum quoted total transaction fee, including priority fee.
    pub max_total_fee_lamports: u64,
    /// Required block heights remaining at the last pre-persistence check.
    pub min_block_heights_remaining: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("an explicitly validated policy is required")]
    MissingPolicy,
    #[error("policy bounds are invalid")]
    InvalidPolicy,
    #[error("RPC request timed out")]
    Timeout,
    #[error("RPC request failed")]
    Rpc(#[from] solana_rpc_client::api::client_error::Error),
    #[error("RPC genesis does not match the ledger")]
    WrongCluster,
    #[error("payer does not match the ledger")]
    WrongPayer,
    #[error("required {0} account is missing from the RPC response")]
    MissingAccount(&'static str),
    #[error("RPC returned invalid account data for {0}")]
    InvalidAccountData(&'static str),
    #[error("account response context is older than the triggering slot")]
    StaleContext,
    #[error("curve is already migrated at confirmed slot {0}")]
    AlreadyMigrated(u64),
    #[error("quoted transaction fee exceeds the policy bound")]
    FeeBounds,
    #[error("blockhash is expired or too close to expiry")]
    NearExpiry,
    #[error("invalid curve: {0}")]
    Curve(#[from] chain::CurveError),
    #[error("invalid global config owner, discriminator, or data")]
    GlobalConfig,
    #[error("simulation exceeded validated resource bounds")]
    ResourceBounds,
    #[error("transaction construction failed: {0}")]
    Build(#[from] transaction::BuildError),
    #[error("simulation failed: {0}")]
    Simulation(#[from] executor::SimulationError),
    #[error("transaction serialization failed")]
    Serialize,
    #[error("attempt persistence failed: {0}")]
    Persistence(#[from] submission::SubmissionError),
}

pub struct PreparedAttempt {
    pub attempt: SignedAttempt,
    pub estimate: SimulationEstimate,
    pub account_slot: u64,
}

fn validate_policy(policy: Option<ValidatedPolicy>) -> Result<ValidatedPolicy, PipelineError> {
    let p = policy.ok_or(PipelineError::MissingPolicy)?;
    if p.min_block_heights_remaining == 0
        || p.max_compute_units == 0
        || p.max_compute_units > u64::from(u32::MAX)
        || p.max_loaded_accounts_data_size == 0
        || p.resources.compute_unit_limit == 0
        || p.resources.loaded_accounts_data_size_limit == 0
        || u64::from(p.resources.compute_unit_limit) > p.max_compute_units
        || p.resources.loaded_accounts_data_size_limit > p.max_loaded_accounts_data_size
        || p.resources.priority_fee_lamports > p.resources.max_priority_fee_lamports
    {
        return Err(PipelineError::InvalidPolicy);
    }
    Ok(p)
}

fn check_estimate(
    policy: &ValidatedPolicy,
    estimate: SimulationEstimate,
) -> Result<(), PipelineError> {
    if estimate.units_consumed > u64::from(policy.resources.compute_unit_limit)
        || estimate.units_consumed > policy.max_compute_units
        || estimate.loaded_accounts_data_size > policy.resources.loaded_accounts_data_size_limit
        || estimate.loaded_accounts_data_size > policy.max_loaded_accounts_data_size
    {
        return Err(PipelineError::ResourceBounds);
    }
    if estimate.payer_delta_lamports < policy.min_profit_lamports {
        return Err(executor::SimulationError::BelowMinimum {
            estimated: estimate.payer_delta_lamports,
            minimum: policy.min_profit_lamports,
        }
        .into());
    }
    Ok(())
}

// Integer ceiling avoids rounding the required 15% margin down.
fn probe_resources(policy: &ValidatedPolicy) -> Result<ResourcePolicy, PipelineError> {
    Ok(ResourcePolicy {
        compute_unit_limit: u32::try_from(policy.max_compute_units)
            .map_err(|_| PipelineError::InvalidPolicy)?,
        loaded_accounts_data_size_limit: policy.max_loaded_accounts_data_size,
        ..policy.resources
    })
}

fn measured_resources(
    policy: &ValidatedPolicy,
    estimate: SimulationEstimate,
) -> Result<ResourcePolicy, PipelineError> {
    fn margin(measured: u64, max: u32) -> Option<u32> {
        if measured == 0 || measured > u64::from(max) {
            return None;
        }
        let with_margin = u128::from(measured).checked_mul(115)?.checked_add(99)? / 100;
        u32::try_from(with_margin.min(u128::from(max))).ok()
    }
    let maximum = probe_resources(policy)?;
    let compute = margin(estimate.units_consumed, maximum.compute_unit_limit)
        .ok_or(PipelineError::ResourceBounds)?;
    let loaded = margin(
        u64::from(estimate.loaded_accounts_data_size),
        maximum.loaded_accounts_data_size_limit,
    )
    .ok_or(PipelineError::ResourceBounds)?;
    Ok(ResourcePolicy {
        compute_unit_limit: compute,
        loaded_accounts_data_size_limit: loaded,
        ..maximum
    })
}

fn check_fee(policy: &ValidatedPolicy, fee: u64) -> Result<(), PipelineError> {
    if fee > policy.max_total_fee_lamports || fee < policy.resources.priority_fee_lamports {
        return Err(PipelineError::FeeBounds);
    }
    Ok(())
}

fn check_expiry(
    last_valid: u64,
    current: u64,
    minimum_remaining: u64,
) -> Result<(), PipelineError> {
    if last_valid
        .checked_sub(current)
        .is_none_or(|remaining| remaining < minimum_remaining)
    {
        return Err(PipelineError::NearExpiry);
    }
    Ok(())
}

/// Fetch finalized curve, global configuration and payer in one RPC snapshot, sign and
/// simulate the exact bytes, then fsync the attempt before returning it. The caller is
/// responsible for supplying an evidence-backed policy and for freshness at send time.
pub async fn prepare_attempt(
    rpc: &RpcClient,
    ledger: &mut Ledger,
    payer: &Keypair,
    curve: Pubkey,
    triggering_slot: u64,
    policy: Option<ValidatedPolicy>,
    timeout: Duration,
) -> Result<PreparedAttempt, PipelineError> {
    let policy = validate_policy(policy)?;
    if payer.pubkey().to_string() != ledger.replay.payer {
        return Err(PipelineError::WrongPayer);
    }
    let genesis = tokio::time::timeout(timeout, rpc.get_genesis_hash())
        .await
        .map_err(|_| PipelineError::Timeout)??;
    if genesis.to_string() != ledger.replay.genesis_hash {
        return Err(PipelineError::WrongCluster);
    }
    let global = derive_global_config().0;
    let account_config = RpcAccountInfoConfig {
        encoding: Some(
            serde_json::from_value(serde_json::json!("base64"))
                .map_err(|_| PipelineError::InvalidAccountData("request encoding"))?,
        ),
        commitment: Some(CommitmentConfig::confirmed()),
        // Use a bank at least as recent as the Yellowstone/reconciliation candidate.
        min_context_slot: Some(triggering_slot),
        ..Default::default()
    };
    let accounts = tokio::time::timeout(
        timeout,
        rpc.get_multiple_ui_accounts_with_config(&[curve, global, payer.pubkey()], account_config),
    )
    .await
    .map_err(|_| PipelineError::Timeout)??;
    if accounts.context.slot < triggering_slot {
        return Err(PipelineError::StaleContext);
    }
    let [curve_account, config_account, payer_account] = accounts.value.as_slice() else {
        return Err(PipelineError::InvalidAccountData("account list"));
    };
    let curve_account = curve_account
        .as_ref()
        .ok_or(PipelineError::MissingAccount("curve"))?;
    let curve_account = curve_account
        .to_account()
        .ok_or(PipelineError::InvalidAccountData("curve"))?;
    let state = chain::decode_curve(&curve, &curve_account.owner, &curve_account.data)?;
    if state.status == lastbuy_sdk::bonding_curve::CurveStatus::Migrated {
        return Err(PipelineError::AlreadyMigrated(accounts.context.slot));
    }
    let config_account = config_account
        .as_ref()
        .ok_or(PipelineError::MissingAccount("global config"))?;
    let payer_account = payer_account
        .as_ref()
        .ok_or(PipelineError::MissingAccount("payer"))?;
    let config_account = config_account
        .to_account()
        .ok_or(PipelineError::InvalidAccountData("global config"))?;
    let payer_account = payer_account
        .to_account()
        .ok_or(PipelineError::InvalidAccountData("payer"))?;
    let (discriminator, mut data) = config_account
        .data
        .split_at_checked(ANCHOR_DISCRIMINATOR_SIZE)
        .ok_or(PipelineError::GlobalConfig)?;
    if config_account.owner != PROGRAM_ID || discriminator != GLOBAL_CONFIG_DISCRIMINATOR {
        return Err(PipelineError::GlobalConfig);
    }
    let config = GlobalConfig::deserialize(&mut data).map_err(|_| PipelineError::GlobalConfig)?;
    let (blockhash, last_valid_block_height) = tokio::time::timeout(
        timeout,
        rpc.get_latest_blockhash_with_commitment(CommitmentConfig::confirmed()),
    )
    .await
    .map_err(|_| PipelineError::Timeout)??;
    let instructions = transaction::build_instruction_plan(payer.pubkey(), curve, &state, &config)?;
    // The first simulation uses the explicit policy ceilings. Its transaction is never persisted.
    let probe =
        transaction::sign_transaction(&instructions, payer, blockhash, probe_resources(&policy)?)?;
    let measured = executor::simulate_final_transaction(
        rpc,
        &probe,
        i128::MIN,
        accounts.context.slot,
        timeout,
    )
    .await?;
    let final_resources = measured_resources(&policy, measured)?;
    let final_policy = ValidatedPolicy {
        resources: final_resources,
        ..policy
    };
    let tx = transaction::sign_transaction(&instructions, payer, blockhash, final_resources)?;
    let solana_message::VersionedMessage::V1(message) = &tx.message else {
        return Err(PipelineError::Serialize);
    };
    let fee = tokio::time::timeout(timeout, rpc.get_fee_for_message(message))
        .await
        .map_err(|_| PipelineError::Timeout)??;
    check_fee(&final_policy, fee)?;
    let estimate = executor::simulate_final_transaction(
        rpc,
        &tx,
        final_policy.min_profit_lamports,
        accounts.context.slot,
        timeout,
    )
    .await?;
    let height = tokio::time::timeout(
        timeout,
        rpc.get_block_height_with_commitment(CommitmentConfig::confirmed()),
    )
    .await
    .map_err(|_| PipelineError::Timeout)??;
    check_expiry(
        last_valid_block_height,
        height,
        policy.min_block_heights_remaining,
    )?;
    check_estimate(&final_policy, estimate)?;
    let signature = tx
        .signatures
        .first()
        .ok_or(PipelineError::Serialize)?
        .to_string();
    let attempt = SignedAttempt {
        signature: signature.clone(),
        curve: curve.to_string(),
        mint: state.mint.to_string(),
        payer: payer.pubkey().to_string(),
        genesis_hash: genesis.to_string(),
        blockhash: blockhash.to_string(),
        last_valid_block_height,
        signed_bytes: wincode::serialize(&tx).map_err(|_| PipelineError::Serialize)?,
        attempt_id: signature,
        compute_unit_limit: u64::from(final_resources.compute_unit_limit),
        loaded_accounts_data_size_limit: u64::from(final_resources.loaded_accounts_data_size_limit),
        priority_fee_lamports: final_resources.priority_fee_lamports,
    };
    let evidence = ExecutionEvidence {
        min_profit_lamports: policy.min_profit_lamports,
        simulated_delta_lamports: estimate.payer_delta_lamports,
        policy_compute_unit_limit: policy.resources.compute_unit_limit,
        policy_loaded_accounts_data_size_limit: policy.resources.loaded_accounts_data_size_limit,
        priority_fee_lamports: policy.resources.priority_fee_lamports,
        max_priority_fee_lamports: policy.resources.max_priority_fee_lamports,
        max_compute_units: policy.max_compute_units,
        max_loaded_accounts_data_size: policy.max_loaded_accounts_data_size,
        max_total_fee_lamports: policy.max_total_fee_lamports,
        min_block_heights_remaining: policy.min_block_heights_remaining,
        simulated_units: estimate.units_consumed,
        simulated_loaded_bytes: estimate.loaded_accounts_data_size,
        quoted_fee_lamports: fee,
        payer_balance: payer_account.lamports,
        account_slot: accounts.context.slot,
        checked_block_height: height,
    };
    submission::persist_validated_attempt(ledger, attempt.clone(), evidence)?;
    Ok(PreparedAttempt {
        attempt,
        estimate,
        account_slot: accounts.context.slot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy() -> ValidatedPolicy {
        ValidatedPolicy {
            resources: ResourcePolicy {
                compute_unit_limit: 100,
                loaded_accounts_data_size_limit: 200,
                priority_fee_lamports: 2,
                max_priority_fee_lamports: 3,
            },
            min_profit_lamports: 5,
            max_compute_units: 100,
            max_loaded_accounts_data_size: 200,
            max_total_fee_lamports: 10,
            min_block_heights_remaining: 5,
        }
    }
    fn estimate() -> SimulationEstimate {
        SimulationEstimate {
            payer_delta_lamports: 5,
            units_consumed: 100,
            loaded_accounts_data_size: 200,
        }
    }
    #[test]
    fn policy_is_required() {
        assert!(matches!(
            validate_policy(None),
            Err(PipelineError::MissingPolicy)
        ));
        assert!(validate_policy(Some(policy())).is_ok());
    }
    #[test]
    fn explicit_negative_profit_fee_and_expiry_bounds() {
        let mut p = policy();
        p.min_profit_lamports = -10;
        assert!(validate_policy(Some(p)).is_ok());
        let mut e = estimate();
        e.payer_delta_lamports = -10;
        assert!(check_estimate(&p, e).is_ok());
        e.payer_delta_lamports = -11;
        assert!(check_estimate(&p, e).is_err());
        assert!(check_fee(&p, 10).is_ok());
        assert!(matches!(check_fee(&p, 11), Err(PipelineError::FeeBounds)));
        assert!(matches!(check_fee(&p, 1), Err(PipelineError::FeeBounds)));
        assert!(check_expiry(100, 95, 5).is_ok());
        assert!(matches!(
            check_expiry(100, 96, 5),
            Err(PipelineError::NearExpiry)
        ));
        assert!(matches!(
            check_expiry(100, 101, 0),
            Err(PipelineError::NearExpiry)
        ));
    }
    #[test]
    fn resource_margin_rounds_up_and_respects_explicit_maxima() {
        let p = policy();
        let mut e = estimate();
        e.units_consumed = 80;
        e.loaded_accounts_data_size = 101;
        let sized = measured_resources(&p, e).unwrap();
        assert_eq!(sized.compute_unit_limit, 92);
        assert_eq!(sized.loaded_accounts_data_size_limit, 117);
        e.units_consumed = 87; // ceil(100.05) is capped to the validated maximum.
        assert_eq!(measured_resources(&p, e).unwrap().compute_unit_limit, 100);
        e.units_consumed = 101;
        assert!(matches!(
            measured_resources(&p, e),
            Err(PipelineError::ResourceBounds)
        ));
        e.units_consumed = 0;
        assert!(matches!(
            measured_resources(&p, e),
            Err(PipelineError::ResourceBounds)
        ));
        e.units_consumed = u64::MAX;
        assert!(matches!(
            measured_resources(&p, e),
            Err(PipelineError::ResourceBounds)
        ));
        let mut lower = p;
        lower.resources.compute_unit_limit = 10;
        lower.resources.loaded_accounts_data_size_limit = 20;
        let probe = probe_resources(&lower).unwrap();
        assert_eq!(probe.compute_unit_limit, 100);
        assert_eq!(probe.loaded_accounts_data_size_limit, 200);
        let mut invalid = p;
        invalid.min_block_heights_remaining = 0;
        assert!(matches!(
            validate_policy(Some(invalid)),
            Err(PipelineError::InvalidPolicy)
        ));
    }
    #[test]
    fn simulation_profit_and_resources_must_meet_bounds() {
        let mut e = estimate();
        e.payer_delta_lamports = 4;
        assert!(matches!(
            check_estimate(&policy(), e),
            Err(PipelineError::Simulation(
                executor::SimulationError::BelowMinimum { .. }
            ))
        ));
        let mut e = estimate();
        e.units_consumed = 101;
        assert!(matches!(
            check_estimate(&policy(), e),
            Err(PipelineError::ResourceBounds)
        ));
        let mut e = estimate();
        e.loaded_accounts_data_size = 201;
        assert!(matches!(
            check_estimate(&policy(), e),
            Err(PipelineError::ResourceBounds)
        ));
    }
}
