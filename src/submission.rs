//! Recovery and send-time revalidation of durable signed attempts.
use std::{str::FromStr, time::Duration};

use solana_message::VersionedMessage;
use solana_rpc_client::{
    api::config::{CommitmentConfig, RpcSendTransactionConfig},
    nonblocking::rpc_client::RpcClient,
};
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status_client_types::TransactionConfirmationStatus;

use crate::{
    accounting, executor,
    ledger::{ExecutionEvidence, Ledger, LedgerError, Record, SignedAttempt},
};

#[derive(Debug, thiserror::Error)]
pub enum SubmissionError {
    #[error("ledger operation failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("RPC request failed; attempt remains unresolved")]
    Rpc(#[from] solana_rpc_client::api::client_error::Error),
    #[error("RPC request timed out; attempt remains unresolved")]
    Timeout,
    #[error("signed attempt is inconsistent with its journal identity")]
    InvalidAttempt,
    #[error("RPC genesis hash does not match the journal")]
    WrongCluster,
    #[error("another attempt remains unresolved")]
    PendingAttempt,
    #[error("this curve has already finalized successfully or awaits settlement")]
    CurvePendingOrComplete,
    #[error("finalized transaction accounting evidence is incomplete: {0}")]
    Accounting(#[from] accounting::AccountingError),
    #[error("send-time simulation failed: {0}")]
    Simulation(#[from] executor::SimulationError),
    #[error("send-time simulation exceeds the signed transaction resource limits")]
    ResourceBounds,
    #[error("send-time transaction fee exceeds the validated policy")]
    FeeBounds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryResult {
    /// Inclusion cannot yet be established, or confirmed status has not finalized.
    Pending,
    /// RPC acknowledged a retransmission; inclusion remains unknown.
    Retransmitted,
    /// Finalized metadata was validated and its settlement durably written.
    Settled,
    /// Finalized status was durably recorded; settlement remains pending.
    Finalized,
    /// Finalized height passed the blockhash expiry; inclusion is still unknown.
    ExpiredUnresolved,
    /// Durable intent was never recorded, so this attempt was never submitted.
    NeverSubmitted,
}

/// Immutable journal snapshot for one background settlement lookup.
#[derive(Debug, Clone)]
pub struct SettlementJob {
    pub(crate) attempt: SignedAttempt,
    pub(crate) finalized: Option<(bool, u64)>,
}

/// A lookup result contains no ledger reference and can be applied by the serial owner.
pub struct SettlementResponse {
    pub(crate) job: SettlementJob,
    pub(crate) result: Result<Option<crate::ledger::Settlement>, SubmissionError>,
}

pub async fn fetch_settlement(
    rpc: &RpcClient,
    job: SettlementJob,
    timeout: Duration,
) -> SettlementResponse {
    let result = tokio::time::timeout(timeout, async {
        validate_attempt(&job.attempt)?;
        let genesis = rpc.get_genesis_hash().await?;
        if genesis.to_string() != job.attempt.genesis_hash {
            return Err(SubmissionError::WrongCluster);
        }
        match job.finalized {
            Some((success, slot)) => {
                accounting::fetch_finalized_settlement(rpc, &job.attempt, success, slot, timeout)
                    .await
                    .map(Some)
                    .map_err(Into::into)
            }
            None => accounting::fetch_unobserved_finalized_settlement(rpc, &job.attempt, timeout)
                .await
                .map_err(Into::into),
        }
    })
    .await
    .unwrap_or(Err(SubmissionError::Timeout));
    SettlementResponse { job, result }
}

fn validate_attempt(
    attempt: &SignedAttempt,
) -> Result<(VersionedTransaction, Signature), SubmissionError> {
    let tx: VersionedTransaction = wincode::deserialize_exact(&attempt.signed_bytes)
        .map_err(|_| SubmissionError::InvalidAttempt)?;
    let signature =
        Signature::from_str(&attempt.signature).map_err(|_| SubmissionError::InvalidAttempt)?;
    let payer = solana_pubkey::Pubkey::from_str(&attempt.payer)
        .map_err(|_| SubmissionError::InvalidAttempt)?;
    let mint = solana_pubkey::Pubkey::from_str(&attempt.mint)
        .map_err(|_| SubmissionError::InvalidAttempt)?;
    let curve = solana_pubkey::Pubkey::from_str(&attempt.curve)
        .map_err(|_| SubmissionError::InvalidAttempt)?;
    let blockhash = solana_hash::Hash::from_str(&attempt.blockhash)
        .map_err(|_| SubmissionError::InvalidAttempt)?;
    let VersionedMessage::V1(message) = &tx.message else {
        return Err(SubmissionError::InvalidAttempt);
    };
    let keys = tx.message.static_account_keys();
    let migrations: Vec<_> = tx
        .message
        .instructions()
        .iter()
        .filter(|ix| {
            keys.get(ix.program_id_index as usize) == Some(&lastbuy_sdk::PROGRAM_ID)
                && ix.data == lastbuy_sdk::MIGRATE_DISCRIMINATOR
        })
        .collect();
    if migrations.len() != 1
        || migrations[0]
            .accounts
            .first()
            .and_then(|i| keys.get(*i as usize))
            != Some(&payer)
        || migrations[0]
            .accounts
            .get(4)
            .and_then(|i| keys.get(*i as usize))
            != Some(&curve)
        || migrations[0]
            .accounts
            .get(10)
            .and_then(|i| keys.get(*i as usize))
            != Some(&mint)
        || message.config.compute_unit_limit.map(u64::from) != Some(attempt.compute_unit_limit)
        || message
            .config
            .loaded_accounts_data_size_limit
            .map(u64::from)
            != Some(attempt.loaded_accounts_data_size_limit)
        || message.config.priority_fee != Some(attempt.priority_fee_lamports)
        || attempt.compute_unit_limit == 0
        || attempt.loaded_accounts_data_size_limit == 0
        || tx.signatures.first() != Some(&signature)
        || tx.message.static_account_keys().first() != Some(&payer)
        || *tx.message.recent_blockhash() != blockhash
        || lastbuy_sdk::pda::derive_curve(&mint).0 != curve
        || tx.verify_and_hash_message().is_err()
        || wincode::serialize(&tx).map_err(|_| SubmissionError::InvalidAttempt)?
            != attempt.signed_bytes
    {
        return Err(SubmissionError::InvalidAttempt);
    }
    Ok((tx, signature))
}

/// Append and fsync a signed attempt. No network send occurs here. Attempts
/// without durable execution evidence cannot be sent by this module.
pub fn persist_attempt(ledger: &mut Ledger, attempt: SignedAttempt) -> Result<(), SubmissionError> {
    validate_and_check_lane(ledger, &attempt)?;
    ledger.append(Record::SignedAttempt { attempt })?;
    Ok(())
}

/// Persist the exact signed bytes and their checked execution evidence atomically.
pub fn persist_validated_attempt(
    ledger: &mut Ledger,
    attempt: SignedAttempt,
    evidence: ExecutionEvidence,
) -> Result<(), SubmissionError> {
    validate_and_check_lane(ledger, &attempt)?;
    ledger.append(Record::PolicyValidatedAttemptV2 { attempt, evidence })?;
    Ok(())
}

fn validate_and_check_lane(
    ledger: &Ledger,
    attempt: &SignedAttempt,
) -> Result<(), SubmissionError> {
    if ledger
        .replay
        .inclusion_ambiguous_attempts()
        .next()
        .is_some()
    {
        return Err(SubmissionError::PendingAttempt);
    }
    if ledger
        .replay
        .attempts
        .values()
        .filter(|prior| prior.curve == attempt.curve)
        .any(|prior| {
            ledger.replay.signature_succeeded(&prior.signature)
                || (ledger
                    .replay
                    .finalized_outcomes
                    .contains_key(&prior.signature)
                    && !ledger.replay.settlements.contains_key(&prior.signature))
        })
    {
        return Err(SubmissionError::CurvePendingOrComplete);
    }
    validate_attempt(attempt)?;
    Ok(())
}

/// Inspect one journaled signature with history search. When enabled, rebroadcast
/// only the durable signed transaction after a fresh successful simulation.
/// Never infer expiry from missing history.
pub async fn recover_attempt(
    rpc: &RpcClient,
    ledger: &mut Ledger,
    signature_text: &str,
    timeout: Duration,
    allow_retransmit: bool,
) -> Result<RecoveryResult, SubmissionError> {
    recover_inner(
        rpc,
        ledger,
        signature_text,
        timeout,
        allow_retransmit,
        false,
    )
    .await
}

/// Only the coordinator may use the freshly prepared result for an initial send.
pub(crate) async fn submit_prepared(
    rpc: &RpcClient,
    ledger: &mut Ledger,
    prepared: crate::pipeline::PreparedAttempt,
    timeout: Duration,
) -> Result<RecoveryResult, SubmissionError> {
    recover_inner(
        rpc,
        ledger,
        &prepared.attempt.signature,
        timeout,
        true,
        true,
    )
    .await
}

async fn recover_inner(
    rpc: &RpcClient,
    ledger: &mut Ledger,
    signature_text: &str,
    timeout: Duration,
    allow_retransmit: bool,
    initial_send: bool,
) -> Result<RecoveryResult, SubmissionError> {
    let attempt = ledger
        .replay
        .unresolved_attempts()
        .find(|a| a.signature == signature_text)
        .cloned()
        .ok_or(SubmissionError::InvalidAttempt)?;
    let (tx, signature) = validate_attempt(&attempt)?;
    if !initial_send
        && ledger.replay.intent_protocol.contains(signature_text)
        && !ledger.replay.send_intents.contains(signature_text)
    {
        ledger.append(Record::NeverSubmitted {
            signature: signature_text.to_owned(),
        })?;
        return Ok(RecoveryResult::NeverSubmitted);
    }
    let genesis = tokio::time::timeout(timeout, rpc.get_genesis_hash())
        .await
        .map_err(|_| SubmissionError::Timeout)??;
    if genesis.to_string() != attempt.genesis_hash {
        return Err(SubmissionError::WrongCluster);
    }
    if let Some(&(success, slot)) = ledger.replay.finalized_outcomes.get(&attempt.signature) {
        let settlement =
            accounting::fetch_finalized_settlement(rpc, &attempt, success, slot, timeout).await?;
        ledger.append(Record::FinalizedSettlement { settlement })?;
        return Ok(RecoveryResult::Settled);
    }
    let statuses = tokio::time::timeout(
        timeout,
        rpc.get_signature_statuses_with_history(&[signature]),
    )
    .await
    .map_err(|_| SubmissionError::Timeout)??
    .value;
    if statuses.len() != 1 {
        return Err(SubmissionError::InvalidAttempt);
    }
    let status = statuses.into_iter().next().flatten();
    if let Some(status) = &status
        && status.confirmation_status == Some(TransactionConfirmationStatus::Finalized)
    {
        ledger.append(Record::FinalizedOutcome {
            signature: attempt.signature.clone(),
            success: status.err.is_none(),
            slot: status.slot,
        })?;
        return Ok(RecoveryResult::Finalized);
    }
    if ledger
        .replay
        .expired_unresolved
        .contains_key(signature_text)
    {
        if let Some(settlement) =
            accounting::fetch_unobserved_finalized_settlement(rpc, &attempt, timeout).await?
        {
            ledger.append(Record::FinalizedOutcome {
                signature: attempt.signature.clone(),
                success: settlement.outcome != accounting::Outcome::Failure,
                slot: settlement.slot,
            })?;
            ledger.append(Record::FinalizedSettlement { settlement })?;
            return Ok(RecoveryResult::Settled);
        }
        return Ok(RecoveryResult::Pending);
    }
    // Height and slot must describe the same finalized bank, even behind a
    // load-balanced endpoint. Separate requests could hit different backends.
    let finalized_epoch = tokio::time::timeout(
        timeout,
        rpc.get_epoch_info_with_commitment(CommitmentConfig::finalized()),
    )
    .await
    .map_err(|_| SubmissionError::Timeout)??;
    if finalized_epoch.block_height > attempt.last_valid_block_height {
        if ledger.replay.intent_protocol.contains(signature_text)
            && !ledger.replay.send_intents.contains(signature_text)
        {
            ledger.append(Record::NeverSubmitted {
                signature: signature_text.to_owned(),
            })?;
            return Ok(RecoveryResult::NeverSubmitted);
        }
        ledger.append(Record::ExpiredUnresolved {
            signature: signature_text.to_owned(),
            finalized_slot: finalized_epoch.absolute_slot,
        })?;
        return Ok(RecoveryResult::ExpiredUnresolved);
    }
    if status.is_some() {
        return Ok(RecoveryResult::Pending);
    }
    let Some(evidence) = ledger.replay.validated_attempts.get(&attempt.signature) else {
        return Ok(RecoveryResult::Pending);
    };
    if !allow_retransmit {
        return Ok(RecoveryResult::Pending);
    }
    let VersionedMessage::V1(message) = &tx.message else {
        return Err(SubmissionError::InvalidAttempt);
    };
    let fee = tokio::time::timeout(timeout, rpc.get_fee_for_message(message))
        .await
        .map_err(|_| SubmissionError::Timeout)??;
    if fee > evidence.max_total_fee_lamports || fee < attempt.priority_fee_lamports {
        return Err(SubmissionError::FeeBounds);
    }
    let estimate = executor::simulate_final_transaction(
        rpc,
        &tx,
        evidence.min_profit_lamports,
        evidence.account_slot,
        timeout,
    )
    .await?;
    if estimate.units_consumed > attempt.compute_unit_limit
        || u64::from(estimate.loaded_accounts_data_size) > attempt.loaded_accounts_data_size_limit
    {
        return Err(SubmissionError::ResourceBounds);
    }
    let height = tokio::time::timeout(
        timeout,
        rpc.get_block_height_with_commitment(CommitmentConfig::confirmed()),
    )
    .await
    .map_err(|_| SubmissionError::Timeout)??;
    if attempt
        .last_valid_block_height
        .checked_sub(height)
        .is_none_or(|remaining| remaining < evidence.min_block_heights_remaining)
    {
        return Ok(RecoveryResult::Pending);
    }
    let config = RpcSendTransactionConfig {
        preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
        max_retries: Some(0),
        ..Default::default()
    };
    if ledger.replay.intent_protocol.contains(signature_text)
        && !ledger.replay.send_intents.contains(signature_text)
    {
        ledger.append(Record::SendIntent {
            signature: signature_text.to_owned(),
        })?;
    }
    let sent = tokio::time::timeout(timeout, rpc.send_transaction_with_config(&tx, config))
        .await
        .map_err(|_| SubmissionError::Timeout)??;
    if sent != signature {
        return Err(SubmissionError::InvalidAttempt);
    }
    Ok(RecoveryResult::Retransmitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::{ResourcePolicy, sign_transaction};
    use lastbuy_sdk::instructions::{MigrateInstruction, MigrateInstructionCtx};
    use solana_keypair::{Keypair, Signer};
    use solana_pubkey::Pubkey;

    fn migration(payer: Pubkey, mint: Pubkey) -> solana_instruction::Instruction {
        MigrateInstruction::new(MigrateInstructionCtx {
            user: payer,
            mint,
            migration_funder: Pubkey::new_unique(),
            raydium_amm_config: Pubkey::new_unique(),
            raydium_fee_account: Pubkey::new_unique(),
            raydium_cpmm_program: Pubkey::new_unique(),
            raydium_lock_program: Pubkey::new_unique(),
        })
    }

    #[test]
    fn durable_persistence_checks_identity_and_prevents_second_unresolved_attempt() {
        let payer = Keypair::new();
        let mint = Pubkey::new_unique();
        let curve = lastbuy_sdk::pda::derive_curve(&mint).0;
        let genesis = solana_hash::Hash::new_unique();
        let hash = solana_hash::Hash::new_unique();
        let tx = sign_transaction(
            &[migration(payer.pubkey(), mint)],
            &payer,
            hash,
            ResourcePolicy {
                compute_unit_limit: 100_000,
                loaded_accounts_data_size_limit: 100_000,
                priority_fee_lamports: 1,
                max_priority_fee_lamports: 1,
            },
        )
        .unwrap();
        let attempt = SignedAttempt {
            signature: tx.signatures[0].to_string(),
            curve: curve.to_string(),
            mint: mint.to_string(),
            payer: payer.pubkey().to_string(),
            genesis_hash: genesis.to_string(),
            blockhash: hash.to_string(),
            last_valid_block_height: 100,
            signed_bytes: wincode::serialize(&tx).unwrap(),
            attempt_id: "attempt-1".into(),
            compute_unit_limit: 100_000,
            loaded_accounts_data_size_limit: 100_000,
            priority_fee_lamports: 1,
        };
        let empty_tx = sign_transaction(
            &[],
            &payer,
            hash,
            ResourcePolicy {
                compute_unit_limit: 100_000,
                loaded_accounts_data_size_limit: 100_000,
                priority_fee_lamports: 1,
                max_priority_fee_lamports: 1,
            },
        )
        .unwrap();
        let mut empty = attempt.clone();
        empty.signature = empty_tx.signatures[0].to_string();
        empty.signed_bytes = wincode::serialize(&empty_tx).unwrap();
        assert!(validate_attempt(&empty).is_err());
        let mut wrong = attempt.clone();
        wrong.mint = Pubkey::new_unique().to_string();
        wrong.curve = lastbuy_sdk::pda::derive_curve(&Pubkey::from_str(&wrong.mint).unwrap())
            .0
            .to_string();
        assert!(validate_attempt(&wrong).is_err());
        let mut wrong_fee = attempt.clone();
        wrong_fee.priority_fee_lamports += 1;
        assert!(validate_attempt(&wrong_fee).is_err());
        let mut wrong_payer_ix = migration(Pubkey::new_unique(), mint);
        wrong_payer_ix.accounts[0].is_signer = false;
        let wrong_payer_tx = sign_transaction(
            &[wrong_payer_ix],
            &payer,
            hash,
            ResourcePolicy {
                compute_unit_limit: 100_000,
                loaded_accounts_data_size_limit: 100_000,
                priority_fee_lamports: 1,
                max_priority_fee_lamports: 1,
            },
        )
        .unwrap();
        let mut wrong_payer = attempt.clone();
        wrong_payer.signature = wrong_payer_tx.signatures[0].to_string();
        wrong_payer.signed_bytes = wincode::serialize(&wrong_payer_tx).unwrap();
        assert!(validate_attempt(&wrong_payer).is_err());
        let mut wrong_loaded = attempt.clone();
        wrong_loaded.loaded_accounts_data_size_limit += 1;
        assert!(validate_attempt(&wrong_loaded).is_err());
        let mut wrong_limit = attempt.clone();
        wrong_limit.compute_unit_limit += 1;
        assert!(validate_attempt(&wrong_limit).is_err());
        let dir =
            std::env::temp_dir().join(format!("keeper-submission-test-{}", Pubkey::new_unique()));
        let mut ledger =
            Ledger::open(&dir, &genesis.to_string(), &payer.pubkey().to_string()).unwrap();
        let mut invalid = attempt.clone();
        invalid.blockhash = solana_hash::Hash::new_unique().to_string();
        assert!(matches!(
            persist_attempt(&mut ledger, invalid),
            Err(SubmissionError::InvalidAttempt)
        ));
        assert_eq!(ledger.replay.unresolved_attempts().count(), 0);
        persist_attempt(&mut ledger, attempt.clone()).unwrap();
        assert!(matches!(
            persist_attempt(&mut ledger, attempt.clone()),
            Err(SubmissionError::PendingAttempt)
        ));
        drop(ledger);
        let mut recovered =
            Ledger::open(&dir, &genesis.to_string(), &payer.pubkey().to_string()).unwrap();
        assert_eq!(
            recovered.replay.unresolved_attempts().next(),
            Some(&attempt)
        );
        recovered
            .append(Record::FinalizedOutcome {
                signature: attempt.signature.clone(),
                success: true,
                slot: 42,
            })
            .unwrap();
        assert_eq!(recovered.replay.inclusion_ambiguous_attempts().count(), 0);
        assert_eq!(recovered.replay.unresolved_attempts().count(), 1);
        assert!(matches!(
            persist_attempt(&mut recovered, attempt.clone()),
            Err(SubmissionError::CurvePendingOrComplete)
        ));
        let other_mint = Pubkey::new_unique();
        let other_hash = solana_hash::Hash::new_unique();
        let other_tx = sign_transaction(
            &[migration(payer.pubkey(), other_mint)],
            &payer,
            other_hash,
            ResourcePolicy {
                compute_unit_limit: 100_000,
                loaded_accounts_data_size_limit: 100_000,
                priority_fee_lamports: 1,
                max_priority_fee_lamports: 1,
            },
        )
        .unwrap();
        let mut other = attempt.clone();
        other.signature = other_tx.signatures[0].to_string();
        other.signed_bytes = wincode::serialize(&other_tx).unwrap();
        other.blockhash = other_hash.to_string();
        other.mint = other_mint.to_string();
        other.curve = lastbuy_sdk::pda::derive_curve(&other_mint).0.to_string();
        persist_attempt(&mut recovered, other).unwrap();
        assert_eq!(recovered.replay.inclusion_ambiguous_attempts().count(), 1);
        assert_eq!(recovered.replay.unresolved_attempts().count(), 2);
        drop(recovered);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
