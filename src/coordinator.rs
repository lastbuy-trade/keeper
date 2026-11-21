//! Serial execution lane joining discovery, durable preparation and recovery.
//!
use std::time::Duration;

use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::watch;

use crate::{
    ledger::Ledger,
    pipeline::{self, PipelineError, ValidatedPolicy},
    scheduler::Candidate,
    submission::{self, RecoveryResult, SubmissionError},
};

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("an explicitly measured execution policy is required")]
    MissingPolicy,
    #[error("operation cancelled")]
    Cancelled,
    #[error("attempt preparation failed: {0}")]
    Preparation(#[from] PipelineError),
    #[error("attempt recovery failed: {0}")]
    Recovery(#[from] SubmissionError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepResult {
    /// An existing journaled attempt was inspected; the candidate was not used.
    Recovery {
        result: RecoveryResult,
        successful_curve: Option<Pubkey>,
    },
    /// The candidate was prepared, persisted and inspected for initial submission.
    Prepared {
        result: RecoveryResult,
        successful_curve: Option<Pubkey>,
    },
    /// A finalized successful migration was already recorded for this curve.
    FinalizedSuccess,
    /// The confirmed account snapshot is already migrated; this hint is terminal only
    /// for its observed slot and does not suppress later hints.
    ObservedMigrated { slot: u64 },
}

/// Owns the only mutable ledger reference for the duration of each step. A caller
/// must retain a candidate if `Recovery` is returned and retry it once the lane is free.
pub struct Coordinator<'a> {
    rpc: &'a RpcClient,
    ledger: &'a mut Ledger,
    payer: &'a Keypair,
    policy: ValidatedPolicy,
    timeout: Duration,
    settlement_cursor: Option<String>,
}

impl<'a> Coordinator<'a> {
    pub fn new(
        rpc: &'a RpcClient,
        ledger: &'a mut Ledger,
        payer: &'a Keypair,
        policy: Option<ValidatedPolicy>,
        timeout: Duration,
    ) -> Result<Self, CoordinatorError> {
        Ok(Self {
            rpc,
            ledger,
            payer,
            policy: policy.ok_or(CoordinatorError::MissingPolicy)?,
            timeout,
            settlement_cursor: None,
        })
    }

    pub fn mark_curve_exhausted(
        &mut self,
        curve: String,
        failures: u8,
    ) -> Result<(), crate::ledger::LedgerError> {
        self.ledger
            .append(crate::ledger::Record::CurveExhausted { curve, failures })
    }

    /// Successful finalized migrations, including legacy settlement-only journal entries.
    pub fn successful_curves(&self) -> impl Iterator<Item = &str> {
        self.ledger.replay.successful_curves()
    }

    /// Whether discovery work can enter without preemptive recovery of ambiguous inclusion.
    pub fn execution_lane_available(&self) -> bool {
        self.ledger
            .replay
            .inclusion_ambiguous_attempts()
            .next()
            .is_none()
    }

    pub fn successful_curve_for_signature(&self, signature: &str) -> Option<Pubkey> {
        self.ledger
            .replay
            .signature_succeeded(signature)
            .then(|| {
                self.ledger
                    .replay
                    .attempts
                    .get(signature)
                    .and_then(|attempt| attempt.curve.parse().ok())
            })
            .flatten()
    }

    /// Inspect one unresolved attempt and revalidate before retransmitting. A missing signature
    /// or expired blockhash is not proof of non-inclusion and keeps the lane closed.
    pub async fn recover_pending(
        &mut self,
        cancelled: &mut watch::Receiver<bool>,
    ) -> Result<Option<(String, RecoveryResult)>, CoordinatorError> {
        let Some(signature) = self
            .ledger
            .replay
            .inclusion_ambiguous_attempts()
            .next()
            .map(|a| a.signature.clone())
        else {
            return Ok(None);
        };
        if *cancelled.borrow() {
            return Err(CoordinatorError::Cancelled);
        }
        let result = tokio::select! {
            biased;
            _ = cancellation(cancelled) => return Err(CoordinatorError::Cancelled),
            result = submission::recover_attempt(self.rpc, self.ledger, &signature, self.timeout, true) => result.inspect_err(|_| log_recovery_error(&signature))?,
        };
        self.log_settlement(&signature, result);
        Ok(Some((signature, result)))
    }

    /// Select an immutable snapshot for a single background settlement lookup.
    pub fn next_settlement_job(&mut self) -> Option<submission::SettlementJob> {
        let eligible = || {
            self.ledger.replay.unresolved_attempts().filter(|a| {
                self.ledger
                    .replay
                    .finalized_outcomes
                    .contains_key(&a.signature)
                    || self
                        .ledger
                        .replay
                        .expired_unresolved
                        .contains_key(&a.signature)
            })
        };
        let attempt = eligible()
            .find(|a| {
                self.settlement_cursor
                    .as_ref()
                    .is_none_or(|cursor| a.signature > *cursor)
            })
            .or_else(|| eligible().next())?
            .clone();
        self.settlement_cursor = Some(attempt.signature.clone());
        let finalized = self
            .ledger
            .replay
            .finalized_outcomes
            .get(&attempt.signature)
            .copied();
        Some(submission::SettlementJob { attempt, finalized })
    }

    /// Apply background evidence only if its journal snapshot is still current.
    pub fn apply_settlement(
        &mut self,
        response: submission::SettlementResponse,
    ) -> Result<Option<RecoveryResult>, CoordinatorError> {
        let signature = &response.job.attempt.signature;
        if !self
            .ledger
            .replay
            .unresolved_attempts()
            .any(|a| a == &response.job.attempt)
        {
            return Ok(None);
        }
        let current = self
            .ledger
            .replay
            .finalized_outcomes
            .get(signature)
            .copied();
        if current != response.job.finalized {
            return Ok(None);
        }
        if current.is_none()
            && !self
                .ledger
                .replay
                .expired_unresolved
                .contains_key(signature)
        {
            return Ok(None);
        }
        let Some(settlement) = response
            .result
            .inspect_err(|_| log_recovery_error(signature))?
        else {
            return Ok(Some(RecoveryResult::Pending));
        };
        if current.is_none() {
            self.ledger
                .append(crate::ledger::Record::FinalizedOutcome {
                    signature: signature.clone(),
                    success: settlement.outcome != crate::accounting::Outcome::Failure,
                    slot: settlement.slot,
                })
                .map_err(SubmissionError::from)?;
        }
        self.ledger
            .append(crate::ledger::Record::FinalizedSettlement { settlement })
            .map_err(SubmissionError::from)?;
        self.log_settlement(signature, RecoveryResult::Settled);
        Ok(Some(RecoveryResult::Settled))
    }

    fn log_settlement(&self, signature: &str, result: RecoveryResult) {
        if result == RecoveryResult::Settled
            && let Some(settlement) = self.ledger.replay.settlements.get(signature)
        {
            let totals = &self.ledger.replay.totals;
            log::info!(
                "signature={} curve={} outcome={:?} net_lamports={} cumulative_lamports={} successful_migrations={}",
                signature,
                settlement.curve,
                settlement.outcome,
                settlement.net_lamports,
                totals.realized_net_lamports,
                totals.successful_migrations
            );
        }
    }

    /// Process a discovery hint, or recover an inclusion-ambiguous attempt first. Preparation
    /// refreshes the account at the hint's slot and persists exact signed bytes
    /// before the initial send can occur. Cancellation can leave a durable,
    /// unresolved attempt, which subsequent calls revalidate before retransmission.
    pub async fn step(
        &mut self,
        candidate: Candidate,
        cancelled: &mut watch::Receiver<bool>,
    ) -> Result<StepResult, CoordinatorError> {
        if *cancelled.borrow() {
            return Err(CoordinatorError::Cancelled);
        }
        let curve_address = candidate.curve.to_string();
        if self.successful_curves().any(|curve| curve == curve_address) {
            return Ok(StepResult::FinalizedSuccess);
        }
        if self
            .ledger
            .replay
            .inclusion_ambiguous_attempts()
            .next()
            .is_some()
            && let Some((signature, result)) = self.recover_pending(cancelled).await?
        {
            let successful_curve = self.successful_curve_for_signature(&signature);
            return Ok(StepResult::Recovery {
                result,
                successful_curve,
            });
        }
        let min_slot = self
            .ledger
            .replay
            .attempts
            .values()
            .filter(|a| a.curve == candidate.curve.to_string())
            .filter_map(|a| {
                self.ledger
                    .replay
                    .expired_unresolved
                    .get(&a.signature)
                    .copied()
            })
            .max()
            .unwrap_or(0)
            .max(candidate.slot);
        let prepared = tokio::select! {
            biased;
            _ = cancellation(cancelled) => return Err(CoordinatorError::Cancelled),
            result = pipeline::prepare_attempt(self.rpc, self.ledger, self.payer, candidate.curve, min_slot, Some(self.policy), self.timeout) => result,
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(PipelineError::AlreadyMigrated(slot)) => {
                return Ok(StepResult::ObservedMigrated { slot });
            }
            Err(error) => return Err(error.into()),
        };
        if *cancelled.borrow() {
            return Err(CoordinatorError::Cancelled);
        }
        let signature = prepared.attempt.signature.clone();
        let result = tokio::select! {
            biased;
            _ = cancellation(cancelled) => return Err(CoordinatorError::Cancelled),
            result = submission::submit_prepared(self.rpc, self.ledger, prepared, self.timeout) => result?,
        };
        let successful_curve = self.successful_curve_for_signature(&signature);
        Ok(StepResult::Prepared {
            result,
            successful_curve,
        })
    }
}

async fn cancellation(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() || receiver.changed().await.is_err() {
            return;
        }
    }
}

fn log_recovery_error(signature: &str) {
    use std::str::FromStr;
    let safe = solana_signature::Signature::from_str(signature)
        .map(|signature| signature.to_string())
        .unwrap_or_else(|_| "invalid".to_owned());
    log::warn!("attempt recovery failed signature={safe}");
}

#[cfg(test)]
mod settlement_tests {
    use super::*;
    use crate::{
        accounting::Outcome,
        ledger::{Record, Settlement, SignedAttempt},
        transaction::ResourcePolicy,
    };

    #[test]
    fn background_evidence_is_serial_deduplicated_and_stale_safe() {
        let path = std::env::temp_dir().join(format!(
            "keeper-settlement-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        let attempt = SignedAttempt {
            signature: "sig".into(),
            curve: "curve".into(),
            mint: "mint".into(),
            payer: "payer".into(),
            genesis_hash: "genesis".into(),
            blockhash: "hash".into(),
            last_valid_block_height: 42,
            signed_bytes: vec![1],
            attempt_id: "attempt".into(),
            compute_unit_limit: 10,
            loaded_accounts_data_size_limit: 20,
            priority_fee_lamports: 1,
        };
        ledger.append(Record::SignedAttempt { attempt }).unwrap();
        ledger
            .append(Record::ExpiredUnresolved {
                signature: "sig".into(),
                finalized_slot: 2,
            })
            .unwrap();
        let rpc = RpcClient::new("http://unused.invalid".to_owned());
        let payer = Keypair::new();
        let policy = ValidatedPolicy {
            resources: ResourcePolicy {
                compute_unit_limit: 10,
                loaded_accounts_data_size_limit: 20,
                priority_fee_lamports: 1,
                max_priority_fee_lamports: 1,
            },
            min_profit_lamports: 0,
            max_compute_units: 10,
            max_loaded_accounts_data_size: 20,
            max_total_fee_lamports: 1,
            min_block_heights_remaining: 1,
        };
        let mut coordinator = Coordinator::new(
            &rpc,
            &mut ledger,
            &payer,
            Some(policy),
            Duration::from_secs(1),
        )
        .unwrap();
        let job = coordinator.next_settlement_job().unwrap();
        assert!(job.finalized.is_none());
        let settlement = Settlement {
            signature: "sig".into(),
            curve: "curve".into(),
            slot: 1,
            fee_lamports: 2,
            pre_balance: 100,
            post_balance: 97,
            net_lamports: -3,
            outcome: Outcome::Failure,
            evidence: "finalized metadata".into(),
        };
        let response = || submission::SettlementResponse {
            job: job.clone(),
            result: Ok(Some(settlement.clone())),
        };
        assert_eq!(
            coordinator.apply_settlement(response()).unwrap(),
            Some(RecoveryResult::Settled)
        );
        assert_eq!(coordinator.apply_settlement(response()).unwrap(), None);
        assert_eq!(
            coordinator.ledger.replay.finalized_outcomes.get("sig"),
            Some(&(false, 1))
        );
        assert_eq!(coordinator.ledger.replay.totals.realized_net_lamports, -3);
        assert!(coordinator.next_settlement_job().is_none());
        drop(coordinator);
        drop(ledger);
        std::fs::remove_dir_all(path).unwrap();
    }
}
