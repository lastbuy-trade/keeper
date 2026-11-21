use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AccountingError {
    #[error("payer index is outside transaction balance arrays")]
    MissingPayerBalance,
    #[error("transaction balance arrays have different lengths")]
    BalanceLengthMismatch,
    #[error("finalized transaction evidence is missing or inconsistent")]
    InvalidEvidence,
    #[error("finalized transaction RPC request failed or timed out")]
    RpcUnavailable,
    #[error("accounting total overflow")]
    Overflow,
}

/// Transaction-local payer delta; fees and account creation costs are already included.
pub fn payer_delta(pre: &[u64], post: &[u64], payer_index: usize) -> Result<i128, AccountingError> {
    if pre.len() != post.len() {
        return Err(AccountingError::BalanceLengthMismatch);
    }
    let before = pre
        .get(payer_index)
        .ok_or(AccountingError::MissingPayerBalance)?;
    let after = post
        .get(payer_index)
        .ok_or(AccountingError::MissingPayerBalance)?;
    Ok(i128::from(*after) - i128::from(*before))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    MigrationSuccess,
    Failure,
    UnattributedSuccess,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Totals {
    pub successful_net_lamports: i128,
    pub failed_net_lamports: i128,
    pub realized_net_lamports: i128,
    pub successful_migrations: u64,
    pub unresolved_settlements: usize,
    pub per_curve_net_lamports: std::collections::BTreeMap<String, i128>,
}

impl Totals {
    pub fn add(&mut self, curve: &str, net: i128, outcome: Outcome) -> Result<(), AccountingError> {
        let total = self
            .realized_net_lamports
            .checked_add(net)
            .ok_or(AccountingError::Overflow)?;
        let curve_total = self
            .per_curve_net_lamports
            .get(curve)
            .copied()
            .unwrap_or(0)
            .checked_add(net)
            .ok_or(AccountingError::Overflow)?;
        let (success, failed, count) = match outcome {
            Outcome::MigrationSuccess => (
                self.successful_net_lamports
                    .checked_add(net)
                    .ok_or(AccountingError::Overflow)?,
                self.failed_net_lamports,
                self.successful_migrations
                    .checked_add(1)
                    .ok_or(AccountingError::Overflow)?,
            ),
            Outcome::Failure => (
                self.successful_net_lamports,
                self.failed_net_lamports
                    .checked_add(net)
                    .ok_or(AccountingError::Overflow)?,
                self.successful_migrations,
            ),
            Outcome::UnattributedSuccess => (
                self.successful_net_lamports,
                self.failed_net_lamports,
                self.successful_migrations,
            ),
        };
        self.realized_net_lamports = total;
        self.per_curve_net_lamports
            .insert(curve.to_owned(), curve_total);
        self.successful_net_lamports = success;
        self.failed_net_lamports = failed;
        self.successful_migrations = count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_delta_and_validation() {
        assert_eq!(
            payer_delta(&[u64::MAX], &[0], 0).unwrap(),
            -i128::from(u64::MAX)
        );
        assert_eq!(payer_delta(&[3], &[10], 0).unwrap(), 7);
        assert!(payer_delta(&[1], &[], 0).is_err());
        assert!(payer_delta(&[], &[], 0).is_err());
    }
}

/// Fetch the transaction at finalized commitment and compare it to the durable outcome.
/// Missing finalized transaction or balance metadata leaves the attempt unresolved.
pub async fn fetch_finalized_settlement(
    rpc: &solana_rpc_client::nonblocking::rpc_client::RpcClient,
    attempt: &crate::ledger::SignedAttempt,
    success: bool,
    slot: u64,
    timeout: std::time::Duration,
) -> Result<crate::ledger::Settlement, AccountingError> {
    let response = fetch_finalized_transaction(rpc, attempt, timeout)
        .await?
        .ok_or(AccountingError::InvalidEvidence)?;
    settlement_from_finalized(attempt, success, slot, &response)
}

/// Resolve an otherwise unknown outcome from complete finalized transaction evidence.
pub async fn fetch_unobserved_finalized_settlement(
    rpc: &solana_rpc_client::nonblocking::rpc_client::RpcClient,
    attempt: &crate::ledger::SignedAttempt,
    timeout: std::time::Duration,
) -> Result<Option<crate::ledger::Settlement>, AccountingError> {
    let Some(response) = fetch_finalized_transaction(rpc, attempt, timeout).await? else {
        return Ok(None);
    };
    let meta = response
        .transaction
        .meta
        .as_ref()
        .ok_or(AccountingError::InvalidEvidence)?;
    settlement_from_finalized(attempt, meta.err.is_none(), response.slot, &response).map(Some)
}

async fn fetch_finalized_transaction(
    rpc: &solana_rpc_client::nonblocking::rpc_client::RpcClient,
    attempt: &crate::ledger::SignedAttempt,
    timeout: std::time::Duration,
) -> Result<
    Option<solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta>,
    AccountingError,
> {
    use solana_transaction_status_client_types::UiTransactionEncoding;
    use std::str::FromStr;
    let signature = solana_signature::Signature::from_str(&attempt.signature)
        .map_err(|_| AccountingError::InvalidEvidence)?;
    let config = solana_rpc_client_types::config::RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        commitment: Some(solana_commitment_config::CommitmentConfig::finalized()),
        max_supported_transaction_version: Some(1),
        min_context_slot: None,
    };
    tokio::time::timeout(
        timeout,
        rpc.send(
            solana_rpc_client::api::request::RpcRequest::GetTransaction,
            serde_json::json!([signature.to_string(), config]),
        ),
    )
    .await
    .map_err(|_| AccountingError::RpcUnavailable)?
    .map_err(|_| AccountingError::RpcUnavailable)
}

// Validate transaction identity, execution status and complete payer balances.
fn settlement_from_finalized(
    attempt: &crate::ledger::SignedAttempt,
    success: bool,
    slot: u64,
    response: &solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta,
) -> Result<crate::ledger::Settlement, AccountingError> {
    use lastbuy_sdk::{MIGRATE_DISCRIMINATOR, PROGRAM_ID, pda::derive_curve};
    use solana_message::VersionedMessage;
    use std::str::FromStr;

    let invalid = || AccountingError::InvalidEvidence;
    if slot != response.slot {
        return Err(invalid());
    }
    let meta = response.transaction.meta.as_ref().ok_or_else(invalid)?;
    if success != meta.err.is_none() {
        return Err(invalid());
    }
    let transaction = response
        .transaction
        .transaction
        .decode()
        .ok_or_else(invalid)?;
    if !matches!(transaction.message, VersionedMessage::V1(_))
        || transaction.verify_and_hash_message().is_err()
    {
        return Err(invalid());
    }
    let encoded = wincode::serialize(&transaction).map_err(|_| invalid())?;
    if encoded != attempt.signed_bytes
        || transaction
            .signatures
            .first()
            .map(ToString::to_string)
            .as_deref()
            != Some(attempt.signature.as_str())
    {
        return Err(invalid());
    }
    let payer = solana_pubkey::Pubkey::from_str(&attempt.payer).map_err(|_| invalid())?;
    let curve = solana_pubkey::Pubkey::from_str(&attempt.curve).map_err(|_| invalid())?;
    let mint = solana_pubkey::Pubkey::from_str(&attempt.mint).map_err(|_| invalid())?;
    if derive_curve(&mint).0 != curve {
        return Err(invalid());
    }
    let keys = transaction.message.static_account_keys();
    if keys.first() != Some(&payer)
        || meta.pre_balances.len() != keys.len()
        || meta.post_balances.len() != keys.len()
    {
        return Err(invalid());
    }
    let instructions = transaction.message.instructions();
    let mut migrations = instructions.iter().enumerate().filter(|(_, ix)| {
        keys.get(ix.program_id_index as usize) == Some(&PROGRAM_ID)
            && ix.data == MIGRATE_DISCRIMINATOR
            && ix.accounts.first().and_then(|i| keys.get(*i as usize)) == Some(&payer)
            && ix.accounts.get(4).and_then(|i| keys.get(*i as usize)) == Some(&curve)
            && ix.accounts.get(10).and_then(|i| keys.get(*i as usize)) == Some(&mint)
    });
    migrations.next().ok_or_else(invalid)?;
    if migrations.next().is_some() {
        return Err(invalid());
    }
    let (outcome, evidence) = if meta.err.is_some() {
        (
            Outcome::Failure,
            "finalized metadata and matching signed transaction",
        )
    } else {
        (
            Outcome::MigrationSuccess,
            "finalized metadata and matching signed migration",
        )
    };
    Ok(crate::ledger::Settlement {
        signature: attempt.signature.clone(),
        curve: attempt.curve.clone(),
        slot: response.slot,
        fee_lamports: meta.fee,
        pre_balance: meta.pre_balances[0],
        post_balance: meta.post_balances[0],
        net_lamports: payer_delta(&meta.pre_balances, &meta.post_balances, 0)?,
        outcome,
        evidence: evidence.into(),
    })
}

#[cfg(test)]
mod settlement_tests {
    use super::*;
    use crate::{
        ledger::SignedAttempt,
        transaction::{ResourcePolicy, sign_transaction},
    };
    use lastbuy_sdk::{
        instructions::{MigrateInstruction, MigrateInstructionCtx},
        pda::derive_curve,
    };
    use serde_json::{Value, json};
    use solana_keypair::{Keypair, Signer};
    use solana_pubkey::Pubkey;
    use solana_transaction_status_client_types::{
        EncodedConfirmedTransactionWithStatusMeta, TransactionStatus,
    };

    fn fixture() -> (SignedAttempt, Value, Value) {
        let payer = Keypair::new();
        let mint = Pubkey::new_unique();
        let curve = derive_curve(&mint).0;
        let funder = Pubkey::new_unique();
        let ix = MigrateInstruction::new(MigrateInstructionCtx {
            user: payer.pubkey(),
            migration_funder: funder,
            mint,
            raydium_amm_config: Pubkey::new_unique(),
            raydium_fee_account: Pubkey::new_unique(),
            raydium_cpmm_program: Pubkey::new_unique(),
            raydium_lock_program: Pubkey::new_unique(),
        });
        let tx = sign_transaction(
            &[ix],
            &payer,
            solana_hash::Hash::default(),
            ResourcePolicy {
                compute_unit_limit: 100_000,
                loaded_accounts_data_size_limit: 100_000,
                priority_fee_lamports: 1,
                max_priority_fee_lamports: 1,
            },
        )
        .unwrap();
        let keys = tx.message.static_account_keys();
        let bytes = wincode::serialize(&tx).unwrap();
        let attempt = SignedAttempt {
            signature: tx.signatures[0].to_string(),
            curve: curve.to_string(),
            mint: mint.to_string(),
            payer: payer.pubkey().to_string(),
            genesis_hash: "genesis".into(),
            blockhash: solana_hash::Hash::default().to_string(),
            last_valid_block_height: 100,
            signed_bytes: bytes.clone(),
            attempt_id: "id".into(),
            compute_unit_limit: 100_000,
            loaded_accounts_data_size_limit: 100_000,
            priority_fee_lamports: 1,
        };
        let mut pre = vec![0u64; keys.len()];
        let mut post = pre.clone();
        pre[0] = 100;
        post[0] = 107;
        let status = json!({"slot": 42, "confirmations": null, "status": {"Ok": null}, "err": null, "confirmationStatus": "finalized"});
        let response = json!({"slot": 42, "transaction": [bs58::encode(bytes).into_string(), "base58"], "meta": {"err": null, "status": {"Ok": null}, "fee": 5, "preBalances": pre, "postBalances": post, "innerInstructions": null}, "version": 1, "blockTime": null});
        (attempt, status, response)
    }
    fn settle(
        attempt: &SignedAttempt,
        status: &Value,
        response: &Value,
    ) -> Result<crate::ledger::Settlement, AccountingError> {
        let status: TransactionStatus = serde_json::from_value(status.clone()).unwrap();
        let response: EncodedConfirmedTransactionWithStatusMeta =
            serde_json::from_value(response.clone()).unwrap();
        if status.confirmation_status
            != Some(
                solana_transaction_status_client_types::TransactionConfirmationStatus::Finalized,
            )
        {
            return Err(AccountingError::InvalidEvidence);
        }
        settlement_from_finalized(attempt, status.err.is_none(), status.slot, &response)
    }
    #[test]
    fn validates_finalized_success_and_rejects_incomplete_or_changed_evidence() {
        let (attempt, status, response) = fixture();
        let settlement = settle(&attempt, &status, &response).unwrap();
        assert_eq!(settlement.net_lamports, 7);
        assert_eq!(settlement.fee_lamports, 5);
        assert_eq!(settlement.outcome, Outcome::MigrationSuccess);
        let mut missing = response.clone();
        missing["meta"]["innerInstructions"] = Value::Null;
        assert_eq!(settle(&attempt, &status, &missing).unwrap().net_lamports, 7);
        let mut provisional = status.clone();
        provisional["confirmationStatus"] = json!("confirmed");
        assert!(settle(&attempt, &provisional, &response).is_err());
        let mut mismatched = attempt.clone();
        mismatched.signed_bytes[0] ^= 1;
        assert!(settle(&mismatched, &status, &response).is_err());
        let mut wrong_payer = attempt.clone();
        wrong_payer.payer = Pubkey::new_unique().to_string();
        assert!(settle(&wrong_payer, &status, &response).is_err());
        let mut mismatch = status.clone();
        mismatch["err"] = json!("AccountNotFound");
        assert!(settle(&attempt, &mismatch, &response).is_err());
        let mut balances = response.clone();
        balances["meta"]["preBalances"] = json!([]);
        assert!(settle(&attempt, &status, &balances).is_err());
    }
    #[test]
    fn durable_outcome_settles_without_status_history() {
        let (attempt, _, response) = fixture();
        let response: EncodedConfirmedTransactionWithStatusMeta =
            serde_json::from_value(response).unwrap();
        assert!(settlement_from_finalized(&attempt, true, 42, &response).is_ok());
        assert!(settlement_from_finalized(&attempt, false, 42, &response).is_err());
        assert!(settlement_from_finalized(&attempt, true, 43, &response).is_err());
    }
    #[test]
    fn foreign_same_slot_transaction_is_rejected() {
        let (attempt, _, _) = fixture();
        let (_, foreign_status, foreign_response) = fixture();
        assert!(settle(&attempt, &foreign_status, &foreign_response).is_err());
    }
    #[test]
    fn failed_attempt_settles_without_event() {
        let (attempt, mut status, mut response) = fixture();
        status["err"] = json!("AccountNotFound");
        status["status"] = json!({"Err": "AccountNotFound"});
        response["meta"]["err"] = json!("AccountNotFound");
        response["meta"]["status"] = json!({"Err": "AccountNotFound"});
        response["meta"]["innerInstructions"] = Value::Null;
        let settlement = settle(&attempt, &status, &response).unwrap();
        assert_eq!(settlement.outcome, Outcome::Failure);
    }
}
