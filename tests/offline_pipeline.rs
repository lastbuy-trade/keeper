//! Scripted in-memory RPC tests. The sender has no socket or network implementation.
use keeper::{
    coordinator::{Coordinator, CoordinatorError},
    ledger::{ExecutionEvidence, Ledger, Record, Settlement, SignedAttempt},
    pipeline::{PipelineError, ValidatedPolicy, prepare_attempt},
    scheduler::Candidate,
    submission::{
        RecoveryResult, SubmissionError, persist_attempt, persist_validated_attempt,
        recover_attempt,
    },
    transaction::{ResourcePolicy, sign_transaction},
};
use lastbuy_sdk::instructions::{MigrateInstruction, MigrateInstructionCtx};
use serde_json::{Value, json};
use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_pubkey::Pubkey;
use solana_rpc_client::{
    api::{client_error::Result as RpcResult, request::RpcRequest},
    nonblocking::rpc_client::RpcClient,
    rpc_client::RpcClientConfig,
    rpc_sender::{RpcSender, RpcTransportStats},
};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

type Script = Arc<Mutex<VecDeque<(RpcRequest, Value)>>>;
fn epoch_info(height: u64, slot: u64) -> Value {
    json!({"absoluteSlot":slot,"blockHeight":height,"epoch":0,"slotIndex":slot,"slotsInEpoch":432000,"transactionCount":null})
}
struct Sender {
    script: Script,
    expected_bytes: Option<Vec<u8>>,
    finalized_only: bool,
    expected_account_slot: Option<u64>,
    simulation_bytes: Mutex<VecDeque<Vec<u8>>>,
}

fn decode_base64(encoded: &str) -> Vec<u8> {
    let mut output = Vec::new();
    let mut bits = 0u32;
    let mut count = 0;
    for byte in encoded.bytes().take_while(|&byte| byte != b'=') {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("invalid base64"),
        };
        bits = (bits << 6) | u32::from(value);
        count += 6;
        if count >= 8 {
            count -= 8;
            output.push((bits >> count) as u8);
            bits &= (1 << count) - 1;
        }
    }
    output
}
impl RpcSender for Sender {
    fn send<'a, 'b>(
        &'a self,
        request: RpcRequest,
        params: Value,
    ) -> Pin<Box<dyn Future<Output = RpcResult<Value>> + Send + 'b>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            let (expected, response) = self
                .script
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected RPC request");
            assert_eq!(request, expected);
            if self.finalized_only && request == RpcRequest::GetEpochInfo {
                assert_eq!(params[0]["commitment"], "finalized");
            }
            if request == RpcRequest::GetMultipleAccounts
                && let Some(slot) = self.expected_account_slot
            {
                assert_eq!(params[1]["minContextSlot"], slot);
                assert_eq!(params[1]["commitment"], "confirmed");
            }
            if request == RpcRequest::GetSignatureStatuses {
                assert_eq!(params[1]["searchTransactionHistory"], true);
            }
            if request == RpcRequest::GetTransaction {
                assert_eq!(params[1]["commitment"], "finalized");
            }
            if matches!(
                request,
                RpcRequest::SimulateTransaction | RpcRequest::SendTransaction
            ) {
                assert_eq!(params[1]["encoding"], "base64");
                let expected = self
                    .simulation_bytes
                    .lock()
                    .unwrap()
                    .pop_front()
                    .or_else(|| self.expected_bytes.clone())
                    .expect("expected transaction bytes");
                assert_eq!(decode_base64(params[0].as_str().unwrap()), expected);
            }
            if request == RpcRequest::SimulateTransaction {
                assert_eq!(params[1]["sigVerify"], true);
                assert_eq!(params[1]["minContextSlot"], 42);
            }
            if request == RpcRequest::SendTransaction {
                assert_eq!(params[1]["maxRetries"], 0);
            }
            if request == RpcRequest::GetTransaction && response == json!("mock_rpc_error") {
                return Err(solana_rpc_client::api::client_error::ErrorKind::Custom(
                    "mock failure".into(),
                )
                .into());
            }
            Ok(response)
        })
    }
    fn get_transport_stats(&self) -> RpcTransportStats {
        RpcTransportStats::default()
    }
    fn url(&self) -> String {
        "in-memory".into()
    }
}
fn rpc(script: Vec<(RpcRequest, Value)>) -> (RpcClient, Script) {
    rpc_with_bytes(script, None)
}
fn rpc_with_bytes(
    script: Vec<(RpcRequest, Value)>,
    expected_bytes: Option<Vec<u8>>,
) -> (RpcClient, Script) {
    rpc_checked(script, expected_bytes, false, None)
}
fn rpc_checked(
    script: Vec<(RpcRequest, Value)>,
    expected_bytes: Option<Vec<u8>>,
    finalized_only: bool,
    expected_account_slot: Option<u64>,
) -> (RpcClient, Script) {
    let script = Arc::new(Mutex::new(script.into()));
    (
        RpcClient::new_sender(
            Sender {
                script: script.clone(),
                expected_bytes,
                finalized_only,
                expected_account_slot,
                simulation_bytes: Mutex::new(VecDeque::new()),
            },
            RpcClientConfig::default(),
        ),
        script,
    )
}
fn policy() -> ValidatedPolicy {
    ValidatedPolicy {
        resources: ResourcePolicy {
            compute_unit_limit: 100_000,
            loaded_accounts_data_size_limit: 100_000,
            priority_fee_lamports: 1,
            max_priority_fee_lamports: 1,
        },
        min_profit_lamports: 0,
        max_compute_units: 100_000,
        max_loaded_accounts_data_size: 100_000,
        max_total_fee_lamports: 50,
        min_block_heights_remaining: 5,
    }
}
fn evidence() -> ExecutionEvidence {
    ExecutionEvidence {
        min_profit_lamports: 0,
        simulated_delta_lamports: 5,
        policy_compute_unit_limit: 100_000,
        policy_loaded_accounts_data_size_limit: 100_000,
        priority_fee_lamports: 1,
        max_priority_fee_lamports: 1,
        max_compute_units: 100_000,
        max_loaded_accounts_data_size: 100_000,
        max_total_fee_lamports: 50,
        min_block_heights_remaining: 5,
        simulated_units: 80_000,
        simulated_loaded_bytes: 80_000,
        quoted_fee_lamports: 5,
        payer_balance: 110,
        account_slot: 42,
        checked_block_height: 95,
    }
}
fn fixture() -> (Ledger, Keypair, SignedAttempt, std::path::PathBuf) {
    let payer = Keypair::new();
    let mint = Pubkey::new_unique();
    let genesis = Hash::new_unique();
    let hash = Hash::new_unique();
    let ix = MigrateInstruction::new(MigrateInstructionCtx {
        user: payer.pubkey(),
        mint,
        migration_funder: Pubkey::new_unique(),
        raydium_amm_config: Pubkey::new_unique(),
        raydium_fee_account: Pubkey::new_unique(),
        raydium_cpmm_program: Pubkey::new_unique(),
        raydium_lock_program: Pubkey::new_unique(),
    });
    let tx = sign_transaction(&[ix], &payer, hash, policy().resources).unwrap();
    let signature = tx.signatures[0].to_string();
    let attempt = SignedAttempt {
        signature: signature.clone(),
        attempt_id: signature,
        curve: lastbuy_sdk::pda::derive_curve(&mint).0.to_string(),
        mint: mint.to_string(),
        payer: payer.pubkey().to_string(),
        genesis_hash: genesis.to_string(),
        blockhash: hash.to_string(),
        last_valid_block_height: 100,
        signed_bytes: wincode::serialize(&tx).unwrap(),
        compute_unit_limit: 100_000,
        loaded_accounts_data_size_limit: 100_000,
        priority_fee_lamports: 1,
    };
    let path = std::env::temp_dir().join(format!(
        "keeper-offline-{}-{}",
        std::process::id(),
        Pubkey::new_unique()
    ));
    let ledger = Ledger::open(&path, &genesis.to_string(), &payer.pubkey().to_string()).unwrap();
    (ledger, payer, attempt, path)
}
#[tokio::test]
async fn pipeline_rejects_wrong_genesis_before_any_accounts_or_persistence() {
    let (mut ledger, payer, _attempt, path) = fixture();
    let (client, script) = rpc(vec![(
        RpcRequest::GetGenesisHash,
        json!(Hash::new_unique().to_string()),
    )]);
    let result = prepare_attempt(
        &client,
        &mut ledger,
        &payer,
        Pubkey::new_unique(),
        42,
        Some(policy()),
        Duration::from_secs(1),
    )
    .await;
    assert!(matches!(result, Err(PipelineError::WrongCluster)));
    assert!(script.lock().unwrap().is_empty());
    assert!(ledger.replay.attempts.is_empty());
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn reopened_unsent_attempt_is_closed_without_rpc_and_unblocks_lane() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    let (client, script) = rpc(vec![]);
    assert_eq!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            true
        )
        .await
        .unwrap(),
        RecoveryResult::NeverSubmitted
    );
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 0);
    assert!(ledger.replay.never_submitted.contains(&attempt.signature));
    drop(ledger);
    let ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    assert_eq!(ledger.replay.unresolved_attempts().count(), 0);
    assert!(ledger.replay.never_submitted.contains(&attempt.signature));
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn reopened_send_intent_with_missing_history_remains_ambiguous() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    let (client, script) = rpc(vec![
        (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
        (
            RpcRequest::GetSignatureStatuses,
            json!({"context":{"slot":44},"value":[null]}),
        ),
        (RpcRequest::GetEpochInfo, epoch_info(95, 44)),
    ]);
    assert_eq!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            false
        )
        .await
        .unwrap(),
        RecoveryResult::Pending
    );
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 1);
    assert!(!ledger.replay.never_submitted.contains(&attempt.signature));
    drop(ledger);
    let ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 1);
    assert!(ledger.replay.send_intents.contains(&attempt.signature));
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn finalized_status_is_durable_before_accounting_is_requested() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    let (client, script) = rpc(vec![
        (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
        (
            RpcRequest::GetSignatureStatuses,
            json!({"context":{"slot":45},"value":[{
                "slot":44,"confirmations":null,"err":null,
                "confirmationStatus":"finalized","status":{"Ok":null}
            }]}),
        ),
    ]);
    assert_eq!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            false
        )
        .await
        .unwrap(),
        RecoveryResult::Finalized
    );
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 0);
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    assert!(ledger.replay.settlements.is_empty());
    drop(ledger);
    let ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    assert_eq!(
        ledger.replay.finalized_outcomes.get(&attempt.signature),
        Some(&(true, 44))
    );
    assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 0);
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    assert!(ledger.replay.settlements.is_empty());
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn finalized_height_must_exceed_expiry_before_lane_is_released() {
    for status in [None, Some("processed"), Some("confirmed")] {
        for (height, releases) in [(100, false), (101, true)] {
            let (mut ledger, _, attempt, path) = fixture();
            persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
            ledger
                .append(Record::SendIntent {
                    signature: attempt.signature.clone(),
                })
                .unwrap();
            drop(ledger);
            let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
            let before = ledger.replay.totals.clone();
            let signature_status = status.map(|confirmation| {
                json!({
                    "slot":44,"confirmations":1,"err":null,
                    "confirmationStatus":confirmation,"status":{"Ok":null}
                })
            });
            let responses = vec![
                (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
                (
                    RpcRequest::GetSignatureStatuses,
                    json!({"context":{"slot":44},"value":[signature_status]}),
                ),
                (RpcRequest::GetEpochInfo, epoch_info(height, 55)),
            ];
            let (client, script) = rpc_checked(responses, None, true, None);
            assert_eq!(
                recover_attempt(
                    &client,
                    &mut ledger,
                    &attempt.signature,
                    Duration::from_secs(1),
                    false
                )
                .await
                .unwrap(),
                if releases {
                    RecoveryResult::ExpiredUnresolved
                } else {
                    RecoveryResult::Pending
                }
            );
            assert!(script.lock().unwrap().is_empty());
            assert_eq!(
                ledger.replay.inclusion_ambiguous_attempts().count(),
                usize::from(!releases)
            );
            assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
            assert_eq!(ledger.replay.totals, before);
            assert!(ledger.replay.settlements.is_empty());
            drop(ledger);
            let ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
            assert_eq!(
                ledger.replay.expired_unresolved.get(&attempt.signature),
                if releases { Some(&55) } else { None }
            );
            assert_eq!(
                ledger.replay.inclusion_ambiguous_attempts().count(),
                usize::from(!releases)
            );
            assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
            assert_eq!(ledger.replay.totals, before);
            drop(ledger);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[tokio::test]
async fn replacement_rejects_snapshot_before_expiry_finalized_slot() {
    let (mut ledger, payer, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    ledger
        .append(Record::ExpiredUnresolved {
            signature: attempt.signature.clone(),
            finalized_slot: 55,
        })
        .unwrap();
    let (client, script) = rpc_checked(
        vec![
            (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
            (
                RpcRequest::GetMultipleAccounts,
                json!({"context":{"slot":54},"value":[null,null,null]}),
            ),
        ],
        None,
        false,
        Some(55),
    );
    let (_cancel_tx, mut cancelled) = tokio::sync::watch::channel(false);
    let mut coordinator = Coordinator::new(
        &client,
        &mut ledger,
        &payer,
        Some(policy()),
        Duration::from_secs(1),
    )
    .unwrap();
    let result = coordinator
        .step(
            Candidate {
                curve: attempt.curve.parse().unwrap(),
                mint: attempt.mint.parse().unwrap(),
                slot: 42,
                write_version: None,
            },
            &mut cancelled,
        )
        .await;
    assert!(matches!(
        result,
        Err(CoordinatorError::Preparation(PipelineError::StaleContext))
    ));
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.attempts.len(), 1);
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn replayed_success_skips_rpc_for_finalized_and_legacy_settlement_records() {
    for legacy_settlement in [false, true] {
        let (mut ledger, payer, attempt, path) = fixture();
        persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
        if legacy_settlement {
            ledger
                .append(Record::FinalizedSettlement {
                    settlement: Settlement {
                        signature: attempt.signature.clone(),
                        curve: attempt.curve.clone(),
                        slot: 77,
                        fee_lamports: 0,
                        pre_balance: 100,
                        post_balance: 100,
                        net_lamports: 0,
                        outcome: keeper::accounting::Outcome::MigrationSuccess,
                        evidence: "legacy finalized settlement".into(),
                    },
                })
                .unwrap();
        } else {
            ledger
                .append(Record::FinalizedOutcome {
                    signature: attempt.signature.clone(),
                    success: true,
                    slot: 77,
                })
                .unwrap();
        }
        drop(ledger);
        let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
        let (client, script) = rpc(vec![]);
        let (_cancel_tx, mut cancelled) = tokio::sync::watch::channel(false);
        let mut coordinator = Coordinator::new(
            &client,
            &mut ledger,
            &payer,
            Some(policy()),
            Duration::from_secs(1),
        )
        .unwrap();
        let result = coordinator
            .step(
                Candidate {
                    curve: attempt.curve.parse().unwrap(),
                    mint: attempt.mint.parse().unwrap(),
                    slot: 1,
                    write_version: None,
                },
                &mut cancelled,
            )
            .await
            .unwrap();
        assert_eq!(result, keeper::coordinator::StepResult::FinalizedSuccess);
        assert!(script.lock().unwrap().is_empty());
        drop(coordinator);
        drop(ledger);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[tokio::test]
async fn expired_attempt_recovers_finalized_transaction_when_status_is_missing() {
    for status in [None, Some("processed"), Some("confirmed")] {
        for case in ["success", "failure", "missing", "mismatch", "rpc_error"] {
            let (mut ledger, _, attempt, path) = fixture();
            persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
            ledger
                .append(Record::SendIntent {
                    signature: attempt.signature.clone(),
                })
                .unwrap();
            ledger
                .append(Record::ExpiredUnresolved {
                    signature: attempt.signature.clone(),
                    finalized_slot: 55,
                })
                .unwrap();
            drop(ledger);
            let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
            let tx: solana_transaction::versioned::VersionedTransaction =
                wincode::deserialize_exact(&attempt.signed_bytes).unwrap();
            let count = tx.message.static_account_keys().len();
            let pre = vec![100u64; count];
            let mut post = pre.clone();
            post[0] = 107;
            let error = if case == "failure" {
                json!("AccountNotFound")
            } else {
                Value::Null
            };
            let bytes = if case == "mismatch" {
                vec![1, 2, 3]
            } else {
                attempt.signed_bytes.clone()
            };
            let response = if case == "missing" {
                Value::Null
            } else if case == "rpc_error" {
                json!("mock_rpc_error")
            } else {
                json!({
                    "slot": 42, "transaction": [bs58::encode(bytes).into_string(), "base58"],
                    "meta": {"err":error,"status":if case == "failure" { json!({"Err":"AccountNotFound"}) } else { json!({"Ok":null}) },
                        "fee":5,"preBalances":pre,"postBalances":post,"innerInstructions":null},
                    "version":1,"blockTime":null
                })
            };
            let signature_status = status.map(|confirmation| {
                json!({
                    "slot":44,"confirmations":1,"err":null,
                    "confirmationStatus":confirmation,"status":{"Ok":null}
                })
            });
            let (client, script) = rpc(vec![
                (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
                (
                    RpcRequest::GetSignatureStatuses,
                    json!({"context":{"slot":60},"value":[signature_status]}),
                ),
                (RpcRequest::GetTransaction, response),
            ]);
            let result = recover_attempt(
                &client,
                &mut ledger,
                &attempt.signature,
                Duration::from_secs(1),
                false,
            )
            .await;
            let settled = case == "success" || case == "failure";
            match case {
                "success" | "failure" => assert!(
                    matches!(result, Ok(RecoveryResult::Settled)),
                    "{case} {status:?}: {result:?}"
                ),
                "missing" => assert!(
                    matches!(result, Ok(RecoveryResult::Pending)),
                    "{case} {status:?}: {result:?}"
                ),
                "mismatch" => assert!(
                    matches!(
                        result,
                        Err(SubmissionError::Accounting(
                            keeper::accounting::AccountingError::InvalidEvidence
                        ))
                    ),
                    "{case} {status:?}: {result:?}"
                ),
                "rpc_error" => assert!(
                    matches!(
                        result,
                        Err(SubmissionError::Accounting(
                            keeper::accounting::AccountingError::RpcUnavailable
                        ))
                    ),
                    "{case} {status:?}: {result:?}"
                ),
                _ => unreachable!(),
            }
            assert!(script.lock().unwrap().is_empty());
            assert_eq!(
                ledger.replay.settlements.contains_key(&attempt.signature),
                settled,
                "{case}"
            );
            assert_eq!(
                ledger
                    .replay
                    .finalized_outcomes
                    .get(&attempt.signature)
                    .copied(),
                if settled {
                    Some((case == "success", 42))
                } else {
                    None
                },
                "{case}"
            );
            assert_eq!(
                ledger.replay.totals.realized_net_lamports,
                if settled { 7 } else { 0 },
                "{case}"
            );
            drop(ledger);
            let ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
            assert_eq!(
                ledger.replay.settlements.contains_key(&attempt.signature),
                settled,
                "{case}"
            );
            assert_eq!(
                ledger.replay.unresolved_attempts().count(),
                usize::from(!settled),
                "{case}"
            );
            drop(ledger);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
#[tokio::test]
async fn recovery_missing_history_never_infers_expiry_or_sends() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    let (client, script) = rpc(vec![
        (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
        (
            RpcRequest::GetSignatureStatuses,
            json!({"context":{"slot":44},"value":[null]}),
        ),
        (RpcRequest::GetEpochInfo, epoch_info(95, 44)),
    ]);
    let result = recover_attempt(
        &client,
        &mut ledger,
        &attempt.signature,
        Duration::from_secs(1),
        false,
    )
    .await
    .unwrap();
    assert_eq!(result, RecoveryResult::Pending);
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    assert!(ledger.replay.expired.is_empty());
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn recovery_wrong_genesis_fails_before_status_lookup() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_attempt(&mut ledger, attempt.clone()).unwrap();
    let (client, script) = rpc(vec![(
        RpcRequest::GetGenesisHash,
        json!(Hash::new_unique().to_string()),
    )]);
    let result = recover_attempt(
        &client,
        &mut ledger,
        &attempt.signature,
        Duration::from_secs(1),
        true,
    )
    .await;
    assert!(matches!(result, Err(SubmissionError::WrongCluster)));
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn recovery_never_retransmits_legacy_attempt_without_durable_evidence() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_attempt(&mut ledger, attempt.clone()).unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    let (client, script) = rpc(vec![
        (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
        (
            RpcRequest::GetSignatureStatuses,
            json!({"context":{"slot":44},"value":[null]}),
        ),
        (RpcRequest::GetEpochInfo, epoch_info(95, 44)),
    ]);
    assert_eq!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            true
        )
        .await
        .unwrap(),
        RecoveryResult::Pending
    );
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn legacy_validated_attempt_without_intent_remains_ambiguous() {
    let (mut ledger, _, attempt, path) = fixture();
    ledger
        .append(Record::PolicyValidatedAttempt {
            attempt: attempt.clone(),
            evidence: evidence(),
        })
        .unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    let (client, script) = rpc(vec![
        (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
        (
            RpcRequest::GetSignatureStatuses,
            json!({"context":{"slot":44},"value":[null]}),
        ),
        (RpcRequest::GetEpochInfo, epoch_info(95, 44)),
    ]);
    assert_eq!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            false
        )
        .await
        .unwrap(),
        RecoveryResult::Pending
    );
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 1);
    assert!(!ledger.replay.never_submitted.contains(&attempt.signature));
    drop(ledger);
    let ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 1);
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

fn recovery_script(
    attempt: &SignedAttempt,
    payer_delta: i64,
    height: u64,
) -> Vec<(RpcRequest, Value)> {
    let tx: solana_transaction::versioned::VersionedTransaction =
        wincode::deserialize_exact(&attempt.signed_bytes).unwrap();
    let count = tx.message.static_account_keys().len();
    let pre = vec![100u64; count];
    let mut post = pre.clone();
    post[0] = (100 + payer_delta) as u64;
    vec![
        (RpcRequest::GetGenesisHash, json!(attempt.genesis_hash)),
        (
            RpcRequest::GetSignatureStatuses,
            json!({"context":{"slot":44},"value":[null]}),
        ),
        (RpcRequest::GetEpochInfo, epoch_info(95, 44)),
        (
            RpcRequest::GetFeeForMessage,
            json!({"context":{"slot":44},"value":5}),
        ),
        (
            RpcRequest::SimulateTransaction,
            json!({"context":{"slot":44},"value":{
                "err":null,"logs":[],"unitsConsumed":80000,"loadedAccountsDataSize":80000,
                "preBalances":pre,"postBalances":post
            }}),
        ),
        (RpcRequest::GetBlockHeight, json!(height)),
    ]
}

#[tokio::test]
async fn reopened_validated_attempt_retransmits_exact_durable_bytes_after_fresh_simulation() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, &attempt.genesis_hash, &attempt.payer).unwrap();
    let mut responses = recovery_script(&attempt, 5, 95);
    responses.push((RpcRequest::SendTransaction, json!(attempt.signature)));
    let (client, script) = rpc_with_bytes(responses, Some(attempt.signed_bytes.clone()));
    assert_eq!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            true
        )
        .await
        .unwrap(),
        RecoveryResult::Retransmitted
    );
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn unprofitable_fresh_simulation_does_not_send() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    let mut responses = recovery_script(&attempt, -1, 95);
    responses.pop();
    let (client, script) = rpc_with_bytes(responses, Some(attempt.signed_bytes.clone()));
    assert!(matches!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            true
        )
        .await,
        Err(SubmissionError::Simulation(
            keeper::executor::SimulationError::BelowMinimum { .. }
        ))
    ));
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn rejected_fresh_simulation_does_not_send() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    let mut responses = recovery_script(&attempt, 5, 95);
    responses.pop();
    responses.last_mut().unwrap().1["value"]["err"] = json!("AccountNotFound");
    let (client, script) = rpc_with_bytes(responses, Some(attempt.signed_bytes.clone()));
    assert!(matches!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            true
        )
        .await,
        Err(SubmissionError::Simulation(
            keeper::executor::SimulationError::Rejected { .. }
        ))
    ));
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn expired_attempt_remains_pending_without_retransmission() {
    let (mut ledger, _, attempt, path) = fixture();
    persist_validated_attempt(&mut ledger, attempt.clone(), evidence()).unwrap();
    ledger
        .append(Record::SendIntent {
            signature: attempt.signature.clone(),
        })
        .unwrap();
    let (client, script) = rpc_with_bytes(
        recovery_script(&attempt, 5, 101),
        Some(attempt.signed_bytes.clone()),
    );
    assert_eq!(
        recover_attempt(
            &client,
            &mut ledger,
            &attempt.signature,
            Duration::from_secs(1),
            true
        )
        .await
        .unwrap(),
        RecoveryResult::Pending
    );
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
    assert!(ledger.replay.expired.is_empty());
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn preparation_persists_exact_simulated_v1_bytes_without_sending() {
    use borsh::BorshSerialize;
    use keeper::transaction::build_instruction_plan;
    use lastbuy_sdk::{
        accounts::{CurveState, GlobalConfig},
        bonding_curve::constants::{
            CURVE_STATE_DISCRIMINATOR, GLOBAL_CONFIG_DISCRIMINATOR, PROGRAM_ID,
        },
        types::{AdminConfig, CurveStatus, FeeConfig, ProtocolConfig, RaydiumConfig},
    };
    fn base64(bytes: &[u8]) -> String {
        const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let n = (u32::from(chunk[0]) << 16)
                | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                | u32::from(*chunk.get(2).unwrap_or(&0));
            out.push(TABLE[((n >> 18) & 63) as usize] as char);
            out.push(TABLE[((n >> 12) & 63) as usize] as char);
            out.push(if chunk.len() > 1 {
                TABLE[((n >> 6) & 63) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                TABLE[(n & 63) as usize] as char
            } else {
                '='
            });
        }
        out
    }
    fn account(owner: Pubkey, data: Vec<u8>, lamports: u64) -> Value {
        json!({"lamports":lamports,"owner":owner.to_string(),"data":[base64(&data),"base64"],"executable":false,"rentEpoch":0,"space":data.len()})
    }
    let (mut ledger, payer, fixture_attempt, path) = fixture();
    let mint = Pubkey::new_unique();
    let state = CurveState {
        bump: 0,
        status: CurveStatus::Graduated,
        migration_funder: Pubkey::new_unique(),
        creator: Pubkey::new_unique(),
        mint,
        last_buyer: Pubkey::default(),
        last_buy_sol: 0,
        last_buy_ts: 0,
        max_hold_duration_secs: 0,
        max_duration_holder: Pubkey::default(),
        real_reserves_sol: 0,
        real_reserves_tokens: 0,
        virtual_reserves_sol: 0,
        virtual_reserves_tokens: 0,
        last_buy_timer_secs_snapshot: 0,
        migration_threshold_sol_snapshot: 0,
    };
    let config = GlobalConfig {
        protocol_version: 1,
        bump: 0,
        fees: FeeConfig {
            protocol_curve_fee_bps: 0,
            protocol_claim_creator_fee_bps: 0,
            creator_fee_bps: 0,
            last_buyer_fee_bps: 0,
            fee_recipient_1: Pubkey::new_unique(),
            fee_recipient_2: Pubkey::new_unique(),
        },
        protocol: ProtocolConfig {
            last_buy_timer_secs: 0,
            migration_threshold_sol: 0,
            protocol_authority: Pubkey::default(),
        },
        raydium: RaydiumConfig {
            raydium_cpmm_program: Pubkey::new_unique(),
            raydium_lock_program: Pubkey::new_unique(),
            raydium_amm_config: Pubkey::new_unique(),
            raydium_fee_account: Pubkey::new_unique(),
            raydium_cpmm_migration_coverage_sol: 0,
        },
        admin: AdminConfig::default(),
    };
    let curve = lastbuy_sdk::pda::derive_curve(&mint).0;
    let mut curve_data = CURVE_STATE_DISCRIMINATOR.to_vec();
    state.serialize(&mut curve_data).unwrap();
    let mut config_data = GLOBAL_CONFIG_DISCRIMINATOR.to_vec();
    config.serialize(&mut config_data).unwrap();
    let hash = Hash::new_unique();
    let instructions = build_instruction_plan(payer.pubkey(), curve, &state, &config).unwrap();
    let probe = sign_transaction(&instructions, &payer, hash, policy().resources).unwrap();
    let final_resources = ResourcePolicy {
        compute_unit_limit: 92_000,
        loaded_accounts_data_size_limit: 92_000,
        ..policy().resources
    };
    let final_tx = sign_transaction(&instructions, &payer, hash, final_resources).unwrap();
    let probe_bytes = wincode::serialize(&probe).unwrap();
    let final_bytes = wincode::serialize(&final_tx).unwrap();
    assert_ne!(probe_bytes, final_bytes);
    let count = final_tx.message.static_account_keys().len();
    let pre = vec![100u64; count];
    let mut post = pre.clone();
    post[0] = 105;
    let simulation = json!({"context":{"slot":42},"value":{"err":null,"logs":[],"unitsConsumed":80000,"loadedAccountsDataSize":80000,"preBalances":pre,"postBalances":post}});
    let responses = vec![
        (
            RpcRequest::GetGenesisHash,
            json!(fixture_attempt.genesis_hash),
        ),
        (
            RpcRequest::GetMultipleAccounts,
            json!({"context":{"slot":42},"value":[account(PROGRAM_ID, curve_data, 1),account(PROGRAM_ID, config_data, 1),account(Pubkey::default(), vec![], 110)]}),
        ),
        (
            RpcRequest::GetLatestBlockhash,
            json!({"context":{"slot":42},"value":{"blockhash":hash.to_string(),"lastValidBlockHeight":100}}),
        ),
        (RpcRequest::SimulateTransaction, simulation.clone()),
        (
            RpcRequest::GetFeeForMessage,
            json!({"context":{"slot":42},"value":5}),
        ),
        (RpcRequest::SimulateTransaction, simulation),
        (RpcRequest::GetBlockHeight, json!(95)),
    ];
    let script: Script = Arc::new(Mutex::new(responses.into()));
    let client = RpcClient::new_sender(
        Sender {
            script: script.clone(),
            expected_bytes: None,
            finalized_only: false,
            expected_account_slot: Some(42),
            simulation_bytes: Mutex::new(vec![probe_bytes, final_bytes.clone()].into()),
        },
        RpcClientConfig::default(),
    );
    let prepared = prepare_attempt(
        &client,
        &mut ledger,
        &payer,
        curve,
        42,
        Some(policy()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(script.lock().unwrap().is_empty());
    assert_eq!(prepared.account_slot, 42);
    assert_eq!(prepared.estimate.payer_delta_lamports, 5);
    assert_eq!(prepared.attempt.signed_bytes, final_bytes);
    assert_eq!(prepared.attempt.compute_unit_limit, 92_000);
    assert_eq!(prepared.attempt.loaded_accounts_data_size_limit, 92_000);
    assert!(matches!(
        final_tx.message,
        solana_message::VersionedMessage::V1(_)
    ));
    assert!(ledger.replay.send_intents.is_empty());
    let signature = prepared.attempt.signature.clone();
    drop(ledger);
    let ledger =
        Ledger::open(&path, &fixture_attempt.genesis_hash, &fixture_attempt.payer).unwrap();
    assert_eq!(ledger.replay.attempts[&signature].signed_bytes, final_bytes);
    let evidence = &ledger.replay.validated_attempts[&signature];
    assert_eq!(
        (
            evidence.simulated_units,
            evidence.simulated_loaded_bytes,
            evidence.simulated_delta_lamports
        ),
        (80_000, 80_000, 5)
    );
    assert_eq!(
        (
            evidence.quoted_fee_lamports,
            evidence.checked_block_height,
            evidence.account_slot
        ),
        (5, 95, 42)
    );
    assert!(ledger.replay.send_intents.is_empty());
    drop(ledger);
    std::fs::remove_dir_all(path).unwrap();
}
