use crate::accounting::{self, AccountingError, Outcome, Totals};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;

const SCHEMA: u32 = 1;
#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("ledger I/O: {0}")]
    Io(#[from] io::Error),
    #[error("invalid ledger JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid ledger: {0}")]
    Invalid(String),
    #[error("ledger accounting: {0}")]
    Accounting(#[from] AccountingError),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedAttempt {
    pub signature: String,
    pub curve: String,
    pub mint: String,
    pub payer: String,
    pub genesis_hash: String,
    pub blockhash: String,
    pub last_valid_block_height: u64,
    pub signed_bytes: Vec<u8>,
    pub attempt_id: String,
    pub compute_unit_limit: u64,
    pub loaded_accounts_data_size_limit: u64,
    pub priority_fee_lamports: u64,
}
/// Measurements and limits checked for the exact signed attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionEvidence {
    #[serde(with = "i128_string")]
    pub min_profit_lamports: i128,
    #[serde(with = "i128_string")]
    pub simulated_delta_lamports: i128,
    pub policy_compute_unit_limit: u32,
    pub policy_loaded_accounts_data_size_limit: u32,
    pub priority_fee_lamports: u64,
    pub max_priority_fee_lamports: u64,
    pub max_compute_units: u64,
    pub max_loaded_accounts_data_size: u32,
    pub max_total_fee_lamports: u64,
    pub min_block_heights_remaining: u64,
    pub simulated_units: u64,
    pub simulated_loaded_bytes: u32,
    pub quoted_fee_lamports: u64,
    pub payer_balance: u64,
    pub account_slot: u64,
    pub checked_block_height: u64,
}
impl ExecutionEvidence {
    fn valid_for(&self, a: &SignedAttempt) -> bool {
        self.policy_compute_unit_limit > 0
            && self.policy_loaded_accounts_data_size_limit > 0
            && self.max_compute_units > 0
            && self.max_compute_units <= u64::from(u32::MAX)
            && u64::from(self.policy_compute_unit_limit) <= self.max_compute_units
            && self.policy_loaded_accounts_data_size_limit <= self.max_loaded_accounts_data_size
            && self.min_block_heights_remaining > 0
            && self.priority_fee_lamports <= self.max_priority_fee_lamports
            && self.priority_fee_lamports == a.priority_fee_lamports
            && self.quoted_fee_lamports >= self.priority_fee_lamports
            && self.quoted_fee_lamports <= self.max_total_fee_lamports
            && self.simulated_delta_lamports >= self.min_profit_lamports
            && self.simulated_units > 0
            && self.simulated_units <= a.compute_unit_limit
            && a.compute_unit_limit <= self.max_compute_units
            && self.simulated_loaded_bytes > 0
            && u64::from(self.simulated_loaded_bytes) <= a.loaded_accounts_data_size_limit
            && a.loaded_accounts_data_size_limit <= u64::from(self.max_loaded_accounts_data_size)
            && a.last_valid_block_height
                .checked_sub(self.checked_block_height)
                .is_some_and(|remaining| remaining >= self.min_block_heights_remaining)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Settlement {
    pub signature: String,
    pub curve: String,
    pub slot: u64,
    pub fee_lamports: u64,
    pub pre_balance: u64,
    pub post_balance: u64,
    #[serde(with = "i128_string")]
    pub net_lamports: i128,
    pub outcome: Outcome,
    pub evidence: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Record {
    Header {
        schema: u32,
        genesis_hash: String,
        payer: String,
    },
    SignedAttempt {
        attempt: SignedAttempt,
    },
    PolicyValidatedAttempt {
        attempt: SignedAttempt,
        evidence: ExecutionEvidence,
    },
    /// This attempt is governed by the durable send-intent protocol.
    PolicyValidatedAttemptV2 {
        attempt: SignedAttempt,
        evidence: ExecutionEvidence,
    },
    SendIntent {
        signature: String,
    },
    NeverSubmitted {
        signature: String,
    },
    /// Finalized height passed expiry, but inclusion has not been determined.
    ExpiredUnresolved {
        signature: String,
        finalized_slot: u64,
    },
    SubmissionObservation {
        signature: String,
        observation: String,
    },
    FinalizedOutcome {
        signature: String,
        success: bool,
        slot: u64,
    },
    FinalizedSettlement {
        settlement: Settlement,
    },
    Expired {
        signature: String,
    },
    CurveExhausted {
        curve: String,
        failures: u8,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Line {
    sequence: u64,
    #[serde(flatten)]
    record: Record,
}
#[derive(Debug, Default)]
pub struct Replay {
    pub genesis_hash: String,
    pub payer: String,
    pub last_sequence: u64,
    pub last_settled_sequence: u64,
    pub attempts: BTreeMap<String, SignedAttempt>,
    pub validated_attempts: BTreeMap<String, ExecutionEvidence>,
    pub settlements: BTreeMap<String, Settlement>,
    pub finalized_outcomes: BTreeMap<String, (bool, u64)>,
    pub expired: BTreeMap<String, bool>,
    pub intent_protocol: BTreeSet<String>,
    pub send_intents: BTreeSet<String>,
    pub never_submitted: BTreeSet<String>,
    pub expired_unresolved: BTreeMap<String, u64>,
    pub totals: Totals,
    pub exhausted_curves: BTreeSet<String>,
}
impl Replay {
    /// Curves with a successful finalized outcome, including legacy settlement-only records.
    pub fn successful_curves(&self) -> impl Iterator<Item = &str> {
        self.attempts.values().filter_map(|attempt| {
            self.signature_succeeded(&attempt.signature)
                .then_some(attempt.curve.as_str())
        })
    }

    pub fn signature_succeeded(&self, signature: &str) -> bool {
        self.settlements
            .get(signature)
            .is_some_and(|settlement| settlement.outcome != Outcome::Failure)
            || self
                .finalized_outcomes
                .get(signature)
                .is_some_and(|(success, _)| *success)
    }

    pub fn unresolved_attempts(&self) -> impl Iterator<Item = &SignedAttempt> {
        self.attempts.values().filter(|a| {
            !self.settlements.contains_key(&a.signature)
                && !self.expired.contains_key(&a.signature)
                && !self.never_submitted.contains(&a.signature)
        })
    }
    /// Attempts whose on-chain inclusion remains ambiguous and still block the submission lane.
    pub fn inclusion_ambiguous_attempts(&self) -> impl Iterator<Item = &SignedAttempt> {
        self.unresolved_attempts().filter(|attempt| {
            !self.finalized_outcomes.contains_key(&attempt.signature)
                && !self.expired_unresolved.contains_key(&attempt.signature)
        })
    }
    fn apply(&mut self, line: Line) -> Result<(), LedgerError> {
        if line.sequence
            != self
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| LedgerError::Invalid("sequence overflow".into()))?
        {
            return Err(LedgerError::Invalid("non-contiguous sequence".into()));
        }
        let intent_protocol_record =
            matches!(&line.record, Record::PolicyValidatedAttemptV2 { .. });
        match line.record {
            Record::Header {
                schema,
                genesis_hash,
                payer,
            } if line.sequence == 1
                && schema == SCHEMA
                && !genesis_hash.is_empty()
                && !payer.is_empty() =>
            {
                self.genesis_hash = genesis_hash;
                self.payer = payer;
            }
            Record::Header { .. } => {
                return Err(LedgerError::Invalid(
                    "invalid header or unsupported schema".into(),
                ));
            }
            Record::SignedAttempt { attempt } => {
                if self.genesis_hash.is_empty()
                    || attempt.genesis_hash != self.genesis_hash
                    || attempt.payer != self.payer
                    || attempt.signature.is_empty()
                    || attempt.curve.is_empty()
                    || attempt.signed_bytes.is_empty()
                {
                    return Err(LedgerError::Invalid(
                        "invalid attempt identity or bytes".into(),
                    ));
                }
                if let Some(previous) = self.attempts.get(&attempt.signature) {
                    if previous != &attempt {
                        return Err(LedgerError::Invalid("conflicting signed attempt".into()));
                    }
                } else {
                    self.attempts.insert(attempt.signature.clone(), attempt);
                }
            }
            Record::PolicyValidatedAttemptV2 { ref attempt, .. }
                if self.attempts.contains_key(&attempt.signature) =>
            {
                return Err(LedgerError::Invalid(
                    "attempt protocol cannot be upgraded or duplicated".into(),
                ));
            }
            Record::PolicyValidatedAttempt { attempt, evidence }
            | Record::PolicyValidatedAttemptV2 { attempt, evidence } => {
                if !evidence.valid_for(&attempt)
                    || self.genesis_hash.is_empty()
                    || attempt.genesis_hash != self.genesis_hash
                    || attempt.payer != self.payer
                    || attempt.signature.is_empty()
                    || attempt.curve.is_empty()
                    || attempt.signed_bytes.is_empty()
                {
                    return Err(LedgerError::Invalid(
                        "invalid validated attempt or evidence".into(),
                    ));
                }
                if self
                    .attempts
                    .get(&attempt.signature)
                    .is_some_and(|prior| prior != &attempt)
                    || self
                        .validated_attempts
                        .get(&attempt.signature)
                        .is_some_and(|prior| prior != &evidence)
                {
                    return Err(LedgerError::Invalid("conflicting validated attempt".into()));
                }
                let signature = attempt.signature.clone();
                self.attempts.insert(signature.clone(), attempt);
                self.validated_attempts.insert(signature.clone(), evidence);
                if intent_protocol_record {
                    self.intent_protocol.insert(signature);
                }
            }
            Record::SendIntent { signature } => {
                if !self.intent_protocol.contains(&signature)
                    || self.never_submitted.contains(&signature)
                    || self.settlements.contains_key(&signature)
                    || self.finalized_outcomes.contains_key(&signature)
                    || self.expired.contains_key(&signature)
                {
                    return Err(LedgerError::Invalid("invalid send intent".into()));
                }
                self.send_intents.insert(signature);
            }
            Record::NeverSubmitted { signature } => {
                if !self.intent_protocol.contains(&signature)
                    || self.send_intents.contains(&signature)
                    || self.settlements.contains_key(&signature)
                    || self.finalized_outcomes.contains_key(&signature)
                    || self.expired.contains_key(&signature)
                {
                    return Err(LedgerError::Invalid(
                        "invalid never-submitted resolution".into(),
                    ));
                }
                self.never_submitted.insert(signature);
            }
            Record::ExpiredUnresolved {
                signature,
                finalized_slot,
            } => {
                if !self.attempts.contains_key(&signature)
                    || self.never_submitted.contains(&signature)
                    || self.settlements.contains_key(&signature)
                    || self.finalized_outcomes.contains_key(&signature)
                    || self.expired.contains_key(&signature)
                    || self.expired_unresolved.contains_key(&signature)
                {
                    return Err(LedgerError::Invalid("invalid unresolved expiration".into()));
                }
                self.expired_unresolved.insert(signature, finalized_slot);
            }
            Record::SubmissionObservation { signature, .. } => {
                if !self.attempts.contains_key(&signature) {
                    return Err(LedgerError::Invalid(
                        "observation for unknown signature".into(),
                    ));
                }
            }
            Record::Expired { signature } => {
                if !self.attempts.contains_key(&signature)
                    || self.settlements.contains_key(&signature)
                    || self.finalized_outcomes.contains_key(&signature)
                    || self.never_submitted.contains(&signature)
                {
                    return Err(LedgerError::Invalid("invalid expiration".into()));
                }
                self.expired.insert(signature, true);
            }
            Record::CurveExhausted { curve, failures } => {
                if curve.is_empty() || failures == 0 {
                    return Err(LedgerError::Invalid(
                        "invalid exhausted curve record".into(),
                    ));
                }
                self.exhausted_curves.insert(curve);
            }
            Record::FinalizedOutcome {
                signature,
                success,
                slot,
            } => {
                if !self.attempts.contains_key(&signature)
                    || self.expired.contains_key(&signature)
                    || self.never_submitted.contains(&signature)
                {
                    return Err(LedgerError::Invalid("invalid finalized outcome".into()));
                }
                if let Some(settlement) = self.settlements.get(&signature)
                    && (settlement.slot != slot
                        || (settlement.outcome != Outcome::Failure) != success)
                {
                    return Err(LedgerError::Invalid(
                        "outcome conflicts with settlement".into(),
                    ));
                }
                if let Some(previous) = self.finalized_outcomes.get(&signature) {
                    if *previous != (success, slot) {
                        return Err(LedgerError::Invalid("conflicting finalized outcome".into()));
                    }
                } else {
                    self.finalized_outcomes.insert(signature, (success, slot));
                }
            }
            Record::FinalizedSettlement { settlement } => {
                if self.expired.contains_key(&settlement.signature)
                    || self.never_submitted.contains(&settlement.signature)
                {
                    return Err(LedgerError::Invalid(
                        "settlement for expired attempt".into(),
                    ));
                }
                let attempt = self.attempts.get(&settlement.signature).ok_or_else(|| {
                    LedgerError::Invalid("settlement for unknown signature".into())
                })?;
                if let Some((success, slot)) = self.finalized_outcomes.get(&settlement.signature)
                    && (*slot != settlement.slot
                        || *success != (settlement.outcome != Outcome::Failure))
                {
                    return Err(LedgerError::Invalid(
                        "settlement conflicts with finalized outcome".into(),
                    ));
                }
                if settlement.curve != attempt.curve
                    || settlement.evidence.is_empty()
                    || settlement.net_lamports
                        != accounting::payer_delta(
                            &[settlement.pre_balance],
                            &[settlement.post_balance],
                            0,
                        )?
                {
                    return Err(LedgerError::Invalid("invalid settlement".into()));
                }
                if let Some(previous) = self.settlements.get(&settlement.signature) {
                    if previous != &settlement {
                        return Err(LedgerError::Invalid("conflicting settlement".into()));
                    }
                } else {
                    self.totals.add(
                        &settlement.curve,
                        settlement.net_lamports,
                        settlement.outcome,
                    )?;
                    self.settlements
                        .insert(settlement.signature.clone(), settlement);
                }
                self.last_settled_sequence = line.sequence;
            }
        }
        self.last_sequence = line.sequence;
        self.totals.unresolved_settlements = self
            .attempts
            .len()
            .saturating_sub(self.settlements.len())
            .saturating_sub(self.expired.len())
            .saturating_sub(self.never_submitted.len());
        Ok(())
    }
}
fn read_prefix(file: &mut File) -> Result<(Replay, u64, Vec<u8>), LedgerError> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let mut replay = Replay::default();
    let mut offset = 0u64;
    loop {
        let mut line = Vec::new();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok((replay, offset, Vec::new()));
        }
        if line.last() != Some(&b'\n') {
            return Ok((replay, offset, line));
        }
        let parsed: Line = serde_json::from_slice(&line)?;
        replay.apply(parsed)?;
        offset = offset
            .checked_add(line.len() as u64)
            .ok_or_else(|| LedgerError::Invalid("ledger offset overflow".into()))?;
    }
}
/// Read a consistent newline-terminated prefix without taking the writer lock.
pub fn read_summary(path: &Path) -> Result<Replay, LedgerError> {
    let mut file = File::open(path)?;
    let (replay, _, _) = read_prefix(&mut file)?;
    if replay.last_sequence == 0 {
        return Err(LedgerError::Invalid("missing ledger header".into()));
    }
    Ok(replay)
}

/// Remove the active ledger while holding the writer lock. A missing state directory or ledger
/// is an idempotent no-op; forensic tail backups and other files are left untouched.
pub fn reset(state_dir: &Path) -> Result<bool, LedgerError> {
    match fs::metadata(state_dir) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(LedgerError::Invalid(
                "state directory path is not a directory".into(),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    }
    let _lock = acquire_lock(state_dir)?;
    let ledger_path = state_dir.join("ledger.jsonl");
    match fs::remove_file(&ledger_path) {
        Ok(()) => {
            File::open(state_dir)?.sync_all()?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn acquire_lock(state_dir: &Path) -> Result<File, LedgerError> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(state_dir.join("ledger.lock"))?;
    lock.try_lock()
        .map_err(|e| LedgerError::Invalid(format!("cannot acquire exclusive ledger lock: {e}")))?;
    Ok(lock)
}

pub struct Ledger {
    file: File,
    _lock: File,
    pub replay: Replay,
    poisoned: bool,
}
impl Ledger {
    pub fn open(state_dir: &Path, genesis_hash: &str, payer: &str) -> Result<Self, LedgerError> {
        fs::create_dir_all(state_dir)?;
        let lock = acquire_lock(state_dir)?;
        let path = state_dir.join("ledger.jsonl");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let (replay, offset, tail) = read_prefix(&mut file)?;
        if !tail.is_empty() {
            // Create a unique evidence file before truncating. Never overwrite a prior crash tail.
            let mut i = 0u64;
            loop {
                let evidence: PathBuf = state_dir.join(format!("ledger.corrupt-tail.{i}"));
                match OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(evidence)
                {
                    Ok(mut backup) => {
                        backup.write_all(&tail)?;
                        backup.sync_all()?;
                        File::open(state_dir)?.sync_all()?;
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                        i = i.checked_add(1).ok_or_else(|| {
                            LedgerError::Invalid("tail backup counter overflow".into())
                        })?;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            file.set_len(offset)?;
            file.sync_all()?;
        }
        if replay.last_sequence > 0
            && (replay.genesis_hash != genesis_hash || replay.payer != payer)
        {
            return Err(LedgerError::Invalid(
                "ledger identity does not match genesis hash and payer".into(),
            ));
        }
        file.seek(SeekFrom::End(0))?;
        let mut ledger = Self {
            file,
            _lock: lock,
            replay,
            poisoned: false,
        };
        if ledger.replay.last_sequence == 0 {
            ledger.append(Record::Header {
                schema: SCHEMA,
                genesis_hash: genesis_hash.into(),
                payer: payer.into(),
            })?;
            File::open(state_dir)?.sync_all()?;
        }
        Ok(ledger)
    }
    /// Validates and synchronizes each complete record before returning.
    pub fn append(&mut self, record: Record) -> Result<(), LedgerError> {
        if self.poisoned {
            return Err(LedgerError::Invalid(
                "ledger write failed; reopen and recover before writing".into(),
            ));
        }
        let sequence = self
            .replay
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| LedgerError::Invalid("sequence overflow".into()))?;
        let line = Line { sequence, record };
        // Replay the prospective record against an independent state before writing it.
        let mut copy = Replay {
            genesis_hash: self.replay.genesis_hash.clone(),
            payer: self.replay.payer.clone(),
            last_sequence: self.replay.last_sequence,
            last_settled_sequence: self.replay.last_settled_sequence,
            attempts: self.replay.attempts.clone(),
            validated_attempts: self.replay.validated_attempts.clone(),
            settlements: self.replay.settlements.clone(),
            finalized_outcomes: self.replay.finalized_outcomes.clone(),
            expired: self.replay.expired.clone(),
            intent_protocol: self.replay.intent_protocol.clone(),
            send_intents: self.replay.send_intents.clone(),
            never_submitted: self.replay.never_submitted.clone(),
            expired_unresolved: self.replay.expired_unresolved.clone(),
            totals: self.replay.totals.clone(),
            exhausted_curves: self.replay.exhausted_curves.clone(),
        };
        copy.apply(line.clone())?;
        let mut bytes = serde_json::to_vec(&line)?;
        bytes.push(b'\n');
        self.poisoned = true;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        self.replay = copy;
        self.poisoned = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "keeper-ledger-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }
    fn attempt() -> SignedAttempt {
        SignedAttempt {
            signature: "sig".into(),
            curve: "curve".into(),
            mint: "mint".into(),
            payer: "payer".into(),
            genesis_hash: "genesis".into(),
            blockhash: "hash".into(),
            last_valid_block_height: 42,
            signed_bytes: vec![1, 2, 3],
            attempt_id: "attempt".into(),
            compute_unit_limit: 10,
            loaded_accounts_data_size_limit: 20,
            priority_fee_lamports: 1,
        }
    }
    fn settlement() -> Settlement {
        Settlement {
            signature: "sig".into(),
            curve: "curve".into(),
            slot: 1,
            fee_lamports: 2,
            pre_balance: 100,
            post_balance: 97,
            net_lamports: -3,
            outcome: Outcome::Failure,
            evidence: "finalized metadata".into(),
        }
    }
    fn evidence() -> ExecutionEvidence {
        ExecutionEvidence {
            min_profit_lamports: 0,
            simulated_delta_lamports: 1,
            policy_compute_unit_limit: 10,
            policy_loaded_accounts_data_size_limit: 20,
            priority_fee_lamports: 1,
            max_priority_fee_lamports: 1,
            max_compute_units: 10,
            max_loaded_accounts_data_size: 20,
            max_total_fee_lamports: 1,
            min_block_heights_remaining: 1,
            simulated_units: 10,
            simulated_loaded_bytes: 20,
            quoted_fee_lamports: 1,
            payer_balance: 100,
            account_slot: 1,
            checked_block_height: 1,
        }
    }
    #[test]
    fn send_intent_and_never_submitted_are_exclusive_after_reopen() {
        let path = dir();
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        ledger
            .append(Record::PolicyValidatedAttemptV2 {
                attempt: attempt(),
                evidence: evidence(),
            })
            .unwrap();
        drop(ledger);
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        ledger
            .append(Record::SendIntent {
                signature: "sig".into(),
            })
            .unwrap();
        drop(ledger);
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        assert!(ledger.replay.send_intents.contains("sig"));
        assert!(
            ledger
                .append(Record::NeverSubmitted {
                    signature: "sig".into()
                })
                .is_err()
        );
        assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
        drop(ledger);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn legacy_attempt_cannot_be_closed_as_never_submitted() {
        let path = dir();
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        ledger
            .append(Record::PolicyValidatedAttempt {
                attempt: attempt(),
                evidence: evidence(),
            })
            .unwrap();
        assert!(
            ledger
                .append(Record::NeverSubmitted {
                    signature: "sig".into()
                })
                .is_err()
        );
        assert!(
            ledger
                .append(Record::PolicyValidatedAttemptV2 {
                    attempt: attempt(),
                    evidence: evidence()
                })
                .is_err()
        );
        drop(ledger);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn unresolved_expiry_releases_lane_but_retains_accounting_across_restart() {
        let path = dir();
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        ledger
            .append(Record::SignedAttempt { attempt: attempt() })
            .unwrap();
        ledger
            .append(Record::ExpiredUnresolved {
                signature: "sig".into(),
                finalized_slot: 99,
            })
            .unwrap();
        drop(ledger);
        let ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        assert_eq!(ledger.replay.expired_unresolved.get("sig"), Some(&99));
        assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 0);
        assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
        assert_eq!(ledger.replay.totals.unresolved_settlements, 1);
        drop(ledger);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn replay_idempotency_and_lock() {
        let path = dir();
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        assert!(Ledger::open(&path, "genesis", "payer").is_err());
        ledger
            .append(Record::SignedAttempt { attempt: attempt() })
            .unwrap();
        ledger
            .append(Record::FinalizedSettlement {
                settlement: settlement(),
            })
            .unwrap();
        ledger
            .append(Record::FinalizedSettlement {
                settlement: settlement(),
            })
            .unwrap();
        assert_eq!(ledger.replay.totals.realized_net_lamports, -3);
        let mut conflicting = settlement();
        conflicting.slot = 2;
        assert!(
            ledger
                .append(Record::FinalizedSettlement {
                    settlement: conflicting
                })
                .is_err()
        );
        drop(ledger);
        let reopened = Ledger::open(&path, "genesis", "payer").unwrap();
        assert_eq!(reopened.replay.totals.failed_net_lamports, -3);
        drop(reopened);
        assert!(Ledger::open(&path, "other", "payer").is_err());
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn finalized_outcome_releases_lane_but_remains_unsettled() {
        let path = dir();
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        ledger
            .append(Record::SignedAttempt { attempt: attempt() })
            .unwrap();
        assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 1);
        let outcome = Record::FinalizedOutcome {
            signature: "sig".into(),
            success: false,
            slot: 1,
        };
        ledger.append(outcome.clone()).unwrap();
        ledger.append(outcome).unwrap();
        assert_eq!(ledger.replay.inclusion_ambiguous_attempts().count(), 0);
        assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
        assert_eq!(ledger.replay.totals.unresolved_settlements, 1);
        assert!(
            ledger
                .append(Record::FinalizedOutcome {
                    signature: "sig".into(),
                    success: true,
                    slot: 1
                })
                .is_err()
        );
        assert!(
            ledger
                .append(Record::Expired {
                    signature: "sig".into()
                })
                .is_err()
        );
        let mut conflict = settlement();
        conflict.slot = 2;
        assert!(
            ledger
                .append(Record::FinalizedSettlement {
                    settlement: conflict
                })
                .is_err()
        );
        drop(ledger);
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        assert_eq!(
            ledger.replay.finalized_outcomes.get("sig"),
            Some(&(false, 1))
        );
        assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
        ledger
            .append(Record::FinalizedSettlement {
                settlement: settlement(),
            })
            .unwrap();
        assert_eq!(ledger.replay.unresolved_attempts().count(), 0);
        assert_eq!(
            ledger.replay.finalized_outcomes.get("sig"),
            Some(&(false, 1))
        );
        drop(ledger);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn partial_tail_is_preserved_and_recovered() {
        let path = dir();
        let mut ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        ledger
            .append(Record::SignedAttempt { attempt: attempt() })
            .unwrap();
        drop(ledger);
        let ledger_path = path.join("ledger.jsonl");
        OpenOptions::new()
            .append(true)
            .open(&ledger_path)
            .unwrap()
            .write_all(b"{partial")
            .unwrap();
        assert_eq!(
            read_summary(&ledger_path)
                .unwrap()
                .unresolved_attempts()
                .count(),
            1
        );
        let ledger = Ledger::open(&path, "genesis", "payer").unwrap();
        assert_eq!(
            fs::read(path.join("ledger.corrupt-tail.0")).unwrap(),
            b"{partial"
        );
        assert_eq!(ledger.replay.unresolved_attempts().count(), 1);
        drop(ledger);
        fs::remove_dir_all(path).unwrap();
    }
}

mod i128_string {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
