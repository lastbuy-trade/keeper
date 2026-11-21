use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};

/// A validated account from a context-bearing confirmed GPA response.
pub struct Snapshot<T> {
    pub slot: u64,
    pub accounts: Vec<(Pubkey, T)>,
}

struct Entry<T> {
    slot: u64,
    // A snapshot observes the entire bank, and therefore supersedes stream writes at its slot.
    write_version: Option<u64>,
    value: T,
}

/// Bounded by the number of tracked curves. Missing entries from one scan are returned for
/// fresh account reads, never interpreted as proof that the curve migrated.
pub struct VersionedAccounts<T> {
    entries: HashMap<Pubkey, Entry<T>>,
}

impl<T> Default for VersionedAccounts<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<T> VersionedAccounts<T> {
    pub fn stream(&mut self, key: Pubkey, slot: u64, write_version: u64, value: T) -> bool {
        let newer = self.entries.get(&key).is_none_or(|old| {
            slot > old.slot
                || (slot == old.slot && old.write_version.is_some_and(|v| write_version > v))
        });
        if newer {
            self.entries.insert(
                key,
                Entry {
                    slot,
                    write_version: Some(write_version),
                    value,
                },
            );
        }
        newer
    }

    // Merge one validated scan while retaining newer stream updates.
    fn snapshot(&mut self, snapshot: Snapshot<T>) -> Vec<Pubkey> {
        let seen: HashSet<_> = snapshot.accounts.iter().map(|(key, _)| *key).collect();
        let missing = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                (entry.slot <= snapshot.slot && !seen.contains(key)).then_some(*key)
            })
            .collect();
        for (key, value) in snapshot.accounts {
            if self
                .entries
                .get(&key)
                .is_none_or(|old| old.slot <= snapshot.slot)
            {
                self.entries.insert(
                    key,
                    Entry {
                        slot: snapshot.slot,
                        write_version: None,
                        value,
                    },
                );
            }
        }
        missing
    }

    /// Merge both status scans before determining which accounts require a fresh read.
    /// The earlier context is the latest slot for which both scans establish coverage.
    pub fn snapshots(&mut self, snapshots: [Snapshot<T>; 2]) -> Vec<Pubkey> {
        let coverage_slot = snapshots[0].slot.min(snapshots[1].slot);
        let seen: HashSet<_> = snapshots
            .iter()
            .flat_map(|scan| scan.accounts.iter().map(|(key, _)| *key))
            .collect();
        let missing = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                (entry.slot <= coverage_slot && !seen.contains(key)).then_some(*key)
            })
            .collect();
        for snapshot in snapshots {
            self.snapshot(snapshot);
        }
        missing
    }

    pub fn get(&self, key: &Pubkey) -> Option<&T> {
        self.entries.get(key).map(|entry| &entry.value)
    }
    /// Remove only after a fresh, validated decision read establishes ineligibility or absence.
    pub fn remove(&mut self, key: &Pubkey) {
        self.entries.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn combined_scans_do_not_mark_the_other_status_missing() {
        let graduated = Pubkey::new_unique();
        let migrating = Pubkey::new_unique();
        let absent = Pubkey::new_unique();
        let fresh = Pubkey::new_unique();
        let mut state = VersionedAccounts::default();
        for key in [graduated, migrating, absent] {
            state.stream(key, 10, 1, "old");
        }
        state.stream(fresh, 12, 1, "fresh");
        let missing = state.snapshots([
            Snapshot {
                slot: 11,
                accounts: vec![(graduated, "graduated")],
            },
            Snapshot {
                slot: 13,
                accounts: vec![(migrating, "migrating")],
            },
        ]);
        assert_eq!(missing, vec![absent]);
        assert_eq!(state.get(&fresh), Some(&"fresh"));
    }
    #[test]
    fn snapshot_does_not_overwrite_newer_stream_and_missing_needs_refresh() {
        let key = Pubkey::new_unique();
        let mut state = VersionedAccounts::default();
        assert!(state.stream(key, 11, 2, "funded"));
        state.snapshot(Snapshot {
            slot: 10,
            accounts: vec![(key, "unfunded")],
        });
        assert_eq!(state.get(&key), Some(&"funded"));
        assert!(!state.stream(key, 11, 1, "old"));
        assert!(
            state
                .snapshot(Snapshot {
                    slot: 12,
                    accounts: vec![]
                })
                .contains(&key)
        );
        assert_eq!(state.get(&key), Some(&"funded"));
        state.snapshot(Snapshot {
            slot: 12,
            accounts: vec![(key, "scan")],
        });
        assert!(!state.stream(key, 12, 99, "late write"));
        assert_eq!(state.get(&key), Some(&"scan"));
    }
}

use lastbuy_sdk::bonding_curve::{CURVE_STATE_DISCRIMINATOR, CurveState, CurveStatus, PROGRAM_ID};
use solana_rpc_client::{
    api::{
        filter::{Memcmp, RpcFilterType},
        request::RpcRequest,
        response::{Response, RpcKeyedAccount},
    },
    nonblocking::rpc_client::RpcClient,
};
use std::{str::FromStr, time::Duration};
use thiserror::Error;

/// Sanitized failure categories; never include RPC-supplied messages or URLs in logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureClass {
    Timeout,
    Quota,
    StaleContext,
    InvalidData,
    Configuration,
    Transport,
}

fn classify_http_status(status: Option<u16>) -> FailureClass {
    match status {
        Some(401 | 403) => FailureClass::Configuration,
        Some(429) => FailureClass::Quota,
        _ => FailureClass::Transport,
    }
}

pub fn classify_rpc(error: &solana_rpc_client::api::client_error::Error) -> FailureClass {
    use solana_rpc_client::api::{client_error::ErrorKind, request::RpcError};
    match error.kind() {
        ErrorKind::Reqwest(err) if err.is_timeout() => FailureClass::Timeout,
        ErrorKind::Reqwest(err) if err.status().is_some() => {
            classify_http_status(err.status().map(|status| status.as_u16()))
        }
        ErrorKind::RpcError(RpcError::RpcResponseError { code: -32016, .. }) => {
            FailureClass::StaleContext
        }
        ErrorKind::RpcError(RpcError::RpcResponseError {
            code: -32602..=-32600,
            ..
        }) => FailureClass::Configuration,
        ErrorKind::SerdeJson(_) => FailureClass::InvalidData,
        _ => FailureClass::Transport,
    }
}

#[derive(Debug, Error)]
pub enum ScanError {
    #[error("reconciliation RPC request timed out")]
    Timeout,
    #[error("reconciliation RPC request failed: {0}")]
    Rpc(#[from] solana_rpc_client::api::client_error::Error),
    #[error("reconciliation returned an invalid account address or owner")]
    Address,
    #[error("reconciliation returned invalid account data")]
    Data,
    #[error("reconciliation returned an invalid curve: {0}")]
    Curve(#[from] crate::chain::CurveError),
    #[error("reconciliation returned a curve outside its status filter")]
    Status,
}

impl ScanError {
    pub fn class(&self) -> FailureClass {
        match self {
            Self::Timeout => FailureClass::Timeout,
            Self::Rpc(error) => classify_rpc(error),
            Self::Address | Self::Data | Self::Curve(_) | Self::Status => FailureClass::InvalidData,
        }
    }
}

fn scan_params(status: u8, min_context_slot: Option<u64>) -> serde_json::Value {
    let filters = vec![
        RpcFilterType::Memcmp(Memcmp::new_base58_encoded(0, &CURVE_STATE_DISCRIMINATOR)),
        RpcFilterType::Memcmp(Memcmp::new_base58_encoded(9, &[status])),
    ];
    let mut config = serde_json::json!({"encoding":"base64", "commitment":"confirmed", "withContext":true, "filters":filters});
    if let Some(slot) = min_context_slot {
        config["minContextSlot"] = slot.into();
    }
    serde_json::json!([PROGRAM_ID.to_string(), config])
}

pub(crate) const RPC_REQUEST_SPACING: Duration = Duration::from_millis(100);

/// Two independent context-bearing confirmed scans. All accounts are validated before either
/// snapshot is returned; callers must treat any error as degraded discovery and retry with backoff.
pub async fn scan(
    client: &RpcClient,
    min_context_slot: Option<u64>,
    timeout: Duration,
) -> Result<[Snapshot<CurveState>; 2], ScanError> {
    let graduated = scan_status(client, 4, min_context_slot, timeout).await?;
    tokio::time::sleep(RPC_REQUEST_SPACING).await;
    let migrating = scan_status(client, 5, min_context_slot, timeout).await?;
    Ok([graduated, migrating])
}

async fn scan_status(
    client: &RpcClient,
    status: u8,
    min_context_slot: Option<u64>,
    timeout: Duration,
) -> Result<Snapshot<CurveState>, ScanError> {
    let response: Response<Vec<RpcKeyedAccount>> = tokio::time::timeout(
        timeout,
        client.send(
            RpcRequest::GetProgramAccounts,
            scan_params(status, min_context_slot),
        ),
    )
    .await
    .map_err(|_| ScanError::Timeout)??;
    let mut accounts = Vec::with_capacity(response.value.len());
    for item in response.value {
        let key = Pubkey::from_str(&item.pubkey).map_err(|_| ScanError::Address)?;
        let owner = Pubkey::from_str(&item.account.owner).map_err(|_| ScanError::Address)?;
        if item.account.executable {
            return Err(ScanError::Data);
        }
        let data = item.account.data.decode().ok_or(ScanError::Data)?;
        let curve = crate::chain::decode_curve(&key, &owner, &data)?;
        if !matches!(
            (status, &curve.status),
            (4, CurveStatus::Graduated) | (5, CurveStatus::Migrating)
        ) {
            return Err(ScanError::Status);
        }
        accounts.push((key, curve));
    }
    Ok(Snapshot {
        slot: response.context.slot,
        accounts,
    })
}

#[cfg(test)]
mod scan_tests {
    use super::*;
    #[test]
    fn rpc_errors_are_classified_without_exposing_server_messages() {
        use solana_rpc_client::api::{
            client_error::{Error, ErrorKind},
            request::{RpcError, RpcResponseErrorData},
        };
        for (code, expected) in [
            (-32016, FailureClass::StaleContext),
            (-32602, FailureClass::Configuration),
            (-32000, FailureClass::Transport),
        ] {
            let error = Error::from(ErrorKind::RpcError(RpcError::RpcResponseError {
                code,
                message: "credential secret".into(),
                data: RpcResponseErrorData::Empty,
            }));
            let class = classify_rpc(&error);
            assert_eq!(class, expected);
            assert!(!format!("{class:?}").contains("secret"));
        }
        assert_eq!(classify_http_status(Some(401)), FailureClass::Configuration);
        assert_eq!(classify_http_status(Some(403)), FailureClass::Configuration);
        assert_eq!(classify_http_status(Some(429)), FailureClass::Quota);
        assert_eq!(classify_http_status(Some(503)), FailureClass::Transport);
        assert_eq!(ScanError::Timeout.class(), FailureClass::Timeout);
        assert_eq!(ScanError::Data.class(), FailureClass::InvalidData);
    }
    #[test]
    fn requests_confirmed_context_and_separate_status_filters() {
        for status in [4, 5] {
            let params = scan_params(status, Some(42));
            assert_eq!(params[1]["commitment"], "confirmed");
            assert_eq!(params[1]["withContext"], true);
            assert_eq!(params[1]["minContextSlot"], 42);
            assert_eq!(params[1]["filters"][0]["memcmp"]["offset"], 0);
            assert_eq!(params[1]["filters"][1]["memcmp"]["offset"], 9);
        }
    }
}
