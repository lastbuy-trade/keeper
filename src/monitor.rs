use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use lastbuy_sdk::bonding_curve::constants::{CURVE_STATE_DISCRIMINATOR, PROGRAM_ID};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
    SubscribeRequestFilterSlots, SubscribeRequestPing, SubscribeUpdateAccount,
    subscribe_request_filter_accounts_filter::Filter,
    subscribe_request_filter_accounts_filter_memcmp::Data, subscribe_update::UpdateOneof,
};

/// Credentials must never be formatted or logged.
pub struct MonitorConfig {
    pub endpoint: String,
    pub token: Option<String>,
    pub global_config: String,
    pub idle_timeout: Duration,
    pub replay_overlap: u64,
}

pub enum MonitorEvent {
    Account(SubscribeUpdateAccount),
    Slot(u64),
}

fn memcmp(offset: u64, bytes: Vec<u8>) -> SubscribeRequestFilterAccountsFilter {
    SubscribeRequestFilterAccountsFilter {
        filter: Some(Filter::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
            offset,
            data: Some(Data::Bytes(bytes)),
        })),
    }
}

pub fn subscription_request(global_config: &str, from_slot: Option<u64>) -> SubscribeRequest {
    let mut accounts = HashMap::new();
    for (name, status) in [("graduated", 4), ("migrating", 5)] {
        accounts.insert(
            name.to_owned(),
            SubscribeRequestFilterAccounts {
                owner: vec![PROGRAM_ID.to_string()],
                filters: vec![
                    memcmp(0, CURVE_STATE_DISCRIMINATOR.to_vec()),
                    memcmp(9, vec![status]),
                ],
                ..Default::default()
            },
        );
    }
    accounts.insert(
        "global_config".to_owned(),
        SubscribeRequestFilterAccounts {
            account: vec![global_config.to_owned()],
            ..Default::default()
        },
    );
    let mut slots = HashMap::new();
    slots.insert(
        "confirmed".to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            ..Default::default()
        },
    );
    SubscribeRequest {
        accounts,
        slots,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        from_slot,
        ..Default::default()
    }
}

/// A full channel sets `reconcile_needed`; discovery must remain degraded until a scan succeeds.
/// Pings are handled on this task and never wait for downstream processing.
pub async fn run(
    config: MonitorConfig,
    sender: mpsc::Sender<MonitorEvent>,
    connection: watch::Sender<bool>,
    reconcile_needed: Arc<AtomicBool>,
) {
    let mut last_slot: Option<u64> = None;
    let mut delay = Duration::from_secs(1);
    let mut replay = true;
    loop {
        if sender.is_closed() {
            return;
        }
        let from_slot = if replay {
            last_slot.map(|slot| slot.saturating_sub(config.replay_overlap))
        } else {
            None
        };
        let result = consume(
            &config,
            &sender,
            &connection,
            &reconcile_needed,
            from_slot,
            &mut last_slot,
        )
        .await;
        reconcile_needed.store(true, Ordering::Release);
        set_connection(&connection, false);
        // Retry live after a rejected or unavailable replay; a full scan provides recovery.
        replay = from_slot.is_none();
        if result {
            delay = Duration::from_secs(1);
        }
        let jitter = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::from(d.subsec_millis()) % 251)
            .unwrap_or(0);
        tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

fn set_connection(connection: &watch::Sender<bool>, connected: bool) {
    connection.send_replace(connected);
}

struct SlotProgress {
    highest: Option<u64>,
    deadline: Instant,
    timeout: Duration,
}

impl SlotProgress {
    fn new(timeout: Duration, now: Instant) -> Self {
        Self {
            highest: None,
            deadline: now + timeout,
            timeout,
        }
    }

    fn observe(&mut self, slot: u64, now: Instant) -> bool {
        if self.highest.is_none_or(|highest| slot > highest) {
            self.highest = Some(slot);
            self.deadline = now + self.timeout;
            true
        } else {
            false
        }
    }
}

fn grpc_status_code(error: &GeyserGrpcClientError) -> Option<tonic::Code> {
    match error {
        GeyserGrpcClientError::TonicStatus(status) => Some(status.code()),
        GeyserGrpcClientError::TransportError(_) => None,
    }
}

async fn consume(
    config: &MonitorConfig,
    sender: &mpsc::Sender<MonitorEvent>,
    connection: &watch::Sender<bool>,
    reconcile_needed: &AtomicBool,
    from_slot: Option<u64>,
    last_slot: &mut Option<u64>,
) -> bool {
    let replay_requested = from_slot.is_some();
    let builder = match GeyserGrpcClient::build_from_shared(config.endpoint.clone())
        .and_then(|b| b.x_token(config.token.clone()))
        .and_then(|b| b.tls_config(ClientTlsConfig::new().with_enabled_roots()))
    {
        Ok(builder) => builder,
        Err(_) => {
            log::warn!("monitor configuration rejected; replay_requested={replay_requested}");
            return false;
        }
    };
    let mut client = match tokio::time::timeout(config.idle_timeout, builder.connect()).await {
        Ok(Ok(client)) => client,
        _ => {
            log::warn!(
                "monitor transport connection failed or timed out; replay_requested={replay_requested}"
            );
            return false;
        }
    };
    let (mut sink, mut stream) = match tokio::time::timeout(
        config.idle_timeout,
        client.subscribe_with_request(Some(subscription_request(&config.global_config, from_slot))),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            log::warn!(
                "monitor subscription failed; grpc_code={:?} replay_requested={replay_requested}",
                grpc_status_code(&error)
            );
            return false;
        }
        Err(_) => {
            log::warn!("monitor subscription timed out; replay_requested={replay_requested}");
            return false;
        }
    };
    reconcile_needed.store(true, Ordering::Release);
    set_connection(connection, true);
    let mut progressed = false;
    let mut progress = SlotProgress::new(config.idle_timeout, Instant::now());
    loop {
        if Instant::now() >= progress.deadline {
            log::warn!(
                "monitor confirmed slot progress stalled; replay_requested={replay_requested}"
            );
            return progressed;
        }
        let update = match tokio::time::timeout_at(progress.deadline, stream.next()).await {
            Ok(Some(Ok(update))) => update,
            Ok(Some(Err(status))) => {
                log::warn!(
                    "monitor stream failed; grpc_code={:?} replay_requested={replay_requested}",
                    status.code()
                );
                return progressed;
            }
            Ok(None) => {
                log::warn!("monitor stream ended; replay_requested={replay_requested}");
                return progressed;
            }
            Err(_) => {
                log::warn!(
                    "monitor confirmed slot progress stalled; replay_requested={replay_requested}"
                );
                return progressed;
            }
        };
        let event = match update.update_oneof {
            Some(UpdateOneof::Ping(_)) => {
                if !matches!(
                    tokio::time::timeout_at(
                        progress.deadline,
                        sink.send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 0 }),
                            ..Default::default()
                        })
                    )
                    .await,
                    Ok(Ok(()))
                ) {
                    log::warn!(
                        "monitor ping response failed or slot progress stalled; replay_requested={replay_requested}"
                    );
                    return progressed;
                }
                continue;
            }
            Some(UpdateOneof::Account(account)) => Some(MonitorEvent::Account(account)),
            Some(UpdateOneof::Slot(slot)) => {
                progressed |= progress.observe(slot.slot, Instant::now());
                Some(MonitorEvent::Slot(slot.slot))
            }
            _ => None,
        };
        if let Some(event) = event {
            let slot = match &event {
                MonitorEvent::Account(a) => a.slot,
                MonitorEvent::Slot(slot) => *slot,
            };
            match sender.try_send(event) {
                Ok(()) => *last_slot = Some(last_slot.unwrap_or(0).max(slot)),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    log::warn!(
                        "monitor event queue full; reconciliation required; replay_requested={replay_requested}"
                    );
                    reconcile_needed.store(true, Ordering::Release)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return progressed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn reconnect_state_is_delivered_when_event_queue_is_full() {
        let (events, mut event_rx) = mpsc::channel(1);
        events.try_send(MonitorEvent::Slot(1)).unwrap();
        let (connection, mut state) = watch::channel(false);
        set_connection(&connection, true);
        state.changed().await.unwrap();
        assert!(*state.borrow_and_update());
        set_connection(&connection, false);
        set_connection(&connection, true);
        state.changed().await.unwrap();
        assert!(*state.borrow_and_update());
        assert!(matches!(event_rx.try_recv(), Ok(MonitorEvent::Slot(1))));
    }
    #[test]
    fn status_diagnostics_extract_only_code() {
        let error = GeyserGrpcClientError::TonicStatus(tonic::Status::permission_denied(
            "secret token and endpoint",
        ));
        assert_eq!(
            grpc_status_code(&error),
            Some(tonic::Code::PermissionDenied)
        );
    }
    #[test]
    fn only_advancing_slots_extend_deadline() {
        let now = Instant::now();
        let mut progress = SlotProgress::new(Duration::from_secs(5), now);
        assert!(progress.observe(10, now + Duration::from_secs(1)));
        let deadline = progress.deadline;
        assert!(!progress.observe(10, now + Duration::from_secs(2)));
        assert!(!progress.observe(9, now + Duration::from_secs(3)));
        assert_eq!(progress.deadline, deadline);
        assert!(progress.observe(11, now + Duration::from_secs(4)));
        assert_eq!(progress.deadline, now + Duration::from_secs(9));
    }
    #[test]
    fn filters_are_confirmed_full_account_and_disjoint_statuses() {
        let req = subscription_request("config", Some(42));
        assert_eq!(req.commitment, Some(CommitmentLevel::Confirmed as i32));
        assert!(req.accounts_data_slice.is_empty());
        assert_eq!(req.from_slot, Some(42));
        for (name, status) in [("graduated", 4), ("migrating", 5)] {
            let filter = &req.accounts[name];
            assert_eq!(filter.owner, vec![PROGRAM_ID.to_string()]);
            assert_eq!(
                filter.filters,
                vec![
                    memcmp(0, CURVE_STATE_DISCRIMINATOR.to_vec()),
                    memcmp(9, vec![status])
                ]
            );
        }
        assert_eq!(req.accounts["global_config"].account, vec!["config"]);
        assert_eq!(req.slots["confirmed"].filter_by_commitment, Some(true));
    }
}
