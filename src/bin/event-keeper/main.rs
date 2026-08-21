use anchor_client::solana_sdk::commitment_config::CommitmentConfig;
use anchor_lang::{AnchorDeserialize, Discriminator, prelude::*};
use anyhow::{Context, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::{
    config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter},
    response::{Response as RpcResponse, RpcLogsResponse},
};
use std::{
    collections::HashSet,
    env,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::MissedTickBehavior;
use twob_keepers::{
    ClosePositionEventRecord, EventSink, MarketUpdateEventRecord, SinkMetricsSnapshot,
    TimescaleSink,
};

declare_program!(twob_anchor);
use twob_anchor::events::*;
use twob_anchor::types::Side;

const PROGRAM_LOG_PREFIX: &str = "Program log: ";
const PROGRAM_DATA_PREFIX: &str = "Program data: ";

#[derive(Debug)]
enum KeeperEvent {
    MarketUpdate(MarketUpdateEvent),
    ClosePosition(ClosePositionEvent),
    AuthorityTransferred(AuthorityTransferred),
}

#[derive(Debug)]
struct IndexedKeeperEvent {
    event_index: u16,
    event: KeeperEvent,
}

struct IngestStats {
    started_at: Instant,
    market_events: u64,
    close_events: u64,
    decode_errors: u64,
    db_errors: u64,
    last_market_at: Option<Instant>,
    last_close_at: Option<Instant>,
    unknown_discriminators: HashSet<[u8; 8]>,
}

impl IngestStats {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            market_events: 0,
            close_events: 0,
            decode_errors: 0,
            db_errors: 0,
            last_market_at: None,
            last_close_at: None,
            unknown_discriminators: HashSet::new(),
        }
    }

    fn record_market_event(&mut self) {
        self.market_events += 1;
        self.last_market_at = Some(Instant::now());
    }

    fn record_close_event(&mut self) {
        self.close_events += 1;
        self.last_close_at = Some(Instant::now());
    }

    fn record_db_error(&mut self) {
        self.db_errors += 1;
    }

    fn record_decode_error(&mut self, signature: &str, slot: u64, error: &str) {
        self.decode_errors += 1;
        eprintln!(
            "Failed to decode Anchor event payload (signature: {signature}, slot: {slot}): {error}"
        );
    }

    fn record_unknown_discriminator(&mut self, log_bytes: &[u8]) {
        if log_bytes.len() < 8 {
            return;
        }

        let discriminator: [u8; 8] = log_bytes[..8].try_into().expect("length is validated");

        if self.unknown_discriminators.insert(discriminator) {
            eprintln!(
                "Observed unknown event discriminator 0x{}; keeper IDL may be outdated",
                hex_discriminator(discriminator)
            );
        }
    }

    fn log_health(&self, sink: &dyn EventSink) {
        let uptime_seconds = self.started_at.elapsed().as_secs();
        let last_market = format_last_seen(self.last_market_at);
        let last_close = format_last_seen(self.last_close_at);

        println!(
            "Health - uptime={}s market_events={} (last={}) close_events={} (last={}) decode_errors={} db_errors={} unknown_discriminators={}",
            uptime_seconds,
            self.market_events,
            last_market,
            self.close_events,
            last_close,
            self.decode_errors,
            self.db_errors,
            self.unknown_discriminators.len(),
        );

        for snapshot in sink.metrics_snapshot() {
            println!("{}", format_sink_metrics(snapshot));
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("DATABASE_URL must be set (Tiger Cloud connection string)"))?;

    let sink: Arc<dyn EventSink> = Arc::new(TimescaleSink::connect(&database_url).await?);
    println!("Connected to Tiger Cloud (Timescale) sink");

    let ws_url = env::var("CLUSTER_WS_URL").expect("CLUSTER_WS_URL must be set");
    let program_id = twob_anchor::ID.to_string();

    let mut backoff = Duration::from_secs(1);

    loop {
        println!(
            "Subscribing to transaction logs for program {} on {}",
            program_id, ws_url
        );

        match run_subscription(&ws_url, &program_id, sink.clone()).await {
            Ok(()) => eprintln!("Log subscription ended, reconnecting"),
            Err(error) => eprintln!("Log subscription failed: {error:#}"),
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn run_subscription(
    ws_url: &str,
    program_id: &str,
    sink: Arc<dyn EventSink>,
) -> anyhow::Result<()> {
    let pubsub_client = PubsubClient::new(ws_url)
        .await
        .context("Failed to create pubsub websocket client")?;

    let config = RpcTransactionLogsConfig {
        commitment: Some(CommitmentConfig::confirmed()),
    };
    let filter = RpcTransactionLogsFilter::Mentions(vec![program_id.to_string()]);

    let (mut notifications, unsubscribe) = pubsub_client
        .logs_subscribe(filter, config)
        .await
        .context("Failed to subscribe to transaction logs")?;

    println!("Subscription established");

    let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut stats = IngestStats::new();

    loop {
        tokio::select! {
            maybe_notification = notifications.next() => {
                let Some(notification) = maybe_notification else {
                    eprintln!("Log notification stream closed by RPC node");
                    break;
                };

                if let Err(error) = handle_logs_notification(sink.as_ref(), program_id, notification, &mut stats).await {
                    eprintln!("Failed to handle log notification: {error:#}");
                }
            }
            _ = heartbeat.tick() => {
                stats.log_health(sink.as_ref());
            }
        }
    }

    unsubscribe().await;
    Ok(())
}

async fn handle_logs_notification(
    sink: &dyn EventSink,
    program_id: &str,
    notification: RpcResponse<RpcLogsResponse>,
    stats: &mut IngestStats,
) -> anyhow::Result<()> {
    let slot = notification.context.slot;
    let signature = notification.value.signature;

    for indexed_event in parse_events_from_logs(
        program_id,
        &notification.value.logs,
        &signature,
        slot,
        stats,
    ) {
        match indexed_event.event {
            KeeperEvent::MarketUpdate(event) => {
                println!(
                    "MarketUpdateEvent - Signature: {}, Slot: {}, Market: {}",
                    signature, slot, event.market_id
                );
                stats.record_market_event();

                let record =
                    market_update_record(&signature, indexed_event.event_index, slot, &event);

                if let Err(error) = sink.insert_market_update_event(record).await {
                    stats.record_db_error();
                    eprintln!("Failed to insert market update event via sink: {error}");
                }
            }
            KeeperEvent::ClosePosition(event) => {
                println!(
                    "ClosePositionEvent - Signature: {}, Slot: {}, Market: {}",
                    signature, slot, event.market_id
                );
                stats.record_close_event();

                let record =
                    close_position_record(&signature, indexed_event.event_index, slot, &event);

                if let Err(error) = sink.insert_close_position_event(record).await {
                    stats.record_db_error();
                    eprintln!("Failed to insert close position event via sink: {error}");
                }
            }
            KeeperEvent::AuthorityTransferred(event) => {
                println!(
                    "AuthorityTransferred - Signature: {}, Slot: {}, New authority: {}",
                    signature, slot, event.new_authority
                );
            }
        }
    }

    Ok(())
}

fn market_update_record(
    signature: &str,
    event_index: u16,
    slot: u64,
    event: &MarketUpdateEvent,
) -> MarketUpdateEventRecord {
    MarketUpdateEventRecord {
        signature: signature.to_owned(),
        event_index,
        slot,
        market_id: u64::from(event.market_id),
        base_flow: event.base_flow,
        quote_flow: event.quote_flow,
    }
}

fn close_position_record(
    signature: &str,
    event_index: u16,
    slot: u64,
    event: &ClosePositionEvent,
) -> ClosePositionEventRecord {
    ClosePositionEventRecord {
        signature: signature.to_owned(),
        event_index,
        slot,
        position_authority: event.position_authority.to_string(),
        market_id: u64::from(event.market_id),
        start_slot: event.start_slot,
        end_slot: event.end_slot,
        deposit_amount: event.deposit_amount,
        swapped_amount: event.swapped_amount,
        remaining_amount: event.remaining_amount,
        fee_amount: event.fee_amount,
        is_buy: side_to_is_buy(event.side),
    }
}

fn side_to_is_buy(side: Side) -> u8 {
    u8::from(matches!(side, Side::Buy))
}

fn parse_events_from_logs(
    program_id: &str,
    logs: &[String],
    signature: &str,
    slot: u64,
    stats: &mut IngestStats,
) -> Vec<IndexedKeeperEvent> {
    let mut call_stack: Vec<&str> = Vec::new();
    let mut events = Vec::new();

    for log_line in logs {
        if let Some(invoked_program) = parse_invoked_program(log_line) {
            call_stack.push(invoked_program);
            continue;
        }

        if is_program_completion(log_line) {
            if call_stack.pop().is_none() {
                eprintln!(
                    "Unexpected empty call stack while parsing logs (signature: {}, slot: {})",
                    signature, slot
                );
            }
            continue;
        }

        if call_stack.last().copied() != Some(program_id) {
            continue;
        }

        let Some(encoded_data) = log_line
            .strip_prefix(PROGRAM_DATA_PREFIX)
            .or_else(|| log_line.strip_prefix(PROGRAM_LOG_PREFIX))
        else {
            continue;
        };

        let Ok(log_bytes) = STANDARD.decode(encoded_data) else {
            continue;
        };

        match decode_event(&log_bytes) {
            Ok(Some(event)) => {
                if events.len() >= u16::MAX as usize {
                    stats.record_decode_error(
                        signature,
                        slot,
                        "Event index overflow while parsing logs",
                    );
                    continue;
                }

                let event_index = events.len() as u16;
                events.push(IndexedKeeperEvent { event_index, event });
            }
            Ok(None) => stats.record_unknown_discriminator(&log_bytes),
            Err(error) => stats.record_decode_error(signature, slot, &error),
        }
    }

    events
}

fn decode_event(log_bytes: &[u8]) -> std::result::Result<Option<KeeperEvent>, String> {
    if log_bytes.starts_with(MarketUpdateEvent::DISCRIMINATOR) {
        let mut data = &log_bytes[MarketUpdateEvent::DISCRIMINATOR.len()..];
        let event = MarketUpdateEvent::deserialize(&mut data)
            .map_err(|error| format!("MarketUpdateEvent decode error: {error}"))?;
        return Ok(Some(KeeperEvent::MarketUpdate(event)));
    }

    if log_bytes.starts_with(ClosePositionEvent::DISCRIMINATOR) {
        let mut data = &log_bytes[ClosePositionEvent::DISCRIMINATOR.len()..];
        let event = ClosePositionEvent::deserialize(&mut data)
            .map_err(|error| format!("ClosePositionEvent decode error: {error}"))?;
        return Ok(Some(KeeperEvent::ClosePosition(event)));
    }

    if log_bytes.starts_with(AuthorityTransferred::DISCRIMINATOR) {
        let mut data = &log_bytes[AuthorityTransferred::DISCRIMINATOR.len()..];
        let event = AuthorityTransferred::deserialize(&mut data)
            .map_err(|error| format!("AuthorityTransferred decode error: {error}"))?;
        return Ok(Some(KeeperEvent::AuthorityTransferred(event)));
    }

    Ok(None)
}

fn parse_invoked_program(log_line: &str) -> Option<&str> {
    let stripped = log_line.strip_prefix("Program ")?;
    let (program, depth) = stripped.split_once(" invoke [")?;

    if depth.ends_with(']') {
        Some(program)
    } else {
        None
    }
}

fn is_program_completion(log_line: &str) -> bool {
    let stripped = match log_line.strip_prefix("Program ") {
        Some(stripped) => stripped,
        None => return false,
    };

    stripped.ends_with(" success") || stripped.contains(" failed:")
}

fn format_last_seen(last_seen: Option<Instant>) -> String {
    match last_seen {
        Some(timestamp) => format!("{}s ago", timestamp.elapsed().as_secs()),
        None => "never".to_string(),
    }
}

fn hex_discriminator(discriminator: [u8; 8]) -> String {
    discriminator
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn format_sink_metrics(snapshot: SinkMetricsSnapshot) -> String {
    format!(
        "SinkHealth - sink={} market_ok={} market_err={} close_ok={} close_err={} queued={} buffered_market={} buffered_close={} flushed_market={} flushed_close={} flush_err={} last_flush_ms={} last_error={}",
        snapshot.sink_name,
        snapshot.market_update_successes,
        snapshot.market_update_failures,
        snapshot.close_position_successes,
        snapshot.close_position_failures,
        optional_u64_as_string(snapshot.queued_events),
        optional_u64_as_string(snapshot.buffered_market_updates),
        optional_u64_as_string(snapshot.buffered_close_positions),
        optional_u64_as_string(snapshot.flushed_market_updates),
        optional_u64_as_string(snapshot.flushed_close_positions),
        optional_u64_as_string(snapshot.flush_failures),
        optional_u64_as_string(snapshot.last_flush_latency_ms),
        snapshot.last_error.unwrap_or_else(|| "none".to_string()),
    )
}

fn optional_u64_as_string(value: Option<u64>) -> String {
    value
        .map(|inner| inner.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anchor_lang::Event;

    fn encoded_event<T: Event>(event: &T) -> Vec<u8> {
        event.data()
    }

    #[test]
    fn decodes_and_maps_current_market_update_event() {
        let event = MarketUpdateEvent {
            base_flow: 11,
            quote_flow: 22,
            market_id: u32::MAX,
        };
        let encoded = encoded_event(&event);

        assert_eq!(
            &encoded[..MarketUpdateEvent::DISCRIMINATOR.len()],
            MarketUpdateEvent::DISCRIMINATOR
        );

        let decoded = decode_event(&encoded).expect("market update event should decode");
        let KeeperEvent::MarketUpdate(decoded) = decoded.expect("event should be recognized")
        else {
            panic!("expected MarketUpdateEvent");
        };
        let record = market_update_record("market-signature", 7, 42, &decoded);

        assert_eq!(record.signature, "market-signature");
        assert_eq!(record.event_index, 7);
        assert_eq!(record.slot, 42);
        assert_eq!(record.market_id, u64::from(u32::MAX));
        assert_eq!(record.base_flow, 11);
        assert_eq!(record.quote_flow, 22);
    }

    #[test]
    fn decodes_and_maps_current_close_position_event() {
        let position_address = Pubkey::new_unique();
        let position_authority = Pubkey::new_unique();
        let base_receiver = Pubkey::new_unique();
        let quote_receiver = Pubkey::new_unique();
        let event = ClosePositionEvent {
            position_address,
            position_authority,
            base_receiver,
            quote_receiver,
            deposit_amount: 100,
            swapped_amount: 80,
            remaining_amount: 20,
            fee_amount: 1,
            start_slot: 10,
            end_slot: 30,
            market_id: 4,
            side: Side::Buy,
        };
        let encoded = encoded_event(&event);

        assert_eq!(
            &encoded[..ClosePositionEvent::DISCRIMINATOR.len()],
            ClosePositionEvent::DISCRIMINATOR
        );

        let decoded = decode_event(&encoded).expect("close-position event should decode");
        let KeeperEvent::ClosePosition(decoded) = decoded.expect("event should be recognized")
        else {
            panic!("expected ClosePositionEvent");
        };
        let record = close_position_record("close-signature", 3, 99, &decoded);

        assert_eq!(decoded.position_address, position_address);
        assert_eq!(decoded.base_receiver, base_receiver);
        assert_eq!(decoded.quote_receiver, quote_receiver);
        assert_eq!(record.signature, "close-signature");
        assert_eq!(record.event_index, 3);
        assert_eq!(record.slot, 99);
        assert_eq!(record.position_authority, position_authority.to_string());
        assert_eq!(record.market_id, 4);
        assert_eq!(record.start_slot, 10);
        assert_eq!(record.end_slot, 30);
        assert_eq!(record.deposit_amount, 100);
        assert_eq!(record.swapped_amount, 80);
        assert_eq!(record.remaining_amount, 20);
        assert_eq!(record.fee_amount, 1);
        assert_eq!(record.is_buy, 1);
        assert_eq!(side_to_is_buy(Side::Sell), 0);
    }

    #[test]
    fn recognizes_authority_transfer_and_preserves_event_indexing() {
        let authority_event = AuthorityTransferred {
            new_authority: Pubkey::new_unique(),
        };
        let market_event = MarketUpdateEvent {
            base_flow: 5,
            quote_flow: 10,
            market_id: 2,
        };
        let authority_bytes = encoded_event(&authority_event);
        let market_bytes = encoded_event(&market_event);

        assert_eq!(
            &authority_bytes[..AuthorityTransferred::DISCRIMINATOR.len()],
            AuthorityTransferred::DISCRIMINATOR
        );
        let decoded = decode_event(&authority_bytes).expect("authority event should decode");
        let KeeperEvent::AuthorityTransferred(decoded) =
            decoded.expect("authority event should be recognized")
        else {
            panic!("expected AuthorityTransferred");
        };
        assert_eq!(decoded.new_authority, authority_event.new_authority);

        let program_id = twob_anchor::ID.to_string();
        let logs = vec![
            format!("Program {program_id} invoke [1]"),
            format!("{PROGRAM_DATA_PREFIX}{}", STANDARD.encode(authority_bytes)),
            format!("{PROGRAM_DATA_PREFIX}{}", STANDARD.encode(market_bytes)),
            format!("Program {program_id} success"),
        ];
        let mut stats = IngestStats::new();
        let events = parse_events_from_logs(&program_id, &logs, "signature", 1, &mut stats);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_index, 0);
        assert!(matches!(
            events[0].event,
            KeeperEvent::AuthorityTransferred(_)
        ));
        assert_eq!(events[1].event_index, 1);
        assert!(matches!(events[1].event, KeeperEvent::MarketUpdate(_)));
        assert!(stats.unknown_discriminators.is_empty());
        assert_eq!(stats.decode_errors, 0);
    }
}
