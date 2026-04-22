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
    ClickHouseSink, ClickHouseSinkConfig, ClosePositionEventRecord, Database, EventSink,
    FanoutSink, MarketUpdateEventRecord, SinkMetricsSnapshot,
};

declare_program!(twob_anchor);
use twob_anchor::events::*;

const PROGRAM_LOG_PREFIX: &str = "Program log: ";
const PROGRAM_DATA_PREFIX: &str = "Program data: ";

#[derive(Debug)]
enum KeeperEvent {
    MarketUpdate(MarketUpdateEvent),
    ClosePosition(ClosePositionEvent),
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

    let mut sinks: Vec<Arc<dyn EventSink>> = Vec::new();

    let database_url = env::var("DATABASE_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            env::var("DATABASE_PASSWORD").ok().map(|database_password| {
                format!(
                    "postgresql://postgres.xzlbpjbsuyrjoijmmtom:{}@aws-1-eu-west-1.pooler.supabase.com:5432/postgres",
                    database_password
                )
            })
        });

    if let Some(database_url) = database_url {
        let postgres_sink: Arc<dyn EventSink> = Arc::new(Database::connect(&database_url).await?);
        sinks.push(postgres_sink);
        println!("Connected to Postgres sink");
    } else {
        println!("Postgres sink is disabled (set DATABASE_URL to enable)");
    }

    if let Some(clickhouse_config) = ClickHouseSinkConfig::from_env_optional()? {
        let clickhouse_sink: Arc<dyn EventSink> =
            Arc::new(ClickHouseSink::connect(clickhouse_config).await?);
        sinks.push(clickhouse_sink);
        println!("Connected to ClickHouse sink");
    } else {
        println!("CLICKHOUSE_URL is not set; ClickHouse sink is disabled");
    }

    if sinks.is_empty() {
        return Err(anyhow!(
            "No sinks configured. Set CLICKHOUSE_URL and/or DATABASE_URL"
        ));
    }

    let sink: Arc<dyn EventSink> = if sinks.len() == 1 {
        sinks
            .into_iter()
            .next()
            .expect("sinks contains one configured sink")
    } else {
        Arc::new(FanoutSink::new(sinks))
    };

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

                let record = MarketUpdateEventRecord {
                    signature: signature.clone(),
                    event_index: indexed_event.event_index,
                    slot,
                    market_id: event.market_id,
                    base_flow: event.base_flow,
                    quote_flow: event.quote_flow,
                };

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

                let record = ClosePositionEventRecord {
                    signature: signature.clone(),
                    event_index: indexed_event.event_index,
                    slot,
                    position_authority: event.position_authority.to_string(),
                    market_id: event.market_id,
                    start_slot: event.start_slot,
                    end_slot: event.end_slot,
                    deposit_amount: event.deposit_amount,
                    swapped_amount: event.swapped_amount,
                    remaining_amount: event.remaining_amount,
                    fee_amount: event.fee_amount,
                    is_buy: event.is_buy,
                };

                if let Err(error) = sink.insert_close_position_event(record).await {
                    stats.record_db_error();
                    eprintln!("Failed to insert close position event via sink: {error}");
                }
            }
        }
    }

    Ok(())
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
