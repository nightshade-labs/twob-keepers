use anchor_client::solana_sdk::commitment_config::CommitmentConfig;
use anchor_lang::{AnchorDeserialize, Discriminator, prelude::*};
use anyhow::Context;
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
use twob_keepers::Database;

declare_program!(twob_anchor);
use twob_anchor::events::*;

const PROGRAM_LOG_PREFIX: &str = "Program log: ";
const PROGRAM_DATA_PREFIX: &str = "Program data: ";

#[derive(Debug)]
enum KeeperEvent {
    MarketUpdate(MarketUpdateEvent),
    ClosePosition(ClosePositionEvent),
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

    fn log_health(&self) {
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
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let database_password =
        std::env::var("DATABASE_PASSWORD").expect("DATABASE_PASSWORD must be set");

    let database_url = format!(
        "postgresql://postgres.xzlbpjbsuyrjoijmmtom:{}@aws-1-eu-west-1.pooler.supabase.com:5432/postgres",
        database_password
    );

    let db = Arc::new(Database::connect(&database_url).await?);
    println!("Connected to database");

    let ws_url = env::var("CLUSTER_WS_URL").expect("CLUSTER_WS_URL must be set");
    let program_id = twob_anchor::ID.to_string();

    let mut backoff = Duration::from_secs(1);

    loop {
        println!(
            "Subscribing to transaction logs for program {} on {}",
            program_id, ws_url
        );

        match run_subscription(&ws_url, &program_id, db.clone()).await {
            Ok(()) => eprintln!("Log subscription ended, reconnecting"),
            Err(error) => eprintln!("Log subscription failed: {error:#}"),
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn run_subscription(ws_url: &str, program_id: &str, db: Arc<Database>) -> anyhow::Result<()> {
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

                if let Err(error) = handle_logs_notification(db.as_ref(), program_id, notification, &mut stats).await {
                    eprintln!("Failed to handle log notification: {error:#}");
                }
            }
            _ = heartbeat.tick() => {
                stats.log_health();
            }
        }
    }

    unsubscribe().await;
    Ok(())
}

async fn handle_logs_notification(
    db: &Database,
    program_id: &str,
    notification: RpcResponse<RpcLogsResponse>,
    stats: &mut IngestStats,
) -> anyhow::Result<()> {
    let slot = notification.context.slot;
    let signature = notification.value.signature;

    for event in parse_events_from_logs(
        program_id,
        &notification.value.logs,
        &signature,
        slot,
        stats,
    ) {
        match event {
            KeeperEvent::MarketUpdate(event) => {
                println!(
                    "MarketUpdateEvent - Signature: {}, Slot: {}, Market: {}",
                    signature, slot, event.market_id
                );
                stats.record_market_event();

                if let Err(error) = db
                    .insert_market_update_event(
                        &signature,
                        slot,
                        event.market_id,
                        event.base_flow,
                        event.quote_flow,
                    )
                    .await
                {
                    stats.record_db_error();
                    eprintln!("Failed to insert market update event: {error}");
                }
            }
            KeeperEvent::ClosePosition(event) => {
                println!(
                    "ClosePositionEvent - Signature: {}, Slot: {}, Market: {}",
                    signature, slot, event.market_id
                );
                stats.record_close_event();

                if let Err(error) = db
                    .insert_close_position_event(
                        &signature,
                        slot,
                        &event.position_authority.to_string(),
                        event.market_id,
                        event.start_slot,
                        event.end_slot,
                        event.deposit_amount,
                        event.swapped_amount,
                        event.remaining_amount,
                        event.fee_amount,
                        event.is_buy,
                    )
                    .await
                {
                    stats.record_db_error();
                    eprintln!("Failed to insert close position event: {error}");
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
) -> Vec<KeeperEvent> {
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
            Ok(Some(event)) => events.push(event),
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
