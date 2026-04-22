use anyhow::{Context, Result, anyhow};
use clickhouse::{Client, Row};
use serde::Serialize;
use std::{
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc,
    time::{MissedTickBehavior, interval},
};

use crate::sink::{
    ClosePositionEventRecord, EventSink, MarketUpdateEventRecord, SinkFuture, SinkMetricsSnapshot,
};

const DEFAULT_DATABASE: &str = "mato";
const DEFAULT_USER: &str = "default";
const DEFAULT_MARKET_UPDATES_TABLE: &str = "raw_market_update_events";
const DEFAULT_CLOSE_POSITIONS_TABLE: &str = "raw_close_position_events";
const DEFAULT_CHANNEL_CAPACITY: usize = 20_000;
const DEFAULT_BATCH_SIZE: usize = 1_000;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Clone)]
pub struct ClickHouseSinkConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub market_updates_table: String,
    pub close_positions_table: String,
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
}

impl ClickHouseSinkConfig {
    pub fn from_env_optional() -> Result<Option<Self>> {
        let raw_url = match env::var("CLICKHOUSE_URL") {
            Ok(raw_url) => raw_url,
            Err(env::VarError::NotPresent) => return Ok(None),
            Err(error) => return Err(anyhow!("Failed to read CLICKHOUSE_URL: {error}")),
        };

        let url = raw_url.trim();
        if url.is_empty() {
            return Ok(None);
        }

        let database =
            env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| DEFAULT_DATABASE.to_string());
        let user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| DEFAULT_USER.to_string());
        let password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
        let market_updates_table = env::var("CLICKHOUSE_MARKET_UPDATES_TABLE")
            .unwrap_or_else(|_| DEFAULT_MARKET_UPDATES_TABLE.to_string());
        let close_positions_table = env::var("CLICKHOUSE_CLOSE_POSITIONS_TABLE")
            .unwrap_or_else(|_| DEFAULT_CLOSE_POSITIONS_TABLE.to_string());

        let channel_capacity =
            parse_usize_env("CLICKHOUSE_CHANNEL_CAPACITY", DEFAULT_CHANNEL_CAPACITY)?;
        let batch_size = parse_usize_env("CLICKHOUSE_BATCH_SIZE", DEFAULT_BATCH_SIZE)?;
        if batch_size == 0 {
            return Err(anyhow!("CLICKHOUSE_BATCH_SIZE must be greater than 0"));
        }

        let flush_interval_ms =
            parse_u64_env("CLICKHOUSE_FLUSH_INTERVAL_MS", DEFAULT_FLUSH_INTERVAL_MS)?;
        if flush_interval_ms == 0 {
            return Err(anyhow!(
                "CLICKHOUSE_FLUSH_INTERVAL_MS must be greater than 0"
            ));
        }

        Ok(Some(Self {
            url: url.to_string(),
            database,
            user,
            password,
            market_updates_table,
            close_positions_table,
            channel_capacity,
            batch_size,
            flush_interval: Duration::from_millis(flush_interval_ms),
        }))
    }
}

fn parse_usize_env(key: &str, default_value: usize) -> Result<usize> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<usize>()
            .with_context(|| format!("{key} must be a valid positive integer")),
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(error) => Err(anyhow!("Failed to read {key}: {error}")),
    }
}

fn parse_u64_env(key: &str, default_value: u64) -> Result<u64> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("{key} must be a valid positive integer")),
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(error) => Err(anyhow!("Failed to read {key}: {error}")),
    }
}

enum ClickHouseEvent {
    MarketUpdate(MarketUpdateEventRecord),
    ClosePosition(ClosePositionEventRecord),
}

#[derive(Clone)]
pub struct ClickHouseSink {
    sender: mpsc::Sender<ClickHouseEvent>,
    metrics: Arc<ClickHouseMetrics>,
}

#[derive(Default)]
struct ClickHouseMetrics {
    market_update_successes: AtomicU64,
    market_update_failures: AtomicU64,
    close_position_successes: AtomicU64,
    close_position_failures: AtomicU64,
    queued_events: AtomicUsize,
    buffered_market_updates: AtomicUsize,
    buffered_close_positions: AtomicUsize,
    flushed_market_updates: AtomicU64,
    flushed_close_positions: AtomicU64,
    flush_failures: AtomicU64,
    last_flush_latency_ms: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl ClickHouseSink {
    pub async fn connect(config: ClickHouseSinkConfig) -> Result<Self> {
        let client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.user)
            .with_password(&config.password);

        client
            .query("SELECT 1")
            .execute()
            .await
            .context("Failed to connect to ClickHouse")?;

        ensure_table_exists(&client, &config.database, &config.market_updates_table)
            .await
            .with_context(|| {
                format!(
                    "Missing ClickHouse table for market updates: {}",
                    qualify_table_name(&config.database, &config.market_updates_table)
                )
            })?;
        ensure_table_exists(&client, &config.database, &config.close_positions_table)
            .await
            .with_context(|| {
                format!(
                    "Missing ClickHouse table for close positions: {}",
                    qualify_table_name(&config.database, &config.close_positions_table)
                )
            })?;

        let metrics = Arc::new(ClickHouseMetrics::default());
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        tokio::spawn(run_worker(client, config, receiver, metrics.clone()));

        Ok(Self { sender, metrics })
    }
}

impl EventSink for ClickHouseSink {
    fn sink_name(&self) -> &'static str {
        "clickhouse"
    }

    fn insert_market_update_event(&self, event: MarketUpdateEventRecord) -> SinkFuture<'_> {
        let sender = self.sender.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let send_result = sender.send(ClickHouseEvent::MarketUpdate(event)).await;

            match send_result {
                Ok(()) => {
                    metrics
                        .market_update_successes
                        .fetch_add(1, Ordering::Relaxed);
                    metrics.queued_events.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(error) => {
                    metrics
                        .market_update_failures
                        .fetch_add(1, Ordering::Relaxed);
                    {
                        let mut guard = metrics.last_error.lock().expect("mutex poisoned");
                        *guard = Some(format!("enqueue market-update event failure: {}", error));
                    }
                    Err(anyhow!(
                        "Failed to enqueue market update for ClickHouse: {error}"
                    ))
                }
            }
        })
    }

    fn insert_close_position_event(&self, event: ClosePositionEventRecord) -> SinkFuture<'_> {
        let sender = self.sender.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let send_result = sender.send(ClickHouseEvent::ClosePosition(event)).await;

            match send_result {
                Ok(()) => {
                    metrics
                        .close_position_successes
                        .fetch_add(1, Ordering::Relaxed);
                    metrics.queued_events.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(error) => {
                    metrics
                        .close_position_failures
                        .fetch_add(1, Ordering::Relaxed);
                    {
                        let mut guard = metrics.last_error.lock().expect("mutex poisoned");
                        *guard = Some(format!("enqueue close-position event failure: {}", error));
                    }
                    Err(anyhow!(
                        "Failed to enqueue close-position event for ClickHouse: {error}"
                    ))
                }
            }
        })
    }

    fn metrics_snapshot(&self) -> Vec<SinkMetricsSnapshot> {
        vec![SinkMetricsSnapshot {
            sink_name: self.sink_name().to_string(),
            market_update_successes: self.metrics.market_update_successes.load(Ordering::Relaxed),
            market_update_failures: self.metrics.market_update_failures.load(Ordering::Relaxed),
            close_position_successes: self
                .metrics
                .close_position_successes
                .load(Ordering::Relaxed),
            close_position_failures: self.metrics.close_position_failures.load(Ordering::Relaxed),
            queued_events: Some(self.metrics.queued_events.load(Ordering::Relaxed) as u64),
            buffered_market_updates: Some(
                self.metrics.buffered_market_updates.load(Ordering::Relaxed) as u64,
            ),
            buffered_close_positions: Some(
                self.metrics
                    .buffered_close_positions
                    .load(Ordering::Relaxed) as u64,
            ),
            flushed_market_updates: Some(
                self.metrics.flushed_market_updates.load(Ordering::Relaxed),
            ),
            flushed_close_positions: Some(
                self.metrics.flushed_close_positions.load(Ordering::Relaxed),
            ),
            flush_failures: Some(self.metrics.flush_failures.load(Ordering::Relaxed)),
            last_flush_latency_ms: Some(self.metrics.last_flush_latency_ms.load(Ordering::Relaxed)),
            last_error: self
                .metrics
                .last_error
                .lock()
                .expect("mutex poisoned")
                .clone(),
        }]
    }
}

#[derive(Row, Serialize)]
struct MarketUpdateInsertRow {
    event_uid: String,
    signature: String,
    event_index: u16,
    slot: u64,
    market_id: u64,
    base_flow: u64,
    quote_flow: u64,
}

#[derive(Row, Serialize)]
struct ClosePositionInsertRow {
    event_uid: String,
    signature: String,
    event_index: u16,
    slot: u64,
    position_authority: String,
    market_id: u64,
    start_slot: u64,
    end_slot: u64,
    deposit_amount: u64,
    swapped_amount: u64,
    remaining_amount: u64,
    fee_amount: u64,
    is_buy: u8,
}

async fn run_worker(
    client: Client,
    config: ClickHouseSinkConfig,
    mut receiver: mpsc::Receiver<ClickHouseEvent>,
    metrics: Arc<ClickHouseMetrics>,
) {
    let mut market_updates_batch: Vec<MarketUpdateEventRecord> =
        Vec::with_capacity(config.batch_size);
    let mut close_positions_batch: Vec<ClosePositionEventRecord> =
        Vec::with_capacity(config.batch_size);
    let mut ticker = interval(config.flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_event = receiver.recv() => {
                match maybe_event {
                    Some(ClickHouseEvent::MarketUpdate(event)) => {
                        decrement_atomic_usize(&metrics.queued_events);
                        market_updates_batch.push(event);
                        metrics
                            .buffered_market_updates
                            .store(market_updates_batch.len(), Ordering::Relaxed);
                    }
                    Some(ClickHouseEvent::ClosePosition(event)) => {
                        decrement_atomic_usize(&metrics.queued_events);
                        close_positions_batch.push(event);
                        metrics
                            .buffered_close_positions
                            .store(close_positions_batch.len(), Ordering::Relaxed);
                    }
                    None => {
                        flush_batches(
                            &client,
                            &config,
                            &mut market_updates_batch,
                            &mut close_positions_batch,
                            metrics.as_ref(),
                        ).await;
                        break;
                    }
                }

                if market_updates_batch.len() >= config.batch_size
                    || close_positions_batch.len() >= config.batch_size
                {
                    flush_batches(
                        &client,
                        &config,
                        &mut market_updates_batch,
                        &mut close_positions_batch,
                        metrics.as_ref(),
                    )
                    .await;
                }
            }
            _ = ticker.tick() => {
                flush_batches(
                    &client,
                    &config,
                    &mut market_updates_batch,
                    &mut close_positions_batch,
                    metrics.as_ref(),
                ).await;
            }
        }
    }
}

fn decrement_atomic_usize(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

async fn flush_batches(
    client: &Client,
    config: &ClickHouseSinkConfig,
    market_updates_batch: &mut Vec<MarketUpdateEventRecord>,
    close_positions_batch: &mut Vec<ClosePositionEventRecord>,
    metrics: &ClickHouseMetrics,
) {
    if !market_updates_batch.is_empty() {
        let market_updates_count = market_updates_batch.len() as u64;
        let started_at = Instant::now();
        if let Err(error) =
            flush_market_updates(
                client,
                &config.database,
                &config.market_updates_table,
                market_updates_batch,
            )
            .await
        {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            {
                let mut guard = metrics.last_error.lock().expect("mutex poisoned");
                *guard = Some(format!(
                    "flush market-update batch failure for table {}: {:#}",
                    qualify_table_name(&config.database, &config.market_updates_table),
                    error
                ));
            }
            eprintln!(
                "Failed to flush {} market-update event(s) to ClickHouse: {:#}",
                market_updates_batch.len(),
                error
            );
        } else {
            metrics
                .flushed_market_updates
                .fetch_add(market_updates_count, Ordering::Relaxed);
            market_updates_batch.clear();
            metrics.buffered_market_updates.store(0, Ordering::Relaxed);
        }
        metrics.last_flush_latency_ms.store(
            started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    if !close_positions_batch.is_empty() {
        let close_positions_count = close_positions_batch.len() as u64;
        let started_at = Instant::now();
        if let Err(error) =
            flush_close_positions(
                client,
                &config.database,
                &config.close_positions_table,
                close_positions_batch,
            )
            .await
        {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            {
                let mut guard = metrics.last_error.lock().expect("mutex poisoned");
                *guard = Some(format!(
                    "flush close-position batch failure for table {}: {:#}",
                    qualify_table_name(&config.database, &config.close_positions_table),
                    error
                ));
            }
            eprintln!(
                "Failed to flush {} close-position event(s) to ClickHouse: {:#}",
                close_positions_batch.len(),
                error
            );
        } else {
            metrics
                .flushed_close_positions
                .fetch_add(close_positions_count, Ordering::Relaxed);
            close_positions_batch.clear();
            metrics.buffered_close_positions.store(0, Ordering::Relaxed);
        }
        metrics.last_flush_latency_ms.store(
            started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

async fn flush_market_updates(
    client: &Client,
    database: &str,
    table: &str,
    batch: &[MarketUpdateEventRecord],
) -> Result<()> {
    let qualified_table = qualify_table_name(database, table);
    let mut insert = client
        .insert::<MarketUpdateInsertRow>(&qualified_table)
        .with_context(|| format!("Failed to start ClickHouse insert for table {qualified_table}"))?;

    for event in batch {
        let row = MarketUpdateInsertRow {
            event_uid: event.event_uid(),
            signature: event.signature.clone(),
            event_index: event.event_index,
            slot: event.slot,
            market_id: event.market_id,
            base_flow: event.base_flow,
            quote_flow: event.quote_flow,
        };
        insert
            .write(&row)
            .await
            .with_context(|| {
                format!(
                    "Failed to write market-update row into table {qualified_table}"
                )
            })?;
    }

    insert
        .end()
        .await
        .with_context(|| {
            format!(
                "Failed to finalize market-update insert into table {qualified_table}"
            )
        })?;
    Ok(())
}

async fn flush_close_positions(
    client: &Client,
    database: &str,
    table: &str,
    batch: &[ClosePositionEventRecord],
) -> Result<()> {
    let qualified_table = qualify_table_name(database, table);
    let mut insert = client
        .insert::<ClosePositionInsertRow>(&qualified_table)
        .with_context(|| format!("Failed to start ClickHouse insert for table {qualified_table}"))?;

    for event in batch {
        let row = ClosePositionInsertRow {
            event_uid: event.event_uid(),
            signature: event.signature.clone(),
            event_index: event.event_index,
            slot: event.slot,
            position_authority: event.position_authority.clone(),
            market_id: event.market_id,
            start_slot: event.start_slot,
            end_slot: event.end_slot,
            deposit_amount: event.deposit_amount,
            swapped_amount: event.swapped_amount,
            remaining_amount: event.remaining_amount,
            fee_amount: event.fee_amount,
            is_buy: event.is_buy,
        };
        insert
            .write(&row)
            .await
            .with_context(|| {
                format!(
                    "Failed to write close-position row into table {qualified_table}"
                )
            })?;
    }

    insert
        .end()
        .await
        .with_context(|| {
            format!(
                "Failed to finalize close-position insert into table {qualified_table}"
            )
        })?;
    Ok(())
}

fn qualify_table_name(database: &str, table: &str) -> String {
    if table.contains('.') {
        table.to_string()
    } else {
        format!("{database}.{table}")
    }
}

async fn ensure_table_exists(client: &Client, database: &str, table: &str) -> Result<()> {
    let qualified_table = qualify_table_name(database, table);
    let query = format!("EXISTS TABLE {qualified_table}");
    let exists = client
        .query(&query)
        .fetch_one::<u8>()
        .await
        .with_context(|| format!("Failed to check table existence with query: {query}"))?;

    if exists == 1 {
        Ok(())
    } else {
        Err(anyhow!("Table does not exist: {qualified_table}"))
    }
}
