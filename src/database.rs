use anyhow::{Context, Result};
use chrono::Utc;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::sink::{
    ClosePositionEventRecord, EventSink, MarketUpdateEventRecord, SinkFuture, SinkMetricsSnapshot,
};

/// Insert the raw market-update event and, in the same statement, recompute the
/// affected 1-minute candle.
///
/// Price is computed in SQL at full `numeric` precision by joining
/// `v1_market_configs` for the token decimals, so the keeper never needs to read
/// values back or carry decimals in process memory.
///
/// Candle semantics (step-function price that persists between updates):
/// - `open` of a freshly created bucket carries forward the previous bucket's
///   `close` (derived from the table itself, so it is restart-safe).
/// - `high`/`low` use `GREATEST`/`LEAST` and are order-independent.
/// - `close` is the latest event's price (last write wins).
///
/// If the raw insert hits `ON CONFLICT DO NOTHING` (duplicate) or the market has
/// no `v1_market_configs` row, the candle CTE simply produces no row and the candle
/// is left untouched.
const INSERT_MARKET_UPDATE_SQL: &str = "\
WITH ev AS ( \
    INSERT INTO v1_raw_market_update_events \
        (event_uid, signature, event_index, slot, market_address, base_flow, quote_flow, event_time) \
    VALUES ($1, $2, $3, $4, $5, $6::text::numeric, $7::text::numeric, $8) \
    ON CONFLICT DO NOTHING \
    RETURNING market_address, base_flow, quote_flow, event_time \
), \
p AS ( \
    SELECT \
        ev.market_address, \
        date_trunc('minute', ev.event_time) AS bucket_start, \
        (ev.quote_flow::numeric * power(10::numeric, mc.base_decimals::numeric)) \
            / (ev.base_flow::numeric * power(10::numeric, mc.quote_decimals::numeric)) AS price \
    FROM ev \
    JOIN v1_market_configs mc ON mc.market_address = ev.market_address \
    WHERE ev.base_flow <> 0 \
      AND mc.base_decimals IS NOT NULL \
      AND mc.quote_decimals IS NOT NULL \
) \
INSERT INTO v1_market_candles_1m (market_address, bucket_start, open, high, low, close, updated_at) \
SELECT \
    p.market_address, \
    p.bucket_start, \
    COALESCE( \
        (SELECT c.close FROM v1_market_candles_1m c \
          WHERE c.market_address = p.market_address AND c.bucket_start < p.bucket_start \
          ORDER BY c.bucket_start DESC LIMIT 1), \
        p.price), \
    p.price, p.price, p.price, now() \
FROM p \
ON CONFLICT (market_address, bucket_start) DO UPDATE SET \
    high  = GREATEST(v1_market_candles_1m.high, EXCLUDED.close), \
    low   = LEAST(v1_market_candles_1m.low,  EXCLUDED.close), \
    close = EXCLUDED.close, \
    updated_at = now()";

const INSERT_CLOSE_POSITION_SQL: &str = "\
INSERT INTO v1_raw_close_position_events \
    (event_uid, signature, event_index, slot, position_authority, market_address, start_slot, \
     end_slot, deposit_amount, swapped_amount, remaining_amount, fee_amount, is_buy, event_time, position_address, base_receiver, quote_receiver) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::text::numeric, $10::text::numeric, $11::text::numeric, $12::text::numeric, $13, $14, $15, $16, $17) \
ON CONFLICT DO NOTHING";

/// Build a TLS-enabled connection pool for Tiger Cloud (Timescale).
///
/// Tiger Cloud requires TLS, so connections go through a native-TLS connector.
/// Use `?sslmode=require` in the connection string.
pub fn connect_pool(database_url: &str, max_size: usize) -> Result<Pool> {
    let config: tokio_postgres::Config = database_url
        .parse()
        .context("Failed to parse database URL")?;

    let tls_connector = TlsConnector::builder()
        .build()
        .context("Failed to build native TLS connector")?;
    let tls = MakeTlsConnector::new(tls_connector);

    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let manager = Manager::from_config(config, tls, manager_config);
    Pool::builder(manager)
        .max_size(max_size)
        .build()
        .context("Failed to create connection pool")
}

pub struct TimescaleSink {
    pool: Pool,
    metrics: Arc<DatabaseMetrics>,
}

#[derive(Default)]
struct DatabaseMetrics {
    market_update_successes: AtomicU64,
    market_update_failures: AtomicU64,
    close_position_successes: AtomicU64,
    close_position_failures: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl TimescaleSink {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = connect_pool(database_url, 16)?;

        // Verify the connection (and TLS handshake) eagerly.
        let client = pool
            .get()
            .await
            .context("Failed to get connection from pool")?;
        client
            .simple_query("SELECT 1")
            .await
            .context("Failed to verify Tiger Cloud connection")?;

        Ok(Self {
            pool,
            metrics: Arc::new(DatabaseMetrics::default()),
        })
    }

    async fn insert_market_update(&self, event: &MarketUpdateEventRecord) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get connection")?;
        client
            .execute(
                INSERT_MARKET_UPDATE_SQL,
                &[
                    &event.event_uid(),
                    &event.signature,
                    &(event.event_index as i32),
                    &(event.slot as i64),
                    &event.market_address,
                    &event.base_flow.to_string(),
                    &event.quote_flow.to_string(),
                    &Utc::now(),
                ],
            )
            .await
            .context("Failed to insert market update event")?;
        Ok(())
    }

    async fn insert_close_position(&self, event: &ClosePositionEventRecord) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get connection")?;
        client
            .execute(
                INSERT_CLOSE_POSITION_SQL,
                &[
                    &event.event_uid(),
                    &event.signature,
                    &(event.event_index as i32),
                    &(event.slot as i64),
                    &event.position_authority,
                    &event.market_address,
                    &(event.start_slot as i64),
                    &(event.end_slot as i64),
                    &event.deposit_amount.to_string(),
                    &event.swapped_amount.to_string(),
                    &event.remaining_amount.to_string(),
                    &event.fee_amount.to_string(),
                    &(event.is_buy != 0),
                    &Utc::now(),
                    &event.position_address,
                    &event.base_receiver,
                    &event.quote_receiver,
                ],
            )
            .await
            .context("Failed to insert close position event")?;
        Ok(())
    }
}

impl EventSink for TimescaleSink {
    fn sink_name(&self) -> &'static str {
        "timescale"
    }

    fn insert_market_update_event(&self, event: MarketUpdateEventRecord) -> SinkFuture<'_> {
        Box::pin(async move {
            match self.insert_market_update(&event).await {
                Ok(()) => {
                    self.metrics
                        .market_update_successes
                        .fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(error) => {
                    self.metrics
                        .market_update_failures
                        .fetch_add(1, Ordering::Relaxed);
                    {
                        let mut guard = self.metrics.last_error.lock().expect("mutex poisoned");
                        *guard = Some(format!("market_update insert failure: {error:#}"));
                    }
                    Err(error)
                }
            }
        })
    }

    fn insert_close_position_event(&self, event: ClosePositionEventRecord) -> SinkFuture<'_> {
        Box::pin(async move {
            match self.insert_close_position(&event).await {
                Ok(()) => {
                    self.metrics
                        .close_position_successes
                        .fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(error) => {
                    self.metrics
                        .close_position_failures
                        .fetch_add(1, Ordering::Relaxed);
                    {
                        let mut guard = self.metrics.last_error.lock().expect("mutex poisoned");
                        *guard = Some(format!("close_position insert failure: {error:#}"));
                    }
                    Err(error)
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
            last_error: self
                .metrics
                .last_error
                .lock()
                .expect("mutex poisoned")
                .clone(),
            ..SinkMetricsSnapshot::default()
        }]
    }
}
