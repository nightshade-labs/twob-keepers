use anyhow::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio_postgres::NoTls;

use crate::sink::{
    ClosePositionEventRecord, EventSink, MarketUpdateEventRecord, SinkFuture, SinkMetricsSnapshot,
};

pub struct Database {
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

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self> {
        // Parse the database URL
        let config: tokio_postgres::Config = database_url
            .parse()
            .context("Failed to parse database URL")?;

        // Create deadpool config
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(config, NoTls, manager_config);
        let pool = Pool::builder(manager)
            .max_size(16)
            .build()
            .context("Failed to create connection pool")?;

        // Test the connection
        let _ = pool
            .get()
            .await
            .context("Failed to get connection from pool")?;

        Ok(Self {
            pool,
            metrics: Arc::new(DatabaseMetrics::default()),
        })
    }

    pub async fn insert_market_update_event(
        &self,
        signature: &str,
        slot: u64,
        market_id: u64,
        base_flow: u64,
        quote_flow: u64,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get connection")?;

        let result = client
            .execute(
                "INSERT INTO market_update_events (signature, slot, market_id, base_flow, quote_flow)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (signature) DO NOTHING",
                &[
                    &signature,
                    &(slot as i64),
                    &(market_id as i64),
                    &(base_flow as i64),
                    &(quote_flow as i64),
                ],
            )
            .await;

        match result {
            Ok(_) => {
                self.metrics
                    .market_update_successes
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.metrics
                    .market_update_failures
                    .fetch_add(1, Ordering::Relaxed);
                {
                    let mut guard = self.metrics.last_error.lock().expect("mutex poisoned");
                    *guard = Some(format!("market_update insert failure: {e}"));
                }
                eprintln!("Database error details: {:?}", e);
                if let Some(db_err) = e.as_db_error() {
                    eprintln!("  Code: {:?}", db_err.code());
                    eprintln!("  Message: {}", db_err.message());
                    eprintln!("  Detail: {:?}", db_err.detail());
                    eprintln!("  Hint: {:?}", db_err.hint());
                }
                Err(anyhow::anyhow!(
                    "Failed to insert market update event: {}",
                    e
                ))
            }
        }
    }

    pub async fn insert_close_position_event(
        &self,
        signature: &str,
        slot: u64,
        position_authority: &str,
        market_id: u64,
        start_slot: u64,
        end_slot: u64,
        deposit_amount: u64,
        swapped_amount: u64,
        remaining_amount: u64,
        fee_amount: u64,
        is_buy: u8,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get connection")?;

        let result = client
            .execute(
                "INSERT INTO close_position_events
                 (signature, slot, position_authority, market_id, start_slot, end_slot, deposit_amount, swapped_amount, remaining_amount, fee_amount, is_buy)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT (signature) DO NOTHING",
                &[
                    &signature,
                    &(slot as i64),
                    &position_authority,
                    &(market_id as i64),
                    &(start_slot as i64),
                    &(end_slot as i64),
                    &(deposit_amount as i64),
                    &(swapped_amount as i64),
                    &(remaining_amount as i64),
                    &(fee_amount as i64),
                    &(is_buy as i16),
                ],
            )
            .await;

        match result {
            Ok(_) => {
                self.metrics
                    .close_position_successes
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.metrics
                    .close_position_failures
                    .fetch_add(1, Ordering::Relaxed);
                {
                    let mut guard = self.metrics.last_error.lock().expect("mutex poisoned");
                    *guard = Some(format!("close_position insert failure: {e}"));
                }
                eprintln!("Database error details: {:?}", e);
                if let Some(db_err) = e.as_db_error() {
                    eprintln!("  Code: {:?}", db_err.code());
                    eprintln!("  Message: {}", db_err.message());
                    eprintln!("  Detail: {:?}", db_err.detail());
                    eprintln!("  Hint: {:?}", db_err.hint());
                }
                Err(anyhow::anyhow!(
                    "Failed to insert close position event: {}",
                    e
                ))
            }
        }
    }
}

impl EventSink for Database {
    fn sink_name(&self) -> &'static str {
        "postgres"
    }

    fn insert_market_update_event(&self, event: MarketUpdateEventRecord) -> SinkFuture<'_> {
        Box::pin(async move {
            Database::insert_market_update_event(
                self,
                &event.signature,
                event.slot,
                event.market_id,
                event.base_flow,
                event.quote_flow,
            )
            .await
        })
    }

    fn insert_close_position_event(&self, event: ClosePositionEventRecord) -> SinkFuture<'_> {
        Box::pin(async move {
            Database::insert_close_position_event(
                self,
                &event.signature,
                event.slot,
                &event.position_authority,
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
