use anyhow::{Result, anyhow};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Clone, Debug)]
pub struct MarketUpdateEventRecord {
    pub signature: String,
    pub event_index: u16,
    pub slot: u64,
    pub market_id: u64,
    pub base_flow: u64,
    pub quote_flow: u64,
}

impl MarketUpdateEventRecord {
    pub fn event_uid(&self) -> String {
        format!(
            "market_update:{}:{}",
            self.signature.as_str(),
            self.event_index
        )
    }
}

#[derive(Clone, Debug)]
pub struct ClosePositionEventRecord {
    pub signature: String,
    pub event_index: u16,
    pub slot: u64,
    pub position_authority: String,
    pub market_id: u64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub deposit_amount: u64,
    pub swapped_amount: u64,
    pub remaining_amount: u64,
    pub fee_amount: u64,
    pub is_buy: u8,
}

impl ClosePositionEventRecord {
    pub fn event_uid(&self) -> String {
        format!(
            "close_position:{}:{}",
            self.signature.as_str(),
            self.event_index
        )
    }
}

pub type SinkFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

#[derive(Clone, Debug, Default)]
pub struct SinkMetricsSnapshot {
    pub sink_name: String,
    pub market_update_successes: u64,
    pub market_update_failures: u64,
    pub close_position_successes: u64,
    pub close_position_failures: u64,
    pub queued_events: Option<u64>,
    pub buffered_market_updates: Option<u64>,
    pub buffered_close_positions: Option<u64>,
    pub flushed_market_updates: Option<u64>,
    pub flushed_close_positions: Option<u64>,
    pub flush_failures: Option<u64>,
    pub last_flush_latency_ms: Option<u64>,
    pub last_error: Option<String>,
}

pub trait EventSink: Send + Sync {
    fn sink_name(&self) -> &'static str;

    fn insert_market_update_event(&self, event: MarketUpdateEventRecord) -> SinkFuture<'_>;

    fn insert_close_position_event(&self, event: ClosePositionEventRecord) -> SinkFuture<'_>;

    fn metrics_snapshot(&self) -> Vec<SinkMetricsSnapshot> {
        Vec::new()
    }
}

pub struct FanoutSink {
    sinks: Vec<Arc<dyn EventSink>>,
    metrics: Arc<FanoutMetrics>,
}

#[derive(Default)]
struct FanoutMetrics {
    market_update_successes: AtomicU64,
    market_update_failures: AtomicU64,
    close_position_successes: AtomicU64,
    close_position_failures: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl FanoutSink {
    pub fn new(sinks: Vec<Arc<dyn EventSink>>) -> Self {
        Self {
            sinks,
            metrics: Arc::new(FanoutMetrics::default()),
        }
    }

    pub fn len(&self) -> usize {
        self.sinks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }
}

impl EventSink for FanoutSink {
    fn sink_name(&self) -> &'static str {
        "fanout"
    }

    fn insert_market_update_event(&self, event: MarketUpdateEventRecord) -> SinkFuture<'_> {
        Box::pin(async move {
            if self.sinks.is_empty() {
                return Err(anyhow!("FanoutSink has no downstream sinks configured"));
            }

            let mut failures: Vec<String> = Vec::new();

            for sink in &self.sinks {
                if let Err(error) = sink.insert_market_update_event(event.clone()).await {
                    failures.push(format!("{}: {}", sink.sink_name(), error));
                }
            }

            if failures.is_empty() {
                self.metrics
                    .market_update_successes
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
                let joined_failures = failures.join(" | ");
                self.metrics
                    .market_update_failures
                    .fetch_add(1, Ordering::Relaxed);
                {
                    let mut guard = self.metrics.last_error.lock().expect("mutex poisoned");
                    *guard = Some(joined_failures.clone());
                }

                Err(anyhow!(
                    "Failed to write market update to {} sink(s): {}",
                    failures.len(),
                    joined_failures
                ))
            }
        })
    }

    fn insert_close_position_event(&self, event: ClosePositionEventRecord) -> SinkFuture<'_> {
        Box::pin(async move {
            if self.sinks.is_empty() {
                return Err(anyhow!("FanoutSink has no downstream sinks configured"));
            }

            let mut failures: Vec<String> = Vec::new();

            for sink in &self.sinks {
                if let Err(error) = sink.insert_close_position_event(event.clone()).await {
                    failures.push(format!("{}: {}", sink.sink_name(), error));
                }
            }

            if failures.is_empty() {
                self.metrics
                    .close_position_successes
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
                let joined_failures = failures.join(" | ");
                self.metrics
                    .close_position_failures
                    .fetch_add(1, Ordering::Relaxed);
                {
                    let mut guard = self.metrics.last_error.lock().expect("mutex poisoned");
                    *guard = Some(joined_failures.clone());
                }

                Err(anyhow!(
                    "Failed to write close-position event to {} sink(s): {}",
                    failures.len(),
                    joined_failures
                ))
            }
        })
    }

    fn metrics_snapshot(&self) -> Vec<SinkMetricsSnapshot> {
        let mut snapshots = vec![SinkMetricsSnapshot {
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
        }];

        for sink in &self.sinks {
            snapshots.extend(sink.metrics_snapshot());
        }

        snapshots
    }
}
