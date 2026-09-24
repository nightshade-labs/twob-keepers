use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{
        counter::Counter,
        family::Family,
        gauge::Gauge,
        histogram::{Histogram, exponential_buckets},
    },
    registry::Registry,
};
use serde::Serialize;
use std::{
    borrow::Cow,
    future::Future,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

const WARNING_NUMERATOR: u64 = 7;
const WARNING_DENOMINATOR: u64 = 10;

#[derive(Clone, Debug)]
pub struct MonitoringConfig {
    pub bind_addr: SocketAddr,
    pub bookkeeper_id: String,
    pub cluster: String,
    pub market_id: u64,
    pub market_address: String,
    pub slots_between_updates: u64,
    pub activity_grace: Duration,
}

#[derive(Clone)]
pub struct BookkeeperMonitoring {
    config: MonitoringConfig,
    registry: Arc<Registry>,
    metrics: Metrics,
    health: Arc<RwLock<HealthState>>,
}

#[derive(Clone)]
struct Metrics {
    current_slot: Gauge,
    last_update_slot: Gauge,
    lag_slots: Gauge,
    overdue_slots: Gauge,
    freshness_boundary_slots: Gauge,
    warning_lag_slots: Gauge,
    critical_lag_slots: Gauge,
    freshness_remaining_slots: Gauge,
    last_activity_timestamp_seconds: Gauge,
    next_expected_activity_timestamp_seconds: Gauge,
    last_chain_observation_timestamp_seconds: Gauge,
    last_confirmation_timestamp_seconds: Gauge,
    payer_balance_lamports: Gauge,
    iterations: Family<OutcomeLabel, Counter>,
    rpc_requests: Family<RpcLabel, Counter>,
    transactions: Family<OutcomeLabel, Counter>,
    broadcasts: Family<OutcomeLabel, Counter>,
    blockhash_expiries: Counter,
    rpc_duration_seconds: Family<OperationLabel, Histogram>,
    confirmation_duration_seconds: Histogram,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabel {
    outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OperationLabel {
    operation: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RpcLabel {
    operation: &'static str,
    outcome: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct HealthResponse {
    status: HealthStatus,
    bookkeeper_id: String,
    cluster: String,
    market_id: u64,
    market_address: String,
    slots_between_updates: u64,
    current_slot: Option<u64>,
    last_update_slot: Option<u64>,
    lag_slots: Option<u64>,
    overdue_slots: Option<u64>,
    freshness_boundary_slots: Option<u64>,
    warning_lag_slots: Option<u64>,
    critical_lag_slots: Option<u64>,
    freshness_remaining_slots: Option<u64>,
    last_activity_timestamp_seconds: u64,
    next_expected_activity_timestamp_seconds: u64,
    last_chain_observation_timestamp_seconds: Option<u64>,
    last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HealthStatus {
    Starting,
    Healthy,
    Warning,
    Critical,
    Stalled,
}

struct HealthState {
    freshness_boundary_slots: Option<u64>,
    warning_lag_slots: Option<u64>,
    critical_lag_slots: Option<u64>,
    current_slot: Option<u64>,
    last_update_slot: Option<u64>,
    lag_slots: Option<u64>,
    overdue_slots: Option<u64>,
    freshness_remaining_slots: Option<u64>,
    last_activity_timestamp_seconds: u64,
    next_expected_activity_timestamp_seconds: u64,
    last_chain_observation_timestamp_seconds: Option<u64>,
    last_error: Option<String>,
}

impl BookkeeperMonitoring {
    pub fn new(config: MonitoringConfig) -> Self {
        let metrics = Metrics::default();
        let mut registry = Registry::with_labels(
            [
                (
                    Cow::Borrowed("bookkeeper_id"),
                    Cow::Owned(config.bookkeeper_id.clone()),
                ),
                (Cow::Borrowed("cluster"), Cow::Owned(config.cluster.clone())),
                (
                    Cow::Borrowed("market_address"),
                    Cow::Owned(config.market_address.clone()),
                ),
                (
                    Cow::Borrowed("market_id"),
                    Cow::Owned(config.market_id.to_string()),
                ),
                (
                    Cow::Borrowed("slots_between_updates"),
                    Cow::Owned(config.slots_between_updates.to_string()),
                ),
            ]
            .into_iter(),
        );
        metrics.register(&mut registry);

        let now = unix_timestamp_seconds();
        metrics.last_activity_timestamp_seconds.set(u64_to_i64(now));
        metrics
            .next_expected_activity_timestamp_seconds
            .set(u64_to_i64(now));

        Self {
            config,
            registry: Arc::new(registry),
            metrics,
            health: Arc::new(RwLock::new(HealthState {
                freshness_boundary_slots: None,
                warning_lag_slots: None,
                critical_lag_slots: None,
                current_slot: None,
                last_update_slot: None,
                lag_slots: None,
                overdue_slots: None,
                freshness_remaining_slots: None,
                last_activity_timestamp_seconds: now,
                next_expected_activity_timestamp_seconds: now,
                last_chain_observation_timestamp_seconds: None,
                last_error: None,
            })),
        }
    }

    pub fn configure_freshness_boundary(
        &self,
        end_slot_interval: u64,
        array_length: u64,
    ) -> Result<()> {
        let boundary = end_slot_interval
            .checked_mul(array_length)
            .context("freshness boundary overflow")?;
        if boundary == 0 {
            return Err(anyhow!("freshness boundary must be greater than zero"));
        }
        let warning = boundary
            .saturating_mul(WARNING_NUMERATOR)
            .div_ceil(WARNING_DENOMINATOR);
        if self.config.slots_between_updates >= warning {
            return Err(anyhow!(
                "SLOTS_BETWEEN_UPDATES={} must be less than warning_lag_slots={} (70% of freshness_boundary_slots={boundary})",
                self.config.slots_between_updates,
                warning,
            ));
        }

        self.metrics
            .freshness_boundary_slots
            .set(u64_to_i64(boundary));
        self.metrics.warning_lag_slots.set(u64_to_i64(warning));
        self.metrics.critical_lag_slots.set(u64_to_i64(boundary));

        let mut health = self.health.write().expect("health lock poisoned");
        health.freshness_boundary_slots = Some(boundary);
        health.warning_lag_slots = Some(warning);
        health.critical_lag_slots = Some(boundary);
        Ok(())
    }

    pub fn observe_chain(&self, current_slot: u64, last_update_slot: u64) {
        let now = unix_timestamp_seconds();
        let lag = current_slot.saturating_sub(last_update_slot);
        let overdue = lag.saturating_sub(self.config.slots_between_updates);

        self.metrics.current_slot.set(u64_to_i64(current_slot));
        self.metrics
            .last_update_slot
            .set(u64_to_i64(last_update_slot));
        self.metrics.lag_slots.set(u64_to_i64(lag));
        self.metrics.overdue_slots.set(u64_to_i64(overdue));
        self.metrics
            .last_chain_observation_timestamp_seconds
            .set(u64_to_i64(now));

        let mut health = self.health.write().expect("health lock poisoned");
        let remaining = health
            .freshness_boundary_slots
            .map(|boundary| boundary.saturating_sub(lag));
        self.metrics
            .freshness_remaining_slots
            .set(u64_to_i64(remaining.unwrap_or_default()));
        health.current_slot = Some(current_slot);
        health.last_update_slot = Some(last_update_slot);
        health.lag_slots = Some(lag);
        health.overdue_slots = Some(overdue);
        health.freshness_remaining_slots = remaining;
        health.last_chain_observation_timestamp_seconds = Some(now);
        health.last_activity_timestamp_seconds = now;
        health.last_error = None;
        self.metrics
            .last_activity_timestamp_seconds
            .set(u64_to_i64(now));
    }

    pub fn plan_activity_after(&self, delay: Duration) {
        let now = unix_timestamp_seconds();
        let expected = now.saturating_add(delay.as_secs().max(1));
        let mut health = self.health.write().expect("health lock poisoned");
        health.last_activity_timestamp_seconds = now;
        health.next_expected_activity_timestamp_seconds = expected;
        self.metrics
            .last_activity_timestamp_seconds
            .set(u64_to_i64(now));
        self.metrics
            .next_expected_activity_timestamp_seconds
            .set(u64_to_i64(expected));
    }

    pub fn record_iteration_success(&self) {
        self.metrics
            .iterations
            .get_or_create(&OutcomeLabel { outcome: "success" })
            .inc();
        self.health
            .write()
            .expect("health lock poisoned")
            .last_error = None;
    }

    pub fn record_iteration_failure(&self, error: &anyhow::Error) {
        self.metrics
            .iterations
            .get_or_create(&OutcomeLabel { outcome: "failure" })
            .inc();
        self.health
            .write()
            .expect("health lock poisoned")
            .last_error = Some(format!("{error:#}"));
    }

    pub fn record_transaction(&self, outcome: &'static str) {
        self.metrics
            .transactions
            .get_or_create(&OutcomeLabel { outcome })
            .inc();
        if outcome == "confirmed" {
            self.metrics
                .last_confirmation_timestamp_seconds
                .set(u64_to_i64(unix_timestamp_seconds()));
        }
    }

    pub fn record_broadcast(&self, outcome: &'static str) {
        self.metrics
            .broadcasts
            .get_or_create(&OutcomeLabel { outcome })
            .inc();
    }

    pub fn record_blockhash_expiry(&self) {
        self.metrics.blockhash_expiries.inc();
    }

    pub fn observe_confirmation_duration(&self, duration: Duration) {
        self.metrics
            .confirmation_duration_seconds
            .observe(duration.as_secs_f64());
    }

    pub fn set_payer_balance(&self, lamports: u64) {
        self.metrics
            .payer_balance_lamports
            .set(u64_to_i64(lamports));
    }

    pub async fn observe_rpc<T, E, F>(&self, operation: &'static str, future: F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        let started_at = Instant::now();
        let result = future.await;
        let outcome = if result.is_ok() { "success" } else { "failure" };
        self.metrics
            .rpc_requests
            .get_or_create(&RpcLabel { operation, outcome })
            .inc();
        self.metrics
            .rpc_duration_seconds
            .get_or_create(&OperationLabel { operation })
            .observe(started_at.elapsed().as_secs_f64());
        result
    }

    pub async fn serve(self) -> Result<()> {
        let bind_addr = self.config.bind_addr;
        let app = Router::new()
            .route("/livez", get(livez))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .with_state(self);
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("failed to bind monitoring server to {bind_addr}"))?;
        println!("Bookkeeper monitoring server listening on {bind_addr}");
        axum::serve(listener, app)
            .await
            .context("monitoring server failed")
    }

    fn health_response(&self) -> (HealthResponse, bool) {
        let now = unix_timestamp_seconds();
        let health = self.health.read().expect("health lock poisoned");
        let activity_deadline = health
            .next_expected_activity_timestamp_seconds
            .saturating_add(self.config.activity_grace.as_secs());
        let (status, ready) = if health.freshness_boundary_slots.is_none()
            || health.last_chain_observation_timestamp_seconds.is_none()
        {
            (HealthStatus::Starting, false)
        } else if now > activity_deadline {
            (HealthStatus::Stalled, false)
        } else if health.lag_slots >= health.critical_lag_slots {
            (HealthStatus::Critical, false)
        } else if health.lag_slots >= health.warning_lag_slots {
            (HealthStatus::Warning, true)
        } else {
            (HealthStatus::Healthy, true)
        };

        (
            HealthResponse {
                status,
                bookkeeper_id: self.config.bookkeeper_id.clone(),
                cluster: self.config.cluster.clone(),
                market_id: self.config.market_id,
                market_address: self.config.market_address.clone(),
                slots_between_updates: self.config.slots_between_updates,
                current_slot: health.current_slot,
                last_update_slot: health.last_update_slot,
                lag_slots: health.lag_slots,
                overdue_slots: health.overdue_slots,
                freshness_boundary_slots: health.freshness_boundary_slots,
                warning_lag_slots: health.warning_lag_slots,
                critical_lag_slots: health.critical_lag_slots,
                freshness_remaining_slots: health.freshness_remaining_slots,
                last_activity_timestamp_seconds: health.last_activity_timestamp_seconds,
                next_expected_activity_timestamp_seconds: health
                    .next_expected_activity_timestamp_seconds,
                last_chain_observation_timestamp_seconds: health
                    .last_chain_observation_timestamp_seconds,
                last_error: health.last_error.clone(),
            },
            ready,
        )
    }

    fn encode_metrics(&self) -> Result<String> {
        let mut body = String::new();
        encode(&mut body, &self.registry).context("failed to encode Prometheus metrics")?;
        Ok(body)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            current_slot: Gauge::default(),
            last_update_slot: Gauge::default(),
            lag_slots: Gauge::default(),
            overdue_slots: Gauge::default(),
            freshness_boundary_slots: Gauge::default(),
            warning_lag_slots: Gauge::default(),
            critical_lag_slots: Gauge::default(),
            freshness_remaining_slots: Gauge::default(),
            last_activity_timestamp_seconds: Gauge::default(),
            next_expected_activity_timestamp_seconds: Gauge::default(),
            last_chain_observation_timestamp_seconds: Gauge::default(),
            last_confirmation_timestamp_seconds: Gauge::default(),
            payer_balance_lamports: Gauge::default(),
            iterations: Family::default(),
            rpc_requests: Family::default(),
            transactions: Family::default(),
            broadcasts: Family::default(),
            blockhash_expiries: Counter::default(),
            rpc_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.01, 2.0, 14))
            }),
            confirmation_duration_seconds: Histogram::new(exponential_buckets(0.1, 2.0, 12)),
        }
    }
}

impl Metrics {
    fn register(&self, registry: &mut Registry) {
        registry.register(
            "bookkeeper_current_slot",
            "Latest confirmed slot observed by this bookkeeper",
            self.current_slot.clone(),
        );
        registry.register(
            "bookkeeper_last_update_slot",
            "Latest bookkeeping update slot observed on chain",
            self.last_update_slot.clone(),
        );
        registry.register(
            "bookkeeper_lag_slots",
            "Confirmed slot lag from the latest bookkeeping update",
            self.lag_slots.clone(),
        );
        registry.register(
            "bookkeeper_overdue_slots",
            "Slots beyond this bookkeeper's configured update cadence",
            self.overdue_slots.clone(),
        );
        registry.register(
            "bookkeeper_freshness_boundary_slots",
            "Maximum safe bookkeeping lag in slots",
            self.freshness_boundary_slots.clone(),
        );
        registry.register(
            "bookkeeper_warning_lag_slots",
            "Warning bookkeeping lag threshold in slots",
            self.warning_lag_slots.clone(),
        );
        registry.register(
            "bookkeeper_critical_lag_slots",
            "Critical bookkeeping lag threshold in slots",
            self.critical_lag_slots.clone(),
        );
        registry.register(
            "bookkeeper_freshness_remaining_slots",
            "Slots remaining before the critical freshness boundary",
            self.freshness_remaining_slots.clone(),
        );
        registry.register(
            "bookkeeper_last_activity_timestamp_seconds",
            "Unix timestamp of the latest bookkeeper loop activity",
            self.last_activity_timestamp_seconds.clone(),
        );
        registry.register(
            "bookkeeper_next_expected_activity_timestamp_seconds",
            "Unix timestamp when the bookkeeper next expects loop activity",
            self.next_expected_activity_timestamp_seconds.clone(),
        );
        registry.register(
            "bookkeeper_last_chain_observation_timestamp_seconds",
            "Unix timestamp of the latest successful bookkeeping and slot observation",
            self.last_chain_observation_timestamp_seconds.clone(),
        );
        registry.register(
            "bookkeeper_last_confirmation_timestamp_seconds",
            "Unix timestamp of the latest confirmed update transaction",
            self.last_confirmation_timestamp_seconds.clone(),
        );
        registry.register(
            "bookkeeper_payer_balance_lamports",
            "Latest observed payer balance in lamports",
            self.payer_balance_lamports.clone(),
        );
        registry.register(
            "bookkeeper_iterations",
            "Bookkeeper loop iterations by outcome",
            self.iterations.clone(),
        );
        registry.register(
            "bookkeeper_rpc_requests",
            "Solana RPC requests by operation and outcome",
            self.rpc_requests.clone(),
        );
        registry.register(
            "bookkeeper_transactions",
            "Bookkeeping transactions by outcome",
            self.transactions.clone(),
        );
        registry.register(
            "bookkeeper_broadcasts",
            "Transaction broadcasts by outcome",
            self.broadcasts.clone(),
        );
        registry.register(
            "bookkeeper_blockhash_expiries",
            "Blockhash expiries encountered while sending bookkeeping updates",
            self.blockhash_expiries.clone(),
        );
        registry.register(
            "bookkeeper_rpc_duration_seconds",
            "Solana RPC request duration in seconds by operation",
            self.rpc_duration_seconds.clone(),
        );
        registry.register(
            "bookkeeper_confirmation_duration_seconds",
            "Bookkeeping transaction confirmation duration in seconds",
            self.confirmation_duration_seconds.clone(),
        );
    }
}

async fn livez(State(monitoring): State<BookkeeperMonitoring>) -> Json<HealthResponse> {
    Json(monitoring.health_response().0)
}

async fn readyz(State(monitoring): State<BookkeeperMonitoring>) -> Response {
    let (response, ready) = monitoring.health_response();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(response)).into_response()
}

async fn metrics(State(monitoring): State<BookkeeperMonitoring>) -> Response {
    match monitoring.encode_metrics() {
        Ok(body) => (
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {error:#}"),
        )
            .into_response(),
    }
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitoring(slots_between_updates: u64) -> BookkeeperMonitoring {
        BookkeeperMonitoring::new(MonitoringConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            bookkeeper_id: "primary".to_string(),
            cluster: "mainnet".to_string(),
            market_id: 1,
            market_address: "F41sZg6i75dd8BC3ZbAqCkYGFtHRo3H1fD6anm4H8AsW".to_string(),
            slots_between_updates,
            activity_grace: Duration::from_secs(15),
        })
    }

    #[test]
    fn mainnet_thresholds_are_warning_at_seventy_percent_and_critical_at_boundary() {
        let monitoring = monitoring(40);
        monitoring.configure_freshness_boundary(7, 10).unwrap();
        let (health, _) = monitoring.health_response();

        assert_eq!(health.freshness_boundary_slots, Some(70));
        assert_eq!(health.warning_lag_slots, Some(49));
        assert_eq!(health.critical_lag_slots, Some(70));
    }

    #[test]
    fn rejects_update_cadence_without_warning_headroom() {
        let monitoring = monitoring(49);
        let error = monitoring.configure_freshness_boundary(7, 10).unwrap_err();
        assert!(error.to_string().contains("must be less than"));
    }

    #[test]
    fn devnet_v1_thresholds_and_cadence_headroom() {
        for cadence in [40, 45, 146] {
            let monitoring = monitoring(cadence);
            monitoring
                .configure_freshness_boundary(crate::END_SLOT_INTERVAL, crate::ARRAY_LENGTH)
                .unwrap();
            let (health, _) = monitoring.health_response();
            assert_eq!(health.freshness_boundary_slots, Some(210));
            assert_eq!(health.warning_lag_slots, Some(147));
            assert_eq!(health.critical_lag_slots, Some(210));
        }
        assert!(
            monitoring(147)
                .configure_freshness_boundary(crate::END_SLOT_INTERVAL, crate::ARRAY_LENGTH)
                .is_err()
        );
    }

    #[test]
    fn readiness_allows_warning_but_rejects_critical_lag() {
        let monitoring = monitoring(40);
        monitoring.configure_freshness_boundary(7, 10).unwrap();
        monitoring.plan_activity_after(Duration::from_secs(60));

        monitoring.observe_chain(1_049, 1_000);
        let (warning, ready) = monitoring.health_response();
        assert_eq!(warning.status, HealthStatus::Warning);
        assert!(ready);

        monitoring.observe_chain(1_070, 1_000);
        let (critical, ready) = monitoring.health_response();
        assert_eq!(critical.status, HealthStatus::Critical);
        assert!(!ready);
    }

    #[test]
    fn metrics_use_bookkeeper_prefix_and_static_identity_labels() {
        let monitoring = monitoring(45);
        monitoring.configure_freshness_boundary(7, 10).unwrap();
        monitoring.observe_chain(1_045, 1_000);
        let encoded = monitoring.encode_metrics().unwrap();

        assert!(encoded.contains("bookkeeper_lag_slots"));
        assert!(encoded.contains("bookkeeper_id=\"primary\""));
        assert!(
            encoded.contains("market_address=\"F41sZg6i75dd8BC3ZbAqCkYGFtHRo3H1fD6anm4H8AsW\"")
        );
        assert!(encoded.contains("slots_between_updates=\"45\""));
        assert!(!encoded.contains("twob_bookkeeper"));
    }
}
