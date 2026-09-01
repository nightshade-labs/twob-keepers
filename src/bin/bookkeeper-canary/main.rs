use anchor_client::solana_sdk::{
    account::Account as SolanaAccount, commitment_config::CommitmentConfig, hash::Hash,
    pubkey::Pubkey,
};
use anchor_lang::{AccountDeserialize, declare_program};
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
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::{
    borrow::Cow,
    env,
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, time::sleep};
use twob_keepers::{ARRAY_LENGTH, AccountResolver};

declare_program!(twob_anchor);
use twob_anchor::accounts::{Bookkeeping, Market};

const DEFAULT_PROGRAM_ID: &str = "CCAmAqvza37EWzou7LoYCaGKzdJsCu1CLPMp3Wvx3Bc5";
const DEFAULT_MAINNET_GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
const DEFAULT_MARKET_ID: u64 = 1;
const DEFAULT_POLL_INTERVAL_MS: u64 = 10_000;
const DEFAULT_STALE_AFTER_MS: u64 = 45_000;
const WARNING_NUMERATOR: u64 = 7;
const WARNING_DENOMINATOR: u64 = 10;

#[derive(Clone, Debug)]
struct CanaryConfig {
    bind_addr: SocketAddr,
    rpc_url: String,
    cluster: String,
    program_id: Pubkey,
    expected_genesis_hash: Hash,
    market_id: u64,
    poll_interval: Duration,
    stale_after: Duration,
}

#[derive(Clone)]
struct CanaryMonitoring {
    config: CanaryConfig,
    registry: Arc<Registry>,
    metrics: CanaryMetrics,
    health: Arc<RwLock<CanaryHealth>>,
}

#[derive(Clone)]
struct CanaryMetrics {
    current_slot: Gauge,
    last_update_slot: Gauge,
    lag_slots: Gauge,
    freshness_boundary_slots: Gauge,
    warning_lag_slots: Gauge,
    critical_lag_slots: Gauge,
    freshness_remaining_slots: Gauge,
    last_observation_timestamp_seconds: Gauge,
    observations: Family<OutcomeLabel, Counter>,
    observation_duration_seconds: Histogram,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabel {
    outcome: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct CanaryHealthResponse {
    status: CanaryHealthStatus,
    cluster: String,
    program_id: String,
    market_id: u64,
    current_slot: Option<u64>,
    last_update_slot: Option<u64>,
    lag_slots: Option<u64>,
    freshness_boundary_slots: Option<u64>,
    warning_lag_slots: Option<u64>,
    critical_lag_slots: Option<u64>,
    freshness_remaining_slots: Option<u64>,
    last_observation_timestamp_seconds: Option<u64>,
    last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CanaryHealthStatus {
    Starting,
    Healthy,
    Stalled,
}

struct CanaryHealth {
    current_slot: Option<u64>,
    last_update_slot: Option<u64>,
    lag_slots: Option<u64>,
    freshness_boundary_slots: Option<u64>,
    warning_lag_slots: Option<u64>,
    critical_lag_slots: Option<u64>,
    freshness_remaining_slots: Option<u64>,
    last_observation_timestamp_seconds: Option<u64>,
    last_error: Option<String>,
}

#[derive(Debug)]
struct ChainObservation {
    current_slot: u64,
    last_update_slot: u64,
    freshness_boundary_slots: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let config = CanaryConfig::from_env()?;
    let monitoring = CanaryMonitoring::new(config.clone());
    let rpc = RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::confirmed());
    validate_cluster(&rpc, config.expected_genesis_hash).await?;

    let poller = poll_chain(rpc, config, monitoring.clone());
    let server = monitoring.serve();
    tokio::try_join!(poller, server)?;
    Ok(())
}

impl CanaryConfig {
    fn from_env() -> Result<Self> {
        let rpc_url = env::var("BOOKKEEPER_CANARY_RPC_URL")
            .context("BOOKKEEPER_CANARY_RPC_URL must be set to a mainnet RPC endpoint")?;
        if rpc_url.trim().is_empty() {
            return Err(anyhow!("BOOKKEEPER_CANARY_RPC_URL must not be empty"));
        }

        let bind_addr = match env::var("BOOKKEEPER_CANARY_BIND_ADDR") {
            Ok(raw) if !raw.trim().is_empty() => raw
                .parse::<SocketAddr>()
                .context("BOOKKEEPER_CANARY_BIND_ADDR must be a valid socket address")?,
            Ok(_) | Err(env::VarError::NotPresent) => {
                let port = parse_u16_env("PORT", 8080)?;
                SocketAddr::from(([0, 0, 0, 0], port))
            }
            Err(error) => {
                return Err(anyhow!(
                    "Failed to read BOOKKEEPER_CANARY_BIND_ADDR: {error}"
                ));
            }
        };

        let cluster = env::var("BOOKKEEPER_CANARY_CLUSTER")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "mainnet".to_string());
        let program_id = Pubkey::from_str(
            &env::var("BOOKKEEPER_CANARY_PROGRAM_ID")
                .unwrap_or_else(|_| DEFAULT_PROGRAM_ID.to_string()),
        )
        .context("BOOKKEEPER_CANARY_PROGRAM_ID must be a valid Solana public key")?;
        let expected_genesis_hash = Hash::from_str(
            &env::var("BOOKKEEPER_CANARY_EXPECTED_GENESIS_HASH")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_MAINNET_GENESIS_HASH.to_string()),
        )
        .context("BOOKKEEPER_CANARY_EXPECTED_GENESIS_HASH must be a valid Solana hash")?;
        let market_id = parse_u64_env("BOOKKEEPER_CANARY_MARKET_ID", DEFAULT_MARKET_ID)?;
        let poll_interval_ms = parse_u64_env(
            "BOOKKEEPER_CANARY_POLL_INTERVAL_MS",
            DEFAULT_POLL_INTERVAL_MS,
        )?;
        if poll_interval_ms == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_CANARY_POLL_INTERVAL_MS must be greater than 0"
            ));
        }
        let stale_after_ms =
            parse_u64_env("BOOKKEEPER_CANARY_STALE_AFTER_MS", DEFAULT_STALE_AFTER_MS)?;
        if stale_after_ms <= poll_interval_ms {
            return Err(anyhow!(
                "BOOKKEEPER_CANARY_STALE_AFTER_MS must be greater than BOOKKEEPER_CANARY_POLL_INTERVAL_MS"
            ));
        }

        Ok(Self {
            bind_addr,
            rpc_url,
            cluster,
            program_id,
            expected_genesis_hash,
            market_id,
            poll_interval: Duration::from_millis(poll_interval_ms),
            stale_after: Duration::from_millis(stale_after_ms),
        })
    }
}

impl CanaryMonitoring {
    fn new(config: CanaryConfig) -> Self {
        let metrics = CanaryMetrics::default();
        let mut registry = Registry::with_labels(
            [
                (Cow::Borrowed("cluster"), Cow::Owned(config.cluster.clone())),
                (
                    Cow::Borrowed("program_id"),
                    Cow::Owned(config.program_id.to_string()),
                ),
                (
                    Cow::Borrowed("genesis_hash"),
                    Cow::Owned(config.expected_genesis_hash.to_string()),
                ),
                (
                    Cow::Borrowed("market_id"),
                    Cow::Owned(config.market_id.to_string()),
                ),
            ]
            .into_iter(),
        );
        metrics.register(&mut registry);

        Self {
            config,
            registry: Arc::new(registry),
            metrics,
            health: Arc::new(RwLock::new(CanaryHealth {
                current_slot: None,
                last_update_slot: None,
                lag_slots: None,
                freshness_boundary_slots: None,
                warning_lag_slots: None,
                critical_lag_slots: None,
                freshness_remaining_slots: None,
                last_observation_timestamp_seconds: None,
                last_error: None,
            })),
        }
    }

    fn record_success(&self, observation: ChainObservation, duration: Duration) {
        let now = unix_timestamp_seconds();
        let lag = observation
            .current_slot
            .saturating_sub(observation.last_update_slot);
        let warning = warning_threshold(observation.freshness_boundary_slots);
        let remaining = observation.freshness_boundary_slots.saturating_sub(lag);

        self.metrics
            .current_slot
            .set(u64_to_i64(observation.current_slot));
        self.metrics
            .last_update_slot
            .set(u64_to_i64(observation.last_update_slot));
        self.metrics.lag_slots.set(u64_to_i64(lag));
        self.metrics
            .freshness_boundary_slots
            .set(u64_to_i64(observation.freshness_boundary_slots));
        self.metrics.warning_lag_slots.set(u64_to_i64(warning));
        self.metrics
            .critical_lag_slots
            .set(u64_to_i64(observation.freshness_boundary_slots));
        self.metrics
            .freshness_remaining_slots
            .set(u64_to_i64(remaining));
        self.metrics
            .last_observation_timestamp_seconds
            .set(u64_to_i64(now));
        self.metrics
            .observations
            .get_or_create(&OutcomeLabel { outcome: "success" })
            .inc();
        self.metrics
            .observation_duration_seconds
            .observe(duration.as_secs_f64());

        let mut health = self.health.write().expect("canary health lock poisoned");
        health.current_slot = Some(observation.current_slot);
        health.last_update_slot = Some(observation.last_update_slot);
        health.lag_slots = Some(lag);
        health.freshness_boundary_slots = Some(observation.freshness_boundary_slots);
        health.warning_lag_slots = Some(warning);
        health.critical_lag_slots = Some(observation.freshness_boundary_slots);
        health.freshness_remaining_slots = Some(remaining);
        health.last_observation_timestamp_seconds = Some(now);
        health.last_error = None;
    }

    fn record_failure(&self, error: &anyhow::Error, duration: Duration) {
        self.metrics
            .observations
            .get_or_create(&OutcomeLabel { outcome: "failure" })
            .inc();
        self.metrics
            .observation_duration_seconds
            .observe(duration.as_secs_f64());
        self.health
            .write()
            .expect("canary health lock poisoned")
            .last_error = Some(format!("{error:#}"));
    }

    fn health_response(&self) -> (CanaryHealthResponse, bool) {
        let now = unix_timestamp_seconds();
        let health = self.health.read().expect("canary health lock poisoned");
        let ready = health
            .last_observation_timestamp_seconds
            .map(|timestamp| now.saturating_sub(timestamp) <= self.config.stale_after.as_secs())
            .unwrap_or(false);
        let status = if health.last_observation_timestamp_seconds.is_none() {
            CanaryHealthStatus::Starting
        } else if ready {
            CanaryHealthStatus::Healthy
        } else {
            CanaryHealthStatus::Stalled
        };

        (
            CanaryHealthResponse {
                status,
                cluster: self.config.cluster.clone(),
                program_id: self.config.program_id.to_string(),
                market_id: self.config.market_id,
                current_slot: health.current_slot,
                last_update_slot: health.last_update_slot,
                lag_slots: health.lag_slots,
                freshness_boundary_slots: health.freshness_boundary_slots,
                warning_lag_slots: health.warning_lag_slots,
                critical_lag_slots: health.critical_lag_slots,
                freshness_remaining_slots: health.freshness_remaining_slots,
                last_observation_timestamp_seconds: health.last_observation_timestamp_seconds,
                last_error: health.last_error.clone(),
            },
            ready,
        )
    }

    fn encode_metrics(&self) -> Result<String> {
        let mut body = String::new();
        encode(&mut body, &self.registry).context("failed to encode canary metrics")?;
        Ok(body)
    }

    async fn serve(self) -> Result<()> {
        let bind_addr = self.config.bind_addr;
        let app = Router::new()
            .route("/livez", get(livez))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .with_state(self);
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("failed to bind canary server to {bind_addr}"))?;
        println!("Bookkeeper canary listening on {bind_addr}");
        axum::serve(listener, app)
            .await
            .context("bookkeeper canary server failed")
    }
}

impl Default for CanaryMetrics {
    fn default() -> Self {
        Self {
            current_slot: Gauge::default(),
            last_update_slot: Gauge::default(),
            lag_slots: Gauge::default(),
            freshness_boundary_slots: Gauge::default(),
            warning_lag_slots: Gauge::default(),
            critical_lag_slots: Gauge::default(),
            freshness_remaining_slots: Gauge::default(),
            last_observation_timestamp_seconds: Gauge::default(),
            observations: Family::default(),
            observation_duration_seconds: Histogram::new(exponential_buckets(0.01, 2.0, 12)),
        }
    }
}

impl CanaryMetrics {
    fn register(&self, registry: &mut Registry) {
        registry.register(
            "bookkeeper_chain_current_slot",
            "Latest confirmed Solana slot observed independently",
            self.current_slot.clone(),
        );
        registry.register(
            "bookkeeper_chain_last_update_slot",
            "Latest bookkeeping update slot read independently from the program account",
            self.last_update_slot.clone(),
        );
        registry.register(
            "bookkeeper_chain_lag_slots",
            "Independent confirmed-slot lag from the latest bookkeeping update",
            self.lag_slots.clone(),
        );
        registry.register(
            "bookkeeper_chain_freshness_boundary_slots",
            "Independent maximum safe bookkeeping lag in slots",
            self.freshness_boundary_slots.clone(),
        );
        registry.register(
            "bookkeeper_chain_warning_lag_slots",
            "Independent warning bookkeeping lag threshold in slots",
            self.warning_lag_slots.clone(),
        );
        registry.register(
            "bookkeeper_chain_critical_lag_slots",
            "Independent critical bookkeeping lag threshold in slots",
            self.critical_lag_slots.clone(),
        );
        registry.register(
            "bookkeeper_chain_freshness_remaining_slots",
            "Independent slots remaining before the critical freshness boundary",
            self.freshness_remaining_slots.clone(),
        );
        registry.register(
            "bookkeeper_chain_last_observation_timestamp_seconds",
            "Unix timestamp of the latest successful independent chain observation",
            self.last_observation_timestamp_seconds.clone(),
        );
        registry.register(
            "bookkeeper_chain_observations",
            "Independent chain observations by outcome",
            self.observations.clone(),
        );
        registry.register(
            "bookkeeper_chain_observation_duration_seconds",
            "Duration of an independent chain observation",
            self.observation_duration_seconds.clone(),
        );
    }
}

async fn poll_chain(
    rpc: RpcClient,
    config: CanaryConfig,
    monitoring: CanaryMonitoring,
) -> Result<()> {
    let resolver = AccountResolver::new(config.program_id);
    let market_address = resolver.market_pda(config.market_id).address();
    let bookkeeping_address = resolver.bookkeeping_pda(&market_address).address();

    println!(
        "Bookkeeper canary started cluster={} program_id={} market_id={} market={} bookkeeping={} poll_interval={}s",
        config.cluster,
        config.program_id,
        config.market_id,
        market_address,
        bookkeeping_address,
        config.poll_interval.as_secs_f64(),
    );

    let mut logged_first_success = false;
    loop {
        let started_at = Instant::now();
        match observe_chain(
            &rpc,
            config.program_id,
            config.market_id,
            market_address,
            bookkeeping_address,
        )
        .await
        {
            Ok(observation) => {
                if !logged_first_success {
                    println!(
                        "Bookkeeper canary first observation current_slot={} last_update_slot={} lag_slots={} freshness_boundary_slots={} warning_lag_slots={} critical_lag_slots={}",
                        observation.current_slot,
                        observation.last_update_slot,
                        observation
                            .current_slot
                            .saturating_sub(observation.last_update_slot),
                        observation.freshness_boundary_slots,
                        warning_threshold(observation.freshness_boundary_slots),
                        observation.freshness_boundary_slots,
                    );
                    logged_first_success = true;
                }
                monitoring.record_success(observation, started_at.elapsed());
            }
            Err(error) => {
                monitoring.record_failure(&error, started_at.elapsed());
                eprintln!("Bookkeeper canary observation failed: {error:#}");
            }
        }
        sleep(config.poll_interval).await;
    }
}

async fn observe_chain(
    rpc: &RpcClient,
    program_id: Pubkey,
    market_id: u64,
    market_address: Pubkey,
    bookkeeping_address: Pubkey,
) -> Result<ChainObservation> {
    let accounts = rpc
        .get_multiple_accounts(&[market_address, bookkeeping_address])
        .await
        .context("getMultipleAccounts RPC failed")?;
    let mut accounts = accounts.into_iter();
    let market = decode_program_account::<Market>(
        accounts.next().flatten(),
        market_address,
        program_id,
        "market",
    )?;
    let bookkeeping = decode_program_account::<Bookkeeping>(
        accounts.next().flatten(),
        bookkeeping_address,
        program_id,
        "bookkeeping",
    )?;
    if market.id != market_id {
        return Err(anyhow!(
            "market account id mismatch: expected {market_id}, observed {}",
            market.id
        ));
    }
    let freshness_boundary_slots = market
        .end_slot_interval
        .checked_mul(ARRAY_LENGTH)
        .context("freshness boundary overflow")?;
    if freshness_boundary_slots == 0 {
        return Err(anyhow!("freshness boundary must be greater than zero"));
    }
    let current_slot = rpc.get_slot().await.context("getSlot RPC failed")?;

    Ok(ChainObservation {
        current_slot,
        last_update_slot: bookkeeping.last_update_slot,
        freshness_boundary_slots,
    })
}

async fn validate_cluster(rpc: &RpcClient, expected_genesis_hash: Hash) -> Result<()> {
    let observed_genesis_hash = rpc
        .get_genesis_hash()
        .await
        .context("getGenesisHash RPC failed while validating the canary cluster")?;
    if observed_genesis_hash != expected_genesis_hash {
        return Err(anyhow!(
            "RPC cluster mismatch: expected genesis hash {expected_genesis_hash}, observed {observed_genesis_hash}"
        ));
    }
    println!("Bookkeeper canary validated genesis_hash={observed_genesis_hash}");
    Ok(())
}

fn decode_program_account<T: AccountDeserialize>(
    account: Option<SolanaAccount>,
    address: Pubkey,
    expected_owner: Pubkey,
    account_name: &str,
) -> Result<T> {
    let account = account.with_context(|| format!("{account_name} account {address} not found"))?;
    if account.owner != expected_owner {
        return Err(anyhow!(
            "{account_name} account {address} has owner {}, expected {expected_owner}",
            account.owner
        ));
    }
    if account.data.len() < 8 {
        return Err(anyhow!(
            "{account_name} account {address} is too short: {} bytes",
            account.data.len()
        ));
    }
    let mut data = account.data.as_slice();
    T::try_deserialize(&mut data).with_context(|| {
        format!("invalid {account_name} discriminator or account data at {address}")
    })
}

async fn livez(State(monitoring): State<CanaryMonitoring>) -> Json<CanaryHealthResponse> {
    Json(monitoring.health_response().0)
}

async fn readyz(State(monitoring): State<CanaryMonitoring>) -> Response {
    let (response, ready) = monitoring.health_response();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(response)).into_response()
}

async fn metrics(State(monitoring): State<CanaryMonitoring>) -> Response {
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
            format!("failed to encode canary metrics: {error:#}"),
        )
            .into_response(),
    }
}

fn warning_threshold(boundary: u64) -> u64 {
    boundary
        .saturating_mul(WARNING_NUMERATOR)
        .div_ceil(WARNING_DENOMINATOR)
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

fn parse_u64_env(key: &str, default_value: u64) -> Result<u64> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("{key} must be a valid u64")),
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(error) => Err(anyhow!("Failed to read {key}: {error}")),
    }
}

fn parse_u16_env(key: &str, default_value: u16) -> Result<u16> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<u16>()
            .with_context(|| format!("{key} must be a valid u16")),
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(error) => Err(anyhow!("Failed to read {key}: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CanaryConfig {
        CanaryConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            rpc_url: "https://example.invalid".to_string(),
            cluster: "mainnet".to_string(),
            program_id: Pubkey::from_str(DEFAULT_PROGRAM_ID).unwrap(),
            expected_genesis_hash: Hash::from_str(DEFAULT_MAINNET_GENESIS_HASH).unwrap(),
            market_id: 1,
            poll_interval: Duration::from_secs(10),
            stale_after: Duration::from_secs(45),
        }
    }

    #[test]
    fn warning_is_seventy_percent_and_critical_is_boundary() {
        let monitoring = CanaryMonitoring::new(test_config());
        monitoring.record_success(
            ChainObservation {
                current_slot: 1_049,
                last_update_slot: 1_000,
                freshness_boundary_slots: 70,
            },
            Duration::from_millis(20),
        );
        let (health, ready) = monitoring.health_response();

        assert!(ready);
        assert_eq!(health.lag_slots, Some(49));
        assert_eq!(health.warning_lag_slots, Some(49));
        assert_eq!(health.critical_lag_slots, Some(70));
        assert_eq!(health.freshness_remaining_slots, Some(21));
    }

    #[test]
    fn rejects_an_account_owned_by_another_program() {
        let expected_owner = Pubkey::new_unique();
        let address = Pubkey::new_unique();
        let account = SolanaAccount {
            lamports: 1,
            data: vec![0; 8],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };

        let error = decode_program_account::<Bookkeeping>(
            Some(account),
            address,
            expected_owner,
            "bookkeeping",
        )
        .unwrap_err();

        assert!(error.to_string().contains("has owner"));
    }

    #[test]
    fn metrics_use_bookkeeper_chain_prefix() {
        let monitoring = CanaryMonitoring::new(test_config());
        monitoring.record_success(
            ChainObservation {
                current_slot: 1_025,
                last_update_slot: 1_000,
                freshness_boundary_slots: 70,
            },
            Duration::from_millis(20),
        );
        let encoded = monitoring.encode_metrics().unwrap();

        assert!(encoded.contains("bookkeeper_chain_lag_slots"));
        assert!(encoded.contains("cluster=\"mainnet\""));
        assert!(encoded.contains("market_id=\"1\""));
        assert!(!encoded.contains("twob_bookkeeper"));
    }
}
