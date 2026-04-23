use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, SecondsFormat, Utc};
use clickhouse::{Client, Row};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;
use tower_http::cors::CorsLayer;

const DEFAULT_CLICKHOUSE_DATABASE: &str = "mato";
const DEFAULT_CLICKHOUSE_USER: &str = "default";
const DEFAULT_MARKET_UPDATES_TABLE: &str = "raw_market_update_events";
const DEFAULT_CANDLES_1M_TABLE: &str = "market_candles_1m";
const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_MAX_POINTS: usize = 1500;
const ABSOLUTE_MAX_POINTS: usize = 5000;
const DEFAULT_MARKET_CONFIG_CACHE_TTL_SECS: u64 = 300;

#[derive(Clone)]
struct AppState {
    client: Client,
    config: ReadApiConfig,
    market_config_store: MarketConfigStore,
}

#[derive(Clone, Debug)]
struct ReadApiConfig {
    bind_addr: SocketAddr,
    market_updates_table: String,
    candles_1m_table: String,
    market_config_cache_ttl: Duration,
}

impl ReadApiConfig {
    fn from_env() -> Result<(Self, Client)> {
        let url = first_env_value(&["READ_API_CLICKHOUSE_URL", "CLICKHOUSE_URL"])
            .ok_or_else(|| anyhow!("CLICKHOUSE_URL must be set"))?;
        let database = first_env_value(&["READ_API_CLICKHOUSE_DATABASE", "CLICKHOUSE_DATABASE"])
            .unwrap_or_else(|| DEFAULT_CLICKHOUSE_DATABASE.to_string());
        let user = first_env_value(&["READ_API_CLICKHOUSE_USER", "CLICKHOUSE_USER"])
            .unwrap_or_else(|| DEFAULT_CLICKHOUSE_USER.to_string());
        let password = first_env_value(&["READ_API_CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD"])
            .unwrap_or_default();
        let bind_addr = resolve_bind_addr()?
            .parse::<SocketAddr>()
            .with_context(|| "READ_API_BIND_ADDR must be a valid socket address")?;

        let market_updates_table = qualify_table_name(
            &database,
            &env::var("CLICKHOUSE_MARKET_UPDATES_TABLE")
                .unwrap_or_else(|_| DEFAULT_MARKET_UPDATES_TABLE.to_string()),
        )?;
        let candles_1m_table = qualify_table_name(
            &database,
            &env::var("CLICKHOUSE_CANDLES_1M_TABLE")
                .unwrap_or_else(|_| DEFAULT_CANDLES_1M_TABLE.to_string()),
        )?;
        let market_config_cache_ttl = Duration::from_secs(parse_u64_env(
            "READ_API_MARKET_CONFIG_CACHE_TTL_SECS",
            DEFAULT_MARKET_CONFIG_CACHE_TTL_SECS,
        )?);

        let client = Client::default()
            .with_url(url.trim())
            .with_database(database.trim())
            .with_user(user.trim())
            .with_password(password.trim());

        Ok((
            Self {
                bind_addr,
                market_updates_table,
                candles_1m_table,
                market_config_cache_ttl,
            },
            client,
        ))
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Deserialize)]
struct PriceQuery {}

#[derive(Serialize)]
struct LatestPriceResponse {
    market_id: u64,
    slot: u64,
    event_time: String,
    price: f64,
}

#[derive(Row, Deserialize)]
struct LatestPriceRow {
    slot: u64,
    event_time_ms: i64,
    raw_price: f64,
}

#[derive(Deserialize)]
struct CandleQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    interval: Option<String>,
    max_points: Option<usize>,
}

#[derive(Serialize)]
struct CandleResponse {
    market_id: u64,
    interval: String,
    from: String,
    to: String,
    points: usize,
    items: Vec<CandleItem>,
}

#[derive(Serialize)]
struct CandleItem {
    time: u64,
    start_slot: u64,
    end_slot: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Row, Deserialize)]
struct CandleRow {
    time: u64,
    start_slot: u64,
    end_slot: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Clone, Copy)]
enum CandleInterval {
    M1,
    M5,
    M15,
    H1,
    H4,
    D1,
}

#[derive(Clone, Copy)]
struct MarketDecimals {
    base_decimals: u8,
    quote_decimals: u8,
}

#[derive(Clone)]
struct MarketConfigStore {
    pool: Pool,
    cache_ttl: Duration,
    cache: Arc<RwLock<HashMap<u64, CachedMarketDecimals>>>,
}

#[derive(Clone, Copy)]
struct CachedMarketDecimals {
    loaded_at: Instant,
    decimals: MarketDecimals,
}

#[derive(Debug)]
struct MarketConfigNotFound {
    market_id: u64,
}

impl std::fmt::Display for MarketConfigNotFound {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "No market config found for market_id={}",
            self.market_id
        )
    }
}

impl std::error::Error for MarketConfigNotFound {}

impl CandleInterval {
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("1m").trim() {
            "1m" => Ok(Self::M1),
            "5m" => Ok(Self::M5),
            "15m" => Ok(Self::M15),
            "1h" => Ok(Self::H1),
            "4h" => Ok(Self::H4),
            "1d" => Ok(Self::D1),
            other => Err(anyhow!(
                "Unsupported interval '{other}'. Use one of: 1m, 5m, 15m, 1h, 4h, 1d"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::D1 => "1d",
        }
    }

    fn step_seconds(self) -> i64 {
        match self {
            Self::M1 => 60,
            Self::M5 => 300,
            Self::M15 => 900,
            Self::H1 => 3600,
            Self::H4 => 14_400,
            Self::D1 => 86_400,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (config, client) = ReadApiConfig::from_env()?;
    let market_config_store = MarketConfigStore::from_env(config.market_config_cache_ttl).await?;

    client
        .query("SELECT 1")
        .execute()
        .await
        .context("Failed to connect to ClickHouse for read-api")?;

    ensure_table_exists(&client, &config.market_updates_table).await?;
    ensure_table_exists(&client, &config.candles_1m_table).await?;

    let state = Arc::new(AppState {
        client,
        config: config.clone(),
        market_config_store,
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/markets/{market_id}/price", get(get_latest_price))
        .route("/v1/markets/{market_id}/candles", get(get_candles))
        .layer(CorsLayer::permissive())
        .with_state(state);

    println!("Read API listening on {}", config.bind_addr);

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .context("Failed to bind read-api listener")?;
    axum::serve(listener, app)
        .await
        .context("Read API server exited unexpectedly")?;
    Ok(())
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn get_latest_price(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<u64>,
    _query: Query<PriceQuery>,
) -> Result<Json<LatestPriceResponse>, ApiError> {
    let decimals = state
        .market_config_store
        .get_market_decimals(market_id)
        .await
        .map_err(map_market_config_error)?;
    let price_scale = price_scale(decimals.base_decimals, decimals.quote_decimals);

    let sql = format!(
        "SELECT \
            slot, \
            toUnixTimestamp64Milli(event_time) AS event_time_ms, \
            abs(toFloat64(quote_flow)) / abs(toFloat64(base_flow)) AS raw_price \
         FROM {} \
         WHERE market_id = ? AND base_flow != 0 \
         ORDER BY slot DESC, event_index DESC \
         LIMIT 1",
        state.config.market_updates_table
    );

    let rows = state
        .client
        .query(&sql)
        .bind(market_id)
        .fetch_all::<LatestPriceRow>()
        .await
        .map_err(|error| ApiError::internal(anyhow!(error).context("Failed to query latest price")))?;

    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::not_found(format!("No market updates found for market_id={market_id}")))?;

    let event_time = DateTime::<Utc>::from_timestamp_millis(row.event_time_ms)
        .ok_or_else(|| ApiError::internal(anyhow!("Invalid event_time_ms {}", row.event_time_ms)))?;

    Ok(Json(LatestPriceResponse {
        market_id,
        slot: row.slot,
        event_time: event_time.to_rfc3339_opts(SecondsFormat::Millis, true),
        price: row.raw_price * price_scale,
    }))
}

async fn get_candles(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<u64>,
    Query(query): Query<CandleQuery>,
) -> Result<Json<CandleResponse>, ApiError> {
    if query.to <= query.from {
        return Err(ApiError::bad_request("'to' must be later than 'from'"));
    }

    let interval = CandleInterval::parse(query.interval.as_deref())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let max_points = query.max_points.unwrap_or(DEFAULT_MAX_POINTS);

    if max_points == 0 || max_points > ABSOLUTE_MAX_POINTS {
        return Err(ApiError::bad_request(format!(
            "max_points must be between 1 and {ABSOLUTE_MAX_POINTS}"
        )));
    }

    let from_ms = query.from.timestamp_millis();
    let to_ms = query.to.timestamp_millis();
    let step_ms = interval.step_seconds() * 1000;
    let estimated_points = ((to_ms - from_ms) as i128 + step_ms as i128 - 1) / step_ms as i128;
    if estimated_points > max_points as i128 {
        return Err(ApiError::bad_request(format!(
            "Requested range produces {estimated_points} points at {}. Lower the range, increase interval, or raise max_points (up to {ABSOLUTE_MAX_POINTS})",
            interval.as_str()
        )));
    }

    let decimals = state
        .market_config_store
        .get_market_decimals(market_id)
        .await
        .map_err(map_market_config_error)?;
    let price_scale = price_scale(decimals.base_decimals, decimals.quote_decimals);
    let quote_scale = token_scale(decimals.quote_decimals);

    let sql = format!(
        "WITH minute_candles AS ( \
            SELECT \
                bucket_start, \
                argMinMerge(open_state) AS open_price, \
                maxMerge(high_state) AS high_price, \
                minMerge(low_state) AS low_price, \
                argMaxMerge(close_state) AS close_price, \
                minMerge(start_slot_state) AS start_slot, \
                maxMerge(end_slot_state) AS end_slot, \
                sumMerge(quote_volume_state) AS quote_volume \
            FROM {} \
            WHERE market_id = ? \
              AND bucket_start >= fromUnixTimestamp64Milli(?) \
              AND bucket_start < fromUnixTimestamp64Milli(?) \
            GROUP BY bucket_start \
        ) \
        SELECT \
            toUnixTimestamp(bucket_time) AS time, \
            min(start_slot) AS start_slot, \
            max(end_slot) AS end_slot, \
            argMin(open_price, bucket_start) * ? AS open, \
            max(high_price) * ? AS high, \
            min(low_price) * ? AS low, \
            argMax(close_price, bucket_start) * ? AS close, \
            sum(quote_volume) / ? AS volume \
        FROM ( \
            SELECT \
                toStartOfInterval(bucket_start, toIntervalSecond({})) AS bucket_time, \
                bucket_start, \
                open_price, \
                high_price, \
                low_price, \
                close_price, \
                start_slot, \
                end_slot, \
                quote_volume \
            FROM minute_candles \
        ) \
        GROUP BY bucket_time \
        ORDER BY bucket_time ASC \
        LIMIT ?",
        state.config.candles_1m_table,
        interval.step_seconds()
    );

    let rows = state
        .client
        .query(&sql)
        .bind(market_id)
        .bind(from_ms)
        .bind(to_ms)
        .bind(price_scale)
        .bind(price_scale)
        .bind(price_scale)
        .bind(price_scale)
        .bind(quote_scale)
        .bind(max_points as u64)
        .fetch_all::<CandleRow>()
        .await
        .map_err(|error| ApiError::internal(anyhow!(error).context("Failed to query candles")))?;

    let items = rows
        .into_iter()
        .map(|row| CandleItem {
            time: row.time,
            start_slot: row.start_slot,
            end_slot: row.end_slot,
            open: row.open,
            high: row.high,
            low: row.low,
            close: row.close,
            volume: row.volume,
        })
        .collect::<Vec<_>>();

    Ok(Json(CandleResponse {
        market_id,
        interval: interval.as_str().to_string(),
        from: query.from.to_rfc3339_opts(SecondsFormat::Secs, true),
        to: query.to.to_rfc3339_opts(SecondsFormat::Secs, true),
        points: items.len(),
        items,
    }))
}

fn first_env_value(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
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

fn resolve_bind_addr() -> Result<String> {
    if let Ok(value) = env::var("READ_API_BIND_ADDR") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    if let Ok(port) = env::var("PORT") {
        let trimmed = port.trim();
        if !trimmed.is_empty() {
            return Ok(format!("0.0.0.0:{trimmed}"));
        }
    }

    Ok(DEFAULT_BIND_ADDR.to_string())
}

fn qualify_table_name(database: &str, table: &str) -> Result<String> {
    let trimmed = table.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("Table name must not be empty"));
    }

    if !is_safe_identifier(trimmed) {
        return Err(anyhow!("Unsafe table identifier: {trimmed}"));
    }

    if trimmed.contains('.') {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("{database}.{trimmed}"))
    }
}

fn is_safe_identifier(value: &str) -> bool {
    value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '.')
}

async fn ensure_table_exists(client: &Client, table: &str) -> Result<()> {
    let query = format!("EXISTS TABLE {table}");
    let exists = client
        .query(&query)
        .fetch_one::<u8>()
        .await
        .with_context(|| format!("Failed to check table existence with query: {query}"))?;

    if exists == 1 {
        Ok(())
    } else {
        Err(anyhow!("Required table does not exist: {table}"))
    }
}

impl MarketConfigStore {
    async fn from_env(cache_ttl: Duration) -> Result<Self> {
        let database_url = first_env_value(&["READ_API_CONFIG_DATABASE_URL", "DATABASE_URL"])
            .ok_or_else(|| {
                anyhow!("DATABASE_URL must be set so read-api can resolve market decimals")
            })?;
        let config: tokio_postgres::Config = database_url
            .parse()
            .context("Failed to parse read-api config database URL")?;

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(config, NoTls, manager_config);
        let pool = Pool::builder(manager)
            .max_size(8)
            .build()
            .context("Failed to create market-config connection pool")?;

        let client = pool
            .get()
            .await
            .context("Failed to get market-config DB connection")?;
        let _ = client
            .query_one("SELECT 1", &[])
            .await
            .context("Failed to verify market-config DB connection")?;

        Ok(Self {
            pool,
            cache_ttl,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn get_market_decimals(&self, market_id: u64) -> Result<MarketDecimals> {
        let now = Instant::now();
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(&market_id) {
                if now.duration_since(cached.loaded_at) <= self.cache_ttl {
                    return Ok(cached.decimals);
                }
            }
        }

        let client = self
            .pool
            .get()
            .await
            .context("Failed to get market-config DB connection")?;
        let row = client
            .query_opt(
                "SELECT base_decimals::int4, quote_decimals::int4
                 FROM market_configs
                 WHERE market_id = $1
                 ORDER BY id DESC
                 LIMIT 1",
                &[&(market_id as i64)],
            )
            .await
            .with_context(|| format!("Failed to query market_configs for market_id={market_id}"))?;

        let row = match row {
            Some(row) => row,
            None => return Err(MarketConfigNotFound { market_id }.into()),
        };

        let base_decimals_i32: i32 = row.get(0);
        let quote_decimals_i32: i32 = row.get(1);
        let base_decimals =
            u8::try_from(base_decimals_i32).context("Invalid base_decimals in market_configs")?;
        let quote_decimals =
            u8::try_from(quote_decimals_i32).context("Invalid quote_decimals in market_configs")?;

        let decimals = MarketDecimals {
            base_decimals,
            quote_decimals,
        };

        {
            let mut cache = self.cache.write().await;
            cache.insert(
                market_id,
                CachedMarketDecimals {
                    loaded_at: now,
                    decimals,
                },
            );
        }

        Ok(decimals)
    }
}

fn map_market_config_error(error: anyhow::Error) -> ApiError {
    match error.downcast_ref::<MarketConfigNotFound>() {
        Some(not_found) => ApiError::not_found(format!(
            "No market config found for market_id={}",
            not_found.market_id
        )),
        None => ApiError::internal(error.context("Failed to resolve market config")),
    }
}

fn price_scale(base_decimals: u8, quote_decimals: u8) -> f64 {
    10f64.powi(base_decimals as i32 - quote_decimals as i32)
}

fn token_scale(decimals: u8) -> f64 {
    10f64.powi(decimals as i32)
}
