use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use chrono::{DateTime, SecondsFormat, Utc};
use deadpool_postgres::Pool;
use futures_util::{Stream, stream};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap, convert::Infallible, env, net::SocketAddr, sync::Arc, time::Duration,
};
use tokio::{
    sync::{RwLock, broadcast},
    time::MissedTickBehavior,
};
use tower_http::cors::CorsLayer;
use twob_keepers::database::connect_pool;

const DEFAULT_MARKET_UPDATES_TABLE: &str = "raw_market_update_events";
const DEFAULT_CANDLES_1M_TABLE: &str = "market_candles_1m";
const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_MAX_POINTS: usize = 1500;
const ABSOLUTE_MAX_POINTS: usize = 5000;
const DEFAULT_HISTORY_MAX_ROWS: usize = 25_000;
const ABSOLUTE_MAX_HISTORY_ROWS: usize = 200_000;
const DEFAULT_CLOSED_POSITION_MINI_CHART_POINTS: usize = 240;
const ABSOLUTE_MAX_CLOSED_POSITION_MINI_CHART_POINTS: usize = 2_000;
const DEFAULT_UPDATES_LIMIT: usize = 200;
const ABSOLUTE_MAX_UPDATES_LIMIT: usize = 5000;
const DEFAULT_PRICE_STREAM_POLL_MS: u64 = 1000;
const POOL_MAX_SIZE: usize = 16;
const MAX_LIGHTWEIGHT_CHART_ABS_VALUE: f64 = 90_071_992_547_409.91;

#[derive(Clone)]
struct AppState {
    pool: Pool,
    config: ReadApiConfig,
    market_price_streams: MarketPriceStreams,
}

#[derive(Clone, Debug)]
struct ReadApiConfig {
    bind_addr: SocketAddr,
    market_updates_table: String,
    candles_1m_table: String,
    price_stream_poll_interval: Duration,
}

impl ReadApiConfig {
    fn from_env() -> Result<(Self, Pool)> {
        let database_url = first_env_value(&["READ_API_DATABASE_URL", "DATABASE_URL"])
            .ok_or_else(|| anyhow!("DATABASE_URL must be set (Tiger Cloud connection string)"))?;

        let bind_addr = resolve_bind_addr()?
            .parse::<SocketAddr>()
            .with_context(|| "READ_API_BIND_ADDR must be a valid socket address")?;

        let market_updates_table = validate_table_name(
            &env::var("MARKET_UPDATES_TABLE")
                .unwrap_or_else(|_| DEFAULT_MARKET_UPDATES_TABLE.to_string()),
        )?;
        let candles_1m_table = validate_table_name(
            &env::var("CANDLES_1M_TABLE").unwrap_or_else(|_| DEFAULT_CANDLES_1M_TABLE.to_string()),
        )?;
        let price_stream_poll_interval = Duration::from_millis(parse_u64_env(
            "READ_API_PRICE_STREAM_POLL_MS",
            DEFAULT_PRICE_STREAM_POLL_MS,
        )?);

        let pool = connect_pool(&database_url, POOL_MAX_SIZE)?;

        Ok((
            Self {
                bind_addr,
                market_updates_table,
                candles_1m_table,
                price_stream_poll_interval,
            },
            pool,
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
            message: format!("{error:#}"),
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

#[derive(Clone, Serialize)]
struct LatestPriceResponse {
    market_id: u64,
    slot: u64,
    event_time: String,
    #[serde(skip_serializing)]
    event_time_ms: i64,
    price: Decimal,
}

#[derive(Deserialize)]
struct CandleQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    interval: Option<String>,
    max_points: Option<usize>,
}

#[derive(Deserialize)]
struct MarketHistoryQuery {
    start_slot: u64,
    end_slot: u64,
    max_rows: Option<usize>,
}

#[derive(Deserialize)]
struct MarketUpdatesQuery {
    before_slot: Option<u64>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct ClosedPositionMiniChartQuery {
    start_slot: u64,
    end_slot: u64,
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
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
}

#[derive(Serialize)]
struct MarketHistoryResponse {
    market_id: u64,
    start_slot: u64,
    end_slot: u64,
    points: usize,
    items: Vec<MarketHistoryItem>,
}

#[derive(Serialize)]
struct MarketUpdatesResponse {
    market_id: u64,
    before_slot: Option<u64>,
    has_more: bool,
    limit: usize,
    points: usize,
    items: Vec<MarketHistoryItem>,
}

#[derive(Serialize)]
struct ClosedPositionMiniChartResponse {
    market_id: u64,
    start_slot: u64,
    end_slot: u64,
    points: usize,
    items: Vec<ClosedPositionMiniChartItem>,
}

#[derive(Serialize)]
struct ClosedPositionMiniChartItem {
    slot: u64,
    price: Decimal,
}

#[derive(Serialize)]
struct MarketHistoryItem {
    event_uid: String,
    signature: String,
    event_index: u16,
    slot: u64,
    market_id: u64,
    base_flow: String,
    quote_flow: String,
    created_at: String,
}

struct MarketHistoryRow {
    event_uid: String,
    signature: String,
    event_index: i32,
    slot: i64,
    market_id: i64,
    base_flow: i64,
    quote_flow: i64,
    event_time_ms: i64,
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

#[derive(Clone)]
struct MarketPriceStreams {
    channels: Arc<RwLock<HashMap<u64, broadcast::Sender<LatestPriceResponse>>>>,
    runtime: PriceStreamRuntime,
    poll_interval: Duration,
}

#[derive(Clone)]
struct PriceStreamRuntime {
    pool: Pool,
    market_updates_table: String,
}

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

    /// Postgres `interval` literal for `time_bucket_gapfill`.
    fn pg_interval(self) -> &'static str {
        match self {
            Self::M1 => "1 minute",
            Self::M5 => "5 minutes",
            Self::M15 => "15 minutes",
            Self::H1 => "1 hour",
            Self::H4 => "4 hours",
            Self::D1 => "1 day",
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

    let (config, pool) = ReadApiConfig::from_env()?;
    let market_price_streams = MarketPriceStreams::new(
        pool.clone(),
        config.market_updates_table.clone(),
        config.price_stream_poll_interval,
    );

    {
        let client = pool
            .get()
            .await
            .context("Failed to connect to Tiger Cloud for read-api")?;
        client
            .simple_query("SELECT 1")
            .await
            .context("Failed to verify Tiger Cloud connection")?;
        ensure_table_exists(&client, &config.market_updates_table).await?;
        ensure_table_exists(&client, &config.candles_1m_table).await?;
    }

    let state = Arc::new(AppState {
        pool,
        config: config.clone(),
        market_price_streams,
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/markets/{market_id}/price", get(get_latest_price))
        .route("/v1/markets/{market_id}/stream", get(stream_market_price))
        .route("/v1/markets/{market_id}/candles", get(get_candles))
        .route("/v1/markets/{market_id}/history", get(get_market_history))
        .route(
            "/v1/markets/{market_id}/closed-position-mini-chart",
            get(get_closed_position_mini_chart),
        )
        .route("/v1/markets/{market_id}/updates", get(get_market_updates))
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
) -> Result<Json<LatestPriceResponse>, ApiError> {
    let maybe_snapshot =
        fetch_latest_price_snapshot(&state.pool, &state.config.market_updates_table, market_id)
            .await
            .map_err(|error| {
                ApiError::internal(error.context("Failed to fetch latest price snapshot"))
            })?;

    let snapshot = maybe_snapshot.ok_or_else(|| {
        ApiError::not_found(format!("No price available for market_id={market_id}"))
    })?;

    Ok(Json(snapshot))
}

async fn stream_market_price(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<u64>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let receiver = state.market_price_streams.subscribe(market_id).await;

    let event_stream = stream::unfold(receiver, move |mut receiver| async move {
        loop {
            match receiver.recv().await {
                Ok(snapshot) => {
                    let event = match Event::default().event("price_update").json_data(&snapshot) {
                        Ok(event) => event,
                        Err(error) => {
                            eprintln!(
                                "Failed to encode price_update SSE payload for market_id={}: {}",
                                market_id, error
                            );
                            continue;
                        }
                    };
                    return Some((Ok::<Event, Infallible>(event), receiver));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    eprintln!(
                        "Price stream lagged for market_id={}; skipped {} event(s)",
                        market_id, skipped
                    );
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Ok(Sse::new(event_stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
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

    let market_id_i64 =
        i64::try_from(market_id).map_err(|_| ApiError::bad_request("market_id out of range"))?;

    // Gap-filled, carry-forward candles directly from the 1m rollup. Empty
    // buckets get `locf` (last observation carried forward), seeded from the
    // last candle strictly before `from` so leading gaps render as flat doji.
    let sql = format!(
        "SELECT \
            time_bucket_gapfill($4::text::interval, bucket_start) AS bucket, \
            first(open, bucket_start)  AS open, \
            max(high)                  AS high, \
            min(low)                   AS low, \
            last(close, bucket_start)  AS close, \
            locf( \
                last(close, bucket_start), \
                (SELECT c.close FROM {0} c \
                   WHERE c.market_id = $1 AND c.bucket_start < $2 \
                   ORDER BY c.bucket_start DESC LIMIT 1) \
            ) AS carried_close \
         FROM {0} \
         WHERE market_id = $1 \
           AND bucket_start >= $2 \
           AND bucket_start < $3 \
         GROUP BY 1 \
         ORDER BY 1",
        state.config.candles_1m_table
    );

    let client = state.pool.get().await.map_err(|error| {
        ApiError::internal(anyhow!(error).context("Failed to get DB connection"))
    })?;
    let rows = client
        .query(
            &sql,
            &[
                &market_id_i64,
                &query.from,
                &query.to,
                &interval.pg_interval(),
            ],
        )
        .await
        .map_err(|error| ApiError::internal(anyhow!(error).context("Failed to query candles")))?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let bucket: DateTime<Utc> = row.get("bucket");
        let bucket_s = bucket.timestamp();
        let open: Option<Decimal> = row.get("open");
        let close: Option<Decimal> = row.get("close");

        let (open, high, low, close) = match (open, close) {
            (Some(open), Some(close)) => {
                let high: Decimal = row.get::<_, Option<Decimal>>("high").unwrap_or(close);
                let low: Decimal = row.get::<_, Option<Decimal>>("low").unwrap_or(close);
                (open, high, low, close)
            }
            // Empty bucket: carry the last known close forward as a flat candle.
            _ => match row.get::<_, Option<Decimal>>("carried_close") {
                Some(carried) => (carried, carried, carried, carried),
                None => continue, // No data at or before this bucket yet.
            },
        };

        if !is_valid_chart_price(open)
            || !is_valid_chart_price(high)
            || !is_valid_chart_price(low)
            || !is_valid_chart_price(close)
        {
            eprintln!(
                "Dropping invalid candle row for market_id={}: time={} open={} high={} low={} close={}",
                market_id, bucket_s, open, high, low, close
            );
            continue;
        }

        let normalized_high = open.max(high).max(low).max(close);
        let normalized_low = open.min(high).min(low).min(close);

        items.push(CandleItem {
            time: bucket_s.max(0) as u64,
            open,
            high: normalized_high,
            low: normalized_low,
            close,
        });
    }

    Ok(Json(CandleResponse {
        market_id,
        interval: interval.as_str().to_string(),
        from: query.from.to_rfc3339_opts(SecondsFormat::Secs, true),
        to: query.to.to_rfc3339_opts(SecondsFormat::Secs, true),
        points: items.len(),
        items,
    }))
}

async fn get_market_history(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<u64>,
    Query(query): Query<MarketHistoryQuery>,
) -> Result<Json<MarketHistoryResponse>, ApiError> {
    if query.start_slot > query.end_slot {
        return Err(ApiError::bad_request(
            "'start_slot' must be less than or equal to 'end_slot'",
        ));
    }

    let max_rows = query.max_rows.unwrap_or(DEFAULT_HISTORY_MAX_ROWS);
    if max_rows == 0 || max_rows > ABSOLUTE_MAX_HISTORY_ROWS {
        return Err(ApiError::bad_request(format!(
            "max_rows must be between 1 and {ABSOLUTE_MAX_HISTORY_ROWS}"
        )));
    }

    let rows = query_market_history_rows(
        &state.pool,
        &state.config.market_updates_table,
        market_id,
        query.start_slot,
        query.end_slot,
        max_rows,
    )
    .await
    .map_err(|error| ApiError::internal(error.context("Failed to query market history")))?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(market_history_item_from_row(row)?);
    }

    Ok(Json(MarketHistoryResponse {
        market_id,
        start_slot: query.start_slot,
        end_slot: query.end_slot,
        points: items.len(),
        items,
    }))
}

async fn get_market_updates(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<u64>,
    Query(query): Query<MarketUpdatesQuery>,
) -> Result<Json<MarketUpdatesResponse>, ApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_UPDATES_LIMIT);
    if limit == 0 || limit > ABSOLUTE_MAX_UPDATES_LIMIT {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {ABSOLUTE_MAX_UPDATES_LIMIT}"
        )));
    }

    let mut rows = query_market_updates_rows(
        &state.pool,
        &state.config.market_updates_table,
        market_id,
        query.before_slot,
        limit.saturating_add(1),
    )
    .await
    .map_err(|error| ApiError::internal(error.context("Failed to query market updates")))?;

    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(market_history_item_from_row(row)?);
    }

    Ok(Json(MarketUpdatesResponse {
        market_id,
        before_slot: query.before_slot,
        has_more,
        limit,
        points: items.len(),
        items,
    }))
}

async fn get_closed_position_mini_chart(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<u64>,
    Query(query): Query<ClosedPositionMiniChartQuery>,
) -> Result<Json<ClosedPositionMiniChartResponse>, ApiError> {
    if query.start_slot > query.end_slot {
        return Err(ApiError::bad_request(
            "'start_slot' must be less than or equal to 'end_slot'",
        ));
    }

    let max_points = query
        .max_points
        .unwrap_or(DEFAULT_CLOSED_POSITION_MINI_CHART_POINTS);
    if max_points == 0 || max_points > ABSOLUTE_MAX_CLOSED_POSITION_MINI_CHART_POINTS {
        return Err(ApiError::bad_request(format!(
            "max_points must be between 1 and {ABSOLUTE_MAX_CLOSED_POSITION_MINI_CHART_POINTS}"
        )));
    }

    let rows = query_closed_position_mini_chart_rows(
        &state.pool,
        &state.config.market_updates_table,
        market_id,
        query.start_slot,
        query.end_slot,
        max_points,
    )
    .await
    .map_err(|error| {
        ApiError::internal(error.context("Failed to query closed-position mini chart"))
    })?;

    let mut items = Vec::with_capacity(rows.len());
    for (slot, price) in rows {
        if !is_valid_chart_price(price) {
            continue;
        }
        items.push(ClosedPositionMiniChartItem {
            slot: slot.max(0) as u64,
            price,
        });
    }

    Ok(Json(ClosedPositionMiniChartResponse {
        market_id,
        start_slot: query.start_slot,
        end_slot: query.end_slot,
        points: items.len(),
        items,
    }))
}

async fn fetch_latest_price_snapshot(
    pool: &Pool,
    market_updates_table: &str,
    market_id: u64,
) -> Result<Option<LatestPriceResponse>> {
    let market_id_i64 = i64::try_from(market_id).context("market_id out of range")?;

    let sql = format!(
        "SELECT \
            r.slot AS slot, \
            (extract(epoch from r.event_time) * 1000)::bigint AS event_time_ms, \
            (r.quote_flow::numeric * power(10::numeric, mc.base_decimals::numeric)) \
              / (r.base_flow::numeric * power(10::numeric, mc.quote_decimals::numeric)) AS price \
         FROM {} r \
         JOIN market_configs mc ON mc.market_id = r.market_id \
         WHERE r.market_id = $1 \
           AND r.base_flow <> 0 \
           AND mc.base_decimals IS NOT NULL \
           AND mc.quote_decimals IS NOT NULL \
           AND r.event_uid NOT LIKE 'debug:%' \
         ORDER BY r.event_time DESC, r.slot DESC, r.event_index DESC \
         LIMIT 1",
        market_updates_table
    );

    let client = pool.get().await.context("Failed to get DB connection")?;
    let maybe_row = client
        .query_opt(&sql, &[&market_id_i64])
        .await
        .context("Failed to query latest price")?;

    let Some(row) = maybe_row else {
        return Ok(None);
    };

    let slot: i64 = row.get("slot");
    let event_time_ms: i64 = row.get("event_time_ms");
    let price: Decimal = row.get("price");

    if !is_valid_chart_price(price) {
        return Err(anyhow!(
            "Latest price out of supported range for market_id={market_id}: {price}"
        ));
    }

    let event_time = DateTime::<Utc>::from_timestamp_millis(event_time_ms)
        .ok_or_else(|| anyhow!("Invalid event_time_ms {event_time_ms}"))?;

    Ok(Some(LatestPriceResponse {
        market_id,
        slot: slot.max(0) as u64,
        event_time: event_time.to_rfc3339_opts(SecondsFormat::Millis, true),
        event_time_ms,
        price,
    }))
}

async fn query_market_history_rows(
    pool: &Pool,
    market_updates_table: &str,
    market_id: u64,
    start_slot: u64,
    end_slot: u64,
    max_rows: usize,
) -> Result<Vec<MarketHistoryRow>> {
    if max_rows == 0 {
        return Ok(Vec::new());
    }

    let market_id_i64 = i64::try_from(market_id).context("market_id out of range")?;
    let start_slot_i64 = i64::try_from(start_slot).context("start_slot out of range")?;
    let end_slot_i64 = i64::try_from(end_slot).context("end_slot out of range")?;

    let client = pool.get().await.context("Failed to get DB connection")?;

    let anchor_sql = format!(
        "SELECT event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, \
            (extract(epoch from event_time) * 1000)::bigint AS event_time_ms \
         FROM {} \
         WHERE market_id = $1 \
           AND slot < $2 \
           AND event_uid NOT LIKE 'debug:%' \
         ORDER BY slot DESC, event_index DESC \
         LIMIT 1",
        market_updates_table
    );

    let mut anchor_rows: Vec<MarketHistoryRow> = client
        .query(&anchor_sql, &[&market_id_i64, &start_slot_i64])
        .await
        .context("Failed to query market history anchor row")?
        .iter()
        .map(market_history_row_from_pg)
        .collect();

    let remaining_capacity = max_rows.saturating_sub(anchor_rows.len());
    if remaining_capacity == 0 {
        anchor_rows.sort_by(history_row_ordering);
        return Ok(anchor_rows);
    }

    let range_sql = format!(
        "SELECT event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, \
            (extract(epoch from event_time) * 1000)::bigint AS event_time_ms \
         FROM {} \
         WHERE market_id = $1 \
           AND slot >= $2 \
           AND slot <= $3 \
           AND event_uid NOT LIKE 'debug:%' \
         ORDER BY slot ASC, event_index ASC \
         LIMIT $4",
        market_updates_table
    );

    let range_limit = remaining_capacity.saturating_add(1) as i64;
    let mut range_rows: Vec<MarketHistoryRow> = client
        .query(
            &range_sql,
            &[&market_id_i64, &start_slot_i64, &end_slot_i64, &range_limit],
        )
        .await
        .context("Failed to query market history range rows")?
        .iter()
        .map(market_history_row_from_pg)
        .collect();

    if range_rows.len() > remaining_capacity {
        return Err(anyhow!(
            "Requested history range for market_id={} exceeds max_rows={} rows",
            market_id,
            max_rows
        ));
    }

    if anchor_rows.is_empty() {
        return Ok(range_rows);
    }

    anchor_rows.append(&mut range_rows);
    anchor_rows.sort_by(history_row_ordering);
    Ok(anchor_rows)
}

async fn query_market_updates_rows(
    pool: &Pool,
    market_updates_table: &str,
    market_id: u64,
    before_slot: Option<u64>,
    limit: usize,
) -> Result<Vec<MarketHistoryRow>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let market_id_i64 = i64::try_from(market_id).context("market_id out of range")?;
    let limit_i64 = limit as i64;

    let client = pool.get().await.context("Failed to get DB connection")?;

    const SELECT_COLUMNS: &str = "SELECT event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, \
            (extract(epoch from event_time) * 1000)::bigint AS event_time_ms";

    let pg_rows = match before_slot {
        Some(before_slot) => {
            let before_slot_i64 = i64::try_from(before_slot).context("before_slot out of range")?;
            let sql = format!(
                "{SELECT_COLUMNS} FROM {market_updates_table} \
                 WHERE market_id = $1 \
                   AND slot < $2 \
                   AND event_uid NOT LIKE 'debug:%' \
                 ORDER BY event_time DESC, slot DESC, event_index DESC \
                 LIMIT $3"
            );
            client
                .query(&sql, &[&market_id_i64, &before_slot_i64, &limit_i64])
                .await
                .context("Failed to query market updates rows")?
        }
        None => {
            let sql = format!(
                "{SELECT_COLUMNS} FROM {market_updates_table} \
                 WHERE market_id = $1 \
                   AND event_uid NOT LIKE 'debug:%' \
                 ORDER BY event_time DESC, slot DESC, event_index DESC \
                 LIMIT $2"
            );
            client
                .query(&sql, &[&market_id_i64, &limit_i64])
                .await
                .context("Failed to query market updates rows")?
        }
    };

    Ok(pg_rows.iter().map(market_history_row_from_pg).collect())
}

async fn query_closed_position_mini_chart_rows(
    pool: &Pool,
    market_updates_table: &str,
    market_id: u64,
    start_slot: u64,
    end_slot: u64,
    max_points: usize,
) -> Result<Vec<(i64, Decimal)>> {
    if max_points == 0 || start_slot > end_slot {
        return Ok(Vec::new());
    }

    let market_id_i64 = i64::try_from(market_id).context("market_id out of range")?;
    let start_slot_i64 = i64::try_from(start_slot).context("start_slot out of range")?;
    let end_slot_i64 = i64::try_from(end_slot).context("end_slot out of range")?;

    let slot_span = end_slot.saturating_sub(start_slot).saturating_add(1);
    let bucket_size = (slot_span as u128).div_ceil(max_points as u128) as i64;
    let bucket_size = bucket_size.max(1);

    let price_expr = "(r.quote_flow::numeric * power(10::numeric, mc.base_decimals::numeric)) \
        / (r.base_flow::numeric * power(10::numeric, mc.quote_decimals::numeric))";

    let client = pool.get().await.context("Failed to get DB connection")?;

    let anchor_sql = format!(
        "SELECT r.slot AS slot, {price_expr} AS price \
         FROM {market_updates_table} r \
         JOIN market_configs mc ON mc.market_id = r.market_id \
         WHERE r.market_id = $1 \
           AND r.base_flow <> 0 \
           AND r.slot < $2 \
           AND mc.base_decimals IS NOT NULL \
           AND mc.quote_decimals IS NOT NULL \
           AND r.event_uid NOT LIKE 'debug:%' \
         ORDER BY r.slot DESC, r.event_index DESC \
         LIMIT 1"
    );

    let mut results: Vec<(i64, Decimal)> = client
        .query(&anchor_sql, &[&market_id_i64, &start_slot_i64])
        .await
        .context("Failed to query closed-position mini chart anchor row")?
        .iter()
        .map(|row| (row.get::<_, i64>("slot"), row.get::<_, Decimal>("price")))
        .collect();

    let sampled_sql = format!(
        "SELECT DISTINCT ON (bucket) slot, price FROM ( \
            SELECT \
                r.slot AS slot, \
                r.event_index AS event_index, \
                (r.slot - $2) / $3 AS bucket, \
                {price_expr} AS price \
            FROM {market_updates_table} r \
            JOIN market_configs mc ON mc.market_id = r.market_id \
            WHERE r.market_id = $1 \
              AND r.base_flow <> 0 \
              AND r.slot >= $2 \
              AND r.slot <= $4 \
              AND mc.base_decimals IS NOT NULL \
              AND mc.quote_decimals IS NOT NULL \
              AND r.event_uid NOT LIKE 'debug:%' \
        ) s \
        ORDER BY bucket, slot ASC, event_index ASC \
        LIMIT $5"
    );

    let max_points_i64 = max_points as i64;
    let mut sampled: Vec<(i64, Decimal)> = client
        .query(
            &sampled_sql,
            &[
                &market_id_i64,
                &start_slot_i64,
                &bucket_size,
                &end_slot_i64,
                &max_points_i64,
            ],
        )
        .await
        .context("Failed to query closed-position mini chart sampled rows")?
        .iter()
        .map(|row| (row.get::<_, i64>("slot"), row.get::<_, Decimal>("price")))
        .collect();

    if results.is_empty() {
        return Ok(sampled);
    }

    results.append(&mut sampled);
    results.sort_by_key(|entry| entry.0);
    results.dedup_by(|left, right| left.0 == right.0);
    Ok(results)
}

fn market_history_row_from_pg(row: &tokio_postgres::Row) -> MarketHistoryRow {
    MarketHistoryRow {
        event_uid: row.get("event_uid"),
        signature: row.get("signature"),
        event_index: row.get("event_index"),
        slot: row.get("slot"),
        market_id: row.get("market_id"),
        base_flow: row.get("base_flow"),
        quote_flow: row.get("quote_flow"),
        event_time_ms: row.get("event_time_ms"),
    }
}

fn history_row_ordering(left: &MarketHistoryRow, right: &MarketHistoryRow) -> std::cmp::Ordering {
    left.slot
        .cmp(&right.slot)
        .then(left.event_index.cmp(&right.event_index))
}

fn market_history_item_from_row(row: MarketHistoryRow) -> Result<MarketHistoryItem, ApiError> {
    let created_at =
        DateTime::<Utc>::from_timestamp_millis(row.event_time_ms).ok_or_else(|| {
            ApiError::internal(anyhow!(
                "Invalid event_time_ms {} for event_uid={}",
                row.event_time_ms,
                row.event_uid
            ))
        })?;

    let event_index = u16::try_from(row.event_index).map_err(|_| {
        ApiError::internal(anyhow!(
            "Invalid event_index {} for event_uid={}",
            row.event_index,
            row.event_uid
        ))
    })?;

    Ok(MarketHistoryItem {
        event_uid: row.event_uid,
        signature: row.signature,
        event_index,
        slot: row.slot.max(0) as u64,
        market_id: row.market_id.max(0) as u64,
        base_flow: row.base_flow.to_string(),
        quote_flow: row.quote_flow.to_string(),
        created_at: created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
    })
}

impl MarketPriceStreams {
    fn new(pool: Pool, market_updates_table: String, poll_interval: Duration) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            runtime: PriceStreamRuntime {
                pool,
                market_updates_table,
            },
            poll_interval,
        }
    }

    async fn subscribe(&self, market_id: u64) -> broadcast::Receiver<LatestPriceResponse> {
        {
            let channels = self.channels.read().await;
            if let Some(sender) = channels.get(&market_id) {
                return sender.subscribe();
            }
        }

        let mut channels = self.channels.write().await;
        if let Some(sender) = channels.get(&market_id) {
            return sender.subscribe();
        }

        let (sender, _receiver) = broadcast::channel::<LatestPriceResponse>(512);
        let runtime = self.runtime.clone();
        let sender_for_task = sender.clone();
        let poll_interval = self.poll_interval;
        tokio::spawn(async move {
            run_market_price_stream(runtime, market_id, sender_for_task, poll_interval).await;
        });

        channels.insert(market_id, sender.clone());
        sender.subscribe()
    }
}

async fn run_market_price_stream(
    runtime: PriceStreamRuntime,
    market_id: u64,
    sender: broadcast::Sender<LatestPriceResponse>,
    poll_interval: Duration,
) {
    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut latest_snapshot_key: Option<(i64, u64)> = None;

    loop {
        match fetch_latest_price_snapshot(&runtime.pool, &runtime.market_updates_table, market_id)
            .await
        {
            Ok(Some(snapshot)) => {
                let snapshot_key = (snapshot.event_time_ms, snapshot.slot);
                let is_newer = latest_snapshot_key
                    .map(|latest_key| snapshot_key > latest_key)
                    .unwrap_or(true);
                if is_newer {
                    latest_snapshot_key = Some(snapshot_key);
                    let _ = sender.send(snapshot);
                }
            }
            Ok(None) => {}
            Err(error) => {
                eprintln!(
                    "Price stream polling error for market_id={}: {:#}",
                    market_id, error
                );
            }
        }

        ticker.tick().await;
    }
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

fn validate_table_name(table: &str) -> Result<String> {
    let trimmed = table.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("Table name must not be empty"));
    }
    if !is_safe_identifier(trimmed) {
        return Err(anyhow!("Unsafe table identifier: {trimmed}"));
    }
    Ok(trimmed.to_string())
}

fn is_safe_identifier(value: &str) -> bool {
    value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '.')
}

async fn ensure_table_exists(client: &tokio_postgres::Client, table: &str) -> Result<()> {
    let row = client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
        .await
        .with_context(|| format!("Failed to check table existence for {table}"))?;
    let exists: bool = row.get(0);
    if exists {
        Ok(())
    } else {
        Err(anyhow!("Required table does not exist: {table}"))
    }
}

fn is_valid_chart_price(value: Decimal) -> bool {
    match value.to_f64() {
        Some(float_value) => {
            float_value.is_finite()
                && float_value > 0.0
                && float_value.abs() <= MAX_LIGHTWEIGHT_CHART_ABS_VALUE
        }
        None => false,
    }
}
