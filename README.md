# TwoB Keepers

Rust services and shared utilities for operating the TwoB Anchor program. The
repository contains keepers that submit on-chain maintenance transactions,
ingest program events from Solana logs, persist those events, and expose a
read API for market data consumers.

## What is included

| Binary | Purpose |
| --- | --- |
| `bookkeeper` | Periodically checks a market's market account and sends `update_books` when the configured slot interval has elapsed. |
| `bookkeeper-canary` | Independently reads the mainnet market account with embedded bookkeeping and exports chain-level freshness metrics without holding a payer or sending transactions. |
| `event-keeper` | Subscribes to Solana transaction logs, decodes TwoB Anchor events, and writes market updates and close-position events to Tiger Cloud (TimescaleDB), recomputing 1-minute candles on every market update. |
| `read-api` | Serves HTTP endpoints for market configs, latest price, price streams, candles, market history, recent updates, closed-position mini charts, and per-wallet closed positions. |
| `trade-keeper` | Closes ended trade positions and abandoned paused positions for `MARKET_ADDRESS`, using the stored receivers and each mint's token program. |
| `liquidity-keeper` | Placeholder binary. |

The shared library exports PDA resolution helpers, event sink abstractions, and
the Tiger Cloud (TimescaleDB) sink implementation used by the binaries.

## Requirements

- Rust 1.85 or newer
- A Solana RPC endpoint and WebSocket endpoint for the target cluster
- A funded payer keypair for transaction-sending keepers
- A Tiger Cloud (TimescaleDB / Postgres) database for market configuration,
  event storage, candles, and read-api queries (TLS required)

## v1 upgrade

This branch targets mainnet program `TwobwMYkKbT8uMWqgPrEPXTPoyYsKAPmaWun6T2WT4A`.
See [the v1 upgrade guide](docs/v1-upgrade.md) for required environment, database,
API, and monitoring changes before rollout.

## Setup

```bash
cp .env.example .env
cargo build
```

Fill `.env` with the values needed by the binary you want to run. Shared
variables are reused where possible:

```bash
CLUSTER_RPC_URL=https://...
CLUSTER_WS_URL=wss://...

# Tiger Cloud requires TLS — include sslmode=require
DATABASE_URL=postgres://tsdbadmin:<password>@<host>.tsdb.cloud.timescale.com:5432/tsdb?sslmode=require
```

`bookkeeper` and `trade-keeper` require (cadence is only used by `bookkeeper`):

```bash
PAYER_KEYPAIR=[...]
MARKET_ADDRESS=<market-address>
SLOTS_BETWEEN_UPDATES=40
```

`PAYER_KEYPAIR` is expected to be a JSON array of keypair bytes.

The bookkeeper supports staggered active redundancy. Give every replica a
unique `BOOKKEEPER_ID` and use different update cadences so one normally reaches
the target first. Both replicas still operate independently and can race when
the earlier replica is delayed; the pre-signing and post-expiry account checks
avoid submitting an update after another replica has already reached the
target. The process signs each update once, logs its signature immediately, and
rebroadcasts that same signed transaction every 1.5 seconds until it is
confirmed or its blockhash expires. Only an expired blockhash causes a newly
signed transaction. The keeper also estimates a localized priority fee from the
update's writable accounts and simulates the instruction to apply a tight
compute-unit limit.

Optional transaction tuning variables (defaults shown):

```bash
BOOKKEEPER_REBROADCAST_INTERVAL_MS=1500
BOOKKEEPER_SEND_RETRY_ATTEMPTS=8       # maximum blockhash lifetimes
BOOKKEEPER_PRIORITY_FEE_PERCENTILE=75
BOOKKEEPER_PRIORITY_FEE_MIN_MICRO_LAMPORTS=10000
BOOKKEEPER_PRIORITY_FEE_MAX_MICRO_LAMPORTS=1000000
BOOKKEEPER_COMPUTE_UNIT_LIMIT=40000    # fallback if simulation RPC fails
BOOKKEEPER_COMPUTE_UNIT_MIN=30000
BOOKKEEPER_COMPUTE_UNIT_MAX=100000
BOOKKEEPER_COMPUTE_UNIT_MARGIN_BPS=12000
```

Route `BOOKKEEPER_STALENESS_WARNING` and `BOOKKEEPER_CRITICAL` log lines to
alerts. They report slot lag, remaining slots before the one-array freshness
boundary, and confirmed transactions that did not produce the expected state.

### Bookkeeper monitoring

The bookkeeper exposes Prometheus/OpenMetrics metrics and health endpoints on
`BOOKKEEPER_MONITOR_BIND_ADDR`. If it is unset, the server uses `PORT`, then
falls back to `0.0.0.0:8080`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/metrics` | Prometheus/OpenMetrics scrape target |
| `GET` | `/livez` | Process/server liveness; always returns HTTP 200 while the server is alive |
| `GET` | `/readyz` | Returns HTTP 503 while starting, stalled, or at critical lag |

Every `bookkeeper_*` metric has `bookkeeper_id`, `cluster`, `market_address`, and
`slots_between_updates` labels. Configure the identity explicitly for each
service:

```bash
# 40-slot instance
BOOKKEEPER_ID=mainnet-market-1-40
BOOKKEEPER_CLUSTER=mainnet
SLOTS_BETWEEN_UPDATES=40

# 45-slot instance
BOOKKEEPER_ID=mainnet-market-1-45
BOOKKEEPER_CLUSTER=mainnet
SLOTS_BETWEEN_UPDATES=45
```

The v1 freshness boundary is `END_SLOT_INTERVAL * ARRAY_LENGTH = 11 * 16 = 176`
slots. Warning starts at 124 slots and critical at 176. Startup fails if the
configured cadence is at or above warning; existing 40/45-slot cadences remain valid.

The key gauges are `bookkeeper_lag_slots`,
`bookkeeper_freshness_remaining_slots`, `bookkeeper_overdue_slots`, and
`bookkeeper_payer_balance_lamports`. Transaction, broadcast, RPC, blockhash
expiry, and loop-outcome counters provide worker-level failure diagnostics.

The separate `bookkeeper-canary` observes the same accounts through an
independent mainnet RPC endpoint. It validates the cluster genesis hash,
account ownership, and Anchor discriminators before publishing
`bookkeeper_chain_*` metrics. This distinguishes a real on-chain freshness
problem from a failure in one bookkeeper process or its RPC provider. See
[`docs/bookkeeper-canary.md`](docs/bookkeeper-canary.md) for Railway deployment.
The complete production topology, update flow, monitoring path, and failure
model are documented in
[`docs/bookkeeping-service-architecture.md`](docs/bookkeeping-service-architecture.md).

`event-keeper` requires `DATABASE_URL` pointing at Tiger Cloud:

```bash
DATABASE_URL=postgres://...?sslmode=require
```

`read-api` uses the same `DATABASE_URL` (override with `READ_API_DATABASE_URL`):

```bash
DATABASE_URL=postgres://...?sslmode=require
READ_API_BIND_ADDR=0.0.0.0:8080
```

## Tiger Cloud schema

The keeper and read-api expect these tables (see `docs/timescale-schema.sql`):

- `v1_raw_market_update_events` — hypertable of decoded market updates
- `v1_raw_close_position_events` — hypertable of decoded close-position events
- `v1_market_candles_1m` — hypertable of 1-minute OHLC candles, upserted by the
  keeper on every market update
- `v1_market_configs` — market token decimals/metadata (used to compute prices)

Candles are stored as true prices (`numeric`); the keeper computes them in SQL
by joining `v1_market_configs` for the token decimals. Empty minutes are not
written — the read-api gap-fills them by carrying the last close forward.

Table-name overrides (defaults shown):

| Variable | Default |
| --- | --- |
| `MARKET_UPDATES_TABLE` | `v1_raw_market_update_events` |
| `CANDLES_1M_TABLE` | `v1_market_candles_1m` |

## Known limitations / follow-ups

**Event de-duplication is best-effort.** `event_uid`
(`<type>:<signature>:<event_index>`) is the natural idempotency key, but
TimescaleDB requires every unique index on a hypertable to include the
partitioning column (`event_time`), and the keeper stamps `event_time = now()`
per ingest. A re-delivered event therefore gets a new `event_time` and is not
deduplicated.

In practice this is low-risk: `logsSubscribe` does not replay history on
reconnect, so a single keeper instance rarely sees duplicates, and the candle
upsert is naturally idempotent (re-applying the same price does not move
OHLC) — the only artifact is an occasional duplicate row in
`v1_raw_market_update_events` / `v1_raw_close_position_events`, visible in `/history`
and `/updates`.

**This must be addressed before** running the keeper active-active (multiple
replicas) or adding a historical backfill job, since both turn duplicates from
rare into guaranteed. The simplest fix is a regular (non-hypertable)
`processed_events(event_uid PRIMARY KEY)` table that both inserts are gated
through; making `event_time` deterministic (on-chain `block_time`) is an
alternative that also improves chart-time accuracy. Note that missed events
during keeper downtime (gaps) are a separate, currently-unaddressed concern that
idempotency does not solve.

## Running services

Run the bookkeeper for one market:

```bash
cargo run --bin bookkeeper
```

Run the read-only chain canary:

```bash
cargo run --bin bookkeeper-canary
```

Run the event ingester:

```bash
cargo run --bin event-keeper
```

Run the read API:

```bash
cargo run --bin read-api
```

The read API binds to `READ_API_BIND_ADDR`, then `PORT`, then
`0.0.0.0:8080`.

## Read API

Available endpoints:

| Method | Path |
| --- | --- |
| `GET` | `/healthz` |
| `GET` | `/v1/markets` |
| `GET` | `/v1/markets/{market_address}/config` |
| `GET` | `/v1/markets/{market_address}/price` |
| `GET` | `/v1/markets/{market_address}/stream` |
| `GET` | `/v1/markets/{market_address}/candles?from=...&to=...&interval=1m` |
| `GET` | `/v1/markets/{market_address}/history?start_slot=...&end_slot=...` |
| `GET` | `/v1/markets/{market_address}/updates` |
| `GET` | `/v1/markets/{market_address}/closed-position-mini-chart?start_slot=...&end_slot=...` |
| `GET` | `/v1/authorities/{authority}/closed-positions?market_address=...&before_slot=...&limit=...` |

Supported candle intervals are `1m`, `5m`, `15m`, `1h`, `4h`, and `1d`.

`/v1/markets` lists every market config (token mints, decimals, tickers) and
`/v1/markets/{market_address}/config` returns a single one. Both send
`Cache-Control: public, max-age=300, stale-while-revalidate=60` since configs
change very rarely.

`/v1/authorities/{authority}/closed-positions` returns a wallet's closed
positions newest-first. It pages with `before_slot`/`limit` (keyset, like
`/updates`, max `limit` 5000) and returns `has_more`; `market_address` optionally
filters to one market. Amounts are raw on-chain integers — scale them with the
token decimals from the market-config endpoints.

## Docker

The Dockerfile builds one binary at a time using the `BIN_NAME` build argument:

```bash
docker build --build-arg BIN_NAME=bookkeeper -t twob-bookkeeper .
docker build --build-arg BIN_NAME=bookkeeper-canary -t bookkeeper-canary .
docker build --build-arg BIN_NAME=event-keeper -t twob-event-keeper .
docker build --build-arg BIN_NAME=read-api -t twob-read-api .
```

Run the resulting image with the same environment variables used locally.

## Development

```bash
cargo fmt
cargo test
cargo run --example accounts_usage
```

Useful source areas:

- `src/accounts`: PDA and token account resolution helpers
- `src/bin`: service entrypoints
- `src/database.rs`: Tiger Cloud (TimescaleDB) event sink and candle upsert
- `src/sink.rs`: event sink trait and fanout implementation
- `docs`: TimescaleDB schema and migration notes
