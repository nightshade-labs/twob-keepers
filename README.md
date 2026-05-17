# TwoB Keepers

Rust services and shared utilities for operating the TwoB Anchor program. The
repository contains keepers that submit on-chain maintenance transactions,
ingest program events from Solana logs, persist those events, and expose a
read API for market data consumers.

## What is included

| Binary | Purpose |
| --- | --- |
| `bookkeeper` | Periodically checks a market's bookkeeping account and sends `update_books` when the configured slot interval has elapsed. |
| `event-keeper` | Subscribes to Solana transaction logs, decodes TwoB Anchor events, and writes market updates and close-position events to Postgres and/or ClickHouse. |
| `read-api` | Serves HTTP endpoints for latest price, price streams, candles, market history, recent updates, and closed-position mini charts. |
| `trade-keeper` | Experimental keeper for publicly closing expired trade positions. It currently contains hard-coded defaults and should be reviewed before production use. |
| `liquidity-keeper` | Placeholder binary. |

The shared library exports PDA resolution helpers, event sink abstractions, and
Postgres/ClickHouse sink implementations used by the binaries.

## Requirements

- Rust 1.85 or newer
- A Solana RPC endpoint and WebSocket endpoint for the target cluster
- A funded payer keypair for transaction-sending keepers
- Postgres for market configuration and optional event writes
- ClickHouse for analytical event storage and read-api queries

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

CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_DATABASE=mato
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

`bookkeeper` also requires:

```bash
PAYER_KEYPAIR=[...]
MARKET_ID=1
SLOTS_BETWEEN_UPDATES=100
```

`PAYER_KEYPAIR` is expected to be a JSON array of keypair bytes.

`event-keeper` requires at least one sink:

```bash
DATABASE_URL=postgresql://...
# or
CLICKHOUSE_URL=http://...
```

`read-api` requires ClickHouse plus a Postgres-compatible config database so it
can resolve market token decimals:

```bash
CLICKHOUSE_URL=http://...
READ_API_CONFIG_DATABASE_URL=postgresql://...
READ_API_BIND_ADDR=0.0.0.0:8080
```

`READ_API_CONFIG_DATABASE_URL` overrides `DATABASE_URL` for the read API.

## ClickHouse

Create the raw event tables before starting `event-keeper`:

```bash
clickhouse-client --multiquery < docs/clickhouse-events-schema.sql
```

Create the candle aggregation table and materialized view before serving
candles from `read-api`:

```bash
clickhouse-client --multiquery < docs/clickhouse-candles-schema.sql
```

For historical candle generation, see:

```bash
docs/clickhouse-candles-backfill.sql
```

The ClickHouse sink uses these defaults unless overridden:

| Variable | Default |
| --- | --- |
| `CLICKHOUSE_DATABASE` | `mato` |
| `CLICKHOUSE_USER` | `default` |
| `CLICKHOUSE_MARKET_UPDATES_TABLE` | `raw_market_update_events` |
| `CLICKHOUSE_CLOSE_POSITIONS_TABLE` | `raw_close_position_events` |
| `CLICKHOUSE_CHANNEL_CAPACITY` | `20000` |
| `CLICKHOUSE_BATCH_SIZE` | `1000` |
| `CLICKHOUSE_FLUSH_INTERVAL_MS` | `1000` |

## Running services

Run the bookkeeper for one market:

```bash
cargo run --bin bookkeeper
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
| `GET` | `/v1/markets/{market_id}/price` |
| `GET` | `/v1/markets/{market_id}/stream` |
| `GET` | `/v1/markets/{market_id}/candles?from=...&to=...&interval=1m` |
| `GET` | `/v1/markets/{market_id}/history?start_slot=...&end_slot=...` |
| `GET` | `/v1/markets/{market_id}/updates` |
| `GET` | `/v1/markets/{market_id}/closed-position-mini-chart?start_slot=...&end_slot=...` |

Supported candle intervals are `1m`, `5m`, `15m`, `1h`, `4h`, and `1d`.

## Docker

The Dockerfile builds one binary at a time using the `BIN_NAME` build argument:

```bash
docker build --build-arg BIN_NAME=bookkeeper -t twob-bookkeeper .
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
- `src/clickhouse_sink.rs`: asynchronous batched ClickHouse writes
- `src/database.rs`: Postgres event sink
- `src/sink.rs`: event sink trait and fanout implementation
- `docs`: ClickHouse schemas and migration notes
