# TwoB Keepers

Rust services and shared utilities for operating the TwoB Anchor program. The
repository contains keepers that submit on-chain maintenance transactions,
ingest program events from Solana logs, persist those events, and expose a
read API for market data consumers.

## What is included

| Binary | Purpose |
| --- | --- |
| `bookkeeper` | Periodically checks a market's bookkeeping account and sends `update_books` when the configured slot interval has elapsed. |
| `event-keeper` | Subscribes to Solana transaction logs, decodes TwoB Anchor events, and writes market updates and close-position events to Tiger Cloud (TimescaleDB), recomputing 1-minute candles on every market update. |
| `read-api` | Serves HTTP endpoints for latest price, price streams, candles, market history, recent updates, and closed-position mini charts. |
| `trade-keeper` | Experimental keeper for publicly closing expired trade positions. It currently contains hard-coded defaults and should be reviewed before production use. |
| `liquidity-keeper` | Placeholder binary. |

The shared library exports PDA resolution helpers, event sink abstractions, and
the Tiger Cloud (TimescaleDB) sink implementation used by the binaries.

## Requirements

- Rust 1.85 or newer
- A Solana RPC endpoint and WebSocket endpoint for the target cluster
- A funded payer keypair for transaction-sending keepers
- A Tiger Cloud (TimescaleDB / Postgres) database for market configuration,
  event storage, candles, and read-api queries (TLS required)

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

`bookkeeper` also requires:

```bash
PAYER_KEYPAIR=[...]
MARKET_ID=1
SLOTS_BETWEEN_UPDATES=100
```

`PAYER_KEYPAIR` is expected to be a JSON array of keypair bytes.

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

- `raw_market_update_events` — hypertable of decoded market updates
- `raw_close_position_events` — hypertable of decoded close-position events
- `market_candles_1m` — hypertable of 1-minute OHLC candles, upserted by the
  keeper on every market update
- `market_configs` — market token decimals/metadata (used to compute prices)

Candles are stored as true prices (`numeric`); the keeper computes them in SQL
by joining `market_configs` for the token decimals. Empty minutes are not
written — the read-api gap-fills them by carrying the last close forward.

Table-name overrides (defaults shown):

| Variable | Default |
| --- | --- |
| `MARKET_UPDATES_TABLE` | `raw_market_update_events` |
| `CANDLES_1M_TABLE` | `market_candles_1m` |

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
- `src/database.rs`: Tiger Cloud (TimescaleDB) event sink and candle upsert
- `src/sink.rs`: event sink trait and fanout implementation
- `docs`: TimescaleDB schema and migration notes
