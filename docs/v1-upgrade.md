# v1 rollout

Target mainnet program: `TwobwMYkKbT8uMWqgPrEPXTPoyYsKAPmaWun6T2WT4A`.
The deployment was verified by RPC at deployment slot 452075425.
The checked-in IDL comes from `nightshade-labs/twob-anchor` commit
`5fcaa62a03206535177bd6ba15fce14a1c1d378b`, with its address replaced by the
deployed address supplied by the operator. Anchor's IDL fetch did not find a
published IDL for this deployment; the deployed interface has not been independently
verified against an on-chain IDL. At validation time the program owned only its
152-byte program-config account; no markets had been initialized, so a live
market decode and transaction simulation were not possible.

## Keeper configuration

Set `MARKET_ADDRESS` to the full market public key for each bookkeeper and trade
keeper. Set `BOOKKEEPER_CANARY_MARKET_ADDRESS` to the same key for its independent
canary. The old `MARKET_ID` variables are no longer sufficient: v1 scopes a `u32`
market ID to an ordered base/quote mint pair. Both open-liquidity and dedicated
markets use `["market", base_mint, quote_mint, id_le_u32]`.

`PAYER_KEYPAIR`, `CLUSTER_RPC_URL`, and `CLUSTER_WS_URL` are used by both transaction
keepers. The trade keeper no longer reads a hard-coded wallet path. Use mainnet
RPCs for this build. The canary defaults to the same program ID and still checks
the mainnet genesis hash.

The program has 16 entries per interval, spaced 11 slots apart. Freshness critical
is 176 slots; warning is 124. Existing 40/45-slot update cadences are valid. Market
bookkeeping is embedded in `Market`; there is no standalone bookkeeping PDA.
`MarketInterval` replaces the old exits and prices accounts.

## Database

Run the additive schema before starting the v1 event keeper or read API:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/timescale-schema.sql
```

This creates `v1_market_configs`, `v1_raw_market_update_events`,
`v1_raw_close_position_events`, and `v1_market_candles_1m`. Existing v0 tables and
history stay intact. Populate `v1_market_configs` with each new **market address**,
mints, decimals, and tickers before expecting prices or candles. Numeric IDs alone
cannot map legacy history to newly deployed markets, so history is not copied.

Flow columns are `NUMERIC(39,0)` for the full unsigned 128-bit range. Close amounts
are `NUMERIC(20,0)` for unsigned 64-bit values. The keeper binds these values as
strings and casts in PostgreSQL; read API history returns exact decimal strings.
Both flows have the same protocol precision factor, which cancels in price ratios.
Close events retain the position address and both receivers, separately from the
position authority. Failed transactions are ignored because their events roll back.

Remove old table-name overrides or point them to the v1 tables. The sink always
writes the standard v1 table names; read API overrides are for compatible tables.
Do not point the v1 API to the v0 numeric-ID schema.

## API and monitoring consumers

Market routes keep their `/v1/markets/` prefix, but the path component is now a
base58 address. Response objects expose `market_address` instead of `market_id`.
The optional closed-position query filter is `market_address=<address>`.
Update consumers together with this deployment. All price/history/config joins
and SSE channels are keyed by address, so same-numbered markets cannot collide.

Metrics and health responses likewise use `market_address`. Import the updated
Grafana dashboard and select the market address. Update saved alerts and external
queries from `market_id="1"` to the new address selector. Canary program identity
and genesis checks remain independent of the transaction senders.

## Validation before enabling transaction keepers

Start the read-only canary against the selected market and verify its owner,
account layout, last-update slot, and 176-slot freshness boundary. Start the event
keeper and confirm v1 rows arrive. Bookkeeper simulations and confirmation checks
remain enabled; monitor simulation errors when starting the transaction keepers.
No mainnet transactions or database migrations were executed as part of this code
migration.
