-- Synthetic test data for verifying the keeper candle logic and read-api.
--
-- Uses a throwaway market_id (999999) so it does not touch real data.
-- Run against your Tiger Cloud DB:
--   psql "$DATABASE_URL" -f docs/test-data.sql
--
-- The INSERT below is a faithful copy of the keeper's INSERT_MARKET_UPDATE_SQL
-- (src/database.rs) with literal values, so it exercises the real candle math:
-- carry-forward open, GREATEST/LEAST high/low, last-write close, base_flow=0 skip.
--
-- Decimals base=9, quote=6  ->  price = quote/base * 10^(9-6) = quote/base * 1000.

\set market 999999

-- Clean any previous run.
DELETE FROM market_candles_1m         WHERE market_id = :market;
DELETE FROM raw_market_update_events  WHERE market_id = :market;
DELETE FROM market_configs            WHERE market_id = :market;

INSERT INTO market_configs (market_id, base_mint, quote_mint, base_decimals, quote_decimals, base_ticker, quote_ticker)
VALUES (:market, 'BaseMintTest', 'QuoteMintTest', 9, 6, 'TST', 'USDC');

-- Helper: one statement per event. Mirrors the keeper's combined upsert.
-- Args inlined: event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, event_time.

-- 12:00:00  base 5e9 quote 715_600_000  -> price 143.12  (first candle: open=close=143.12)
WITH ev AS (
    INSERT INTO raw_market_update_events
        (event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, event_time)
    VALUES ('market_update:sigA:0', 'sigA', 0, 400, :market, 5000000000, 715600000, '2026-06-22 12:00:10+00')
    ON CONFLICT DO NOTHING
    RETURNING market_id, base_flow, quote_flow, event_time
), p AS (
    SELECT ev.market_id, date_trunc('minute', ev.event_time) AS bucket_start,
        (ev.quote_flow::numeric * power(10::numeric, mc.base_decimals::numeric))
            / (ev.base_flow::numeric * power(10::numeric, mc.quote_decimals::numeric)) AS price
    FROM ev JOIN market_configs mc ON mc.market_id = ev.market_id
    WHERE ev.base_flow <> 0 AND mc.base_decimals IS NOT NULL AND mc.quote_decimals IS NOT NULL
)
INSERT INTO market_candles_1m (market_id, bucket_start, open, high, low, close, updated_at)
SELECT p.market_id, p.bucket_start,
    COALESCE((SELECT c.close FROM market_candles_1m c
        WHERE c.market_id = p.market_id AND c.bucket_start < p.bucket_start
        ORDER BY c.bucket_start DESC LIMIT 1), p.price),
    p.price, p.price, p.price, now()
FROM p
ON CONFLICT (market_id, bucket_start) DO UPDATE SET
    high = GREATEST(market_candles_1m.high, EXCLUDED.close),
    low  = LEAST(market_candles_1m.low,  EXCLUDED.close),
    close = EXCLUDED.close, updated_at = now();

-- 12:00:30  base 5e9 quote 720_000_000  -> price 144.0  (same bucket: high->144, close->144, open stays 143.12)
WITH ev AS (
    INSERT INTO raw_market_update_events
        (event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, event_time)
    VALUES ('market_update:sigB:0', 'sigB', 0, 401, :market, 5000000000, 720000000, '2026-06-22 12:00:30+00')
    ON CONFLICT DO NOTHING
    RETURNING market_id, base_flow, quote_flow, event_time
), p AS (
    SELECT ev.market_id, date_trunc('minute', ev.event_time) AS bucket_start,
        (ev.quote_flow::numeric * power(10::numeric, mc.base_decimals::numeric))
            / (ev.base_flow::numeric * power(10::numeric, mc.quote_decimals::numeric)) AS price
    FROM ev JOIN market_configs mc ON mc.market_id = ev.market_id
    WHERE ev.base_flow <> 0 AND mc.base_decimals IS NOT NULL AND mc.quote_decimals IS NOT NULL
)
INSERT INTO market_candles_1m (market_id, bucket_start, open, high, low, close, updated_at)
SELECT p.market_id, p.bucket_start,
    COALESCE((SELECT c.close FROM market_candles_1m c
        WHERE c.market_id = p.market_id AND c.bucket_start < p.bucket_start
        ORDER BY c.bucket_start DESC LIMIT 1), p.price),
    p.price, p.price, p.price, now()
FROM p
ON CONFLICT (market_id, bucket_start) DO UPDATE SET
    high = GREATEST(market_candles_1m.high, EXCLUDED.close),
    low  = LEAST(market_candles_1m.low,  EXCLUDED.close),
    close = EXCLUDED.close, updated_at = now();

-- (12:01 intentionally has NO event -> read-api must gap-fill flat at 144)

-- 12:02:05  base 5e9 quote 700_000_000  -> price 140.0  (new bucket: open carries forward 144, low->140, close->140)
WITH ev AS (
    INSERT INTO raw_market_update_events
        (event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, event_time)
    VALUES ('market_update:sigC:0', 'sigC', 0, 402, :market, 5000000000, 700000000, '2026-06-22 12:02:05+00')
    ON CONFLICT DO NOTHING
    RETURNING market_id, base_flow, quote_flow, event_time
), p AS (
    SELECT ev.market_id, date_trunc('minute', ev.event_time) AS bucket_start,
        (ev.quote_flow::numeric * power(10::numeric, mc.base_decimals::numeric))
            / (ev.base_flow::numeric * power(10::numeric, mc.quote_decimals::numeric)) AS price
    FROM ev JOIN market_configs mc ON mc.market_id = ev.market_id
    WHERE ev.base_flow <> 0 AND mc.base_decimals IS NOT NULL AND mc.quote_decimals IS NOT NULL
)
INSERT INTO market_candles_1m (market_id, bucket_start, open, high, low, close, updated_at)
SELECT p.market_id, p.bucket_start,
    COALESCE((SELECT c.close FROM market_candles_1m c
        WHERE c.market_id = p.market_id AND c.bucket_start < p.bucket_start
        ORDER BY c.bucket_start DESC LIMIT 1), p.price),
    p.price, p.price, p.price, now()
FROM p
ON CONFLICT (market_id, bucket_start) DO UPDATE SET
    high = GREATEST(market_candles_1m.high, EXCLUDED.close),
    low  = LEAST(market_candles_1m.low,  EXCLUDED.close),
    close = EXCLUDED.close, updated_at = now();

-- 12:02:30  base 0 (undefined price) -> raw row inserted, candle skipped.
WITH ev AS (
    INSERT INTO raw_market_update_events
        (event_uid, signature, event_index, slot, market_id, base_flow, quote_flow, event_time)
    VALUES ('market_update:sigD:0', 'sigD', 0, 403, :market, 0, 700000000, '2026-06-22 12:02:30+00')
    ON CONFLICT DO NOTHING
    RETURNING market_id, base_flow, quote_flow, event_time
), p AS (
    SELECT ev.market_id, date_trunc('minute', ev.event_time) AS bucket_start,
        (ev.quote_flow::numeric * power(10::numeric, mc.base_decimals::numeric))
            / (ev.base_flow::numeric * power(10::numeric, mc.quote_decimals::numeric)) AS price
    FROM ev JOIN market_configs mc ON mc.market_id = ev.market_id
    WHERE ev.base_flow <> 0 AND mc.base_decimals IS NOT NULL AND mc.quote_decimals IS NOT NULL
)
INSERT INTO market_candles_1m (market_id, bucket_start, open, high, low, close, updated_at)
SELECT p.market_id, p.bucket_start,
    COALESCE((SELECT c.close FROM market_candles_1m c
        WHERE c.market_id = p.market_id AND c.bucket_start < p.bucket_start
        ORDER BY c.bucket_start DESC LIMIT 1), p.price),
    p.price, p.price, p.price, now()
FROM p
ON CONFLICT (market_id, bucket_start) DO UPDATE SET
    high = GREATEST(market_candles_1m.high, EXCLUDED.close),
    low  = LEAST(market_candles_1m.low,  EXCLUDED.close),
    close = EXCLUDED.close, updated_at = now();

-- ---------------------------------------------------------------------------
-- Verify stored candles. Expected:
--   12:00  open 143.12  high 144  low 143.12  close 144
--   12:02  open 144     high 144  low 140     close 140
-- (no 12:01 row; no 12:02-base-0 effect)
-- ---------------------------------------------------------------------------
SELECT bucket_start, open, high, low, close
FROM market_candles_1m
WHERE market_id = :market
ORDER BY bucket_start;

-- Verify raw rows (4 rows incl. the base_flow=0 one).
SELECT event_uid, slot, base_flow, quote_flow, event_time
FROM raw_market_update_events
WHERE market_id = :market
ORDER BY event_time;

-- ---------------------------------------------------------------------------
-- read-api gap-fill check (run separately, mirrors the /candles SQL).
-- Expected over 11:58..12:05 at 1m: 12:00 real, 12:01 flat@144, 12:02 real,
-- 12:03/12:04 flat@140. 11:58/11:59 dropped (no anchor before 12:00).
-- ---------------------------------------------------------------------------
SELECT
    time_bucket_gapfill('1 minute', bucket_start) AS bucket,
    first(open, bucket_start)  AS open,
    max(high)                  AS high,
    min(low)                   AS low,
    last(close, bucket_start)  AS close,
    locf(last(close, bucket_start)) AS carried_close
FROM market_candles_1m
WHERE market_id = :market
  AND bucket_start >= '2026-06-22 11:58:00+00'
  AND bucket_start <  '2026-06-22 12:05:00+00'
GROUP BY 1
ORDER BY 1;

-- Cleanup (uncomment to remove the test market):
-- DELETE FROM market_candles_1m        WHERE market_id = 999999;
-- DELETE FROM raw_market_update_events WHERE market_id = 999999;
-- DELETE FROM market_configs           WHERE market_id = 999999;
