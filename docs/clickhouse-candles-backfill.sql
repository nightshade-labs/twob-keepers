-- One-time backfill for historical rows.
-- Run only once on an empty mato.market_candles_1m table (or after TRUNCATE),
-- otherwise you'll double-count volume and duplicate aggregates.
INSERT INTO mato.market_candles_1m
SELECT
    market_id,
    toStartOfMinute(event_time) AS bucket_start,
    argMinState(
        abs(toFloat64(quote_flow)) / abs(toFloat64(base_flow)),
        tuple(event_time, slot, event_uid)
    ) AS open_state,
    maxState(abs(toFloat64(quote_flow)) / abs(toFloat64(base_flow))) AS high_state,
    minState(abs(toFloat64(quote_flow)) / abs(toFloat64(base_flow))) AS low_state,
    argMaxState(
        abs(toFloat64(quote_flow)) / abs(toFloat64(base_flow)),
        tuple(event_time, slot, event_uid)
    ) AS close_state,
    sumState(abs(toFloat64(quote_flow))) AS quote_volume_state,
    minState(slot) AS start_slot_state,
    maxState(slot) AS end_slot_state,
    countState() AS points_state
FROM mato.raw_market_update_events
WHERE base_flow != 0
  AND NOT startsWith(event_uid, 'debug:')
GROUP BY market_id, bucket_start;
