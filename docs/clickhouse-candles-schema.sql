CREATE TABLE IF NOT EXISTS mato.market_candles_1m
(
    market_id UInt64,
    bucket_start DateTime64(3, 'UTC'),

    open_state AggregateFunction(argMin, Float64, Tuple(DateTime64(3, 'UTC'), UInt64, String)),
    high_state AggregateFunction(max, Float64),
    low_state AggregateFunction(min, Float64),
    close_state AggregateFunction(argMax, Float64, Tuple(DateTime64(3, 'UTC'), UInt64, String)),

    quote_volume_state AggregateFunction(sum, Float64),
    start_slot_state AggregateFunction(min, UInt64),
    end_slot_state AggregateFunction(max, UInt64),
    points_state AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (market_id, bucket_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS mato.mv_market_candles_1m
TO mato.market_candles_1m
AS
SELECT
    market_id,
    toStartOfMinute(event_time) AS bucket_start,

    argMinState(
        abs(toFloat64(quote_flow)) / nullIf(abs(toFloat64(base_flow)), 0.0),
        tuple(event_time, slot, event_uid)
    ) AS open_state,
    maxState(abs(toFloat64(quote_flow)) / nullIf(abs(toFloat64(base_flow)), 0.0)) AS high_state,
    minState(abs(toFloat64(quote_flow)) / nullIf(abs(toFloat64(base_flow)), 0.0)) AS low_state,
    argMaxState(
        abs(toFloat64(quote_flow)) / nullIf(abs(toFloat64(base_flow)), 0.0),
        tuple(event_time, slot, event_uid)
    ) AS close_state,

    sumState(abs(toFloat64(quote_flow))) AS quote_volume_state,
    minState(slot) AS start_slot_state,
    maxState(slot) AS end_slot_state,
    countState() AS points_state
FROM mato.raw_market_update_events
WHERE base_flow != 0
GROUP BY market_id, bucket_start;
