CREATE DATABASE IF NOT EXISTS mato;

CREATE TABLE IF NOT EXISTS mato.raw_market_update_events
(
    event_uid String,
    signature String,
    event_index UInt16,
    slot UInt64,
    market_id UInt64,
    base_flow UInt64,
    quote_flow UInt64,
    event_time DateTime64(3, 'UTC') DEFAULT now64(3),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (market_id, slot, event_index, event_uid);

CREATE TABLE IF NOT EXISTS mato.raw_close_position_events
(
    event_uid String,
    signature String,
    event_index UInt16,
    slot UInt64,
    position_authority String,
    market_id UInt64,
    start_slot UInt64,
    end_slot UInt64,
    deposit_amount UInt64,
    swapped_amount UInt64,
    remaining_amount UInt64,
    fee_amount UInt64,
    is_buy UInt8,
    event_time DateTime64(3, 'UTC') DEFAULT now64(3),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (market_id, position_authority, end_slot, slot, event_index, event_uid);
