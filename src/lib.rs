//! TwoB Keepers Library
//!
//! A library for building and sending transactions to interact with the TwoB Anchor program.
//! This library provides utilities for the bookkeeper, liquidity-keeper, and trade-keeper binaries.

pub mod accounts;
pub mod database;
pub mod monitoring;
pub mod sink;

anchor_lang::declare_program!(twob_anchor);

// Re-export commonly used types
pub use accounts::{AccountResolver, PdaResult};
pub use database::TimescaleSink;
pub use sink::{
    ClosePositionEventRecord, EventSink, FanoutSink, MarketUpdateEventRecord, SinkMetricsSnapshot,
};

/// The TwoB Anchor program ID
pub const TWOB_PROGRAM_ID: &str = "TwobwMYkKbT8uMWqgPrEPXTPoyYsKAPmaWun6T2WT4A";

/// Parse the program ID from the constant string
pub fn program_id() -> anchor_lang::prelude::Pubkey {
    twob_anchor::ID
}

pub const ARRAY_LENGTH: u64 = 16;
pub const END_SLOT_INTERVAL: u64 = 11;
pub const MAXIMUM_DURATION_SLOTS: u64 = 160_000_000;

/// Canonical market account to operate on; numeric market IDs are scoped to mint pairs.
pub fn market_address_from_env(key: &str) -> anyhow::Result<anchor_lang::prelude::Pubkey> {
    use anyhow::Context;
    std::env::var(key)
        .with_context(|| format!("{key} must be set to the market address"))?
        .parse()
        .with_context(|| format!("{key} must be a valid Solana public key"))
}
