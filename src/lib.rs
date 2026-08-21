//! TwoB Keepers Library
//!
//! A library for building and sending transactions to interact with the TwoB Anchor program.
//! This library provides utilities for the bookkeeper, liquidity-keeper, and trade-keeper binaries.

pub mod accounts;
pub mod database;
pub mod sink;

// Re-export commonly used types
pub use accounts::{AccountResolver, PdaResult};
pub use database::TimescaleSink;
pub use sink::{
    ClosePositionEventRecord, EventSink, FanoutSink, MarketUpdateEventRecord, SinkMetricsSnapshot,
};

/// The TwoB Anchor program ID
pub const TWOB_PROGRAM_ID: &str = "CCAd78ZgUBAFNQmCCD5z4oGuFzb8uXLw5kfnBcRvDw16";

/// Parse the program ID from the constant string
pub fn program_id() -> anchor_lang::prelude::Pubkey {
    TWOB_PROGRAM_ID.parse().expect("Invalid program ID")
}

pub const ARRAY_LENGTH: u64 = 20;
