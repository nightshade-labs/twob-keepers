//! TwoB Keepers Library
//!
//! A library for building and sending transactions to interact with the TwoB Anchor program.
//! This library provides utilities for the bookkeeper, liquidity-keeper, and trade-keeper binaries.

pub mod accounts;
pub mod database;

// Re-export commonly used types
pub use accounts::{AccountResolver, PdaResult};
pub use database::Database;

/// The TwoB Anchor program ID
pub const TWOB_PROGRAM_ID: &str = "twobmF9NrRYUA6AN1yTdnWYfEpCr9UXWpESTRPG1KJj";

/// Parse the program ID from the constant string
pub fn program_id() -> anchor_lang::prelude::Pubkey {
    TWOB_PROGRAM_ID.parse().expect("Invalid program ID")
}

pub const ARRAY_LENGTH: u64 = 20;
