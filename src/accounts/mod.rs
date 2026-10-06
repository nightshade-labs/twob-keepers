//! Account types and PDA resolution for the Twob Anchor program.
//!
//! This module provides:
//! - PDA (Program Derived Address) resolution utilities
//! - Helper functions for account derivation

pub mod resolvers;

pub use resolvers::*;

use anchor_lang::{Discriminator, ZeroCopy, prelude::Pubkey};
use anyhow::{Context, Result, ensure};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

/// Anchor 0.32's generated zero-copy decoder assumes aligned RPC buffers.
/// Copy the validated payload instead: the 8-byte discriminator can leave u128
/// fields unaligned, and malformed/truncated accounts must return an error.
pub fn decode_account_data<T: ZeroCopy + Discriminator>(data: &[u8]) -> Result<T> {
    ensure!(
        data.starts_with(T::DISCRIMINATOR),
        "invalid account discriminator"
    );
    let start = T::DISCRIMINATOR.len();
    let payload = data
        .get(start..start + std::mem::size_of::<T>())
        .context("truncated account payload")?;
    Ok(bytemuck::pod_read_unaligned(payload))
}

pub async fn fetch_market(
    rpc: &RpcClient,
    address: &Pubkey,
) -> Result<crate::twob_anchor::accounts::Market> {
    let account = rpc
        .get_account(address)
        .await
        .context("failed to fetch market")?;
    ensure!(
        account.owner == crate::program_id(),
        "market belongs to another program"
    );
    let market: crate::twob_anchor::accounts::Market = decode_account_data(&account.data)?;
    let expected = AccountResolver::new(crate::program_id())
        .market_pda(&market.base_mint, &market.quote_mint, market.id)
        .address();
    ensure!(
        *address == expected,
        "market address does not match its mint pair and id"
    );
    Ok(market)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::twob_anchor::accounts::{Market, MarketInterval, TradePosition};

    #[test]
    fn v1_market_wire_layout_decodes_unaligned_high_precision_fields() {
        // Offsets from twob-anchor state/layout.rs, including the discriminator.
        let mut bytes = vec![0; 488];
        bytes[..8].copy_from_slice(Market::DISCRIMINATOR);
        bytes[72..88].copy_from_slice(&u128::MAX.to_le_bytes());
        bytes[88..104].copy_from_slice(&(1u128 << 100).to_le_bytes());
        bytes[152..156].copy_from_slice(&u32::MAX.to_le_bytes());
        bytes[456..464].copy_from_slice(&452_075_425u64.to_le_bytes());
        let mut unaligned = vec![0xa5; 3];
        unaligned.extend_from_slice(&bytes);
        let market = decode_account_data::<Market>(&unaligned[3..]).unwrap();
        assert_eq!(market.base_flow, u128::MAX);
        assert_eq!(market.quote_flow, 1u128 << 100);
        assert_eq!(market.id, u32::MAX);
        assert_eq!(market.bookkeeping.last_update_slot, 452_075_425);
        for len in 0..bytes.len() {
            assert!(decode_account_data::<Market>(&bytes[..len]).is_err());
        }
        bytes[0] ^= 1;
        assert!(decode_account_data::<Market>(&bytes).is_err());
    }

    #[test]
    fn v1_layouts_and_program_identity_match_the_integration_contract() {
        assert_eq!(std::mem::size_of::<Market>(), 480);
        assert_eq!(std::mem::size_of::<TradePosition>(), 304);
        assert_eq!(std::mem::size_of::<MarketInterval>(), 1168);
        assert_eq!(crate::program_id().to_string(), crate::TWOB_PROGRAM_ID);
        assert_eq!(crate::ARRAY_LENGTH * crate::END_SLOT_INTERVAL, 176);
    }
}
