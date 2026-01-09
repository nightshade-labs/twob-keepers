//! Example demonstrating how to use the accounts module.
//!
//! Run with: cargo run --example accounts_usage

use twob_keepers::{AccountResolver, program_id};
use anchor_lang::prelude::Pubkey;

fn main() {
    // Create an account resolver with the TwoB program ID
    let program_id = program_id();
    let resolver = AccountResolver::new(program_id);

    println!("TwoB Program ID: {}", program_id);
    println!();

    // Example 1: Derive program config PDA
    let program_config = resolver.program_config_pda();
    println!("Program Config PDA:");
    println!("  Address: {}", program_config.address());
    println!("  Bump: {}", program_config.bump());
    println!();

    // Example 2: Derive market PDA
    let market_id = 1u64;
    let market = resolver.market_pda(market_id);
    println!("Market PDA (id={}):", market_id);
    println!("  Address: {}", market.address());
    println!("  Bump: {}", market.bump());
    println!();

    // Example 3: Derive bookkeeping account PDA
    let bookkeeping = resolver.bookkeeping_pda(&market.address());
    println!("Bookkeeping PDA:");
    println!("  Address: {}", bookkeeping.address());
    println!("  Bump: {}", bookkeeping.bump());
    println!();

    // Example 4: Derive liquidity position PDA
    let authority = Pubkey::new_unique();
    let liquidity_position = resolver.liquidity_position_pda(&market.address(), &authority);
    println!("Liquidity Position PDA:");
    println!("  Authority: {}", authority);
    println!("  Address: {}", liquidity_position.address());
    println!("  Bump: {}", liquidity_position.bump());
    println!();

    // Example 5: Derive trade position PDA
    let position_id = 1u64;
    let trade_position = resolver.trade_position_pda(&market.address(), &authority, position_id);
    println!("Trade Position PDA (id={}):", position_id);
    println!("  Authority: {}", authority);
    println!("  Address: {}", trade_position.address());
    println!("  Bump: {}", trade_position.bump());
    println!();

    // Example 6: Derive exits account PDA
    let index = 0u64;
    let exits = resolver.exits_pda(&market.address(), index);
    println!("Exits PDA (index={}):", index);
    println!("  Address: {}", exits.address());
    println!("  Bump: {}", exits.bump());
    println!();

    // Example 7: Derive prices account PDA
    let prices = resolver.prices_pda(&market.address(), index);
    println!("Prices PDA (index={}):", index);
    println!("  Address: {}", prices.address());
    println!("  Bump: {}", prices.bump());
    println!();

    // Example 8: Derive associated token account
    let mint = Pubkey::new_unique();
    let ata = resolver.associated_token_account(&authority, &mint);
    println!("Associated Token Account:");
    println!("  Authority: {}", authority);
    println!("  Mint: {}", mint);
    println!("  ATA: {}", ata);
    println!();

    // Example 9: Derive market vault (ATA owned by market)
    let base_mint = Pubkey::new_unique();
    let vault = resolver.market_vault(&market.address(), &base_mint);
    println!("Market Vault:");
    println!("  Market: {}", market.address());
    println!("  Mint: {}", base_mint);
    println!("  Vault: {}", vault);
}
