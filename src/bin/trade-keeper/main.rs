use anchor_client::{
    Client, Cluster,
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, signer::Signer},
};
use anchor_lang::prelude::*;
use anchor_spl::{associated_token, token::spl_token, token_2022::spl_token_2022};
use anyhow::{Context, Result, anyhow};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::{env, sync::Arc};
use tokio::time::{Duration, sleep};
use twob_keepers::{ARRAY_LENGTH, AccountResolver};

declare_program!(twob_anchor);
use twob_anchor::{client::accounts, client::args};

use crate::twob_anchor::accounts::{Market, TradePosition};

const ESTIMATED_SLOT_DURATION_MS: u64 = 350;
const REFERENCE_INDEX_LOOKAHEAD_SLOTS: u64 = 20;
const MAX_IDLE_SLEEP: Duration = Duration::from_secs(60);
const RETRY_SLEEP: Duration = Duration::from_secs(10);
const POST_CLOSE_SLEEP: Duration = Duration::from_secs(1);

// Keep synchronized with twob-anchor's MAXIMUM_DURATION_SLOTS. A paused position only becomes
// publicly closable after this abandonment window has elapsed since its original start slot.
const MAXIMUM_DURATION_SLOTS: u64 = 160_000_000;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let payer = Arc::new(read_payer_from_env()?);
    let rpc_url = env::var("CLUSTER_RPC_URL").context("CLUSTER_RPC_URL must be set")?;
    let ws_url = env::var("CLUSTER_WS_URL").context("CLUSTER_WS_URL must be set")?;
    let market_id = env::var("MARKET_ID")
        .context("MARKET_ID must be set")?
        .parse::<u32>()
        .context("MARKET_ID must be a valid u32")?;
    let url = Cluster::Custom(rpc_url, ws_url);

    let client = Client::new_with_options(url, payer.clone(), CommitmentConfig::confirmed());
    let program = client.program(twob_anchor::ID)?;
    let rpc = program.rpc();
    let resolver = AccountResolver::new(twob_anchor::ID);

    let market_address = resolver.market_pda(market_id).address();
    let bookkeeping_address = resolver.bookkeeping_pda(&market_address).address();
    let market_account = program
        .account::<Market>(market_address)
        .await
        .with_context(|| format!("failed to fetch market {market_id} at {market_address}"))?;
    let end_slot_interval = u64::from(market_account.end_slot_interval);
    if end_slot_interval == 0 {
        return Err(anyhow!("market end_slot_interval must be greater than 0"));
    }

    let base_token_program = mint_token_program(&rpc, &market_account.base_mint).await?;
    let quote_token_program = mint_token_program(&rpc, &market_account.quote_mint).await?;
    let base_vault = resolver.market_vault_with_program_id(
        &market_address,
        &market_account.base_mint,
        &base_token_program,
    );
    let quote_vault = resolver.market_vault_with_program_id(
        &market_address,
        &market_account.quote_mint,
        &quote_token_program,
    );

    println!(
        "Trade keeper started for market_id={} market={} base_token_program={} quote_token_program={}",
        market_id, market_address, base_token_program, quote_token_program
    );

    loop {
        let current_slot = match rpc.get_slot().await {
            Ok(slot) => slot,
            Err(error) => {
                eprintln!("Failed to fetch current slot: {error}. Retrying in 10 seconds");
                sleep(RETRY_SLEEP).await;
                continue;
            }
        };
        let reference_index = reference_index_for(current_slot, end_slot_interval);
        let previous_index = reference_index - 1;

        let position_accounts = match program.accounts::<TradePosition>(vec![]).await {
            Ok(accounts) => accounts,
            Err(error) => {
                eprintln!("Failed to fetch trade positions: {error}. Retrying in 10 seconds");
                sleep(RETRY_SLEEP).await;
                continue;
            }
        };

        let current_exits = resolver
            .exits_pda(&market_address, reference_index)
            .address();
        let previous_exits = resolver
            .exits_pda(&market_address, previous_index)
            .address();
        let current_prices = resolver
            .prices_pda(&market_address, reference_index)
            .address();
        let previous_prices = resolver
            .prices_pda(&market_address, previous_index)
            .address();

        let mut next_eligible_slot: Option<u64> = None;
        let mut closed_any = false;
        let mut retry_ready_position = false;

        for (position_address, position) in position_accounts {
            if position.market_id != market_id {
                continue;
            }

            let eligible_slot = public_close_eligible_slot(&position);
            if current_slot < eligible_slot {
                next_eligible_slot = Some(
                    next_eligible_slot
                        .map(|next| next.min(eligible_slot))
                        .unwrap_or(eligible_slot),
                );
                continue;
            }

            let end_slot = trade_position_end_slot(&position);
            let future_index = end_slot / end_slot_interval / ARRAY_LENGTH;
            let future_exits = resolver.exits_pda(&market_address, future_index).address();
            let future_prices = resolver.prices_pda(&market_address, future_index).address();

            let receiver_base_token_account = receiver_token_account(
                &resolver,
                &position_address,
                &position.base_receiver,
                &market_account.base_mint,
                &base_token_program,
            );
            let receiver_quote_token_account = receiver_token_account(
                &resolver,
                &position_address,
                &position.quote_receiver,
                &market_account.quote_mint,
                &quote_token_program,
            );

            let required_accounts = [
                (
                    "receiver base token account",
                    receiver_base_token_account,
                    market_account.base_mint != spl_token::native_mint::ID,
                    Some(base_token_program),
                ),
                (
                    "receiver quote token account",
                    receiver_quote_token_account,
                    market_account.quote_mint != spl_token::native_mint::ID,
                    Some(quote_token_program),
                ),
                (
                    "future exits account",
                    future_exits,
                    true,
                    Some(twob_anchor::ID),
                ),
                (
                    "future prices account",
                    future_prices,
                    true,
                    Some(twob_anchor::ID),
                ),
            ];

            let mut missing_required_account = false;
            for (label, address, must_exist, expected_owner) in required_accounts {
                if !must_exist {
                    continue;
                }

                match account_exists_with_owner(&rpc, &address, expected_owner.as_ref()).await {
                    Ok(true) => {}
                    Ok(false) => {
                        eprintln!(
                            "Skipping publicly closable position {} because its {} {} is missing or has the wrong owner",
                            position_address, label, address
                        );
                        missing_required_account = true;
                    }
                    Err(error) => {
                        eprintln!(
                            "Failed to validate {} {} for position {}: {error}",
                            label, address, position_address
                        );
                        missing_required_account = true;
                    }
                }
            }
            if missing_required_account {
                retry_ready_position = true;
                continue;
            }

            let instruction = match program
                .request()
                .accounts(accounts::PublicCloseTradePosition {
                    signer: payer.pubkey(),
                    payer: position.payer,
                    base_receiver: position.base_receiver,
                    quote_receiver: position.quote_receiver,
                    base_mint: market_account.base_mint,
                    quote_mint: market_account.quote_mint,
                    receiver_base_token_account,
                    receiver_quote_token_account,
                    market: market_address,
                    trade_position: position_address,
                    base_vault,
                    quote_vault,
                    bookkeeping: bookkeeping_address,
                    future_exits,
                    future_prices,
                    current_exits,
                    previous_exits,
                    current_prices,
                    previous_prices,
                    base_token_program,
                    quote_token_program,
                    associated_token_program: associated_token::ID,
                    system_program: system_program::ID,
                })
                .args(args::PublicCloseTradePosition { reference_index })
                .instructions()
                .context("failed to build public_close_trade_position instruction")
                .and_then(|instructions| {
                    instructions
                        .into_iter()
                        .next()
                        .context("public_close_trade_position builder returned no instructions")
                }) {
                Ok(instruction) => instruction,
                Err(error) => {
                    eprintln!(
                        "Failed to build close instruction for position {}: {error:#}",
                        position_address
                    );
                    retry_ready_position = true;
                    continue;
                }
            };

            match program
                .request()
                .instruction(instruction)
                .signer(payer.clone())
                .send()
                .await
            {
                Ok(signature) => {
                    closed_any = true;
                    println!(
                        "Closed finished trade position {} at effective_end_slot={}. Signature: {}",
                        position_address, end_slot, signature
                    );
                }
                Err(error) => {
                    retry_ready_position = true;
                    eprintln!(
                        "Failed to close publicly closable position {}: {error}",
                        position_address
                    );
                }
            }
        }

        let sleep_duration = if closed_any {
            POST_CLOSE_SLEEP
        } else if retry_ready_position {
            RETRY_SLEEP
        } else {
            sleep_until_next_eligible(current_slot, next_eligible_slot)
        };
        println!(
            "No more publicly closable trade positions; sleeping for {:.1} seconds",
            sleep_duration.as_secs_f64()
        );
        sleep(sleep_duration).await;
    }
}

fn read_payer_from_env() -> Result<Keypair> {
    let payer_bytes: Vec<u8> =
        serde_json::from_str(&env::var("PAYER_KEYPAIR").context("PAYER_KEYPAIR must be set")?)
            .context("PAYER_KEYPAIR must be a valid JSON array of bytes")?;

    Keypair::try_from(payer_bytes.as_slice()).context("PAYER_KEYPAIR must be a valid keypair")
}

async fn mint_token_program(rpc: &RpcClient, mint: &Pubkey) -> Result<Pubkey> {
    let mint_account = rpc
        .get_account(mint)
        .await
        .with_context(|| format!("failed to fetch mint account {mint}"))?;
    validate_token_program(mint, mint_account.owner)
}

fn validate_token_program(mint: &Pubkey, owner: Pubkey) -> Result<Pubkey> {
    if owner == spl_token::ID || owner == spl_token_2022::ID {
        Ok(owner)
    } else {
        Err(anyhow!(
            "mint {mint} is owned by unsupported token program {owner}"
        ))
    }
}

async fn account_exists_with_owner(
    rpc: &RpcClient,
    address: &Pubkey,
    expected_owner: Option<&Pubkey>,
) -> Result<bool> {
    let response = rpc
        .get_account_with_commitment(address, CommitmentConfig::confirmed())
        .await
        .with_context(|| format!("failed to fetch account {address}"))?;

    Ok(response
        .value
        .is_some_and(|account| expected_owner.is_none_or(|owner| account.owner == *owner)))
}

fn receiver_token_account(
    resolver: &AccountResolver,
    position_address: &Pubkey,
    receiver: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> Pubkey {
    if *mint == spl_token::native_mint::ID {
        Pubkey::find_program_address(&[position_address.as_ref()], resolver.program_id()).0
    } else {
        resolver.associated_token_account_with_program_id(receiver, mint, token_program)
    }
}

fn trade_position_end_slot(position: &TradePosition) -> u64 {
    position
        .last_update_slot
        .saturating_add(u64::from(position.remaining_slots))
}

fn public_close_eligible_slot(position: &TradePosition) -> u64 {
    if position.paused_at_slot > 0 {
        position
            .start_slot
            .saturating_add(MAXIMUM_DURATION_SLOTS)
            .saturating_add(1)
    } else {
        trade_position_end_slot(position)
    }
}

fn reference_index_for(current_slot: u64, end_slot_interval: u64) -> u64 {
    current_slot
        .saturating_add(REFERENCE_INDEX_LOOKAHEAD_SLOTS)
        .checked_div(end_slot_interval.saturating_mul(ARRAY_LENGTH))
        .unwrap_or(0)
        .max(1)
}

fn sleep_until_next_eligible(current_slot: u64, next_eligible_slot: Option<u64>) -> Duration {
    let Some(next_eligible_slot) = next_eligible_slot else {
        return MAX_IDLE_SLEEP;
    };
    let slots = next_eligible_slot.saturating_sub(current_slot).max(1);
    Duration::from_millis(slots.saturating_mul(ESTIMATED_SLOT_DURATION_MS)).min(MAX_IDLE_SLEEP)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anchor_lang::{InstructionData, ToAccountMetas};
    use twob_anchor::types::Side;

    fn trade_position() -> TradePosition {
        TradePosition {
            authority: Pubkey::new_unique(),
            payer: Pubkey::new_unique(),
            operator: Pubkey::new_unique(),
            base_receiver: Pubkey::new_unique(),
            quote_receiver: Pubkey::new_unique(),
            amount: 1,
            inactive_refund: 0,
            start_slot: 1_000,
            last_update_slot: 1_200,
            remaining_slots: 300,
            flow: 1,
            bookkeeping_snapshot: 0,
            slots_without_trades_snapshot: 0,
            paused_at_slot: 0,
            swapped_amount_at_snapshot: 0,
            withdrawn_amount: 0,
            id: 7,
            market_id: 2,
            side: Side::Sell,
            bump: 255,
        }
    }

    #[test]
    fn derives_effective_end_and_paused_abandonment_slots() {
        let mut position = trade_position();
        assert_eq!(trade_position_end_slot(&position), 1_500);
        assert_eq!(public_close_eligible_slot(&position), 1_500);

        position.paused_at_slot = 1_300;
        assert_eq!(
            public_close_eligible_slot(&position),
            position.start_slot + MAXIMUM_DURATION_SLOTS + 1
        );
    }

    #[test]
    fn reference_index_looks_across_an_imminent_boundary() {
        let slots_per_account = 107 * ARRAY_LENGTH;
        assert_eq!(reference_index_for(slots_per_account - 21, 107), 1);
        assert_eq!(reference_index_for(slots_per_account - 20, 107), 1);
        assert_eq!(reference_index_for(slots_per_account * 2 - 20, 107), 2);
    }

    #[test]
    fn accepts_only_supported_token_program_owners() {
        let mint = Pubkey::new_unique();
        assert_eq!(
            validate_token_program(&mint, spl_token::ID).unwrap(),
            spl_token::ID
        );
        assert_eq!(
            validate_token_program(&mint, spl_token_2022::ID).unwrap(),
            spl_token_2022::ID
        );
        assert!(validate_token_program(&mint, Pubkey::new_unique()).is_err());
    }

    #[test]
    fn public_close_instruction_matches_current_idl_shape() {
        let addresses = std::array::from_fn::<_, 23, _>(|_| Pubkey::new_unique());
        let account_metas = accounts::PublicCloseTradePosition {
            signer: addresses[0],
            payer: addresses[1],
            base_receiver: addresses[2],
            quote_receiver: addresses[3],
            base_mint: addresses[4],
            quote_mint: addresses[5],
            receiver_base_token_account: addresses[6],
            receiver_quote_token_account: addresses[7],
            market: addresses[8],
            trade_position: addresses[9],
            base_vault: addresses[10],
            quote_vault: addresses[11],
            bookkeeping: addresses[12],
            future_exits: addresses[13],
            future_prices: addresses[14],
            current_exits: addresses[15],
            previous_exits: addresses[16],
            current_prices: addresses[17],
            previous_prices: addresses[18],
            base_token_program: addresses[19],
            quote_token_program: addresses[20],
            associated_token_program: addresses[21],
            system_program: addresses[22],
        }
        .to_account_metas(None);

        assert_eq!(account_metas.len(), addresses.len());
        assert_eq!(
            account_metas
                .iter()
                .map(|meta| meta.pubkey)
                .collect::<Vec<_>>(),
            addresses
        );
        assert!(account_metas[0].is_signer);
        assert!(account_metas[0].is_writable);
        assert!(!account_metas[15].is_writable);
        assert!(!account_metas[16].is_writable);

        let data = args::PublicCloseTradePosition {
            reference_index: 42,
        }
        .data();
        assert_eq!(&data[..8], &[84, 88, 98, 249, 38, 129, 179, 30]);
        assert_eq!(&data[8..], &42_u64.to_le_bytes());
    }
}
