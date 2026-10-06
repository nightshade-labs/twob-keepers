use anchor_client::{
    Client, Cluster,
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, signer::Signer},
};
use anchor_lang::{Discriminator, prelude::*};
use anchor_spl::{associated_token::spl_associated_token_account, token::spl_token};
use anyhow::{Context, ensure};
use solana_rpc_client_types::{
    config::RpcProgramAccountsConfig,
    filter::{Memcmp, RpcFilterType},
};
use std::{env, sync::Arc};
use tokio::time::{Duration, sleep};
use twob_anchor::{
    accounts::TradePosition,
    client::{accounts, args},
};
use twob_keepers::{
    ARRAY_LENGTH, AccountResolver, END_SLOT_INTERVAL, MAXIMUM_DURATION_SLOTS,
    accounts::{decode_account_data, fetch_market},
    market_address_from_env, twob_anchor,
};

/// Pauses freeze the position's end slot. Only abandoned pauses may be closed publicly.
fn public_close_slot(position: &TradePosition) -> Option<u64> {
    if position.paused_at_slot > 0 {
        position
            .start_slot
            .checked_add(MAXIMUM_DURATION_SLOTS)?
            .checked_add(1)
    } else {
        position
            .last_update_slot
            .checked_add(u64::from(position.remaining_slots))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let payer_bytes: Vec<u8> = serde_json::from_str(&env::var("PAYER_KEYPAIR")?)
        .context("PAYER_KEYPAIR must be a JSON array of keypair bytes")?;
    let payer = Arc::new(
        Keypair::try_from(payer_bytes.as_slice())
            .context("PAYER_KEYPAIR must be a valid keypair")?,
    );
    let market_address = market_address_from_env("MARKET_ADDRESS")?;
    let cluster = Cluster::Custom(env::var("CLUSTER_RPC_URL")?, env::var("CLUSTER_WS_URL")?);
    let client = Client::new_with_options(cluster, payer.clone(), CommitmentConfig::confirmed());
    let program = client.program(twob_anchor::ID)?;
    let rpc = program.rpc();
    let resolver = AccountResolver::new(twob_anchor::ID);
    let market = fetch_market(&rpc, &market_address).await?;
    let base_token_program = rpc.get_account(&market.base_mint).await?.owner;
    let quote_token_program = rpc.get_account(&market.quote_mint).await?.owner;
    for owner in [base_token_program, quote_token_program] {
        ensure!(
            owner == spl_token::ID || owner == anchor_spl::token_2022::ID,
            "market mint is not owned by a supported token program"
        );
    }
    let base_vault = resolver.associated_token_account_with_program(
        &market_address,
        &market.base_mint,
        &base_token_program,
    );
    let quote_vault = resolver.associated_token_account_with_program(
        &market_address,
        &market.quote_mint,
        &quote_token_program,
    );

    loop {
        // Market follows the authority in the zero-copy TradePosition header.
        let positions = rpc
            .get_program_accounts_with_config(
                &program.id(),
                RpcProgramAccountsConfig {
                    filters: Some(vec![
                        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                            0,
                            TradePosition::DISCRIMINATOR.to_vec(),
                        )),
                        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                            40,
                            market_address.to_bytes().to_vec(),
                        )),
                    ]),
                    ..Default::default()
                },
            )
            .await?;
        for (position_address, account) in positions {
            let position: TradePosition = decode_account_data(&account.data)?;
            if position.market != market_address {
                continue;
            }
            let current_slot = rpc.get_slot().await?;
            let Some(close_slot) = public_close_slot(&position) else {
                continue;
            };
            if current_slot < close_slot {
                continue;
            }
            let reference_index = current_slot / END_SLOT_INTERVAL / ARRAY_LENGTH;
            let Some(previous_index) = reference_index.checked_sub(1) else {
                continue;
            };
            let end_slot = position
                .last_update_slot
                .checked_add(u64::from(position.remaining_slots))
                .context("position end slot overflow")?;
            let end_index = end_slot / END_SLOT_INTERVAL / ARRAY_LENGTH;
            let receiver_base_token_account = resolver.receiver_token_account(
                &position_address,
                &position.base_receiver,
                &market.base_mint,
                &base_token_program,
            );
            let receiver_quote_token_account = resolver.receiver_token_account(
                &position_address,
                &position.quote_receiver,
                &market.quote_mint,
                &quote_token_program,
            );

            // Public settlement requires existing non-native ATAs. Native payouts use a temporary PDA.
            let mut receivers_ready = true;
            for (mint, receiver) in [
                (market.base_mint, receiver_base_token_account),
                (market.quote_mint, receiver_quote_token_account),
            ] {
                if mint != spl_token::native_mint::ID && rpc.get_account(&receiver).await.is_err() {
                    receivers_ready = false;
                }
            }
            if !receivers_ready {
                continue;
            }

            let result = program
                .request()
                .accounts(accounts::PublicCloseTradePosition {
                    signer: payer.pubkey(),
                    program_config: resolver.program_config_pda().address(),
                    payer: position.payer,
                    base_receiver: position.base_receiver,
                    quote_receiver: position.quote_receiver,
                    base_mint: market.base_mint,
                    quote_mint: market.quote_mint,
                    receiver_base_token_account,
                    receiver_quote_token_account,
                    market: market_address,
                    trade_position: position_address,
                    base_vault,
                    quote_vault,
                    future_interval: resolver
                        .market_interval_pda(&market_address, end_index)
                        .address(),
                    current_interval: resolver
                        .market_interval_pda(&market_address, reference_index)
                        .address(),
                    previous_interval: resolver
                        .market_interval_pda(&market_address, previous_index)
                        .address(),
                    base_token_program,
                    quote_token_program,
                    associated_token_program: spl_associated_token_account::ID,
                    system_program: system_program::ID,
                })
                .args(args::PublicCloseTradePosition { reference_index })
                .send()
                .await;
            match result {
                Ok(signature) => println!("Closed trade position {position_address}: {signature}"),
                Err(error) => {
                    eprintln!("Failed to close trade position {position_address}: {error}")
                }
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_closure_uses_resumed_end_slot_and_abandonment_for_pauses() {
        let mut position = TradePosition {
            start_slot: 100,
            last_update_slot: 200,
            remaining_slots: 33,
            ..Default::default()
        };
        assert_eq!(public_close_slot(&position), Some(233));
        position.paused_at_slot = 211;
        assert_eq!(
            public_close_slot(&position),
            Some(100 + MAXIMUM_DURATION_SLOTS + 1)
        );
        position.paused_at_slot = 0;
        position.last_update_slot = u64::MAX;
        assert_eq!(public_close_slot(&position), None);
    }
}
