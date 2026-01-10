use anchor_client::{
    Client, Cluster,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::read_keypair_file, signer::Signer,
    },
};
use anchor_lang::prelude::*;
use anchor_spl::{associated_token::spl_associated_token_account, token::spl_token};

use std::{sync::Arc, u64};
use twob_keepers::AccountResolver;

use tokio::time::{Duration, sleep};

declare_program!(twob_anchor);
use twob_anchor::{client::accounts, client::args};

use crate::twob_anchor::accounts::{Market, TradePosition};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let payer = read_keypair_file("/Users/thgehr/.config/solana/id.json")
        .expect("Keypair file is required");
    let url = Cluster::Custom(
        "http://127.0.0.1:8899".to_string(),
        "ws://127.0.0.1:8900".to_string(),
    );

    let market_id = 1u64;

    let payer = Arc::new(payer);
    let client = Client::new_with_options(url, payer.clone(), CommitmentConfig::confirmed());

    let program = client.program(twob_anchor::ID)?;
    let resolver = AccountResolver::new(twob_anchor::ID);

    let market_pda = resolver.market_pda(market_id);
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());

    let market_account = program.account::<Market>(market_pda.address()).await?;
    let base_mint = market_account.base_mint;
    let quote_mint = market_account.quote_mint;
    let base_vault_address =
        resolver.market_vault(&market_pda.address(), &market_account.base_mint);
    let quote_vault_address =
        resolver.market_vault(&market_pda.address(), &market_account.quote_mint);

    loop {
        let mut next_end_slot = u64::MAX;
        let current_slot = program.rpc().get_slot().await?;
        let reference_index = current_slot / 1000;

        // TODO: Need to filter position accounts for correct market, currently there will be only one market, maybe add market id to position account
        let position_accounts = program.accounts::<TradePosition>(vec![]).await?;

        for (position_address, position_account) in position_accounts.iter() {
            if current_slot > position_account.end_slot {
                let payer = payer.clone();

                let end_index = position_account.end_slot / 1000;

                let future_exits_pda = resolver.exits_pda(&market_pda.address(), end_index);
                let current_exits_pda = resolver.exits_pda(&market_pda.address(), reference_index);
                let previous_exits_pda =
                    resolver.exits_pda(&market_pda.address(), reference_index - 1);

                let future_prices_pda = resolver.prices_pda(&market_pda.address(), end_index);
                let current_prices_pda =
                    resolver.prices_pda(&market_pda.address(), reference_index);
                let previous_prices_pda =
                    resolver.prices_pda(&market_pda.address(), reference_index - 1);

                // // // We know that base is wrapped sol, that's why we generate keypair
                // let authority_base_token_account = resolver.associated_token_account(
                //     &position_account.authority,
                //     &market_account.base_mint,
                // );
                let (tmp_pubkey, _) =
                    Pubkey::find_program_address(&[position_address.as_ref()], &program.id());
                let authority_quote_token_account = resolver.associated_token_account(
                    &position_account.authority,
                    &market_account.quote_mint,
                );

                let public_close_position_ix = program
                    .request()
                    .accounts(accounts::PublicClosePosition {
                        signer: payer.pubkey(),
                        position_authority: position_account.authority,
                        base_mint: base_mint,
                        quote_mint: quote_mint,
                        authority_base_token_account: tmp_pubkey,
                        authority_quote_token_account: authority_quote_token_account,
                        market: market_pda.address(),
                        trade_position: *position_address,
                        base_vault: base_vault_address,
                        quote_vault: quote_vault_address,
                        bookkeeping: bookkeeping_pda.address(),
                        future_exits: future_exits_pda.address(),
                        future_prices: future_prices_pda.address(),
                        current_exits: current_exits_pda.address(),
                        previous_exits: previous_exits_pda.address(),
                        current_prices: current_prices_pda.address(),
                        previous_prices: previous_prices_pda.address(),
                        base_token_program: spl_token::ID,
                        quote_token_program: spl_token::ID,
                        associated_token_program: spl_associated_token_account::ID,
                        system_program: system_program::ID,
                    })
                    .args(args::PublicClosePosition {
                        reference_index: reference_index,
                    })
                    .instructions()?
                    .remove(0);

                program
                    .request()
                    .instruction(public_close_position_ix)
                    .signer(payer)
                    .send()
                    .await?;
            } else {
                if position_account.end_slot < next_end_slot {
                    next_end_slot = position_account.end_slot;
                }
            }
        }

        println!("No open trade positions right now :(");
        sleep(Duration::from_millis(400 * 1000)).await;
    }
}
