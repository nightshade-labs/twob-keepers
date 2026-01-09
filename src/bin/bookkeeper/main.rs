use anchor_client::{
    Client, Cluster,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::read_keypair_file, signer::Signer,
    },
};
use anchor_lang::prelude::*;
use std::sync::Arc;
use twob_keepers::AccountResolver;

use tokio::time::{Duration, sleep};

declare_program!(twob_anchor);
use twob_anchor::{accounts::Bookkeeping, client::accounts, client::args};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let payer = read_keypair_file("/Users/thgehr/.config/solana/id.json")
        .expect("Keypair file is required");
    let url = Cluster::Custom(
        "http://127.0.0.1:8899".to_string(),
        "ws://127.0.0.1:8900".to_string(),
    );

    let market_id = 1u64;
    let estimated_slot_duration_ms = 401; // +1 to sleep a bit longer
    let slots_between_updates = 500;

    let payer = Arc::new(payer);
    let client = Client::new_with_options(url, payer.clone(), CommitmentConfig::confirmed());

    let program = client.program(twob_anchor::ID)?;
    let resolver = AccountResolver::new(twob_anchor::ID);

    let market_pda = resolver.market_pda(market_id);
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());

    loop {
        let payer = payer.clone();
        let bookkeeping_account = program
            .account::<Bookkeeping>(bookkeeping_pda.address())
            .await?;
        let last_update_slot = bookkeeping_account.last_update_slot;
        let current_slot = program.rpc().get_slot().await?;

        if current_slot >= last_update_slot + slots_between_updates {
            println!("Updating book");
            let reference_slot = last_update_slot + slots_between_updates;
            let reference_index = reference_slot / 1000; // 1000 = Arraylength (10) * market end slot interval (100)

            let reference_exits_pda = resolver.exits_pda(&market_pda.address(), reference_index);
            let previous_exits_pda = resolver.exits_pda(&market_pda.address(), reference_index - 1);
            let reference_prices_pda = resolver.prices_pda(&market_pda.address(), reference_index);
            let previous_prices_pda =
                resolver.prices_pda(&market_pda.address(), reference_index - 1);

            let bookkeeping_ix = program
                .request()
                .accounts(accounts::UpdateBooks {
                    signer: payer.pubkey(),
                    market: market_pda.address(),
                    bookkeeping: bookkeeping_pda.address(),
                    reference_exits: reference_exits_pda.address(),
                    previous_exits: previous_exits_pda.address(),
                    reference_prices: reference_prices_pda.address(),
                    previous_prices: previous_prices_pda.address(),
                    system_program: system_program::ID,
                })
                .args(args::UpdateBooks {
                    reference_index: reference_index,
                    slot: reference_slot,
                })
                .instructions()?
                .remove(0);

            program
                .request()
                .instruction(bookkeeping_ix)
                .signer(payer)
                .send()
                .await?;
        } else {
            let duration_ms = (last_update_slot + slots_between_updates - current_slot)
                * estimated_slot_duration_ms;

            println!(
                "Sleeping for {} seconds",
                Duration::from_millis(duration_ms).as_secs_f64()
            );
            sleep(Duration::from_millis(duration_ms)).await;
        }
    }
}
