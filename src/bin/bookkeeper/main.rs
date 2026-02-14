use anchor_client::{
    Client, Cluster,
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, signer::Signer},
};
use anchor_lang::prelude::*;
use std::env;
use std::sync::Arc;
use twob_keepers::{ARRAY_LENGTH, AccountResolver};

use tokio::time::{Duration, sleep};

declare_program!(twob_anchor);
use twob_anchor::{accounts::Bookkeeping, client::accounts, client::args};

use crate::twob_anchor::accounts::Market;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let payer_bytes: Vec<u8> =
        serde_json::from_str(&env::var("PAYER_KEYPAIR").expect("PAYER_KEYPAIR must be set"))
            .expect("PAYER_KEYPAIR must be a valid JSON array of bytes");
    let payer =
        Keypair::try_from(payer_bytes.as_slice()).expect("PAYER_KEYPAIR must be a valid keypair");

    let rpc_url = env::var("CLUSTER_RPC_URL").expect("CLUSTER_RPC_URL must be set");
    let ws_url = env::var("CLUSTER_WS_URL").expect("CLUSTER_WS_URL must be set");
    let url = Cluster::Custom(rpc_url, ws_url);

    let market_id: u64 = env::var("MARKET_ID")
        .expect("MARKET_ID must be set")
        .parse()
        .expect("MARKET_ID must be a valid u64");
    let estimated_slot_duration_ms = 401; // +1 to sleep a bit longer
    let slots_between_updates: u64 = env::var("SLOTS_BETWEEN_UPDATES")
        .expect("SLOTS_BETWEEN_UPDATES must be set")
        .parse()
        .expect("SLOTS_BETWEEN_UPDATES must be a valid u64");

    let payer = Arc::new(payer);
    let client = Client::new_with_options(url, payer.clone(), CommitmentConfig::confirmed());

    let program = client.program(twob_anchor::ID)?;
    let resolver = AccountResolver::new(twob_anchor::ID);

    let market_pda = resolver.market_pda(market_id);
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());

    let market_account = program.account::<Market>(market_pda.address()).await?;
    let end_slot_interval = market_account.end_slot_interval;

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
            let reference_index = reference_slot / end_slot_interval / ARRAY_LENGTH;

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
