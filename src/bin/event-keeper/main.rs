use anchor_client::{
    Client, Cluster,
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair},
};
use anchor_lang::prelude::*;
use std::{env, sync::Arc};
use tokio::sync::mpsc;
use twob_keepers::Database;

declare_program!(twob_anchor);
use twob_anchor::events::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    // Load database credentials
    // let supabase_url = std::env::var("SUPABASE_URL").expect("SUPABASE_URL must be set");
    let database_password =
        std::env::var("DATABASE_PASSWORD").expect("DATABASE_PASSWORD must be set");

    // Construct PostgreSQL connection string
    // Using direct connection (port 5432) instead of pooler (port 6543) to support prepared statements
    let database_url = format!(
        "postgresql://postgres:{}@db.xzlbpjbsuyrjoijmmtom.supabase.co:5432/postgres",
        database_password
    );

    // Connect to database
    let db = Arc::new(Database::connect(&database_url).await?);
    println!("Connected to database");

    // let payer = read_keypair_file("/Users/thgehr/.config/solana/id.json")
    //     .expect("Keypair file is required");
    let payer = Keypair::new();
    let rpc_url = env::var("CLUSTER_RPC_URL").expect("CLUSTER_RPC_URL must be set");
    let ws_url = env::var("CLUSTER_WS_URL").expect("CLUSTER_WS_URL must be set");
    let url = Cluster::Custom(rpc_url, ws_url);

    let payer = Arc::new(payer);
    let client = Client::new_with_options(url, payer.clone(), CommitmentConfig::confirmed());

    let program = client.program(twob_anchor::ID)?;

    let (market_update_event_sender, mut market_update_event_receiver) = mpsc::unbounded_channel();
    let market_update_event_unsubscriber = program
        .on(move |event_ctx, event: MarketUpdateEvent| {
            if market_update_event_sender
                .send((event_ctx.signature, event_ctx.slot, event))
                .is_err()
            {
                println!("Error while transferring the event.")
            }
        })
        .await?;

    let (close_position_event_sender, mut close_position_event_receiver) =
        mpsc::unbounded_channel();
    let close_position_event_unsubscriber = program
        .on(move |event_ctx, event: ClosePositionEvent| {
            if close_position_event_sender
                .send((event_ctx.signature, event_ctx.slot, event))
                .is_err()
            {
                println!("Error while transferring the event.")
            }
        })
        .await?;

    let db_clone = db.clone();
    let market_update_task = tokio::spawn(async move {
        while let Some((sig, slot, event)) = market_update_event_receiver.recv().await {
            println!(
                "MarketUpdateEvent - Signature: {}, Slot: {}, Market: {}",
                sig, slot, event.market_id
            );

            if let Err(e) = db_clone
                .insert_market_update_event(
                    &sig.to_string(),
                    slot,
                    event.market_id,
                    event.base_flow,
                    event.quote_flow,
                )
                .await
            {
                eprintln!("Failed to insert market update event: {}", e);
            }
        }
    });

    let db_clone = db.clone();
    let close_position_task = tokio::spawn(async move {
        while let Some((sig, slot, event)) = close_position_event_receiver.recv().await {
            println!(
                "ClosePositionEvent - Signature: {}, Slot: {}, Market: {}",
                sig, slot, event.market_id
            );

            if let Err(e) = db_clone
                .insert_close_position_event(
                    &sig.to_string(),
                    slot,
                    &event.position_authority.to_string(),
                    event.market_id,
                    event.deposit_amount,
                    event.swapped_amount,
                    event.remaining_amount,
                    event.fee_amount,
                    event.is_buy,
                )
                .await
            {
                eprintln!("Failed to insert close position event: {}", e);
            }
        }
    });

    tokio::try_join!(market_update_task, close_position_task)?;

    market_update_event_unsubscriber.unsubscribe().await;
    close_position_event_unsubscriber.unsubscribe().await;

    Ok(())
}
