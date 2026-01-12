use anchor_client::{
    Client, Cluster,
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair},
};
use anchor_lang::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

declare_program!(twob_anchor);
use twob_anchor::events::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let payer = read_keypair_file("/Users/thgehr/.config/solana/id.json")
    //     .expect("Keypair file is required");
    let payer = Keypair::new();
    let url = Cluster::Custom(
        "http://127.0.0.1:8899".to_string(),
        "ws://127.0.0.1:8900".to_string(),
    );

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

    let market_update_task = tokio::spawn(async move {
        while let Some((sig, slot, event)) = market_update_event_receiver.recv().await {
            println!("Signature {}", sig);
            println!("Slot: {}", slot);
            println!("Event {:?}", event);
        }
    });

    let close_position_task = tokio::spawn(async move {
        while let Some((sig, slot, event)) = close_position_event_receiver.recv().await {
            println!("Signature {}", sig);
            println!("Slot: {}", slot);
            println!("Event {:?}", event);
        }
    });

    tokio::try_join!(market_update_task, close_position_task)?;

    market_update_event_unsubscriber.unsubscribe().await;
    close_position_event_unsubscriber.unsubscribe().await;

    Ok(())
}
