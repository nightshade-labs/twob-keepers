use anchor_client::solana_sdk::{account::Account, commitment_config::CommitmentConfig};
use anchor_lang::{Discriminator, prelude::Pubkey};
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::{
    config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    filter::{Memcmp, RpcFilterType},
    request::RpcRequest,
    response::{Response, RpcKeyedAccount},
};
use tokio::{
    sync::mpsc,
    time::{Duration, Instant, sleep, timeout, timeout_at},
};
use twob_anchor::accounts::TradePosition;
use twob_keepers::{accounts::decode_account_data, twob_anchor};

const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub enum DiscoveryEvent {
    Connected,
    Account {
        slot: u64,
        address: Pubkey,
        account: Account,
    },
}

fn account_config(market: &Pubkey, min_slot: Option<u64>) -> RpcProgramAccountsConfig {
    RpcProgramAccountsConfig {
        filters: Some(vec![
            RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                0,
                TradePosition::DISCRIMINATOR.to_vec(),
            )),
            // Discriminator (8 bytes), then the position authority (32 bytes).
            RpcFilterType::Memcmp(Memcmp::new_raw_bytes(40, market.to_bytes().to_vec())),
        ]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            min_context_slot: min_slot,
            ..Default::default()
        },
        with_context: Some(true),
        ..Default::default()
    }
}

/// Retain the snapshot's bank slot so an older snapshot cannot undo newer notifications.
pub async fn snapshot(
    rpc: &RpcClient,
    market: &Pubkey,
    min_slot: Option<u64>,
) -> Result<(u64, Vec<(Pubkey, Account)>)> {
    // The SDK's get_program_accounts_with_config helper discards this context.
    let response: Response<Vec<RpcKeyedAccount>> = rpc
        .send(
            RpcRequest::GetProgramAccounts,
            serde_json::json!([
                twob_anchor::ID.to_string(),
                account_config(market, min_slot)
            ]),
        )
        .await
        .context("failed to scan trade positions")?;
    let accounts = response
        .value
        .into_iter()
        .map(|keyed| {
            let address = keyed.pubkey.parse().context("invalid position address")?;
            let account = keyed
                .account
                .decode::<Account>()
                .context("invalid position account encoding")?;
            Ok((address, account))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((response.context.slot, accounts))
}

/// Closed accounts and other account types are not positions; malformed positions are errors.
pub fn decode_position(account: &Account, market: &Pubkey) -> Result<Option<TradePosition>> {
    if account.lamports == 0
        || account.owner != twob_anchor::ID
        || !account.data.starts_with(TradePosition::DISCRIMINATOR)
    {
        return Ok(None);
    }
    let position: TradePosition = decode_account_data(&account.data)?;
    Ok((position.market == *market).then_some(position))
}

/// Subscribe before requesting reconciliation. A quiet market keeps the socket open:
/// the SDK already sends WebSocket pings, and snapshots cover missed close tombstones.
pub async fn subscribe(ws_url: String, market: Pubkey, tx: mpsc::Sender<DiscoveryEvent>) {
    let mut reconnect_delay = Duration::from_secs(1);
    loop {
        if tx.is_closed() {
            return;
        }
        let started = Instant::now();
        let setup_deadline = started + SETUP_TIMEOUT;
        match timeout_at(setup_deadline, PubsubClient::new(&ws_url)).await {
            Ok(Ok(client)) => {
                let result = watch(&client, market, &tx, setup_deadline).await;
                if !tx.is_closed() {
                    if let Err(error) = result {
                        eprintln!("Trade position subscription interrupted: {error:#}");
                    }
                }
                // Drop the borrowed subscription before shutting down its client.
                let _ = timeout(Duration::from_secs(5), client.shutdown()).await;
            }
            Ok(Err(error)) => eprintln!("Trade position subscription connection failed: {error}"),
            Err(_) => eprintln!("Trade position subscription connection timed out"),
        }
        // Do not reset the backoff for a repeatedly accepted, immediately broken socket.
        if started.elapsed() >= Duration::from_secs(60) {
            reconnect_delay = Duration::from_secs(1);
        }
        tokio::select! {
            _ = tx.closed() => return,
            _ = sleep(reconnect_delay) => {},
        }
        reconnect_delay = reconnect_delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
    }
}

async fn watch(
    client: &PubsubClient,
    market: Pubkey,
    tx: &mpsc::Sender<DiscoveryEvent>,
    setup_deadline: Instant,
) -> Result<()> {
    let (mut stream, _unsubscribe) = timeout_at(
        setup_deadline,
        client.program_subscribe(&twob_anchor::ID, Some(account_config(&market, None))),
    )
    .await
    .context("position subscription setup timed out")?
    .context("position subscription rejected")?;
    tx.send(DiscoveryEvent::Connected)
        .await
        .context("position discovery receiver stopped")?;
    loop {
        let notification = tokio::select! {
            _ = tx.closed() => return Ok(()),
            notification = stream.next() => notification,
        };
        let Some(notification) = notification else {
            bail!("position subscription ended");
        };
        let address = notification
            .value
            .pubkey
            .parse()
            .context("invalid subscribed position address")?;
        let account = notification
            .value
            .account
            .decode::<Account>()
            .context("invalid subscribed position account encoding")?;
        // Backpressure avoids dropping a position mutation while settlement is busy.
        tx.send(DiscoveryEvent::Account {
            slot: notification.context.slot,
            address,
            account,
        })
        .await
        .context("position discovery receiver stopped")?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use std::collections::HashMap;

    fn position_account(market: Pubkey) -> Account {
        let position = TradePosition {
            authority: Pubkey::new_unique(),
            market,
            remaining_slots: 11,
            ..Default::default()
        };
        let mut data = TradePosition::DISCRIMINATOR.to_vec();
        data.extend_from_slice(bytemuck::bytes_of(&position));
        Account {
            lamports: 1,
            owner: twob_anchor::ID,
            data,
            ..Default::default()
        }
    }

    #[test]
    fn filters_match_the_position_wire_layout_and_request_confirmed_context() {
        let market = Pubkey::new_unique();
        let account = position_account(market);
        assert_eq!(&account.data[40..72], market.as_ref());
        let config = account_config(&market, Some(123));
        let filters = config.filters.as_ref().unwrap();
        assert_eq!(filters.len(), 2);
        for filter in filters {
            let RpcFilterType::Memcmp(filter) = filter else {
                panic!("unexpected filter");
            };
            assert!(filter.bytes_match(&account.data));
            assert!(!filter.bytes_match(&[0; 312]));
        }
        assert_eq!(config.account_config.min_context_slot, Some(123));
        assert_eq!(
            config.account_config.commitment,
            Some(CommitmentConfig::confirmed())
        );
        assert_eq!(
            config.account_config.encoding,
            Some(UiAccountEncoding::Base64)
        );
        assert_eq!(config.with_context, Some(true));
    }

    #[test]
    fn decoding_distinguishes_live_positions_closed_foreign_and_malformed_accounts() {
        let market = Pubkey::new_unique();
        let account = position_account(market);
        assert_eq!(
            decode_position(&account, &market)
                .unwrap()
                .unwrap()
                .remaining_slots,
            11
        );
        assert!(
            decode_position(&account, &Pubkey::new_unique())
                .unwrap()
                .is_none()
        );
        let mut changed = account.clone();
        changed.lamports = 0;
        assert!(decode_position(&changed, &market).unwrap().is_none());
        changed = account.clone();
        changed.owner = Pubkey::new_unique();
        assert!(decode_position(&changed, &market).unwrap().is_none());
        changed = account.clone();
        changed.data[0] ^= 1;
        assert!(decode_position(&changed, &market).unwrap().is_none());
        changed = account;
        changed.data.truncate(8);
        assert!(decode_position(&changed, &market).is_err());
    }

    #[tokio::test]
    async fn snapshot_preserves_context_and_decodes_accounts() {
        let market = Pubkey::new_unique();
        let address = Pubkey::new_unique();
        let account = position_account(market);
        let response = serde_json::json!({
            "context": { "slot": 12345 },
            "value": [{
                "pubkey": address.to_string(),
                "account": {
                    "lamports": account.lamports,
                    "data": [BASE64_STANDARD.encode(&account.data), "base64"],
                    "owner": account.owner.to_string(),
                    "executable": false,
                    "rentEpoch": 0,
                    "space": account.data.len()
                }
            }]
        });
        let rpc = RpcClient::new_mock_with_mocks(
            "succeeds".into(),
            HashMap::from([(RpcRequest::GetProgramAccounts, response)]),
        );
        let (slot, accounts) = snapshot(&rpc, &market, Some(12300)).await.unwrap();
        assert_eq!(slot, 12345);
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].0, address);
        assert_eq!(accounts[0].1, account);
    }
}
