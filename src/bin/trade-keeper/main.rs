mod discovery;
mod schedule;
mod settlement;

use anchor_client::solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use anchor_lang::prelude::Pubkey;
use anyhow::{Context, Result, ensure};
use discovery::{DiscoveryEvent, decode_position};
use schedule::Schedule;
use settlement::{Disposition, Settlement, SettlementConfig};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::{env, sync::Arc};
use tokio::{
    sync::mpsc,
    time::{Duration, Instant, sleep_until},
};
use twob_keepers::market_address_from_env;

const CANDIDATES_PER_PASS: usize = 32;

struct Config {
    batch_window: Duration,
    reconcile_interval: Duration,
    missing_receiver_retry: Duration,
    retry_delay: Duration,
    slot_duration: Duration,
    settlement: SettlementConfig,
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            batch_window: duration_env("TRADE_KEEPER_BATCH_WINDOW_MS", 5_000)?,
            reconcile_interval: duration_env("TRADE_KEEPER_RECONCILE_INTERVAL_MS", 300_000)?,
            missing_receiver_retry: duration_env(
                "TRADE_KEEPER_MISSING_RECEIVER_RETRY_MS",
                300_000,
            )?,
            retry_delay: duration_env("TRADE_KEEPER_RETRY_DELAY_MS", 15_000)?,
            slot_duration: Duration::from_millis(number_env(
                "TRADE_KEEPER_ESTIMATED_SLOT_DURATION_MS",
                400,
                10_000,
            )?),
            settlement: SettlementConfig {
                max_batch_size: number_env("TRADE_KEEPER_MAX_BATCH_SIZE", 4, 16)? as usize,
                compute_unit_limit: number_env(
                    "TRADE_KEEPER_COMPUTE_UNIT_LIMIT",
                    100_000,
                    1_400_000,
                )? as u32,
            },
        })
    }
}

fn duration_env(key: &str, default: u64) -> Result<Duration> {
    Ok(Duration::from_millis(number_env(key, default, 86_400_000)?))
}

fn number_env(key: &str, default: u64, max: u64) -> Result<u64> {
    let value = env::var(key).ok().filter(|value| !value.trim().is_empty());
    parse_number(key, value.as_deref(), default, max)
}

fn parse_number(key: &str, value: Option<&str>, default: u64, max: u64) -> Result<u64> {
    let value = value
        .map(|value| value.trim().parse::<u64>())
        .transpose()
        .with_context(|| format!("{key} must be an unsigned integer"))?
        .unwrap_or(default);
    ensure!(
        value > 0 && value <= max,
        "{key} must be between 1 and {max}"
    );
    Ok(value)
}

fn handle_notification(
    notification: Option<DiscoveryEvent>,
    market: &Pubkey,
    schedule: &mut Schedule,
    next_scan: &mut Instant,
    scan_retry_not_before: Instant,
    subscription_open: &mut bool,
) {
    match notification {
        Some(DiscoveryEvent::Connected) => {
            *next_scan = Instant::now().max(scan_retry_not_before);
        }
        Some(DiscoveryEvent::Account {
            slot,
            address,
            account,
        }) => match decode_position(&account, market) {
            Ok(position) => schedule.notification(address, position, slot, Instant::now()),
            Err(error) => eprintln!("Ignoring invalid position update {address}: {error:#}"),
        },
        None => {
            *subscription_open = false;
            eprintln!("Trade subscription task stopped; continuing periodic HTTP discovery");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let config = Config::from_env()?;
    let payer_bytes: Vec<u8> = serde_json::from_str(&env::var("PAYER_KEYPAIR")?)
        .context("PAYER_KEYPAIR must be a JSON array of keypair bytes")?;
    let payer = Arc::new(
        Keypair::try_from(payer_bytes.as_slice())
            .context("PAYER_KEYPAIR must be a valid keypair")?,
    );
    let market = market_address_from_env("MARKET_ADDRESS")?;
    let rpc = Arc::new(RpcClient::new_with_timeout_and_commitment(
        env::var("CLUSTER_RPC_URL")?,
        Duration::from_secs(30),
        CommitmentConfig::confirmed(),
    ));
    let settlement = Settlement::new(rpc.clone(), payer, market, config.settlement).await?;
    let mut schedule = Schedule::new(config.slot_duration, config.batch_window);
    let (sender, mut notifications) = mpsc::channel(1024);
    let subscription = tokio::spawn(discovery::subscribe(
        env::var("CLUSTER_WS_URL")?,
        market,
        sender,
    ));
    // A successful subscription triggers an immediate snapshot; a failed/unsupported
    // subscription still gets HTTP discovery after the bounded startup wait.
    let mut next_scan = Instant::now() + Duration::from_secs(5);
    let mut scan_retry_not_before = Instant::now();
    let mut scan_backoff = config.retry_delay;
    let mut subscription_open = true;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    println!(
        "Trade keeper started market={market} batch_window_ms={} reconcile_interval_ms={} missing_receiver_retry_ms={}",
        config.batch_window.as_millis(),
        config.reconcile_interval.as_millis(),
        config.missing_receiver_retry.as_millis(),
    );

    'keeper: loop {
        let wake = schedule
            .next_check()
            .map_or(next_scan, |due| due.min(next_scan));
        tokio::select! {
            _ = &mut shutdown => break,
            notification = notifications.recv(), if subscription_open => {
                handle_notification(notification, &market, &mut schedule, &mut next_scan,
                    scan_retry_not_before, &mut subscription_open);
            }
            _ = sleep_until(wake) => {
                if Instant::now() >= next_scan {
                    match discovery::snapshot(&rpc, &market, schedule.snapshot_slot()).await {
                        Ok((slot, accounts)) => {
                            let mut positions = Vec::with_capacity(accounts.len());
                            for (address, account) in accounts {
                                match decode_position(&account, &market) {
                                    Ok(Some(position)) => positions.push((address, position)),
                                    Ok(None) => {},
                                    Err(error) => eprintln!("Ignoring invalid position {address}: {error:#}"),
                                }
                            }
                            schedule.reconcile(positions, slot, Instant::now());
                            println!("Reconciled trade positions slot={slot} tracked={}", schedule.len());
                            next_scan = Instant::now() + config.reconcile_interval;
                            scan_retry_not_before = Instant::now();
                            scan_backoff = config.retry_delay;
                        }
                        Err(error) => {
                            eprintln!("Trade discovery failed; retrying in {}s: {error:#}", scan_backoff.as_secs());
                            next_scan = Instant::now() + scan_backoff;
                            scan_retry_not_before = next_scan;
                            scan_backoff = (scan_backoff * 2).min(config.reconcile_interval.max(config.retry_delay));
                        }
                    }
                }
                let candidates = schedule.due(Instant::now(), CANDIDATES_PER_PASS);
                if candidates.is_empty() {
                    continue;
                }
                // One in-flight pass avoids competing writes to the shared market and vaults.
                // Keep consuming mutations and shutdown while RPC/confirmation is in flight.
                // Version guards prevent the older settlement read from undoing newer updates.
                let processing = settlement.process(&candidates);
                tokio::pin!(processing);
                let result = loop {
                    tokio::select! {
                        result = &mut processing => break result,
                        _ = &mut shutdown => break 'keeper,
                        notification = notifications.recv(), if subscription_open => {
                            handle_notification(notification, &market, &mut schedule, &mut next_scan,
                                scan_retry_not_before, &mut subscription_open);
                        }
                    }
                };
                match result {
                    Ok(outcomes) => {
                        for outcome in outcomes {
                            let now = Instant::now();
                            match outcome.disposition {
                                Disposition::Closed => schedule.outcome(outcome.address, None, outcome.slot, now, None),
                                Disposition::NotDue => schedule.outcome(outcome.address, outcome.position, outcome.slot, now, None),
                                Disposition::MissingReceiver => {
                                    println!("Deferring position {}: receiving token account missing or invalid", outcome.address);
                                    schedule.outcome(outcome.address, outcome.position, outcome.slot, now, Some(config.missing_receiver_retry));
                                }
                                Disposition::Retry => {
                                    if outcome.position.is_some() {
                                        schedule.outcome(outcome.address, outcome.position, outcome.slot, now, Some(config.retry_delay));
                                    } else {
                                        schedule.retry(outcome.address, now, config.retry_delay);
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("Trade settlement pass failed: {error:#}");
                        for &address in &candidates {
                            schedule.retry(address, Instant::now(), config.retry_delay);
                        }
                    }
                }
            }
        }
    }
    subscription.abort();
    println!("Trade keeper stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuning_rejects_zero_overflow_and_unbounded_values() {
        assert_eq!(parse_number("TEST", None, 4, 16).unwrap(), 4);
        assert_eq!(parse_number("TEST", Some(" 16 "), 4, 16).unwrap(), 16);
        for invalid in ["0", "17", "-1", "18446744073709551616", "abc"] {
            assert!(parse_number("TEST", Some(invalid), 4, 16).is_err());
        }
    }
}
