use anchor_client::{
    Client, Cluster,
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, signer::Signer},
};
use anchor_lang::prelude::*;
use anyhow::{Context, Result, anyhow};
use std::{env, future::Future, sync::Arc};
use twob_keepers::{ARRAY_LENGTH, AccountResolver};

use tokio::time::{Duration, sleep};

declare_program!(twob_anchor);
use twob_anchor::{accounts::Bookkeeping, client::accounts, client::args};

use crate::twob_anchor::accounts::Market;

const DEFAULT_ESTIMATED_SLOT_DURATION_MS: u64 = 401;
const DEFAULT_MIN_UPDATE_DELAY_MS: u64 = 1_000;
const DEFAULT_MAX_IDLE_SLEEP_MS: u64 = 60_000;
const DEFAULT_RETRY_INITIAL_DELAY_MS: u64 = 2_000;
const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 60_000;
const DEFAULT_SEND_RETRY_ATTEMPTS: u32 = 8;

#[derive(Clone, Copy)]
struct BookkeeperConfig {
    estimated_slot_duration_ms: u64,
    slots_between_updates: u64,
    min_update_delay: Duration,
    max_idle_sleep: Duration,
    retry_backoff: BackoffConfig,
    send_retry_attempts: u32,
}

#[derive(Clone, Copy)]
struct BackoffConfig {
    initial_delay: Duration,
    max_delay: Duration,
}

struct Backoff {
    config: BackoffConfig,
    next_delay: Duration,
}

impl Backoff {
    fn new(config: BackoffConfig) -> Self {
        Self {
            config,
            next_delay: config.initial_delay,
        }
    }

    fn reset(&mut self) {
        self.next_delay = self.config.initial_delay;
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.next_delay;
        self.next_delay = std::cmp::min(self.next_delay.saturating_mul(2), self.config.max_delay);
        delay
    }
}

#[tokio::main]
async fn main() -> Result<()> {
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
    let config = BookkeeperConfig::from_env()?;

    let payer = Arc::new(payer);
    let client = Client::new_with_options(url, payer.clone(), CommitmentConfig::confirmed());

    let program = client.program(twob_anchor::ID)?;
    let resolver = AccountResolver::new(twob_anchor::ID);

    let market_pda = resolver.market_pda(market_id);
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());

    let market_account =
        retry_until_success("fetch market account", config.retry_backoff, || async {
            program
                .account::<Market>(market_pda.address())
                .await
                .context("failed to fetch market account")
        })
        .await;
    let end_slot_interval = market_account.end_slot_interval;
    if end_slot_interval == 0 {
        return Err(anyhow!("market end_slot_interval must be greater than 0"));
    }

    println!(
        "Bookkeeper started for market_id={} slots_between_updates={} send_retry_attempts={} min_update_delay={}s retry_backoff={}s..{}s",
        market_id,
        config.slots_between_updates,
        config.send_retry_attempts,
        seconds(config.min_update_delay),
        seconds(config.retry_backoff.initial_delay),
        seconds(config.retry_backoff.max_delay),
    );

    let mut iteration_backoff = Backoff::new(config.retry_backoff);
    loop {
        let iteration = async {
            let payer = payer.clone();
            let bookkeeping_account = program
                .account::<Bookkeeping>(bookkeeping_pda.address())
                .await
                .context("failed to fetch bookkeeping account")?;
            let last_update_slot = bookkeeping_account.last_update_slot;
            let current_slot = program
                .rpc()
                .get_slot()
                .await
                .context("failed to fetch current slot")?;
            let next_update_slot = last_update_slot.saturating_add(config.slots_between_updates);

            if current_slot >= next_update_slot {
                println!(
                    "Updating books at reference_slot={} current_slot={} last_update_slot={}",
                    next_update_slot, current_slot, last_update_slot
                );
                let reference_slot = next_update_slot;
                let reference_index = reference_slot / end_slot_interval / ARRAY_LENGTH;
                let previous_index = reference_index.checked_sub(1).with_context(|| {
                    format!(
                        "reference_index is 0 for reference_slot={reference_slot}; cannot derive previous accounts yet"
                    )
                })?;

                let reference_exits_pda =
                    resolver.exits_pda(&market_pda.address(), reference_index);
                let previous_exits_pda = resolver.exits_pda(&market_pda.address(), previous_index);
                let reference_prices_pda =
                    resolver.prices_pda(&market_pda.address(), reference_index);
                let previous_prices_pda =
                    resolver.prices_pda(&market_pda.address(), previous_index);

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
                        reference_index,
                        slot: reference_slot,
                    })
                    .instructions()
                    .context("failed to build update_books instruction")?
                    .into_iter()
                    .next()
                    .context("update_books instruction builder returned no instructions")?;

                let mut send_backoff = Backoff::new(config.retry_backoff);
                for attempt in 1..=config.send_retry_attempts {
                    match program
                        .request()
                        .instruction(bookkeeping_ix.clone())
                        .signer(payer.clone())
                        .send()
                        .await
                    {
                        Ok(signature) => {
                            println!(
                                "Updated books at reference_slot={reference_slot}. Sig: {signature}"
                            );
                            return Ok(config.min_update_delay);
                        }
                        Err(error) if attempt < config.send_retry_attempts => {
                            let delay = send_backoff.next_delay();
                            eprintln!(
                                "Send failed (attempt {attempt}/{}): {error}. Retrying in {} seconds",
                                config.send_retry_attempts,
                                seconds(delay),
                            );
                            sleep(delay).await;
                        }
                        Err(error) => {
                            return Err(anyhow!(
                                "failed to send update_books transaction after {} attempts: {}",
                                config.send_retry_attempts,
                                error
                            ));
                        }
                    }
                }

                unreachable!("send retry loop must return");
            } else {
                let slots_until_update = next_update_slot.saturating_sub(current_slot);
                let planned_duration_ms =
                    slots_until_update.saturating_mul(config.estimated_slot_duration_ms);
                let planned_sleep = Duration::from_millis(planned_duration_ms);
                let sleep_duration = if planned_sleep > config.max_idle_sleep {
                    config.max_idle_sleep
                } else {
                    planned_sleep
                };

                println!(
                    "Next update at slot {} (current slot {}). Sleeping for {} seconds",
                    next_update_slot,
                    current_slot,
                    seconds(sleep_duration)
                );
                Ok(sleep_duration)
            }
        }
        .await;

        match iteration {
            Ok(delay) => {
                iteration_backoff.reset();
                if !delay.is_zero() {
                    sleep(delay).await;
                }
            }
            Err(error) => {
                let delay = iteration_backoff.next_delay();
                eprintln!(
                    "Bookkeeper iteration failed: {error:#}. Retrying in {} seconds",
                    seconds(delay)
                );
                sleep(delay).await;
            }
        }
    }
}

impl BookkeeperConfig {
    fn from_env() -> Result<Self> {
        let slots_between_updates =
            parse_required_u64_env("SLOTS_BETWEEN_UPDATES", "must be a valid u64")?;
        if slots_between_updates == 0 {
            return Err(anyhow!("SLOTS_BETWEEN_UPDATES must be greater than 0"));
        }

        let estimated_slot_duration_ms = parse_u64_env(
            "BOOKKEEPER_ESTIMATED_SLOT_DURATION_MS",
            DEFAULT_ESTIMATED_SLOT_DURATION_MS,
        )?;
        if estimated_slot_duration_ms == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_ESTIMATED_SLOT_DURATION_MS must be greater than 0"
            ));
        }

        let retry_backoff = BackoffConfig::from_env(
            "BOOKKEEPER_RETRY_INITIAL_DELAY_MS",
            DEFAULT_RETRY_INITIAL_DELAY_MS,
            "BOOKKEEPER_RETRY_MAX_DELAY_MS",
            DEFAULT_RETRY_MAX_DELAY_MS,
        )?;
        let send_retry_attempts = parse_u32_env(
            "BOOKKEEPER_SEND_RETRY_ATTEMPTS",
            DEFAULT_SEND_RETRY_ATTEMPTS,
        )?;
        if send_retry_attempts == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_SEND_RETRY_ATTEMPTS must be greater than 0"
            ));
        }
        let min_update_delay_ms = parse_u64_env(
            "BOOKKEEPER_MIN_UPDATE_DELAY_MS",
            DEFAULT_MIN_UPDATE_DELAY_MS,
        )?;
        if min_update_delay_ms == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_MIN_UPDATE_DELAY_MS must be greater than 0"
            ));
        }
        let max_idle_sleep_ms =
            parse_u64_env("BOOKKEEPER_MAX_IDLE_SLEEP_MS", DEFAULT_MAX_IDLE_SLEEP_MS)?;
        if max_idle_sleep_ms == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_MAX_IDLE_SLEEP_MS must be greater than 0"
            ));
        }

        Ok(Self {
            estimated_slot_duration_ms,
            slots_between_updates,
            min_update_delay: Duration::from_millis(min_update_delay_ms),
            max_idle_sleep: Duration::from_millis(max_idle_sleep_ms),
            retry_backoff,
            send_retry_attempts,
        })
    }
}

impl BackoffConfig {
    fn from_env(
        initial_key: &str,
        default_initial_ms: u64,
        max_key: &str,
        default_max_ms: u64,
    ) -> Result<Self> {
        let initial_delay_ms = parse_u64_env(initial_key, default_initial_ms)?;
        if initial_delay_ms == 0 {
            return Err(anyhow!("{initial_key} must be greater than 0"));
        }

        let max_delay_ms = parse_u64_env(max_key, default_max_ms)?;
        if max_delay_ms == 0 {
            return Err(anyhow!("{max_key} must be greater than 0"));
        }

        let initial_delay = Duration::from_millis(initial_delay_ms);
        let max_delay = Duration::from_millis(max_delay_ms);
        if initial_delay > max_delay {
            return Err(anyhow!(
                "{initial_key} must be less than or equal to {max_key}"
            ));
        }

        Ok(Self {
            initial_delay,
            max_delay,
        })
    }
}

async fn retry_until_success<T, F, Fut>(
    operation: &str,
    backoff_config: BackoffConfig,
    mut action: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut backoff = Backoff::new(backoff_config);
    loop {
        match action().await {
            Ok(value) => return value,
            Err(error) => {
                let delay = backoff.next_delay();
                eprintln!(
                    "{operation} failed: {error:#}. Retrying in {} seconds",
                    seconds(delay)
                );
                sleep(delay).await;
            }
        }
    }
}

fn parse_required_u64_env(key: &str, validation_message: &str) -> Result<u64> {
    env::var(key)
        .with_context(|| format!("{key} must be set"))?
        .parse::<u64>()
        .with_context(|| format!("{key} {validation_message}"))
}

fn parse_u64_env(key: &str, default_value: u64) -> Result<u64> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("{key} must be a valid u64")),
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(error) => Err(anyhow!("Failed to read {key}: {error}")),
    }
}

fn parse_u32_env(key: &str, default_value: u32) -> Result<u32> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<u32>()
            .with_context(|| format!("{key} must be a valid u32")),
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(error) => Err(anyhow!("Failed to read {key}: {error}")),
    }
}

fn seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}
