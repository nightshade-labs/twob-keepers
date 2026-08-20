use anchor_client::{
    Client, Cluster,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        hash::Hash,
        instruction::Instruction,
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::Transaction,
    },
};
use anchor_lang::prelude::*;
use anyhow::{Context, Result, anyhow};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_transaction_status_client_types::TransactionStatus;
use std::{env, future::Future, sync::Arc, time::Instant};
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
const DEFAULT_REBROADCAST_INTERVAL_MS: u64 = 1_500;
const DEFAULT_PRIORITY_FEE_PERCENTILE: u32 = 75;
const DEFAULT_PRIORITY_FEE_MIN_MICRO_LAMPORTS: u64 = 10_000;
const DEFAULT_PRIORITY_FEE_MAX_MICRO_LAMPORTS: u64 = 1_000_000;
const DEFAULT_COMPUTE_UNIT_LIMIT: u32 = 40_000;
const DEFAULT_COMPUTE_UNIT_MIN: u32 = 30_000;
const DEFAULT_COMPUTE_UNIT_MAX: u32 = 100_000;
const DEFAULT_COMPUTE_UNIT_MARGIN_BPS: u64 = 12_000;
const SIMULATION_COMPUTE_UNIT_LIMIT: u32 = 200_000;
const BPS_DENOMINATOR: u64 = 10_000;
const CONFIRMATION_COMMITMENT: CommitmentConfig = CommitmentConfig::confirmed();

#[derive(Clone, Copy)]
struct BookkeeperConfig {
    estimated_slot_duration_ms: u64,
    slots_between_updates: u64,
    min_update_delay: Duration,
    max_idle_sleep: Duration,
    retry_backoff: BackoffConfig,
    send_retry_attempts: u32,
    rebroadcast_interval: Duration,
    priority_fee_percentile: u32,
    priority_fee_min_micro_lamports: u64,
    priority_fee_max_micro_lamports: u64,
    compute_unit_limit: u32,
    compute_unit_min: u32,
    compute_unit_max: u32,
    compute_unit_margin_bps: u64,
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

#[derive(Debug, PartialEq, Eq)]
enum RebroadcastOutcome {
    Confirmed {
        broadcast_attempts: u32,
        confirmed_slot: u64,
        elapsed: Duration,
    },
    Expired {
        broadcast_attempts: u32,
        elapsed: Duration,
    },
}

trait TransactionRpc {
    async fn broadcast(
        &self,
        transaction: &Transaction,
        config: RpcSendTransactionConfig,
    ) -> Result<Signature>;

    async fn signature_status(&self, signature: &Signature) -> Result<Option<TransactionStatus>>;

    async fn current_block_height(&self) -> Result<u64>;
}

impl TransactionRpc for RpcClient {
    async fn broadcast(
        &self,
        transaction: &Transaction,
        config: RpcSendTransactionConfig,
    ) -> Result<Signature> {
        self.send_transaction_with_config(transaction, config)
            .await
            .context("sendTransaction RPC failed")
    }

    async fn signature_status(&self, signature: &Signature) -> Result<Option<TransactionStatus>> {
        let mut statuses = self
            .get_signature_statuses(&[*signature])
            .await
            .context("getSignatureStatuses RPC failed")?
            .value;
        Ok(statuses.pop().flatten())
    }

    async fn current_block_height(&self) -> Result<u64> {
        self.get_block_height_with_commitment(CONFIRMATION_COMMITMENT)
            .await
            .context("getBlockHeight RPC failed")
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
    let rpc = program.rpc();
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
        "Bookkeeper started for market_id={} slots_between_updates={} blockhash_attempts={} rebroadcast_interval={}s priority_fee_percentile={} priority_fee_range={}..{} micro_lamports compute_unit_range={}..{} min_update_delay={}s retry_backoff={}s..{}s",
        market_id,
        config.slots_between_updates,
        config.send_retry_attempts,
        seconds(config.rebroadcast_interval),
        config.priority_fee_percentile,
        config.priority_fee_min_micro_lamports,
        config.priority_fee_max_micro_lamports,
        config.compute_unit_min,
        config.compute_unit_max,
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
            let current_slot = rpc
                .get_slot()
                .await
                .context("failed to fetch current slot")?;
            log_book_staleness(current_slot, last_update_slot, end_slot_interval);
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

                let refreshed_bookkeeping = program
                    .account::<Bookkeeping>(bookkeeping_pda.address())
                    .await
                    .context("failed to recheck bookkeeping account before signing")?;
                if refreshed_bookkeeping.last_update_slot >= reference_slot {
                    println!(
                        "Skipping update_books because target was already reached before signing reference_slot={} observed_last_update_slot={} outcome=avoided_no_op",
                        reference_slot, refreshed_bookkeeping.last_update_slot
                    );
                    return Ok(config.min_update_delay);
                }

                let writable_accounts = [
                    payer.pubkey(),
                    market_pda.address(),
                    bookkeeping_pda.address(),
                    reference_exits_pda.address(),
                    previous_exits_pda.address(),
                    reference_prices_pda.address(),
                    previous_prices_pda.address(),
                ];

                for blockhash_attempt in 1..=config.send_retry_attempts {
                    if blockhash_attempt > 1 {
                        let latest_bookkeeping = program
                            .account::<Bookkeeping>(bookkeeping_pda.address())
                            .await
                            .context("failed to recheck bookkeeping after blockhash expiry")?;
                        if latest_bookkeeping.last_update_slot >= reference_slot {
                            println!(
                                "Skipping replacement update_books because target was reached while the previous blockhash expired reference_slot={} observed_last_update_slot={} outcome=avoided_no_op",
                                reference_slot, latest_bookkeeping.last_update_slot
                            );
                            return Ok(config.min_update_delay);
                        }
                    }

                    let priority_fee_micro_lamports =
                        estimate_priority_fee(&rpc, &writable_accounts, config).await;
                    let (blockhash, last_valid_block_height) = rpc
                        .get_latest_blockhash_with_commitment(CONFIRMATION_COMMITMENT)
                        .await
                        .context("failed to get latest blockhash")?;

                    let provisional_transaction = build_update_transaction(
                        payer.as_ref(),
                        &bookkeeping_ix,
                        blockhash,
                        SIMULATION_COMPUTE_UNIT_LIMIT,
                        priority_fee_micro_lamports,
                    );
                    let compute_unit_limit = match rpc
                        .simulate_transaction_with_config(
                            &provisional_transaction,
                            RpcSimulateTransactionConfig {
                                commitment: Some(CONFIRMATION_COMMITMENT),
                                ..RpcSimulateTransactionConfig::default()
                            },
                        )
                        .await
                    {
                        Ok(response) => {
                            if let Some(error) = response.value.err {
                                return Err(anyhow!(
                                    "update_books simulation failed for reference_slot={reference_slot}: {error:?}; logs={:?}",
                                    response.value.logs
                                ));
                            }
                            response
                                .value
                                .units_consumed
                                .map(|units| compute_unit_limit(units, config))
                                .unwrap_or(config.compute_unit_limit)
                        }
                        Err(error) => {
                            eprintln!(
                                "Compute-unit simulation RPC failed for reference_slot={reference_slot}: {error}. Falling back to configured limit={}",
                                config.compute_unit_limit
                            );
                            config.compute_unit_limit
                        }
                    };

                    let transaction = build_update_transaction(
                        payer.as_ref(),
                        &bookkeeping_ix,
                        blockhash,
                        compute_unit_limit,
                        priority_fee_micro_lamports,
                    );
                    let signature = transaction
                        .signatures
                        .first()
                        .copied()
                        .context("signed transaction has no signature")?;
                    let max_priority_lamports = priority_fee_micro_lamports
                        .saturating_mul(u64::from(compute_unit_limit))
                        .div_ceil(1_000_000);
                    println!(
                        "Signed update_books reference_slot={} signature={} blockhash_attempt={}/{} last_valid_block_height={} compute_unit_limit={} priority_fee_micro_lamports={} max_priority_lamports={}",
                        reference_slot,
                        signature,
                        blockhash_attempt,
                        config.send_retry_attempts,
                        last_valid_block_height,
                        compute_unit_limit,
                        priority_fee_micro_lamports,
                        max_priority_lamports,
                    );

                    match rebroadcast_until_confirmed(
                        &rpc,
                        &transaction,
                        last_valid_block_height,
                        config.rebroadcast_interval,
                    )
                    .await?
                    {
                        RebroadcastOutcome::Confirmed {
                            broadcast_attempts,
                            confirmed_slot,
                            elapsed,
                        } => {
                            match program
                                .account::<Bookkeeping>(bookkeeping_pda.address())
                                .await
                            {
                                Ok(after) if after.last_update_slot >= reference_slot => {
                                    println!(
                                        "update_books confirmed signature={} reference_slot={} confirmed_slot={} broadcast_attempts={} landing_ms={} observed_last_update_slot={} outcome=target_reached",
                                        signature,
                                        reference_slot,
                                        confirmed_slot,
                                        broadcast_attempts,
                                        elapsed.as_millis(),
                                        after.last_update_slot,
                                    );
                                }
                                Ok(after) => {
                                    eprintln!(
                                        "BOOKKEEPER_CRITICAL confirmed update_books did not reach target signature={} reference_slot={} confirmed_slot={} observed_last_update_slot={} outcome=confirmed_without_target",
                                        signature,
                                        reference_slot,
                                        confirmed_slot,
                                        after.last_update_slot,
                                    );
                                }
                                Err(error) => {
                                    eprintln!(
                                        "update_books confirmed but post-confirmation bookkeeping fetch failed signature={} reference_slot={}: {}",
                                        signature, reference_slot, error
                                    );
                                }
                            }
                            return Ok(config.min_update_delay);
                        }
                        RebroadcastOutcome::Expired {
                            broadcast_attempts,
                            elapsed,
                        } => {
                            eprintln!(
                                "update_books blockhash expired without confirmation signature={} reference_slot={} blockhash_attempt={}/{} broadcast_attempts={} elapsed_ms={}; obtaining a fresh blockhash immediately",
                                signature,
                                reference_slot,
                                blockhash_attempt,
                                config.send_retry_attempts,
                                broadcast_attempts,
                                elapsed.as_millis(),
                            );
                        }
                    }
                }

                Err(anyhow!(
                    "update_books did not confirm after {} blockhash lifetimes for reference_slot={reference_slot}",
                    config.send_retry_attempts
                ))
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
        let rebroadcast_interval_ms = parse_u64_env(
            "BOOKKEEPER_REBROADCAST_INTERVAL_MS",
            DEFAULT_REBROADCAST_INTERVAL_MS,
        )?;
        if rebroadcast_interval_ms == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_REBROADCAST_INTERVAL_MS must be greater than 0"
            ));
        }
        let priority_fee_percentile = parse_u32_env(
            "BOOKKEEPER_PRIORITY_FEE_PERCENTILE",
            DEFAULT_PRIORITY_FEE_PERCENTILE,
        )?;
        if !(1..=100).contains(&priority_fee_percentile) {
            return Err(anyhow!(
                "BOOKKEEPER_PRIORITY_FEE_PERCENTILE must be between 1 and 100"
            ));
        }
        let priority_fee_min_micro_lamports = parse_u64_env(
            "BOOKKEEPER_PRIORITY_FEE_MIN_MICRO_LAMPORTS",
            DEFAULT_PRIORITY_FEE_MIN_MICRO_LAMPORTS,
        )?;
        let priority_fee_max_micro_lamports = parse_u64_env(
            "BOOKKEEPER_PRIORITY_FEE_MAX_MICRO_LAMPORTS",
            DEFAULT_PRIORITY_FEE_MAX_MICRO_LAMPORTS,
        )?;
        if priority_fee_min_micro_lamports == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_PRIORITY_FEE_MIN_MICRO_LAMPORTS must be greater than 0"
            ));
        }
        if priority_fee_min_micro_lamports > priority_fee_max_micro_lamports {
            return Err(anyhow!(
                "BOOKKEEPER_PRIORITY_FEE_MIN_MICRO_LAMPORTS must be less than or equal to BOOKKEEPER_PRIORITY_FEE_MAX_MICRO_LAMPORTS"
            ));
        }

        let compute_unit_limit =
            parse_u32_env("BOOKKEEPER_COMPUTE_UNIT_LIMIT", DEFAULT_COMPUTE_UNIT_LIMIT)?;
        let compute_unit_min =
            parse_u32_env("BOOKKEEPER_COMPUTE_UNIT_MIN", DEFAULT_COMPUTE_UNIT_MIN)?;
        let compute_unit_max =
            parse_u32_env("BOOKKEEPER_COMPUTE_UNIT_MAX", DEFAULT_COMPUTE_UNIT_MAX)?;
        if compute_unit_min == 0 {
            return Err(anyhow!(
                "BOOKKEEPER_COMPUTE_UNIT_MIN must be greater than 0"
            ));
        }
        if compute_unit_min > compute_unit_max {
            return Err(anyhow!(
                "BOOKKEEPER_COMPUTE_UNIT_MIN must be less than or equal to BOOKKEEPER_COMPUTE_UNIT_MAX"
            ));
        }
        if !(compute_unit_min..=compute_unit_max).contains(&compute_unit_limit) {
            return Err(anyhow!(
                "BOOKKEEPER_COMPUTE_UNIT_LIMIT must be between BOOKKEEPER_COMPUTE_UNIT_MIN and BOOKKEEPER_COMPUTE_UNIT_MAX"
            ));
        }
        let compute_unit_margin_bps = parse_u64_env(
            "BOOKKEEPER_COMPUTE_UNIT_MARGIN_BPS",
            DEFAULT_COMPUTE_UNIT_MARGIN_BPS,
        )?;
        if compute_unit_margin_bps < BPS_DENOMINATOR {
            return Err(anyhow!(
                "BOOKKEEPER_COMPUTE_UNIT_MARGIN_BPS must be at least {BPS_DENOMINATOR}"
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
            rebroadcast_interval: Duration::from_millis(rebroadcast_interval_ms),
            priority_fee_percentile,
            priority_fee_min_micro_lamports,
            priority_fee_max_micro_lamports,
            compute_unit_limit,
            compute_unit_min,
            compute_unit_max,
            compute_unit_margin_bps,
        })
    }
}

async fn estimate_priority_fee(
    rpc: &RpcClient,
    writable_accounts: &[Pubkey],
    config: BookkeeperConfig,
) -> u64 {
    match rpc.get_recent_prioritization_fees(writable_accounts).await {
        Ok(fees) => priority_fee_from_samples(
            fees.into_iter().map(|fee| fee.prioritization_fee),
            config.priority_fee_percentile,
            config.priority_fee_min_micro_lamports,
            config.priority_fee_max_micro_lamports,
        ),
        Err(error) => {
            eprintln!(
                "Priority-fee estimation failed: {error}. Falling back to minimum={} micro_lamports",
                config.priority_fee_min_micro_lamports
            );
            config.priority_fee_min_micro_lamports
        }
    }
}

fn priority_fee_from_samples(
    samples: impl IntoIterator<Item = u64>,
    percentile: u32,
    minimum: u64,
    maximum: u64,
) -> u64 {
    let mut samples: Vec<u64> = samples.into_iter().collect();
    if samples.is_empty() {
        return minimum;
    }
    samples.sort_unstable();
    let rank = (samples.len() as u64)
        .saturating_mul(u64::from(percentile))
        .div_ceil(100);
    let index = rank.saturating_sub(1) as usize;
    samples[index].clamp(minimum, maximum)
}

fn compute_unit_limit(units_consumed: u64, config: BookkeeperConfig) -> u32 {
    let with_margin = units_consumed
        .saturating_mul(config.compute_unit_margin_bps)
        .div_ceil(BPS_DENOMINATOR);
    let bounded = with_margin.clamp(
        u64::from(config.compute_unit_min),
        u64::from(config.compute_unit_max),
    );
    bounded as u32
}

fn build_update_transaction(
    payer: &Keypair,
    bookkeeping_ix: &Instruction,
    blockhash: Hash,
    compute_unit_limit: u32,
    priority_fee_micro_lamports: u64,
) -> Transaction {
    let instructions = [
        ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
        ComputeBudgetInstruction::set_compute_unit_price(priority_fee_micro_lamports),
        bookkeeping_ix.clone(),
    ];
    Transaction::new_signed_with_payer(&instructions, Some(&payer.pubkey()), &[payer], blockhash)
}

async fn rebroadcast_until_confirmed<R: TransactionRpc>(
    rpc: &R,
    transaction: &Transaction,
    last_valid_block_height: u64,
    rebroadcast_interval: Duration,
) -> Result<RebroadcastOutcome> {
    let signature = transaction
        .signatures
        .first()
        .copied()
        .context("transaction has no signature")?;
    let started_at = Instant::now();
    let mut broadcast_attempts = 0_u32;
    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(CONFIRMATION_COMMITMENT.commitment),
        max_retries: Some(0),
        ..RpcSendTransactionConfig::default()
    };

    loop {
        if broadcast_attempts > 0 {
            match rpc.signature_status(&signature).await {
                Ok(Some(status)) => {
                    if let Some(error) = status.err {
                        return Err(anyhow!(
                            "transaction {signature} failed on chain in slot {}: {error:?}",
                            status.slot
                        ));
                    }
                    if status.satisfies_commitment(CONFIRMATION_COMMITMENT) {
                        return Ok(RebroadcastOutcome::Confirmed {
                            broadcast_attempts,
                            confirmed_slot: status.slot,
                            elapsed: started_at.elapsed(),
                        });
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!(
                        "Status check failed for signature={signature}: {error:#}. Rebroadcasting while the blockhash remains valid"
                    );
                }
            }
        }

        match rpc.current_block_height().await {
            Ok(block_height) if block_height > last_valid_block_height => {
                return Ok(RebroadcastOutcome::Expired {
                    broadcast_attempts,
                    elapsed: started_at.elapsed(),
                });
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!(
                    "Block-height check failed for signature={signature}: {error:#}. Keeping the same signed transaction until expiry can be established"
                );
            }
        }

        broadcast_attempts = broadcast_attempts.saturating_add(1);
        match rpc.broadcast(transaction, send_config).await {
            Ok(returned_signature) if returned_signature != signature => {
                return Err(anyhow!(
                    "sendTransaction returned unexpected signature {returned_signature}; expected {signature}"
                ));
            }
            Ok(_) if broadcast_attempts == 1 || broadcast_attempts.is_multiple_of(10) => {
                println!(
                    "Broadcast update_books signature={} attempt={}",
                    signature, broadcast_attempts
                );
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!(
                    "Broadcast failed for signature={} attempt={}: {:#}. Will retry the same signed transaction in {} seconds",
                    signature,
                    broadcast_attempts,
                    error,
                    seconds(rebroadcast_interval),
                );
            }
        }

        sleep(rebroadcast_interval).await;
    }
}

fn log_book_staleness(current_slot: u64, last_update_slot: u64, end_slot_interval: u64) {
    let stale_slots = current_slot.saturating_sub(last_update_slot);
    let freshness_boundary_slots = end_slot_interval.saturating_mul(ARRAY_LENGTH);
    if stale_slots >= freshness_boundary_slots {
        eprintln!(
            "BOOKKEEPER_CRITICAL current_slot={} last_update_slot={} stale_slots={} freshness_boundary_slots={} freshness_remaining_slots=0",
            current_slot, last_update_slot, stale_slots, freshness_boundary_slots
        );
    } else if stale_slots >= freshness_boundary_slots / 2 {
        eprintln!(
            "BOOKKEEPER_STALENESS_WARNING current_slot={} last_update_slot={} stale_slots={} freshness_boundary_slots={} freshness_remaining_slots={}",
            current_slot,
            last_update_slot,
            stale_slots,
            freshness_boundary_slots,
            freshness_boundary_slots.saturating_sub(stale_slots),
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use solana_transaction_status_client_types::TransactionConfirmationStatus;
    use std::{collections::VecDeque, sync::Mutex};

    struct FakeTransactionRpc {
        statuses: Mutex<VecDeque<Option<TransactionStatus>>>,
        block_heights: Mutex<VecDeque<u64>>,
        broadcast_signatures: Mutex<Vec<Signature>>,
    }

    impl TransactionRpc for FakeTransactionRpc {
        async fn broadcast(
            &self,
            transaction: &Transaction,
            _config: RpcSendTransactionConfig,
        ) -> Result<Signature> {
            let signature = transaction.signatures[0];
            self.broadcast_signatures.lock().unwrap().push(signature);
            Ok(signature)
        }

        async fn signature_status(
            &self,
            _signature: &Signature,
        ) -> Result<Option<TransactionStatus>> {
            Ok(self.statuses.lock().unwrap().pop_front().flatten())
        }

        async fn current_block_height(&self) -> Result<u64> {
            self.block_heights
                .lock()
                .unwrap()
                .pop_front()
                .context("no scripted block height")
        }
    }

    fn test_transaction() -> Transaction {
        Transaction {
            signatures: vec![Signature::new_unique()],
            ..Transaction::default()
        }
    }

    fn confirmed_status(slot: u64) -> TransactionStatus {
        TransactionStatus {
            slot,
            confirmations: Some(1),
            status: Ok(()),
            err: None,
            confirmation_status: Some(TransactionConfirmationStatus::Confirmed),
        }
    }

    fn test_config() -> BookkeeperConfig {
        BookkeeperConfig {
            estimated_slot_duration_ms: DEFAULT_ESTIMATED_SLOT_DURATION_MS,
            slots_between_updates: 40,
            min_update_delay: Duration::from_millis(DEFAULT_MIN_UPDATE_DELAY_MS),
            max_idle_sleep: Duration::from_millis(DEFAULT_MAX_IDLE_SLEEP_MS),
            retry_backoff: BackoffConfig {
                initial_delay: Duration::from_millis(DEFAULT_RETRY_INITIAL_DELAY_MS),
                max_delay: Duration::from_millis(DEFAULT_RETRY_MAX_DELAY_MS),
            },
            send_retry_attempts: DEFAULT_SEND_RETRY_ATTEMPTS,
            rebroadcast_interval: Duration::from_millis(DEFAULT_REBROADCAST_INTERVAL_MS),
            priority_fee_percentile: DEFAULT_PRIORITY_FEE_PERCENTILE,
            priority_fee_min_micro_lamports: DEFAULT_PRIORITY_FEE_MIN_MICRO_LAMPORTS,
            priority_fee_max_micro_lamports: DEFAULT_PRIORITY_FEE_MAX_MICRO_LAMPORTS,
            compute_unit_limit: DEFAULT_COMPUTE_UNIT_LIMIT,
            compute_unit_min: DEFAULT_COMPUTE_UNIT_MIN,
            compute_unit_max: DEFAULT_COMPUTE_UNIT_MAX,
            compute_unit_margin_bps: DEFAULT_COMPUTE_UNIT_MARGIN_BPS,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rebroadcasts_the_same_signature_until_confirmed() {
        let transaction = test_transaction();
        let expected_signature = transaction.signatures[0];
        let rpc = FakeTransactionRpc {
            statuses: Mutex::new(VecDeque::from([None, Some(confirmed_status(123))])),
            block_heights: Mutex::new(VecDeque::from([100, 101])),
            broadcast_signatures: Mutex::new(Vec::new()),
        };

        let outcome =
            rebroadcast_until_confirmed(&rpc, &transaction, 150, Duration::from_millis(1_500))
                .await
                .unwrap();

        assert!(matches!(
            outcome,
            RebroadcastOutcome::Confirmed {
                broadcast_attempts: 2,
                confirmed_slot: 123,
                ..
            }
        ));
        assert_eq!(
            *rpc.broadcast_signatures.lock().unwrap(),
            vec![expected_signature, expected_signature]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expires_only_after_the_last_valid_block_height() {
        let transaction = test_transaction();
        let rpc = FakeTransactionRpc {
            statuses: Mutex::new(VecDeque::from([None])),
            block_heights: Mutex::new(VecDeque::from([150, 151])),
            broadcast_signatures: Mutex::new(Vec::new()),
        };

        let outcome =
            rebroadcast_until_confirmed(&rpc, &transaction, 150, Duration::from_millis(1_500))
                .await
                .unwrap();

        assert!(matches!(
            outcome,
            RebroadcastOutcome::Expired {
                broadcast_attempts: 1,
                ..
            }
        ));
        assert_eq!(rpc.broadcast_signatures.lock().unwrap().len(), 1);
    }

    #[test]
    fn priority_fee_uses_requested_percentile_and_bounds() {
        assert_eq!(
            priority_fee_from_samples([0, 100, 200, 300], 75, 10, 1_000),
            200
        );
        assert_eq!(priority_fee_from_samples([0, 0], 75, 10, 1_000), 10);
        assert_eq!(
            priority_fee_from_samples([2_000, 3_000], 75, 10, 1_000),
            1_000
        );
        assert_eq!(priority_fee_from_samples([], 75, 10, 1_000), 10);
    }

    #[test]
    fn compute_unit_limit_adds_margin_and_clamps() {
        let config = test_config();
        assert_eq!(compute_unit_limit(24_000, config), 30_000);
        assert_eq!(compute_unit_limit(40_000, config), 48_000);
        assert_eq!(compute_unit_limit(100_000, config), 100_000);
    }
}
