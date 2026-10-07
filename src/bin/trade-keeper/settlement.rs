use anchor_client::solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::Instruction,
    program_pack::Pack,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::{Transaction, TransactionError},
};
use anchor_lang::{InstructionData, ToAccountMetas, system_program};
use anchor_spl::{
    associated_token::spl_associated_token_account, token::spl_token, token_2022::spl_token_2022,
};
use anyhow::{Context, ensure};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::{
    config::{RpcAccountInfoConfig, RpcSendTransactionConfig},
    request::RpcRequest,
    response::{Response, RpcBlockhash},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::time::{Duration, sleep, timeout};
use twob_keepers::{
    ARRAY_LENGTH, AccountResolver, END_SLOT_INTERVAL, MAXIMUM_DURATION_SLOTS,
    accounts::{decode_account_data, fetch_market},
    twob_anchor::{
        self,
        accounts::TradePosition,
        client::{accounts, args},
    },
};

const PACKET_BYTES: u64 = 1232;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;

pub struct SettlementConfig {
    pub max_batch_size: usize,
    /// Compute allowance per position, summed for the transaction.
    pub compute_unit_limit: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Closed,
    NotDue,
    MissingReceiver,
    Retry,
}

pub struct PositionOutcome {
    pub address: Pubkey,
    pub position: Option<TradePosition>,
    pub slot: u64,
    pub disposition: Disposition,
}

/// Pausing freezes the end slot; the public abandonment timeout is strictly greater.
pub fn public_close_slot(position: &TradePosition) -> Option<u64> {
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

#[derive(Clone, Copy)]
struct Token {
    mint: Pubkey,
    program: Pubkey,
    vault: Pubkey,
}

struct Receiver {
    address: Pubkey,
    wallet: Pubkey,
    token: Token,
}

#[derive(Clone, Copy)]
struct PendingTransaction {
    signature: Signature,
    last_valid_block_height: u64,
    submitted_slot: u64,
}

pub struct Settlement {
    rpc: Arc<RpcClient>,
    payer: Arc<Keypair>,
    market: Pubkey,
    resolver: AccountResolver,
    base: Token,
    quote: Token,
    config: SettlementConfig,
    pending: Mutex<HashMap<Pubkey, PendingTransaction>>,
}

impl Settlement {
    pub async fn new(
        rpc: Arc<RpcClient>,
        payer: Arc<Keypair>,
        market_address: Pubkey,
        config: SettlementConfig,
    ) -> anyhow::Result<Self> {
        ensure!(config.max_batch_size > 0, "batch size must be positive");
        ensure!(
            (1..=MAX_COMPUTE_UNITS).contains(&config.compute_unit_limit),
            "per-position compute allowance must be between 1 and 1,400,000"
        );
        let market = fetch_market(&rpc, &market_address).await?;
        let mints = rpc
            .get_multiple_accounts(&[market.base_mint, market.quote_mint])
            .await?;
        let resolver = AccountResolver::new(twob_anchor::ID);
        let token = |index: usize, mint| -> anyhow::Result<Token> {
            let program = mints[index]
                .as_ref()
                .context("market mint is missing")?
                .owner;
            ensure!(
                program == spl_token::ID || program == spl_token_2022::ID,
                "market mint is not owned by a supported token program"
            );
            Ok(Token {
                mint,
                program,
                vault: resolver.associated_token_account_with_program(
                    &market_address,
                    &mint,
                    &program,
                ),
            })
        };
        let base = token(0, market.base_mint)?;
        let quote = token(1, market.quote_mint)?;
        Ok(Self {
            rpc,
            payer,
            market: market_address,
            resolver,
            base,
            quote,
            config,
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// Refresh positions and the two receiving ATAs in bulk before spending fees.
    /// Missing non-native receivers are retried by the scheduler, never created here.
    pub async fn process(&self, addresses: &[Pubkey]) -> anyhow::Result<Vec<PositionOutcome>> {
        let mut seen = HashSet::new();
        let addresses: Vec<_> = addresses
            .iter()
            .copied()
            .filter(|key| seen.insert(*key))
            .collect();
        ensure!(
            addresses.len() <= 100,
            "a settlement pass supports at most 100 positions"
        );
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        let min_context_slot = {
            let pending = self.pending.lock().unwrap();
            addresses
                .iter()
                .filter_map(|address| pending.get(address).map(|entry| entry.submitted_slot))
                .max()
        };
        let response = self
            .rpc
            .get_multiple_accounts_with_config(
                &addresses,
                RpcAccountInfoConfig {
                    commitment: Some(CommitmentConfig::confirmed()),
                    min_context_slot,
                    ..Default::default()
                },
            )
            .await?;
        let slot = response.context.slot;
        let mut outcomes: Vec<_> = addresses
            .into_iter()
            .zip(response.value)
            .map(|(address, account)| {
                let mut result = PositionOutcome {
                    address,
                    position: None,
                    slot,
                    disposition: Disposition::Closed,
                };
                if let Some(account) = account.filter(|account| account.owner == twob_anchor::ID) {
                    match decode_account_data::<TradePosition>(&account.data) {
                        Ok(position) if position.market == self.market => {
                            result.disposition = match public_close_slot(&position) {
                                Some(end) if end <= slot => Disposition::Retry,
                                Some(_) => Disposition::NotDue,
                                None => Disposition::Retry,
                            };
                            result.position = Some(position);
                        }
                        Ok(_) => {}
                        Err(error) => {
                            eprintln!("Cannot decode trade position {address}: {error}");
                            result.disposition = Disposition::Retry;
                        }
                    }
                }
                result
            })
            .collect();

        let waiting = self.refresh_pending(&mut outcomes).await?;
        let mut receivers = HashSet::new();
        let mut candidates = Vec::new();
        for (index, outcome) in outcomes.iter().enumerate() {
            if outcome.disposition != Disposition::Retry || waiting.contains(&outcome.address) {
                continue;
            }
            let Some(position) = &outcome.position else {
                continue;
            };
            if public_close_slot(position).is_none()
                || position
                    .last_update_slot
                    .checked_add(u64::from(position.remaining_slots))
                    .is_none()
            {
                continue;
            }
            for receiver in self.receivers(&outcome.address, position) {
                if receiver.token.mint != spl_token::native_mint::ID {
                    receivers.insert(receiver.address);
                }
            }
            candidates.push(index);
        }
        let receiver_addresses: Vec<_> = receivers.into_iter().collect();
        let mut receiver_accounts = HashMap::new();
        for chunk in receiver_addresses.chunks(100) {
            let response = self
                .rpc
                .get_multiple_accounts_with_config(
                    chunk,
                    RpcAccountInfoConfig {
                        commitment: Some(CommitmentConfig::confirmed()),
                        min_context_slot: Some(slot),
                        ..Default::default()
                    },
                )
                .await?;
            receiver_accounts.extend(chunk.iter().copied().zip(response.value));
        }
        candidates.retain(|&index| {
            let outcome = &mut outcomes[index];
            let ready = self
                .receivers(&outcome.address, outcome.position.as_ref().unwrap())
                .iter()
                .all(|receiver| {
                    receiver.token.mint == spl_token::native_mint::ID
                        || receiver_accounts
                            .get(&receiver.address)
                            .and_then(Option::as_ref)
                            .is_some_and(|account| receiver_is_ready(account, receiver))
                });
            if !ready {
                outcome.disposition = Disposition::MissingReceiver;
            }
            ready
        });

        // Failed preflights are divided until a bad position cannot block its peers.
        // A transport failure leaves Retry outcomes rather than looking like a missing ATA.
        let mut queue: VecDeque<Vec<usize>> = candidates
            .chunks(self.config.max_batch_size)
            .map(<[usize]>::to_vec)
            .collect();
        let mut submitted = Vec::new();
        while let Some(mut batch) = queue.pop_front() {
            let prepared = async {
                // Never carry an interval reference across batches: it changes every 176 slots.
                // Preserve the blockhash response's bank context instead of another getSlot call.
                let response: Response<RpcBlockhash> = self
                    .rpc
                    .send(
                        RpcRequest::GetLatestBlockhash,
                        serde_json::json!([{"commitment": "confirmed", "minContextSlot": slot}]),
                    )
                    .await?;
                let blockhash: Hash = response.value.blockhash.parse()?;
                let last_valid_block_height = response.value.last_valid_block_height;
                let current_slot = response.context.slot;
                ensure!(
                    current_slot >= slot,
                    "RPC slot moved behind the refreshed positions"
                );
                let count = self.fitting_prefix(&batch, &outcomes, current_slot, blockhash)?;
                ensure!(
                    count > 0,
                    "single-position transaction exceeds the packet limit"
                );
                if count < batch.len() {
                    queue.push_front(batch.split_off(count));
                }
                let transaction = self.transaction(&batch, &outcomes, current_slot, blockhash)?;
                Ok::<_, anyhow::Error>((transaction, current_slot, last_valid_block_height))
            }
            .await;
            let (transaction, current_slot, last_valid_block_height) = match prepared {
                Ok(value) => value,
                Err(error) => {
                    eprintln!("Cannot prepare settlement transaction: {error}");
                    break;
                }
            };
            // Record the locally known signature before RPC: a lost response is not a failed send.
            let signature = transaction.signatures[0];
            {
                let mut pending = self.pending.lock().unwrap();
                for &index in &batch {
                    pending.insert(
                        outcomes[index].address,
                        PendingTransaction {
                            signature,
                            last_valid_block_height,
                            submitted_slot: current_slot,
                        },
                    );
                }
            }
            println!(
                "Submitting settlement for {} positions: {signature}",
                batch.len()
            );
            match self
                .rpc
                .send_transaction_with_config(
                    &transaction,
                    RpcSendTransactionConfig {
                        skip_preflight: false,
                        preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
                        min_context_slot: Some(current_slot),
                        max_retries: Some(2),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(signature) => submitted.push((signature, batch)),
                Err(error) => {
                    eprintln!("Settlement submission failed: {error}");
                    if matches!(
                        error.get_transaction_error(),
                        Some(TransactionError::AlreadyProcessed)
                    ) {
                        submitted.push((signature, batch));
                    } else if error.get_transaction_error().is_some() {
                        self.clear_pending(&batch, &outcomes);
                        split_failed(batch, &mut queue);
                    } else {
                        // Unknown send outcome may already have landed. Refresh on the next pass.
                        break;
                    }
                }
            }
        }
        if !submitted.is_empty() {
            // Confirm all batches together, with a deadline including slow RPC calls.
            let confirmation = timeout(Duration::from_secs(20), async {
                while !submitted.is_empty() {
                    let signatures: Vec<_> =
                        submitted.iter().map(|(signature, _)| *signature).collect();
                    let statuses = self.rpc.get_signature_statuses(&signatures).await?;
                    let mut pending = Vec::new();
                    for ((signature, batch), status) in submitted.drain(..).zip(statuses.value) {
                        match status {
                            Some(status)
                                if status.satisfies_commitment(CommitmentConfig::confirmed()) =>
                            {
                                self.clear_pending(&batch, &outcomes);
                                if status.err.is_none() {
                                    for index in batch {
                                        outcomes[index].disposition = Disposition::Closed;
                                        outcomes[index].slot = status.slot;
                                        println!(
                                            "Closed trade position {}: {signature}",
                                            outcomes[index].address
                                        );
                                    }
                                } else {
                                    eprintln!("Settlement {signature} failed: {:?}", status.err);
                                }
                            }
                            _ => pending.push((signature, batch)),
                        }
                    }
                    submitted = pending;
                    if !submitted.is_empty() {
                        sleep(Duration::from_secs(1)).await;
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await;
            match confirmation {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("Settlement confirmation RPC failed: {error}"),
                Err(_) => eprintln!(
                    "Settlement confirmation timed out; refreshing positions on the next pass"
                ),
            }
        }
        Ok(outcomes)
    }

    fn clear_pending(&self, batch: &[usize], outcomes: &[PositionOutcome]) {
        let mut pending = self.pending.lock().unwrap();
        for &index in batch {
            pending.remove(&outcomes[index].address);
        }
    }

    /// An unresolved signature holds the position until it fails or its blockhash expires.
    /// This prevents a second fee-paying transaction after a lost send/confirmation response.
    async fn refresh_pending(
        &self,
        outcomes: &mut [PositionOutcome],
    ) -> anyhow::Result<HashSet<Pubkey>> {
        let snapshot: HashMap<_, _> = {
            let mut pending = self.pending.lock().unwrap();
            for outcome in outcomes
                .iter()
                .filter(|outcome| outcome.disposition == Disposition::Closed)
            {
                pending.remove(&outcome.address);
            }
            pending.clone()
        };
        if snapshot.is_empty() {
            return Ok(HashSet::new());
        }
        let signatures: Vec<_> = snapshot
            .values()
            .map(|entry| entry.signature)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut statuses = HashMap::new();
        for chunk in signatures.chunks(256) {
            let response = self.rpc.get_signature_statuses(chunk).await?;
            statuses.extend(chunk.iter().copied().zip(response.value));
        }
        let confirmed = |signature: &Signature| {
            statuses
                .get(signature)
                .and_then(Option::as_ref)
                .filter(|status| status.satisfies_commitment(CommitmentConfig::confirmed()))
        };
        let height = if snapshot
            .values()
            .any(|entry| confirmed(&entry.signature).is_none())
        {
            Some(
                self.rpc
                    .get_block_height_with_commitment(CommitmentConfig::confirmed())
                    .await?,
            )
        } else {
            None
        };
        let mut waiting = HashSet::new();
        let mut pending = self.pending.lock().unwrap();
        for (address, entry) in snapshot {
            if let Some(status) = confirmed(&entry.signature) {
                pending.remove(&address);
                if status.err.is_none() {
                    if let Some(outcome) = outcomes
                        .iter_mut()
                        .find(|outcome| outcome.address == address)
                    {
                        // A later live snapshot may be a new position reusing the same PDA.
                        if status.slot > outcome.slot {
                            outcome.disposition = Disposition::Closed;
                            outcome.slot = status.slot;
                        }
                    }
                }
            } else if height.is_some_and(|height| height > entry.last_valid_block_height) {
                pending.remove(&address);
                // It may have landed between the earlier account/status reads and expiry.
                // Require another fresh account read before constructing a replacement.
                waiting.insert(address);
            } else {
                waiting.insert(address);
            }
        }
        Ok(waiting)
    }

    fn receivers(&self, address: &Pubkey, position: &TradePosition) -> [Receiver; 2] {
        [
            (position.base_receiver, self.base),
            (position.quote_receiver, self.quote),
        ]
        .map(|(wallet, token)| Receiver {
            address: self.resolver.receiver_token_account(
                address,
                &wallet,
                &token.mint,
                &token.program,
            ),
            wallet,
            token,
        })
    }

    fn close_instruction(
        &self,
        address: &Pubkey,
        position: &TradePosition,
        slot: u64,
    ) -> anyhow::Result<Instruction> {
        let reference_index = slot / END_SLOT_INTERVAL / ARRAY_LENGTH;
        let previous_index = reference_index
            .checked_sub(1)
            .context("market has no previous interval yet")?;
        let end_slot = position
            .last_update_slot
            .checked_add(u64::from(position.remaining_slots))
            .context("position end slot overflow")?;
        let end_index = end_slot / END_SLOT_INTERVAL / ARRAY_LENGTH;
        let [base_receiver, quote_receiver] = self.receivers(address, position);
        Ok(Instruction {
            program_id: twob_anchor::ID,
            accounts: accounts::PublicCloseTradePosition {
                signer: self.payer.pubkey(),
                program_config: self.resolver.program_config_pda().address(),
                payer: position.payer,
                base_receiver: position.base_receiver,
                quote_receiver: position.quote_receiver,
                base_mint: self.base.mint,
                quote_mint: self.quote.mint,
                receiver_base_token_account: base_receiver.address,
                receiver_quote_token_account: quote_receiver.address,
                market: self.market,
                trade_position: *address,
                base_vault: self.base.vault,
                quote_vault: self.quote.vault,
                future_interval: self
                    .resolver
                    .market_interval_pda(&self.market, end_index)
                    .address(),
                current_interval: self
                    .resolver
                    .market_interval_pda(&self.market, reference_index)
                    .address(),
                previous_interval: self
                    .resolver
                    .market_interval_pda(&self.market, previous_index)
                    .address(),
                base_token_program: self.base.program,
                quote_token_program: self.quote.program,
                associated_token_program: spl_associated_token_account::ID,
                system_program: system_program::ID,
            }
            .to_account_metas(None),
            data: args::PublicCloseTradePosition { reference_index }.data(),
        })
    }

    fn transaction(
        &self,
        batch: &[usize],
        outcomes: &[PositionOutcome],
        slot: u64,
        blockhash: Hash,
    ) -> anyhow::Result<Transaction> {
        let units = self
            .config
            .compute_unit_limit
            .saturating_mul(batch.len() as u32)
            .min(MAX_COMPUTE_UNITS);
        let mut instructions = vec![ComputeBudgetInstruction::set_compute_unit_limit(units)];
        for &index in batch {
            let outcome = &outcomes[index];
            instructions.push(self.close_instruction(
                &outcome.address,
                outcome.position.as_ref().unwrap(),
                slot,
            )?);
        }
        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.payer.pubkey()),
            &[self.payer.as_ref()],
            blockhash,
        ))
    }

    fn fitting_prefix(
        &self,
        batch: &[usize],
        outcomes: &[PositionOutcome],
        slot: u64,
        blockhash: Hash,
    ) -> anyhow::Result<usize> {
        let mut count = 0;
        for end in 1..=batch.len() {
            if u64::from(self.config.compute_unit_limit) * end as u64 > u64::from(MAX_COMPUTE_UNITS)
            {
                break;
            }
            let transaction = self.transaction(&batch[..end], outcomes, slot, blockhash)?;
            if bincode::serialized_size(&transaction)? > PACKET_BYTES {
                break;
            }
            count = end;
        }
        Ok(count)
    }
}

fn receiver_is_ready(account: &Account, receiver: &Receiver) -> bool {
    if account.owner != receiver.token.program || account.executable {
        return false;
    }
    if account.owner == spl_token::ID {
        spl_token::state::Account::unpack(&account.data).is_ok_and(|token| {
            token.mint == receiver.token.mint
                && token.owner == receiver.wallet
                && token.state == spl_token::state::AccountState::Initialized
        })
    } else if account.owner == spl_token_2022::ID {
        spl_token_2022::extension::StateWithExtensions::<spl_token_2022::state::Account>::unpack(
            &account.data,
        )
        .is_ok_and(|token| {
            token.base.mint == receiver.token.mint
                && token.base.owner == receiver.wallet
                && token.base.state == spl_token_2022::state::AccountState::Initialized
        })
    } else {
        false
    }
}

fn split_failed(mut batch: Vec<usize>, queue: &mut VecDeque<Vec<usize>>) {
    if batch.len() > 1 {
        let second = batch.split_off(batch.len() / 2);
        queue.push_front(second);
        queue.push_front(batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anchor_lang::Discriminator;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use serde_json::{Value, json};
    use solana_rpc_client::{
        rpc_client::RpcClientConfig,
        rpc_sender::{RpcSender, RpcTransportStats},
    };
    use solana_rpc_client_api::{
        client_error::Result as RpcResult,
        request::{RpcError, RpcRequest, RpcResponseErrorData},
    };

    type Requests = Arc<Mutex<Vec<(RpcRequest, Value)>>>;

    struct TestRpc {
        accounts: HashMap<Pubkey, Account>,
        position_addresses: HashSet<Pubkey>,
        requests: Requests,
        fail_receiver_fetch: bool,
        bad_position: Option<Pubkey>,
        behavior: Arc<Mutex<TestBehavior>>,
    }

    struct TestBehavior {
        ambiguous_sends: usize,
        block_height: u64,
        fail_statuses: bool,
        unconfirmed: HashSet<String>,
    }

    impl Default for TestBehavior {
        fn default() -> Self {
            Self {
                ambiguous_sends: 0,
                block_height: 1760,
                fail_statuses: false,
                unconfirmed: HashSet::new(),
            }
        }
    }

    fn decode_transaction(params: &Value) -> Transaction {
        bincode::deserialize(&BASE64_STANDARD.decode(params[0].as_str().unwrap()).unwrap()).unwrap()
    }

    #[async_trait::async_trait]
    impl RpcSender for TestRpc {
        async fn send(&self, request: RpcRequest, params: Value) -> RpcResult<Value> {
            self.requests
                .lock()
                .unwrap()
                .push((request, params.clone()));
            Ok(match request {
                RpcRequest::GetMultipleAccounts => {
                    let addresses: Vec<Pubkey> = params[0]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|value| value.as_str().unwrap().parse().unwrap())
                        .collect();
                    if self.fail_receiver_fetch
                        && addresses
                            .iter()
                            .any(|address| !self.position_addresses.contains(address))
                    {
                        return Err(RpcError::RpcRequestError(
                            "receiver RPC temporarily unavailable".to_owned(),
                        )
                        .into());
                    }
                    let accounts: Vec<_> = addresses
                        .iter()
                        .map(|address| {
                            self.accounts.get(address).map(|account| json!({
                            "data": [BASE64_STANDARD.encode(&account.data), "base64"],
                            "owner": account.owner.to_string(), "lamports": account.lamports,
                            "executable": account.executable, "rentEpoch": account.rent_epoch,
                        }))
                        })
                        .collect();
                    json!({"context": {"slot": 1760}, "value": accounts})
                }
                RpcRequest::GetLatestBlockhash => json!({"context": {"slot": 1760}, "value": {
                    "blockhash": Hash::new_unique().to_string(),
                    "lastValidBlockHeight": self.behavior.lock().unwrap().block_height + 150
                }}),
                RpcRequest::GetBlockHeight => json!(self.behavior.lock().unwrap().block_height),
                RpcRequest::SendTransaction => {
                    let transaction = decode_transaction(&params);
                    assert_eq!(params[1]["skipPreflight"], false);
                    if self
                        .bad_position
                        .is_some_and(|key| transaction.message.account_keys.contains(&key))
                    {
                        return Err(RpcError::RpcResponseError {
                            code: -32002, message: "transaction simulation failed".to_owned(),
                            data: RpcResponseErrorData::SendTransactionPreflightFailure(serde_json::from_value(json!({
                                "err": {"InstructionError": [1, {"Custom": 1}]}, "logs": [], "unitsConsumed": 35_500
                            })).unwrap()),
                        }.into());
                    }
                    let mut behavior = self.behavior.lock().unwrap();
                    if behavior.ambiguous_sends > 0 {
                        behavior.ambiguous_sends -= 1;
                        behavior
                            .unconfirmed
                            .insert(transaction.signatures[0].to_string());
                        return Err(
                            RpcError::RpcRequestError("send response lost".to_owned()).into()
                        );
                    }
                    json!(transaction.signatures[0].to_string())
                }
                RpcRequest::GetSignatureStatuses => {
                    let behavior = self.behavior.lock().unwrap();
                    if behavior.fail_statuses {
                        return Err(
                            RpcError::RpcRequestError("status RPC unavailable".to_owned()).into(),
                        );
                    }
                    let statuses: Vec<_> = params[0]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|signature| {
                            if behavior.unconfirmed.contains(signature.as_str().unwrap()) {
                                Value::Null
                            } else {
                                json!({
                                    "slot": 1761, "confirmations": 1, "status": {"Ok": null},
                                    "err": null, "confirmationStatus": "confirmed"
                                })
                            }
                        })
                        .collect();
                    json!({"context": {"slot": 1762}, "value": statuses})
                }
                _ => panic!("unexpected RPC request: {request:?}"),
            })
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }
        fn url(&self) -> String {
            "mock://settlement".to_owned()
        }
    }

    fn mock_accounts(
        settlement: &Settlement,
        outcomes: &[PositionOutcome],
    ) -> HashMap<Pubkey, Account> {
        let mut accounts = HashMap::new();
        for outcome in outcomes {
            let position = outcome.position.as_ref().unwrap();
            let mut data = TradePosition::DISCRIMINATOR.to_vec();
            data.extend_from_slice(bytemuck::bytes_of(position));
            accounts.insert(
                outcome.address,
                Account {
                    owner: twob_anchor::ID,
                    data,
                    lamports: 1,
                    ..Default::default()
                },
            );
            for receiver in settlement.receivers(&outcome.address, position) {
                if receiver.token.mint == spl_token::native_mint::ID {
                    continue;
                }
                let mut data = vec![0; spl_token::state::Account::LEN];
                // Base token-account layouts are shared by SPL Token and Token-2022.
                spl_token::state::Account::pack(
                    spl_token::state::Account {
                        mint: receiver.token.mint,
                        owner: receiver.wallet,
                        state: spl_token::state::AccountState::Initialized,
                        ..Default::default()
                    },
                    &mut data,
                )
                .unwrap();
                accounts.insert(
                    receiver.address,
                    Account {
                        owner: receiver.token.program,
                        data,
                        lamports: 1,
                        ..Default::default()
                    },
                );
            }
        }
        accounts
    }

    fn attach_mock(
        settlement: &mut Settlement,
        outcomes: &[PositionOutcome],
        accounts: HashMap<Pubkey, Account>,
        fail_receiver_fetch: bool,
        bad_position: Option<Pubkey>,
    ) -> Requests {
        attach_mock_behavior(
            settlement,
            outcomes,
            accounts,
            fail_receiver_fetch,
            bad_position,
            Arc::new(Mutex::new(TestBehavior::default())),
        )
    }

    fn attach_mock_behavior(
        settlement: &mut Settlement,
        outcomes: &[PositionOutcome],
        accounts: HashMap<Pubkey, Account>,
        fail_receiver_fetch: bool,
        bad_position: Option<Pubkey>,
        behavior: Arc<Mutex<TestBehavior>>,
    ) -> Requests {
        let requests = Arc::new(Mutex::new(Vec::new()));
        settlement.rpc = Arc::new(RpcClient::new_sender(
            TestRpc {
                accounts,
                requests: requests.clone(),
                fail_receiver_fetch,
                bad_position,
                behavior,
                position_addresses: outcomes.iter().map(|outcome| outcome.address).collect(),
            },
            RpcClientConfig::with_commitment(CommitmentConfig::confirmed()),
        ));
        requests
    }

    #[tokio::test]
    async fn process_filters_not_due_positions_and_missing_receivers_before_submission() {
        let (mut settlement, mut outcomes) = fixture();
        outcomes.truncate(2);
        outcomes[0].position.as_mut().unwrap().remaining_slots = 2000;
        let mut accounts = mock_accounts(&settlement, &outcomes);
        let receivers =
            settlement.receivers(&outcomes[1].address, outcomes[1].position.as_ref().unwrap());
        accounts.remove(&receivers[1].address);
        let requests = attach_mock(&mut settlement, &outcomes, accounts, false, None);
        let missing = Pubkey::new_unique();
        let results = settlement
            .process(&[
                outcomes[0].address,
                outcomes[1].address,
                missing,
                outcomes[1].address,
            ])
            .await
            .unwrap();
        assert_eq!(results.len(), 3, "duplicate candidates must be removed");
        assert_eq!(results[0].disposition, Disposition::NotDue);
        assert_eq!(results[1].disposition, Disposition::MissingReceiver);
        assert_eq!(results[2].disposition, Disposition::Closed);
        assert!(results.iter().all(|outcome| outcome.slot == 1760));
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            2,
            "no simulation or send for ineligible positions"
        );
        assert_eq!(
            requests[1].1[0].as_array().unwrap().len(),
            2,
            "check only the due position's two ATAs"
        );
        assert_eq!(requests[1].1[1]["minContextSlot"], 1760);
        assert!(
            requests
                .iter()
                .all(|(_, params)| params[1]["commitment"] == "confirmed")
        );
    }

    #[tokio::test]
    async fn process_does_not_mistake_receiver_rpc_failure_for_missing_accounts() {
        let (mut settlement, mut outcomes) = fixture();
        outcomes.truncate(1);
        let accounts = mock_accounts(&settlement, &outcomes);
        let requests = attach_mock(&mut settlement, &outcomes, accounts, true, None);
        assert!(settlement.process(&[outcomes[0].address]).await.is_err());
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn process_requires_only_the_non_native_ata_for_sol_settlement() {
        let (mut settlement, mut outcomes) = fixture();
        outcomes.truncate(1);
        settlement.base.mint = spl_token::native_mint::ID;
        let accounts = mock_accounts(&settlement, &outcomes);
        let requests = attach_mock(&mut settlement, &outcomes, accounts, false, None);
        let results = settlement.process(&[outcomes[0].address]).await.unwrap();
        assert_eq!(results[0].disposition, Disposition::Closed);
        assert_eq!(
            requests.lock().unwrap()[1].1[0].as_array().unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn ambiguous_send_waits_without_resigning_and_later_confirmation_closes_stale_snapshot() {
        let (mut settlement, mut outcomes) = fixture();
        outcomes.truncate(1);
        let accounts = mock_accounts(&settlement, &outcomes);
        let behavior = Arc::new(Mutex::new(TestBehavior {
            ambiguous_sends: 1,
            ..Default::default()
        }));
        let requests = attach_mock_behavior(
            &mut settlement,
            &outcomes,
            accounts,
            false,
            None,
            behavior.clone(),
        );
        let addresses = [outcomes[0].address];
        assert_eq!(
            settlement.process(&addresses).await.unwrap()[0].disposition,
            Disposition::Retry
        );
        assert_eq!(
            settlement.process(&addresses).await.unwrap()[0].disposition,
            Disposition::Retry
        );
        behavior.lock().unwrap().fail_statuses = true;
        assert!(settlement.process(&addresses).await.is_err());
        assert_eq!(settlement.pending.lock().unwrap().len(), 1);
        {
            let mut behavior = behavior.lock().unwrap();
            behavior.fail_statuses = false;
            behavior.unconfirmed.clear();
        }
        let results = settlement.process(&addresses).await.unwrap();
        assert_eq!(results[0].disposition, Disposition::Closed);
        assert_eq!(
            results[0].slot, 1761,
            "confirmed status must supersede the stale account snapshot"
        );
        assert!(settlement.pending.lock().unwrap().is_empty());
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(request, _)| *request == RpcRequest::SendTransaction)
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(request, _)| *request == RpcRequest::GetLatestBlockhash)
                .count(),
            1
        );
        assert!(
            requests
                .iter()
                .filter(
                    |(request, params)| *request == RpcRequest::GetMultipleAccounts
                        && params[0][0] == addresses[0].to_string()
                )
                .skip(1)
                .all(|(_, params)| params[1]["minContextSlot"] == 1760),
            "a pending signature requires a position snapshot at least as recent as its submission bank"
        );
    }

    #[tokio::test]
    async fn ambiguous_send_can_be_resigned_only_after_expiration_and_cleans_untracked_entries() {
        let (mut settlement, mut outcomes) = fixture();
        outcomes.truncate(1);
        let accounts = mock_accounts(&settlement, &outcomes);
        let behavior = Arc::new(Mutex::new(TestBehavior {
            ambiguous_sends: 1,
            ..Default::default()
        }));
        let requests = attach_mock_behavior(
            &mut settlement,
            &outcomes,
            accounts,
            false,
            None,
            behavior.clone(),
        );
        let addresses = [outcomes[0].address];
        assert_eq!(
            settlement.process(&addresses).await.unwrap()[0].disposition,
            Disposition::Retry
        );
        let entry = *settlement
            .pending
            .lock()
            .unwrap()
            .get(&addresses[0])
            .unwrap();
        settlement
            .pending
            .lock()
            .unwrap()
            .insert(Pubkey::new_unique(), entry);
        behavior.lock().unwrap().block_height = entry.last_valid_block_height;
        assert_eq!(
            settlement.process(&addresses).await.unwrap()[0].disposition,
            Disposition::Retry
        );
        behavior.lock().unwrap().block_height += 1;
        assert_eq!(
            settlement.process(&addresses).await.unwrap()[0].disposition,
            Disposition::Retry
        );
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(request, _)| *request == RpcRequest::SendTransaction)
                .count(),
            1,
            "observing expiration must not reuse the account snapshot from before expiration"
        );
        assert_eq!(
            settlement.process(&addresses).await.unwrap()[0].disposition,
            Disposition::Closed
        );
        assert!(settlement.pending.lock().unwrap().is_empty());
        let requests = requests.lock().unwrap();
        let sends: Vec<_> = requests
            .iter()
            .filter(|(request, _)| *request == RpcRequest::SendTransaction)
            .collect();
        assert_eq!(sends.len(), 2);
        assert_ne!(
            decode_transaction(&sends[0].1).signatures[0],
            decode_transaction(&sends[1].1).signatures[0]
        );
    }

    #[tokio::test]
    async fn process_splits_failed_preflight_and_closes_the_healthy_peer() {
        let (mut settlement, mut outcomes) = fixture();
        outcomes.truncate(2);
        let accounts = mock_accounts(&settlement, &outcomes);
        let requests = attach_mock(
            &mut settlement,
            &outcomes,
            accounts,
            false,
            Some(outcomes[0].address),
        );
        let results = settlement
            .process(&[outcomes[0].address, outcomes[1].address])
            .await
            .unwrap();
        assert_eq!(results[0].disposition, Disposition::Retry);
        assert_eq!(results[1].disposition, Disposition::Closed);
        assert_eq!(results[1].slot, 1761);
        assert!(
            settlement.pending.lock().unwrap().is_empty(),
            "definitive preflight failures must release the pending guard"
        );
        let requests = requests.lock().unwrap();
        let submissions: Vec<_> = requests
            .iter()
            .filter(|(request, _)| *request == RpcRequest::SendTransaction)
            .collect();
        assert_eq!(submissions.len(), 3);
        assert_eq!(
            decode_transaction(&submissions[0].1)
                .message
                .instructions
                .len(),
            3,
            "first preflight must batch both closes"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(request, _)| *request == RpcRequest::GetLatestBlockhash)
                .count(),
            3,
            "each split transaction must get a fresh interval reference"
        );
    }

    #[test]
    fn public_closure_tracks_resumed_end_and_strict_abandonment_timeout() {
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
        position.last_update_slot = u64::MAX - 50;
        assert_eq!(
            public_close_slot(&position),
            Some(100 + MAXIMUM_DURATION_SLOTS + 1)
        );
        position.start_slot = u64::MAX;
        assert_eq!(public_close_slot(&position), None);
        position.paused_at_slot = 0;
        position.remaining_slots = 51;
        assert_eq!(public_close_slot(&position), None);
    }

    fn receiver(program: Pubkey) -> Receiver {
        Receiver {
            address: Pubkey::new_unique(),
            wallet: Pubkey::new_unique(),
            token: Token {
                mint: Pubkey::new_unique(),
                program,
                vault: Pubkey::new_unique(),
            },
        }
    }

    #[test]
    fn receiving_accounts_must_have_the_correct_program_mint_owner_and_state() {
        let receiver = receiver(spl_token::ID);
        let mut token = spl_token::state::Account {
            mint: receiver.token.mint,
            owner: receiver.wallet,
            state: spl_token::state::AccountState::Initialized,
            ..Default::default()
        };
        let mut account = Account {
            owner: spl_token::ID,
            data: vec![0; spl_token::state::Account::LEN],
            ..Default::default()
        };
        spl_token::state::Account::pack(token, &mut account.data).unwrap();
        assert!(receiver_is_ready(&account, &receiver));
        account.owner = spl_token_2022::ID;
        assert!(!receiver_is_ready(&account, &receiver));
        account.owner = spl_token::ID;
        token.mint = Pubkey::new_unique();
        spl_token::state::Account::pack(token, &mut account.data).unwrap();
        assert!(!receiver_is_ready(&account, &receiver));
        token.mint = receiver.token.mint;
        token.owner = Pubkey::new_unique();
        spl_token::state::Account::pack(token, &mut account.data).unwrap();
        assert!(!receiver_is_ready(&account, &receiver));
        token.owner = receiver.wallet;
        for state in [
            spl_token::state::AccountState::Frozen,
            spl_token::state::AccountState::Uninitialized,
        ] {
            token.state = state;
            spl_token::state::Account::pack(token, &mut account.data).unwrap();
            assert!(!receiver_is_ready(&account, &receiver));
        }
        account.data.truncate(10);
        assert!(!receiver_is_ready(&account, &receiver));
    }

    #[test]
    fn token_2022_accounts_with_extensions_are_supported() {
        use spl_token_2022::extension::{
            BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
            immutable_owner::ImmutableOwner,
        };
        let receiver = receiver(spl_token_2022::ID);
        let len = ExtensionType::try_calculate_account_len::<spl_token_2022::state::Account>(&[
            ExtensionType::ImmutableOwner,
        ])
        .unwrap();
        let mut account = Account {
            owner: spl_token_2022::ID,
            data: vec![0; len],
            ..Default::default()
        };
        let mut state =
            StateWithExtensionsMut::<spl_token_2022::state::Account>::unpack_uninitialized(
                &mut account.data,
            )
            .unwrap();
        state.base.mint = receiver.token.mint;
        state.base.owner = receiver.wallet;
        state.base.state = spl_token_2022::state::AccountState::Initialized;
        state.init_extension::<ImmutableOwner>(true).unwrap();
        state.pack_base();
        state.init_account_type().unwrap();
        assert!(receiver_is_ready(&account, &receiver));
        let mut state =
            StateWithExtensionsMut::<spl_token_2022::state::Account>::unpack(&mut account.data)
                .unwrap();
        state.base.state = spl_token_2022::state::AccountState::Frozen;
        state.pack_base();
        assert!(!receiver_is_ready(&account, &receiver));
    }

    fn fixture() -> (Settlement, Vec<PositionOutcome>) {
        let market = Pubkey::new_unique();
        let settlement = Settlement {
            rpc: Arc::new(RpcClient::new_mock("succeeds".to_owned())),
            payer: Arc::new(Keypair::new()),
            market,
            resolver: AccountResolver::new(twob_anchor::ID),
            base: receiver(spl_token::ID).token,
            quote: receiver(spl_token_2022::ID).token,
            config: SettlementConfig {
                max_batch_size: 32,
                compute_unit_limit: 100_000,
            },
            pending: Mutex::new(HashMap::new()),
        };
        let outcomes = (0..10)
            .map(|_| PositionOutcome {
                address: Pubkey::new_unique(),
                slot: 1760,
                disposition: Disposition::Retry,
                position: Some(TradePosition {
                    market,
                    payer: Pubkey::new_unique(),
                    base_receiver: Pubkey::new_unique(),
                    quote_receiver: Pubkey::new_unique(),
                    last_update_slot: 1000,
                    remaining_slots: 100,
                    ..Default::default()
                }),
            })
            .collect();
        (settlement, outcomes)
    }

    #[test]
    fn packet_sizing_batches_real_closes_without_creating_accounts() {
        let (settlement, outcomes) = fixture();
        let batch: Vec<_> = (0..outcomes.len()).collect();
        let count = settlement
            .fitting_prefix(&batch, &outcomes, 1760, Hash::default())
            .unwrap();
        assert!(count >= 2, "must actually batch transactions");
        assert!(count < batch.len());
        let transaction = settlement
            .transaction(&batch[..count], &outcomes, 1760, Hash::default())
            .unwrap();
        assert!(bincode::serialized_size(&transaction).unwrap() <= PACKET_BYTES);
        let oversized = settlement
            .transaction(&batch[..count + 1], &outcomes, 1760, Hash::default())
            .unwrap();
        assert!(bincode::serialized_size(&oversized).unwrap() > PACKET_BYTES);
        assert_eq!(transaction.message.instructions.len(), count + 1);
        for instruction in &transaction.message.instructions[1..] {
            assert_eq!(
                transaction.message.account_keys[instruction.program_id_index as usize],
                twob_anchor::ID
            );
            assert_eq!(
                &instruction.data[..8],
                &args::PublicCloseTradePosition {
                    reference_index: 10
                }
                .data()[..8]
            );
        }
        transaction.verify().unwrap();
    }

    #[test]
    fn compute_budget_and_failed_batch_splitting_are_bounded() {
        let (mut settlement, outcomes) = fixture();
        settlement.config.compute_unit_limit = 800_000;
        assert_eq!(
            settlement
                .fitting_prefix(&[0, 1], &outcomes, 1760, Hash::default())
                .unwrap(),
            1
        );
        let mut queue = VecDeque::new();
        split_failed(vec![0, 1, 2], &mut queue);
        assert_eq!(queue.pop_front(), Some(vec![0]));
        assert_eq!(queue.pop_front(), Some(vec![1, 2]));
        split_failed(vec![0], &mut queue);
        assert!(queue.is_empty());
    }

    #[test]
    fn fresh_interval_references_and_native_receivers_use_correct_pdas() {
        let (mut settlement, outcomes) = fixture();
        settlement.base.mint = spl_token::native_mint::ID;
        let outcome = &outcomes[0];
        let position = outcome.position.as_ref().unwrap();
        let before = settlement
            .close_instruction(&outcome.address, position, 1759)
            .unwrap();
        let after = settlement
            .close_instruction(&outcome.address, position, 1760)
            .unwrap();
        assert_eq!(
            before.data,
            args::PublicCloseTradePosition { reference_index: 9 }.data()
        );
        assert_eq!(
            after.data,
            args::PublicCloseTradePosition {
                reference_index: 10
            }
            .data()
        );
        assert_ne!(before.accounts[14].pubkey, after.accounts[14].pubkey);
        let receivers = settlement.receivers(&outcome.address, position);
        assert_eq!(
            receivers[0].address,
            Pubkey::find_program_address(&[outcome.address.as_ref()], &twob_anchor::ID).0
        );
        assert_eq!(
            receivers[1].address,
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &position.quote_receiver,
                &settlement.quote.mint,
                &settlement.quote.program
            )
        );
    }
}
