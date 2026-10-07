# Trade keeper

The trade keeper closes eligible positions for one `MARKET_ADDRESS`. Run a
bookkeeper for that market too: public settlement updates bookkeeping but cannot
repair a market that has fallen more than one interval behind.

## Discovery and scheduling

The keeper subscribes to the program's accounts using the `TradePosition`
discriminator and market address (byte offset 40). This delivers new positions,
pauses, resumes, withdrawals, and receiver changes without fetching each
transaction or subscribing separately to every position. It requests a full
snapshot after subscribing, after reconnecting, and every five minutes. If the
provider rejects the subscription, snapshots continue to discover positions.

The snapshot includes its confirmed bank slot. Older queued notifications cannot
overwrite it, and a snapshot cannot overwrite a newer position update. Filtered
subscriptions may omit closed accounts after their discriminator disappears;
reconciliation and the pre-settlement account refresh remove these entries.

Each position has one local timer:

- Active: `last_update_slot + remaining_slots`.
- Paused: `start_slot + MAXIMUM_DURATION_SLOTS + 1`, even if its frozen end slot
  lies further in the future. The extra slot implements the program's strict
  abandonment comparison. `MAXIMUM_DURATION_SLOTS` is currently 160,000,000.

The keeper estimates the wall-clock deadline at 400 ms per slot, then adds a
five-second batching window. Slot timing is an estimate, never authorization to
close: it rereads due positions with confirmed `getMultipleAccounts` and checks
their eligibility against that response's actual slot. A slower chain reschedules
the timer; snapshots can bring estimates forward when the chain runs faster.
There is no continuous `getSlot` polling while waiting for a deadline.

When the first deadline fires, the pass also includes other positions estimated
to have ended during that window. Reconciliation may drain an already-overdue
backlog immediately without waiting another window.

Up to 32 due positions are handled per pass. Pauses, resumes, and receiver changes
replace their existing timer. Subscription messages continue to be consumed
during settlement, and newer updates take precedence over older HTTP outcomes.

## Existing receiving accounts only

Standard `getProgramAccounts` filters compare bytes within the position account.
They cannot join it to accounts owned by a token program, test whether an ATA
exists, or filter by a computed end-slot inequality. Missing receivers therefore
cannot be excluded from the server's search using the current position layout.
See the official [getProgramAccounts](https://solana.com/docs/rpc/http/getprogramaccounts)
and [programSubscribe](https://solana.com/docs/rpc/websocket/programsubscribe) interfaces.

Instead, after selecting due positions locally, the keeper derives both receiving
ATAs from the position's **stored receivers**, mint, and token program. It
deduplicates addresses and fetches them in
[getMultipleAccounts](https://solana.com/docs/rpc/http/getmultipleaccounts) chunks
of at most 100. It checks the account's token program, mint, wallet authority, and
initialized/non-frozen state, including Token-2022 account extensions. It checks
both sides because settlement can return unspent input as well as swapped output.

Positions with missing or invalid receivers leave the ready queue for five
minutes. Unchanged snapshots preserve that delay; a receiver change makes the
position eligible for a fresh check. If the user creates the missing ATA, a later
retry finds it. RPC failures use the shorter error retry and are not interpreted
as missing accounts. There is no global token-account subscription or permanent
exclusion of a position.

Transactions contain only compute-budget and `PublicCloseTradePosition`
instructions. The current public-close program does **not** create non-native
receiving ATAs, so an account disappearing between validation and execution can
fail a transaction but cannot charge the keeper for creating the user's account.

Native SOL uses the program's temporary position-derived token account instead
of a receiver ATA. Its rent is advanced by the keeper and returned to the keeper
within the same instruction, while only the payout goes to the receiver. This
requires a temporary balance but has no net rent expense. Position rent is
returned to the position's original payer. The keeper still pays transaction
fees, including fees for any failed transactions that land.

## Batching and failure recovery

Ready positions share a transaction when possible. The default target is four
positions, but exact signed serialization must fit Solana's 1,232-byte packet
limit, and the sum of compute allowances must not exceed 1,400,000 units. Different
receivers and interval accounts can reduce the actual batch size. The default
allowance is 100,000 units per close; no priority fee is added.

The keeper fetches a blockhash and its confirmed bank slot together for each batch, derives its
current/previous interval accounts, and submits with preflight enabled. This
simulates before broadcasting without a second, duplicate simulation RPC.
Definitive preflight failures split the batch to isolate a bad position.
Transport errors defer work instead of splitting into more RPC
traffic. Confirmations are checked together with a bounded wait. Successful
confirmed closures leave the queue; unknown or failed submissions are refreshed
on a later pass. An uncertain submission retains its signature and last valid
block height in memory. The keeper checks pending signatures together and will
not sign a replacement for those positions until a confirmed failure or blockhash
expiry; a wall-clock timeout alone does not permit another transaction. After
observing expiry, the keeper waits for another position refresh before signing.
If another
actor already closed a position, the account refresh removes it. Interval-boundary
races and stale bookkeeping are retried with fresh data.

Only one settlement pass runs at a time. Ctrl-C stops the process; transactions
already submitted may still land. Restarting reconstructs all state from chain.
The pending-signature guard is in memory, so restarting during an uncertain send
can lose that guard; account validation and on-chain close checks still apply.

## RPC budget and configuration

In steady state the full scan falls from twelve per minute in the previous
implementation to one every five minutes: **60 times fewer full scans**. A healthy
subscription supplies intervening changes. No eligible positions means no
receiver lookups or transaction submissions. Due work costs one
bulk position refresh per pass, bulk receiver reads, and the contextual blockhash,
send (including preflight), and confirmation calls required by its actual transaction
batches. Unresolved submissions also require signature-status and block-height
checks before a replacement can be signed. Provider WebSocket/response-byte
pricing still matters.

Required: `CLUSTER_RPC_URL`, `CLUSTER_WS_URL`, `MARKET_ADDRESS`, and
`PAYER_KEYPAIR` (JSON keypair bytes). Optional tuning:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `TRADE_KEEPER_BATCH_WINDOW_MS` | 5000 | Delay after estimated eligibility to collect due work |
| `TRADE_KEEPER_RECONCILE_INTERVAL_MS` | 300000 | Full snapshot interval and discovery fallback |
| `TRADE_KEEPER_MISSING_RECEIVER_RETRY_MS` | 300000 | Retry delay for missing or invalid receiving accounts |
| `TRADE_KEEPER_RETRY_DELAY_MS` | 15000 | Position error retry; initial discovery backoff |
| `TRADE_KEEPER_ESTIMATED_SLOT_DURATION_MS` | 400 | Wall-clock scheduling estimate only |
| `TRADE_KEEPER_MAX_BATCH_SIZE` | 4 | Maximum closes per transaction; packet size can reduce this |
| `TRADE_KEEPER_COMPUTE_UNIT_LIMIT` | 100000 | Compute-unit allowance per close |

All values must be positive. Durations are capped at one day, slot duration at
10 seconds, batch size at 16, and compute allowance at 1,400,000. Discovery errors
back off exponentially up to the larger of the reconciliation interval and retry
delay; WebSocket reconnects back off from one to 30 seconds. Logs report snapshot
counts, receiver deferrals, transaction failures, and confirmed close signatures.

These changes require no on-chain program or database migration. Tests use
synthetic account data and mocked RPC responses; production transactions are not
sent by the test suite.
