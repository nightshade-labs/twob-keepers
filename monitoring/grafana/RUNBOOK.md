# Bookkeeper monitoring runbook

This runbook covers mainnet market 1 for program
`CCAmAqvza37EWzou7LoYCaGKzdJsCu1CLPMp3Wvx3Bc5`.

For the system topology and the relationship between the two workers, canary,
Alloy, Grafana Cloud, and Telegram, see
[`docs/bookkeeping-service-architecture.md`](../../docs/bookkeeping-service-architecture.md).

## Production topology

All services run in the same Railway project and production environment.

| Railway service | Role | Cadence / endpoint |
| --- | --- | --- |
| `bookkeeper` | Primary transaction-sending worker | 40 slots, port 8080 |
| `bookkeeper-backup` | Backup transaction-sending worker | 45 slots, port 8080 |
| `bookkeeper-canary` | Read-only independent mainnet observer | Polls every 10 seconds, port 8080 |
| `bookkeeper-alloy` | Prometheus scraper and Grafana Cloud remote writer | Scrapes every 10 seconds, port 12345 |

Alloy reaches the services through Railway private networking:

```text
bookkeeper.railway.internal:8080
bookkeeper-backup.railway.internal:8080
bookkeeper-canary.railway.internal:8080
```

The expected Grafana instances are:

```text
mainnet-market-1
mainnet-market-1-backup
mainnet-market-1-chain
```

## Freshness boundaries

The canary derives the critical boundary from the on-chain market account as
`end_slot_interval * ARRAY_LENGTH`. For mainnet market 1:

```text
Informational warning boundary: 49 slots
Critical boundary:             70 slots
```

The 49-slot boundary is shown on the dashboard but does not send a freshness
notification. Freshness notifications are critical-only.

## Alert routing

Grafana notification-policy matcher:

```text
service = bookkeeper
```

The matching route sends notifications to the Bookkeeper Telegram contact
point. All production rules use these labels:

```text
service=bookkeeper
environment=mainnet
market_id=1
```

Rules use the `bookkeeper-mainnet` evaluation group with a 10-second interval.

## Production alert inventory

### Bookkeeper freshness critical

Fires immediately when the independent chain lag reaches the dynamic on-chain
critical boundary, currently 70 slots.

```promql
max(bookkeeper_chain_lag_slots{cluster="mainnet", market_id="1"})
```

Compared with:

```promql
max(bookkeeper_chain_critical_lag_slots{cluster="mainnet", market_id="1"})
```

Configuration:

```text
severity=critical
pending=0s
keep_firing_for=1m
no_data=Normal
error=Keep Last State
```

### Bookkeeper telemetry stale

Fires when the independent observation age reaches 45 seconds. No-data and
query-error states also alert, covering canary, Alloy, remote-write, and
Grafana data-source failures.

```promql
time() - max(
  bookkeeper_chain_last_observation_timestamp_seconds{
    cluster="mainnet",
    market_id="1"
  }
)
```

Configuration:

```text
condition >= 45
severity=critical
pending=0s
keep_firing_for=1m
no_data=Alerting
error=Alerting
```

### Bookkeeper redundancy lost

Fires when fewer than two bookkeepers are scrapeable and progressing within
their planned activity deadline plus the 15-second grace period.

```promql
sum(
  (up{
    job="bookkeeper",
    instance=~"mainnet-market-1(-backup)?"
  } == bool 1)
  * on(instance)
  (
    time() -
    bookkeeper_next_expected_activity_timestamp_seconds{
      cluster="mainnet",
      market_id="1"
    } <= bool 15
  )
) or vector(0)
```

Configuration:

```text
condition < 2
severity=critical
pending=20s
keep_firing_for=1m
no_data=Alerting
error=Alerting
```

### Bookkeeper payer balance warning

Fires when the lowest reported payer balance is below 0.2 SOL but not below
0.1 SOL.

```promql
min(
  bookkeeper_payer_balance_lamports{
    cluster="mainnet",
    market_id="1"
  } / 1e9
)
```

```text
condition < 0.2 and >= 0.1
severity=warning
pending=0s
keep_firing_for=1m
no_data=Normal
error=Keep Last State
```

### Bookkeeper payer balance critical

Uses the same query and fires below 0.1 SOL.

```text
condition < 0.1
severity=critical
pending=0s
keep_firing_for=1m
no_data=Normal
error=Keep Last State
```

The balance is polled every five minutes by default. A refill can therefore
take approximately five minutes to appear and resolve an alert.

## First-response queries

Start every incident with the independent chain state:

```promql
bookkeeper_chain_lag_slots{cluster="mainnet", market_id="1"}
bookkeeper_chain_freshness_remaining_slots{cluster="mainnet", market_id="1"}
time() - bookkeeper_chain_last_observation_timestamp_seconds{
  cluster="mainnet",
  market_id="1"
}
```

Check all scrape targets:

```promql
up{job="bookkeeper"}
up{job="bookkeeper-canary"}
```

Check worker progress and balances:

```promql
bookkeeper_lag_slots{cluster="mainnet", market_id="1"}
time() - bookkeeper_next_expected_activity_timestamp_seconds{
  cluster="mainnet",
  market_id="1"
}
bookkeeper_payer_balance_lamports{cluster="mainnet", market_id="1"} / 1e9
```

Check recent failures and transaction outcomes:

```promql
sum by (instance, operation) (
  increase(bookkeeper_rpc_requests_total{
    cluster="mainnet",
    market_id="1",
    outcome="failure"
  }[15m])
)

sum by (instance, outcome) (
  increase(bookkeeper_transactions_total{
    cluster="mainnet",
    market_id="1"
  }[1h])
)
```

## Incident response

### Freshness critical

1. Confirm the canary observation age is current. If it is stale, follow the
   telemetry procedure instead of trusting the lag value.
2. Check how many bookkeepers are healthy and inspect each Railway service's
   latest logs.
3. Check payer balance and RPC failures.
4. Restart only an unhealthy replica. Never restart both bookkeepers at once.
5. Confirm `bookkeeper_chain_lag_slots` falls below 70 after a transaction is
   confirmed.
6. If both workers are healthy but transactions repeatedly fail, inspect the
   on-chain error, payer funding, RPC health, priority fees, and compute-unit
   simulation logs before changing configuration.

### Telemetry stale

1. Check `bookkeeper-canary` logs for genesis-hash, RPC, account-owner, or
   account-decoding errors.
2. Check `bookkeeper-alloy` logs for scrape failures, DNS errors, `401`
   responses, or remote-write queue/WAL errors.
3. Verify the canary and Alloy are in the same Railway project and environment.
4. Verify the Grafana Cloud token still has metrics-write permission.
5. Treat this as an observability outage until fresh canary samples resume;
   separately inspect the bookkeeper logs because their transactions may still
   be operating normally.

### Redundancy lost

1. Use `up{job="bookkeeper"}` to identify an unreachable replica.
2. Compare each replica's next expected activity time and Railway logs.
3. Restart or repair only the unhealthy replica while leaving the healthy one
   untouched.
4. Confirm the healthy-count query returns `2` before closing the incident.

### Low payer balance

1. Confirm the balance from a trusted Solana wallet or RPC source.
2. Refill the configured payer above 0.2 SOL with operational headroom.
3. Verify the payer address before sending funds; never copy or disclose the
   `PAYER_KEYPAIR` secret during diagnosis.
4. Wait for the next five-minute balance poll and confirm the alert resolves.

## Safe deployment procedure

For changes to the transaction-sending worker:

1. Deploy one bookkeeper first.
2. Confirm its Railway health check, logs, and Grafana metrics are healthy.
3. Wait for at least one normal loop or confirmed update.
4. Deploy the other bookkeeper.
5. Never intentionally stop or redeploy both replicas simultaneously.

The canary is read-only and can be deployed independently. Alloy has a volume
mounted at `/var/lib/alloy/data`; keep it attached so its remote-write WAL
survives restarts.

## Notification test

To test routing without stopping production services, create a temporary
Grafana-managed rule:

```promql
vector(1)
```

Set the alert condition to `$A == 1` and include
`service=bookkeeper`, `severity=critical`, and `test=true`. Verify Telegram
delivery, then delete the temporary rule. Never weaken a production threshold
to test notifications.
