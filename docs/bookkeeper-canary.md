# Bookkeeper chain canary

The `bookkeeper-canary` is a read-only service that independently observes the
mainnet market and bookkeeping accounts. It does not load a payer keypair, sign
transactions, or submit anything to Solana.

For mainnet market 1 it verifies:

- the RPC reports the mainnet genesis hash;
- the market and bookkeeping PDAs exist and are owned by the configured program;
- both accounts have valid Anchor discriminators and can be decoded;
- the decoded market ID is 1;
- the freshness boundary can be computed from the on-chain market account.

## Create the Railway service

1. In the same Railway project and production environment as
   `bookkeeper-alloy`, choose **New → GitHub Repo** and select this repository.
2. Name the service `bookkeeper-canary`. The name matters because Alloy reaches
   it at `bookkeeper-canary.railway.internal`.
3. Keep the root directory `/` so Railway uses the repository's root
   `Dockerfile`.
4. Add the variables below. Set `BIN_NAME` as a build variable if Railway
   provides a separate build-time toggle; the root Dockerfile declares it as an
   `ARG`.

```text
BIN_NAME=bookkeeper-canary
PORT=8080
BOOKKEEPER_CANARY_RPC_URL=<independent-mainnet-rpc-url>
BOOKKEEPER_CANARY_CLUSTER=mainnet
BOOKKEEPER_CANARY_PROGRAM_ID=CCAmAqvza37EWzou7LoYCaGKzdJsCu1CLPMp3Wvx3Bc5
BOOKKEEPER_CANARY_MARKET_ID=1
BOOKKEEPER_CANARY_EXPECTED_GENESIS_HASH=5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d
BOOKKEEPER_CANARY_POLL_INTERVAL_MS=10000
BOOKKEEPER_CANARY_STALE_AFTER_MS=45000
```

Use an RPC endpoint from a different provider or account than the endpoints
used by the two transaction-sending bookkeepers. Do not use the repository's
current `CLUSTER_RPC_URL` for this mainnet canary: its genesis hash identifies a
different Solana cluster. Solana's public mainnet endpoint can be used for a
short smoke test, but use a dedicated endpoint for production monitoring.

5. Set the Railway health-check path to `/readyz` and its timeout to at least
   60 seconds.
6. Deploy one replica. The service needs no volume and no public domain.

The canary exits before opening its HTTP server if the RPC genesis hash is not
mainnet. After startup, `/readyz` returns HTTP 503 if no successful on-chain
observation has occurred within 45 seconds.

## Connect Alloy

The Alloy configuration scrapes the canary through Railway private networking:

```text
http://bookkeeper-canary.railway.internal:8080/metrics
```

After the canary is healthy, redeploy `bookkeeper-alloy` from the version that
contains the canary scrape target. Then verify these queries in Grafana Explore:

```promql
up{job="bookkeeper-canary"}
bookkeeper_chain_lag_slots{cluster="mainnet", market_id="1"}
bookkeeper_chain_warning_lag_slots{cluster="mainnet", market_id="1"}
bookkeeper_chain_critical_lag_slots{cluster="mainnet", market_id="1"}
time() - bookkeeper_chain_last_observation_timestamp_seconds{cluster="mainnet", market_id="1"}
```

Expected values are one `up` series with value `1`, a warning threshold of 49,
and a critical threshold of 70. The lag changes as transactions land and slots
advance.

## Exposed endpoints

| Path | Meaning |
| --- | --- |
| `/livez` | The HTTP process is alive. |
| `/readyz` | A valid on-chain observation was completed recently. |
| `/metrics` | Prometheus/OpenMetrics output for Alloy. |
