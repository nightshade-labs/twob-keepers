# Bookkeeper Grafana Alloy collector

This service scrapes both mainnet bookkeepers, the independent on-chain
canary, the market maker's process metrics, and its confirmed-flow observer
every 10 seconds, then sends their Prometheus metrics to Grafana Cloud.
The target addresses and instance labels are defined in `config.alloy`;
Grafana Cloud credentials are supplied only as runtime environment variables.

## Required Railway variables

```text
PORT=12345
GRAFANA_CLOUD_PROMETHEUS_URL=https://prometheus-<region>.grafana.net/api/prom/push
GRAFANA_CLOUD_PROMETHEUS_USERNAME=<metrics-instance-id>
GRAFANA_CLOUD_API_TOKEN=<access-policy-token-with-metrics-write>
MARKET_MAKER_METRICS_ADDRESS=${{cca-market-maker.RAILWAY_PRIVATE_DOMAIN}}:8080
MAKER_MONITOR_METRICS_ADDRESS=${{maker-monitor.RAILWAY_PRIVATE_DOMAIN}}:8080
```

Do not commit the API token. In Grafana Cloud, find the remote-write URL and
username under the stack's Prometheus details. Create an access-policy token
with permission to write metrics and store it as a sealed Railway variable.

## Railway service settings

1. Create a new service from this repository and name it `bookkeeper-alloy`.
2. Set its root directory to `/monitoring/alloy` so Railway detects this
   directory's `Dockerfile`.
3. Add the Grafana Cloud variables and both Railway target references above,
   and set `PORT=12345`.
4. Set the health-check path to `/-/healthy`.
5. Attach a small persistent volume at `/var/lib/alloy/data` to preserve the
   remote-write WAL across restarts.
6. Deploy one replica. Multiple independent replicas would scrape and write the
   same series, so don't scale this service horizontally without configuring
   Alloy clustering.

The canary target is reached privately at
`bookkeeper-canary.railway.internal:8080`. The primary and backup bookkeepers
are reached at `bookkeeper.railway.internal:8080` and
`bookkeeper-backup.railway.internal:8080`. All scrape targets and this collector
must be in the same Railway project and environment. Deploy the canary before
redeploying Alloy; see `../../docs/bookkeeper-canary.md`.

Before deploying these maker targets, deploy the market maker telemetry with
`MAKER_METRICS_BIND_ADDR=[::]:8080` on `cca-market-maker`, and deploy the separate
read-only `maker-monitor` service with `MAKER_MONITOR_BIND_ADDR=[::]:8080` in the
same Railway project and environment. The target variables above must be Railway
service references, so private DNS follows the actual service names. The observer
also needs its RPC endpoint and market address; see the
[market maker monitoring runbook](https://github.com/Nachtschatten-Labs/cca-market-maker/blob/main/monitoring/README.md)
for the full deployment prerequisites, metric meanings, and Grafana alert rules.
These scrape blocks reuse the existing Grafana Cloud receiver and its
`environment="mainnet"` and `collector="railway-alloy"` labels.

A public domain is optional. If one is enabled, these endpoints are useful:

| Path | Purpose |
| --- | --- |
| `/-/healthy` | All Alloy components loaded without configuration errors |
| `/-/ready` | Initial configuration is loaded |
| `/metrics` | Alloy's own operational metrics |

## Verify ingestion

After deployment, open Grafana Cloud **Explore**, select the Prometheus data
source, and run:

```promql
up{job="bookkeeper"}
```

There must be exactly two series, both with value `1`, for instances
`mainnet-market-1` and `mainnet-market-1-backup`. Then verify the application
metric and its thresholds:

```promql
bookkeeper_lag_slots{cluster="mainnet", market_address="<market-address>"}
bookkeeper_warning_lag_slots{cluster="mainnet", market_address="<market-address>"}
bookkeeper_critical_lag_slots{cluster="mainnet", market_address="<market-address>"}
```

Finally, verify that Alloy can scrape the independent canary and that the
canary has completed a recent on-chain observation:

```promql
up{job="bookkeeper-canary"}
bookkeeper_chain_lag_slots{cluster="mainnet", market_address="<market-address>"}
time() - bookkeeper_chain_last_observation_timestamp_seconds{cluster="mainnet", market_address="<market-address>"}
```

Verify both maker endpoints return `up=1`, the process has completed a cycle,
and the independent confirmed observation is recent:

```promql
up{job=~"market-maker|maker-monitor"}
time() - market_maker_last_successful_cycle_timestamp_seconds{live="true"}
time() - market_maker_chain_last_observation_timestamp_seconds
market_maker_chain_flows_active
```

A timestamp of zero means no successful cycle or observation has occurred yet.
Missing flow metrics mean the observer has not obtained a validated account;
they must not be treated as confirmed zero flows. The monitoring runbook covers
stale-data alerts and durable maker stop events between scrapes.
