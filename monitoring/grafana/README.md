# Grafana Cloud dashboard

`bookkeeper-mainnet-dashboard.json` is an importable Grafana dashboard for
mainnet market 1. It covers independent chain freshness, both redundant
bookkeepers, scrape availability, payer balance, transaction outcomes, RPC
failures, and latency.

## Import

1. In Grafana Cloud, open **Dashboards**.
2. Click **New → Import**.
3. Upload `bookkeeper-mainnet-dashboard.json`, or paste its JSON.
4. Select the Grafana Cloud Prometheus data source receiving metrics from
   `bookkeeper-alloy` when prompted for `Prometheus`.
5. Keep the dashboard UID `bookkeeper-mainnet-market-1` and click **Import**.

The dashboard refreshes every 10 seconds and defaults to the last six hours.
Warning and critical visual thresholds are 49 and 70 slots. Alert rules are
configured separately so dashboard edits do not accidentally change paging.

## Expected targets

```promql
up{job="bookkeeper", instance=~"mainnet-market-1(-backup)?"}
up{job="bookkeeper-canary", instance="mainnet-market-1-chain"}
```

The first query must return two series with value `1`; the second must return
one series with value `1`.

See `RUNBOOK.md` for the production alert inventory, diagnostic queries, and
incident-response procedures.
