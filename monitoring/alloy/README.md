# Bookkeeper Grafana Alloy collector

This service scrapes both mainnet bookkeepers every 10 seconds and sends their
Prometheus metrics to Grafana Cloud. The target addresses and instance labels
are defined in `config.alloy`; Grafana Cloud credentials are supplied only as
runtime environment variables.

## Required Railway variables

```text
PORT=12345
GRAFANA_CLOUD_PROMETHEUS_URL=https://prometheus-<region>.grafana.net/api/prom/push
GRAFANA_CLOUD_PROMETHEUS_USERNAME=<metrics-instance-id>
GRAFANA_CLOUD_API_TOKEN=<access-policy-token-with-metrics-write>
```

Do not commit the API token. In Grafana Cloud, find the remote-write URL and
username under the stack's Prometheus details. Create an access-policy token
with permission to write metrics and store it as a sealed Railway variable.

## Railway service settings

1. Create a new service from this repository and name it `bookkeeper-alloy`.
2. Set its root directory to `/monitoring/alloy` so Railway detects this
   directory's `Dockerfile`.
3. Add the three Grafana Cloud variables above and set `PORT=12345`.
4. Set the health-check path to `/-/healthy`.
5. Attach a small persistent volume at `/var/lib/alloy/data` to preserve the
   remote-write WAL across restarts.
6. Deploy one replica. Multiple independent replicas would scrape and write the
   same series, so don't scale this service horizontally without configuring
   Alloy clustering.

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
`mainnet-market-1-40` and `mainnet-market-1-45`. Then verify the application
metric and its thresholds:

```promql
bookkeeper_lag_slots{cluster="mainnet", market_id="1"}
bookkeeper_warning_lag_slots{cluster="mainnet", market_id="1"}
bookkeeper_critical_lag_slots{cluster="mainnet", market_id="1"}
```
