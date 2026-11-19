# ANT/USDC monitoring rollout — 2026-11-19

Branch `codex/ant-monitoring` starts from the deployed Alloy revision
`964db36e90681bc18d802052bd7fb7085308e072` so existing SOL scrape components remain intact.

The additional scrape targets cover two ANT bookkeepers, an independent chain
canary, the permissionless maker, its independent position observer, and the
oracle. A blackbox probe requests the oracle `/health` endpoint: it returns 503
when the source observation exceeds 30 seconds. The relay fetch timestamp alone
cannot distinguish a newly fetched stale upstream price.

All ANT samples carry market address
`H16ZeZee8yGw94kGVx4aEHmC8PwWjwuLuqsW2g8MK2Pz`. Maker samples also identify LP
`HtfJYkhttTPi7tXQvd87zuphrxfPTqBGsvBwjnKQ8jeE` and the deployed program. The
new observers require no signing key or volume. Bind them to `[::]:8080` for
Railway private networking and retain the existing Alloy remote-write credentials
and WAL volume.

Validated the complete config using the deployed Alloy version, v1.19.2:

```sh
alloy validate monitoring/alloy/config.alloy
```

Alert definitions and behavior tests live in the launch repository under
`runbooks/ant-usdc/monitoring/`. Activate alerts only after confirming current ANT
samples in Grafana. Existing SOL rules and notification routes are not replaced.
The trade keeper does not currently export an HTTP health/metrics endpoint; these
keeper rules cover the two bookkeeping workers and independent chain progress.
