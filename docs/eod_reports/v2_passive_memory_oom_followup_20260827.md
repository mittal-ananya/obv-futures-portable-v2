# OBVFUTPORT-v2 Passive Memory / OOM Follow-Up - 2026-08-27

## Incident

During the Aug27 EOD window, `cloud-obvfutport-v2-passive.service` was killed by
the OOM killer around 19:15 IST. The killed process had about 7.95 GB anonymous
RSS. The service restarted, but after warm-up the passive runner again climbed
near 9 GB RSS. This happened after market close while EOD processing and service
rebuild work had recently run on the same host.

## Immediate Mitigation

After the Aug27 EOD install and audit passed, the live-only v2 services were
stopped for the overnight period:

- `cloud-obvfutport-v2-passive.service`
- `cloud-obvfutport-v2-target-stream.service`
- `cloud-obvfutport-v2-live-watchdog.service`

Dashboard and Matrix services were left active. The next-day timers remain
armed:

- future-chain refresh: 08:52 IST.
- passive start: 08:55 IST.

This reduced host memory pressure from roughly 5.2 GB available during the
post-restart warm state to roughly 13 GB available after the live-only services
were stopped.

## Classification

This is an operational memory-retention issue, not an EOD replay-integrity
failure. The final Aug27 EOD audit passed and the consolidated post-EOD baseline
was preserved.

## Required Follow-Up

1. Add memory-slope telemetry for the passive runner, not only absolute memory
   thresholds.
2. Add an after-close service lifecycle rule: once EOD install, Matrix rebuild,
   and audit pass, stop live-only v2 runner/stream/watchdog until the next
   market-day timers start them.
3. Continue the structural retention work already listed in
   `docs/memory_optimization_plan.md`: compact active-position state, compact
   MFE/MAE/trail state, and parity-proven compact OBV percentile state.
4. Do not increase the passive service memory cap again without a written RCA.

## Next-Day Readiness Gate

Tomorrow morning should verify:

- future-chain refresh fires cleanly at 08:52 IST;
- passive starts cleanly at 08:55 IST and pulls the target stream;
- stream target-key count and runner target-key count match after warm-up;
- no stale accepted entries, no raw fallback, no unexpected v1 strategy worker;
- Matrix active selected-leg state remains aligned with v2 ledgers.
