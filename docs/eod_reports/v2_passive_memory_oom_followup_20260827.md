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

The daily EOD automation was also updated so future EOD runs stop only the v2
live-runner, v2 target-stream, and v2 live-watchdog services before replay once
the market is closed. Dashboard, Matrix, and Matrix bridge stay active. This
prevents the EOD append/rebuild path from competing with the live passive
runner's retained memory.

## Classification

This is an operational memory-retention issue, not an EOD replay-integrity
failure. The final Aug27 EOD audit passed and the consolidated post-EOD baseline
was preserved.

## Hardening Applied

1. Added watchdog passive-memory soft-limit alerts at 8 GB warning and 10 GB
   critical defaults. These sit below the temporary 12 GB systemd override.
2. Added watchdog per-service memory growth-rate alerts: warning at 64 MB/min
   and critical at 128 MB/min over a sustained sample window.
3. Updated the daily EOD automation with a v2-only pre-replay service stop rule
   after market close.
4. Added targeted regression coverage for the watchdog memory-slope alert.

## Remaining Follow-Up

1. Continue the structural retention work already listed in
   `docs/memory_optimization_plan.md`: compact active-position state, compact
   MFE/MAE/trail state, and parity-proven compact OBV percentile state.
2. Do not increase the passive service memory cap again without a written RCA.

## Next-Day Readiness Gate

Tomorrow morning should verify:

- future-chain refresh fires cleanly at 08:52 IST;
- passive starts cleanly at 08:55 IST and pulls the target stream;
- stream target-key count and runner target-key count match after warm-up;
- no stale accepted entries, no raw fallback, no unexpected v1 strategy worker;
- Matrix active selected-leg state remains aligned with v2 ledgers.
- watchdog has no `passive_memory_soft_limit` or
  `service_memory_growth_critical` alert after warm-up.
