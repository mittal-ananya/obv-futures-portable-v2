# OBVFUTPORT-v2 Memory Optimization Plan

## Current State

The Aug24 EOD path is now memory-safe when run symbol-incrementally. The broad replay path should not be used as the default daily runner because it can hold too much multi-symbol state at once.

Live-session memory growth is improved but not structurally closed. On 2026-08-27,
`cloud-obvfutport-v2-passive.service` was OOM-killed during the post-EOD window
with about 7.95 GB anonymous RSS, then climbed back near 9 GB RSS after restart.
The final EOD audit still passed; the issue is operational memory retention and
service lifecycle pressure, not replay-state corruption. After the final audit,
live-only v2 services were stopped overnight and available memory recovered to
about 13 GB.

The main contributors are:

- active-position second-level row retention for T1/T2/T3/risk updates;
- wider lifecycle/shadow retention around rollover days;
- exact OBV percentile history retained for parity-safe z/percentile computation;
- service status files written by root-owned systemd services.

## Already Applied

- Daily EOD default is `scripts/run_symbol_incremental_eod_append.py`.
- Flat symbols use bounded second-row retention.
- Pending/active/transition/shadow lifecycle retention is separated.
- Stale open positions are ignored for live retention, so dead carry rows do not keep widening memory.
- Watchdog checks v2 service health, stream freshness, restarts, OOMs, disk/log pressure, and stale-entry diagnostics.
- After-close mitigation now stops live-only v2 runner/stream/watchdog after EOD
  install, dashboard rebuild, Matrix rebuild, and post-install audit pass.

## Durable Fixes Still To Build

1. Replace active-position row retention with compact per-position state plus small ring buffers.
2. Maintain MFE/MAE, hard SL, trail, T2, and T3 state incrementally instead of retaining full active paths.
3. Add a parity-proven compact percentile/quantile structure for live OBV percentiles while keeping exact archive replay as the EOD oracle.
4. Add per-service memory slope alerts, not only absolute cap alerts.
5. Require RCA before any future memory-cap increase.

## Operating Rule

Do not solve recurring memory growth by repeated cap increases. Increase caps only as a temporary safety valve, then record the cause and either patch retention or add a measured follow-up.
