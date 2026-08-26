# OBVFUTPORT-v2 Aug26 Stale RCA

## Summary
- State rows audited: 2832.
- Stale diagnostics in cumulative ledgers through Aug26: 111.
- Accepted stale paper entries: 0.
- Entry signal skipped rows: 0.
- Replay treatment: diagnostic_only_excluded_from_real_transactions.
- Matching accepted paper entries by signal/position: 0 / 0.

## Stale By Due Date
- 2026-08-24: 66
- 2026-08-25: 24
- 2026-08-26: 21

## Stale By Symbol
- YESBANK: 33
- JSWENERGY: 27
- ADANIGREEN: 22
- KALYANKJIL: 14
- EICHERMOT: 2
- AMBER: 1
- DELHIVERY: 1
- DIVISLAB: 1
- FINNIFTY: 1
- HCLTECH: 1
- INFY: 1
- NBCC: 1
- NTPC: 1
- PIIND: 1
- PRESTIGE: 1
- SUNPHARMA: 1
- TVSMOTOR: 1
- VMM: 1

## Cause Classification
- startup_or_readiness_late_entry_rejected: 23
- retained_old_edge_rejected: 85
- minor_lag_rejected: 2
- decision_or_transition_lag_rejected: 1

## RCA
- Primary cause: Recovered Aug24-Aug26 chain preserved live/replay stale-entry diagnostics for old retained edges; strict max_live_entry_fill_lag_seconds=5 rejected every stale candidate from real positioning.
- Replay treatment: None of the stale diagnostics has a matching accepted paper_entry by signal_id or position_id; accepted stale paper_entry count is zero.
- Prevention already in place:
  - current active futures key included by futures-chain refresh when it differs from base manifest key
  - far-month keys excluded until lifecycle/shadow start
  - finite-history/passive startup guard patched
  - stale open positions excluded from live active loops by default
  - retained-window old edges filtered/rejected under strict 5-second live lag guard
  - Matrix full historical rebuild uses once-mode; tail bridge is only for live incremental sync
  - Dashboard has stale diagnostics and readiness/missing-clock diagnostics sections

## Baseline Continuity For Tomorrow
- Post-EOD baseline: `/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/eod_baselines/post_eod_20260826`
- Includes Aug24/Aug25/Aug26 event history: True
- Production/baseline event checksums match: True
- 2026-08-24: production 349 rows, baseline 349 rows
- 2026-08-25: production 273 rows, baseline 273 rows
- 2026-08-26: production 192 rows, baseline 192 rows

## Regression Checks
- post_install_audit_ok: True
- audit_failures: []
- t3_issue_count: 0
- matrix_instrument_count: 0
- matrix_event_count: 0
- matrix_stale_like_event_count: 0
- install_ok: True
- install_source_symbol_count: 212
- performance_report: /opt/cloud-deploy-candidates/obv-futures-portable-v2/state/reports/v2_tranche_performance_20260810_20260826.json

## Matrix Regression Correction
- Matrix state file: `/opt/cloud-deploy-candidates/matrix-v1/state/matrix_state.json`
- Matrix events file: `/opt/cloud-deploy-candidates/matrix-v1/state/matrix_events.jsonl`
- Matrix instruments: 212
- Matrix events: 2014
- Matrix active position symbols: 0
- Matrix active by leg: {}
- Matrix stale-like events: 0

## Matrix Active Count Correction
- Matrix active position symbols: 92
- Matrix active by leg: {"T2": 92}

## Dashboard Verification After Instrument Union Patch
- dashboard_snapshot_ok: True
- dashboard_instrument_count: 212
- dashboard_symbols_len: 212
- dashboard_has_DALBHARAT: True
- dashboard_stale_suppressed_entry_count: 21
- dashboard_readiness_miss_count: 28
- dashboard_missing_clock_count: 1
- dashboard_has_stale_entries_field: True
- dashboard_has_decision_miss_field: True
- dashboard_summary_open: 236
- dashboard_tranche_open: {'T1': 130, 'T2': 92, 'T3': 14}
- dashboard_created_at_utc: 2026-08-26T16:15:48+00:00

## Final Regression Metadata
- Post event-history repair audit: `/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/reports/v2_post_eod_aug26_after_event_history_repair_audit_20260826.json`
- py_compile_ok: true
- Dashboard instrument union patch: dashboard-only; strategy state/subscriptions unchanged.
