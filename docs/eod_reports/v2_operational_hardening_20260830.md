# OBVFUTPORT-v2 Operational Hardening - 2026-08-30

Scope: v2/Matrix/cloud health only. No v1, Compass v1, dedicated Nifty ONNFOBVLSFT/v51, strategy-parameter promotion, full replay, or destructive production-state cleanup was performed.

## Completed

1. Weekly recalibration gate blocker prepared for Sep 4
   - Fixed scorer entry-fill boundary parity so the scorer uses arrival-aware quote selection at the entry due boundary.
   - This prevents future-arriving same-second quotes from creating scorer-vs-replay fill mismatches such as the OFSS case.
   - Weekly promotion itself remains deferred to Friday 2026-09-04.

2. Late stale-entry materialization hardening
   - Added a pre-fill guard that rejects overdue pending entries during market hours before they can become live open positions.
   - Rejected entries are preserved as `entry_signal_skipped` diagnostics with `stale_pending_entry_at_evaluation` and `suppress_downstream=true`.

3. Passive memory retention hardening
   - Added memory-pressure retention control.
   - Default soft pressure threshold: 8192 MB RSS.
   - Under pressure, shadow-lifecycle retention is capped at 900 seconds while active/pending position retention is left unchanged.
   - Added status visibility through `latest_memory_pressure_report` and retention-trim counters.

4. Feed/cadence timing visibility
   - Added watchdog session classification: `pre_open`, `market_data_expected`, `post_close`, or `weekend`.
   - Fixed watchdog live-market checks so weekend/post-close stale prior-day feed and service state do not emit false live-market criticals.
   - Added current-session scoping for stale-open-position and target-key mismatch alerts so a prior trading day's status cannot trigger false pre-open readiness failures before the new session state is written.

5. Install preflight guard
   - Added write-access preflight before v2 reseed installs begin backup/install work.
   - A file ownership issue such as the prior `M_M/ledger.jsonl` case should now fail before partial install work starts.

6. DALBHARAT baseline/universe reconciliation guard
   - Added baseline-only symbol preservation in reseed-root preparation using the prior verified EOD source when available.
   - If a baseline-held symbol is absent from current universe and cannot be preserved, the script fails unless `--allow-baseline-symbol-drop` is explicitly passed for a deliberate baseline transition.

7. Disk cleanup
   - Archived metadata/checksum bundles before deletion.
   - Deleted old Aug27 EOD temp root after Aug28 verified baseline superseded it.
   - Deleted remaining explicit v1-only append temp workspaces.
   - Deleted old non-production smoke/harness cache roots after process and targeted reference checks.
   - Disk improved to about 91% used with about 31 GB free.

## Validation

Local focused tests: 45 passed.

Cloud direct smoke checks passed:
- scorer boundary fill
- stale pending rejection
- memory-pressure shadow retention cap
- watchdog weekend/live-market gate
- install preflight
- baseline/universe reconciliation

Cloud isolated five-symbol Aug28 smoke gate passed under production Python:
- Symbols: OFSS, BHARATFORG, HAL, LICI, MIDCPNIFTY
- mismatch_count: 0
- impossible_t3_rows: installed=0, scored=0
- Report: `/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/reports/ofss_boundary_five_symbol_smoke_gate_20260830.json`

Cloud health after work:
- v2 passive: inactive for weekend
- v2 target-stream: inactive for weekend
- v2 watchdog: active and clean
- latest status trade date is correctly classified as prior-session telemetry outside live readiness
- Matrix service/bridge: active
- no v1 strategy worker process observed
- load acceptable, available memory about 13 GiB, disk about 73% used / 85 GB free

## Deferred

Weekly recalibration promotion is intentionally deferred to Friday 2026-09-04. The gate blocker is fixed and smoke-tested, but no promotion was run today.
