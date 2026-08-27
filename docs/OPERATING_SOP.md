# OBVFUTPORT V2 Operating SOP

This package is OBVFUTPORT-v2 only. It must not mutate OBVFUTPORT-v1,
Compass v1, or the dedicated Nifty ONNFOBVLSFT/v51 package.

## Morning Readiness

Before market open:

- Confirm the shared/owned target stream is active and writing quote-valid rows.
- Confirm the v2 passive service is active only when intended for the session.
- Confirm the dashboard and Matrix services are active.
- Confirm the active adaptive override version and quarantined-symbol list.
- Confirm all 212 symbols are represented and the runner target-key count matches
  the owned target-stream key count after the stream starts. Do not hard-code the
  key count across rollover/lifecycle modes.
- Confirm there are no raw rebuilds, stale compact states, or missed readiness gates.
- Check CPU, memory, disk, and log growth.
- Watch the 09:20 and 09:35 decision windows for stale entries, missed_not_ready,
  stream lag, or unexpected queue buildup.
- During live hours, treat watchdog `decision_catchup_deferred`,
  `missed_not_ready`, and `stale_open_positions` alerts as immediate RCA items.
  They expose timing/visibility failures before stale entries can be mistaken
  for strategy behavior.

## Daily EOD

Daily EOD is incremental by default:

- Use `scripts/run_symbol_incremental_eod_append.py` for the daily append path.
  The broad multi-symbol replay path is not the default because it was proven
  memory-heavy on Aug 24; per-symbol append keeps cloud memory bounded and gives
  symbol-level retry/reporting.
- Append only the new trade date's quote-valid compact stream/archive-canonical input.
- Update v2 ledgers, decision events, open T1/T2/T3 positions, tranche state,
  margins, summaries, dashboard state, and Matrix state.
- Compare live v2 ledger against replay v2 ledger for missing/extra entries,
  missing/extra exits, stale tags, signal-vs-entry timing, fill/price differences,
  open-position differences, T2/T3 state differences, PnL/margin differences,
  Matrix mismatches, and dashboard summary gaps.
- Treat stale live entries as an execution defect. Corrected replay must remove
  stale `paper_entry` rows from installed state and preserve rejected entries as
  explicit `stale_entry_rejected` diagnostics with symbol and reason.
- Include any live watchdog timing alerts in the EOD RCA with the latest compact
  decision report, stream catch-up reason, and whether Matrix accepted or skipped
  the impacted event.
- Rebuild Matrix from the corrected selected-leg ledger using the bridge's
  historical `--once` mode after reset; the service tail mode intentionally skips
  historical rows and is not sufficient for EOD rebuilds.
- Pin historical research/recalibration proof runs with `--contract-as-of-iso`
  when replaying a pre-rollover date range after rollover has already occurred.
- Generate a state manifest after every install/update.
- After EOD install, dashboard rebuild, Matrix rebuild, and post-install audit
  pass, stop live-only v2 runner/stream/watchdog services for the overnight
  period unless an explicit live-monitor task is still active. Keep dashboard and
  Matrix available. The next market-day timers start the live path again.
- Do not run a full Aug10 onward replay unless the user explicitly approves after
  a detailed explanation.

## Friday Weekly Recalibration

On the last trading day of each week:

- Re-score T1/T2/T3 candidates incrementally against the active adaptive baseline.
- Promote only if the new candidate improves the agreed risk-first hierarchy:
  lower worst loss/drawdown, better or acceptable success rate, acceptable net
  return, and no obvious one-trade distortion.
- Version promoted overrides and keep a rollback pointer.
- Rebuild dashboard/Matrix only if a promoted version changes historical state.
- Keep quarantined symbols excluded until their focused proof passes.

## Git Discipline

Commit scripts, runtime templates, candidate override versions, manifests, and
SOP files. Do not commit heavy compact streams, extracted tick archives, bulky
ledgers, or dashboard data. Store hashes, row counts, paths, and date ranges for
heavy artifacts instead.

## Quarantined Symbols

Current quarantined cleanup plan:

- Keep IOC, MAXHEALTH, and WAAREEENER on current v2 baseline until their focused
  proof and state install are explicitly completed.
- Patch duplicate-second ordering once in the indexed execution path, then test
  only IOC and MAXHEALTH.
- WAAREEENER proof passed on 2026-08-24 after adding fill-bound validation; it
  still needs a controlled WAAREEENER-only state/dashboard/Matrix install before
  leaving baseline in production.
- Do not rerun successful symbols unless the patch changes shared candidate
  semantics.
