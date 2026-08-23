# OBVFUTPORT V2 Operating SOP

This package is OBVFUTPORT-v2 only. It must not mutate OBVFUTPORT-v1,
Compass v1, or the dedicated Nifty ONNFOBVLSFT/v51 package.

## Morning Readiness

Before market open:

- Confirm the shared/owned target stream is active and writing quote-valid rows.
- Confirm the v2 passive service is active only when intended for the session.
- Confirm the dashboard and Matrix services are active.
- Confirm the active adaptive override version and quarantined-symbol list.
- Confirm all 212 symbols and 631 target keys become fresh after the stream starts.
- Confirm there are no raw rebuilds, stale compact states, or missed readiness gates.
- Check CPU, memory, disk, and log growth.
- Watch the 09:20 and 09:35 decision windows for stale entries, missed_not_ready,
  stream lag, or unexpected queue buildup.

## Daily EOD

Daily EOD is incremental by default:

- Append only the new trade date's quote-valid compact stream/archive-canonical input.
- Update v2 ledgers, decision events, open T1/T2/T3 positions, tranche state,
  margins, summaries, dashboard state, and Matrix state.
- Compare live v2 ledger against replay v2 ledger for missing/extra entries,
  missing/extra exits, stale tags, signal-vs-entry timing, fill/price differences,
  open-position differences, T2/T3 state differences, PnL/margin differences,
  Matrix mismatches, and dashboard summary gaps.
- Generate a state manifest after every install/update.
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

- Keep IOC, MAXHEALTH, and WAAREEENER on current v2 baseline until the end of
  the operational setup.
- Patch duplicate-second ordering once in the indexed execution path, then test
  only IOC and MAXHEALTH.
- Patch T3 pullback-short bounds, then test only WAAREEENER.
- Do not rerun successful symbols unless the patch changes shared candidate
  semantics.
