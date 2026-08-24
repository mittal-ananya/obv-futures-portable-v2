# OBVFUTPORT-v2 EOD Report - 2026-08-24

## Scope

V2-only EOD correction and install. OBVFUTPORT-v1, Compass v1, and dedicated
Nifty ONNFOBVLSFT/v51 were not mutated.

## Final Installed State

- v2 decision events for Aug 24: 342.
- v2 open positions after corrected replay: 69.
- stale `paper_entry` rows after install: 0.
- stale open positions after install: 0.
- explicit `stale_entry_rejected` diagnostics: 16.
- stale-rejected symbols: FINNIFTY 9; AMBER, DIVISLAB, EICHERMOT, JSWSTEEL,
  PIIND, PRESTIGE, TVSMOTOR 1 each.
- bootstrap readiness: 2026-08-24, 631/631 target keys, validation OK.

## Matrix

- Matrix rebuilt from corrected v2 selected-leg ledger.
- Matrix instruments represented: 212.
- Matrix events: 1,625.
- Matrix regimes after rebuild: 178 Neutral, 25 Bearish, 9 Bullish.
- Matrix bridge full backfill result: accepted 1,556, skipped 375, failed 0.

## Live-vs-Replay Finding

Before corrected install, live v2 state was materially stale-contaminated:

- live rows: 877 vs corrected replay rows: 342.
- live stale count: 93.
- live stale `paper_entry` rows: 88.
- live open positions: 102 vs corrected replay open positions: 69.

The corrected symbol-incremental replay suppressed stale entries and preserved
them as diagnostics instead of carrying them into dashboard/Matrix state.

## Stale-Entry RCA

Two bugs contributed:

- after-hours replay used wall-clock market-hours checks, so stale-entry
  rejection could be bypassed during EOD replay;
- stale rows already marked by the v1/v2 lifecycle as `stale_live_entry` were
  not always rejected by the v2 retained-window rejection path.

Fix installed in `passive_runner.py`:

- market-hours checks now use the evaluation epoch;
- stale-marked positions/events are rejected even when the retained-window lag
  predicate alone is not enough.

## Rollover RCA

Aug 24 was rollover day. v2 had earlier stalled at the 15:25 rollover boundary
and required after-close recovery. The EOD replay/install now validates the
canonical corrected post-rollover state, with 631 target keys ready and no stale
open positions. Any future rollover incident must be checked in the EOD
live-vs-replay report before dashboard/Matrix are trusted.

## Cloud Health At Close

- memory available: about 12 GB.
- disk free: about 67 GB on `/`.
- v2 dashboard, Matrix app, Matrix bridge, target stream, and watchdog active.
- v2 passive intentionally inactive after EOD.

## Operational Follow-Up

- Daily EOD must use the symbol-incremental runner by default.
- Broad full replay remains blocked without explicit approval.
- v1 strategy worker shutdown is deferred until final review because another
  thread may own v1.
