# OBVFUTPORT-v2 Quarantined Symbols - 2026-08-24

## Production Position

`IOC`, `MAXHEALTH`, and `WAAREEENER` remain isolated from the active `209`-symbol adaptive install until their focused proof and state install are explicitly completed. The current production v2 state/dashboard/Matrix were not mutated by this cleanup note.

## WAAREEENER

Status: proof-cleared, not installed into production state yet.

Fixes applied to the research/recalibration kernel:

- T3 pullback entries now validate the actual execution fill, not only the trigger LTP.
- Pullback long fill must remain between hard SL and base entry.
- Pullback short fill must remain between base entry and hard SL.
- Historical recalibration/proof runs can now pin contract lifecycle selection with `--contract-as-of-iso`, preventing post-rollover runs from looking for the wrong month in old Aug10-Aug21 indexes.

Focused proof:

- Folder: `/tmp/obvfutport_v2_quarantine_cleanup_waareeener_20260824`
- Contract as-of: `2026-08-21T15:24:00+05:30`
- Result: completed, `1/1` frozen, `0` bugs.
- Best label: `primary_abs=1.5|fresh_m=1|long_pct=90|short_pct=1|early_exhaustion|t3_pullback_4c_0.50R`
- Candidate entries: `39`
- Accepted combos: `4,680`
- Three-lot summary: `4` closed trades, `0` wins, net `-33015.87`, worst loss `-10389.68`.

Next action before production adoption: run a WAAREEENER-only state/dashboard/Matrix restatement from the proof-cleared artifact, or include it in the next controlled adaptive install. Do not silently mix the new override with old WAAREEENER carry state.

## IOC And MAXHEALTH

Status: still quarantined on current baseline.

Observed issue:

- Focused cleanup stopped on proof-comparison mismatches.
- Differences were same-second fill/LTP ordering effects that changed exit fill price and PnL.
- This remains a proof-kernel determinism issue, not a live production blocker while these symbols stay on baseline.

Recommended next action:

- Patch same-second execution-row ordering once in the indexed proof path.
- Retest only `IOC` and `MAXHEALTH`.
- Do not rerun the `204` successful main-run symbols unless the patch changes shared candidate semantics.

