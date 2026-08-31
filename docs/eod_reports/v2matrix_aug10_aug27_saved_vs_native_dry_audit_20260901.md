# v2Matrix Aug10-Aug27 Saved vs Native Dry Audit

Generated: 2026-09-01

## Scope

Dry/audit-only comparison of:

- saved Aug10-Aug27 v2Matrix Portfolio opportunity frame, enriched with metadata only;
- fresh native Aug10-Aug27 opportunity replay using current code and canonical v2 state;
- portfolio replay outputs for the two originally deployed variants.

No production state, dashboard state, v2Matrix state, or portfolio URL state was changed.

## Inputs

Saved enriched frame:

`/tmp/v2matrix_aug10_aug27_saved_enriched_metadata_only_20260831/continuation_opportunities.enriched.parquet`

Saved enriched portfolio replay:

`/tmp/v2matrix_aug10_aug27_enriched_replay_20260831/portfolio_state.json`

Fresh native frame:

`/tmp/v2matrix_aug10_aug27_native_current_forensic_20260901/continuation_opportunities.parquet`

Fresh native portfolio replay:

`/tmp/v2matrix_aug10_aug27_native_portfolio_replay_forensic_20260901/portfolio_state.json`

Comparison outputs:

- `/tmp/v2matrix_aug10_aug27_saved_vs_native_forensic_20260901/frame_comparison_summary.json`
- `/tmp/v2matrix_aug10_aug27_saved_vs_native_forensic_20260901/normalized_semantic_leg_comparison.json`
- `/tmp/v2matrix_aug10_aug27_saved_vs_native_forensic_20260901/candidate_saved_enriched_vs_native_comparison.json`
- `/tmp/v2matrix_aug10_aug27_saved_vs_native_forensic_20260901/portfolio_saved_enriched_vs_native_replay_comparison.json`
- `/tmp/v2matrix_aug10_aug27_saved_vs_native_forensic_20260901/forensic_digest.json`

## Step 1 Result: Saved Artifact Remains Internally Valid

The saved Aug10-Aug27 frame was enriched with metadata-only fields and replayed through the current portfolio install path.

Result: it reproduced the saved deployment exactly for the two original variants.

| Variant | Saved closed | Replay closed | Saved wins/losses | Replay wins/losses | Saved net | Replay net | Saved return / peak margin | Replay return / peak margin |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Armed20 Floor80 / age60 / max3 / no stop / first only | 35 | 35 | 31/4 | 31/4 | 107378.55380556946 | 107378.55380556946 | 7.529373449108263 | 7.529373449108263 |
| Profit25 / age60 / max3 / no stop / first only | 37 | 37 | 31/6 | 31/6 | 105483.91638772067 | 105483.91638772067 | 7.396521662936248 | 7.396521662936248 |

Transaction parity:

- Armed20 Floor80: `70/70` transaction keys matched; no open holdings in either.
- Profit25: `74/74` transaction keys matched; no open holdings in either.

Conclusion: the saved Aug10-Aug27 deployed results are internally valid under their own saved opportunity rows and strategy rules.

## Step 2 Result: Rules Did Not Drift

The portfolio definitions are identical between saved-enriched replay and native replay.

Confirmed identical:

- variant labels;
- max positions;
- fixed entry margin;
- `requalify` flags;
- cooldown;
- max entries per T2 leg;
- overlay definitions;
- policy thresholds.

Original age60 policy:

- formula: `smooth_survivor`
- min score: `0.80`
- min age: `60` minutes
- max age: `240` minutes
- min current return: `0.0005`
- min MFE: `0.0015`
- max MAE abs: `0.0030`
- max drawdown-to-MFE: `0.50`
- min positive RAM count: `2`
- max spread: `12` bps
- min edge/cost multiple: `5`
- min minutes to session end: `0`

Overlay definitions:

- Armed20 Floor80: arm at `20` bps, exit if return falls to `80%` of achieved favorable peak.
- Profit25: fixed profit capture at `25` bps.

Conclusion: the divergence is not caused by portfolio-rule drift.

## Step 3 Result: Source Opportunity Frame Drift

The saved and native frames are materially different even before portfolio construction.

| Metric | Saved enriched Aug10-Aug27 | Native Aug10-Aug27 |
|---|---:|---:|
| rows | 202345 | 242909 |
| row IDs | 1069 | 950 |
| semantic T2 legs | 1044 | 950 |
| symbols | 211 | 211 |
| open-at-period-end rows | 0 | 47468 |
| open-at-period-end legs | 0 | 73 |
| row-key common | 115781 | 115781 |
| saved-only row keys | 86564 | 0 |
| native-only row keys | 0 | 127128 |
| common semantic legs | 835 | 835 |
| saved-only semantic legs | 209 | 0 |
| native-only semantic legs | 0 | 115 |
| changed common semantic legs, normalized | 124 | 124 |
| changed common exit/status legs, normalized | 122 | 122 |

The saved frame was stitched:

- old frame: `/tmp/obvfutport_v2_t2_continuation_filters_aug10_aug25_20260826/continuation_opportunities.parquet`
- new frame: `/tmp/obvfutport_v2_t2_continuation_filters_aug26_aug27_20260827/continuation_opportunities.parquet`
- combined output: `/tmp/obvfutport_v2_t2_continuation_filters_aug10_aug27_research_aligned_20260827/continuation_opportunities.parquet`
- stitch de-duplication: `(row_id, clock_epoch)`

Component reports:

| Component | Period legs | Closed period legs | Open-after-period legs | Outcome rows | Open rows skipped |
|---|---:|---:|---:|---:|---:|
| saved Aug10-Aug25 component | 1119 | 985 | not recorded | 182514 | 37415 |
| saved Aug26-Aug27 component | 190 | 107 | 83 | 19831 | 45330 |
| native Aug10-Aug27 | 1087 | 1014 | 73 | 242909 | 73 |

Interpretation:

- The saved Aug27 artifact was not one native end-to-end replay. It was a stitched frame from older and newer research runs.
- The saved components skipped open-after-period rows; the fresh native run includes open-after-period rows and marks them at the period end.
- The current canonical T2 ledger/state changed underneath the portfolio layer: the old Aug10-Aug25 component had 1119 period T2 legs, while the native Aug10-Aug27 replay has 1087 total period T2 legs.
- Therefore a longer current replay can legitimately have fewer old legs if stale/duplicate/incorrect T2 rows were corrected out of the canonical ledger.

## Feature Drift On Common Rows

On common row keys:

| Field | Differing common rows |
|---|---:|
| `entry_fill_price` | 0 |
| `exit_fill_price` | 0 |
| `current_ret` | 0 |
| `spread_bps` | 0 |
| `edge_return` | 627 |
| `edge_to_cost_multiple` | 363 |
| `mfe` | 8184 |
| `mae` / `mae_abs` | 10295 |
| `ram_10` | 26782 |
| `ram_30` | 1141 |
| `ram_60` | 695 |
| `rank_ram_10` | 102475 |
| `rank_ram_30` | 88846 |
| `rank_ram_60` | 87832 |
| `score_smooth_survivor` | 98808 |

Interpretation:

- Execution prices on common rows are stable.
- The major drift is in rank/score context and candidate universe.
- Some raw RAM/path fields also drift, which is consistent with different quote-index/cache construction and different forward-window paths.

## Candidate-Level Impact

Before the portfolio cap is applied:

| Variant | Saved candidates | Native candidates | Common candidates | Saved-only | Native-only | Changed same candidate leg | Saved open candidates | Native open candidates |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Armed20 Floor80 | 59 | 70 | 32 | 27 | 38 | 27 | 0 | 1 |
| Profit25 | 59 | 70 | 32 | 27 | 38 | 27 | 0 | 1 |

Candidate exit reason counts:

| Variant | Saved | Native |
|---|---|---|
| Armed20 Floor80 | `armed_peak_floor=46`, `underlying_t2_exit=13` | `armed_peak_floor=55`, `underlying_t2_exit=14`, `open_at_period_end=1` |
| Profit25 | `profit_capture=45`, `underlying_t2_exit=14` | `profit_capture=55`, `underlying_t2_exit=14`, `open_at_period_end=1` |

Conclusion: the portfolio differences are already present before sizing, max-position caps, or page rendering.

## Portfolio Impact

| Variant | Saved entries | Native entries | Saved exits | Native exits | Saved net | Native realized net | Saved open holdings | Native open holdings |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Armed20 Floor80 / age60 / max3 / no stop / first only | 35 | 42 | 35 | 41 | 107378.55380556946 | 111507.62768087955 | 0 | 1 |
| Profit25 / age60 / max3 / no stop / first only | 37 | 43 | 37 | 42 | 105483.91638772067 | 134912.44796660604 | 0 | 1 |

The native replay carries one open TCS holding at Aug27 period end for Armed20. Profit25 also carries one open holding. The saved Aug27 state carries none, because the saved frame had no open-at-period-end rows.

## Concrete Examples

BAJAJ-AUTO Aug24:

- saved candidate: `OBVFUTPORT_V2_PASSIVE:BAJAJ-AUTO:long:1787543400:7263401dce51011b:position`, entry `2026-08-24T10:21:00+05:30`, exit `2026-08-24T15:24:00+05:30`, saved score `0.8176923076923076`;
- native candidate: `OBVFUTPORT_V2_PASSIVE:BAJAJ-AUTO:long:1787543400:dd22ce966fed1e63:position`, entry `2026-08-24T10:21:00+05:30`, exit `2026-08-25T10:37:00+05:30`, native score `0.8471014492753624`.

ANGELONE Aug24:

- saved candidate: `OBVFUTPORT_V2_PASSIVE:ANGELONE:short:1787559600:1ab280fddca170aa:position`, exit `2026-08-24T15:07:00+05:30`;
- native candidate: `OBVFUTPORT_V2_PASSIVE:ANGELONE:short:1787559600:30877387eb7c5c28:position`, exit `2026-08-24T15:08:00+05:30`.

Common-leg score drift example:

- AMBUJACEM short, same position ID and same entry/exit times:
  - saved score: `0.800609756097561`
  - native score: `0.8030120481927712`
  - saved quote history mode: `legacy_saved_aug10_aug27_enriched_metadata_only`
  - native quote history mode: `research_full_session_quote_index`

## RCA

Root cause:

The saved Aug10-Aug27 portfolio state was produced from a stitched legacy opportunity frame, while the fresh replay was produced from the current native end-to-end opportunity frame. Both used the same named portfolio rules, but they did not use the same source-frame contract.

Contributing causes:

1. The saved frame was stitched from Aug10-Aug25 plus Aug26-Aug27, not generated as one native Aug10-Aug27 replay.
2. The saved components skipped open-after-period rows. Native replay includes and marks open-after-period rows.
3. The canonical T2 leg ledger/state changed after the saved research run. This removed some saved-only stale/duplicate/older T2 legs and introduced native-only corrected T2 legs.
4. Rank-based `smooth_survivor` scores changed because rank context depends on the full candidate universe at each clock.
5. Some raw RAM/MFE/MAE fields changed due to different quote-index/cache and forward-window construction.
6. The saved artifact lacked native provenance fields, so the contract drift was not visible until metadata/parity fields were added later.

What this is not:

- Not a dashboard display bug.
- Not a portfolio simulator arithmetic bug.
- Not a drift in the two original portfolio variant definitions.
- Not caused by max-position caps alone.

## Recommendation

Do not reseed or alter production state yet from this dry audit alone.

Next decision required:

Choose the canonical contract for v2Matrix Portfolio history:

1. Legacy-compatible contract: preserve the saved Aug27/Aug28 stitched artifact as the historical truth for already published portfolio history.
2. Native-replay contract: accept the current native end-to-end replay as canonical, then do a controlled reseed/restatement with explicit approval.

Engineering recommendation:

Use native end-to-end replay as canonical going forward, but only after a controlled reseed decision. It gives consistent open-carry handling, explicit quote-history provenance, and replay/live parity. The saved stitched artifact should be treated as internally valid but legacy/non-canonical for future parity work.

Required gate before any install:

- variant definition hash equal;
- source-frame schema equal;
- date range equal;
- T2 ledger/source checksum equal;
- quote history mode and key scope equal;
- required key hash equal;
- open-after-period policy equal;
- candidate counts equal by variant, unless user approves a restatement;
- summary and transaction parity checked before replacement.

