# v2Matrix Portfolio Forensic Comparison - Aug28 Saved Artifacts vs Aug31 Corrected Replay

Generated: 2026-08-31

## Scope

Compare the saved Aug10-Aug28 dry-run/live v2Matrix Portfolio artifacts against the corrected Aug10-Aug31 replay/backtest dry-run, with a specific focus on whether portfolio rules changed or whether the source candidate/opportunity frame changed.

Artifacts inspected:

- Saved Aug28 dry-run install report:
  `/tmp/obvfutport_v2matrix_expanded_portfolios_install_aug10_aug28_include_open_20260830_dryrun/v2matrix_research_portfolio_install_report.json`
- Saved Aug28 live install report:
  `/tmp/obvfutport_v2matrix_expanded_portfolios_install_aug10_aug28_include_open_20260830_live/v2matrix_research_portfolio_install_report.json`
- Saved Aug28 stitched opportunity frame:
  `/tmp/obvfutport_v2_t2_continuation_filters_aug10_aug28_include_open_20260830/continuation_opportunities.parquet`
- Corrected Aug31 native replay report:
  `/tmp/v2matrix_aug31_parity_install_metadata_path_fixed_20260831_180500/v2matrix_research_portfolio_install_report.json`
- Corrected Aug31 native opportunity frame:
  `/tmp/v2matrix_aug31_parity_research_20260831_173100/continuation_opportunities.parquet`
- Control replay: current code run on the saved Aug28 opportunity frame:
  `/tmp/v2matrix_aug28_current_code_install_replay_20260831_forensic`
- Control replay: current code native Aug10-Aug28 opportunity frame:
  `/tmp/v2matrix_aug28_native_current_forensic_20260831`

## Executive Finding

The portfolio strategy rules did not change after normalizing by variant name. The material difference is that the saved Aug28 artifacts and the corrected Aug31 replay were not fed the same candidate/opportunity frame.

The effective assumption that changed was the source-frame contract:

- saved Aug28 used a stitched frame with partial historical provenance/status;
- corrected Aug31 used a native end-to-end frame generated from the current canonical T2 ledger and full quote-index context.

This is a strategy-input contract drift, not a cosmetic dashboard issue.

## Rule Comparison

After normalizing portfolio definitions by variant name, all eight portfolio variants are identical across:

- saved Aug28 dry-run;
- saved Aug28 live install;
- corrected Aug31 dry-run;
- current-code replay on saved Aug28 frame;
- current-code native Aug10-Aug28 replay.

Only display/order changed later.

Common portfolio assumptions:

- initial capital: `2000000`
- fixed entry margin: `500000` per entry
- cash constrained: `false`
- replacement: `none`
- real costs/slippage included
- no portfolio-level forced replacement

Common policy for age60 variants:

- formula: `smooth_survivor`
- min age: `60` minutes
- max age: `240` minutes
- min current return: `0.0005`
- min MFE: `0.0015`
- max MAE abs: `0.003`
- max drawdown-to-MFE: `0.5`
- min positive RAM count: `2`
- max spread: `12` bps
- min edge/cost multiple: `5`
- min score: `0.8`

Common policy for age0 variants:

- same as age60 variants except min age is `0` minutes.

Common overlays:

- Armed20 Floor80:
  - `kind=armed_peak_floor`
  - `arm_target=0.002`
  - `floor_fraction=0.8`
  - hard stop only on stop100 variants: `0.01`
- Profit25:
  - age60 first/requalify: `kind=profit`, `target=0.0025`
  - age0 stop100 variants: `kind=profit_stop_or_failure`, `target=0.0025`, `hard_stop=0.01`, `max_wait_minutes=0`, `failure_floor=0.0`

Control test result:

- Current code replayed on the saved Aug28 opportunity frame exactly reproduced the saved Aug28 portfolio summaries and transaction/holding sets.
- Therefore the portfolio simulator/rule implementation did not drift economically.

## Source Frame Difference

Saved Aug28 stitched frame:

- schema: `obvfutport_v2.t2_continuation_combined_include_open.v1`
- rows: `224475`
- unique legs: `1158`
- unique symbols: `211`
- open-at-period-end rows: `19195`
- `open_at_period_end` non-null rows: `22130`
- RAM/provenance metadata columns: absent

Native Aug10-Aug28 frame rebuilt with current code:

- schema: `obvfutport_v2.t2_continuation_filter_research.v1`
- rows: `298214`
- unique legs: `1118`
- unique symbols: `212`
- open-at-period-end rows: `48296`
- `open_at_period_end` non-null rows: `298214`
- RAM/provenance metadata columns: present

Corrected Aug10-Aug31 native frame:

- schema: `obvfutport_v2.t2_continuation_filter_research.v1`
- rows: `327371`
- unique legs: `1165`
- unique symbols: `212`
- open-at-period-end rows: `47457`
- `open_at_period_end` non-null rows: `327371`
- RAM/provenance metadata columns: present

Row-key comparison, saved Aug28 stitched vs native Aug10-Aug28:

- saved-only row keys: `68286`
- native-only row keys: `142025`
- common row keys: `156189`

Leg-level comparison:

- saved-only semantic T2 entry keys: `72`
- native-only semantic T2 entry keys: `63`
- common semantic T2 entry keys: `1055`
- common semantic keys with changed row ID or exit/status fields: `164`

This proves the two runs were not operating on the same data contract.

## Feature Difference On Common Rows

Even for the `156189` row keys common to both saved Aug28 and native Aug10-Aug28:

- `entry_fill_price`: no differences
- `exit_fill_price`: no differences
- `current_ret`: no differences
- `spread_bps`: no differences
- `edge_return`: no differences
- `edge_to_cost_multiple`: no differences
- `mfe`: `14459` rows differ
- `mae`: `15645` rows differ
- `ram_10`: `37732` rows differ
- `ram_30`: `1782` rows differ
- `ram_60`: `314` rows differ
- `score_smooth_survivor`: `138991` rows differ

Interpretation:

- execution/current pricing was stable on common rows;
- ranking and score context changed materially because the available candidate universe changed;
- some raw RAM/path values also changed, consistent with different quote-index and forward-window construction;
- MFE/MAE drift was concentrated on Aug26-Aug28, where the saved stitched frame had partial status/open-period treatment.

## Portfolio Impact, Same Aug10-Aug28 End Date

The strongest isolation test is saved Aug28 stitched versus native Aug10-Aug28, both using the same current portfolio rules and same period end date.

| Variant | Saved all-qualified | Native all-qualified | Saved closed | Native closed | Saved net | Native net |
|---|---:|---:|---:|---:|---:|---:|
| Armed20 Floor80 / age60 / max3 / no stop / first only | 64 | 80 | 37 | 50 | 116808 | 85650 |
| Armed20 Floor80 / age60 / max3 / no stop / requalify cd0 max3 | 122 | 153 | 49 | 64 | 119402 | 179494 |
| Profit25 / age60 / max3 / no stop / first only | 64 | 80 | 38 | 50 | 113377 | 109929 |
| Profit25 / age60 / max3 / no stop / requalify cd0 max3 | 122 | 153 | 45 | 55 | 84301 | 110516 |
| Armed20 Floor80 / age0 / max5 / stop100 / requalify cd0 max3 | 302 | 375 | 187 | 244 | 297512 | 419682 |
| Armed20 Floor80 / age0 / max5 / stop100 / requalify cd15 max2 | 239 | 289 | 149 | 197 | 245775 | 276509 |
| Profit25 / age0 / max5 / stop100 / requalify cd0 max3 | 308 | 377 | 182 | 223 | 38748 | -42903 |
| Profit25 / age0 / max5 / stop100 / requalify cd15 max2 | 238 | 288 | 145 | 189 | 42203 | 91417 |

This difference appears before Aug31 is added. It is not caused only by Aug31 market data.

## RCA

Root cause:

The Aug28 portfolio state was installed from a stitched opportunity frame, while the later corrected replay was produced from a native full-period opportunity frame. These frames had materially different row scope, T2 leg IDs/statuses, quote-key scope, RAM/rank context, and open-period treatment.

Contributing factors:

1. The Aug28 frame was built through `obvfutport_v2.t2_continuation_combined_include_open.v1`, combining older Aug10-Aug27 research rows with Aug28-only include-open rows.
2. Older rows in the stitched frame did not carry complete `t2_status`, `open_at_period_end`, `t2_actual_exit`, or RAM provenance metadata.
3. The native replay used current canonical T2 ledger state and full-period generation, producing different T2 leg membership and more complete per-leg forward rows.
4. The native replay used a broader quote-index context. Aug28-only incremental had `203` quote keys and `75205` quote rows; native Aug10-Aug28 used about `620` quote keys and `3336621` quote rows.
5. Rank-based scores changed because rank context changed with the candidate universe.
6. The EOD parity gate did not require source-frame contract equality before comparing/installing summaries.
7. The unsafe historical backfill install path could overwrite live-forward portfolio state without proving equivalence to the saved canonical artifact.
8. Metadata gaps in older installed rows initially obscured the contract drift.

The issue was not that the named portfolio variants changed. The issue was that the candidate/opportunity-frame assumption changed under the same variant names.

## Corrective Requirement

Before any future install or reseed, enforce a hard artifact contract gate:

- same variant definition hash after normalizing by name;
- same source-frame schema contract;
- same frame date range;
- same source T2 ledger checksum or selected-candidate checksum;
- same quote-history mode and key scope;
- same required key count and key hash;
- same open-after-period policy;
- same candidate counts by variant, or explicit approved restatement;
- metadata completeness for `ram_60_available_from`, `quote_history_mode`, `quote_history_key_scope`, and entry/exit provenance.

Recommended canonical path:

- Use native end-to-end generation as the canonical backtest/replay contract.
- Do not stitch old historical research rows with new daily rows unless the stitcher proves contract equivalence.
- Treat the saved Aug28 stitched state as legacy/non-canonical unless the user explicitly chooses backward compatibility over replay parity.
- If native canonical is accepted, perform a controlled v2Matrix/v2Matrix Portfolio reseed from the native replay artifact after explicit approval.

