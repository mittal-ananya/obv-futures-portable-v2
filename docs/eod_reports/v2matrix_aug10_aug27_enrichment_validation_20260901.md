# v2Matrix Aug10-Aug27 Enrichment Validation

Generated: 2026-09-01

## Objective

Validate whether the saved Aug10-Aug27 v2Matrix Portfolio research/deployment frame remains internally correct after adding missing audit/provenance fields as metadata only.

No production state was changed.

## Inputs

Saved Aug10-Aug27 opportunity frame:

`/tmp/obvfutport_v2_t2_continuation_filters_aug10_aug27_research_aligned_20260827/continuation_opportunities.parquet`

Saved deployment states:

- `/tmp/v2matrix_research_install_aug10_aug27_20260827/portfolio_state.json`
- `/tmp/v2matrix_research_install_aug10_aug27_dedup_20260827/portfolio_state.json`

Enriched metadata-only copy:

`/tmp/v2matrix_aug10_aug27_saved_enriched_metadata_only_20260831/continuation_opportunities.enriched.parquet`

Replay output:

`/tmp/v2matrix_aug10_aug27_enriched_replay_20260831/portfolio_state.json`

## Enrichment

Added columns:

- `t2_status`
- `open_at_period_end`
- `t2_actual_exit_epoch`
- `t2_actual_exit_time`
- `quote_history_mode`
- `quote_history_key_scope`
- `ram_60_available_from_epoch`
- `ram_60_available_from`
- `signal_key_history_earliest_epoch`
- `signal_key_history_earliest_time`
- `signal_key_history_latest_epoch`
- `signal_key_history_latest_time`
- `ram_60_window_start_epoch`
- `ram_60_window_end_epoch`
- `ram_60_window_start`
- `ram_60_window_end`

Validation:

- row count unchanged: `202345`
- unique T2 legs unchanged: `1069`
- unique symbols unchanged: `211`
- all original strategy-driving columns unchanged
- all saved T2 legs were closed before the Aug27 15:30 period mark, so `open_at_period_end=false` is metadata-only for this period

Metadata limitation:

- `quote_history_mode` is marked as legacy metadata-only, not native quote-index provenance.
- `ram_60_available_from_epoch` is the earliest observed emitted row per symbol in the saved frame, not a native source-history proof.
- legacy frame still cannot prove source quote-key checksums or native RAM60 availability.

## Replay Result

Replay command used the original two deployed variants:

- `smooth_survivor_armed20_floor80`
- `smooth_survivor_profit25`

Replay summary gate: `ok=true`, no mismatches.

## Performance Match

| Variant | Saved closed | Replay closed | Saved wins/losses | Replay wins/losses | Saved net | Replay net | Saved return on peak margin | Replay return on peak margin |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Armed20 Floor80 / age60 / max3 / no stop / first only | 35 | 35 | 31/4 | 31/4 | 107378.55380556946 | 107378.55380556946 | 7.529373449108263 | 7.529373449108263 |
| Profit25 / age60 / max3 / no stop / first only | 37 | 37 | 31/6 | 31/6 | 105483.91638772067 | 105483.91638772067 | 7.396521662936248 | 7.396521662936248 |

Transaction/holding comparison:

- Armed20 Floor80: `70/70` transaction keys matched, `0` differences; no open holdings in either.
- Profit25: `74/74` transaction keys matched, `0` differences; no open holdings in either.

## Candidate Count Note

The original Aug27 install report recorded candidate counts `60/60`.

The later dedup install and enriched replay recorded `59/59`.

This candidate-count representation difference did not affect portfolio performance because the saved original install state, saved dedup install state, and enriched replay all have identical economic summaries and identical portfolio transaction/holding keys.

## Conclusion

The saved Aug10-Aug27 deployed portfolio results are internally valid under their saved opportunity frame and strategy rules.

Adding the missing fields as metadata-only does not change portfolio performance, transactions, or holdings.

This validates that the later Aug28/Aug31 divergence came from source-frame contract drift versus native replay, not from an arithmetic error in the saved Aug10-Aug27 portfolio application.

