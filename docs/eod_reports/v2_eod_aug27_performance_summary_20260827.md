# OBVFUTPORT-v2 Aug27 Performance Summary

## Source

- Full cloud report: `/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/reports/v2_tranche_performance_20260810_20260827.json`.
- Basis: entries from 2026-08-10 onward; open rows marked to latest model-state price.
- Final audit: `docs/install_reports/v2_post_eod_aug27_incremental_audit_20260827.json`.

## Tranche Summary

| Tranche | Rows | Closed | Open Mark | Wins | Losses | Closed Success | Closed Net Rs | Open Net Rs | Total Net Rs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| T1 | 1,089 | 948 | 141 | 291 | 657 | 30.70% | -2,079,725.83 | 445,321.89 | -1,634,403.93 |
| T2 | 1,100 | 1,027 | 73 | 354 | 673 | 34.47% | -1,091,986.12 | -13,305.93 | -1,105,292.05 |
| T3 | 248 | 239 | 9 | 78 | 161 | 32.64% | -218,706.12 | 64,501.53 | -154,204.59 |

## Notes

- The dashboard view uses display-row accounting and showed 2,426 rows: 2,203
  closed, 223 open, total net about -2,964,907.08 Rs.
- The performance JSON is intentionally not copied into Git because it is large
  by-symbol operational output. Keep the cloud artifact and commit compact
  summaries/manifests unless a full report snapshot is explicitly needed.
