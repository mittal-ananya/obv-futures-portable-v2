# v2Matrix Overlay Install - 2026-08-27

## Scope

Installed an isolated v2Matrix layer for OBVFUTPORT-v2 T2 smooth-survivor overlay research.

## New Services

- `cloud-v2matrix.service`: cloned Matrix frontend on local port `8098`.
- `cloud-v2matrix-overlay.service`: reads canonical OBVFUTPORT-v2 T2 ledgers plus quote-valid target stream and publishes only `smooth_survivor_armed20_floor80` overlay events to v2Matrix.
- `cloud-v2matrix-portfolios.service`: portfolio monitor on local port `8099`.

## Public URLs

- `https://viveka.sarrvdhara.com/v2Matrix`
- `https://viveka.sarrvdhara.com/v2Matrix_portfolios`

## Isolation

- No OBVFUTPORT-v1 mutation.
- No Compass v1 mutation.
- No existing Matrix mutation.
- No OBVFUTPORT-v2 production ledger/state mutation.
- v2Matrix writes only under `/opt/cloud-deploy-candidates/v2matrix/state`.

## Portfolio Definitions

- `fixed5L_no_replacement_max3_smooth_survivor_armed20_floor80`
- `fixed5L_no_replacement_max3_smooth_survivor_profit25`

Both are paper-only, max 3 positions, fixed Rs 5L max margin per entry, multi-lot, no replacement.

## Smoke Results

- v2Matrix public page: HTTP 200.
- v2Matrix portfolio public page: HTTP 200.
- v2Matrix API: healthy, `event_count=0` at install because the equity session was already closed.
- v2Matrix portfolio API: healthy, 2 portfolios configured.
- Overlay status: healthy idle after market close with `outside_market_session_idle`.
- Cloud disk after install: `/opt` at 80% used, about 64G free.
- New service memory after install:
  - v2Matrix frontend: about 43 MB.
  - overlay daemon: about 50 MB in after-hours idle mode.
  - portfolio frontend: about 30 MB.

## Notes

- v2Matrix is live-forward from installation time. It does not backfill historical smooth-survivor overlay events yet.
- The active shared universe manifest currently reports 211 symbols, and v2Matrix follows that manifest.
- The overlay avoids consuming a newly created target-stream file before market session, so the next session should start from the beginning of the day stream rather than skipping early rows.
