# v2Matrix Portfolio Aug31 Live vs Canonical RCA

- Mode: dry audit plus small safe fixes; no reseed, no large job.
- Portfolio semantic match vs canonical temp: `True`
- Portfolio order matches requested order: `True`
- Portfolio entry metadata missing: `0`
- Matrix event metadata missing: `{'matrix_events.jsonl': 0, 'matrix_state.json': 0}`

## RCA
- pre_install_live_forward_state_differed_from canonical Aug10-Aug31 install across all 8 portfolios: live-forward state before the install had not been generated under the fully corrected canonical history contract and variant/requalification path; the post-install state is canonical for Aug10-Aug31 Fix: current state kept from canonical install; no reseed performed in this audit
- portfolio page/order contract drift: live PORTFOLIO_DEFINITIONS order placed Profit25 first-only before Armed20 requalifier, contrary to the requested UI order Fix: definition order, current portfolio state order, and regression assertion corrected
- some v2Matrix historical exit rows had null quote-history/RAM60 metadata: matrix payload creation used current candidate metadata and did not fall back to the original overlay entry metadata when the exit clock had no fresh feature row Fix: live payload fallback and installer payload fallback added; current matrix_events/matrix_state filled from overlay entry metadata
- historical portfolio quote_history_mode label differs from tomorrow live contract label: installed historical rows are from the research full-session quote index while live uses bounded prior-session warmup Fix: recorded as EOD parity item; semantic decisions are equivalent if RAM windows use only latest 60 quote rows, but exact mode label should be compared tonight/tomorrow before any future reseed

## Portfolio Backup To Current
- `fixed5L_no_replacement_max3_smooth_survivor_armed20_floor80`: open `2->3`, closed `45->54`, net `58502.223840013045->12061.810858618788`
- `fixed5L_no_replacement_max3_smooth_survivor_armed20_floor80_age60_max3_requal_cd0_max3`: open `3->2`, closed `54->65`, net `25624.699058730126->-19805.520130852747`
- `fixed5L_no_replacement_max3_smooth_survivor_profit25`: open `2->3`, closed `47->52`, net `77111.20706100286->-31698.23238013338`
- `fixed5L_no_replacement_max3_smooth_survivor_profit25_age60_max3_requal_cd0_max3`: open `3->2`, closed `50->60`, net `266.96657795696956->-105579.69279079296`
- `fixed5L_no_replacement_max5_smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd0_max3`: open `0->3`, closed `198->217`, net `233954.0799873352->-431135.64144819335`
- `fixed5L_no_replacement_max5_smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd15_max2`: open `0->2`, closed `158->199`, net `184102.7203200055->-364078.5308720667`
- `fixed5L_no_replacement_max5_smooth_survivor_profit25_age0_max5_stop100_requal_cd0_max3`: open `0->3`, closed `192->232`, net `-29045.411457708462->-731398.1969731705`
- `fixed5L_no_replacement_max5_smooth_survivor_profit25_age0_max5_stop100_requal_cd15_max2`: open `0->3`, closed `154->194`, net `-29089.88386430047->-490254.1137399852`

## Tests
- py_compile target scripts passed
- pytest tests/test_v2matrix_overlay_research_parity.py tests/test_v2matrix_portfolios_page.py passed 14/14
