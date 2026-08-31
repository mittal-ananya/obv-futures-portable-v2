from __future__ import annotations

import json
import math
import gzip
import pickle
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import backfill_v2matrix_history as history_backfill  # noqa: E402
import install_v2matrix_from_research_portfolios as installer  # noqa: E402
import research_t2_mfe_first_profit_capture as mfe_research  # noqa: E402
import research_t2_overlay_portfolios as research_portfolios  # noqa: E402
import research_t2_overlay_variant_compare as research_compare  # noqa: E402
import run_v2matrix_overlay as live  # noqa: E402


IST = ZoneInfo("Asia/Kolkata")


class DummyV1Portfolio:
    def execution_fill_from_row(self, row, *, side, phase, point_config=None):  # noqa: ANN001
        price = float(row["price"])
        return {"fill_price": price, "ltp_price": price, "side": side, "phase": phase}

    def futures_trade_accounting(self, *, side, entry_fill_price, exit_fill_price, lot_size, lots, point_config=None):  # noqa: ANN001
        direction = 1.0 if str(side).lower() == "long" else -1.0
        gross = direction * (float(exit_fill_price) - float(entry_fill_price)) * int(lot_size) * int(lots)
        return {"gross_rupees": gross, "charges_rupees": 0.0, "net_rupees": gross}


def epoch_at(hour: int, minute: int) -> int:
    return int(datetime(2026, 8, 27, hour, minute, tzinfo=IST).timestamp())


def make_leg(*, side: str = "long", exit_epoch: int | None = None) -> base.TrancheLeg:
    return base.TrancheLeg(
        row_id="TEST|T2|pos-1|1",
        symbol="TEST",
        tranche="T2",
        side=side,
        entry_epoch=epoch_at(9, 20),
        exit_epoch=exit_epoch,
        position_id="pos-1",
        signal_source="cash",
        signal_key="NSE:TEST",
        execution_key="NFO:TEST_FUT",
        entry_fill_price=100.0,
        exit_fill_price=101.0 if exit_epoch is not None else None,
        margin_per_lot=100000.0,
        lot_size=100,
        source_row={},
    )


def make_position(variant: str = "smooth_survivor_armed20_floor80") -> live.OverlayPosition:
    return live.OverlayPosition(
        variant=variant,
        row_id="TEST|T2|pos-1|1",
        position_id="pos-1",
        symbol="TEST",
        side="long",
        entry_epoch=epoch_at(9, 30),
        entry_time=live.epoch_ist_iso(epoch_at(9, 30)),
        entry_fill_price=100.0,
        entry_ltp_price=100.0,
        entry_score=0.92,
        entry_features={},
    )


def make_index(prices_by_epoch: dict[int, float]) -> live.QuoteRingIndex:
    index = live.QuoteRingIndex()
    for epoch, price in prices_by_epoch.items():
        index.add("NFO:TEST_FUT", epoch, price, price - 0.01, price + 0.01)
    index.finalize()
    return index


def test_history_backfill_install_requires_explicit_full_history_approval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_v2matrix_history.py",
            "--output-dir",
            str(tmp_path),
            "--install",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        history_backfill.main()

    assert "--allow-full-history-install" in str(exc.value)


def test_potential_t2_required_keys_include_manifest_universe_before_leg_is_active(tmp_path: Path) -> None:
    root = tmp_path
    config_dir = root / "config"
    config_dir.mkdir()
    (config_dir / "obvfutport_v2_contract_chain_manifest.json").write_text(
        json.dumps(
            {
                "symbols": {
                    "TEST": {
                        "cash_key": "NSE:TEST",
                        "base_fut_key": "NFO:TEST26SEPFUT",
                        "contracts": [{"instrument_key": "NFO:TEST26SEPFUT"}],
                    },
                    "FRESH": {
                        "cash_key": "NSE:FRESH",
                        "base_fut_key": "NFO:FRESH26SEPFUT",
                        "contracts": [{"instrument_key": "NFO:FRESH26SEPFUT"}],
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    keys = live.potential_t2_required_keys(root, {})

    assert "NSE:FRESH" in keys
    assert "NFO:FRESH26SEPFUT" in keys
    assert "NSE:TEST" in keys


def test_live_features_record_ram60_history_contract() -> None:
    clock = epoch_at(9, 21)
    start = clock - (61 * 60)
    index = live.QuoteRingIndex(retention_seconds=100_000)
    for i in range(61):
        epoch = start + (i * 60)
        price = 100.0 + (i * 0.01)
        index.add("NSE:TEST", epoch, price, price - 0.01, price + 0.01)
        index.add("NFO:TEST_FUT", epoch, price, price - 0.01, price + 0.01)
    index.finalize()
    state = {
        "quote_history_contract": {
            "mode": live.QUOTE_HISTORY_MODE,
            "key_scope": live.QUOTE_HISTORY_KEY_SCOPE,
            "required_key_count": 2,
        }
    }

    features, stats = live.build_features(
        active_legs=[make_leg()],
        index=index,
        v1_portfolio=DummyV1Portfolio(),
        clock_epoch=clock,
        state=state,
        risk_floor=0.0005,
    )

    assert stats["missing_ram"] == 0
    feature = features["TEST|T2|pos-1|1"]
    assert feature["quote_history_mode"] == live.QUOTE_HISTORY_MODE
    assert feature["quote_history_key_scope"] == live.QUOTE_HISTORY_KEY_SCOPE
    assert feature["ram_60_available_from_epoch"] == clock
    assert feature["ram_60_available_from"] == live.epoch_ist_iso(clock)
    assert feature["ram_60_window_end_epoch"] == clock - 60


def test_cached_prior_session_warmup_reports_partial_key_coverage(tmp_path: Path) -> None:
    root = tmp_path
    cache_dir = root / "state" / "research_cache" / "t2_quote_index"
    cache_dir.mkdir(parents=True)
    trade_date = "2026-08-27"
    cache_file = cache_dir / f"{trade_date}_abc.quote_index.pkl.gz"
    meta_file = cache_dir / f"{trade_date}_abc.quote_index_meta.json"
    with gzip.open(cache_file, "wb") as handle:
        pickle.dump(
            {
                "NSE:TEST": [
                    (epoch_at(14, 58), float(epoch_at(14, 58)), 100.0, 99.9, 100.1),
                    (epoch_at(14, 59), float(epoch_at(14, 59)), 101.0, 100.9, 101.1),
                ]
            },
            handle,
        )
    meta_file.write_text(json.dumps({"schema": live.QUOTE_INDEX_CACHE_SCHEMA}), encoding="utf-8")
    index = live.QuoteRingIndex()

    report = live.load_cached_quote_index_into_ring(
        root=root,
        trade_date=datetime.fromisoformat(trade_date).date(),
        required_keys={"NSE:TEST", "NSE:MISSING"},
        index=index,
        max_rows_per_key=96,
    )

    assert report["cache_status"] == "hit"
    assert report["warmed_key_count"] == 1
    assert report["missing_required_key_count"] == 1
    assert index.key_count() == 1
    assert index.row_count() == 2


def test_live_entry_contract_is_stored_on_payload_and_portfolio_holding() -> None:
    clock = epoch_at(10, 30)
    leg = make_leg()
    features = {
        "portfolio_score": 0.91,
        "quote_history_mode": live.QUOTE_HISTORY_MODE,
        "quote_history_key_scope": live.QUOTE_HISTORY_KEY_SCOPE,
        "ram_60_available_from_epoch": epoch_at(10, 20),
        "ram_60_available_from": live.epoch_ist_iso(epoch_at(10, 20)),
    }
    position = make_position()
    position.entry_epoch = clock
    position.entry_time = live.epoch_ist_iso(clock)
    position.entry_features = dict(features)

    payload = live.matrix_payload(
        event_type="paper_entry",
        variant=position.variant,
        leg=leg,
        position=position,
        event_epoch_value=clock,
        trigger_price=100.0,
        trigger_source="unit_test",
        features=features,
    )

    assert payload["quote_history_mode"] == live.QUOTE_HISTORY_MODE
    assert payload["quote_history_key_scope"] == live.QUOTE_HISTORY_KEY_SCOPE
    assert payload["ram_60_available_from_epoch"] == epoch_at(10, 20)
    assert payload["ram_60_available_from"] == live.epoch_ist_iso(epoch_at(10, 20))
    assert payload["overlay_entry_features"]["quote_history_mode"] == live.QUOTE_HISTORY_MODE

    portfolio = {
        "portfolio_id": "unit-portfolio",
        "cash_rupees": 2_000_000.0,
        "peak_margin_rupees": 0.0,
        "holdings": {},
        "transactions": [],
        "diagnostics": {},
    }
    assert live.open_portfolio_holding(
        portfolio=portfolio,
        variant=position.variant,
        overlay_key_value="overlay-key-1",
        position=position,
        leg=leg,
    )
    holding = portfolio["holdings"]["overlay-key-1"]
    tx = portfolio["transactions"][-1]
    assert holding["quote_history_mode"] == live.QUOTE_HISTORY_MODE
    assert holding["quote_history_key_scope"] == live.QUOTE_HISTORY_KEY_SCOPE
    assert holding["ram_60_available_from_epoch"] == epoch_at(10, 20)
    assert tx["quote_history_mode"] == live.QUOTE_HISTORY_MODE
    assert tx["ram_60_available_from"] == live.epoch_ist_iso(epoch_at(10, 20))


def test_matrix_payload_falls_back_to_position_entry_metadata_for_exit() -> None:
    clock = epoch_at(11, 15)
    leg = make_leg()
    position = make_position()
    position.entry_features = {
        "portfolio_score": 0.91,
        "quote_history_mode": live.QUOTE_HISTORY_MODE,
        "quote_history_key_scope": live.QUOTE_HISTORY_KEY_SCOPE,
        "ram_60_available_from_epoch": epoch_at(10, 20),
        "ram_60_available_from": live.epoch_ist_iso(epoch_at(10, 20)),
    }

    payload = live.matrix_payload(
        event_type="paper_exit",
        variant=position.variant,
        leg=leg,
        position=position,
        event_epoch_value=clock,
        trigger_price=101.0,
        trigger_source="unit_test",
        features=None,
        exit_reason="armed_peak_floor",
        position_closed=True,
    )

    assert payload["overlay_entry_features"]["quote_history_mode"] == live.QUOTE_HISTORY_MODE
    assert payload["quote_history_mode"] == live.QUOTE_HISTORY_MODE
    assert payload["quote_history_key_scope"] == live.QUOTE_HISTORY_KEY_SCOPE
    assert payload["ram_60_available_from_epoch"] == epoch_at(10, 20)
    assert payload["ram_60_available_from"] == live.epoch_ist_iso(epoch_at(10, 20))


def research_exit(config: dict, returns: list[float], *, start_hour: int = 9, start_minute: int = 30) -> dict:
    start = epoch_at(start_hour, start_minute)
    window = pd.DataFrame(
        {
            "clock_epoch": [start + 60 * i for i in range(len(returns))],
            "clock_time": [live.epoch_ist_iso(start + 60 * i) for i in range(len(returns))],
            "forward_return": returns,
            "score_smooth_survivor": [0.91 for _ in returns],
        }
    )
    return research_compare.choose_exit(
        {
            "window": window,
            "qualification_epoch": start,
            "entry_fill_price": 100.0,
            "side": "long",
        },
        config,
        "score_smooth_survivor",
        0.80,
    )


def test_live_policy_and_variants_match_research_definitions() -> None:
    selected = {name: (policy_name, config) for name, policy_name, config in research_portfolios.portfolio_variants()}
    expected_order = (
        "smooth_survivor_armed20_floor80",
        "smooth_survivor_armed20_floor80_age60_max3_requal_cd0_max3",
        "smooth_survivor_profit25",
        "smooth_survivor_profit25_age60_max3_requal_cd0_max3",
        "smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd0_max3",
        "smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd15_max2",
        "smooth_survivor_profit25_age0_max5_stop100_requal_cd0_max3",
        "smooth_survivor_profit25_age0_max5_stop100_requal_cd15_max2",
    )

    assert expected_order == live.PORTFOLIO_VARIANTS
    assert set(expected_order) == set(live.PORTFOLIO_VARIANTS)
    for variant in ("smooth_survivor_armed20_floor80", "smooth_survivor_profit25"):
        policy_name, config = selected[variant]
        assert policy_name == live.POLICY.name
        assert live.variant_config(variant) == config
        assert live.portfolio_def(variant).max_positions == 3
        assert not live.portfolio_def(variant).requalify
    assert live.portfolio_def("smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd0_max3").max_positions == 5
    assert live.portfolio_def("smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd0_max3").policy.min_age_minutes == 0.0
    assert live.portfolio_def("smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd0_max3").max_entries_per_t2_leg == 3
    assert live.portfolio_def("smooth_survivor_profit25_age0_max5_stop100_requal_cd15_max2").cooldown_minutes == 15
    assert live.variant_config("smooth_survivor_profit25_age0_max5_stop100_requal_cd0_max3")["kind"] == "profit_stop_or_failure"
    assert live.FIXED_ENTRY_MARGIN == 500_000.0
    assert live.MAX_PORTFOLIO_POSITIONS == 3


def test_requalifying_overlay_keys_are_unique_but_legacy_keys_are_stable() -> None:
    row_id = "TEST|T2|pos-1|1"
    assert live.overlay_key("smooth_survivor_profit25", row_id) == f"smooth_survivor_profit25|{row_id}"
    first = live.overlay_key("smooth_survivor_profit25_age0_max5_stop100_requal_cd0_max3", row_id, epoch_at(9, 35))
    second = live.overlay_key("smooth_survivor_profit25_age0_max5_stop100_requal_cd0_max3", row_id, epoch_at(10, 5))
    assert first != second
    assert first.endswith(f"entry{epoch_at(9, 35)}")


def test_requalify_gate_respects_cooldown_and_max_entries() -> None:
    row_id = "TEST|T2|pos-1|1"
    variant = "smooth_survivor_profit25_age0_max5_stop100_requal_cd15_max2"
    state: dict[str, object] = {"completed_overlay_keys": [], "completed_overlay_history": {}}
    active: dict[str, object] = {}
    completed: set[str] = set()
    first_key = live.overlay_key(variant, row_id, epoch_at(9, 35))
    live.record_completed_overlay(state, variant=variant, row_id=row_id, key=first_key, exit_epoch=epoch_at(10, 0), exit_reason="profit_capture")

    assert live.can_open_overlay(state, variant=variant, row_id=row_id, clock_epoch=epoch_at(10, 10), active_overlay=active, completed_overlay=completed) == (
        False,
        "requalify_cooldown_active",
    )
    assert live.can_open_overlay(state, variant=variant, row_id=row_id, clock_epoch=epoch_at(10, 15), active_overlay=active, completed_overlay=completed) == (
        True,
        "ok",
    )
    second_key = live.overlay_key(variant, row_id, epoch_at(10, 15))
    live.record_completed_overlay(state, variant=variant, row_id=row_id, key=second_key, exit_epoch=epoch_at(10, 30), exit_reason="adverse_stop")
    assert live.can_open_overlay(state, variant=variant, row_id=row_id, clock_epoch=epoch_at(10, 45), active_overlay=active, completed_overlay=completed) == (
        False,
        "max_entries_per_t2_leg_reached",
    )


def test_live_row_id_reconciliation_uses_position_id_for_research_rows() -> None:
    leg = make_leg()
    old_row_id = "TEST|T2|pos-1|999"
    state = {
        "active_overlay": {
            "smooth_survivor_profit25|TEST|T2|pos-1|999": {
                **live.asdict(make_position("smooth_survivor_profit25")),
                "row_id": old_row_id,
            }
        },
        "portfolios": {
            "fixed5L": {
                "holdings": {
                    "smooth_survivor_profit25|TEST|T2|pos-1|999": {
                        "overlay_key": "smooth_survivor_profit25|TEST|T2|pos-1|999",
                        "row_id": old_row_id,
                        "position_id": "pos-1",
                        "symbol": "TEST",
                        "side": "long",
                        "lots": 1,
                        "lot_size": 100,
                        "margin_locked": 100000.0,
                        "entry_epoch": epoch_at(9, 30),
                        "entry_time": live.epoch_ist_iso(epoch_at(9, 30)),
                        "entry_fill_price": 100.0,
                        "entry_ltp_price": 100.0,
                        "entry_score": 0.92,
                    }
                }
            }
        },
    }

    changed = live.canonicalize_live_overlay_rows(state, {leg.row_id: leg})

    assert changed == 2
    active = next(iter(state["active_overlay"].values()))
    holding = next(iter(state["portfolios"]["fixed5L"]["holdings"].values()))
    assert active["row_id"] == leg.row_id
    assert active["legacy_row_id"] == old_row_id
    assert holding["row_id"] == leg.row_id
    assert holding["legacy_row_id"] == old_row_id


def test_non_requalifying_overlay_does_not_reopen_after_row_id_reconciliation() -> None:
    state: dict[str, object] = {"completed_overlay_keys": [], "completed_overlay_history": {}}
    row_id = "TEST|T2|pos-1|1"
    legacy_key = "smooth_survivor_profit25|TEST|T2|pos-1|999"
    live.record_completed_overlay(
        state,
        variant="smooth_survivor_profit25",
        row_id=row_id,
        key=legacy_key,
        exit_epoch=epoch_at(10, 0),
        exit_reason="underlying_t2_exit",
    )

    assert live.can_open_overlay(
        state,
        variant="smooth_survivor_profit25",
        row_id=row_id,
        clock_epoch=epoch_at(10, 5),
        active_overlay={},
        completed_overlay=set(),
    ) == (False, "already_completed_for_t2_leg")


def test_portfolio_summary_marks_unrealized_using_position_id_reconciled_leg() -> None:
    leg = make_leg()
    portfolio = {
        "variant": "smooth_survivor_profit25",
        "portfolio_id": "fixed5L",
        "peak_margin_rupees": 100000.0,
        "holdings": {
            "smooth_survivor_profit25|TEST|T2|pos-1|999": {
                "overlay_key": "smooth_survivor_profit25|TEST|T2|pos-1|999",
                "row_id": "TEST|T2|pos-1|999",
                "position_id": "pos-1",
                "symbol": "TEST",
                "side": "long",
                "lots": 1,
                "lot_size": 100,
                "margin_locked": 100000.0,
                "entry_epoch": epoch_at(9, 30),
                "entry_time": live.epoch_ist_iso(epoch_at(9, 30)),
                "entry_fill_price": 100.0,
                "entry_ltp_price": 100.0,
                "entry_score": 0.92,
            }
        },
        "transactions": [],
    }

    summary = live.portfolio_summary(
        portfolio,
        make_index({epoch_at(9, 35): 101.0}),
        DummyV1Portfolio(),
        {leg.row_id: leg},
        epoch_at(9, 35),
    )

    assert summary["open_positions"] == 1
    assert summary["unrealized_net_rupees"] == 100.0


def test_armed_floor_live_exit_matches_research_floor_price_and_reason() -> None:
    first = epoch_at(9, 31)
    second = epoch_at(9, 32)
    leg = make_leg()
    position = make_position()
    v1 = DummyV1Portfolio()

    first_index = make_index({first: 100.30})
    should_exit, reason, ret, exit_fill, _fill = live.should_exit_overlay(
        variant="smooth_survivor_armed20_floor80",
        leg=leg,
        position=position,
        index=first_index,
        v1_portfolio=v1,
        clock_epoch=first,
    )
    assert not should_exit
    assert position.armed
    assert math.isclose(position.peak_return, 0.003, abs_tol=1e-12)

    second_index = make_index({second: 100.15})
    should_exit, reason, ret, exit_fill, _fill = live.should_exit_overlay(
        variant="smooth_survivor_armed20_floor80",
        leg=leg,
        position=position,
        index=second_index,
        v1_portfolio=v1,
        clock_epoch=second,
    )

    expected = research_exit(live.variant_config("smooth_survivor_armed20_floor80"), [0.0, 0.003, 0.0015])
    assert should_exit
    assert reason == expected["exit_reason"] == "armed_peak_floor"
    assert math.isclose(float(ret), float(expected["exit_return"]), abs_tol=1e-12)
    assert math.isclose(float(exit_fill), float(expected["exit_price"]), abs_tol=1e-12)


def test_profit_exit_takes_priority_over_same_clock_underlying_t2_exit() -> None:
    clock = epoch_at(10, 5)
    leg = make_leg(exit_epoch=clock)
    position = make_position("smooth_survivor_profit25")
    index = make_index({clock: 100.30})

    should_exit, reason, ret, exit_fill, _fill = live.should_exit_overlay(
        variant="smooth_survivor_profit25",
        leg=leg,
        position=position,
        index=index,
        v1_portfolio=DummyV1Portfolio(),
        clock_epoch=clock,
    )

    expected = research_exit(live.variant_config("smooth_survivor_profit25"), [0.0, 0.003])
    assert should_exit
    assert reason == expected["exit_reason"] == "profit_capture"
    assert math.isclose(float(ret), float(expected["exit_return"]), abs_tol=1e-12)
    assert math.isclose(float(exit_fill), float(expected["exit_price"]), abs_tol=1e-12)


def test_profit_stop_variant_exits_on_adverse_stop() -> None:
    clock = epoch_at(10, 5)
    variant = "smooth_survivor_profit25_age0_max5_stop100_requal_cd0_max3"
    leg = make_leg()
    position = make_position(variant)
    index = make_index({clock: 98.80})

    should_exit, reason, ret, exit_fill, _fill = live.should_exit_overlay(
        variant=variant,
        leg=leg,
        position=position,
        index=index,
        v1_portfolio=DummyV1Portfolio(),
        clock_epoch=clock,
    )

    assert should_exit
    assert reason == "adverse_stop"
    assert math.isclose(float(ret), -0.012, abs_tol=1e-12)
    assert math.isclose(float(exit_fill), 98.80, abs_tol=1e-12)


def test_armed_stop_variant_exits_on_adverse_stop_before_arming() -> None:
    clock = epoch_at(10, 5)
    variant = "smooth_survivor_armed20_floor80_age0_max5_stop100_requal_cd0_max3"
    leg = make_leg()
    position = make_position(variant)
    index = make_index({clock: 98.80})

    should_exit, reason, ret, exit_fill, _fill = live.should_exit_overlay(
        variant=variant,
        leg=leg,
        position=position,
        index=index,
        v1_portfolio=DummyV1Portfolio(),
        clock_epoch=clock,
    )

    assert should_exit
    assert reason == "adverse_stop"
    assert math.isclose(float(ret), -0.012, abs_tol=1e-12)
    assert math.isclose(float(exit_fill), 98.80, abs_tol=1e-12)


def test_armed_position_exits_at_session_close_when_floor_not_hit() -> None:
    armed_clock = epoch_at(14, 0)
    close_clock = epoch_at(15, 30)
    leg = make_leg()
    position = make_position()

    live.should_exit_overlay(
        variant="smooth_survivor_armed20_floor80",
        leg=leg,
        position=position,
        index=make_index({armed_clock: 100.30}),
        v1_portfolio=DummyV1Portfolio(),
        clock_epoch=armed_clock,
    )
    should_exit, reason, ret, exit_fill, _fill = live.should_exit_overlay(
        variant="smooth_survivor_armed20_floor80",
        leg=leg,
        position=position,
        index=make_index({close_clock: 100.28}),
        v1_portfolio=DummyV1Portfolio(),
        clock_epoch=close_clock,
    )

    assert should_exit
    assert reason == "armed_session_close"
    assert math.isclose(float(ret), 0.0028, abs_tol=1e-12)
    assert math.isclose(float(exit_fill), 100.28, abs_tol=1e-12)


def make_research_candidate(*, exit_reason: str = "open_at_period_end") -> research_portfolios.Candidate:
    entry = epoch_at(14, 0)
    mark = epoch_at(15, 30)
    window = pd.DataFrame(
        {
            "clock_epoch": [entry, entry + 60, mark],
            "clock_time": [live.epoch_ist_iso(entry), live.epoch_ist_iso(entry + 60), live.epoch_ist_iso(mark)],
            "forward_return": [0.0, 0.001, 0.002],
            "score_smooth_survivor": [0.91, 0.91, 0.91],
            "quote_history_mode": ["research_full_session_quote_index"] * 3,
            "quote_history_key_scope": ["all_candidate_keys"] * 3,
            "ram_60_available_from_epoch": [entry] * 3,
            "ram_60_available_from": [live.epoch_ist_iso(entry)] * 3,
        }
    )
    return research_portfolios.Candidate(
        variant="smooth_survivor_armed20_floor80",
        policy_name="smooth_survivor_tight_risk_score0p80_age60to240_runway0",
        overlay="armed20bps_floor80pct_peak",
        row_id="TEST|T2|pos-1|1",
        symbol="TEST",
        side="long",
        entry_epoch=entry,
        exit_epoch=mark,
        exit_reason=exit_reason,
        entry_fill_price=100.0,
        exit_fill_price=100.20,
        margin_per_lot=100000.0,
        lot_size=100,
        score=0.91,
        score_column="score_smooth_survivor",
        window=window,
    )


def test_open_at_period_end_candidate_remains_open_in_portfolio_simulation() -> None:
    candidate = make_research_candidate()

    result = research_portfolios.run_portfolio(
        variant=candidate.variant,
        candidates=[candidate],
        max_positions=3,
        initial_capital=2_000_000.0,
        v1_portfolio=DummyV1Portfolio(),
        sizing_mode="fixed_entry_margin_unconstrained",
        fixed_entry_margin=500_000.0,
        replacement_policy=research_portfolios.ReplacementPolicy(name="none", enabled=False),
    )

    assert result["summary"]["current_open_positions"] == 1
    assert result["summary"]["closed_trades"] == 0
    assert [row["event"] for row in result["transactions"]] == ["entry"]


def test_research_portfolio_transactions_and_installed_holdings_keep_quote_history_metadata() -> None:
    candidate = make_research_candidate()
    result = research_portfolios.run_portfolio(
        variant=candidate.variant,
        candidates=[candidate],
        max_positions=3,
        initial_capital=2_000_000.0,
        v1_portfolio=DummyV1Portfolio(),
        sizing_mode="fixed_entry_margin_unconstrained",
        fixed_entry_margin=500_000.0,
        replacement_policy=research_portfolios.ReplacementPolicy(name="none", enabled=False),
    )

    entry = result["transactions"][0]
    assert entry["quote_history_mode"] == "research_full_session_quote_index"
    assert entry["quote_history_key_scope"] == "all_candidate_keys"
    assert entry["ram_60_available_from_epoch"] == candidate.entry_epoch

    portfolio = installer.portfolio_state_from_research_result(
        variant=candidate.variant,
        result=result,
        initial_capital=2_000_000.0,
    )
    holding = next(iter(portfolio["holdings"].values()))
    tx = portfolio["transactions"][0]
    assert tx["quote_history_mode"] == "research_full_session_quote_index"
    assert tx["ram_60_available_from_epoch"] == candidate.entry_epoch
    assert holding["quote_history_mode"] == "research_full_session_quote_index"
    assert holding["ram_60_available_from"] == live.epoch_ist_iso(candidate.entry_epoch)


def test_research_path_lookup_keeps_quote_history_metadata_for_candidates() -> None:
    entry = epoch_at(10, 0)
    exit_epoch = epoch_at(10, 2)
    frame = pd.DataFrame(
        {
            "row_id": ["TEST|T2|pos-1|1"] * 3,
            "symbol": ["TEST"] * 3,
            "side": ["long"] * 3,
            "clock_epoch": [entry, entry + 60, exit_epoch],
            "clock_time": [live.epoch_ist_iso(entry), live.epoch_ist_iso(entry + 60), live.epoch_ist_iso(exit_epoch)],
            "current_ret": [0.0010, 0.0020, 0.0030],
            "t2_exit_epoch": [exit_epoch] * 3,
            "entry_fill_price": [100.0] * 3,
            "margin_per_lot": [100000.0] * 3,
            "lot_size": [100] * 3,
            "score_smooth_survivor": [0.91] * 3,
            "quote_history_mode": ["research_full_session_quote_index"] * 3,
            "quote_history_key_scope": ["all_t2_ledger_keys"] * 3,
            "ram_60_available_from_epoch": [entry] * 3,
            "ram_60_available_from": [live.epoch_ist_iso(entry)] * 3,
        }
    )
    lookup = mfe_research.build_path_lookup(frame)
    row = frame.iloc[0]
    metrics = mfe_research.path_metrics(row, lookup, 0.0005, include_window=True)

    assert metrics["ok"]
    window = metrics["window"]
    assert window.iloc[0]["quote_history_mode"] == "research_full_session_quote_index"
    assert window.iloc[0]["quote_history_key_scope"] == "all_t2_ledger_keys"
    assert window.iloc[0]["ram_60_available_from_epoch"] == entry


def test_open_at_period_end_candidate_remains_active_in_installed_state() -> None:
    candidate = make_research_candidate()
    state, overlay_events, matrix_payloads, report = installer.overlay_state_from_candidates(
        candidates_by_variant={candidate.variant: [candidate]},
        variants=(candidate.variant,),
        primary_variant=candidate.variant,
        final_epoch=candidate.exit_epoch,
    )

    assert report["active_overlay_count"] == 1
    assert report["completed_overlay_count"] == 0
    assert len(state["active_overlay"]) == 1
    assert [row["event"] for row in overlay_events] == ["overlay_entry"]
    assert [row["event_type"] for row in matrix_payloads] == ["paper_entry"]


def test_installer_matrix_payload_falls_back_to_position_entry_metadata_for_exit() -> None:
    candidate = make_research_candidate(exit_reason="armed_peak_floor")
    position = live.OverlayPosition(
        variant=candidate.variant,
        row_id="TEST|T2|pos-1|1",
        position_id="pos-1",
        symbol=candidate.symbol,
        side=candidate.side,
        entry_epoch=int(candidate.entry_epoch),
        entry_time=live.epoch_ist_iso(candidate.entry_epoch),
        entry_fill_price=float(candidate.entry_fill_price),
        entry_ltp_price=float(candidate.entry_fill_price),
        entry_score=float(candidate.score),
        entry_features={
            "quote_history_mode": "research_full_session_quote_index",
            "quote_history_key_scope": "all_candidate_keys",
            "ram_60_available_from_epoch": epoch_at(13, 50),
            "ram_60_available_from": live.epoch_ist_iso(epoch_at(13, 50)),
        },
    )

    payload = installer.matrix_payload_from_candidate(
        candidate=candidate,
        event_type="paper_exit",
        event_epoch=int(candidate.exit_epoch),
        event_price=float(candidate.exit_fill_price),
        position=position,
        exit_reason=candidate.exit_reason,
    )

    assert payload["quote_history_mode"] == "research_full_session_quote_index"
    assert payload["quote_history_key_scope"] == "all_candidate_keys"
    assert payload["ram_60_available_from_epoch"] == epoch_at(13, 50)
    assert payload["ram_60_available_from"] == live.epoch_ist_iso(epoch_at(13, 50))
