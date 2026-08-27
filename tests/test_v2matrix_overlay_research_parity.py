from __future__ import annotations

import math
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_overlay_portfolios as research_portfolios  # noqa: E402
import research_t2_overlay_variant_compare as research_compare  # noqa: E402
import run_v2matrix_overlay as live  # noqa: E402


IST = ZoneInfo("Asia/Kolkata")


class DummyV1Portfolio:
    def execution_fill_from_row(self, row, *, side, phase, point_config=None):  # noqa: ANN001
        price = float(row["price"])
        return {"fill_price": price, "ltp_price": price, "side": side, "phase": phase}


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

    assert set(live.PORTFOLIO_VARIANTS) == {"smooth_survivor_armed20_floor80", "smooth_survivor_profit25"}
    for variant in live.PORTFOLIO_VARIANTS:
        policy_name, config = selected[variant]
        assert policy_name == live.POLICY.name
        assert live.variant_config(variant) == config
    assert live.FIXED_ENTRY_MARGIN == 500_000.0
    assert live.MAX_PORTFOLIO_POSITIONS == 3


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
