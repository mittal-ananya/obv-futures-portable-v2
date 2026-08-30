from __future__ import annotations

from datetime import datetime, timedelta, timezone

from scripts.score_t1_t2_exit_candidates_risk_first import PathArrays, entry_row_for_due


IST = timezone(timedelta(hours=5, minutes=30))


def _row(epoch: int, *, price: float, bid: float, ask: float, received_offset: float) -> dict[str, object]:
    return {
        "epoch_second": epoch,
        "price": price,
        "bid": bid,
        "ask": ask,
        "received_epoch": float(epoch) + float(received_offset),
    }


def test_entry_row_for_due_prefers_first_arriving_boundary_quote() -> None:
    due = int(datetime(2026, 8, 28, 15, 6, tzinfo=IST).timestamp())
    rows = PathArrays()
    rows.append(_row(due - 1, price=12229.0, bid=12223.0, ask=12232.0, received_offset=1.452))
    rows.append(_row(due, price=12229.0, bid=12221.0, ask=12232.0, received_offset=1.025))

    entry_idx, entry_row = entry_row_for_due(
        rows,
        due_epoch=due,
        max_carry_age_seconds=45.0,
        receive_grace_seconds=2.0,
    )

    assert entry_idx == 1
    assert entry_row is not None
    assert entry_row["epoch_second"] == due
    assert entry_row["source_epoch_second"] == due - 1
    assert entry_row["bid"] == 12223.0
    assert entry_row["ask"] == 12232.0


def test_entry_row_for_due_uses_exact_second_when_it_arrived_first() -> None:
    due = int(datetime(2026, 8, 28, 15, 6, tzinfo=IST).timestamp())
    rows = PathArrays()
    rows.append(_row(due - 1, price=12229.0, bid=12223.0, ask=12232.0, received_offset=1.452))
    rows.append(_row(due, price=12229.0, bid=12221.0, ask=12232.0, received_offset=0.100))

    entry_idx, entry_row = entry_row_for_due(
        rows,
        due_epoch=due,
        max_carry_age_seconds=45.0,
        receive_grace_seconds=2.0,
    )

    assert entry_idx == 1
    assert entry_row is not None
    assert entry_row.get("source_epoch_second") is None
    assert entry_row["bid"] == 12221.0
