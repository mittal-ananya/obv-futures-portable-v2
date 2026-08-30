from __future__ import annotations

import json
import sys

from scripts import report_tranche_performance as report


def append_jsonl(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def test_report_dedupes_closed_t2_lifecycle_rows(tmp_path, monkeypatch):
    state_dir = tmp_path / "state"
    instrument_dir = state_dir / "instruments" / "MARUTI"
    position_id = "OBVFUTPORT_V2_SELECTED:MARUTI:short:1786438200:test"
    instrument_dir.mkdir(parents=True)
    append_jsonl(
        instrument_dir / "ledger.jsonl",
        [
            {
                "event": "paper_exit",
                "position_id": position_id,
                "symbol": "MARUTI",
                "side": "short",
                "entry_epoch": 1786438260,
                "entry_time": "2026-08-11T14:21:00+05:30",
                "exit_epoch": 1787306399,
                "exit_time": "2026-08-21T15:29:59+05:30",
                "net_rupees": 1.0,
                "two_lot_ttsl": {
                    "tranche2": {
                        "status": "closed",
                        "entry_epoch": 1786438260,
                        "entry_time": "2026-08-11T14:21:00+05:30",
                        "exit_epoch": 1787306399,
                        "exit_time": "2026-08-21T15:29:59+05:30",
                        "exit_reason": "ttsl_exit",
                        "net_rupees": 10.0,
                    }
                },
            },
            {
                "event": "tranche2_exit",
                "position_id": position_id,
                "symbol": "MARUTI",
                "side": "short",
                "entry_epoch": 1786438260,
                "entry_time": "2026-08-11T14:21:00+05:30",
                "exit_epoch": 1787630134,
                "exit_time": "2026-08-25T11:55:34+05:30",
                "exit_reason": "tranche2_delayed_ttsl",
                "net_rupees": 20.0,
            },
        ],
    )
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report_tranche_performance.py",
            "--state-dir",
            str(state_dir),
            "--start-date",
            "2026-08-10",
            "--end-date",
            "2026-08-28",
            "--output",
            str(output),
        ],
    )

    assert report.main() == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    t2_rows = payload["rows_by_tranche"]["T2"]

    assert len(t2_rows) == 1
    assert t2_rows[0]["exit_reason"] == "ttsl_exit"
    assert payload["summary"]["T2"]["rows"] == 1
