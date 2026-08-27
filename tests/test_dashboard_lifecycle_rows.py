from __future__ import annotations

import json
import sys
import types


try:
    from obvfut_portable_v2 import dashboard
except ModuleNotFoundError as exc:
    if exc.name != "fastapi":
        raise

    fastapi_stub = types.ModuleType("fastapi")

    class FastAPI:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            return lambda func: func

        def head(self, *args, **kwargs):
            return lambda func: func

        def post(self, *args, **kwargs):
            return lambda func: func

    class HTTPException(Exception):
        pass

    class Response:
        def __init__(self, *args, **kwargs):
            pass

    responses_stub = types.ModuleType("fastapi.responses")
    responses_stub.HTMLResponse = Response
    responses_stub.JSONResponse = Response
    responses_stub.Response = Response
    fastapi_stub.FastAPI = FastAPI
    fastapi_stub.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi_stub
    sys.modules["fastapi.responses"] = responses_stub

    from obvfut_portable_v2 import dashboard


def append_jsonl(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def test_closed_t2_ledger_row_supersedes_model_state_open_row(tmp_path, monkeypatch):
    state_dir = tmp_path / "state"
    root_dir = tmp_path / "root"
    instrument_dir = state_dir / "instruments" / "INDUSTOWER"
    position_id = "OBVFUTPORT_V2_PASSIVE:INDUSTOWER:long:1787630700:test:position"
    signal_id = position_id.removesuffix(":position")
    instrument_dir.mkdir(parents=True)
    (instrument_dir / "model_state.json").write_text(
        json.dumps(
            {
                "position": {
                    "position_id": position_id,
                    "signal_id": signal_id,
                    "symbol": "INDUSTOWER",
                    "side": "long",
                    "entry_epoch": 1787630700,
                    "entry_time": "2026-08-25T09:36:00+05:30",
                    "entry_price": 358.5,
                    "latest_epoch": 1787803365,
                    "latest_time": "2026-08-27T09:32:45+05:30",
                    "latest_price": 360.0,
                    "two_lot_ttsl": {
                        "tranche2": {
                            "status": "open",
                            "entry_epoch": 1787630700,
                            "entry_time": "2026-08-25T09:36:00+05:30",
                            "entry_price": 358.5,
                            "latest_epoch": 1787803365,
                            "latest_time": "2026-08-27T09:32:45+05:30",
                            "latest_price": 360.0,
                        }
                    },
                }
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    append_jsonl(
        instrument_dir / "ledger.jsonl",
        [
            {
                "event": "tranche2_exit",
                "position_id": position_id,
                "signal_id": signal_id,
                "symbol": "INDUSTOWER",
                "side": "long",
                "entry_epoch": 1787630700,
                "entry_time": "2026-08-25T09:36:00+05:30",
                "entry_price": 358.5,
                "exit_epoch": 1787803375,
                "exit_time": "2026-08-27T09:32:55+05:30",
                "exit_price": 359.8,
                "exit_reason": "tranche2_delayed_ttsl",
                "net_rupees": 100.0,
            }
        ],
    )
    monkeypatch.setattr(dashboard, "STATE_DIR", state_dir)
    monkeypatch.setattr(dashboard, "ROOT_DIR", root_dir)
    monkeypatch.setattr(dashboard, "START_DATE", "2026-08-10")
    monkeypatch.setattr(dashboard, "_CLOCK_DIAGNOSTIC_CACHE", {"trade_date": None, "path": None, "offset": 0, "size": 0, "rows": [], "seen": set(), "clock_epochs": set()})

    loaded = dashboard.load_rows()
    t2_rows = loaded["rows_by_tranche"]["T2"]

    assert len(t2_rows) == 1
    assert t2_rows[0]["status"] == "closed"
    assert t2_rows[0]["exit_epoch"] == 1787803375
