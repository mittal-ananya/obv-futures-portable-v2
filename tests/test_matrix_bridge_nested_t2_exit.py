from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path


def load_sync_module():
    script = Path(
        os.environ.get(
            "MATRIX_SYNC_FROM_V2_SCRIPT",
            Path(__file__).resolve().parents[1] / "matrix_v2_adapter" / "scripts" / "sync_from_v2.py",
        )
    )
    spec = importlib.util.spec_from_file_location("matrix_sync_from_v2", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def test_paper_exit_with_nested_closed_t2_posts_selected_t2_exit(tmp_path, monkeypatch):
    module = load_sync_module()
    v2_state = tmp_path / "v2_state"
    matrix_root = tmp_path / "matrix"
    position_id = "OBVFUTPORT_V2_PASSIVE:NMDC:long:1787565300:roll:test:position"
    signal_id = position_id.removesuffix(":position")
    append_jsonl(
        v2_state / "instruments" / "NMDC" / "ledger.jsonl",
        [
            {
                "event": "paper_entry",
                "position": {
                    "position_id": position_id,
                    "signal_id": signal_id,
                    "symbol": "NMDC",
                    "side": "long",
                    "entry_epoch": 1787565300,
                    "entry_time": "2026-08-24T15:25:00+05:30",
                    "signal_price": 86.29,
                    "signal_instrument_key": "NSE:NMDC",
                    "signal_source": "cash",
                    "instrument_key": "NFO:NMDC26SEPFUT",
                },
            },
            {
                "event": "paper_exit",
                "position_id": position_id,
                "signal_id": signal_id,
                "symbol": "NMDC",
                "side": "long",
                "exit_epoch": 1787720400,
                "exit_time": "2026-08-26T10:30:00+05:30",
                "exit_price": 86.91,
                "exit_reason": "profit_trailing_sl",
                "signal_instrument_key": "NSE:NMDC",
                "signal_source": "cash",
                "instrument_key": "NFO:NMDC26SEPFUT",
                "two_lot_ttsl": {
                    "tranche2": {
                        "status": "closed",
                        "exit_epoch": 1787720400,
                        "exit_time": "2026-08-26T10:30:00+05:30",
                        "exit_price": 86.91,
                        "exit_reason": "profit_trailing_sl",
                    }
                },
            },
        ],
    )
    posted_payloads: list[dict] = []
    monkeypatch.setattr(module, "V2_STATE_ROOT", v2_state)
    monkeypatch.setattr(module, "MATRIX_ROOT", matrix_root)
    monkeypatch.setattr(module, "BRIDGE_STATE_PATH", matrix_root / "state" / "matrix_v2_bridge_state.json")
    monkeypatch.setattr(module, "post_matrix_batch", lambda payloads, dry_run=False: posted_payloads.extend(payloads) or True)

    result = module.sync_once()

    assert result["failed"] == 0
    assert any(payload["event_type"] == "tranche2_exit" for payload in posted_payloads)
    assert not any(payload.get("matrix_selected_leg") == "T1_base_exit" for payload in posted_payloads)
    exit_payload = [payload for payload in posted_payloads if payload["event_type"] == "tranche2_exit"][0]
    assert exit_payload["event_id"] == f"MATRIX:v2:selected_t2_exit:{position_id}:1787720400"
    assert exit_payload["position_closed"] is True

    bridge_state = json.loads((matrix_root / "state" / "matrix_v2_bridge_state.json").read_text(encoding="utf-8"))
    assert bridge_state["active_position_by_symbol"] == {}
    assert position_id in bridge_state["selected_exited_position_ids"]
