from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path


def load_audit_module():
    script = Path(__file__).resolve().parents[1] / "scripts" / "audit_v2_post_install.py"
    spec = importlib.util.spec_from_file_location("audit_v2_post_install", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def create_minimal_audit_fixture(tmp_path: Path, *, matrix_active: bool) -> dict[str, Path | str]:
    state_dir = tmp_path / "v2_state"
    matrix_dir = tmp_path / "matrix_state"
    override_path = tmp_path / "override.json"
    position_id = "OBVFUTPORT_V2_PASSIVE:NMDC:long:1787565300:test:position"
    signal_id = position_id.removesuffix(":position")

    write_jsonl(
        state_dir / "instruments" / "NMDC" / "ledger.jsonl",
        [
            {
                "event": "paper_entry",
                "symbol": "NMDC",
                "position": {
                    "position_id": position_id,
                    "signal_id": signal_id,
                    "entry_epoch": 1787565300,
                    "entry_time": "2026-08-24T15:25:00+05:30",
                    "symbol": "NMDC",
                },
            },
            {
                "event": "paper_exit",
                "position_id": position_id,
                "signal_id": signal_id,
                "symbol": "NMDC",
                "exit_epoch": 1787720400,
                "exit_time": "2026-08-26T10:30:00+05:30",
                "two_lot_ttsl": {
                    "tranche2": {
                        "status": "closed",
                        "exit_epoch": 1787720400,
                        "exit_time": "2026-08-26T10:30:00+05:30",
                    }
                },
            },
        ],
    )
    write_json(
        matrix_dir / "matrix_state.json",
        {
            "event_count": 2,
            "instruments": {
                "NMDC": {
                    "active_position_id": position_id if matrix_active else None,
                }
            },
        },
    )
    write_jsonl(matrix_dir / "matrix_events.jsonl", [{"event_id": "entry"}, {"event_id": "exit"}])
    write_json(
        matrix_dir / "matrix_v2_bridge_state.json",
        {
            "selected_exited_position_ids": [position_id],
            "active_position_by_symbol": {"NMDC": position_id} if matrix_active else {},
            "last_result": {"accepted": 0, "failed": 0},
        },
    )
    write_json(override_path, {"NMDC": {"adaptive_calibration": {"status": "active"}}})
    return {
        "state_dir": state_dir,
        "matrix_dir": matrix_dir,
        "override_path": override_path,
        "position_id": position_id,
    }


def run_audit(fixture: dict[str, Path | str]) -> subprocess.CompletedProcess[str]:
    script = Path(__file__).resolve().parents[1] / "scripts" / "audit_v2_post_install.py"
    return subprocess.run(
        [
            sys.executable,
            str(script),
            "--state-dir",
            str(fixture["state_dir"]),
            "--matrix-state-dir",
            str(fixture["matrix_dir"]),
            "--override",
            str(fixture["override_path"]),
            "--expected-symbols",
            "1",
            "--expected-adaptive",
            "1",
            "--quarantined",
            "",
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_post_install_audit_rejects_exited_matrix_active_position(tmp_path):
    fixture = create_minimal_audit_fixture(tmp_path, matrix_active=True)

    result = run_audit(fixture)
    payload = json.loads(result.stdout)

    assert result.returncode == 2
    assert payload["matrix"]["active_exited_contradiction_count"] == 1
    assert payload["matrix"]["extra_active_t2_count"] == 1
    assert "matrix_active_exited_contradiction" in payload["failures"]
    assert "matrix_active_t2_mismatch" in payload["failures"]


def test_post_install_audit_accepts_matrix_active_matching_v2_ledgers(tmp_path):
    fixture = create_minimal_audit_fixture(tmp_path, matrix_active=False)

    result = run_audit(fixture)
    payload = json.loads(result.stdout)

    assert result.returncode == 0
    assert payload["ok"] is True
    assert payload["matrix"]["actual_active_t2_count"] == 0
    assert payload["matrix"]["expected_active_t2_count"] == 0
