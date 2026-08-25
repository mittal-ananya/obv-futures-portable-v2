#!/usr/bin/env python3
"""Generate a lightweight OBVFUTPORT-v2 state manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def file_sha256(path: Path) -> str | None:
    try:
        h = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def count_jsonl(path: Path) -> int:
    try:
        with path.open("rb") as handle:
            return sum(1 for line in handle if line.strip())
    except OSError:
        return 0


def instrument_snapshot(instrument_dir: Path) -> dict[str, Any]:
    ledger = instrument_dir / "ledger.jsonl"
    model_state = read_json(instrument_dir / "model_state.json", {})
    open_rows = []
    if isinstance(model_state, dict):
        for key in ("open_positions", "open_tranches", "active_positions"):
            value = model_state.get(key)
            if isinstance(value, list):
                open_rows.extend(value)
            elif isinstance(value, dict):
                open_rows.extend(value.values())
    return {
        "symbol": instrument_dir.name,
        "ledger_rows": count_jsonl(ledger),
        "ledger_sha256": file_sha256(ledger),
        "model_state_sha256": file_sha256(instrument_dir / "model_state.json"),
        "open_rows_detected": len(open_rows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
    parser.add_argument("--matrix-state-dir", default="/opt/cloud-deploy-candidates/matrix-v1/state")
    parser.add_argument("--override", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    state_dir = Path(args.state_dir)
    matrix_state_dir = Path(args.matrix_state_dir)
    override_path = Path(args.override) if args.override else state_dir / "adaptive_calibration" / "v2_symbol_overrides_latest.json"
    override = read_json(override_path, {})
    instruments_dir = state_dir / "instruments"
    instruments = sorted(path for path in instruments_dir.iterdir() if path.is_dir()) if instruments_dir.exists() else []
    instrument_rows = [instrument_snapshot(path) for path in instruments]
    decision_dir = state_dir / "decision_events"
    decision_files = sorted(decision_dir.glob("decision_events_*.jsonl")) if decision_dir.exists() else []
    matrix_files = {
        "matrix_state": matrix_state_dir / "matrix_state.json",
        "matrix_events": matrix_state_dir / "matrix_events.jsonl",
        "bridge_state": matrix_state_dir / "matrix_v2_bridge_state.json",
    }
    manifest = {
        "schema": "obvfutport_v2.state_manifest.v1",
        "label": args.label,
        "generated_at_ist": datetime.now().astimezone().replace(microsecond=0).isoformat(),
        "state_dir": str(state_dir),
        "matrix_state_dir": str(matrix_state_dir),
        "adaptive_override": {
            "path": str(override_path),
            "sha256": file_sha256(override_path),
            "version": override.get("version") if isinstance(override, dict) else None,
            "symbol_count": len((override.get("symbols") or {})) if isinstance(override, dict) else 0,
            "quarantined_symbols": sorted(
                symbol
                for symbol, item in ((override.get("symbols") or {}).items() if isinstance(override, dict) else [])
                if isinstance(item, dict) and isinstance(item.get("quarantine"), dict)
            ),
        },
        "instruments": {
            "count": len(instrument_rows),
            "ledger_rows_total": sum(int(row["ledger_rows"]) for row in instrument_rows),
            "open_rows_detected_total": sum(int(row["open_rows_detected"]) for row in instrument_rows),
            "rows": instrument_rows,
        },
        "decision_events": {
            "files": [
                {"path": str(path), "rows": count_jsonl(path), "sha256": file_sha256(path)}
                for path in decision_files
            ],
        },
        "matrix": {
            name: {"path": str(path), "exists": path.exists(), "rows": count_jsonl(path) if path.suffix == ".jsonl" else None, "sha256": file_sha256(path)}
            for name, path in matrix_files.items()
        },
    }
    output = Path(args.output) if args.output else state_dir / "reports" / "v2_state_manifest_latest.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(output)
    print(json.dumps({"ok": True, "output": str(output), "instrument_count": len(instrument_rows)}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
