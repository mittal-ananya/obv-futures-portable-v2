#!/usr/bin/env python3
"""Read-only post-install audit for OBVFUTPORT-v2 adaptive state."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    rows.append(item)
    except OSError:
        return rows
    return rows


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def as_epoch(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def first_present(row: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def nested_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    out = [row]
    for value in row.values():
        if isinstance(value, dict):
            out.append(value)
    return out


def audit_t3_rows(symbol: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    by_position: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(rows):
        pid = str(row.get("position_id") or row.get("signal_id") or "")
        position = row.get("position") if isinstance(row.get("position"), dict) else {}
        pid = pid or str(position.get("position_id") or position.get("signal_id") or "")
        if not pid:
            continue
        merged = by_position.setdefault(pid, {"symbol": symbol, "position_id": pid})
        for item in nested_rows(row) + nested_rows(position):
            for key, value in item.items():
                lowered = str(key).lower()
                if "tranche3" in lowered or lowered.startswith("t3_") or "t3" in lowered:
                    merged.setdefault(key, value)
            if row.get("event") == "tranche3_entry":
                merged.setdefault("t3_entry_epoch", first_present(item, ("entry_epoch", "t3_entry_epoch", "tranche3_entry_epoch")))
                merged.setdefault("t3_entry_price", first_present(item, ("entry_price", "fill_price", "t3_entry_price", "tranche3_entry_price")))
            if row.get("event") == "tranche3_exit":
                merged.setdefault("t3_exit_epoch", first_present(item, ("exit_epoch", "t3_exit_epoch", "tranche3_exit_epoch")))
                merged.setdefault("t3_exit_price", first_present(item, ("exit_price", "fill_price", "t3_exit_price", "tranche3_exit_price")))
        merged.setdefault("_last_row_index", idx)

    for state in by_position.values():
        entry_epoch = as_epoch(first_present(state, ("t3_entry_epoch", "tranche3_entry_epoch", "tranche3_entry_time_epoch")))
        exit_epoch = as_epoch(first_present(state, ("t3_exit_epoch", "tranche3_exit_epoch", "tranche3_exit_time_epoch")))
        selected_t2_exit_epoch = as_epoch(first_present(state, ("selected_t2_exit_epoch", "t2_exit_epoch", "tranche2_exit_epoch")))
        if exit_epoch is not None and entry_epoch is None:
            issues.append({"symbol": symbol, "position_id": state["position_id"], "issue": "t3_exit_without_entry"})
        if exit_epoch is not None and entry_epoch is not None and exit_epoch < entry_epoch:
            issues.append({"symbol": symbol, "position_id": state["position_id"], "issue": "t3_exit_before_entry"})
        if entry_epoch is not None and selected_t2_exit_epoch is not None and entry_epoch > selected_t2_exit_epoch:
            issues.append({"symbol": symbol, "position_id": state["position_id"], "issue": "t3_entry_after_t2_exit"})
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
    parser.add_argument("--matrix-state-dir", default="/opt/cloud-deploy-candidates/matrix-v1/state")
    parser.add_argument("--override", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/adaptive_calibration/v2_symbol_overrides_latest.json")
    parser.add_argument("--expected-symbols", type=int, default=212)
    parser.add_argument("--expected-adaptive", type=int, default=209)
    parser.add_argument("--quarantined", default="IOC,MAXHEALTH,WAAREEENER")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    state_dir = Path(args.state_dir)
    matrix_state_dir = Path(args.matrix_state_dir)
    override_path = Path(args.override)
    override = read_json(override_path, {})
    records = override.get("symbols") if isinstance(override.get("symbols"), dict) else override
    if not isinstance(records, dict):
        records = {}
    quarantined = {item.strip().upper() for item in str(args.quarantined).split(",") if item.strip()}

    symbol_records = {str(sym).upper(): rec for sym, rec in records.items() if isinstance(rec, dict)}
    adaptive_symbols: set[str] = set()
    baseline_quarantine: set[str] = set()
    for sym, rec in symbol_records.items():
        meta = rec.get("adaptive_calibration") if isinstance(rec.get("adaptive_calibration"), dict) else rec
        tags = set(meta.get("tags") or rec.get("tags") or [])
        status = str(meta.get("status") or rec.get("status") or "")
        if sym in quarantined or "quarantined_baseline_kept" in tags or "quarantined" in status:
            baseline_quarantine.add(sym)
        else:
            adaptive_symbols.add(sym)

    instrument_dirs = sorted(path for path in (state_dir / "instruments").glob("*") if path.is_dir())
    event_type_counts: dict[str, int] = {}
    ledger_rows = 0
    open_positions = 0
    symbols_with_ledger = 0
    t3_issues: list[dict[str, Any]] = []
    missing_ledger_symbols: list[str] = []
    for sym_dir in instrument_dirs:
        ledger = sym_dir / "ledger.jsonl"
        rows = iter_jsonl(ledger)
        if rows:
            symbols_with_ledger += 1
        else:
            missing_ledger_symbols.append(sym_dir.name)
        ledger_rows += len(rows)
        for row in rows:
            event = str(row.get("event") or row.get("event_type") or "unknown")
            event_type_counts[event] = event_type_counts.get(event, 0) + 1
            position = row.get("position") if isinstance(row.get("position"), dict) else {}
            if row.get("event") == "paper_entry" and not bool(position.get("closed") or row.get("position_closed")):
                open_positions += 1
        t3_issues.extend(audit_t3_rows(sym_dir.name, rows))

    matrix_state = read_json(matrix_state_dir / "matrix_state.json", {})
    bridge_state = read_json(matrix_state_dir / "matrix_v2_bridge_state.json", {})
    matrix_events_path = matrix_state_dir / "matrix_events.jsonl"
    matrix_event_count = len(iter_jsonl(matrix_events_path))
    matrix_instruments = matrix_state.get("instruments") if isinstance(matrix_state.get("instruments"), dict) else {}

    manifest = {
        "schema": "obvfutport_v2.post_install_audit.v1",
        "ok": True,
        "state_dir": str(state_dir),
        "matrix_state_dir": str(matrix_state_dir),
        "override": {
            "path": str(override_path),
            "sha256": sha256_file(override_path),
            "symbol_count": len(symbol_records),
            "adaptive_count": len(adaptive_symbols),
            "quarantined_count": len(baseline_quarantine),
            "quarantined_symbols": sorted(baseline_quarantine),
            "expected_quarantined_symbols": sorted(quarantined),
        },
        "v2_state": {
            "instrument_count": len(instrument_dirs),
            "symbols_with_ledger": symbols_with_ledger,
            "missing_ledger_symbols": missing_ledger_symbols[:50],
            "ledger_rows": ledger_rows,
            "event_type_counts": event_type_counts,
            "open_position_rows_unverified": open_positions,
            "t3_issue_count": len(t3_issues),
            "t3_issues_sample": t3_issues[:20],
        },
        "matrix": {
            "matrix_state_exists": bool((matrix_state_dir / "matrix_state.json").exists()),
            "matrix_events_exists": bool(matrix_events_path.exists()),
            "bridge_state_exists": bool((matrix_state_dir / "matrix_v2_bridge_state.json").exists()),
            "matrix_state_event_count": matrix_state.get("event_count"),
            "matrix_events_line_count": matrix_event_count,
            "matrix_instrument_count": len(matrix_instruments),
            "bridge_last_result": bridge_state.get("last_result") if isinstance(bridge_state, dict) else None,
            "matrix_state_sha256": sha256_file(matrix_state_dir / "matrix_state.json"),
            "matrix_events_sha256": sha256_file(matrix_events_path),
        },
        "updated_epoch": time.time(),
    }

    failures: list[str] = []
    if len(symbol_records) != int(args.expected_symbols):
        failures.append("override_symbol_count")
    if len(adaptive_symbols) != int(args.expected_adaptive):
        failures.append("adaptive_symbol_count")
    if baseline_quarantine != quarantined:
        failures.append("quarantine_symbol_set")
    if len(instrument_dirs) != int(args.expected_symbols):
        failures.append("instrument_count")
    if len(t3_issues) > 0:
        failures.append("impossible_t3_rows")
    if not matrix_state or matrix_event_count <= 0:
        failures.append("matrix_empty")
    manifest["ok"] = not failures
    manifest["failures"] = failures

    if args.output:
        atomic_write_json(Path(args.output), manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

