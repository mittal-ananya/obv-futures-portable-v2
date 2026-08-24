#!/usr/bin/env python3
"""Verify an OBVFUTPORT-v2 install state against frozen selected candidates.

This is a hard publication gate for adaptive installs. It compares the selected
candidate rows produced by the atomic recalibration runner with the installable
v2 state that would feed the dashboard and Matrix. If they do not match
symbol-by-symbol, the install must not be published.
"""

from __future__ import annotations

import argparse
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
    out: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    out.append(row)
    except OSError:
        return []
    return out


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def as_epoch(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def round_money(value: Any) -> float | None:
    number = as_float(value)
    return round(number, 2) if number is not None else None


def normalize_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    if status in {"open", "open_mark", "marked_open", "mark", "live_open"}:
        return "open"
    if status == "closed":
        return "closed"
    return "open" if status else "closed"


def row_is_closed(row: dict[str, Any]) -> bool:
    return normalize_status(row.get("status")) == "closed"


def candidate_dirs(raw_roots: list[str]) -> list[Path]:
    roots: list[Path] = []
    for raw in raw_roots:
        path = Path(raw)
        if (path / "frozen_candidates").is_dir():
            path = path / "frozen_candidates"
        if path.is_dir():
            roots.append(path)
    return roots


def load_candidate_artifacts(roots: list[Path], symbols: set[str] | None = None) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    for root in roots:
        for path in sorted(root.glob("*.json")):
            payload = read_json(path, {})
            symbol = str(payload.get("symbol") or path.stem).upper()
            if symbols is not None and symbol not in symbols:
                continue
            if payload.get("freeze_status") != "frozen":
                continue
            best = payload.get("best_candidate")
            if not isinstance(best, dict) or not isinstance(best.get("rows"), list):
                continue
            artifacts[symbol] = payload
    return artifacts


def selected_rows_from_artifact(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    best = artifact.get("best_candidate") if isinstance(artifact.get("best_candidate"), dict) else {}
    rows = best.get("rows") if isinstance(best.get("rows"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def t2_exit_epoch(row: dict[str, Any]) -> int | None:
    specific_exit = as_epoch(row.get("t2_exit_epoch"))
    if specific_exit is not None:
        return specific_exit
    return as_epoch(row.get("exit_epoch")) if row_is_closed(row) else None


def t3_exit_epoch(row: dict[str, Any]) -> int | None:
    return as_epoch(row.get("t3_exit_epoch"))


def expected_tranche_rows(artifacts: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for symbol, artifact in artifacts.items():
        rows: list[dict[str, Any]] = []
        for row in selected_rows_from_artifact(artifact):
            side = str(row.get("side") or "").lower()
            entry_epoch = as_epoch(row.get("entry_epoch"))
            exit_epoch = as_epoch(row.get("exit_epoch"))
            status = normalize_status(row.get("status"))
            t2_epoch = t2_exit_epoch(row)
            t3_epoch = t3_exit_epoch(row)
            base = {
                "symbol": symbol,
                "side": side,
                "signal_epoch": as_epoch(row.get("signal_epoch")),
                "entry_epoch": entry_epoch,
                "exit_epoch": exit_epoch,
                "entry_fill_price": round_money(row.get("entry_fill_price")),
                "exit_fill_price": round_money(row.get("exit_fill_price")),
                "net_rupees": round_money(row.get("t1_net_rupees")),
                "status": status,
            }
            rows.append({"tranche": "T1", **base})
            rows.append(
                {
                    "tranche": "T2",
                    **base,
                    "exit_epoch": t2_epoch,
                    "exit_fill_price": round_money(row.get("t2_exit_fill_price") or row.get("exit_fill_price")) if t2_epoch is not None else None,
                    "net_rupees": round_money(row.get("t2_net_rupees")),
                    "status": "closed" if t2_epoch is not None else "open",
                }
            )
            if truthy(row.get("t3_entered")):
                rows.append(
                    {
                        "tranche": "T3",
                        "symbol": symbol,
                        "side": side,
                        "signal_epoch": as_epoch(row.get("signal_epoch")),
                        "entry_epoch": as_epoch(row.get("t3_entry_epoch")),
                        "exit_epoch": t3_epoch,
                        "entry_fill_price": round_money(row.get("t3_entry_fill_price")),
                        "exit_fill_price": round_money(row.get("t3_exit_fill_price")),
                        "net_rupees": round_money(row.get("t3_net_rupees")),
                        "status": "closed" if t3_epoch is not None else "open",
                    }
                )
        out[symbol] = sorted(rows, key=row_sort_key)
    return out


def row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("tranche") or "",
        row.get("signal_epoch") or 0,
        row.get("entry_epoch") or 0,
        row.get("exit_epoch") or 0,
        row.get("side") or "",
    )


def normalize_event_row(symbol: str, tranche: str, source: dict[str, Any], parent: dict[str, Any] | None = None) -> dict[str, Any]:
    parent = parent or {}
    side = str(source.get("side") or parent.get("side") or "").lower()
    status = normalize_status(source.get("status") or parent.get("status") or "closed")

    def inherited(key: str) -> Any:
        if key in source:
            return source.get(key)
        return parent.get(key)

    return {
        "tranche": tranche,
        "symbol": symbol,
        "side": side,
        "signal_epoch": as_epoch(inherited("signal_epoch")),
        "entry_epoch": as_epoch(inherited("entry_epoch")),
        "exit_epoch": as_epoch(inherited("exit_epoch")),
        "entry_fill_price": round_money(inherited("entry_fill_price")),
        "exit_fill_price": round_money(inherited("exit_fill_price")),
        "net_rupees": round_money(source.get("net_rupees") if source.get("net_rupees") is not None else parent.get("net_rupees")),
        "status": status,
    }


def installed_tranche_rows(state_dir: Path, symbols: set[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for ledger_path in sorted((state_dir / "instruments").glob("*/ledger.jsonl")):
        symbol = ledger_path.parent.name.upper()
        if symbols is not None and symbol not in symbols:
            continue
        rows: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()

        def add(row: dict[str, Any]) -> None:
            key = (
                row.get("tranche"),
                row.get("signal_epoch"),
                row.get("entry_epoch"),
                row.get("exit_epoch"),
                row.get("side"),
                row.get("status"),
            )
            if key in seen:
                return
            seen.add(key)
            rows.append(row)

        model = read_json(ledger_path.parent / "model_state.json", {})
        position = model.get("position") if isinstance(model.get("position"), dict) else {}
        if position:
            add(normalize_event_row(symbol, "T1", position, {"status": "open"}))
            ttsl = position.get("two_lot_ttsl") if isinstance(position.get("two_lot_ttsl"), dict) else {}
            tranche2 = ttsl.get("tranche2") if isinstance(ttsl.get("tranche2"), dict) else {}
            if tranche2:
                add(normalize_event_row(symbol, "T2", tranche2, {**position, "status": tranche2.get("status") or "open"}))
            tranche3 = position.get("tranche3") if isinstance(position.get("tranche3"), dict) else {}
            if tranche3 and as_epoch(tranche3.get("entry_epoch")) is not None:
                add(normalize_event_row(symbol, "T3", tranche3, {**position, "status": tranche3.get("status") or "open"}))

        for event in iter_jsonl(ledger_path):
            event_type = str(event.get("event") or "")
            if event_type == "paper_exit":
                add(normalize_event_row(symbol, "T1", event, {"status": "closed"}))
                ttsl = event.get("two_lot_ttsl") if isinstance(event.get("two_lot_ttsl"), dict) else {}
                tranche2 = ttsl.get("tranche2") if isinstance(ttsl.get("tranche2"), dict) else {}
                if tranche2:
                    add(normalize_event_row(symbol, "T2", tranche2, {**event, "status": "closed"}))
                tranche3 = event.get("tranche3") if isinstance(event.get("tranche3"), dict) else {}
                if tranche3 and as_epoch(tranche3.get("entry_epoch")) is not None:
                    add(normalize_event_row(symbol, "T3", tranche3, {**event, "status": "closed"}))
            elif event_type == "tranche2_exit":
                add(normalize_event_row(symbol, "T2", event, {"status": "closed"}))
            elif event_type == "tranche3_exit":
                add(normalize_event_row(symbol, "T3", event, {"status": "closed"}))
        out[symbol] = sorted(rows, key=row_sort_key)
    return out


def compare_rows(expected: list[dict[str, Any]], installed: list[dict[str, Any]], price_tolerance: float, pnl_tolerance: float) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if len(expected) != len(installed):
        issues.append({"issue": "row_count_mismatch", "expected": len(expected), "installed": len(installed)})
    limit = min(len(expected), len(installed))
    for idx in range(limit):
        left = expected[idx]
        right = installed[idx]
        for key in ("tranche", "side", "signal_epoch", "entry_epoch", "exit_epoch", "status"):
            if left.get(key) != right.get(key):
                issues.append({"issue": "field_mismatch", "row": idx, "field": key, "expected": left.get(key), "installed": right.get(key)})
        for key in ("entry_fill_price", "exit_fill_price"):
            lv = left.get(key)
            rv = right.get(key)
            if lv is None and rv is None:
                continue
            if lv is None or rv is None or abs(float(lv) - float(rv)) > price_tolerance:
                issues.append({"issue": "price_mismatch", "row": idx, "field": key, "expected": lv, "installed": rv})
        lv = left.get("net_rupees")
        rv = right.get("net_rupees")
        if lv is None and rv is None:
            continue
        if lv is None or rv is None or abs(float(lv) - float(rv)) > pnl_tolerance:
            issues.append({"issue": "pnl_mismatch", "row": idx, "field": "net_rupees", "expected": lv, "installed": rv})
    return issues


def aggregate(rows_by_symbol: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for rows in rows_by_symbol.values():
        for row in rows:
            tranche = str(row.get("tranche") or "")
            item = out.setdefault(tranche, {"rows": 0, "closed": 0, "wins": 0, "net_rupees": 0.0})
            item["rows"] += 1
            if row.get("status") == "closed":
                item["closed"] += 1
                net = as_float(row.get("net_rupees")) or 0.0
                item["net_rupees"] += net
                if net > 0:
                    item["wins"] += 1
    for item in out.values():
        item["success_rate_pct"] = round(100.0 * item["wins"] / item["closed"], 4) if item["closed"] else None
        item["net_rupees"] = round(item["net_rupees"], 2)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--candidate-root", action="append", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--quarantined-symbols", default="")
    parser.add_argument("--price-tolerance", type=float, default=0.01)
    parser.add_argument("--pnl-tolerance", type=float, default=1.0)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    requested = {item.strip().upper() for item in args.symbols.split(",") if item.strip()} or None
    quarantined = {item.strip().upper() for item in args.quarantined_symbols.split(",") if item.strip()}
    roots = candidate_dirs(args.candidate_root)
    artifacts = load_candidate_artifacts(roots, requested)
    comparable_symbols = sorted(set(artifacts) - quarantined)
    expected = expected_tranche_rows({symbol: artifacts[symbol] for symbol in comparable_symbols})
    installed = installed_tranche_rows(Path(args.state_dir), set(comparable_symbols))

    symbol_results: list[dict[str, Any]] = []
    failing: list[str] = []
    for symbol in comparable_symbols:
        issues = compare_rows(
            expected.get(symbol, []),
            installed.get(symbol, []),
            price_tolerance=float(args.price_tolerance),
            pnl_tolerance=float(args.pnl_tolerance),
        )
        if issues:
            failing.append(symbol)
        symbol_results.append(
            {
                "symbol": symbol,
                "ok": not issues,
                "expected_rows": len(expected.get(symbol, [])),
                "installed_rows": len(installed.get(symbol, [])),
                "issues": issues[:20],
                "issue_count": len(issues),
            }
        )

    report = {
        "schema": "obvfutport_v2.selected_candidate_install_audit.v1",
        "ok": not failing and bool(comparable_symbols),
        "state_dir": str(Path(args.state_dir)),
        "candidate_roots": [str(path) for path in roots],
        "requested_symbols": sorted(requested) if requested else None,
        "quarantined_symbols": sorted(quarantined),
        "comparable_symbol_count": len(comparable_symbols),
        "failing_symbol_count": len(failing),
        "failing_symbols": failing[:100],
        "expected_aggregate": aggregate(expected),
        "installed_aggregate": aggregate(installed),
        "symbol_results": symbol_results,
        "updated_epoch": time.time(),
    }
    if args.output:
        atomic_write_json(Path(args.output), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
