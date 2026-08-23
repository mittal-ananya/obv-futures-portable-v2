#!/usr/bin/env python3
"""Merge per-symbol OBVFUTPORT-v2 checkpoint replays into one installable state."""

from __future__ import annotations

import argparse
import json
import shutil
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def trade_dates(start_date: str, end_date: str) -> list[str]:
    current = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    out: list[str] = []
    while current <= final:
        if current.weekday() < 5:
            out.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return out


def copytree_replace(source: Path, target: Path) -> int:
    if not source.exists():
        return 0
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target)
    return 1


def copy_file_replace(source: Path, target: Path) -> int:
    if not source.exists():
        return 0
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return 1


def safe_symbol(symbol: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(symbol))


def resolve_instrument_dir(run_dir: Path, symbol: str) -> Path | None:
    instruments_dir = run_dir / "instruments"
    candidates = [instruments_dir / symbol, instruments_dir / safe_symbol(symbol)]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    dirs = [path for path in instruments_dir.glob("*") if path.is_dir()]
    if len(dirs) == 1:
        return dirs[0]
    return None


def expected_target_keys(entries: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for entry in entries:
        cash = entry.get("cash_key")
        fut = entry.get("fut_key")
        if cash:
            keys.add(str(cash))
        if fut:
            fut_key = str(fut)
            keys.add(fut_key)
            if "26AUGFUT" in fut_key:
                keys.add(fut_key.replace("26AUGFUT", "26SEPFUT"))
    return keys


def event_sort_key(line: str) -> tuple[Any, ...]:
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return (9999999999, "", "", line)
    epoch = (
        obj.get("event_epoch")
        or obj.get("execution_epoch")
        or obj.get("entry_epoch")
        or obj.get("exit_epoch")
        or obj.get("created_epoch")
        or obj.get("timestamp_epoch")
        or 0
    )
    return (
        epoch,
        obj.get("symbol") or "",
        obj.get("event_type") or obj.get("type") or "",
        obj.get("signal_id") or obj.get("position_id") or obj.get("id") or "",
        line,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2")
    parser.add_argument("--filtered-root", required=True)
    parser.add_argument("--output-state", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--prod-state", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
    parser.add_argument("--require-symbol-count", type=int, default=212)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    filtered_root = Path(args.filtered_root)
    if not filtered_root.is_absolute():
        filtered_root = root / filtered_root
    output_state = Path(args.output_state)
    if not output_state.is_absolute():
        output_state = root / output_state
    prod_state = Path(args.prod_state)

    setup = read_json(filtered_root / "shard_setup.json", {})
    entries: list[dict[str, Any]] = []
    for shard in setup.get("shards") or []:
        universe = read_json(Path(shard.get("universe") or ""), {})
        entries.extend(list(universe.get("entries") or []))
    expected_symbols = sorted({str(entry.get("symbol")) for entry in entries if entry.get("symbol")})

    summary_path = filtered_root / "reports" / "symbol_checkpoint_replay_summary.json"
    summary = read_json(summary_path, {})
    rows = list(summary.get("rows") or [])
    ok_rows = [row for row in rows if row.get("status") == "ok"]
    row_symbols = sorted({str(row.get("symbol")) for row in ok_rows if row.get("symbol")})
    missing_symbols = sorted(set(expected_symbols) - set(row_symbols))
    extra_symbols = sorted(set(row_symbols) - set(expected_symbols))

    preflight_ok = (
        bool(expected_symbols)
        and len(expected_symbols) >= int(args.require_symbol_count)
        and int(summary.get("complete") or 0) >= int(args.require_symbol_count)
        and int(summary.get("failed") or 0) == 0
        and int(summary.get("pending") or 0) == 0
        and not missing_symbols
    )
    if not preflight_ok:
        report = {
            "schema": "obvfutport_v2.symbol_checkpoint_reseed_merge.v1",
            "ok": False,
            "reason": "checkpoint_summary_not_complete",
            "filtered_root": str(filtered_root),
            "summary_path": str(summary_path),
            "expected_symbol_count": len(expected_symbols),
            "complete": summary.get("complete"),
            "failed": summary.get("failed"),
            "pending": summary.get("pending"),
            "missing_symbols": missing_symbols[:50],
            "extra_symbols": extra_symbols[:50],
            "updated_epoch": time.time(),
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2

    if output_state.exists():
        if not args.force:
            raise SystemExit(f"output state exists; pass --force to replace: {output_state}")
        shutil.rmtree(output_state)
    output_state.mkdir(parents=True, exist_ok=True)

    copied_symbols = 0
    event_lines_by_date: dict[str, list[str]] = {trade_date: [] for trade_date in trade_dates(args.start_date, args.end_date)}
    date_rollups: dict[str, dict[str, Any]] = {
        trade_date: {"trade_date": trade_date, "rows_seen": 0, "quotes_used": 0, "trade_state_events": 0, "duration_seconds": 0.0}
        for trade_date in trade_dates(args.start_date, args.end_date)
    }
    symbol_reports: list[dict[str, Any]] = []

    for row in ok_rows:
        symbol = str(row["symbol"])
        run_dir = Path(str(row["run_dir"]))
        report = read_json(run_dir / "archive_replay_report.json", {})
        if not (bool(report.get("ok")) and not bool(report.get("partial"))):
            raise SystemExit(f"unexpected bad symbol report after preflight: {symbol} {run_dir}")
        source_instrument = resolve_instrument_dir(run_dir, symbol)
        if source_instrument is None:
            raise SystemExit(f"missing instrument state for {symbol}: {run_dir / 'instruments'}")
        copytree_replace(source_instrument, output_state / "instruments" / source_instrument.name)
        copied_symbols += 1

        for events_file in sorted((run_dir / "decision_events").glob("decision_events_*.jsonl")):
            trade_date = events_file.stem.replace("decision_events_", "")
            if trade_date not in event_lines_by_date:
                continue
            event_lines_by_date[trade_date].extend(
                line for line in events_file.read_text(encoding="utf-8").splitlines() if line.strip()
            )

        for date_report in report.get("date_reports") or []:
            trade_date = str(date_report.get("trade_date") or "")
            if trade_date not in date_rollups:
                continue
            rollup = date_rollups[trade_date]
            rollup["rows_seen"] += int(date_report.get("rows_seen") or 0)
            rollup["quotes_used"] += int(date_report.get("quotes_used") or 0)
            rollup["trade_state_events"] += int(date_report.get("trade_state_events") or 0)
            rollup["duration_seconds"] = round(float(rollup["duration_seconds"]) + float(date_report.get("duration_seconds") or 0.0), 4)
        symbol_reports.append({"symbol": symbol, "run_dir": str(run_dir), "events": row.get("events"), "dates": row.get("dates")})

    for trade_date, lines in event_lines_by_date.items():
        if not lines:
            continue
        out = output_state / "decision_events" / f"decision_events_{trade_date}.jsonl"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("\n".join(sorted(lines, key=event_sort_key)) + "\n", encoding="utf-8")

    # Bootstrap compact state is data-derived, not strategy-override-derived, so reuse
    # the existing validated 212-symbol bootstrap tree for the same end date.
    copied_bootstrap = copytree_replace(prod_state / "bootstrap_state", output_state / "bootstrap_state")
    copied_bootstrap_status = copy_file_replace(prod_state / "bootstrap_status.json", output_state / "bootstrap_status.json")
    copied_status = copy_file_replace(prod_state / "status.json", output_state / "status.json")

    archive_report = {
        "schema": "obvfutport_v2.symbol_checkpoint_merged_archive_replay.v1",
        "ok": True,
        "partial": False,
        "strategy_id": "OBVFUTPORT_V2_PASSIVE",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "symbols": copied_symbols,
        "target_keys": len(expected_target_keys(entries)),
        "targets_missing": 0,
        "source": "merged_symbol_checkpoint_replay",
        "filtered_root": str(filtered_root),
        "date_reports": [date_rollups[trade_date] for trade_date in sorted(date_rollups)],
        "symbol_reports": symbol_reports,
        "updated_at_ist": datetime.now().astimezone().isoformat(),
    }
    atomic_write_json(output_state / "archive_replay_report.json", archive_report)

    merge_report = {
        "schema": "obvfutport_v2.symbol_checkpoint_reseed_merge.v1",
        "ok": True,
        "filtered_root": str(filtered_root),
        "output_state": str(output_state),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "expected_symbol_count": len(expected_symbols),
        "copied_symbols": copied_symbols,
        "decision_event_files": sum(1 for lines in event_lines_by_date.values() if lines),
        "decision_events_total": sum(len(lines) for lines in event_lines_by_date.values()),
        "bootstrap_state_reused_from_prod": bool(copied_bootstrap),
        "bootstrap_status_copied": bool(copied_bootstrap_status),
        "status_copied": bool(copied_status),
        "target_keys": len(expected_target_keys(entries)),
        "updated_epoch": time.time(),
    }
    atomic_write_json(output_state / "reports" / "symbol_checkpoint_reseed_merge_report.json", merge_report)
    print(json.dumps(merge_report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
