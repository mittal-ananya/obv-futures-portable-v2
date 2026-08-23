#!/usr/bin/env python3
"""Install Aug-10-flat OBVFUTPORT-v2 overlap replay state.

The replay itself is produced in small shard/symbol directories. This helper
backs up the current v2 state, copies only completed model/ledger outputs for
the 50 overlap symbols, and merges decision events date-wise.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    tmp.replace(path)


def safe_symbol(symbol: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(symbol))


def event_epoch(event: dict[str, Any]) -> int:
    for key in ("exit_epoch", "entry_epoch", "signal_epoch", "evaluation_epoch"):
        value = event.get(key)
        try:
            if value is not None:
                return int(float(value))
        except Exception:
            pass
    return 0


def event_key(line: str) -> tuple[Any, ...]:
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return ("raw", hashlib.sha1(line.encode("utf-8", "ignore")).hexdigest())
    return (
        event.get("symbol"),
        event.get("event"),
        event.get("signal_id"),
        event.get("position_id"),
        event.get("signal_epoch"),
        event.get("entry_epoch"),
        event.get("exit_epoch"),
        event.get("entry_reason"),
        event.get("exit_reason"),
    )


def copy_file_if_exists(source: Path, target: Path) -> bool:
    if not source.exists():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return True


def discover_symbol_source(
    *,
    root: Path,
    full_root: Path,
    filtered_root: Path,
    shard_id: int,
    symbol: str,
) -> tuple[Path | None, str]:
    run_dir = root / filtered_root / "symbol_runs" / f"shard_{shard_id:02d}_{safe_symbol(symbol)}"
    model = run_dir / "instruments" / safe_symbol(symbol) / "model_state.json"
    report = read_json(run_dir / "archive_replay_report.json", {})
    if model.exists() and bool(report.get("ok")) and not bool(report.get("partial")):
        return run_dir, "single_symbol_run"
    full_dir = root / full_root / f"shard_{shard_id:02d}"
    model = full_dir / "instruments" / safe_symbol(symbol) / "model_state.json"
    report = read_json(full_dir / "archive_replay_report.json", {})
    if model.exists() and bool(report.get("ok")) and not bool(report.get("partial")):
        return full_dir, "multi_symbol_shard"
    return None, "missing_or_incomplete"


def load_shard_symbols(root: Path, filtered_root: Path) -> list[dict[str, Any]]:
    setup = read_json(root / filtered_root / "shard_setup.json", {})
    rows: list[dict[str, Any]] = []
    for shard in setup.get("shards") or []:
        shard_id = int(shard.get("shard"))
        for symbol in shard.get("symbols") or []:
            rows.append({"shard": shard_id, "symbol": str(symbol)})
    return rows


def backup_current_state(prod_state: Path, symbols: list[str], dates: list[str]) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = prod_state / "backups" / f"pre_aug10_flat_v2_population_{stamp}"
    for symbol in symbols:
        source = prod_state / "instruments" / safe_symbol(symbol)
        if source.exists():
            shutil.copytree(source, backup / "instruments" / safe_symbol(symbol), dirs_exist_ok=True)
    for trade_date in dates:
        source = prod_state / "decision_events" / f"decision_events_{trade_date}.jsonl"
        if source.exists():
            copy_file_if_exists(source, backup / "decision_events" / source.name)
    return backup


def merge_decision_events(
    sources: list[Path],
    prod_state: Path,
    dates: list[str],
    *,
    preserve_existing: bool = False,
) -> dict[str, Any]:
    date_rows: dict[str, list[tuple[int, str]]] = {trade_date: [] for trade_date in dates}
    seen: dict[str, set[tuple[Any, ...]]] = {trade_date: set() for trade_date in dates}
    if preserve_existing:
        events_dir = prod_state / "decision_events"
        for trade_date in dates:
            path = events_dir / f"decision_events_{trade_date}.jsonl"
            if not path.exists():
                continue
            for raw in path.read_text().splitlines():
                line = raw.strip()
                if not line:
                    continue
                key = event_key(line)
                if key in seen[trade_date]:
                    continue
                seen[trade_date].add(key)
                try:
                    epoch = event_epoch(json.loads(line))
                except json.JSONDecodeError:
                    epoch = 0
                date_rows[trade_date].append((epoch, line))
    for source in sources:
        events_dir = source / "decision_events"
        if not events_dir.exists():
            continue
        for trade_date in dates:
            path = events_dir / f"decision_events_{trade_date}.jsonl"
            if not path.exists():
                continue
            for raw in path.read_text().splitlines():
                line = raw.strip()
                if not line:
                    continue
                key = event_key(line)
                if key in seen[trade_date]:
                    continue
                seen[trade_date].add(key)
                try:
                    epoch = event_epoch(json.loads(line))
                except json.JSONDecodeError:
                    epoch = 0
                date_rows[trade_date].append((epoch, line))
    out: dict[str, Any] = {}
    target_dir = prod_state / "decision_events"
    target_dir.mkdir(parents=True, exist_ok=True)
    for trade_date, rows in date_rows.items():
        rows.sort(key=lambda item: (item[0], item[1]))
        target = target_dir / f"decision_events_{trade_date}.jsonl"
        target.write_text("".join(line + "\n" for _, line in rows))
        out[trade_date] = {"events": len(rows), "path": str(target)}
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2")
    parser.add_argument("--prod-state", default="state")
    parser.add_argument("--full-root", default="state_aug10_flat_50_checkpoint_full_20260819")
    parser.add_argument("--filtered-root", default="state_aug10_flat_50_checkpoint_filtered_20260819")
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-19")
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--preserve-existing-events", action="store_true")
    parser.add_argument("--report-name", default="v2_aug10_flat_population_20260819.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    prod_state = root / args.prod_state
    dates: list[str] = []
    current = datetime.fromisoformat(args.start_date).date()
    final = datetime.fromisoformat(args.end_date).date()
    while current <= final:
        if current.weekday() < 5:
            dates.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)

    expected = load_shard_symbols(root, Path(args.filtered_root))
    sources: list[Path] = []
    rows: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    for item in expected:
        symbol = item["symbol"]
        source, source_type = discover_symbol_source(
            root=root,
            full_root=Path(args.full_root),
            filtered_root=Path(args.filtered_root),
            shard_id=int(item["shard"]),
            symbol=symbol,
        )
        row = {**item, "source_type": source_type, "source_dir": str(source) if source else None}
        if source is None:
            missing.append(row)
        else:
            sources.append(source)
        rows.append(row)

    sources = sorted(set(sources), key=lambda item: str(item))
    ok = not missing
    if args.require_complete and missing:
        report = {
            "schema": "obvfutport_v2.aug10_flat_population.v1",
            "ok": False,
            "dry_run": True,
            "reason": "missing_or_incomplete_symbol_replays",
            "expected_symbols": len(expected),
            "missing": missing,
            "rows": rows,
            "updated_epoch": time.time(),
        }
        atomic_write_json(prod_state / "reports" / args.report_name, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2

    if args.dry_run:
        backup = None
    else:
        backup = backup_current_state(prod_state, [item["symbol"] for item in expected], dates)

    copied_models = 0
    copied_ledgers = 0
    open_positions = 0
    open_symbols: list[str] = []
    for item in rows:
        source_dir = Path(item["source_dir"]) if item.get("source_dir") else None
        if source_dir is None:
            continue
        symbol = str(item["symbol"])
        source_instrument = source_dir / "instruments" / safe_symbol(symbol)
        target_instrument = prod_state / "instruments" / safe_symbol(symbol)
        model = read_json(source_instrument / "model_state.json", {})
        if isinstance(model, dict) and isinstance(model.get("position"), dict):
            open_positions += 1
            open_symbols.append(symbol)
        if not args.dry_run:
            copied_models += int(copy_file_if_exists(source_instrument / "model_state.json", target_instrument / "model_state.json"))
            copied_ledgers += int(copy_file_if_exists(source_instrument / "ledger.jsonl", target_instrument / "ledger.jsonl"))

    merged_events = (
        {}
        if args.dry_run
        else merge_decision_events(
            sources,
            prod_state,
            dates,
            preserve_existing=bool(args.preserve_existing_events),
        )
    )
    report = {
        "schema": "obvfutport_v2.aug10_flat_population.v1",
        "ok": ok,
        "dry_run": bool(args.dry_run),
        "root": str(root),
        "prod_state": str(prod_state),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "expected_symbols": len(expected),
        "complete_symbols": len(expected) - len(missing),
        "missing_symbols": len(missing),
        "missing": missing,
        "source_dirs": len(sources),
        "copied_models": copied_models,
        "copied_ledgers": copied_ledgers,
        "merged_events": merged_events,
        "preserve_existing_events": bool(args.preserve_existing_events),
        "open_positions": open_positions,
        "open_symbols": sorted(open_symbols),
        "backup_dir": str(backup) if backup else None,
        "rows": rows,
        "updated_epoch": time.time(),
    }
    atomic_write_json(prod_state / "reports" / args.report_name, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
