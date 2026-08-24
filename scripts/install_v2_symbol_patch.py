#!/usr/bin/env python3
"""Install selected OBVFUTPORT-v2 symbol state into production.

This is a narrow installer for quarantine cleanup and other symbol-scoped
repairs. It copies only requested instrument folders from a source state,
merges only those symbols' decision events into the requested date files, and
optionally installs a full override JSON that was produced from the current
production override plus the symbol-scoped change.
"""

from __future__ import annotations

import argparse
import hashlib
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


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    tmp.replace(path)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def file_sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def trade_dates(start_date: str, end_date: str) -> list[str]:
    current = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    out: list[str] = []
    while current <= final:
        if current.weekday() < 5:
            out.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return out


def event_epoch(row: dict[str, Any]) -> float:
    for key in ("event_epoch", "entry_epoch", "exit_epoch", "signal_epoch", "created_epoch"):
        value = row.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    position = row.get("position")
    if isinstance(position, dict):
        for key in ("event_epoch", "entry_epoch", "exit_epoch", "signal_epoch"):
            value = position.get(key)
            if isinstance(value, (int, float)):
                return float(value)
    return 0.0


def collect_symbols(value: Any, out: set[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "symbol" and isinstance(item, str):
                out.add(item.upper())
            collect_symbols(item, out)
    elif isinstance(value, list):
        for item in value:
            collect_symbols(item, out)


def event_mentions_symbol(row: dict[str, Any], symbols: set[str]) -> bool:
    found: set[str] = set()
    collect_symbols(row, found)
    if found & symbols:
        return True
    for item in row.values():
        if not isinstance(item, str):
            continue
        upper = item.upper()
        for symbol in symbols:
            if upper == symbol or upper.startswith(f"NSE:{symbol}") or upper.startswith(f"NFO:{symbol}"):
                return True
    return False


def copytree_replace(source: Path, target: Path, dry_run: bool) -> int:
    if not source.exists():
        return 0
    if dry_run:
        return 1
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target)
    return 1


def copy_file_replace(source: Path, target: Path, dry_run: bool) -> int:
    if not source.exists():
        return 0
    if dry_run:
        return 1
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return 1


def backup_paths(prod_state: Path, backup_dir: Path, symbols: set[str], dates: list[str], dry_run: bool) -> dict[str, Any]:
    copied: dict[str, Any] = {"symbols": {}, "decision_events": {}}
    for symbol in sorted(symbols):
        source = prod_state / "instruments" / symbol
        target = backup_dir / "instruments" / symbol
        copied["symbols"][symbol] = copytree_replace(source, target, dry_run=dry_run)
    for trade_date in dates:
        source = prod_state / "decision_events" / f"decision_events_{trade_date}.jsonl"
        target = backup_dir / "decision_events" / source.name
        copied["decision_events"][trade_date] = copy_file_replace(source, target, dry_run=dry_run)
    override = prod_state / "adaptive_calibration" / "v2_symbol_overrides_latest.json"
    copied["adaptive_override"] = copy_file_replace(override, backup_dir / "adaptive_calibration" / override.name, dry_run=dry_run)
    return copied


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-state", required=True)
    parser.add_argument("--prod-state", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--override-source", default="")
    parser.add_argument("--report-name", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    source_state = Path(args.source_state)
    prod_state = Path(args.prod_state)
    symbols = {item.strip().upper() for item in args.symbols.split(",") if item.strip()}
    if not symbols:
        raise SystemExit("no symbols requested")
    dates = trade_dates(args.start_date, args.end_date)
    missing_sources = [symbol for symbol in sorted(symbols) if not (source_state / "instruments" / symbol).exists()]
    archive_report = read_json(source_state / "archive_replay_report.json", {})
    source_ok = bool(archive_report.get("ok")) and not bool(archive_report.get("partial"))
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = prod_state / "backups" / f"pre_symbol_patch_{'_'.join(sorted(symbols))}_{stamp}"

    ok = source_ok and not missing_sources
    installed: dict[str, Any] = {"symbols": {}, "decision_events": {}, "override": {}}
    backup: dict[str, Any] = {}
    if ok:
        backup = backup_paths(prod_state, backup_dir, symbols, dates, dry_run=bool(args.dry_run))
        for symbol in sorted(symbols):
            source = source_state / "instruments" / symbol
            target = prod_state / "instruments" / symbol
            before_sha = file_sha256(target / "ledger.jsonl")
            copied = copytree_replace(source, target, dry_run=bool(args.dry_run))
            installed["symbols"][symbol] = {
                "copied": copied,
                "source_ledger_rows": len(iter_jsonl(source / "ledger.jsonl")),
                "before_ledger_sha256": before_sha,
                "after_ledger_sha256": file_sha256(target / "ledger.jsonl") if not args.dry_run else None,
            }

        for trade_date in dates:
            prod_path = prod_state / "decision_events" / f"decision_events_{trade_date}.jsonl"
            source_path = source_state / "decision_events" / prod_path.name
            prod_rows = iter_jsonl(prod_path)
            source_rows = [row for row in iter_jsonl(source_path) if event_mentions_symbol(row, symbols)]
            kept_rows = [row for row in prod_rows if not event_mentions_symbol(row, symbols)]
            merged = sorted(kept_rows + source_rows, key=lambda row: (event_epoch(row), json.dumps(row, sort_keys=True)))
            if not args.dry_run:
                write_jsonl(prod_path, merged)
            installed["decision_events"][trade_date] = {
                "prod_before": len(prod_rows),
                "removed_symbol_rows": len(prod_rows) - len(kept_rows),
                "source_symbol_rows": len(source_rows),
                "prod_after": len(merged),
            }

        if args.override_source:
            override_source = Path(args.override_source)
            override_target = prod_state / "adaptive_calibration" / "v2_symbol_overrides_latest.json"
            installed["override"] = {
                "source": str(override_source),
                "target": str(override_target),
                "before_sha256": file_sha256(override_target),
                "copied": copy_file_replace(override_source, override_target, dry_run=bool(args.dry_run)),
                "after_sha256": file_sha256(override_target) if not args.dry_run else None,
            }

    report = {
        "schema": "obvfutport_v2.symbol_patch_install.v1",
        "ok": bool(ok),
        "dry_run": bool(args.dry_run),
        "source_state": str(source_state),
        "prod_state": str(prod_state),
        "symbols": sorted(symbols),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "dates": dates,
        "source_replay_ok": source_ok,
        "source_replay_partial": bool(archive_report.get("partial")),
        "missing_sources": missing_sources,
        "backup_dir": str(backup_dir) if ok and not args.dry_run else None,
        "backup": backup,
        "install": installed,
        "updated_epoch": time.time(),
    }
    report_name = args.report_name or f"v2_symbol_patch_install_{stamp}.json"
    atomic_write_json(prod_state / "reports" / report_name, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
