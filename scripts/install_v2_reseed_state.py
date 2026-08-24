#!/usr/bin/env python3
"""Install an isolated OBVFUTPORT-v2 replay/reseed state into production state.

The source state must already be produced by the normal v2 replay engine. This
helper backs up the current v2 production state and installs only v2-owned
runtime products. It deliberately does not touch OBVFUTPORT-v1, Compass v1, or
the dedicated Nifty package.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
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


def run_selected_candidate_gate(
    *,
    source_state: Path,
    candidate_roots: list[str],
    quarantined_symbols: str,
    symbols: str,
    output: Path,
) -> dict[str, Any]:
    script = Path(__file__).resolve().parent / "audit_v2_selected_candidate_install.py"
    cmd = [
        sys.executable,
        str(script),
        "--state-dir",
        str(source_state),
        "--quarantined-symbols",
        quarantined_symbols,
        "--output",
        str(output),
    ]
    if symbols:
        cmd.extend(["--symbols", symbols])
    for root in candidate_roots:
        cmd.extend(["--candidate-root", root])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    payload = read_json(output, {})
    if not isinstance(payload, dict) or not payload:
        payload = {
            "schema": "obvfutport_v2.selected_candidate_install_audit.subprocess.v1",
            "ok": False,
            "reason": "selected_candidate_gate_report_missing",
        }
    payload["returncode"] = proc.returncode
    payload["stdout_tail"] = proc.stdout[-4000:]
    payload["stderr_tail"] = proc.stderr[-4000:]
    return payload


def backup_current(prod_state: Path, backup_dir: Path, dates: list[str]) -> dict[str, Any]:
    copied: dict[str, int] = {}
    copied["instruments"] = copytree_replace(prod_state / "instruments", backup_dir / "instruments")
    copied["decision_events"] = copytree_replace(prod_state / "decision_events", backup_dir / "decision_events")
    copied["bootstrap_state"] = copytree_replace(prod_state / "bootstrap_state", backup_dir / "bootstrap_state")
    for name in ("bootstrap_status.json", "status.json", "target_stream_consumer_pointer.json"):
        copied[name] = copy_file_replace(prod_state / name, backup_dir / name)
    reports_dir = backup_dir / "reports"
    for report_name in ("v2_tranche_performance_20260810_20260821.json",):
        copied[f"reports/{report_name}"] = copy_file_replace(
            prod_state / "reports" / report_name,
            reports_dir / report_name,
        )
    copied["date_count"] = len(dates)
    return copied


def install(source_state: Path, prod_state: Path, dates: list[str], *, dry_run: bool) -> dict[str, Any]:
    counts: dict[str, int] = {}
    source_instruments = source_state / "instruments"
    source_events = source_state / "decision_events"
    source_bootstrap = source_state / "bootstrap_state"

    symbols = sorted(path.name for path in source_instruments.iterdir() if path.is_dir()) if source_instruments.exists() else []
    if not dry_run:
        counts["instruments_tree"] = copytree_replace(source_instruments, prod_state / "instruments")
        counts["bootstrap_state_tree"] = copytree_replace(source_bootstrap, prod_state / "bootstrap_state")
        counts["bootstrap_status"] = copy_file_replace(source_state / "bootstrap_status.json", prod_state / "bootstrap_status.json")
        counts["status"] = copy_file_replace(source_state / "status.json", prod_state / "status.json")
    else:
        counts["instruments_tree"] = int(source_instruments.exists())
        counts["bootstrap_state_tree"] = int(source_bootstrap.exists())

    event_counts: dict[str, int] = {}
    for trade_date in dates:
        source = source_events / f"decision_events_{trade_date}.jsonl"
        target = prod_state / "decision_events" / source.name
        if source.exists():
            event_counts[trade_date] = len([line for line in source.read_text(encoding="utf-8").splitlines() if line.strip()])
            if not dry_run:
                copy_file_replace(source, target)
        else:
            event_counts[trade_date] = 0
            if not dry_run and target.exists():
                target.unlink()
    counts["decision_event_files"] = sum(1 for count in event_counts.values() if count)
    counts["decision_events_total"] = sum(event_counts.values())
    counts["symbols"] = len(symbols)
    return {"counts": counts, "symbols": symbols, "decision_event_counts": event_counts}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-state", required=True)
    parser.add_argument("--prod-state", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--report-name", default="")
    parser.add_argument("--require-symbol-count", type=int, default=212)
    parser.add_argument("--selected-candidate-root", action="append", default=[])
    parser.add_argument("--selected-candidate-symbols", default="")
    parser.add_argument("--quarantined-symbols", default="IOC,MAXHEALTH,WAAREEENER")
    parser.add_argument("--require-selected-candidate-match", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    source_state = Path(args.source_state)
    prod_state = Path(args.prod_state)
    dates = trade_dates(args.start_date, args.end_date)
    archive_report = read_json(source_state / "archive_replay_report.json", {})
    source_ok = bool(archive_report.get("ok")) and not bool(archive_report.get("partial"))
    symbols = sorted((source_state / "instruments").glob("*")) if (source_state / "instruments").exists() else []
    symbol_count = sum(1 for path in symbols if path.is_dir())
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    selected_gate: dict[str, Any] = {}
    selected_gate_ok = True
    if args.require_selected_candidate_match or args.selected_candidate_root:
        selected_gate_path = prod_state / "reports" / f"selected_candidate_install_gate_{stamp}.json"
        selected_gate = run_selected_candidate_gate(
            source_state=source_state,
            candidate_roots=list(args.selected_candidate_root),
            quarantined_symbols=str(args.quarantined_symbols),
            symbols=str(args.selected_candidate_symbols),
            output=selected_gate_path,
        )
        selected_gate_ok = bool(selected_gate.get("ok"))
    ok = source_ok and symbol_count >= int(args.require_symbol_count) and selected_gate_ok

    backup_dir = prod_state / "backups" / f"pre_v2_reseed_install_{stamp}"
    backup = {}
    install_report: dict[str, Any] = {}
    if ok:
        backup = {} if args.dry_run else backup_current(prod_state, backup_dir, dates)
        install_report = install(source_state, prod_state, dates, dry_run=bool(args.dry_run))

    report = {
        "schema": "obvfutport_v2.reseed_install.v1",
        "ok": bool(ok),
        "dry_run": bool(args.dry_run),
        "source_state": str(source_state),
        "prod_state": str(prod_state),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "source_replay_ok": source_ok,
        "source_replay_partial": bool(archive_report.get("partial")),
        "source_symbol_count": symbol_count,
        "required_symbol_count": int(args.require_symbol_count),
        "selected_candidate_gate_required": bool(args.require_selected_candidate_match or args.selected_candidate_root),
        "selected_candidate_gate_ok": selected_gate_ok,
        "selected_candidate_gate": selected_gate,
        "backup_dir": str(backup_dir) if not args.dry_run else None,
        "backup": backup,
        "install": install_report,
        "updated_epoch": time.time(),
    }
    report_name = args.report_name or f"v2_reseed_install_{args.end_date.replace('-', '')}.json"
    atomic_write_json(prod_state / "reports" / report_name, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
