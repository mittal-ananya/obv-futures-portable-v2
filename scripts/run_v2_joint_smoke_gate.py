#!/usr/bin/env python3
"""Run a deterministic OBVFUTPORT-v2 5-symbol recalibration/replay smoke gate.

The gate is intentionally isolated: it writes only to the requested output
directory, filters the large target stream down to the selected symbol keys
once, then runs installed replay and scorer against the same filtered input.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PACKAGE_ROOT / "src"
SCRIPT_ROOT = PACKAGE_ROOT / "scripts"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from obvfut_portable_v2.passive_runner import (  # noqa: E402
    PassiveV2Runner,
    as_float,
    atomic_write_json,
    epoch_ist_iso,
    iter_target_stream_normalized_rows,
    json_clean,
    read_json,
)

import score_t1_t2_exit_candidates_risk_first as scorer  # noqa: E402


DEFAULT_SYMBOLS = ["ABCAPITAL", "OFSS", "RELIANCE", "SOLARINDS", "HDFCAMC"]


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip().upper() for part in str(raw).split(",") if part.strip()]


def trade_dates(start: str, end: str) -> list[str]:
    current = date.fromisoformat(start)
    final = date.fromisoformat(end)
    out: list[str] = []
    while current <= final:
        if current.weekday() < 5:
            out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def resolve_path(root: Path, value: Any) -> Path | None:
    if value in (None, ""):
        return None
    path = Path(str(value))
    return path if path.is_absolute() else root / path


def write_json(path: Path, payload: Any) -> None:
    atomic_write_json(path, json_clean(payload))


def choose_stream_source(root: Path, config: dict[str, Any], trade_date: str) -> tuple[Path | None, list[dict[str, Any]]]:
    filename = f"target_quotes_{trade_date}.jsonl"
    candidates: list[Path] = []
    state_dir = resolve_path(root, config.get("state_dir"))
    if state_dir is not None:
        candidates.append(state_dir / "target_stream" / trade_date / filename)
    for key in ("target_stream_root", "target_stream_root_local"):
        path = resolve_path(root, config.get(key))
        if path is not None:
            candidates.append(path / trade_date / filename)
    seen: set[str] = set()
    unique: list[Path] = []
    for path in candidates:
        marker = str(path)
        if marker not in seen:
            seen.add(marker)
            unique.append(path)
    rows = [
        {
            "path": str(path),
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else 0,
        }
        for path in unique
    ]
    existing = [path for path in unique if path.exists() and path.stat().st_size > 0]
    return (max(existing, key=lambda path: path.stat().st_size) if existing else None, rows)


def prepare_mini_config(root: Path, config_path: Path, output_dir: Path, symbols: list[str]) -> Path:
    runtime = read_json(config_path, {})
    universe_path = resolve_path(root, runtime.get("hurst_universe_manifest_path")) or resolve_path(
        root, runtime.get("hurst_universe_manifest_path_local")
    )
    if universe_path is None or not universe_path.exists():
        raise SystemExit("Unable to resolve v2 universe manifest")
    universe = read_json(universe_path, {})
    selected = {symbol.upper() for symbol in symbols}
    entries = [entry for entry in universe.get("entries", []) if str(entry.get("symbol") or "").upper() in selected]
    found = {str(entry.get("symbol") or "").upper() for entry in entries}
    missing = sorted(selected - found)
    if missing:
        raise SystemExit(f"Missing symbols in universe: {missing}")
    entries.sort(key=lambda item: symbols.index(str(item.get("symbol") or "").upper()))

    config_dir = output_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    mini_universe = {key: value for key, value in universe.items() if key != "entries"}
    mini_universe["entries"] = entries
    mini_universe["symbol_count"] = len(entries)
    mini_universe["created_for"] = "v2_joint_smoke_gate"
    universe_out = config_dir / "universe.json"
    write_json(universe_out, mini_universe)

    mini_runtime = dict(runtime)
    work_state = output_dir / "work_state"
    filtered_stream = output_dir / "filtered_target_stream"
    mini_runtime["state_dir"] = str(work_state)
    mini_runtime["state_dir_local"] = str(work_state)
    mini_runtime["hurst_universe_manifest_path"] = str(universe_out)
    mini_runtime["hurst_universe_manifest_path_local"] = str(universe_out)
    mini_runtime["target_stream_root"] = str(filtered_stream)
    mini_runtime["target_stream_root_local"] = str(filtered_stream)
    mini_runtime["bootstrap_load_enabled"] = False
    mini_runtime["skip_past_due_clocks_on_start"] = False
    mini_runtime["archive_replay_event_time_checkpoints_enabled"] = True
    mini_runtime["archive_replay_disable_live_stale_entry_marking"] = True
    mini_runtime["archive_replay_second_row_retention_seconds"] = int(
        mini_runtime.get("archive_replay_second_row_retention_seconds") or 30000
    )
    config_out = config_dir / "runtime.json"
    write_json(config_out, mini_runtime)
    return config_out


def build_filtered_stream(
    *,
    root: Path,
    production_config: dict[str, Any],
    mini_config: Path,
    output_dir: Path,
    start_date: str,
    end_date: str,
    reuse_existing: bool = False,
) -> dict[str, Any]:
    runner = PassiveV2Runner(mini_config)
    target_keys = set(runner.targets)
    filtered_root = output_dir / "filtered_target_stream"
    reports: list[dict[str, Any]] = []
    started_all = time.perf_counter()
    for trade_date in trade_dates(start_date, end_date):
        source, candidates = choose_stream_source(root, production_config, trade_date)
        target_dir = filtered_root / trade_date
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"target_quotes_{trade_date}.jsonl"
        started = time.perf_counter()
        rows_written = 0
        if reuse_existing and target_path.exists() and target_path.stat().st_size > 1024:
            reports.append(
                {
                    "trade_date": trade_date,
                    "source": str(source) if source else None,
                    "source_found": source is not None,
                    "source_candidates": candidates,
                    "target": str(target_path),
                    "rows_written": None,
                    "size_bytes": target_path.stat().st_size,
                    "duration_seconds": round(time.perf_counter() - started, 4),
                    "reused_existing": True,
                }
            )
            continue
        if source is not None:
            tmp = target_path.with_suffix(target_path.suffix + ".tmp")
            if tmp.exists():
                tmp.unlink()
            with tmp.open("w", encoding="utf-8") as handle:
                for row in iter_target_stream_normalized_rows(source, trade_date, target_keys):
                    handle.write(json.dumps(json_clean(row), sort_keys=True) + "\n")
                    rows_written += 1
            tmp.replace(target_path)
        reports.append(
            {
                "trade_date": trade_date,
                "source": str(source) if source else None,
                "source_found": source is not None,
                "source_candidates": candidates,
                "target": str(target_path),
                "rows_written": rows_written,
                "size_bytes": target_path.stat().st_size if target_path.exists() else 0,
                "duration_seconds": round(time.perf_counter() - started, 4),
            }
        )
    return {
        "target_key_count": len(target_keys),
        "target_keys": sorted(target_keys),
        "reports": reports,
        "duration_seconds": round(time.perf_counter() - started_all, 4),
        "ok": all(item.get("source_found") for item in reports),
    }


def run_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    report: dict[str, Any],
    key: str,
    output_dir: Path | None = None,
) -> None:
    started = time.perf_counter()
    result = subprocess.run(command, cwd=str(cwd), env=env, text=True, capture_output=True)
    report[key] = {
        "command": command,
        "returncode": result.returncode,
        "duration_seconds": round(time.perf_counter() - started, 4),
        "stdout_tail": result.stdout[-8000:],
        "stderr_tail": result.stderr[-8000:],
    }
    if result.returncode != 0:
        if output_dir is not None:
            report["failed_stage"] = key
            report["completed_at_ist"] = epoch_ist_iso(time.time())
            write_json(output_dir / "smoke_gate_report.json", report)
        raise SystemExit(f"{key} failed with return code {result.returncode}")


def nested_or_event(event: dict[str, Any]) -> dict[str, Any]:
    position = event.get("position") if isinstance(event.get("position"), dict) else {}
    return {**position, **{k: v for k, v in event.items() if v is not None}}


def event_signal_id(event: dict[str, Any]) -> str:
    merged = nested_or_event(event)
    return str(merged.get("signal_id") or merged.get("base_signal_id") or "")


def load_installed_rows(state_dir: Path, symbols: list[str]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for symbol in symbols:
        ledger = state_dir / "instruments" / symbol / "ledger.jsonl"
        model_state_path = state_dir / "instruments" / symbol / "model_state.json"
        groups: dict[str, dict[str, Any]] = {}
        if ledger.exists():
            with ledger.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    signal_id = event_signal_id(event)
                    if not signal_id:
                        continue
                    group = groups.setdefault(signal_id, {"events": []})
                    group["events"].append(event)
                    if event.get("event") == "paper_entry":
                        group["entry"] = nested_or_event(event)
                    elif event.get("event") == "paper_exit":
                        group["paper_exit"] = nested_or_event(event)
                    elif event.get("event") == "tranche2_exit":
                        group["tranche2_exit"] = nested_or_event(event)
                    elif event.get("event") == "tranche3_entry":
                        group["tranche3_entry"] = nested_or_event(event)
                    elif event.get("event") == "tranche3_exit":
                        group["tranche3_exit"] = nested_or_event(event)
        model_state = read_json(model_state_path, {}) if model_state_path.exists() else {}
        open_position = model_state.get("position") if isinstance(model_state.get("position"), dict) else None
        if open_position and open_position.get("signal_id"):
            group = groups.setdefault(str(open_position.get("signal_id")), {"events": []})
            group.setdefault("entry", open_position)
            group["open_position"] = open_position
        rows: list[dict[str, Any]] = []
        for signal_id, group in groups.items():
            entry = group.get("entry")
            if not isinstance(entry, dict):
                continue
            paper_exit = group.get("paper_exit") if isinstance(group.get("paper_exit"), dict) else None
            open_pos = group.get("open_position") if isinstance(group.get("open_position"), dict) else None
            position_for_state = open_pos or paper_exit or entry
            t2 = {}
            if isinstance(position_for_state.get("two_lot_ttsl"), dict):
                t2 = position_for_state.get("two_lot_ttsl", {}).get("tranche2") or {}
            if group.get("tranche2_exit"):
                t2 = {**t2, **group["tranche2_exit"]}
            t3 = position_for_state.get("tranche3") if isinstance(position_for_state.get("tranche3"), dict) else {}
            if (
                isinstance(t3, dict)
                and t3.get("status") not in {"open", "closed"}
                and not t3.get("entry_epoch")
            ):
                t3 = {}
            if group.get("tranche3_entry"):
                t3 = {**t3, **group["tranche3_entry"]}
            if group.get("tranche3_exit"):
                t3 = {**t3, **group["tranche3_exit"]}
            status = "closed" if paper_exit else "open_mark"
            rows.append(
                {
                    "symbol": symbol,
                    "signal_id": signal_id,
                    "signal_epoch": entry.get("signal_epoch"),
                    "source": entry.get("source"),
                    "side": entry.get("side"),
                    "status": status,
                    "entry_epoch": entry.get("entry_epoch"),
                    "entry_ltp_price": entry.get("entry_ltp_price") or entry.get("entry_price"),
                    "entry_fill_price": entry.get("entry_fill_price"),
                    "exit_epoch": paper_exit.get("exit_epoch") if paper_exit else (open_pos or {}).get("latest_epoch"),
                    "exit_reason": paper_exit.get("exit_reason") if paper_exit else "open_mark_if_closed",
                    "exit_ltp_price": paper_exit.get("exit_ltp_price") if paper_exit else (open_pos or {}).get("latest_price"),
                    "exit_fill_price": paper_exit.get("exit_fill_price") if paper_exit else (open_pos or {}).get("latest_fill_price_if_closed"),
                    "t1_net_rupees": paper_exit.get("net_rupees") if paper_exit else (open_pos or {}).get("net_rupees_if_closed"),
                    "t2_exit_epoch": t2.get("exit_epoch"),
                    "t2_exit_ltp_price": t2.get("exit_price") or t2.get("partial_exit_ltp"),
                    "t2_exit_fill_price": t2.get("exit_fill_price") or t2.get("partial_exit_fill_price"),
                    "t2_net_rupees": t2.get("net_rupees"),
                    "t3_entry_epoch": t3.get("entry_epoch"),
                    "t3_entry_fill_price": t3.get("entry_fill_price"),
                    "t3_exit_epoch": t3.get("exit_epoch"),
                    "t3_exit_ltp_price": t3.get("exit_price"),
                    "t3_exit_fill_price": t3.get("exit_fill_price"),
                    "t3_net_rupees": t3.get("net_rupees"),
                }
            )
        out[symbol] = sorted(rows, key=lambda row: int(row.get("signal_epoch") or row.get("entry_epoch") or 0))
    return out


def load_scorer_current_rows(score_dir: Path, symbols: list[str]) -> dict[str, list[dict[str, Any]]]:
    report = read_json(score_dir / "symbol_t1_risk_first_report.json", {})
    report_symbols = report.get("symbols") if isinstance(report.get("symbols"), dict) else {}
    if report_symbols:
        embedded: dict[str, list[dict[str, Any]]] = {}
        has_embedded = False
        for symbol in symbols:
            item = report_symbols.get(symbol) or {}
            current = item.get("current_deployed") if isinstance(item, dict) else None
            rows = current.get("rows") if isinstance(current, dict) else None
            if isinstance(rows, list):
                embedded[symbol] = [
                    row for row in rows if isinstance(row, dict) and row.get("status") in {"closed", "open_mark"}
                ]
                has_embedded = True
            else:
                embedded[symbol] = []
        if has_embedded:
            return embedded
    outcomes_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    outcome_path = score_dir / "candidate_outcomes.jsonl"
    with outcome_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            symbol = str(row.get("symbol") or "")
            if symbol in outcomes_by_symbol:
                outcomes_by_symbol[symbol].append(row)
    selected_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for symbol in symbols:
        item = (report.get("symbols") or {}).get(symbol) or {}
        current = item.get("current_deployed")
        if not isinstance(current, dict):
            selected_by_symbol[symbol] = []
            continue
        variants = scorer.combo_variants(current.get("combo") or {})
        exit_label = current.get("exit_combo_label")
        t3_label = current.get("tranche3_combo_label")
        rows = [
            row
            for row in outcomes_by_symbol.get(symbol, [])
            if row.get("status") in {"closed", "open_mark"}
            and str(row.get("variant") or "") in variants
            and row.get("exit_combo_label") == exit_label
            and row.get("tranche3_combo_label") == t3_label
        ]
        rows.sort(key=lambda row: int(row.get("signal_epoch") or 0))
        selected: list[dict[str, Any]] = []
        last_exit_epoch = 0
        for row in rows:
            entry_epoch = int(row.get("entry_epoch") or 0)
            if entry_epoch <= last_exit_epoch:
                continue
            selected.append(row)
            if row.get("status") == "open_mark":
                last_exit_epoch = 10**18
            else:
                last_exit_epoch = max(last_exit_epoch, int(row.get("exit_epoch") or entry_epoch))
        selected_by_symbol[symbol] = selected
    return selected_by_symbol


def close_enough(a: Any, b: Any, tolerance: float = 0.05) -> bool:
    fa = as_float(a)
    fb = as_float(b)
    if fa is None and fb is None:
        return True
    if fa is None or fb is None:
        return False
    return abs(float(fa) - float(fb)) <= tolerance


def comparable_exact_value(row: dict[str, Any], field: str) -> Any:
    if field == "exit_epoch" and row.get("status") == "open_mark":
        return None
    return row.get(field)


def compare_rows(installed: dict[str, list[dict[str, Any]]], scored: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    fields_exact = ["signal_epoch", "entry_epoch", "exit_epoch", "t2_exit_epoch", "t3_entry_epoch", "t3_exit_epoch"]
    fields_price = [
        "entry_ltp_price",
        "entry_fill_price",
        "exit_ltp_price",
        "exit_fill_price",
        "t2_exit_ltp_price",
        "t2_exit_fill_price",
        "t3_entry_fill_price",
        "t3_exit_ltp_price",
        "t3_exit_fill_price",
    ]
    mismatches: list[dict[str, Any]] = []
    summaries: dict[str, Any] = {}
    for symbol in sorted(set(installed) | set(scored)):
        left = installed.get(symbol, [])
        right = scored.get(symbol, [])
        summaries[symbol] = {"installed_rows": len(left), "scored_rows": len(right)}
        if len(left) != len(right):
            mismatches.append({"symbol": symbol, "field": "row_count", "installed": len(left), "scored": len(right)})
        for idx, (a, b) in enumerate(zip(left, right)):
            for field in fields_exact:
                av = comparable_exact_value(a, field)
                bv = comparable_exact_value(b, field)
                if (av or None) != (bv or None):
                    mismatches.append({"symbol": symbol, "row": idx, "field": field, "installed": av, "scored": bv})
            for field in fields_price:
                if not close_enough(a.get(field), b.get(field)):
                    mismatches.append({"symbol": symbol, "row": idx, "field": field, "installed": a.get(field), "scored": b.get(field)})
    return {"ok": not mismatches, "mismatch_count": len(mismatches), "mismatches": mismatches[:200], "symbol_summaries": summaries}


def impossible_t3_count(rows_by_symbol: dict[str, list[dict[str, Any]]]) -> int:
    count = 0
    for rows in rows_by_symbol.values():
        for row in rows:
            entry = as_float(row.get("t3_entry_epoch"))
            exit_epoch = as_float(row.get("t3_exit_epoch"))
            t2_exit = as_float(row.get("t2_exit_epoch"))
            if exit_epoch is not None and (entry is None or exit_epoch < entry):
                count += 1
            if entry is not None and t2_exit is not None and entry > t2_exit:
                count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(PACKAGE_ROOT))
    parser.add_argument("--config", default="config/runtime.json")
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-19")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--reuse-filtered-stream", action="store_true")
    parser.add_argument("--chronological-combo-simulation", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    config_path = resolve_path(root, args.config)
    if config_path is None or not config_path.exists():
        raise SystemExit(f"Config not found: {args.config}")
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = root / output_dir
    if output_dir.exists() and args.reuse_filtered_stream:
        for child in [
            "installed_replay",
            "precompute",
            "score_current_exit_t3",
            "work_state",
            "smoke_gate_report.json",
        ]:
            path = output_dir / child
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            elif path.exists() or path.is_symlink():
                path.unlink()
    elif output_dir.exists():
        if not args.force:
            raise SystemExit(f"Output dir exists; pass --force to replace: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    symbols = parse_csv(args.symbols) or DEFAULT_SYMBOLS
    report: dict[str, Any] = {
        "schema": "obvfutport_v2.joint_smoke_gate.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "root": str(root),
        "config": str(config_path),
        "output_dir": str(output_dir),
        "symbols": symbols,
        "start_date": args.start_date,
        "end_date": args.end_date,
    }

    production_config = read_json(config_path, {})
    mini_config = prepare_mini_config(root, config_path, output_dir, symbols)
    report["mini_config"] = str(mini_config)

    report["filtered_stream"] = build_filtered_stream(
        root=root,
        production_config=production_config,
        mini_config=mini_config,
        output_dir=output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        reuse_existing=bool(args.reuse_filtered_stream),
    )
    if not report["filtered_stream"]["ok"]:
        write_json(output_dir / "smoke_gate_report.json", report)
        raise SystemExit("Filtered stream build failed for one or more dates")

    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    installed_state = output_dir / "installed_replay" / "state"
    installed_state.mkdir(parents=True, exist_ok=True)
    link = installed_state / "target_stream"
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(output_dir / "filtered_target_stream", target_is_directory=True)

    run_command(
        [
            args.python,
            "-m",
            "obvfut_portable_v2.passive_runner",
            "archive-replay",
            "--config",
            str(mini_config),
            "--start-date",
            args.start_date,
            "--end-date",
            args.end_date,
            "--output-state-dir",
            str(installed_state),
            "--use-target-stream-history",
            "--max-trade-state-passes",
            "20",
        ],
        cwd=root,
        env=env,
        report=report,
        key="installed_replay",
        output_dir=output_dir,
    )

    precompute_dir = output_dir / "precompute"
    run_command(
        [
            args.python,
            str(root / "scripts" / "recalibrate_t1_entry_fast.py"),
            "--config",
            str(mini_config),
            "--start-date",
            args.start_date,
            "--end-date",
            args.end_date,
            "--symbols",
            ",".join(symbols),
            "--output-dir",
            str(precompute_dir),
            "--write-panels",
            "--max-load1",
            "8.0",
            "--load-guard-sleep-seconds",
            "0.1",
        ],
        cwd=root,
        env=env,
        report=report,
        key="precompute",
        output_dir=output_dir,
    )

    score_dir = output_dir / "score_current_exit_t3"
    run_command(
        [
            args.python,
            str(root / "scripts" / "score_t1_t2_exit_candidates_risk_first.py"),
            "--config",
            str(mini_config),
            "--candidate-file",
            str(precompute_dir / "entry_candidates.jsonl"),
            "--start-date",
            args.start_date,
            "--end-date",
            args.end_date,
            "--symbols",
            ",".join(symbols),
            "--output-dir",
            str(score_dir),
            "--current-runtime-combos-only",
            *(["--chronological-combo-simulation", "--include-score-rows"] if args.chronological_combo_simulation else []),
            "--min-trades",
            "1",
            "--min-closed-trades",
            "1",
            "--min-success-rate-pct",
            "0",
        ],
        cwd=root,
        env=env,
        report=report,
        key="scorer",
        output_dir=output_dir,
    )

    installed_rows = load_installed_rows(installed_state, symbols)
    scored_rows = load_scorer_current_rows(score_dir, symbols)
    report["installed_rows"] = installed_rows
    report["scored_current_rows"] = scored_rows
    report["comparison"] = compare_rows(installed_rows, scored_rows)
    report["impossible_t3_rows"] = {
        "installed": impossible_t3_count(installed_rows),
        "scored": impossible_t3_count(scored_rows),
    }
    report["ok"] = bool(report["comparison"]["ok"]) and report["impossible_t3_rows"] == {"installed": 0, "scored": 0}
    report["completed_at_ist"] = epoch_ist_iso(time.time())
    write_json(output_dir / "smoke_gate_report.json", report)
    print(json.dumps(json_clean({k: report[k] for k in ["ok", "comparison", "impossible_t3_rows", "output_dir"]}), indent=2, sort_keys=True))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
