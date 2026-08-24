#!/usr/bin/env python3
"""Memory-safe per-symbol OBVFUTPORT-v2 EOD append.

This script is v2-only and isolated. It starts from an existing carry-state
tree, processes one trade date from the quote-valid target stream, and writes
per-symbol replay outputs that can be merged by the existing checkpoint merge
tool. It does not install production state.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any


IST_OFFSET = "+05:30"


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def safe_symbol(symbol: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(symbol))


def parse_hhmm_to_epoch(trade_date: str, hhmm: str) -> int:
    return int(datetime.fromisoformat(f"{trade_date}T{hhmm}:00{IST_OFFSET}").timestamp())


def mem_available_mb() -> int:
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            name, raw = line.split(":", 1)
            if name in {"MemAvailable", "SwapFree"}:
                values[name] = int(raw.strip().split()[0]) // 1024
    except Exception:
        return 999_999
    return int(values.get("MemAvailable", 0) + values.get("SwapFree", 0))


def report_ok(path: Path) -> bool:
    report = read_json(path / "archive_replay_report.json", {})
    return bool(report.get("ok")) and not bool(report.get("partial"))


def extract_key_from_line(line: bytes) -> str | None:
    marker = b'"key"'
    idx = line.find(marker)
    if idx < 0:
        return None
    cursor = idx + len(marker)
    end = len(line)
    while cursor < end and line[cursor] in b" \t\r\n":
        cursor += 1
    if cursor >= end or line[cursor : cursor + 1] != b":":
        return None
    cursor += 1
    while cursor < end and line[cursor] in b" \t\r\n":
        cursor += 1
    if cursor >= end or line[cursor : cursor + 1] != b'"':
        return None
    cursor += 1
    stop = line.find(b'"', cursor)
    if stop < 0:
        return None
    try:
        return line[cursor:stop].decode("utf-8")
    except UnicodeDecodeError:
        return None


def entry_target_keys(entry: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
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


def symbol_run_dir(root: Path, shard: int, symbol: str) -> Path:
    return root / "symbol_runs" / f"shard_{shard:02d}_{safe_symbol(symbol)}"


def symbol_stream_dir(root: Path, shard: int, symbol: str) -> Path:
    return root / "symbol_streams" / f"shard_{shard:02d}_{safe_symbol(symbol)}"


def source_instrument_dir(source_state: Path, symbol: str) -> Path:
    for candidate in (
        source_state / "instruments" / symbol,
        source_state / "instruments" / safe_symbol(symbol),
    ):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"source instrument missing for {symbol}: {source_state / 'instruments'}")


def load_tasks(filtered_root: Path) -> list[dict[str, Any]]:
    setup = read_json(filtered_root / "shard_setup.json", {})
    tasks: list[dict[str, Any]] = []
    for shard in setup.get("shards") or []:
        shard_id = int(shard["shard"])
        universe = read_json(Path(str(shard["universe"])), {})
        entries = {str(entry.get("symbol")): entry for entry in universe.get("entries") or []}
        for symbol in shard.get("symbols") or sorted(entries):
            entry = entries.get(str(symbol))
            if not entry:
                continue
            tasks.append(
                {
                    "shard": shard_id,
                    "symbol": str(symbol),
                    "entry": entry,
                    "runtime": str(shard["runtime"]),
                    "keys": sorted(entry_target_keys(entry)),
                }
            )
    return tasks


def materialize_symbol_streams(filtered_root: Path, tasks: list[dict[str, Any]], trade_date: str, force: bool) -> dict[str, Any]:
    key_to_tasks: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        for key in task["keys"]:
            key_to_tasks.setdefault(str(key), []).append(task)
    source = filtered_root / "curated_target_stream" / trade_date / f"target_quotes_{trade_date}.jsonl"
    if not source.exists():
        raise FileNotFoundError(f"missing curated target stream: {source}")
    expected = [
        symbol_stream_dir(filtered_root, int(task["shard"]), str(task["symbol"]))
        / trade_date
        / f"target_quotes_{trade_date}.jsonl"
        for task in tasks
    ]
    if not force and all(path.exists() and path.stat().st_size > 0 for path in expected):
        return {
            "event": "symbol_streams_reused",
            "trade_date": trade_date,
            "symbols": len(tasks),
            "source": str(source),
        }

    handles: dict[str, Any] = {}
    counts: dict[str, int] = {}
    started = time.perf_counter()
    try:
        for task in tasks:
            symbol = str(task["symbol"])
            out_dir = symbol_stream_dir(filtered_root, int(task["shard"]), symbol) / trade_date
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"target_quotes_{trade_date}.jsonl"
            handles[symbol] = out_path.open("wb")
            counts[symbol] = 0
        rows_seen = 0
        rows_written = 0
        with source.open("rb") as src:
            for line in src:
                rows_seen += 1
                key = extract_key_from_line(line)
                for task in key_to_tasks.get(key or "", []):
                    symbol = str(task["symbol"])
                    handles[symbol].write(line)
                    counts[symbol] += 1
                    rows_written += 1
        return {
            "event": "symbol_streams_materialized",
            "trade_date": trade_date,
            "source": str(source),
            "rows_seen": rows_seen,
            "rows_written": rows_written,
            "symbols": len(tasks),
            "symbols_with_rows": sum(1 for count in counts.values() if count),
            "duration_seconds": round(time.perf_counter() - started, 3),
        }
    finally:
        for handle in handles.values():
            handle.close()


def prepare_one_symbol_run(
    *,
    filtered_root: Path,
    source_state: Path,
    task: dict[str, Any],
    trade_date: str,
    force: bool,
) -> Path:
    symbol = str(task["symbol"])
    shard = int(task["shard"])
    run_dir = symbol_run_dir(filtered_root, shard, symbol)
    if force and run_dir.exists():
        shutil.rmtree(run_dir)
    (run_dir / "config").mkdir(parents=True, exist_ok=True)
    src_inst = source_instrument_dir(source_state, symbol)
    dst_inst = run_dir / "instruments" / src_inst.name
    if dst_inst.exists():
        shutil.rmtree(dst_inst)
    dst_inst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src_inst, dst_inst)

    base_runtime = read_json(Path(str(task["runtime"])), {})
    entry = dict(task["entry"])
    universe = {
        "schema": "obvfutport_v2.single_symbol_incremental_universe.v1",
        "created_for": "v2_symbol_incremental_eod_append",
        "entries": [entry],
        "symbol_count": 1,
    }
    universe_path = run_dir / "config" / "universe.json"
    atomic_write_json(universe_path, universe)

    stream_source = symbol_stream_dir(filtered_root, shard, symbol)
    link = run_dir / "target_stream"
    if link.exists() or link.is_symlink():
        if link.is_dir() and not link.is_symlink():
            shutil.rmtree(link)
        else:
            link.unlink()
    link.symlink_to(stream_source, target_is_directory=True)

    runtime = dict(base_runtime)
    runtime["state_dir"] = str(run_dir)
    runtime["state_dir_local"] = str(run_dir)
    runtime["hurst_universe_manifest_path"] = str(universe_path)
    runtime["hurst_universe_manifest_path_local"] = str(universe_path)
    runtime["target_stream_root"] = str(link)
    runtime["target_stream_root_local"] = str(link)
    runtime["bootstrap_load_enabled"] = False
    runtime["skip_past_due_clocks_on_start"] = False
    runtime["start_at_eof_on_market_restart"] = False
    runtime["archive_replay_disable_live_stale_entry_marking"] = False
    runtime["compute_non_clock_percentiles"] = False
    atomic_write_json(run_dir / "config" / "runtime.json", runtime)
    return run_dir


def run_one_symbol(args: argparse.Namespace) -> int:
    root = Path(args.root)
    sys.path.insert(0, str(root / "src"))
    import obvfut_portable_v2.passive_runner as pr  # noqa: WPS433

    filtered_root = Path(args.filtered_root)
    source_state = Path(args.source_state)
    task = read_json(Path(args.task_json), {})
    trade_date = str(args.trade_date)
    run_dir = prepare_one_symbol_run(
        filtered_root=filtered_root,
        source_state=source_state,
        task=task,
        trade_date=trade_date,
        force=bool(args.force),
    )
    if report_ok(run_dir) and not args.force:
        print(json.dumps({"event": "already_ok", "symbol": task["symbol"], "run_dir": str(run_dir)}), flush=True)
        return 0

    real_now_ist = pr.now_ist
    fake_pre_roll_now = datetime.fromisoformat(f"{trade_date}T09:00:00{IST_OFFSET}")
    pr.now_ist = lambda: fake_pre_roll_now
    runner = pr.PassiveV2Runner(run_dir / "config" / "runtime.json")
    pr.now_ist = real_now_ist

    runner.add_clock_epochs_for_trade_date(trade_date)
    trade_day = date.fromisoformat(trade_date)
    clock_epochs = pr.clock_epochs_for_day(
        trade_day,
        clock_start=runner.clock_start,
        clock_end=runner.clock_end,
        clock_step_minutes=runner.clock_step_minutes,
    )
    entry_delay_seconds = int(runner.config.get("entry_delay_seconds") or 60)
    checkpoint_delays = {int(runner.decision_delay_seconds), int(entry_delay_seconds)}
    checkpoints = sorted({int(epoch) + int(delay) for epoch in clock_epochs for delay in checkpoint_delays})
    rollover_epoch = parse_hhmm_to_epoch(trade_date, "15:25")
    checkpoints = sorted(set(checkpoints + [rollover_epoch]))
    checkpoint_index = 0
    rows_seen = 0
    quotes_used = 0
    last_replay_epoch = 0
    trade_state_reports: list[dict[str, Any]] = []
    rollover_reports: list[dict[str, Any]] = []
    target_set = set(runner.targets)
    started = time.perf_counter()
    stream_path = runner.target_stream_path(trade_date)
    if not stream_path.exists():
        raise FileNotFoundError(f"missing symbol target stream: {stream_path}")

    def run_trade_state_passes(checkpoint_epoch: int, reason_prefix: str) -> None:
        for state in runner.states.values():
            state.finalize_until(int(checkpoint_epoch))
        for pass_index in range(max(1, int(args.max_trade_state_passes))):
            report = runner.evaluate_frozen_trade_state(
                trade_date,
                reason=f"{reason_prefix}_pass_{pass_index + 1}",
                evaluation_epoch=int(checkpoint_epoch),
            )
            trade_state_reports.append(report)
            if int(report.get("events") or 0) == 0:
                break

    with stream_path.open("rb") as handle:
        for line in handle:
            rows_seen += 1
            row = pr.row_from_target_stream_line(line, trade_date, target_set)
            if row is None:
                continue
            runner.states[str(row["target"])].process_row(row)
            quotes_used += 1
            row_epoch = int(row.get("epoch_second") or row.get("epoch") or 0)
            if row_epoch > 0:
                last_replay_epoch = max(last_replay_epoch, row_epoch)
                received_epoch = pr.as_float(row.get("received_epoch")) or pr.as_float(row.get("epoch"))
                if received_epoch is not None:
                    runner.latest_feed_epoch = max(runner.latest_feed_epoch or received_epoch, received_epoch)
                while checkpoint_index < len(checkpoints) and int(checkpoints[checkpoint_index]) <= row_epoch:
                    checkpoint_epoch = int(checkpoints[checkpoint_index])
                    if checkpoint_epoch == rollover_epoch:
                        for state in runner.states.values():
                            state.finalize_until(checkpoint_epoch)
                        rollover_reports.append(
                            runner.evaluate_rollovers(
                                trade_date,
                                when=datetime.fromtimestamp(rollover_epoch).astimezone(),
                            )
                        )
                    run_trade_state_passes(
                        checkpoint_epoch,
                        f"symbol_incremental_{trade_date}_checkpoint_{pr.epoch_ist_iso(checkpoint_epoch)}",
                    )
                    checkpoint_index += 1

    for state in runner.states.values():
        state.flush_until_latest()
    final_epoch = int(last_replay_epoch or runner.latest_feed_epoch or time.time())
    while checkpoint_index < len(checkpoints) and int(checkpoints[checkpoint_index]) <= final_epoch:
        checkpoint_epoch = int(checkpoints[checkpoint_index])
        if checkpoint_epoch == rollover_epoch:
            rollover_reports.append(
                runner.evaluate_rollovers(
                    trade_date,
                    when=datetime.fromtimestamp(rollover_epoch).astimezone(),
                )
            )
        run_trade_state_passes(
            checkpoint_epoch,
            f"symbol_incremental_{trade_date}_checkpoint_{pr.epoch_ist_iso(checkpoint_epoch)}",
        )
        checkpoint_index += 1
    run_trade_state_passes(final_epoch, f"symbol_incremental_{trade_date}_final")
    manifest = runner.save_bootstrap_states(
        trade_date,
        [
            {
                "trade_date": trade_date,
                "source": str(stream_path),
                "source_type": "target_stream_history_symbol_filtered",
                "source_found": True,
                "rows_seen": rows_seen,
                "quotes_used": quotes_used,
                "duration_seconds": round(time.perf_counter() - started, 4),
                "partial": False,
            }
        ],
        promote_latest=False,
    )
    runner.write_status()
    date_report = {
        "trade_date": trade_date,
        "source": str(stream_path),
        "source_type": "target_stream_history_symbol_filtered",
        "source_found": True,
        "rows_seen": rows_seen,
        "quotes_used": quotes_used,
        "trade_state_passes": len(trade_state_reports),
        "trade_state_events": sum(int(item.get("events") or 0) for item in trade_state_reports),
        "trade_state_last_report": trade_state_reports[-1] if trade_state_reports else None,
        "rollover_reports": rollover_reports,
        "duration_seconds": round(time.perf_counter() - started, 4),
        "partial": False,
    }
    report = {
        "schema": "obvfutport_v2.symbol_incremental_eod_report.v1",
        "ok": True,
        "partial": False,
        "history_source": "target_stream_symbol_incremental",
        "start_date": trade_date,
        "end_date": trade_date,
        "output_state_dir": str(run_dir),
        "symbols": len(runner.instruments),
        "target_keys": len(runner.targets),
        "date_reports": [date_report],
        "bootstrap_manifest": manifest,
        "updated_at_ist": real_now_ist().isoformat(),
    }
    atomic_write_json(run_dir / "archive_replay_report.json", report)
    print(
        json.dumps(
            {
                "event": "symbol_done",
                "symbol": task["symbol"],
                "rows_seen": rows_seen,
                "quotes_used": quotes_used,
                "events": date_report["trade_state_events"],
                "duration_seconds": date_report["duration_seconds"],
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


def summarize(filtered_root: Path, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    complete = 0
    failed = 0
    pending = 0
    total_events = 0
    for task in tasks:
        run_dir = symbol_run_dir(filtered_root, int(task["shard"]), str(task["symbol"]))
        report = read_json(run_dir / "archive_replay_report.json", None)
        row = {"shard": task["shard"], "symbol": task["symbol"], "run_dir": str(run_dir), "status": "pending"}
        if report is not None:
            events = sum(int(item.get("trade_state_events") or 0) for item in report.get("date_reports") or [])
            ok = bool(report.get("ok")) and not bool(report.get("partial"))
            row.update({"status": "ok" if ok else "failed", "ok": ok, "partial": bool(report.get("partial")), "events": events})
            total_events += events
            if ok:
                complete += 1
            else:
                failed += 1
        else:
            pending += 1
        rows.append(row)
    out = {
        "schema": "obvfutport_v2.symbol_incremental_eod_summary.v1",
        "root": str(filtered_root),
        "tasks": len(tasks),
        "complete": complete,
        "failed": failed,
        "pending": pending,
        "total_events": total_events,
        "rows": rows,
        "updated_epoch": time.time(),
    }
    atomic_write_json(filtered_root / "reports" / "symbol_checkpoint_replay_summary.json", out)
    return out


def orchestrate(args: argparse.Namespace) -> int:
    filtered_root = Path(args.filtered_root)
    source_state = Path(args.source_state)
    tasks = load_tasks(filtered_root)
    if not tasks:
        raise SystemExit("no tasks found")
    materialize_report = materialize_symbol_streams(filtered_root, tasks, str(args.trade_date), bool(args.force_materialize))
    atomic_write_json(filtered_root / "reports" / f"symbol_stream_materialize_{args.trade_date}.json", materialize_report)
    print(json.dumps(materialize_report, sort_keys=True), flush=True)

    task_root = filtered_root / "tasks"
    task_root.mkdir(parents=True, exist_ok=True)
    logs_dir = filtered_root / "logs" / "symbol_incremental"
    logs_dir.mkdir(parents=True, exist_ok=True)
    running: dict[subprocess.Popen[bytes], dict[str, Any]] = {}
    task_index = 0
    last_summary = 0.0
    max_workers = max(1, int(args.max_workers))
    min_free_mb = int(args.min_free_mb)
    python = str(args.python or sys.executable)

    while task_index < len(tasks) or running:
        while task_index < len(tasks) and len(running) < max_workers:
            task = tasks[task_index]
            task_index += 1
            run_dir = symbol_run_dir(filtered_root, int(task["shard"]), str(task["symbol"]))
            if report_ok(run_dir) and not args.force:
                continue
            if mem_available_mb() < min_free_mb:
                task_index -= 1
                break
            task_json = task_root / f"shard_{int(task['shard']):02d}_{safe_symbol(str(task['symbol']))}.json"
            atomic_write_json(task_json, task)
            log_path = logs_dir / f"shard_{int(task['shard']):02d}_{safe_symbol(str(task['symbol']))}.log"
            cmd = [
                "nice",
                "-n",
                "12",
                python,
                str(Path(__file__).resolve()),
                "--one-symbol",
                "--root",
                str(args.root),
                "--filtered-root",
                str(filtered_root),
                "--source-state",
                str(source_state),
                "--trade-date",
                str(args.trade_date),
                "--task-json",
                str(task_json),
                "--max-trade-state-passes",
                str(args.max_trade_state_passes),
            ]
            if args.force:
                cmd.append("--force")
            handle = log_path.open("wb")
            proc = subprocess.Popen(cmd, cwd=str(args.root), stdout=handle, stderr=subprocess.STDOUT)
            running[proc] = {
                "symbol": task["symbol"],
                "shard": task["shard"],
                "run_dir": str(run_dir),
                "log": str(log_path),
                "started_epoch": time.time(),
                "_handle": handle,
            }
            print(
                json.dumps(
                    {
                        "event": "started",
                        "pid": proc.pid,
                        "symbol": task["symbol"],
                        "shard": task["shard"],
                        "free_mb": mem_available_mb(),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

        for proc, task in list(running.items()):
            rc = proc.poll()
            if rc is None:
                continue
            handle = task.pop("_handle", None)
            if handle is not None:
                handle.close()
            status = "ok" if rc == 0 and report_ok(Path(str(task["run_dir"]))) else "failed"
            print(
                json.dumps(
                    {
                        "event": "finished",
                        "symbol": task["symbol"],
                        "shard": task["shard"],
                        "status": status,
                        "returncode": rc,
                        "duration_seconds": round(time.time() - float(task["started_epoch"]), 3),
                        "log": task["log"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            del running[proc]

        now = time.time()
        if now - last_summary >= 30:
            summary = summarize(filtered_root, tasks)
            print(
                json.dumps(
                    {
                        "event": "summary",
                        "complete": summary["complete"],
                        "failed": summary["failed"],
                        "pending": summary["pending"],
                        "running": len(running),
                        "launched": task_index,
                        "free_mb": mem_available_mb(),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            last_summary = now
        time.sleep(2.0)

    summary = summarize(filtered_root, tasks)
    print(json.dumps({"event": "done", **{k: summary[k] for k in ("tasks", "complete", "failed", "pending", "total_events")}}, sort_keys=True), flush=True)
    return 0 if summary["failed"] == 0 and summary["pending"] == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2")
    parser.add_argument("--filtered-root", required=True)
    parser.add_argument("--source-state", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--python", default="/opt/cloud-deploy-candidates/intraday-short-straddle-v1/.venv/bin/python")
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--min-free-mb", type=int, default=3500)
    parser.add_argument("--max-trade-state-passes", type=int, default=20)
    parser.add_argument("--force-materialize", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--one-symbol", action="store_true")
    parser.add_argument("--task-json", default=None)
    args = parser.parse_args()
    if args.one_symbol:
        if not args.task_json:
            raise SystemExit("--task-json is required with --one-symbol")
        return run_one_symbol(args)
    return orchestrate(args)


if __name__ == "__main__":
    raise SystemExit(main())
