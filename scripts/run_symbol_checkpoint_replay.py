#!/usr/bin/env python3
"""Run OBVFUTPORT-v2 checkpoint replay one symbol at a time.

This is an operational helper for canonical ledger population. It keeps the
strategy path unchanged by invoking the normal archive-replay command, but it
reduces memory pressure by giving each process only one symbol universe.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
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


def mem_available_mb() -> int:
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            name, raw = line.split(":", 1)
            if name in {"MemAvailable", "SwapFree"}:
                values[name] = int(raw.strip().split()[0]) // 1024
    except Exception:
        return 999_999
    return int(values.get("MemAvailable", 0) + values.get("SwapFree", 0))


def report_ok(path: Path) -> bool:
    report = path / "archive_replay_report.json"
    if not report.exists():
        return False
    data = read_json(report, {})
    return bool(data.get("ok")) and not bool(data.get("partial"))


def symbol_run_dir(root: Path, shard: int, symbol: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in symbol)
    return root / "symbol_runs" / f"shard_{shard:02d}_{safe}"


def symbol_stream_dir(root: Path, shard: int, symbol: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in symbol)
    return root / "symbol_streams" / f"shard_{shard:02d}_{safe}"


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


def materialize_symbol_streams(
    *,
    filtered_root: Path,
    tasks: list[dict[str, Any]],
    start_date: str,
    end_date: str,
    force: bool,
) -> None:
    by_shard: dict[int, list[dict[str, Any]]] = {}
    for task in tasks:
        by_shard.setdefault(int(task["shard"]), []).append(task)

    for shard, shard_tasks in sorted(by_shard.items()):
        key_to_task: dict[str, dict[str, Any]] = {}
        for task in shard_tasks:
            for key in task.get("keys") or []:
                key_to_task[str(key)] = task
        shard_stream = filtered_root / f"shard_{shard:02d}" / "target_stream"
        for date_dir in sorted(shard_stream.glob("2026-*-*")):
            trade_date = date_dir.name
            if trade_date < start_date or trade_date > end_date:
                continue
            source = date_dir / f"target_quotes_{trade_date}.jsonl"
            if not source.exists():
                continue
            expected = [
                symbol_stream_dir(filtered_root, shard, str(task["symbol"]))
                / trade_date
                / f"target_quotes_{trade_date}.jsonl"
                for task in shard_tasks
            ]
            if not force and all(path.exists() and path.stat().st_size > 0 for path in expected):
                continue
            handles: dict[str, Any] = {}
            counts: dict[str, int] = {}
            try:
                for task in shard_tasks:
                    symbol = str(task["symbol"])
                    out_dir = symbol_stream_dir(filtered_root, shard, symbol) / trade_date
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_path = out_dir / f"target_quotes_{trade_date}.jsonl"
                    handles[symbol] = out_path.open("wb")
                    counts[symbol] = 0
                rows_seen = 0
                rows_written = 0
                started = time.perf_counter()
                with source.open("rb") as src:
                    for line in src:
                        rows_seen += 1
                        key = extract_key_from_line(line)
                        task = key_to_task.get(key or "")
                        if task is None:
                            continue
                        symbol = str(task["symbol"])
                        handles[symbol].write(line)
                        counts[symbol] += 1
                        rows_written += 1
                print(
                    json.dumps(
                        {
                            "event": "symbol_stream_materialized",
                            "shard": shard,
                            "trade_date": trade_date,
                            "rows_seen": rows_seen,
                            "rows_written": rows_written,
                            "duration_seconds": round(time.perf_counter() - started, 3),
                            "symbols_with_rows": sum(1 for count in counts.values() if count),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
            finally:
                for handle in handles.values():
                    handle.close()


def prepare_symbol_run(
    *,
    filtered_root: Path,
    symbol_root: Path,
    shard: dict[str, Any],
    entry: dict[str, Any],
    stream_source: Path,
    force: bool,
) -> Path:
    symbol = str(entry["symbol"])
    run_dir = symbol_run_dir(symbol_root, int(shard["shard"]), symbol)
    if force and run_dir.exists():
        shutil.rmtree(run_dir)
    (run_dir / "config").mkdir(parents=True, exist_ok=True)

    shard_dir = filtered_root / f"shard_{int(shard['shard']):02d}"
    shard_universe = read_json(shard_dir / "config" / "universe.json", {})
    shard_runtime = read_json(shard_dir / "config" / "runtime.json", {})

    universe = {k: v for k, v in shard_universe.items() if k != "entries"}
    universe["entries"] = [entry]
    universe["symbol_count"] = 1
    universe["created_for"] = "v2_aug10_flat_symbol_checkpoint_replay"
    universe_path = run_dir / "config" / "universe.json"
    atomic_write_json(universe_path, universe)

    runtime = dict(shard_runtime)
    runtime["state_dir"] = str(run_dir)
    runtime["state_dir_local"] = str(run_dir)
    runtime["hurst_universe_manifest_path"] = str(universe_path)
    runtime["hurst_universe_manifest_path_local"] = str(universe_path)
    runtime["archive_replay_event_time_checkpoints_enabled"] = True
    runtime["archive_replay_disable_live_stale_entry_marking"] = True
    runtime["archive_replay_second_row_retention_seconds"] = int(
        runtime.get("archive_replay_second_row_retention_seconds") or 30000
    )
    link = run_dir / "target_stream"
    runtime["target_stream_root"] = str(link)
    runtime["target_stream_root_local"] = str(link)
    runtime_path = run_dir / "config" / "runtime.json"
    atomic_write_json(runtime_path, runtime)

    if link.exists() or link.is_symlink():
        if link.is_symlink() and link.resolve() == stream_source.resolve():
            pass
        else:
            if link.is_dir() and not link.is_symlink():
                shutil.rmtree(link)
            else:
                link.unlink()
    if not link.exists():
        link.symlink_to(stream_source, target_is_directory=True)
    return run_dir


def summarize(root: Path, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    complete = 0
    failed = 0
    pending = 0
    total_events = 0
    for task in tasks:
        run_dir = Path(task["run_dir"])
        report = read_json(run_dir / "archive_replay_report.json", None)
        row = {
            "shard": task["shard"],
            "symbol": task["symbol"],
            "run_dir": str(run_dir),
            "status": "pending",
        }
        if report is not None:
            events = sum(int(item.get("trade_state_events") or 0) for item in report.get("date_reports") or [])
            row.update(
                {
                    "status": "ok" if bool(report.get("ok")) and not bool(report.get("partial")) else "failed",
                    "ok": bool(report.get("ok")),
                    "partial": bool(report.get("partial")),
                    "dates": len(report.get("date_reports") or []),
                    "events": events,
                    "updated_at_ist": report.get("updated_at_ist"),
                }
            )
            total_events += events
            if row["status"] == "ok":
                complete += 1
            else:
                failed += 1
        else:
            pending += 1
        rows.append(row)
    out = {
        "schema": "obvfutport_v2.symbol_checkpoint_replay_summary.v1",
        "root": str(root),
        "tasks": len(tasks),
        "complete": complete,
        "failed": failed,
        "pending": pending,
        "total_events": total_events,
        "rows": rows,
        "updated_epoch": time.time(),
    }
    atomic_write_json(root / "reports" / "symbol_checkpoint_replay_summary.json", out)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2")
    parser.add_argument("--filtered-root", default="state_aug10_flat_50_checkpoint_filtered_20260819")
    parser.add_argument("--start-shard", type=int, default=0)
    parser.add_argument("--end-shard", type=int, default=9)
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-19")
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--min-free-mb", type=int, default=3500)
    parser.add_argument("--python", default="/opt/cloud-deploy-candidates/intraday-short-straddle-v1/.venv/bin/python")
    parser.add_argument("--materialize-symbol-streams", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    filtered_root = root / args.filtered_root
    setup = read_json(filtered_root / "shard_setup.json", {})
    shards = [
        item
        for item in setup.get("shards", [])
        if int(args.start_shard) <= int(item.get("shard", -1)) <= int(args.end_shard)
    ]
    raw_tasks: list[dict[str, Any]] = []
    for shard in shards:
        shard_dir = filtered_root / f"shard_{int(shard['shard']):02d}"
        universe = read_json(shard_dir / "config" / "universe.json", {})
        entries = list(universe.get("entries") or [])
        by_symbol = {str(entry.get("symbol")): entry for entry in entries}
        for symbol in shard.get("symbols") or sorted(by_symbol):
            entry = by_symbol.get(str(symbol))
            if not entry:
                continue
            raw_tasks.append(
                {
                    "shard": int(shard["shard"]),
                    "symbol": str(symbol),
                    "entry": entry,
                    "keys": sorted(entry_target_keys(entry)),
                }
            )

    if args.materialize_symbol_streams:
        materialize_symbol_streams(
            filtered_root=filtered_root,
            tasks=raw_tasks,
            start_date=args.start_date,
            end_date=args.end_date,
            force=bool(args.force),
        )

    tasks: list[dict[str, Any]] = []
    for raw_task in raw_tasks:
        shard = next(item for item in shards if int(item["shard"]) == int(raw_task["shard"]))
        stream_source = (
            symbol_stream_dir(filtered_root, int(raw_task["shard"]), str(raw_task["symbol"]))
            if args.materialize_symbol_streams
            else filtered_root / f"shard_{int(raw_task['shard']):02d}" / "target_stream"
        )
        run_dir = prepare_symbol_run(
                filtered_root=filtered_root,
                symbol_root=filtered_root,
                shard=shard,
                entry=raw_task["entry"],
                stream_source=stream_source,
                force=bool(args.force),
            )
        tasks.append({"shard": int(raw_task["shard"]), "symbol": str(raw_task["symbol"]), "run_dir": str(run_dir)})

    if not tasks:
        print(json.dumps({"ok": False, "reason": "no_tasks"}, indent=2))
        return 2

    env = dict(os.environ)
    env["PYTHONPATH"] = str(root / "src")
    logs_dir = filtered_root / "logs" / "symbol_runs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    running: dict[subprocess.Popen[bytes], dict[str, Any]] = {}
    completed_this_run: list[dict[str, Any]] = []
    task_index = 0
    last_summary = 0.0

    while task_index < len(tasks) or running:
        while task_index < len(tasks) and len(running) < max(1, int(args.max_workers)):
            task = tasks[task_index]
            task_index += 1
            run_dir = Path(task["run_dir"])
            if not args.force and report_ok(run_dir):
                task["status"] = "already_ok"
                completed_this_run.append(task)
                continue
            free_mb = mem_available_mb()
            if free_mb < int(args.min_free_mb):
                task_index -= 1
                break
            symbol = task["symbol"]
            log_path = logs_dir / f"shard_{task['shard']:02d}_{symbol}.log"
            cmd = [
                "nice",
                "-n",
                "12",
                args.python,
                "-m",
                "obvfut_portable_v2.passive_runner",
                "archive-replay",
                "--config",
                str(run_dir / "config" / "runtime.json"),
                "--start-date",
                args.start_date,
                "--end-date",
                args.end_date,
                "--output-state-dir",
                str(run_dir),
                "--use-target-stream-history",
                "--max-trade-state-passes",
                "20",
                "--output",
                str(run_dir / "archive_replay_report_copy.json"),
            ]
            handle = log_path.open("wb")
            proc = subprocess.Popen(cmd, cwd=str(root), env=env, stdout=handle, stderr=subprocess.STDOUT)
            running[proc] = {**task, "log": str(log_path), "started_epoch": time.time(), "_handle": handle}
            print(
                json.dumps(
                    {
                        "event": "started",
                        "pid": proc.pid,
                        "symbol": symbol,
                        "shard": task["shard"],
                        "free_mb": free_mb,
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
            task["returncode"] = rc
            task["duration_seconds"] = round(time.time() - float(task.get("started_epoch") or time.time()), 3)
            task["status"] = "ok" if rc == 0 and report_ok(Path(task["run_dir"])) else "failed"
            completed_this_run.append(task)
            del running[proc]
            print(json.dumps({"event": "finished", **{k: v for k, v in task.items() if not k.startswith("_")}}, sort_keys=True), flush=True)

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


if __name__ == "__main__":
    raise SystemExit(main())
