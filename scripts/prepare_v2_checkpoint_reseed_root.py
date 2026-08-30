#!/usr/bin/env python3
"""Prepare an OBVFUTPORT-v2 symbol-checkpoint replay root.

This builds a v2-only replay workspace from the current universe, a curated
per-date target-stream source, and a versioned adaptive override file. It does
not run replay or mutate production state.
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_json_default(path: Path, default: Any) -> Any:
    try:
        return read_json(path)
    except Exception:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def safe_symbol(symbol: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(symbol))


def resolve_path(root: Path, value: Any) -> Path | None:
    if value in (None, ""):
        return None
    path = Path(str(value))
    return path if path.is_absolute() else root / path


def trade_dates(start_date: str, end_date: str) -> list[str]:
    current = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    out: list[str] = []
    while current <= final:
        if current.weekday() < 5:
            out.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return out


def choose_stream_file(roots: list[Path], trade_date: str) -> tuple[Path | None, list[dict[str, Any]]]:
    filename = f"target_quotes_{trade_date}.jsonl"
    candidates: list[dict[str, Any]] = []
    for root in roots:
        path = root / trade_date / filename
        exists = path.exists()
        size = path.stat().st_size if exists else 0
        candidates.append({"root": str(root), "path": str(path), "exists": exists, "size_bytes": size})
    valid = [Path(item["path"]) for item in candidates if item["exists"] and int(item["size_bytes"]) > 1024]
    if not valid:
        return None, candidates
    return max(valid, key=lambda path: path.stat().st_size), candidates


def entry_symbol(entry: dict[str, Any]) -> str:
    return str(entry.get("symbol") or entry.get("instrument_id") or "").strip()


def baseline_state_symbols(state_dir: Path) -> set[str]:
    instruments = state_dir / "instruments"
    if not instruments.exists():
        return set()
    return {
        path.name
        for path in instruments.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    }


def baseline_manifest_source_roots(root: Path, manifest: dict[str, Any]) -> list[Path]:
    candidates: list[Any] = [
        manifest.get("run_root"),
        manifest.get("source_run_root"),
        manifest.get("source_root"),
        manifest.get("replay_root"),
        manifest.get("output_root"),
    ]
    for key in ("source_pointer", "source", "reseed_source", "source_chain"):
        value = manifest.get(key)
        if isinstance(value, dict):
            candidates.extend(
                [
                    value.get("run_root"),
                    value.get("source_run_root"),
                    value.get("source_root"),
                    value.get("replay_root"),
                    value.get("output_root"),
                ]
            )
    roots: list[Path] = []
    seen: set[str] = set()
    for raw in candidates:
        if not raw:
            continue
        path = Path(str(raw))
        if not path.is_absolute():
            path = root / path
        key = str(path)
        if key not in seen:
            roots.append(path)
            seen.add(key)
    return roots


def entries_from_reseed_root(reseed_root: Path) -> dict[str, dict[str, Any]]:
    setup = read_json_default(reseed_root / "shard_setup.json", {})
    if not isinstance(setup, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for shard in setup.get("shards") or []:
        if not isinstance(shard, dict):
            continue
        universe_path = Path(str(shard.get("universe") or ""))
        if not universe_path.is_absolute():
            universe_path = reseed_root / universe_path
        universe = read_json_default(universe_path, {})
        for entry in universe.get("entries") or []:
            if not isinstance(entry, dict):
                continue
            symbol = entry_symbol(entry)
            if symbol and symbol not in out:
                out[symbol] = dict(entry)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--adaptive-override", required=True)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--stream-root", action="append", default=[])
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--allow-baseline-symbol-drop",
        action="store_true",
        help="Allow symbols present in the verified baseline state to be absent from the prepared universe.",
    )
    args = parser.parse_args()

    root = Path(args.root)
    output_root = Path(args.output_root)
    if not output_root.is_absolute():
        output_root = root / output_root
    if output_root.exists():
        if not args.force:
            raise SystemExit(f"output root exists; pass --force to replace: {output_root}")
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    runtime = read_json(root / "config" / "runtime.json")
    universe_path = resolve_path(root, runtime.get("hurst_universe_manifest_path")) or resolve_path(
        root, runtime.get("hurst_universe_manifest_path_local")
    )
    if universe_path is None or not universe_path.exists():
        raise SystemExit("unable to resolve v2 universe manifest")
    universe = read_json(universe_path)
    entries = sorted(list(universe.get("entries") or []), key=lambda item: str(item.get("symbol") or ""))
    if not entries:
        raise SystemExit("v2 universe contains no entries")

    baseline_manifest_path = root / "state" / "eod_baselines" / "latest_post_eod_baseline_manifest.json"
    baseline_manifest = read_json_default(baseline_manifest_path, {})
    baseline_state = Path(str(baseline_manifest.get("baseline_state") or ""))
    if baseline_state and not baseline_state.is_absolute():
        baseline_state = root / baseline_state
    baseline_symbols = baseline_state_symbols(baseline_state) if baseline_state.exists() else set()
    current_symbols = {entry_symbol(entry) for entry in entries if isinstance(entry, dict)}
    missing_baseline_symbols = sorted(symbol for symbol in baseline_symbols - current_symbols if symbol)
    preserved_entries: list[dict[str, Any]] = []
    unresolved_missing_symbols: list[str] = []
    if missing_baseline_symbols:
        previous_entries: dict[str, dict[str, Any]] = {}
        for source_root in baseline_manifest_source_roots(root, baseline_manifest if isinstance(baseline_manifest, dict) else {}):
            previous_entries.update(entries_from_reseed_root(source_root))
        for symbol in missing_baseline_symbols:
            preserved = previous_entries.get(symbol)
            if not preserved:
                unresolved_missing_symbols.append(symbol)
                continue
            item = dict(preserved)
            item["baseline_universe_reconciliation"] = {
                "mode": "preserve_baseline_only_symbol",
                "baseline_manifest": str(baseline_manifest_path),
                "baseline_state": str(baseline_state),
                "reason": "symbol_present_in_verified_baseline_state_but_absent_from_current_universe",
            }
            preserved_entries.append(item)
        if unresolved_missing_symbols and not args.allow_baseline_symbol_drop:
            raise SystemExit(
                "baseline symbols missing from current universe without preservation source: "
                + ",".join(unresolved_missing_symbols)
                + "; pass --allow-baseline-symbol-drop only for an explicit baseline transition"
            )
        entries = sorted(
            [*entries, *preserved_entries],
            key=lambda item: str(item.get("symbol") or item.get("instrument_id") or ""),
        )

    stream_roots: list[Path] = []
    for raw in args.stream_root:
        path = Path(raw)
        stream_roots.append(path if path.is_absolute() else root / path)
    for key in ("state_dir", "target_stream_root", "target_stream_root_local"):
        value = runtime.get(key)
        if key == "state_dir" and value:
            base = resolve_path(root, value)
            if base is not None:
                stream_roots.append(base / "target_stream")
            continue
        path = resolve_path(root, value)
        if path is not None:
            stream_roots.append(path)
    seen: set[str] = set()
    unique_roots: list[Path] = []
    for path in stream_roots:
        key = str(path)
        if key not in seen:
            unique_roots.append(path)
            seen.add(key)

    curated_root = output_root / "curated_target_stream"
    source_rows: list[dict[str, Any]] = []
    missing_dates: list[str] = []
    for trade_date in trade_dates(args.start_date, args.end_date):
        source, candidates = choose_stream_file(unique_roots, trade_date)
        source_rows.append({"trade_date": trade_date, "selected": str(source) if source else None, "candidates": candidates})
        if source is None:
            missing_dates.append(trade_date)
            continue
        target_dir = curated_root / trade_date
        target_dir.mkdir(parents=True, exist_ok=True)
        link = target_dir / f"target_quotes_{trade_date}.jsonl"
        link.symlink_to(source)

    shard_count = max(1, int(args.shard_count))
    shards: list[dict[str, Any]] = []
    for shard_id in range(shard_count):
        shard_entries = [entry for index, entry in enumerate(entries) if index % shard_count == shard_id]
        shard_dir = output_root / f"shard_{shard_id:02d}"
        config_dir = shard_dir / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        shard_universe = {k: v for k, v in universe.items() if k != "entries"}
        shard_universe["entries"] = shard_entries
        shard_universe["symbol_count"] = len(shard_entries)
        shard_universe["created_for"] = "v2_joint_adaptive_checkpoint_reseed"
        universe_out = config_dir / "universe.json"
        atomic_write_json(universe_out, shard_universe)

        shard_runtime = dict(runtime)
        shard_runtime["state_dir"] = str(shard_dir)
        shard_runtime["state_dir_local"] = str(shard_dir)
        shard_runtime["hurst_universe_manifest_path"] = str(universe_out)
        shard_runtime["hurst_universe_manifest_path_local"] = str(universe_out)
        shard_runtime["target_stream_root"] = str(shard_dir / "target_stream")
        shard_runtime["target_stream_root_local"] = str(shard_dir / "target_stream")
        shard_runtime["adaptive_calibration_enabled"] = True
        shard_runtime["adaptive_calibration_path"] = str(Path(args.adaptive_override))
        shard_runtime["adaptive_calibration_path_local"] = str(Path(args.adaptive_override))
        shard_runtime["archive_replay_event_time_checkpoints_enabled"] = True
        shard_runtime["archive_replay_disable_live_stale_entry_marking"] = True
        shard_runtime["archive_replay_second_row_retention_seconds"] = int(
            shard_runtime.get("archive_replay_second_row_retention_seconds") or 30000
        )
        atomic_write_json(config_dir / "runtime.json", shard_runtime)

        stream_link = shard_dir / "target_stream"
        stream_link.symlink_to(curated_root, target_is_directory=True)
        shards.append(
            {
                "shard": shard_id,
                "symbols": [str(entry.get("symbol")) for entry in shard_entries],
                "symbol_count": len(shard_entries),
                "runtime": str(config_dir / "runtime.json"),
                "universe": str(universe_out),
            }
        )

    report = {
        "schema": "obvfutport_v2.checkpoint_reseed_root.v1",
        "ok": not missing_dates,
        "root": str(root),
        "output_root": str(output_root),
        "created_at_ist": datetime.now().astimezone().isoformat(),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "adaptive_override": str(Path(args.adaptive_override)),
        "source_universe": str(universe_path),
        "symbol_count": len(entries),
        "baseline_manifest": str(baseline_manifest_path) if baseline_manifest else None,
        "baseline_state": str(baseline_state) if baseline_symbols else None,
        "baseline_symbol_count": len(baseline_symbols),
        "baseline_symbols_missing_from_current_universe": missing_baseline_symbols,
        "baseline_symbols_preserved": [entry_symbol(entry) for entry in preserved_entries],
        "baseline_symbols_unresolved": unresolved_missing_symbols,
        "baseline_symbol_drop_allowed": bool(args.allow_baseline_symbol_drop),
        "shard_count": shard_count,
        "shards": shards,
        "stream_roots_considered": [str(path) for path in unique_roots],
        "stream_sources": source_rows,
        "missing_dates": missing_dates,
    }
    atomic_write_json(output_root / "shard_setup.json", report)
    atomic_write_json(output_root / "reseed_root_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
