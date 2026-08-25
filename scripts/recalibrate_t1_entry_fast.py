#!/usr/bin/env python3
"""Fast OBVFUTPORT-v2 T1 entry recalibration precompute/smoke runner.

This utility is deliberately read-only for live strategy state. It validates
quote-valid target-stream inputs, builds reusable per-symbol clock panels in a
single pass over each day, and sweeps T1 entry thresholds from the cached clock
rows instead of rescanning the stream for every candidate.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PACKAGE_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from obvfut_portable_v2.passive_runner import (  # noqa: E402
    IST,
    OnlineObvState,
    PassiveV2Runner,
    as_float,
    atomic_write_json,
    clock_epochs_for_day,
    dynamic_points,
    epoch_ist_iso,
    iter_target_stream_normalized_rows,
    json_clean,
    read_json,
    row_from_target_stream_line,
    safe_key,
)


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def parse_float_csv(raw: str | None, default: Iterable[float]) -> list[float]:
    values: list[float] = []
    for part in parse_csv(raw):
        try:
            value = float(part)
        except ValueError:
            continue
        if math.isfinite(value):
            values.append(value)
    return values or list(default)


def date_range(start: str, end: str, *, skip_weekends: bool = True) -> list[str]:
    current = date.fromisoformat(start)
    final = date.fromisoformat(end)
    out: list[str] = []
    while current <= final:
        if not skip_weekends or current.weekday() < 5:
            out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def stage_timer(report: dict[str, Any], stage: str):
    class _Timer:
        def __enter__(self) -> "_Timer":
            self.started = time.perf_counter()
            return self

        def __exit__(self, *_exc: object) -> None:
            report.setdefault("stage_timings", {})[stage] = round(time.perf_counter() - self.started, 4)

    return _Timer()


def maybe_load_guard(max_load1: float | None, sleep_seconds: float, every_n_lines: int, lines_seen: int) -> int:
    if not max_load1 or every_n_lines <= 0 or lines_seen % every_n_lines:
        return 0
    try:
        load1 = os.getloadavg()[0]
    except Exception:
        return 0
    if load1 < max_load1:
        return 0
    time.sleep(max(0.0, sleep_seconds))
    return 1


def target_stream_candidates(config: dict[str, Any], trade_date: str) -> list[Path]:
    filename = f"target_quotes_{trade_date}.jsonl"
    candidates: list[Path] = []
    state_dir = Path(str(config.get("state_dir") or ""))
    if state_dir:
        candidates.append(state_dir / "target_stream" / trade_date / filename)
    configured_root = Path(str(config.get("target_stream_root") or ""))
    if configured_root:
        candidates.append(configured_root / trade_date / filename)
    local_root = Path(str(config.get("target_stream_root_local") or ""))
    if local_root:
        candidates.append(local_root / trade_date / filename)
    out: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def target_stream_path(config: dict[str, Any], trade_date: str) -> Path:
    candidates = target_stream_candidates(config, trade_date)
    existing = [path for path in candidates if path.exists()]
    if not existing:
        return candidates[0]
    return max(existing, key=lambda path: path.stat().st_size)


def prepare_runner(config_path: Path, output_dir: Path) -> PassiveV2Runner:
    cfg = read_json(config_path, {})
    tmp_state = output_dir / "_runner_state"
    tmp_state.mkdir(parents=True, exist_ok=True)
    cfg["state_dir"] = str(tmp_state)
    cfg["state_dir_local"] = str(tmp_state)
    cfg["bootstrap_load_enabled"] = False
    cfg["skip_past_due_clocks_on_start"] = False
    cfg["second_row_retention_seconds"] = 0
    cfg["flat_second_row_retention_seconds"] = 0
    cfg["pending_second_row_retention_seconds"] = 0
    cfg["active_second_row_retention_seconds"] = 0
    tmp_config = output_dir / "_runtime_recalibration_tmp.json"
    atomic_write_json(tmp_config, cfg)
    return PassiveV2Runner(tmp_config)


def selected_instruments(runner: PassiveV2Runner, symbols: list[str], max_symbols: int | None) -> list[Any]:
    if symbols:
        missing = [symbol for symbol in symbols if symbol not in runner.instruments]
        if missing:
            raise SystemExit(f"Unknown symbols in v2 universe: {', '.join(missing)}")
        metas = [runner.instruments[symbol] for symbol in symbols]
    else:
        metas = list(runner.instruments.values())
    if max_symbols is not None:
        metas = metas[: max(0, int(max_symbols))]
    return metas


def validate_sources(config: dict[str, Any], dates: list[str], sample_bytes: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for trade_date in dates:
        path = target_stream_path(config, trade_date)
        item: dict[str, Any] = {
            "trade_date": trade_date,
            "path": str(path),
            "candidate_paths": [str(candidate) for candidate in target_stream_candidates(config, trade_date)],
            "exists": path.exists(),
            "is_symlink": path.is_symlink(),
            "resolved_path": str(path.resolve()) if path.exists() else None,
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "sample_rows": 0,
            "sample_missing_fields": {},
            "first_epoch": None,
            "last_sample_epoch": None,
        }
        if path.exists() and sample_bytes > 0:
            missing_counts: dict[str, int] = defaultdict(int)
            start = time.perf_counter()
            bytes_read = 0
            with path.open("rb") as handle:
                while bytes_read < sample_bytes:
                    line = handle.readline()
                    if not line:
                        break
                    bytes_read += len(line)
                    if not line.strip():
                        continue
                    row = row_from_target_stream_line(line, trade_date, None)
                    if row is None:
                        try:
                            raw_row = json.loads(line)
                        except json.JSONDecodeError:
                            raw_row = {}
                        for field in ("key", "exchange_epoch", "event_epoch", "price", "last_price", "volume_traded", "bid", "ask"):
                            if raw_row.get(field) is None:
                                missing_counts[field] += 1
                        continue
                    item["sample_rows"] += 1
                    epoch = as_float(row.get("epoch"))
                    if item["first_epoch"] is None:
                        item["first_epoch"] = epoch
                    item["last_sample_epoch"] = epoch
                    for field in ("target", "epoch", "price", "volume_traded", "bid", "ask"):
                        if row.get(field) is None:
                            missing_counts[field] += 1
            item["sample_bytes"] = bytes_read
            item["sample_duration_seconds"] = round(time.perf_counter() - start, 4)
            item["sample_missing_fields"] = dict(sorted(missing_counts.items()))
        out.append(item)
    return out


def point_config_with_fresh_multiplier(point_config: dict[str, Any], multiplier: float) -> dict[str, Any]:
    cfg = copy.deepcopy(point_config or {})
    payload = cfg.get("point_thresholds") if isinstance(cfg.get("point_thresholds"), dict) else cfg
    fresh = dict(payload.get("fresh_breakout") or {})
    fresh["multiplier"] = float(multiplier)
    payload["fresh_breakout"] = fresh
    return cfg


def clock_label(epoch: int) -> str:
    return epoch_ist_iso(epoch)[11:16] if epoch_ist_iso(epoch) else str(epoch)


def row_signal_quote_fresh(row: dict[str, Any], max_age_seconds: float) -> tuple[bool, float | None]:
    age = as_float(row.get("source_quote_age_seconds"))
    if age is None or not math.isfinite(age):
        return False, age
    return age <= float(max_age_seconds), age


def build_candidate_entries(
    *,
    metas: list[Any],
    clock_rows_by_symbol: dict[str, list[dict[str, Any]]],
    primary_short_thresholds: list[float],
    fresh_breakout_multipliers: list[float],
    long_strength_pcts: list[float],
    short_weakness_pcts: list[float],
    max_signal_quote_age_seconds: float,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    clock_panel_samples: list[dict[str, Any]] = []
    for meta in metas:
        rows = [dict(row) for row in clock_rows_by_symbol.get(meta.symbol, [])]
        if len(clock_panel_samples) < 20:
            clock_panel_samples.extend(rows[: max(0, 20 - len(clock_panel_samples))])
        rows.sort(key=lambda item: int(item.get("epoch_second") or 0))
        module_counts: dict[str, int] = defaultdict(int)
        stale_edge_counts: dict[str, int] = defaultdict(int)
        stale_clock_rows = 0
        previous_active: dict[tuple[str, str], bool] = defaultdict(bool)
        for row in rows:
            signal_quote_fresh, signal_quote_age = row_signal_quote_fresh(row, max_signal_quote_age_seconds)
            if not signal_quote_fresh:
                stale_clock_rows += 1
            enough = bool(row.get("signal_enough_history"))
            metric = as_float(row.get("obv_minus_price_prior_z"))
            prior_p90 = as_float(row.get("prior_p90"))
            price = as_float(row.get("price"))
            price_pct = as_float(row.get("price_change_prior_pct"))
            lookback_high = as_float(row.get("prior_lookback_high"))
            lookback_low = as_float(row.get("prior_lookback_low"))
            prior_clock_vol = as_float(row.get("prior_clock_vol_points"))
            bearish_absent = bool(row.get("fresh_long_bearish_absent_pass"))
            bullish_absent = bool(row.get("fresh_short_bullish_absent_pass"))
            primary_execution_confirm = bool(row.get("primary_short_execution_confirm_pass"))

            for threshold in primary_short_thresholds:
                active = (
                    enough
                    and metric is not None
                    and prior_p90 is not None
                    and metric >= prior_p90
                    and metric >= threshold
                    and primary_execution_confirm
                )
                key = ("primary_obv_short", f"abs{threshold:g}")
                edge = active and not previous_active[key]
                previous_active[key] = bool(active)
                if edge:
                    module = f"primary_obv_short_abs{threshold:g}"
                    if not signal_quote_fresh:
                        stale_edge_counts[module] += 1
                        continue
                    module_counts[module] += 1
                    candidates.append(
                        {
                            "symbol": meta.symbol,
                            "module": module,
                            "side": "short",
                            "variant": f"primary_abs_{threshold:g}",
                            "signal_epoch": int(row["epoch_second"]),
                            "signal_time": row.get("actual_time"),
                            "signal_price": price,
                            "metric": metric,
                            "prior_p90": prior_p90,
                            "price_change_prior_pct": price_pct,
                            "signal_source": meta.signal_source,
                            "signal_key": meta.signal_key,
                            "execution_key": meta.execution_key,
                            "signal_quote_age_seconds": signal_quote_age,
                        }
                    )

            for multiplier in fresh_breakout_multipliers:
                cfg = point_config_with_fresh_multiplier(meta.signal_point_config, multiplier)
                breakout = dynamic_points(
                    prior_clock_vol if prior_clock_vol is not None else math.nan,
                    price=price if price is not None else math.nan,
                    kind="fresh_breakout",
                    point_config=cfg,
                    legacy_multiplier=1.4,
                    legacy_floor=30.0,
                    legacy_cap=120.0,
                    legacy_fallback=50.0,
                )
                long_trigger = lookback_high + breakout if lookback_high is not None and math.isfinite(breakout) else None
                short_trigger = lookback_low - breakout if lookback_low is not None and math.isfinite(breakout) else None
                for long_strength in long_strength_pcts:
                    active = (
                        enough
                        and bearish_absent
                        and price is not None
                        and price_pct is not None
                        and price_pct >= long_strength
                        and long_trigger is not None
                        and price >= long_trigger
                    )
                    key = ("fresh_trend_long", f"m{multiplier:g}_p{long_strength:g}")
                    edge = active and not previous_active[key]
                    previous_active[key] = bool(active)
                    if edge:
                        module = "fresh_trend_long"
                        variant = f"fresh_m{multiplier:g}_longp{long_strength:g}"
                        if not signal_quote_fresh:
                            stale_edge_counts[f"{module}:{variant}"] += 1
                            continue
                        module_counts[f"{module}:{variant}"] += 1
                        candidates.append(
                            {
                                "symbol": meta.symbol,
                                "module": module,
                                "side": "long",
                                "variant": variant,
                                "signal_epoch": int(row["epoch_second"]),
                                "signal_time": row.get("actual_time"),
                                "signal_price": price,
                                "price_change_prior_pct": price_pct,
                                "trigger_price": long_trigger,
                                "fresh_breakout_points": breakout,
                                "signal_source": meta.signal_source,
                                "signal_key": meta.signal_key,
                                "execution_key": meta.execution_key,
                                "signal_quote_age_seconds": signal_quote_age,
                            }
                        )
                for short_weakness in short_weakness_pcts:
                    active = (
                        enough
                        and bullish_absent
                        and price is not None
                        and price_pct is not None
                        and price_pct <= short_weakness
                        and short_trigger is not None
                        and price <= short_trigger
                    )
                    key = ("fresh_trend_short", f"m{multiplier:g}_p{short_weakness:g}")
                    edge = active and not previous_active[key]
                    previous_active[key] = bool(active)
                    if edge:
                        module = "fresh_trend_short"
                        variant = f"fresh_m{multiplier:g}_shortp{short_weakness:g}"
                        if not signal_quote_fresh:
                            stale_edge_counts[f"{module}:{variant}"] += 1
                            continue
                        module_counts[f"{module}:{variant}"] += 1
                        candidates.append(
                            {
                                "symbol": meta.symbol,
                                "module": module,
                                "side": "short",
                                "variant": variant,
                                "signal_epoch": int(row["epoch_second"]),
                                "signal_time": row.get("actual_time"),
                                "signal_price": price,
                                "price_change_prior_pct": price_pct,
                                "trigger_price": short_trigger,
                                "fresh_breakout_points": breakout,
                                "signal_source": meta.signal_source,
                                "signal_key": meta.signal_key,
                                "execution_key": meta.execution_key,
                                "signal_quote_age_seconds": signal_quote_age,
                            }
                        )
        summary[meta.symbol] = {
            "clock_rows": len(rows),
            "stale_signal_clock_rows": stale_clock_rows,
            "candidate_entries": sum(module_counts.values()),
            "module_counts": dict(sorted(module_counts.items())),
            "stale_edge_counts": dict(sorted(stale_edge_counts.items())),
            "signal_source": meta.signal_source,
            "signal_key": meta.signal_key,
            "execution_key": meta.execution_key,
            "threshold_source": meta.source,
            "threshold_synthesized": meta.synthesized,
        }
    candidates.sort(key=lambda item: (int(item.get("signal_epoch") or 0), str(item.get("symbol") or ""), str(item.get("variant") or "")))
    return candidates, summary, clock_panel_samples


def emit_due_clock_rows(
    *,
    metas: list[Any],
    states: dict[str, OnlineObvState],
    trade_date: str,
    clock_epochs: list[int],
    checkpoint_index: int,
    max_seen_epoch: int,
    decision_delay_seconds: int,
    clock_rows_by_symbol: dict[str, list[dict[str, Any]]],
) -> int:
    while checkpoint_index < len(clock_epochs):
        clock_epoch = int(clock_epochs[checkpoint_index])
        checkpoint_epoch = clock_epoch + int(decision_delay_seconds)
        if checkpoint_epoch > int(max_seen_epoch):
            break
        for state in states.values():
            state.finalize_until(checkpoint_epoch)
        label = clock_label(clock_epoch)
        for meta in metas:
            state = states.get(meta.signal_key)
            if state is None:
                continue
            row, _reason = state.build_clock_row(clock_epoch, label, meta.signal_point_config)
            if row is None:
                continue
            clock_rows_by_symbol[meta.symbol].append(
                {
                    **row,
                    "symbol": meta.symbol,
                    "signal_source": meta.signal_source,
                    "signal_key": meta.signal_key,
                    "execution_key": meta.execution_key,
                    "decision_checkpoint_epoch": checkpoint_epoch,
                    "decision_checkpoint_time": epoch_ist_iso(checkpoint_epoch),
                }
            )
        checkpoint_index += 1
    return checkpoint_index


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "schema": "obvfutport_v2.t1_entry_fast_recalibration_smoke.v1",
        "mode": args.mode,
        "started_at_ist": epoch_ist_iso(time.time()),
        "config": str(args.config),
        "output_dir": str(output_dir),
    }
    config = read_json(Path(args.config), {})
    dates = date_range(args.start_date, args.end_date, skip_weekends=not args.no_skip_weekends)
    report["dates"] = dates
    with stage_timer(report, "source_validation"):
        report["sources"] = validate_sources(config, dates, int(args.source_sample_bytes))
    if args.mode == "preflight":
        report["completed_at_ist"] = epoch_ist_iso(time.time())
        atomic_write_json(output_dir / "preflight_report.json", report)
        print(json.dumps(json_clean(report), indent=2, sort_keys=True))
        return report

    with stage_timer(report, "instrument_load"):
        runner = prepare_runner(Path(args.config), output_dir)
        metas = selected_instruments(runner, parse_csv(args.symbols), args.max_symbols)
    target_keys = sorted({meta.signal_key for meta in metas} | {meta.execution_key for meta in metas})
    clock_epochs_by_date = {
        trade_date: clock_epochs_for_day(
            date.fromisoformat(trade_date),
            clock_start=str(config.get("clock_start_ist") or "09:20"),
            clock_end=str(config.get("clock_end_ist") or "15:20"),
            clock_step_minutes=int(config.get("clock_step_minutes") or 15),
        )
        for trade_date in dates
    }
    all_clock_epochs = {epoch for epochs in clock_epochs_by_date.values() for epoch in epochs}
    states = {
        key: OnlineObvState(
            key=key,
            clock_epochs=set(all_clock_epochs),
            second_row_retention_seconds=0,
            compute_non_clock_percentiles=False,
        )
        for key in target_keys
    }
    scan_stats: dict[str, Any] = {}
    clock_rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    decision_delay_seconds = int(config.get("decision_delay_seconds") or 25)
    load_guard_sleeps = 0
    with stage_timer(report, "single_pass_stream_scan"):
        for trade_date in dates:
            path = target_stream_path(config, trade_date)
            date_started = time.perf_counter()
            rows_used = 0
            max_seen_epoch = 0
            checkpoint_index = 0
            day_clock_epochs = list(clock_epochs_by_date.get(trade_date) or [])
            bytes_budget = args.max_bytes_per_day
            if not path.exists():
                scan_stats[trade_date] = {"source_found": False, "path": str(path)}
                continue
            start_size = path.stat().st_size
            for row in iter_target_stream_normalized_rows(
                path,
                trade_date,
                target_keys,
                max_bytes=bytes_budget,
            ):
                key = str(row.get("target") or "")
                state = states.get(key)
                if state is None:
                    continue
                state.process_row(row)
                row_epoch = int(row.get("epoch_second") or row.get("epoch") or 0)
                if row_epoch > 0:
                    max_seen_epoch = max(max_seen_epoch, row_epoch)
                    checkpoint_index = emit_due_clock_rows(
                        metas=metas,
                        states=states,
                        trade_date=trade_date,
                        clock_epochs=day_clock_epochs,
                        checkpoint_index=checkpoint_index,
                        max_seen_epoch=max_seen_epoch,
                        decision_delay_seconds=decision_delay_seconds,
                        clock_rows_by_symbol=clock_rows_by_symbol,
                    )
                rows_used += 1
                load_guard_sleeps += maybe_load_guard(
                    args.max_load1,
                    float(args.load_guard_sleep_seconds),
                    int(args.load_guard_every_lines),
                    rows_used,
                )
            for state in states.values():
                state.flush_until_latest()
            if max_seen_epoch > 0:
                checkpoint_index = emit_due_clock_rows(
                    metas=metas,
                    states=states,
                    trade_date=trade_date,
                    clock_epochs=day_clock_epochs,
                    checkpoint_index=checkpoint_index,
                    max_seen_epoch=max_seen_epoch,
                    decision_delay_seconds=decision_delay_seconds,
                    clock_rows_by_symbol=clock_rows_by_symbol,
                )
            scan_stats[trade_date] = {
                "source_found": True,
                "path": str(path),
                "size_bytes": start_size,
                "max_bytes_per_day": bytes_budget,
                "bytes_scanned_estimate": min(start_size, int(bytes_budget)) if bytes_budget is not None else start_size,
                "target_rows_used": rows_used,
                "clock_rows_emitted": sum(
                    1
                    for rows in clock_rows_by_symbol.values()
                    for row in rows
                    if str(row.get("trade_date")) == trade_date
                ),
                "decision_checkpoints_emitted": checkpoint_index,
                "duration_seconds": round(time.perf_counter() - date_started, 4),
            }
    report["scan_stats"] = scan_stats
    report["load_guard_sleeps"] = load_guard_sleeps
    report["symbols"] = [meta.symbol for meta in metas]
    report["symbol_count"] = len(metas)
    report["target_keys"] = target_keys
    report["target_key_count"] = len(target_keys)

    primary_short_thresholds = parse_float_csv(args.primary_short_thresholds, [1.5, 1.75, 2.0])
    fresh_breakout_multipliers = parse_float_csv(args.fresh_breakout_multipliers, [1.4])
    long_strength_pcts = parse_float_csv(args.long_strength_pcts, [95.0])
    short_weakness_pcts = parse_float_csv(args.short_weakness_pcts, [1.0])
    entry_variant_grid = sorted(
        {
            *(f"primary_abs_{threshold:g}" for threshold in primary_short_thresholds),
            *(
                f"fresh_m{multiplier:g}_longp{long_strength:g}"
                for multiplier in fresh_breakout_multipliers
                for long_strength in long_strength_pcts
            ),
            *(
                f"fresh_m{multiplier:g}_shortp{short_weakness:g}"
                for multiplier in fresh_breakout_multipliers
                for short_weakness in short_weakness_pcts
            ),
        }
    )
    report["entry_variant_grid"] = entry_variant_grid
    report["entry_variant_grid_by_symbol"] = {meta.symbol: entry_variant_grid for meta in metas}
    max_signal_quote_age_seconds = (
        as_float(args.signal_quote_max_age_seconds)
        or as_float(config.get("signal_quote_max_age_seconds"))
        or 45.0
    )
    report["max_signal_quote_age_seconds"] = max_signal_quote_age_seconds
    with stage_timer(report, "candidate_sweep_from_cached_clock_rows"):
        candidates, summary, clock_samples = build_candidate_entries(
            metas=metas,
            clock_rows_by_symbol=clock_rows_by_symbol,
            primary_short_thresholds=primary_short_thresholds,
            fresh_breakout_multipliers=fresh_breakout_multipliers,
            long_strength_pcts=long_strength_pcts,
            short_weakness_pcts=short_weakness_pcts,
            max_signal_quote_age_seconds=float(max_signal_quote_age_seconds),
        )
    report["candidate_summary"] = summary
    report["candidate_count"] = len(candidates)
    report["candidate_samples"] = candidates[:50]
    report["clock_panel_samples"] = clock_samples[:20]
    total_bytes = sum(int(item.get("bytes_scanned_estimate") or 0) for item in scan_stats.values() if item.get("source_found"))
    scan_seconds = float(report.get("stage_timings", {}).get("single_pass_stream_scan") or 0.0)
    if scan_seconds > 0 and total_bytes > 0:
        bytes_per_second = total_bytes / scan_seconds
        report["throughput_estimate"] = {
            "source_bytes_considered": total_bytes,
            "scan_seconds": scan_seconds,
            "bytes_per_second": round(bytes_per_second, 2),
            "gb_per_minute": round(bytes_per_second * 60 / (1024**3), 3),
        }
    if args.write_panels:
        with (output_dir / "entry_candidates.jsonl").open("w", encoding="utf-8") as handle:
            for item in candidates:
                handle.write(json.dumps(json_clean(item), sort_keys=True) + "\n")
        atomic_write_json(
            output_dir / "entry_variant_support.json",
            {
                "schema": "obvfutport_v2.entry_variant_support.v1",
                "symbols": {meta.symbol: entry_variant_grid for meta in metas},
                "entry_variant_grid": entry_variant_grid,
                "generated_at_ist": epoch_ist_iso(time.time()),
            },
        )
        with (output_dir / "clock_panel_samples.jsonl").open("w", encoding="utf-8") as handle:
            for item in clock_samples:
                handle.write(json.dumps(json_clean(item), sort_keys=True) + "\n")
    report["completed_at_ist"] = epoch_ist_iso(time.time())
    atomic_write_json(output_dir / "recalibration_smoke_report.json", report)
    print(json.dumps(json_clean(report), indent=2, sort_keys=True))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast OBVFUTPORT-v2 T1 entry recalibration precompute/smoke")
    parser.add_argument("--config", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--mode", choices=["preflight", "smoke"], default="smoke")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--primary-short-thresholds", default="1.5,1.75,2.0")
    parser.add_argument("--fresh-breakout-multipliers", default="1,1.2,1.4,1.6")
    parser.add_argument("--long-strength-pcts", default="90,95")
    parser.add_argument("--short-weakness-pcts", default="1,5,10")
    parser.add_argument("--source-sample-bytes", type=int, default=2_000_000)
    parser.add_argument("--signal-quote-max-age-seconds", type=float, default=None)
    parser.add_argument("--max-bytes-per-day", type=int, default=None)
    parser.add_argument("--max-load1", type=float, default=None)
    parser.add_argument("--load-guard-sleep-seconds", type=float, default=0.5)
    parser.add_argument("--load-guard-every-lines", type=int, default=50_000)
    parser.add_argument("--write-panels", action="store_true")
    parser.add_argument("--no-skip-weekends", action="store_true")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
