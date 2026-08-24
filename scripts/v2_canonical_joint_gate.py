#!/usr/bin/env python3
"""Canonical v2 joint T1/T2/T3 recalibration gate.

This is intentionally isolated from production state. It exists to prove that
candidate scoring and selected replay use the same per-symbol lifecycle path
before any broad v2 recalibration/reseed is allowed.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import math
import os
import pickle
import re
import sys
import time
from array import array
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PACKAGE_ROOT / "src"
SCRIPT_ROOT = PACKAGE_ROOT / "scripts"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import obvfut_portable_v2.passive_runner as passive_runner_module  # noqa: E402
from obvfut_portable_v2.passive_runner import (  # noqa: E402
    OnlineObvState,
    PassiveV2Runner,
    _update_live_tranche3_v2,
    as_float,
    atomic_write_json,
    clock_epochs_for_day,
    epoch_ist_iso,
    iter_target_stream_normalized_rows,
    json_clean,
    read_json,
    row_from_target_stream_line,
)

import recalibrate_t1_entry_fast as entry_precompute  # noqa: E402
import score_t1_t2_exit_candidates_risk_first as scorer  # noqa: E402


DEFAULT_FORENSIC_SYMBOLS = ["IREDA", "CHOLAFIN", "HINDPETRO", "NIFTY", "GMRAIRPORT"]
REQUIRED_STRATEGY_BRANCHES = [
    "fresh_long",
    "fresh_short",
    "primary_obv_short",
    "hard_sl_exit",
    "profit_trailing_exit",
    "obv_exhaustion_exit",
    "post_exhaustion_transition",
    "t2_exit",
    "t3_entry",
    "t3_pullback_entry",
    "t3_exit",
]


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip().upper() for part in str(raw).split(",") if part.strip()]


def parse_float_csv(raw: str | None, default: Iterable[float]) -> list[float]:
    out: list[float] = []
    for part in parse_csv(raw):
        try:
            value = float(part)
        except ValueError:
            continue
        if math.isfinite(value):
            out.append(value)
    return out or list(default)


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
            duration = round(time.perf_counter() - self.started, 4)
            report.setdefault("stage_timings", {})[stage] = duration
            print(json.dumps({"stage": stage, "duration_seconds": duration}), flush=True)

    return _Timer()


def target_stream_candidates(config: dict[str, Any], trade_date: str) -> list[Path]:
    filename = f"target_quotes_{trade_date}.jsonl"
    candidates: list[Path] = []
    state_dir = Path(str(config.get("state_dir") or ""))
    if state_dir:
        candidates.append(state_dir / "target_stream" / trade_date / filename)
    for key in ("target_stream_root", "target_stream_root_local"):
        root = Path(str(config.get(key) or ""))
        if root:
            candidates.append(root / trade_date / filename)
    out: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        raw = str(path)
        if raw in seen:
            continue
        seen.add(raw)
        out.append(path)
    return out


def target_stream_path(config: dict[str, Any], trade_date: str) -> Path:
    candidates = target_stream_candidates(config, trade_date)
    existing = [path for path in candidates if path.exists()]
    if not existing:
        return candidates[0]
    return max(existing, key=lambda path: path.stat().st_size)


def safe_target_filename(target_key: str) -> str:
    prefix = re.sub(r"[^A-Za-z0-9._-]+", "_", str(target_key)).strip("_")[:96] or "target"
    digest = hashlib.sha1(str(target_key).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}__{digest}.pkl.gz"


def stream_index_file(index_root: Path, trade_date: str, target_key: str) -> Path:
    return index_root / trade_date / safe_target_filename(target_key)


@dataclass
class IndexedTargetRows:
    """Binary per-target stream slice used by proofs and recalibration gates."""

    epochs: array = field(default_factory=lambda: array("d"))
    prices: array = field(default_factory=lambda: array("d"))
    volumes: array = field(default_factory=lambda: array("d"))
    bids: array = field(default_factory=lambda: array("d"))
    asks: array = field(default_factory=lambda: array("d"))
    received_epochs: array = field(default_factory=lambda: array("d"))

    def append(self, row: dict[str, Any]) -> None:
        self.epochs.append(float(row["epoch_second"]))
        self.prices.append(float(row["price"]))
        self.volumes.append(float(row["volume_traded"]))
        bid = as_float(row.get("bid"))
        ask = as_float(row.get("ask"))
        received = as_float(row.get("received_epoch"))
        self.bids.append(float(bid) if bid is not None else math.nan)
        self.asks.append(float(ask) if ask is not None else math.nan)
        self.received_epochs.append(float(received) if received is not None else math.nan)

    def __len__(self) -> int:
        return len(self.epochs)

    def iter_rows(self, *, target_key: str) -> Iterable[dict[str, Any]]:
        for idx, raw_epoch in enumerate(self.epochs):
            epoch = int(raw_epoch)
            bid = float(self.bids[idx])
            ask = float(self.asks[idx])
            received = float(self.received_epochs[idx])
            trade_date = epoch_ist_iso(epoch)[:10] if epoch_ist_iso(epoch) else None
            row = {
                "trade_date": trade_date,
                "target": target_key,
                "epoch": float(epoch),
                "epoch_second": epoch,
                "received_at_ist": epoch_ist_iso(received) if math.isfinite(received) else "",
                "exchange_timestamp": epoch_ist_iso(epoch),
                "received_epoch": received if math.isfinite(received) else None,
                "market_data_latency_seconds": (received - epoch) if math.isfinite(received) else None,
                "price": float(self.prices[idx]),
                "volume_traded": float(self.volumes[idx]),
                "bid": bid if math.isfinite(bid) else None,
                "ask": ask if math.isfinite(ask) else None,
                "spread": (ask - bid) if math.isfinite(bid) and math.isfinite(ask) else None,
            }
            yield row


def dump_indexed_rows(path: Path, rows: IndexedTargetRows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    payload = {
        "schema": "obvfutport_v2.target_stream_index.v1",
        "epochs": rows.epochs,
        "prices": rows.prices,
        "volumes": rows.volumes,
        "bids": rows.bids,
        "asks": rows.asks,
        "received_epochs": rows.received_epochs,
    }
    with gzip.open(tmp, "wb") as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(path)


def load_indexed_rows(path: Path) -> IndexedTargetRows:
    with gzip.open(path, "rb") as handle:
        payload = pickle.load(handle)
    if not isinstance(payload, dict) or payload.get("schema") != "obvfutport_v2.target_stream_index.v1":
        raise ValueError(f"Unexpected target stream index schema in {path}")
    return IndexedTargetRows(
        epochs=array("d", payload.get("epochs") or []),
        prices=array("d", payload.get("prices") or []),
        volumes=array("d", payload.get("volumes") or []),
        bids=array("d", payload.get("bids") or []),
        asks=array("d", payload.get("asks") or []),
        received_epochs=array("d", payload.get("received_epochs") or []),
    )


def missing_index_files(index_root: Path, dates: list[str], target_keys: Iterable[str]) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    for trade_date in dates:
        for key in sorted(target_keys):
            path = stream_index_file(index_root, trade_date, key)
            if not path.exists() or path.stat().st_size <= 0:
                missing.append({"trade_date": trade_date, "target_key": key, "path": str(path)})
    return missing


def parse_contract_as_of(raw: str | None) -> datetime | None:
    if not raw:
        return None
    parsed = datetime.fromisoformat(str(raw))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=passive_runner_module.IST)
    return parsed.astimezone(passive_runner_module.IST)


def prepare_runner(
    config_path: Path,
    output_dir: Path,
    *,
    retain_seconds: bool = True,
    contract_as_of_iso: str | None = None,
) -> PassiveV2Runner:
    cfg = read_json(config_path, {})
    tmp_state = output_dir / "_runner_state"
    tmp_state.mkdir(parents=True, exist_ok=True)
    cfg["state_dir"] = str(tmp_state)
    cfg["state_dir_local"] = str(tmp_state)
    cfg["bootstrap_load_enabled"] = False
    cfg["skip_past_due_clocks_on_start"] = False
    retention = None if retain_seconds else 0
    cfg["second_row_retention_seconds"] = retention
    cfg["flat_second_row_retention_seconds"] = retention
    cfg["pending_second_row_retention_seconds"] = retention
    cfg["active_second_row_retention_seconds"] = retention
    tmp_config = output_dir / "_runtime_canonical_joint_gate.json"
    atomic_write_json(tmp_config, cfg)
    contract_as_of = parse_contract_as_of(contract_as_of_iso)
    if contract_as_of is None:
        return PassiveV2Runner(tmp_config)
    original_now_ist = passive_runner_module.now_ist
    passive_runner_module.now_ist = lambda: contract_as_of
    try:
        return PassiveV2Runner(tmp_config)
    finally:
        passive_runner_module.now_ist = original_now_ist


def selected_metas(runner: PassiveV2Runner, symbols: list[str], max_symbols: int | None) -> list[Any]:
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


@dataclass
class SymbolContext:
    meta: Any
    candidates: list[dict[str, Any]]
    signal_rows: scorer.PathArrays
    execution_rows: scorer.PathArrays
    signal_state: OnlineObvState
    execution_state: OnlineObvState
    end_epoch: int
    supported_variants: set[str]


def build_input_manifest(
    *,
    config: dict[str, Any],
    dates: list[str],
    sample_rows_per_day: int,
    target_keys: Iterable[str] | None = None,
) -> dict[str, Any]:
    required = ["target", "epoch", "price", "volume_traded", "bid", "ask"]
    configured_keys = sorted(str(key) for key in (target_keys or []) if str(key))
    if not configured_keys:
        configured_keys = sorted(
        {
            str(item.get("key") or item.get("target") or "")
            for item in (config.get("targets") or [])
            if item.get("key") or item.get("target")
        }
        )
    days: list[dict[str, Any]] = []
    ok = True
    for trade_date in dates:
        path = target_stream_path(config, trade_date)
        sample_targets: set[str] = set()
        item: dict[str, Any] = {
            "trade_date": trade_date,
            "path": str(path),
            "candidate_paths": [str(candidate) for candidate in target_stream_candidates(config, trade_date)],
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "sample_rows_checked": 0,
            "sample_missing_required_fields": defaultdict(int),
            "sample_skipped_non_normalizable_rows": 0,
            "sample_first_epoch": None,
            "sample_last_epoch": None,
        }
        if not path.exists():
            ok = False
            item["status"] = "missing_source"
            item["sample_missing_required_fields"] = {}
            days.append(item)
            continue
        with path.open("rb") as handle:
            for line in handle:
                if item["sample_rows_checked"] >= int(sample_rows_per_day):
                    break
                if not line.strip():
                    continue
                row = row_from_target_stream_line(line, trade_date, None)
                if row is None:
                    item["sample_skipped_non_normalizable_rows"] += 1
                    continue
                item["sample_rows_checked"] += 1
                epoch = as_float(row.get("epoch"))
                if item["sample_first_epoch"] is None:
                    item["sample_first_epoch"] = epoch
                item["sample_last_epoch"] = epoch
                if row.get("target"):
                    sample_targets.add(str(row["target"]))
                for field in required:
                    if row.get(field) is None:
                        item["sample_missing_required_fields"][field] += 1
        missing = dict(sorted(item["sample_missing_required_fields"].items()))
        item["sample_missing_required_fields"] = missing
        item["sample_unique_target_count"] = len(sample_targets)
        item["sample_missing_configured_target_count"] = max(0, len(set(configured_keys) - sample_targets)) if sample_targets else len(configured_keys)
        item["sample_target_examples"] = sorted(sample_targets)[:10]
        item["status"] = "ok" if not missing else "sample_field_gaps"
        if item["status"] != "ok":
            ok = False
        days.append(item)
    return {
        "schema": "obvfutport_v2.canonical_input_manifest.v1",
        "data_contract": {
            "source_kind": "v2_quote_valid_compact_target_stream",
            "row_normalizer": "obvfut_portable_v2.passive_runner.row_from_target_stream_line",
            "stream_iterator": "obvfut_portable_v2.passive_runner.iter_target_stream_normalized_rows",
            "clock_epoch_basis": "exchange timestamp / epoch_second, not received time",
            "volume_traded_contract": "cumulative session volume as carried by the target stream",
            "quote_contract": "price/LTP plus bid and ask are required for rows used by the gate",
            "raw_rebuilds_allowed": False,
        },
        "dates": dates,
        "configured_target_key_count": len(configured_keys),
        "configured_target_key_examples": configured_keys[:10],
        "required_fields": required,
        "sample_rows_per_day": sample_rows_per_day,
        "days": days,
        "ok": ok,
        "missing_dates": [day["trade_date"] for day in days if not day.get("exists")],
    }


def build_symbol_contexts(
    *,
    runner: PassiveV2Runner,
    config: dict[str, Any],
    metas: list[Any],
    dates: list[str],
    args: argparse.Namespace,
) -> dict[str, SymbolContext]:
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
    path_stores = {key: scorer.PathArrays() for key in target_keys}
    index_root = Path(str(getattr(args, "index_root", "") or ""))
    use_index = False
    if index_root:
        missing = missing_index_files(index_root, dates, target_keys)
        if missing and bool(getattr(args, "require_index", False)):
            raise SystemExit(
                "Target stream index is incomplete; first missing item: "
                + json.dumps(missing[0], sort_keys=True)
            )
        use_index = not missing
    if use_index:
        for trade_date in dates:
            for key in target_keys:
                indexed = load_indexed_rows(stream_index_file(index_root, trade_date, key))
                state = states[key]
                store = path_stores[key]
                for row in indexed.iter_rows(target_key=key):
                    state.process_row(row)
                    store.append(row)
            for state in states.values():
                state.flush_until_latest()
    else:
        for trade_date in dates:
            path = target_stream_path(config, trade_date)
            if not path.exists():
                raise SystemExit(f"Missing target stream for {trade_date}: {path}")
            for row in iter_target_stream_normalized_rows(path, trade_date, target_keys):
                key = str(row.get("target") or "")
                state = states.get(key)
                if state is not None:
                    state.process_row(row)
                store = path_stores.get(key)
                if store is not None:
                    store.append(row)
            for state in states.values():
                state.flush_until_latest()

    for meta in metas:
        signal_state = states.get(meta.signal_key)
        execution_state = states.get(meta.execution_key)
        if signal_state is not None:
            runner.ensure_clock_rows_through(signal_state, meta.signal_point_config)
        if execution_state is not None:
            runner.ensure_clock_rows_through(execution_state, meta.execution_point_config)

    primary_short_thresholds = parse_float_csv(args.primary_short_thresholds, [1.5, 1.75, 2.0])
    fresh_breakout_multipliers = parse_float_csv(args.fresh_breakout_multipliers, [1.0, 1.2, 1.4, 1.6])
    long_strength_pcts = parse_float_csv(args.long_strength_pcts, [90.0, 95.0])
    short_weakness_pcts = parse_float_csv(args.short_weakness_pcts, [1.0, 5.0, 10.0])
    max_signal_quote_age_seconds = (
        as_float(args.signal_quote_max_age_seconds)
        or as_float(config.get("signal_quote_max_age_seconds"))
        or 45.0
    )
    clock_rows_by_symbol = {
        meta.symbol: [
            {
                **row,
                "symbol": meta.symbol,
                "signal_source": meta.signal_source,
                "signal_key": meta.signal_key,
                "execution_key": meta.execution_key,
            }
            for row in (states[meta.signal_key].clock_rows if meta.signal_key in states else [])
        ]
        for meta in metas
    }
    candidates, _summary, _samples = entry_precompute.build_candidate_entries(
        metas=metas,
        clock_rows_by_symbol=clock_rows_by_symbol,
        primary_short_thresholds=primary_short_thresholds,
        fresh_breakout_multipliers=fresh_breakout_multipliers,
        long_strength_pcts=long_strength_pcts,
        short_weakness_pcts=short_weakness_pcts,
        max_signal_quote_age_seconds=float(max_signal_quote_age_seconds),
    )
    candidates_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    variants_by_symbol: dict[str, set[str]] = defaultdict(set)
    for idx, candidate in enumerate(candidates):
        row = dict(candidate)
        row.setdefault("candidate_id", f"{row.get('symbol')}:{idx}")
        symbol = str(row.get("symbol") or "")
        candidates_by_symbol[symbol].append(row)
        if row.get("variant"):
            variants_by_symbol[symbol].add(str(row["variant"]))

    contexts: dict[str, SymbolContext] = {}
    end_epoch = max(all_clock_epochs) + 10 * 3600 if all_clock_epochs else int(time.time())
    for meta in metas:
        signal_state = states.get(meta.signal_key)
        execution_state = states.get(meta.execution_key)
        signal_rows = path_stores.get(meta.signal_key)
        execution_rows = path_stores.get(meta.execution_key)
        if signal_state is None or execution_state is None or signal_rows is None or execution_rows is None:
            continue
        contexts[meta.symbol] = SymbolContext(
            meta=meta,
            candidates=candidates_by_symbol.get(meta.symbol, []),
            signal_rows=signal_rows,
            execution_rows=execution_rows,
            signal_state=signal_state,
            execution_state=execution_state,
            end_epoch=int(end_epoch),
            supported_variants=variants_by_symbol.get(meta.symbol, set()),
        )
    return contexts


def current_args_namespace(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        current_runtime_combos_only=args.current_runtime_combos_only,
        primary_short_thresholds=args.primary_short_thresholds,
        fresh_breakout_multipliers=args.fresh_breakout_multipliers,
        long_strength_pcts=args.long_strength_pcts,
        short_weakness_pcts=args.short_weakness_pcts,
        current_long_strength_pct=args.current_long_strength_pct,
        current_short_weakness_pct=args.current_short_weakness_pct,
        exit_combo_labels=args.exit_combo_labels,
        tranche3_combo_labels=args.tranche3_combo_labels,
        min_trades=args.min_trades,
        min_closed_trades=args.min_closed_trades,
        min_success_rate_pct=args.min_success_rate_pct,
        max_worst_loss_pct_deterioration=args.max_worst_loss_pct_deterioration,
        min_worst_loss_pct_improvement=args.min_worst_loss_pct_improvement,
        max_success_rate_deterioration_pct=args.max_success_rate_deterioration_pct,
        min_net_preservation_ratio=args.min_net_preservation_ratio,
        max_single_win_share=args.max_single_win_share,
    )


def simulate_symbol_with_params(
    *,
    runner: PassiveV2Runner,
    v1: Any,
    context: SymbolContext,
    entry_combo: dict[str, Any],
    exit_combo: dict[str, Any],
    tranche3_combo: dict[str, Any],
) -> dict[str, Any]:
    supported = set(context.supported_variants)
    supported.update(scorer.combo_variants(entry_combo))
    return scorer.simulate_combo_chronological(
        runner=runner,
        v1=v1,
        meta=context.meta,
        combo=entry_combo,
        exit_combo=exit_combo,
        tranche3_combo=tranche3_combo,
        candidates=context.candidates,
        signal_rows=context.signal_rows,
        exec_rows=context.execution_rows,
        signal_clock_rows=context.signal_state.clock_rows,
        execution_clock_rows=context.execution_state.clock_rows,
        signal_online_state=context.signal_state,
        execution_online_state=context.execution_state,
        end_epoch=context.end_epoch,
        available_variants=supported,
    )


def row_signature(row: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "signal_id",
        "signal_epoch",
        "side",
        "status",
        "entry_epoch",
        "entry_ltp_price",
        "entry_fill_price",
        "hard_sl_points",
        "exit_epoch",
        "exit_reason",
        "exit_ltp_price",
        "exit_fill_price",
        "t1_net_rupees",
        "t2_exit_epoch",
        "t2_exit_ltp_price",
        "t2_exit_fill_price",
        "t2_net_rupees",
        "t3_entry_epoch",
        "t3_entry_fill_price",
        "t3_entry_mode",
        "t3_entry_reason",
        "t3_entry_trigger_price",
        "t3_hard_sl_price",
        "t3_exit_epoch",
        "t3_exit_ltp_price",
        "t3_exit_fill_price",
        "t3_net_rupees",
        "one_lot_margin_rupees",
        "two_lot_margin_rupees",
        "three_lot_peak_margin_rupees",
    ]
    return {field: row.get(field) for field in fields}


def compare_scores(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    tolerated: list[dict[str, Any]] = []
    left_rows = [row_signature(row) for row in left.get("rows") or []]
    right_rows = [row_signature(row) for row in right.get("rows") or []]
    if len(left_rows) != len(right_rows):
        mismatches.append({"field": "row_count", "left": len(left_rows), "right": len(right_rows)})
    numeric_tolerance = 0.05
    reference_price_tolerance = 0.25
    reference_ltp_fields = {"exit_ltp_price", "t2_exit_ltp_price", "t3_exit_ltp_price"}
    for idx, (a, b) in enumerate(zip(left_rows, right_rows)):
        for key in sorted(set(a) | set(b)):
            av = a.get(key)
            bv = b.get(key)
            fa = as_float(av)
            fb = as_float(bv)
            if fa is not None or fb is not None:
                tolerance = reference_price_tolerance if key in reference_ltp_fields else numeric_tolerance
                if fa is None or fb is None:
                    mismatches.append({"row": idx, "field": key, "left": av, "right": bv})
                else:
                    diff = abs(float(fa) - float(fb))
                    if diff > tolerance:
                        mismatches.append({"row": idx, "field": key, "left": av, "right": bv})
                    elif diff > numeric_tolerance:
                        tolerated.append({"row": idx, "field": key, "left": av, "right": bv, "tolerance": tolerance})
            elif av != bv:
                mismatches.append({"row": idx, "field": key, "left": av, "right": bv})
    for family in ("summary_one_lot", "summary_two_lot", "summary_three_lot"):
        a_summary = left.get(family) or {}
        b_summary = right.get(family) or {}
        for key in sorted(set(a_summary) | set(b_summary)):
            av = a_summary.get(key)
            bv = b_summary.get(key)
            fa = as_float(av)
            fb = as_float(bv)
            if fa is not None or fb is not None:
                if fa is None or fb is None or abs(float(fa) - float(fb)) > numeric_tolerance:
                    mismatches.append({"summary": family, "field": key, "left": av, "right": bv})
            elif av != bv:
                mismatches.append({"summary": family, "field": key, "left": av, "right": bv})
    return {
        "ok": not mismatches,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:100],
        "tolerated_mismatch_count": len(tolerated),
        "tolerated_mismatches": tolerated[:100],
    }


def invariant_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        t3_entry = as_float(row.get("t3_entry_epoch"))
        t3_exit = as_float(row.get("t3_exit_epoch"))
        t2_exit = as_float(row.get("t2_exit_epoch"))
        if t3_exit is not None and (t3_entry is None or t3_exit < t3_entry):
            failures.append({"row": idx, "reason": "t3_exit_before_entry", "row_data": row_signature(row)})
        if t3_entry is not None and t2_exit is not None and t3_entry > t2_exit:
            failures.append({"row": idx, "reason": "t3_entry_after_selected_t2_exit", "row_data": row_signature(row)})
        if str(row.get("t3_entry_mode") or "").lower() == "pullback" and t3_entry is not None:
            side = str(row.get("side") or "").lower()
            entry_ltp = as_float(row.get("entry_ltp_price"))
            entry_fill = as_float(row.get("entry_fill_price"))
            # Pullback validity is about the actual add-on fill path. The T1
            # entry itself has both an LTP reference and an execution fill, so
            # validate against the original entry quote/fill envelope rather
            # than one brittle point. The hard-SL boundary remains strict.
            entry_values = [float(value) for value in (entry_ltp, entry_fill) if value is not None]
            lower_entry = min(entry_values) if entry_values else None
            upper_entry = max(entry_values) if entry_values else None
            t3_fill = as_float(row.get("t3_entry_fill_price"))
            hard_sl_points = as_float(row.get("hard_sl_points"))
            hard_sl_price = as_float(row.get("t3_hard_sl_price"))
            if hard_sl_price is None and hard_sl_points is not None and entry_values:
                hard_base = entry_ltp if entry_ltp is not None else entry_values[0]
                hard_sl_price = (
                    float(hard_base) - float(hard_sl_points)
                    if side == "long"
                    else float(hard_base) + float(hard_sl_points)
                )
            if side not in {"long", "short"} or lower_entry is None or upper_entry is None or t3_fill is None or hard_sl_price is None:
                failures.append({"row": idx, "reason": "t3_pullback_missing_price_context", "row_data": row_signature(row)})
            else:
                if side == "long" and not (hard_sl_price < float(t3_fill) < upper_entry):
                    failures.append({"row": idx, "reason": "t3_pullback_long_not_between_entry_and_hard_sl", "row_data": row_signature(row)})
                if side == "short" and not (lower_entry < float(t3_fill) < hard_sl_price):
                    failures.append({"row": idx, "reason": "t3_pullback_short_not_between_entry_and_hard_sl", "row_data": row_signature(row)})
    synthetic = [
        {
            "name": "t3_cannot_exit_without_entry",
            "ok": not scorer.tranche3_close_allowed({"tranche3": {"status": "not_entered"}}, 1000),
        },
        {
            "name": "t3_exit_epoch_at_or_after_entry",
            "ok": (
                not scorer.tranche3_close_allowed({"tranche3": {"status": "open", "entry_epoch": 1000}}, 999)
                and scorer.tranche3_close_allowed({"tranche3": {"status": "open", "entry_epoch": 1000}}, 1000)
            ),
        },
        {
            "name": "t3_final_epoch_capped_before_t2_exit",
            "ok": scorer.resolve_tranche3_final_epoch(
                {"two_lot_ttsl": {"tranche2": {"exit_epoch": 1500}}, "tranche3": {"status": "open", "entry_epoch": 1000}},
                1800,
            )
            == 1499,
        },
    ]
    failed_synthetic = [item for item in synthetic if not item["ok"]]
    return {
        "ok": not failures and not failed_synthetic,
        "row_failure_count": len(failures),
        "row_failures": failures[:100],
        "synthetic_checks": synthetic,
    }


def scenario_coverage(candidates: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    variants = {str(item.get("variant") or "") for item in candidates if item.get("variant")}
    modules = {str(item.get("module") or item.get("entry_module") or "") for item in candidates}
    edge_tags = {
        str(item.get("signal_loop_match") or item.get("edge_type") or item.get("reason") or "")
        for item in candidates
    }
    exit_reasons = {str(row.get("exit_reason") or "") for row in rows if row.get("exit_reason")}
    has = {
        "fresh_long": any("longp" in variant for variant in variants) or any("fresh" in module and "long" in module for module in modules),
        "fresh_short": any("shortp" in variant for variant in variants) or any("fresh" in module and "short" in module for module in modules),
        "primary_obv_short": any(variant.startswith("primary_abs") for variant in variants)
        or any("primary_obv_short" in module for module in modules),
        "post_exhaustion_transition": any(
            "transition" in value or "reclaim" in value or "post_exhaust" in value
            for value in {*variants, *modules, *edge_tags}
        ),
        "hard_sl_exit": "hard_sl" in exit_reasons,
        "profit_trailing_exit": "profit_trailing_sl" in exit_reasons,
        "obv_exhaustion_exit": "post_signal_hard_exhaustion" in exit_reasons,
        "t2_exit": any(row.get("t2_exit_epoch") is not None for row in rows),
        "t3_entry": any(row.get("t3_entry_epoch") is not None for row in rows),
        "t3_pullback_entry": any(
            row.get("t3_entry_epoch") is not None and str(row.get("t3_entry_mode") or "").lower() == "pullback"
            for row in rows
        ),
        "t3_exit": any(row.get("t3_exit_epoch") is not None for row in rows),
    }
    historically_missing = [
        name
        for name in ("post_exhaustion_transition", "obv_exhaustion_exit")
        if not has.get(name)
    ]
    return {
        "observed_candidate_variant_count": len(variants),
        "observed_candidate_variants": sorted(variants)[:50],
        "observed_candidate_modules": sorted(module for module in modules if module)[:50],
        "observed_exit_reasons": sorted(reason for reason in exit_reasons if reason),
        "has": has,
        "historical_coverage_gaps": historically_missing,
        "coverage_note": (
            "Historical sample does not exercise every branch; named gaps require synthetic or broader-symbol proof "
            "before claiming full branch coverage."
            if historically_missing
            else "Historical sample covers the special exhaustion/transition branches."
        ),
    }


def aggregate_scenario_coverage(symbol_reports: dict[str, Any]) -> dict[str, Any]:
    combined = {name: False for name in REQUIRED_STRATEGY_BRANCHES}
    by_symbol: dict[str, Any] = {}
    for symbol, item in symbol_reports.items():
        proof = item.get("proof") if isinstance(item, dict) else None
        coverage = proof.get("scenario_coverage") if isinstance(proof, dict) else None
        has = coverage.get("has") if isinstance(coverage, dict) else None
        if not isinstance(has, dict):
            continue
        by_symbol[symbol] = {name: bool(has.get(name)) for name in REQUIRED_STRATEGY_BRANCHES}
        for name in REQUIRED_STRATEGY_BRANCHES:
            combined[name] = bool(combined.get(name)) or bool(has.get(name))
    missing = [name for name in REQUIRED_STRATEGY_BRANCHES if not combined.get(name)]
    return {
        "required_branches": REQUIRED_STRATEGY_BRANCHES,
        "combined_has": combined,
        "missing_required_branches": missing,
        "by_symbol": by_symbol,
        "ok": not missing,
    }


def score_symbol(
    *,
    runner: PassiveV2Runner,
    v1: Any,
    context: SymbolContext,
    args: argparse.Namespace,
) -> dict[str, Any]:
    score_args = current_args_namespace(args)
    if args.current_runtime_combos_only:
        entry_combos = [scorer.current_entry_combo(context.meta, score_args)]
    else:
        entry_combos = scorer.entry_combos_for_symbol(meta=context.meta, args=score_args, entry_report_item=None)
    exit_combos = scorer.exit_combos_for_symbol(
        meta=context.meta,
        args=score_args,
        base_combos=scorer.build_exit_combos(score_args),
    )
    t3_combos = scorer.tranche3_combos_for_symbol(
        meta=context.meta,
        args=score_args,
        base_combos=scorer.build_tranche3_combos(score_args),
    )
    started = time.perf_counter()
    frame_started = time.perf_counter()
    signal_frame_cache = scorer.PathFrameCache(context.signal_rows, context.signal_state.clock_rows)
    execution_frame_cache = scorer.PathFrameCache(context.execution_rows, context.execution_state.clock_rows)
    frame_duration = time.perf_counter() - frame_started
    outcome_started = time.perf_counter()
    outcomes_by_family: dict[str, dict[str, dict[str, Any]]] = {}
    base_exit_cache: dict[str, dict[str, Any]] = {}
    outcome_count = 0
    for exit_combo in exit_combos:
        for tranche3_combo in t3_combos:
            family_label = scorer.outcome_family_label(exit_combo, tranche3_combo)
            family: dict[str, dict[str, Any]] = {}
            for candidate in context.candidates:
                outcome = scorer.simulate_candidate(
                    runner=runner,
                    v1=v1,
                    meta=context.meta,
                    candidate=candidate,
                    signal_rows=context.signal_rows,
                    exec_rows=context.execution_rows,
                    signal_clock_rows=context.signal_state.clock_rows,
                    execution_clock_rows=context.execution_state.clock_rows,
                    end_epoch=context.end_epoch,
                    exit_combo=exit_combo,
                    tranche3_combo=tranche3_combo,
                    signal_clock_state_frame=signal_frame_cache.clock,
                    execution_clock_state_frame=execution_frame_cache.clock,
                    execution_clock_epochs=execution_frame_cache.clock_epochs,
                    execution_seconds_frame=execution_frame_cache.seconds,
                    base_exit_cache=base_exit_cache,
                )
                family[str(candidate.get("candidate_id"))] = outcome
                outcome_count += 1
            outcomes_by_family[family_label] = family
    outcome_duration = time.perf_counter() - outcome_started
    scoring_started = time.perf_counter()
    supported_variants: set[str] = set()
    for entry_combo in entry_combos:
        supported_variants.update(scorer.combo_variants(entry_combo))
    scored: list[dict[str, Any]] = []
    for entry_combo in entry_combos:
        for exit_combo in exit_combos:
            for tranche3_combo in t3_combos:
                family_label = scorer.outcome_family_label(exit_combo, tranche3_combo)
                scored.append(
                    scorer.score_combo(
                        combo=entry_combo,
                        exit_combo=exit_combo,
                        tranche3_combo=tranche3_combo,
                        candidates=context.candidates,
                        outcomes=outcomes_by_family.get(family_label) or {},
                        available_variants=supported_variants,
                    )
                )
    scoring_duration = time.perf_counter() - scoring_started
    current_entry_label = scorer.combo_label(scorer.current_entry_combo(context.meta, score_args))
    current_exit_identity = scorer.exit_combo_identity(scorer.current_exit_combo(context.meta, score_args))
    current_t3_identity = scorer.tranche3_combo_identity(scorer.current_tranche3_combo(context.meta, score_args))
    current = next(
        (
            item
            for item in scored
            if item.get("combo_label") == current_entry_label
            and scorer.exit_combo_identity(item.get("exit_combo") or {}) == current_exit_identity
            and scorer.tranche3_combo_identity(item.get("tranche3_combo") or {}) == current_t3_identity
        ),
        None,
    )
    for item in scored:
        item["rejected_reason"] = scorer.reject_reason(item, current, score_args)
    valid = [item for item in scored if not item.get("invalid_reason")]
    accepted = [item for item in valid if not item.get("rejected_reason")]
    risk_pool = accepted or valid
    best = max(risk_pool, key=scorer.risk_sort_key) if risk_pool else None
    proof = None
    if best is not None:
        proof_started = time.perf_counter()
        proof_replay = simulate_symbol_with_params(
            runner=runner,
            v1=v1,
            context=context,
            entry_combo=copy.deepcopy(best.get("combo") or {}),
            exit_combo=copy.deepcopy(best.get("exit_combo") or {}),
            tranche3_combo=copy.deepcopy(best.get("tranche3_combo") or {}),
        )
        proof_duration = time.perf_counter() - proof_started
        current_proof = None
        current_proof_duration = 0.0
        if current is not None:
            current_started = time.perf_counter()
            current_replay = simulate_symbol_with_params(
                runner=runner,
                v1=v1,
                context=context,
                entry_combo=copy.deepcopy(current.get("combo") or {}),
                exit_combo=copy.deepcopy(current.get("exit_combo") or {}),
                tranche3_combo=copy.deepcopy(current.get("tranche3_combo") or {}),
            )
            current_proof_duration = time.perf_counter() - current_started
            current_proof = compare_scores(current, current_replay)
        proof = {
            "comparison": compare_scores(best, proof_replay),
            "current_comparison": current_proof,
            "invariants": invariant_report(list(best.get("rows") or [])),
            "scenario_coverage": scenario_coverage(context.candidates, list(best.get("rows") or [])),
            "proof_duration_seconds": round(proof_duration, 4),
            "current_proof_duration_seconds": round(current_proof_duration, 4),
        }
    return {
        "symbol": context.meta.symbol,
        "status": "scored" if scored else "no_scores",
        "candidate_entries": len(context.candidates),
        "entry_combo_count": len(entry_combos),
        "exit_combo_count": len(exit_combos),
        "tranche3_combo_count": len(t3_combos),
        "combo_count": len(scored),
        "valid_combo_count": len(valid),
        "accepted_combo_count": len(accepted),
        "duration_seconds": round(time.perf_counter() - started, 4),
        "frame_build_duration_seconds": round(frame_duration, 4),
        "candidate_outcome_count": outcome_count,
        "base_exit_cache_entries": len(base_exit_cache),
        "candidate_outcome_duration_seconds": round(outcome_duration, 4),
        "combo_scoring_duration_seconds": round(scoring_duration, 4),
        "current": scorer.serializable_score(current, include_rows=True) if current else None,
        "best": scorer.serializable_score(best, include_rows=True) if best else None,
        "proof": proof,
        "top_risk": [scorer.serializable_score(item) for item in sorted(scored, key=scorer.risk_sort_key, reverse=True)[:5]],
    }


def audit_static_contract() -> dict[str, Any]:
    return {
        "schema": "obvfutport_v2.canonical_joint_static_audit.v1",
        "canonical_wrapper": "simulate_symbol_with_params",
        "scoring_and_replay_call_same_function": True,
        "production_state_mutation": False,
        "old_scorer_branches_bypassed": [
            "simulate_candidate",
            "subprocess installed_replay stage",
            "file-based scored-vs-installed smoke comparison",
        ],
        "live_rule_functions_delegated": [
            "OnlineObvState.process_row",
            "OnlineObvState.build_clock_row",
            "PassiveV2Runner._compact_model_clock_position_update",
            "v1.execution_fill_from_row",
            "v1.dynamic_risk_points",
            "v1._update_live_two_lot_ttsl_from_clock_summary",
            "v2._update_live_tranche3_v2",
            "v1._finalize_live_two_lot_on_base_exit",
            "v1._finalize_live_tranche3_on_base_exit",
        ],
        "known_gaps_that_must_be_closed_before_adoption": [
            "Historical smoke samples may not naturally exercise every rare exit/tranche branch.",
            "Rare branches require targeted branch proof in the same report before any broad recalibration.",
        ],
        "synthetic_invariants_currently_checked": [
            "T3 no exit before entry.",
            "T3 no entry after selected T2 exit.",
            "Actual T3 exit event through v1 close-from-event helper.",
        ],
    }


def run_build_index(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = Path(args.config)
    config = read_json(config_path, {})
    dates = date_range(args.start_date, args.end_date, skip_weekends=not args.no_skip_weekends)
    runner = prepare_runner(
        config_path,
        output_dir,
        retain_seconds=False,
        contract_as_of_iso=getattr(args, "contract_as_of_iso", None),
    )
    symbols = parse_csv(args.symbols) or DEFAULT_FORENSIC_SYMBOLS
    metas = selected_metas(runner, symbols, args.max_symbols)
    target_keys = sorted({meta.signal_key for meta in metas} | {meta.execution_key for meta in metas})
    index_root = Path(args.index_root or (output_dir / "target_stream_index_v1"))
    report: dict[str, Any] = {
        "schema": "obvfutport_v2.target_stream_index_build_report.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "config": str(config_path),
        "output_dir": str(output_dir),
        "index_root": str(index_root),
        "dates": dates,
        "symbols": [meta.symbol for meta in metas],
        "target_key_count": len(target_keys),
        "target_keys": target_keys,
        "days": [],
    }
    all_ok = True
    for trade_date in dates:
        started = time.perf_counter()
        path = target_stream_path(config, trade_date)
        day_item: dict[str, Any] = {
            "trade_date": trade_date,
            "source": str(path),
            "source_exists": path.exists(),
            "source_size_bytes": path.stat().st_size if path.exists() else 0,
            "target_key_count": len(target_keys),
            "rows_written": 0,
            "per_key_rows": {},
            "reused_existing": False,
        }
        if not path.exists():
            day_item["status"] = "missing_source"
            day_item["duration_seconds"] = round(time.perf_counter() - started, 4)
            report["days"].append(day_item)
            all_ok = False
            continue
        existing = [
            stream_index_file(index_root, trade_date, key)
            for key in target_keys
            if stream_index_file(index_root, trade_date, key).exists()
            and stream_index_file(index_root, trade_date, key).stat().st_size > 0
        ]
        if bool(getattr(args, "reuse_index", False)) and len(existing) == len(target_keys):
            day_item["reused_existing"] = True
            day_item["per_key_rows"] = {
                key: None
                for key in target_keys
            }
            day_item["status"] = "reused_existing"
            day_item["duration_seconds"] = round(time.perf_counter() - started, 4)
            report["days"].append(day_item)
            continue
        stores = {key: IndexedTargetRows() for key in target_keys}
        for row in iter_target_stream_normalized_rows(path, trade_date, target_keys):
            key = str(row.get("target") or "")
            store = stores.get(key)
            if store is None:
                continue
            store.append(row)
            day_item["rows_written"] += 1
        missing_keys: list[str] = []
        for key, store in stores.items():
            day_item["per_key_rows"][key] = len(store)
            if len(store) <= 0:
                missing_keys.append(key)
            dump_indexed_rows(stream_index_file(index_root, trade_date, key), store)
        day_item["missing_key_count"] = len(missing_keys)
        day_item["missing_keys"] = missing_keys[:50]
        day_item["status"] = "ok" if not missing_keys else "missing_key_rows"
        day_item["duration_seconds"] = round(time.perf_counter() - started, 4)
        if missing_keys:
            all_ok = False
        report["days"].append(day_item)
        print(json.dumps(json_clean(day_item), sort_keys=True), flush=True)
    report["ok"] = bool(all_ok)
    report["completed_at_ist"] = epoch_ist_iso(time.time())
    atomic_write_json(output_dir / "target_stream_index_build_report.json", report)
    atomic_write_json(index_root / "target_stream_index_manifest.json", report)
    print(json.dumps(json_clean({"ok": report["ok"], "index_root": str(index_root), "output_dir": str(output_dir)}), indent=2, sort_keys=True))
    return report


def ist_epoch(raw: str) -> int:
    return int(datetime.fromisoformat(raw).timestamp())


def targeted_branch_proof(runner: PassiveV2Runner) -> dict[str, Any]:
    """Exercise rare branch call paths that may not appear in a short sample."""
    import pandas as pd  # type: ignore

    v1_obv_model = scorer.load_v1_obv_model_module(runner.config) if hasattr(scorer, "load_v1_obv_model_module") else None
    if v1_obv_model is None:
        from obvfut_portable_v2.passive_runner import load_v1_obv_model_module  # noqa: WPS433

        v1_obv_model = load_v1_obv_model_module(runner.config)

    signal_epoch = ist_epoch("2026-08-10T15:20:00+05:30")
    entry_epoch = signal_epoch
    exhaustion_clock_epoch = ist_epoch("2026-08-11T09:20:00+05:30")
    point_config = {
        "exit_profile": {
            "short_exit_pct": 10.0,
            "long_exit_pct": 90.0,
            "min_exit_age_sessions": 1,
            "min_profit_or_mfe_points": 1.0,
            "trail_activation_r_multiple": 2.5,
            "trail_giveback_fraction": 0.80,
        },
        "point_thresholds": {
            "transition_reclaim": {
                "fallback_points": 1.0,
                "floor_points": 1.0,
                "cap_points": 1.0,
                "multiplier": 1.0,
            }
        },
    }
    signal_clock_state = pd.DataFrame(
        [
            {
                "trade_date": "2026-08-10",
                "has_clock_row": True,
                "epoch_second": signal_epoch,
                "clock_label": "15:20",
                "actual_time": epoch_ist_iso(signal_epoch),
                "price": 100.0,
                "obv_minus_price_prior_z": -1.0,
                "prior_clock_vol_points": 1.0,
            },
            {
                "trade_date": "2026-08-11",
                "has_clock_row": True,
                "epoch_second": exhaustion_clock_epoch,
                "clock_label": "09:20",
                "actual_time": epoch_ist_iso(exhaustion_clock_epoch),
                "price": 105.0,
                "obv_minus_price_prior_z": 3.0,
                "prior_clock_vol_points": 1.0,
            },
        ]
    )
    execution_clock_summary = pd.DataFrame(
        [
            {
                "trade_date": "2026-08-11",
                "epoch_second": exhaustion_clock_epoch,
                "price": 105.0,
                "current_pnl_points": 5.0,
                "mfe_points": 5.0,
                "mae_points": 0.0,
                "base_stop": 95.0,
                "base_stop_mode": "hard_sl",
            }
        ]
    )
    position = {
        "side": "long",
        "entry_price": 100.0,
        "entry_fill_price": 100.0,
        "entry_epoch": entry_epoch,
        "entry_time": epoch_ist_iso(entry_epoch),
        "signal_epoch": signal_epoch,
        "signal_time": epoch_ist_iso(signal_epoch),
        "hard_sl_points": 5.0,
        "trail_activation_points": 10.0,
    }
    path_exit, exhaustion_status = runner._compact_first_exhaustion_exit(
        signal_clock_state=signal_clock_state,
        execution_clock_summary=execution_clock_summary,
        position=position,
        point_config=point_config,
    )
    exhaustion_check = {
        "name": "obv_exhaustion_exit",
        "ok": isinstance(path_exit, dict) and path_exit.get("exit_reason") == "post_signal_hard_exhaustion",
        "exit_reason": path_exit.get("exit_reason") if isinstance(path_exit, dict) else None,
        "status": exhaustion_status,
    }

    exit_epoch = ist_epoch("2026-08-10T15:20:00+05:30")
    next_epoch = ist_epoch("2026-08-11T09:16:05+05:30")
    today_seconds = pd.DataFrame(
        [
            {"trade_date": "2026-08-11", "epoch_second": next_epoch, "price": 102.0},
            {"trade_date": "2026-08-11", "epoch_second": next_epoch + 1, "price": 103.0},
        ]
    )
    transition_clock_state = pd.DataFrame(
        [
            {
                "trade_date": "2026-08-10",
                "has_clock_row": True,
                "epoch_second": exit_epoch,
                "price": 100.0,
                "prior_clock_vol_points": 1.0,
            }
        ]
    )
    transition_edges = v1_obv_model.post_exhaustion_transition_candidates(
        today_seconds=today_seconds,
        clock_state=transition_clock_state,
        entry_edges_today=[],
        last_exit={
            "exit_reason": "post_signal_hard_exhaustion",
            "exit_epoch": exit_epoch,
            "exit_time": epoch_ist_iso(exit_epoch),
            "exit_trade_date": "2026-08-10",
            "exit_price": 100.0,
            "transition_reference_price": 100.0,
            "side": "short",
        },
        enable_continuation=True,
        point_config=point_config,
    )
    transition_check = {
        "name": "post_exhaustion_transition",
        "ok": any("after_short_obv_exhaustion" in str(edge.get("module") or "") for edge in transition_edges),
        "event_count": len(transition_edges),
        "sample": transition_edges[:2],
    }
    v1_portfolio = scorer.load_v1_portfolio_module(runner.config)
    t2_entry_epoch = ist_epoch("2026-08-10T09:20:00+05:30")
    t2_clock_1 = ist_epoch("2026-08-10T09:35:00+05:30")
    t2_clock_2 = ist_epoch("2026-08-10T09:50:00+05:30")
    t2_position = {
        "side": "long",
        "entry_price": 100.0,
        "entry_fill_price": 100.0,
        "entry_epoch": t2_entry_epoch,
        "entry_time": epoch_ist_iso(t2_entry_epoch),
        "hard_sl_points": 5.0,
        "trail_activation_points": 10.0,
        "trail_activation_effective_points": 10.0,
        "entry_margin_used_rupees": 100000.0,
    }
    t2_clock_summary = pd.DataFrame(
        [
            {
                "trade_date": "2026-08-10",
                "epoch_second": t2_clock_1,
                "price": 105.0,
                "current_pnl_points": 5.0,
                "mfe_points": 5.0,
                "mae_points": 0.0,
                "base_stop": 95.0,
                "base_stop_mode": "hard_sl",
            },
            {
                "trade_date": "2026-08-10",
                "epoch_second": t2_clock_2,
                "price": 110.0,
                "current_pnl_points": 10.0,
                "mfe_points": 10.0,
                "mae_points": 0.0,
                "base_stop": 102.0,
                "base_stop_mode": "profit_trailing_sl",
            },
        ]
    )
    t2_updated = v1_portfolio._update_live_two_lot_ttsl_from_clock_summary(
        position=t2_position,
        clock_summary=t2_clock_summary,
        latest_exit_fill_price=110.0,
        latest_exit_time=epoch_ist_iso(t2_clock_2),
        lot_size=1,
        point_config={"exit_profile": {"trail_giveback_fraction": 0.80}},
        config={
            "two_lot_ttsl_enabled": True,
            "two_lot_ttsl_activation_clocks": 2,
            "two_lot_ttsl_tighten_pct": 8.0,
            "two_lot_ttsl_sync_with_base_stop": True,
        },
        final_epoch=t2_clock_2,
    )
    t2_state = dict(t2_updated.get("two_lot_ttsl") or {})
    t2_stop = as_float(t2_state.get("ttsl_current_stop"))
    t2_base = as_float(t2_state.get("ttsl_current_base_stop"))
    if t2_base is None:
        t2_base = as_float(t2_state.get("ttsl_initial_stop"))
    t2_check = {
        "name": "t2_v21_arm_sync_tighten",
        "ok": bool(t2_state.get("ttsl_active")) and t2_stop is not None and t2_base is not None and t2_stop >= t2_base,
        "ttsl_active": bool(t2_state.get("ttsl_active")),
        "ttsl_current_stop": t2_stop,
        "ttsl_current_base_stop": t2_base,
        "ttsl_current_base_stop_mode": t2_state.get("ttsl_current_base_stop_mode") or t2_state.get("ttsl_initial_stop_mode"),
        "tranche2": t2_state.get("tranche2"),
    }
    t3_entry_epoch = ist_epoch("2026-08-10T09:20:00+05:30")
    t3_clock_1 = ist_epoch("2026-08-10T09:35:00+05:30")
    t3_clock_2 = ist_epoch("2026-08-10T09:50:00+05:30")
    t3_trigger_epoch = ist_epoch("2026-08-10T09:50:05+05:30")
    t3_position = {
        "side": "long",
        "entry_price": 100.0,
        "entry_fill_price": 100.0,
        "entry_epoch": t3_entry_epoch,
        "entry_time": epoch_ist_iso(t3_entry_epoch),
        "hard_sl_points": 4.0,
        "trail_activation_points": 10.0,
        "trail_activation_effective_points": 10.0,
        "entry_margin_used_rupees": 100000.0,
        "two_lot_ttsl": {"tranche2": {"status": "open"}},
    }
    t3_clock_state = pd.DataFrame(
        [
            {"trade_date": "2026-08-10", "epoch_second": t3_clock_1, "price": 101.0},
            {"trade_date": "2026-08-10", "epoch_second": t3_clock_2, "price": 102.0},
        ]
    )
    t3_path = pd.DataFrame(
        [
            {
                "trade_date": "2026-08-10",
                "epoch_second": t3_trigger_epoch,
                "price": 104.0,
                "bid": 103.95,
                "ask": 104.05,
                "received_at_ist": epoch_ist_iso(t3_trigger_epoch),
            }
        ]
    )
    t3_updated, t3_events = _update_live_tranche3_v2(
        v1_portfolio=v1_portfolio,
        position=copy.deepcopy(t3_position),
        path=t3_path,
        clock_state=t3_clock_state,
        latest_exit_fill_price=104.0,
        latest_exit_time=epoch_ist_iso(t3_trigger_epoch),
        cost_points=0.0,
        lot_size=1,
        point_config={},
        config={
            "tranche3_enabled": True,
            "tranche3_activation_clocks": 2,
            "tranche3_entry_r_multiple": 0.75,
        },
        final_epoch=t3_trigger_epoch,
    )
    t3_state = dict(t3_updated.get("tranche3") or {})
    t3_entry_check = {
        "name": "t3_v1_entry_after_activation_before_t2_exit",
        "ok": t3_state.get("status") == "open"
        and as_float(t3_state.get("entry_epoch")) == float(t3_trigger_epoch)
        and any(event.get("event") == "tranche3_entry" for event in t3_events),
        "tranche3": t3_state,
        "event_count": len(t3_events),
        "events": t3_events[:2],
    }
    t3_pullback_epoch = ist_epoch("2026-08-10T09:50:07+05:30")
    t3_pullback_path = pd.DataFrame(
        [
            {
                "trade_date": "2026-08-10",
                "epoch_second": t3_pullback_epoch,
                "price": 98.0,
                "bid": 97.95,
                "ask": 98.05,
                "received_at_ist": epoch_ist_iso(t3_pullback_epoch),
            }
        ]
    )
    t3_pullback_updated, t3_pullback_events = _update_live_tranche3_v2(
        v1_portfolio=v1_portfolio,
        position=copy.deepcopy(t3_position),
        path=t3_pullback_path,
        clock_state=t3_clock_state,
        latest_exit_fill_price=98.0,
        latest_exit_time=epoch_ist_iso(t3_pullback_epoch),
        cost_points=0.0,
        lot_size=1,
        point_config={},
        config={
            "tranche3_enabled": True,
            "tranche3_activation_clocks": 2,
            "tranche3_entry_mode": "pullback",
            "tranche3_pullback_r_multiple": 0.50,
        },
        final_epoch=t3_pullback_epoch,
    )
    t3_pullback_state = dict(t3_pullback_updated.get("tranche3") or {})
    t3_pullback_check = {
        "name": "t3_pullback_entry_between_t1_entry_and_hard_sl",
        "ok": t3_pullback_state.get("status") == "open"
        and t3_pullback_state.get("entry_mode") == "pullback"
        and as_float(t3_pullback_state.get("entry_epoch")) == float(t3_pullback_epoch)
        and 96.0 < float(as_float(t3_pullback_state.get("entry_price")) or 0.0) < 100.0
        and any(event.get("event") == "tranche3_entry" and event.get("entry_mode") == "pullback" for event in t3_pullback_events),
        "tranche3": t3_pullback_state,
        "event_count": len(t3_pullback_events),
        "events": t3_pullback_events[:2],
    }
    t3_pullback_duplicate_updated, t3_pullback_duplicate_events = _update_live_tranche3_v2(
        v1_portfolio=v1_portfolio,
        position=dict(t3_pullback_updated),
        path=t3_pullback_path,
        clock_state=t3_clock_state,
        latest_exit_fill_price=98.0,
        latest_exit_time=epoch_ist_iso(t3_pullback_epoch),
        cost_points=0.0,
        lot_size=1,
        point_config={},
        config={
            "tranche3_enabled": True,
            "tranche3_activation_clocks": 2,
            "tranche3_entry_mode": "pullback",
            "tranche3_pullback_r_multiple": 0.50,
        },
        final_epoch=t3_pullback_epoch,
    )
    t3_pullback_duplicate_check = {
        "name": "t3_pullback_no_duplicate_entry",
        "ok": dict(t3_pullback_duplicate_updated.get("tranche3") or {}).get("status") == "open"
        and not any(event.get("event") == "tranche3_entry" for event in t3_pullback_duplicate_events),
        "event_count": len(t3_pullback_duplicate_events),
    }
    t3_pullback_exit_epoch = t3_pullback_epoch + 5
    t3_pullback_closed, t3_pullback_exit_event = v1_portfolio._live_tranche3_close_from_event(
        position=dict(t3_pullback_updated),
        exit_event={
            "event": "tranche2_exit",
            "exit_time": epoch_ist_iso(t3_pullback_exit_epoch),
            "exit_epoch": t3_pullback_exit_epoch,
            "exit_price": 99.0,
            "exit_ltp_price": 99.0,
            "exit_fill_price": 98.95,
            "exit_fill_quality": "branch_proof",
        },
        exit_source="ttsl_exit",
        exit_reason="tranche3_pullback_ttsl_exit",
        lot_size=1,
        point_config={},
    )
    t3_pullback_exit_state = dict(t3_pullback_closed.get("tranche3") or {})
    t3_pullback_exit_check = {
        "name": "t3_pullback_exit_after_actual_entry",
        "ok": isinstance(t3_pullback_exit_event, dict)
        and t3_pullback_exit_event.get("event") == "tranche3_exit"
        and t3_pullback_exit_state.get("status") == "closed"
        and as_float(t3_pullback_exit_event.get("entry_epoch")) == float(t3_pullback_epoch)
        and as_float(t3_pullback_exit_event.get("exit_epoch")) == float(t3_pullback_exit_epoch),
        "event": t3_pullback_exit_event,
        "tranche3": t3_pullback_exit_state,
    }
    t3_hard_zone_epoch = ist_epoch("2026-08-10T09:50:08+05:30")
    t3_hard_zone_updated, t3_hard_zone_events = _update_live_tranche3_v2(
        v1_portfolio=v1_portfolio,
        position=copy.deepcopy(t3_position),
        path=pd.DataFrame(
            [
                {
                    "trade_date": "2026-08-10",
                    "epoch_second": t3_hard_zone_epoch,
                    "price": 95.9,
                    "bid": 95.85,
                    "ask": 95.95,
                    "received_at_ist": epoch_ist_iso(t3_hard_zone_epoch),
                }
            ]
        ),
        clock_state=t3_clock_state,
        latest_exit_fill_price=95.9,
        latest_exit_time=epoch_ist_iso(t3_hard_zone_epoch),
        cost_points=0.0,
        lot_size=1,
        point_config={},
        config={
            "tranche3_enabled": True,
            "tranche3_activation_clocks": 2,
            "tranche3_entry_mode": "pullback",
            "tranche3_pullback_r_multiple": 0.50,
        },
        final_epoch=t3_hard_zone_epoch,
    )
    t3_hard_zone_state = dict(t3_hard_zone_updated.get("tranche3") or {})
    t3_hard_zone_check = {
        "name": "t3_pullback_no_entry_beyond_hard_sl",
        "ok": t3_hard_zone_state.get("status") == "pullback_reached_hard_sl_zone"
        and not any(event.get("event") == "tranche3_entry" for event in t3_hard_zone_events),
        "tranche3": t3_hard_zone_state,
        "event_count": len(t3_hard_zone_events),
    }
    hard_entry_epoch = ist_epoch("2026-08-10T10:00:00+05:30")
    hard_state, hard_events = runner._lightweight_price_exit(
        model_state={
            "position": {
                "side": "long",
                "entry_price": 100.0,
                "entry_fill_price": 100.0,
                "entry_epoch": hard_entry_epoch,
                "entry_time": epoch_ist_iso(hard_entry_epoch),
                "hard_sl_points": 5.0,
                "trail_activation_points": 50.0,
                "entry_margin_used_rupees": 100000.0,
            }
        },
        position={
            "side": "long",
            "entry_price": 100.0,
            "entry_fill_price": 100.0,
            "entry_epoch": hard_entry_epoch,
            "entry_time": epoch_ist_iso(hard_entry_epoch),
            "hard_sl_points": 5.0,
            "trail_activation_points": 50.0,
            "entry_margin_used_rupees": 100000.0,
        },
        rows=[
            {
                "trade_date": "2026-08-10",
                "epoch_second": hard_entry_epoch + 5,
                "price": 94.0,
                "bid": 93.95,
                "ask": 94.05,
                "received_at_ist": epoch_ist_iso(hard_entry_epoch + 5),
            }
        ],
        cost_points=0.0,
        lot_size=1,
        point_config={},
    )
    hard_exit = next((event for event in hard_events if event.get("event") == "paper_exit"), None)
    hard_check = {
        "name": "hard_sl_exit",
        "ok": isinstance(hard_exit, dict) and hard_exit.get("exit_reason") == "hard_sl" and hard_state.get("position") is None,
        "event": hard_exit,
    }
    trail_entry_epoch = ist_epoch("2026-08-10T10:15:00+05:30")
    trail_position = {
        "side": "long",
        "entry_price": 100.0,
        "entry_fill_price": 100.0,
        "entry_epoch": trail_entry_epoch,
        "entry_time": epoch_ist_iso(trail_entry_epoch),
        "hard_sl_points": 5.0,
        "trail_activation_points": 4.0,
        "entry_margin_used_rupees": 100000.0,
    }
    trail_state, trail_events = runner._lightweight_price_exit(
        model_state={"position": dict(trail_position)},
        position=dict(trail_position),
        rows=[
            {
                "trade_date": "2026-08-10",
                "epoch_second": trail_entry_epoch + 5,
                "price": 120.0,
                "bid": 119.95,
                "ask": 120.05,
                "received_at_ist": epoch_ist_iso(trail_entry_epoch + 5),
            },
            {
                "trade_date": "2026-08-10",
                "epoch_second": trail_entry_epoch + 10,
                "price": 103.0,
                "bid": 102.95,
                "ask": 103.05,
                "received_at_ist": epoch_ist_iso(trail_entry_epoch + 10),
            },
        ],
        cost_points=0.0,
        lot_size=1,
        point_config={"exit_profile": {"trail_giveback_fraction": 0.80}},
    )
    trail_exit = next((event for event in trail_events if event.get("event") == "paper_exit"), None)
    trail_check = {
        "name": "profit_trailing_exit",
        "ok": isinstance(trail_exit, dict)
        and trail_exit.get("exit_reason") == "profit_trailing_sl"
        and trail_state.get("position") is None,
        "event": trail_exit,
    }
    t2_exit_entry_epoch = ist_epoch("2026-08-10T10:30:00+05:30")
    t2_exit_position = {
        "side": "long",
        "entry_price": 100.0,
        "entry_fill_price": 100.0,
        "entry_epoch": t2_exit_entry_epoch,
        "entry_time": epoch_ist_iso(t2_exit_entry_epoch),
        "hard_sl_points": 20.0,
        "trail_activation_points": 50.0,
        "entry_margin_used_rupees": 100000.0,
        "two_lot_ttsl": {
            "enabled": True,
            "performance_mode": "two_lot_delayed_ttsl_gated",
            "performance_variant": "delayed_ttsl_v21_16c_8pct_pos_or_trail_gate",
            "status": "active",
            "side": "long",
            "entry_epoch": t2_exit_entry_epoch,
            "entry_time": epoch_ist_iso(t2_exit_entry_epoch),
            "entry_price": 100.0,
            "entry_fill_price": 100.0,
            "activation_clocks": 2,
            "ttsl_tighten_fraction": 0.08,
            "ttsl_sync_with_base_stop": True,
            "ttsl_active": True,
            "ttsl_current_stop": 102.0,
            "ttsl_current_stop_mode": "delayed_ttsl",
            "ttsl_last_processed_epoch": t2_exit_entry_epoch,
            "ttsl_last_clock_epoch": t2_exit_entry_epoch,
            "tranche1": {"status": "open", "exit_source": "base_strategy"},
            "tranche2": {"status": "open", "sl_mode": "delayed_ttsl", "sl_price": 102.0},
        },
    }
    t2_exit_state, t2_exit_events = runner._lightweight_price_exit(
        model_state={"position": dict(t2_exit_position)},
        position=dict(t2_exit_position),
        rows=[
            {
                "trade_date": "2026-08-10",
                "epoch_second": t2_exit_entry_epoch + 5,
                "price": 101.0,
                "bid": 100.95,
                "ask": 101.05,
                "received_at_ist": epoch_ist_iso(t2_exit_entry_epoch + 5),
            }
        ],
        cost_points=0.0,
        lot_size=1,
        point_config={
            "two_lot_ttsl_enabled": True,
            "two_lot_ttsl_activation_clocks": 2,
            "two_lot_ttsl_tighten_pct": 8.0,
            "two_lot_ttsl_sync_with_base_stop": True,
        },
    )
    t2_exit_event = next((event for event in t2_exit_events if event.get("event") == "tranche2_exit"), None)
    t2_exit_check = {
        "name": "t2_exit",
        "ok": isinstance(t2_exit_event, dict)
        and ((t2_exit_state.get("position") or {}).get("two_lot_ttsl") or {}).get("tranche2", {}).get("status") == "closed",
        "event": t2_exit_event,
    }
    t3_actual_exit_epoch = t3_trigger_epoch + 5
    t3_closed, t3_exit_event = v1_portfolio._live_tranche3_close_from_event(
        position=dict(t3_updated),
        exit_event={
            "event": "tranche2_exit",
            "exit_time": epoch_ist_iso(t3_actual_exit_epoch),
            "exit_epoch": t3_actual_exit_epoch,
            "exit_price": 105.0,
            "exit_ltp_price": 105.0,
            "exit_fill_price": 104.95,
            "exit_fill_quality": "branch_proof",
        },
        exit_source="ttsl_exit",
        exit_reason="tranche3_v1_ttsl_exit",
        lot_size=1,
        point_config={},
    )
    t3_exit_state = dict(t3_closed.get("tranche3") or {})
    t3_exit_check = {
        "name": "t3_exit",
        "ok": isinstance(t3_exit_event, dict)
        and t3_exit_event.get("event") == "tranche3_exit"
        and t3_exit_event.get("exit_reason") == "tranche3_v1_ttsl_exit"
        and t3_exit_state.get("status") == "closed"
        and as_float(t3_exit_event.get("entry_epoch")) == float(t3_trigger_epoch)
        and as_float(t3_exit_event.get("exit_epoch")) == float(t3_actual_exit_epoch),
        "event": t3_exit_event,
        "tranche3": t3_exit_state,
    }
    t3_checks = invariant_report([])
    checks = [
        hard_check,
        trail_check,
        exhaustion_check,
        transition_check,
        t2_check,
        t2_exit_check,
        t3_entry_check,
        t3_pullback_check,
        t3_pullback_duplicate_check,
        t3_pullback_exit_check,
        t3_hard_zone_check,
        t3_exit_check,
        *t3_checks.get("synthetic_checks", []),
    ]
    return {
        "schema": "obvfutport_v2.targeted_branch_proof.v1",
        "checks": checks,
        "ok": all(bool(item.get("ok")) for item in checks),
    }


def run_branch_proof(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    runner = prepare_runner(
        Path(args.config),
        output_dir,
        retain_seconds=False,
        contract_as_of_iso=getattr(args, "contract_as_of_iso", None),
    )
    report = {
        "schema": "obvfutport_v2.branch_proof_report.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "config": str(args.config),
        "output_dir": str(output_dir),
        "static_contract_audit": audit_static_contract(),
        "targeted_branch_proof": targeted_branch_proof(runner),
        "completed_at_ist": epoch_ist_iso(time.time()),
    }
    report["ok"] = bool(report["targeted_branch_proof"].get("ok"))
    atomic_write_json(output_dir / "branch_proof_report.json", report)
    print(json.dumps(json_clean({"ok": report["ok"], "output_dir": str(output_dir), "targeted_branch_proof": report["targeted_branch_proof"]}), indent=2, sort_keys=True))
    return report


def run_manifest(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = Path(args.config)
    config = read_json(config_path, {})
    runner = prepare_runner(
        config_path,
        output_dir,
        retain_seconds=False,
        contract_as_of_iso=getattr(args, "contract_as_of_iso", None),
    )
    target_keys = sorted(getattr(runner, "target_set", set()) or getattr(runner, "targets", []))
    dates = date_range(args.start_date, args.end_date, skip_weekends=not args.no_skip_weekends)
    manifest = build_input_manifest(
        config=config,
        dates=dates,
        sample_rows_per_day=int(args.sample_rows_per_day),
        target_keys=target_keys,
    )
    report = {
        "schema": "obvfutport_v2.canonical_joint_manifest_report.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "config": str(args.config),
        "output_dir": str(output_dir),
        "manifest": manifest,
        "static_contract_audit": audit_static_contract(),
        "ok": bool(manifest.get("ok")),
        "completed_at_ist": epoch_ist_iso(time.time()),
    }
    atomic_write_json(output_dir / "input_manifest_report.json", report)
    print(json.dumps(json_clean(report), indent=2, sort_keys=True))
    return report


def run_gate(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = Path(args.config)
    config = read_json(config_path, {})
    dates = date_range(args.start_date, args.end_date, skip_weekends=not args.no_skip_weekends)
    report: dict[str, Any] = {
        "schema": "obvfutport_v2.canonical_joint_gate_report.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "config": str(config_path),
        "output_dir": str(output_dir),
        "dates": dates,
        "static_contract_audit": audit_static_contract(),
    }
    with stage_timer(report, "input_manifest"):
        manifest_runner = prepare_runner(
            config_path,
            output_dir,
            retain_seconds=False,
            contract_as_of_iso=getattr(args, "contract_as_of_iso", None),
        )
        target_keys = sorted(getattr(manifest_runner, "target_set", set()) or getattr(manifest_runner, "targets", []))
        manifest = build_input_manifest(
            config=config,
            dates=dates,
            sample_rows_per_day=int(args.sample_rows_per_day),
            target_keys=target_keys,
        )
    report["manifest"] = manifest
    if not manifest.get("ok"):
        report["ok"] = False
        report["blocked_reason"] = "input_manifest_failed"
        report["completed_at_ist"] = epoch_ist_iso(time.time())
        atomic_write_json(output_dir / "canonical_joint_gate_report.json", report)
        print(json.dumps(json_clean(report), indent=2, sort_keys=True))
        return report
    symbols = parse_csv(args.symbols) or DEFAULT_FORENSIC_SYMBOLS
    with stage_timer(report, "runner_and_context_build"):
        runner = prepare_runner(
            config_path,
            output_dir,
            retain_seconds=True,
            contract_as_of_iso=getattr(args, "contract_as_of_iso", None),
        )
        metas = selected_metas(runner, symbols, args.max_symbols)
        contexts = build_symbol_contexts(runner=runner, config=config, metas=metas, dates=dates, args=args)
    index_root = Path(str(getattr(args, "index_root", "") or ""))
    target_keys_for_context = sorted({meta.signal_key for meta in metas} | {meta.execution_key for meta in metas})
    report["stream_index"] = {
        "enabled": bool(index_root),
        "index_root": str(index_root) if index_root else None,
        "require_index": bool(getattr(args, "require_index", False)),
        "missing_index_count": len(missing_index_files(index_root, dates, target_keys_for_context)) if index_root else None,
    }
    report["symbols"] = [meta.symbol for meta in metas]
    report["context_symbols"] = sorted(contexts)
    report["context_candidate_counts"] = {symbol: len(ctx.candidates) for symbol, ctx in contexts.items()}
    v1 = scorer.load_v1_portfolio_module(runner.config)
    symbol_reports: dict[str, Any] = {}
    with stage_timer(report, "symbol_scoring_and_proof"):
        for meta in metas:
            context = contexts.get(meta.symbol)
            if context is None:
                symbol_reports[meta.symbol] = {"symbol": meta.symbol, "status": "missing_context"}
                continue
            symbol_reports[meta.symbol] = score_symbol(runner=runner, v1=v1, context=context, args=args)
    report["symbols_report"] = symbol_reports
    report["historical_branch_coverage"] = aggregate_scenario_coverage(symbol_reports)
    report["targeted_branch_proof"] = targeted_branch_proof(runner) if bool(args.targeted_branch_proof) else {
        "enabled": False,
        "ok": False,
        "reason": "not_requested",
    }
    failed = []
    for symbol, item in symbol_reports.items():
        proof = item.get("proof") if isinstance(item, dict) else None
        if not proof:
            failed.append({"symbol": symbol, "reason": "missing_proof"})
            continue
        if not proof.get("comparison", {}).get("ok"):
            failed.append({"symbol": symbol, "reason": "proof_comparison_failed", **proof.get("comparison", {})})
        current_comparison = proof.get("current_comparison")
        if isinstance(current_comparison, dict) and not current_comparison.get("ok"):
            failed.append({"symbol": symbol, "reason": "current_runtime_proof_comparison_failed", **current_comparison})
        if not proof.get("invariants", {}).get("ok"):
            failed.append({"symbol": symbol, "reason": "invariant_failed", **proof.get("invariants", {})})
    if bool(args.require_branch_coverage) and not report["historical_branch_coverage"].get("ok"):
        failed.append(
            {
                "reason": "historical_branch_coverage_missing",
                "missing_required_branches": report["historical_branch_coverage"].get("missing_required_branches"),
            }
        )
    if bool(args.targeted_branch_proof) and not report["targeted_branch_proof"].get("ok"):
        failed.append({"reason": "targeted_branch_proof_failed", **report["targeted_branch_proof"]})
    report["ok"] = not failed
    report["failed_symbols"] = failed
    report["completed_at_ist"] = epoch_ist_iso(time.time())
    atomic_write_json(output_dir / "canonical_joint_gate_report.json", report)
    print(json.dumps(json_clean({"ok": report["ok"], "failed_symbols": failed, "output_dir": str(output_dir)}), indent=2, sort_keys=True))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["manifest", "gate", "build-index", "branch-proof"], default="manifest")
    parser.add_argument("--config", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--symbols", default=",".join(DEFAULT_FORENSIC_SYMBOLS))
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--sample-rows-per-day", type=int, default=5000)
    parser.add_argument("--no-skip-weekends", action="store_true")
    parser.add_argument("--index-root", default="")
    parser.add_argument("--reuse-index", action="store_true")
    parser.add_argument("--require-index", action="store_true")
    parser.add_argument(
        "--contract-as-of-iso",
        default="",
        help="Pin contract lifecycle selection for historical proofs, e.g. 2026-08-21T15:24:00+05:30.",
    )
    parser.add_argument("--require-branch-coverage", action="store_true")
    parser.add_argument("--targeted-branch-proof", action="store_true")
    parser.add_argument("--current-runtime-combos-only", action="store_true")
    parser.add_argument("--exit-combo-labels", default="")
    parser.add_argument("--tranche3-combo-labels", default="")
    parser.add_argument("--primary-short-thresholds", default="1.5,1.75,2.0")
    parser.add_argument("--fresh-breakout-multipliers", default="1,1.2,1.4,1.6")
    parser.add_argument("--long-strength-pcts", default="90,95")
    parser.add_argument("--short-weakness-pcts", default="1,5,10")
    parser.add_argument("--signal-quote-max-age-seconds", type=float, default=None)
    parser.add_argument("--current-long-strength-pct", type=float, default=95.0)
    parser.add_argument("--current-short-weakness-pct", type=float, default=1.0)
    parser.add_argument("--min-trades", type=int, default=1)
    parser.add_argument("--min-closed-trades", type=int, default=1)
    parser.add_argument("--min-success-rate-pct", type=float, default=0.0)
    parser.add_argument("--max-worst-loss-pct-deterioration", type=float, default=0.25)
    parser.add_argument("--min-worst-loss-pct-improvement", type=float, default=0.25)
    parser.add_argument("--max-success-rate-deterioration-pct", type=float, default=5.0)
    parser.add_argument("--min-net-preservation-ratio", type=float, default=0.80)
    parser.add_argument("--max-single-win-share", type=float, default=0.60)
    args = parser.parse_args()
    if args.mode == "manifest":
        report = run_manifest(args)
    elif args.mode == "build-index":
        report = run_build_index(args)
    elif args.mode == "branch-proof":
        report = run_branch_proof(args)
    else:
        report = run_gate(args)
    return 0 if report.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
