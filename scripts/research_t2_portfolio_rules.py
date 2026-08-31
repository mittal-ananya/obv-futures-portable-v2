from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import hashlib
import json
import math
import os
import pickle
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402


QUOTE_INDEX_CACHE_SCHEMA = "obvfutport_v2.t2_research.quote_index_day.v1"


def fnum(value: Any, default: float | None = None) -> float | None:
    out = base.as_float(value)
    return default if out is None else out


def write_json(path: Path, payload: Any) -> None:
    base.write_json(path, payload)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    base.write_jsonl(path, rows)


@dataclass(frozen=True)
class Policy:
    name: str
    formula: str
    min_score: float
    min_age_minutes: float
    max_age_minutes: float | None
    min_current_ret: float | None
    min_mfe: float | None
    max_mae_abs: float | None
    max_drawdown_from_mfe: float | None
    max_drawdown_to_mfe: float | None
    min_positive_ram_count: int
    max_spread_bps: float | None
    min_edge_cost_multiple: float | None
    min_minutes_to_session_end: float | None
    allow_replacement: bool
    min_hold_minutes: int
    replacement_gap: float
    replace_only_if_held_score_below: float | None
    replace_only_if_held_ret_below: float | None
    max_replacements_per_day: int | None


@dataclass
class LegRuntimeState:
    leg: base.TrancheLeg
    entry_ref_price: float
    mfe: float = 0.0
    mae: float = 0.0
    current_ret: float = 0.0
    mfe_epoch: int | None = None


@dataclass
class ResearchHolding:
    leg: base.TrancheLeg
    lots: int
    margin_locked: float
    entry_epoch: int
    entry_fill_price: float
    entry_ltp_price: float
    entry_score: float
    entry_features: dict[str, Any]


def trading_dates(start_date: date, end_date: date, stream_paths: list[tuple[str, Path]]) -> list[date]:
    available = [base.parse_date(day) for day, _path in stream_paths]
    return [day for day in available if start_date <= day <= end_date]


def required_key_hash(required_keys: set[str]) -> str:
    payload = "\n".join(sorted(required_keys)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:20]


def stream_fingerprint(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "resolved_path": str(path.resolve()),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def cache_paths(cache_dir: Path, trade_date: str, key_hash: str) -> tuple[Path, Path]:
    stem = f"{trade_date}_{key_hash}"
    return cache_dir / f"{stem}.quote_index.pkl.gz", cache_dir / f"{stem}.quote_index_meta.json"


def cache_meta_matches(meta: dict[str, Any], *, trade_date: str, key_hash: str, source: dict[str, Any]) -> bool:
    return (
        meta.get("schema") == QUOTE_INDEX_CACHE_SCHEMA
        and meta.get("trade_date") == trade_date
        and meta.get("required_key_hash") == key_hash
        and meta.get("source") == source
    )


def load_day_quote_cache(cache_file: Path) -> dict[str, list[tuple[int, float, float, float | None, float | None]]]:
    with gzip.open(cache_file, "rb") as handle:
        payload = pickle.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"invalid quote-index cache payload: {cache_file}")
    return payload


def save_day_quote_cache(
    *,
    cache_file: Path,
    meta_file: Path,
    payload: dict[str, list[tuple[int, float, float, float | None, float | None]]],
    meta: dict[str, Any],
) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_cache = cache_file.with_name(f"{cache_file.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with gzip.open(tmp_cache, "wb", compresslevel=3) as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    tmp_cache.replace(cache_file)
    write_json(meta_file, meta)


def merge_day_cache(
    index: base.QuoteIndex,
    payload: dict[str, list[tuple[int, float, float, float | None, float | None]]],
) -> int:
    merged = 0
    raw = index._raw  # type: ignore[attr-defined]
    for key, rows in payload.items():
        by_minute = raw.setdefault(key, {})
        for minute, event_epoch, price, bid, ask in rows:
            old = by_minute.get(int(minute))
            if old is None or float(event_epoch) >= old.event_epoch:
                by_minute[int(minute)] = base.Quote(int(minute), float(event_epoch), float(price), bid, ask)
            merged += 1
    return merged


def load_quote_index_throttled(
    root: Path,
    stream_paths: list[tuple[str, Path]],
    required_keys: set[str],
    *,
    progress_path: Path | None,
    yield_every_lines: int,
    yield_seconds: float,
    cache_dir: Path | None = None,
    rebuild_cache: bool = False,
) -> tuple[base.QuoteIndex, dict[str, Any]]:
    from obvfut_portable_v2.passive_runner import row_from_target_stream_line  # type: ignore

    index = base.QuoteIndex()
    per_day: dict[str, dict[str, Any]] = {}
    key_hash = required_key_hash(required_keys)
    total_lines = 0
    kept_rows = 0
    loaded_cache_rows = 0
    cache_hits = 0
    cache_misses = 0
    started = time.monotonic()
    last_progress = started
    for trade_date, path in stream_paths:
        source = stream_fingerprint(path)
        cache_file: Path | None = None
        meta_file: Path | None = None
        if cache_dir is not None:
            cache_file, meta_file = cache_paths(cache_dir, trade_date, key_hash)
            if not rebuild_cache and cache_file.exists() and meta_file.exists():
                meta = base.read_json(meta_file, {})
                if cache_meta_matches(meta, trade_date=trade_date, key_hash=key_hash, source=source):
                    payload = load_day_quote_cache(cache_file)
                    day_rows = merge_day_cache(index, payload)
                    loaded_cache_rows += day_rows
                    cache_hits += 1
                    per_day[trade_date] = {
                        **source,
                        "line_count": None,
                        "kept_target_rows": day_rows,
                        "cache_status": "hit",
                        "cache_file": str(cache_file),
                        "cache_meta_file": str(meta_file),
                    }
                    if progress_path is not None:
                        write_json(
                            progress_path,
                            {
                                "phase": "quote_index_cache_hit",
                                "trade_date": trade_date,
                                "cache_file": str(cache_file),
                                "loaded_cache_rows": loaded_cache_rows,
                                "cache_hits": cache_hits,
                                "cache_misses": cache_misses,
                                "elapsed_seconds": round(time.monotonic() - started, 3),
                            },
                        )
                    continue
        cache_misses += 1
        day_lines = 0
        day_kept = 0
        day_payload: dict[str, dict[int, tuple[int, float, float, float | None, float | None]]] = {}
        size = int(source["size_bytes"])
        with path.open("rb") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                day_lines += 1
                total_lines += 1
                row = row_from_target_stream_line(raw_line, trade_date, required_keys)
                if row is not None:
                    price = base.as_float(row.get("price"))
                    epoch = base.as_float(row.get("epoch"))
                    if price is not None and epoch is not None:
                        key = str(row.get("target") or "")
                        bid = base.as_float(row.get("bid"))
                        ask = base.as_float(row.get("ask"))
                        index.add(key, epoch, price, bid, ask)
                        minute = base.minute_floor(epoch)
                        by_minute = day_payload.setdefault(key, {})
                        old = by_minute.get(minute)
                        if old is None or float(epoch) >= old[1]:
                            by_minute[minute] = (int(minute), float(epoch), float(price), bid, ask)
                        day_kept += 1
                        kept_rows += 1
                if yield_every_lines > 0 and total_lines % yield_every_lines == 0:
                    now = time.monotonic()
                    if progress_path is not None and now - last_progress >= 5:
                        try:
                            position = handle.tell()
                        except OSError:
                            position = None
                        write_json(
                            progress_path,
                            {
                                "phase": "quote_index_scanning",
                                "trade_date": trade_date,
                                "current_file": str(path),
                                "current_file_position": position,
                                "current_file_size": size,
                                "current_file_pct": (position / size * 100.0) if position is not None and size else None,
                                "total_lines_scanned": total_lines,
                                "kept_target_rows": kept_rows,
                                "loaded_cache_rows": loaded_cache_rows,
                                "cache_hits": cache_hits,
                                "cache_misses": cache_misses,
                                "elapsed_seconds": round(now - started, 3),
                                "yield_every_lines": yield_every_lines,
                                "yield_seconds": yield_seconds,
                            },
                        )
                        last_progress = now
                    if yield_seconds > 0:
                        time.sleep(yield_seconds)
        if cache_file is not None and meta_file is not None:
            serializable_payload = {key: list(rows.values()) for key, rows in day_payload.items()}
            cache_meta = {
                "schema": QUOTE_INDEX_CACHE_SCHEMA,
                "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "trade_date": trade_date,
                "required_key_hash": key_hash,
                "required_key_count": len(required_keys),
                "source": source,
                "line_count": day_lines,
                "kept_target_rows": day_kept,
                "minute_quote_rows": sum(len(rows) for rows in serializable_payload.values()),
                "cache_file": str(cache_file),
            }
            save_day_quote_cache(
                cache_file=cache_file,
                meta_file=meta_file,
                payload=serializable_payload,
                meta=cache_meta,
            )
        per_day[trade_date] = {
            **source,
            "line_count": day_lines,
            "kept_target_rows": day_kept,
            "is_symlink": path.is_symlink(),
            "cache_status": "rebuilt" if cache_file is not None else "disabled",
            "cache_file": str(cache_file) if cache_file is not None else None,
        }
    index.finalize()
    return index, {
        "stream_days": per_day,
        "total_lines_scanned": total_lines,
        "kept_target_rows": kept_rows,
        "loaded_cache_rows": loaded_cache_rows,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "required_key_hash": key_hash,
        "quote_keys_loaded": index.key_count(),
        "minute_quote_rows": index.row_count(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "throttled": True,
        "yield_every_lines": yield_every_lines,
        "yield_seconds": yield_seconds,
    }


def quote_series(index: base.QuoteIndex, key: str, start_epoch: int, end_epoch: int) -> list[base.Quote]:
    minutes = index._keys.get(key)  # type: ignore[attr-defined]
    values = index._values.get(key)  # type: ignore[attr-defined]
    if not minutes or not values:
        return []
    start = base.minute_floor(start_epoch)
    end = base.minute_floor(end_epoch)
    left = bisect.bisect_left(minutes, start)
    right = bisect.bisect_right(minutes, end)
    return values[left:right]


def calc_ram_bundle(index: base.QuoteIndex, leg: base.TrancheLeg, clock_epoch: int, risk_floor: float) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for lookback in (10, 30, 60):
        out[f"ram_{lookback}"] = base.risk_adjusted_momentum(index, leg, clock_epoch, lookback, risk_floor=risk_floor)
        out[f"ret_{lookback}"] = base.directional_return(index, leg, clock_epoch, lookback)
    return out


def percentile_rank_values(items: list[tuple[str, float | None]]) -> dict[str, float | None]:
    return base.percentile_ranks(items)


def add_ranks(features: list[dict[str, Any]]) -> None:
    high_good_fields = [
        "ram_10",
        "ram_30",
        "ram_60",
        "ret_10",
        "ret_30",
        "ret_60",
        "current_ret",
        "mfe",
        "path_quality",
        "edge_return",
        "edge_to_cost_multiple",
    ]
    low_good_fields = ["mae_abs", "drawdown_from_mfe", "spread_bps", "age_minutes", "minutes_since_mfe"]
    for field in high_good_fields:
        ranks = percentile_rank_values([(row["row_id"], fnum(row.get(field))) for row in features])
        for row in features:
            row[f"rank_{field}"] = ranks.get(row["row_id"])
    for field in low_good_fields:
        ranks = percentile_rank_values(
            [(row["row_id"], -value if (value := fnum(row.get(field))) is not None else None) for row in features]
        )
        for row in features:
            row[f"rank_low_{field}"] = ranks.get(row["row_id"])


def blended_score(feature: dict[str, Any], formula: str) -> float | None:
    formulas = {
        "risk_first": [
            ("rank_low_mae_abs", 0.22),
            ("rank_low_drawdown_from_mfe", 0.22),
            ("rank_current_ret", 0.18),
            ("rank_path_quality", 0.18),
            ("rank_ram_60", 0.10),
            ("rank_ram_30", 0.05),
            ("rank_edge_to_cost_multiple", 0.05),
        ],
        "continuation": [
            ("rank_ram_60", 0.25),
            ("rank_ram_30", 0.20),
            ("rank_ram_10", 0.15),
            ("rank_current_ret", 0.20),
            ("rank_path_quality", 0.15),
            ("rank_low_spread_bps", 0.05),
        ],
        "smooth_survivor": [
            ("rank_current_ret", 0.25),
            ("rank_mfe", 0.15),
            ("rank_low_drawdown_from_mfe", 0.20),
            ("rank_low_mae_abs", 0.20),
            ("rank_ram_60", 0.15),
            ("rank_low_minutes_since_mfe", 0.05),
        ],
        "cost_adjusted": [
            ("rank_edge_to_cost_multiple", 0.25),
            ("rank_edge_return", 0.15),
            ("rank_current_ret", 0.20),
            ("rank_low_mae_abs", 0.15),
            ("rank_low_drawdown_from_mfe", 0.15),
            ("rank_ram_60", 0.10),
        ],
    }
    weighted = formulas[formula]
    score = 0.0
    total = 0.0
    for key, weight in weighted:
        value = fnum(feature.get(key))
        if value is None:
            continue
        score += float(value) * weight
        total += weight
    if total <= 0:
        return None
    return score / total


def positive_ram_count(feature: dict[str, Any]) -> int:
    return sum(1 for key in ("ram_10", "ram_30", "ram_60") if (value := fnum(feature.get(key))) is not None and value > 0)


def estimate_edge(index: base.QuoteIndex, v1_portfolio: Any, leg: base.TrancheLeg, clock_epoch: int, feature: dict[str, Any]) -> dict[str, Any]:
    rets = [fnum(feature.get("ret_10")), fnum(feature.get("ret_30")), fnum(feature.get("ret_60"))]
    if any(value is None for value in rets):
        return {"ok": False, "reason": "missing_directional_return"}
    edge_return = max(0.0, 0.35 * rets[0] + 0.25 * rets[1] + 0.40 * rets[2])  # type: ignore[operator]
    if edge_return <= 0:
        return {"ok": False, "reason": "non_positive_edge_return", "edge_return": edge_return}
    fill = base.execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")
    if fill is None:
        return {"ok": False, "reason": "missing_entry_fill_for_edge"}
    entry_fill = float(fill["fill_price"])
    projected_exit = entry_fill * (1.0 + edge_return) if leg.side == "long" else entry_fill * (1.0 - edge_return)
    acct = base.accounting(v1_portfolio, leg, entry_fill, projected_exit, 1)
    gross = abs(float(acct.get("gross_rupees") or 0.0))
    charges = float(acct.get("charges_rupees") or 0.0)
    net = float(acct.get("net_rupees") or 0.0)
    return {
        "ok": net > 0 and gross > 0,
        "reason": "ok" if net > 0 and gross > 0 else "non_positive_projected_net",
        "edge_return": edge_return,
        "gross_edge_rupees_per_lot": gross,
        "estimated_charges_rupees_per_lot": charges,
        "estimated_net_edge_rupees_per_lot": net,
        "edge_to_cost_multiple": (gross / charges) if charges > 0 else None,
    }


def build_feature_panel(
    *,
    legs: list[base.TrancheLeg],
    dates: list[date],
    index: base.QuoteIndex,
    v1_portfolio: Any,
    risk_floor: float,
) -> tuple[dict[int, dict[str, dict[str, Any]]], dict[str, Any]]:
    started = time.monotonic()
    panel: dict[int, dict[str, dict[str, Any]]] = {}
    sorted_legs = sorted(legs, key=lambda leg: (leg.entry_epoch, leg.symbol, leg.row_id))
    next_leg_idx = 0
    active: dict[str, LegRuntimeState] = {}
    stats = {
        "feature_rows": 0,
        "missing_execution_ref": 0,
        "missing_entry_ref": 0,
        "missing_signal_ram": 0,
        "missing_edge": 0,
        "evaluated_clocks": 0,
    }
    for day in dates:
        clocks = base.session_clock_epochs(day)
        session_end = clocks[-1] if clocks else None
        for clock_epoch in clocks:
            stats["evaluated_clocks"] += 1
            while next_leg_idx < len(sorted_legs) and sorted_legs[next_leg_idx].entry_epoch <= clock_epoch:
                leg = sorted_legs[next_leg_idx]
                next_leg_idx += 1
                if leg.exit_epoch is not None and leg.exit_epoch <= clock_epoch:
                    continue
                entry_ref = leg.entry_fill_price
                if entry_ref is None or entry_ref <= 0:
                    q = index.quote_at_or_before(leg.execution_key, leg.entry_epoch, max_age_seconds=300)
                    entry_ref = q.price if q is not None else None
                if entry_ref is None or entry_ref <= 0:
                    stats["missing_entry_ref"] += 1
                    continue
                active[leg.row_id] = LegRuntimeState(leg=leg, entry_ref_price=float(entry_ref))
            for row_id, state in list(active.items()):
                if state.leg.exit_epoch is not None and state.leg.exit_epoch <= clock_epoch:
                    active.pop(row_id, None)
            features: list[dict[str, Any]] = []
            ref_epoch = clock_epoch - 60
            for state in active.values():
                leg = state.leg
                if leg.exit_epoch is not None and leg.exit_epoch <= clock_epoch:
                    continue
                ram = calc_ram_bundle(index, leg, clock_epoch, risk_floor)
                if ram["ram_10"] is None or ram["ram_60"] is None:
                    stats["missing_signal_ram"] += 1
                    continue
                exec_quote = index.quote_at_or_before(leg.execution_key, ref_epoch, max_age_seconds=300)
                entry_quote = index.quote_at_or_before(leg.execution_key, clock_epoch, max_age_seconds=300)
                if exec_quote is None or entry_quote is None:
                    stats["missing_execution_ref"] += 1
                    continue
                direction = base.signed_direction(leg.side)
                if ref_epoch >= leg.entry_epoch:
                    current_ret = direction * ((exec_quote.price / state.entry_ref_price) - 1.0)
                    state.current_ret = current_ret
                    if current_ret >= state.mfe:
                        state.mfe = current_ret
                        state.mfe_epoch = exec_quote.minute_epoch
                    if current_ret <= state.mae:
                        state.mae = current_ret
                else:
                    current_ret = 0.0
                mae_abs = max(0.0, -state.mae)
                drawdown = max(0.0, state.mfe - current_ret)
                minutes_since_mfe = ((ref_epoch - state.mfe_epoch) / 60.0) if state.mfe_epoch else 0.0
                spread_bps = None
                if entry_quote.bid is not None and entry_quote.ask is not None and entry_quote.price > 0:
                    spread_bps = max(0.0, (float(entry_quote.ask) - float(entry_quote.bid)) / entry_quote.price * 10000.0)
                edge = estimate_edge(index, v1_portfolio, leg, clock_epoch, {**ram})
                if not edge.get("ok"):
                    stats["missing_edge"] += 1
                ram60_available_from = index.ram_available_from_epoch(leg.signal_key, 60)
                ram60_bounds = index.lookback_window_bounds(leg.signal_key, ref_epoch, 60)
                feature = {
                    "row_id": leg.row_id,
                    "symbol": leg.symbol,
                    "side": leg.side,
                    "entry_epoch": leg.entry_epoch,
                    "exit_epoch": leg.exit_epoch,
                    "clock_epoch": clock_epoch,
                    "age_minutes": max(0.0, (clock_epoch - leg.entry_epoch) / 60.0),
                    "minutes_to_session_end": ((session_end - clock_epoch) / 60.0) if session_end else None,
                    "current_ret": current_ret,
                    "mfe": state.mfe,
                    "mae": state.mae,
                    "mae_abs": mae_abs,
                    "drawdown_from_mfe": drawdown,
                    "drawdown_to_mfe": (drawdown / max(state.mfe, risk_floor)) if state.mfe > 0 else 1.0,
                    "path_quality": current_ret - drawdown - (0.5 * mae_abs),
                    "minutes_since_mfe": minutes_since_mfe,
                    "spread_bps": spread_bps,
                    "edge_return": edge.get("edge_return"),
                    "edge_to_cost_multiple": edge.get("edge_to_cost_multiple"),
                    "edge_diagnostics": edge,
                    "quote_history_mode": "research_full_session_quote_index",
                    "quote_history_key_scope": "all_t2_ledger_keys",
                    "signal_key_history_earliest_epoch": index.key_earliest_epoch(leg.signal_key),
                    "signal_key_history_earliest_time": base.epoch_ist_iso(index.key_earliest_epoch(leg.signal_key)),
                    "signal_key_history_latest_epoch": index.key_latest_epoch(leg.signal_key),
                    "signal_key_history_latest_time": base.epoch_ist_iso(index.key_latest_epoch(leg.signal_key)),
                    "ram_60_available_from_epoch": ram60_available_from,
                    "ram_60_available_from": base.epoch_ist_iso(ram60_available_from),
                    "ram_60_window_start_epoch": ram60_bounds[0] if ram60_bounds else None,
                    "ram_60_window_start": base.epoch_ist_iso(ram60_bounds[0]) if ram60_bounds else None,
                    "ram_60_window_end_epoch": ram60_bounds[1] if ram60_bounds else None,
                    "ram_60_window_end": base.epoch_ist_iso(ram60_bounds[1]) if ram60_bounds else None,
                    **ram,
                }
                features.append(feature)
            if features:
                add_ranks(features)
                for feature in features:
                    panel.setdefault(clock_epoch, {})[feature["row_id"]] = feature
                stats["feature_rows"] += len(features)
    stats["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return panel, stats


def candidate_passes_policy(feature: dict[str, Any], policy: Policy) -> bool:
    score = fnum(feature.get("portfolio_score"))
    if score is None or score < policy.min_score:
        return False
    age = fnum(feature.get("age_minutes"), 0.0) or 0.0
    if age < policy.min_age_minutes:
        return False
    if policy.max_age_minutes is not None and age > policy.max_age_minutes:
        return False
    if policy.min_minutes_to_session_end is not None:
        runway = fnum(feature.get("minutes_to_session_end"))
        if runway is not None and runway < policy.min_minutes_to_session_end:
            return False
    if policy.min_current_ret is not None and (fnum(feature.get("current_ret")) or 0.0) < policy.min_current_ret:
        return False
    if policy.min_mfe is not None and (fnum(feature.get("mfe")) or 0.0) < policy.min_mfe:
        return False
    if policy.max_mae_abs is not None and (fnum(feature.get("mae_abs")) or 0.0) > policy.max_mae_abs:
        return False
    if policy.max_drawdown_from_mfe is not None and (fnum(feature.get("drawdown_from_mfe")) or 0.0) > policy.max_drawdown_from_mfe:
        return False
    if policy.max_drawdown_to_mfe is not None and (fnum(feature.get("drawdown_to_mfe")) or 0.0) > policy.max_drawdown_to_mfe:
        return False
    if policy.min_positive_ram_count and positive_ram_count(feature) < policy.min_positive_ram_count:
        return False
    if policy.max_spread_bps is not None:
        spread = fnum(feature.get("spread_bps"))
        if spread is None or spread > policy.max_spread_bps:
            return False
    if policy.min_edge_cost_multiple is not None:
        edge = fnum(feature.get("edge_to_cost_multiple"))
        if edge is None or edge < policy.min_edge_cost_multiple:
            return False
    return True


def choose_lots(cash: float, equity: float, margin_per_lot: float) -> int:
    return base.choose_lots(cash, equity, margin_per_lot)


def holding_mtm(index: base.QuoteIndex, v1_portfolio: Any, holding: ResearchHolding, epoch: int) -> float:
    fill = base.execution_fill(index, v1_portfolio, holding.leg, epoch, phase="exit")
    if fill is None:
        return 0.0
    acct = base.accounting(v1_portfolio, holding.leg, holding.entry_fill_price, float(fill["fill_price"]), holding.lots)
    return float(acct.get("net_rupees") or 0.0)


def portfolio_equity(cash: float, holdings: dict[str, ResearchHolding], index: base.QuoteIndex, v1_portfolio: Any, epoch: int) -> float:
    return cash + sum(item.margin_locked for item in holdings.values()) + sum(holding_mtm(index, v1_portfolio, item, epoch) for item in holdings.values())


def close_holding(
    *,
    holding: ResearchHolding,
    exit_epoch: int,
    reason: str,
    index: base.QuoteIndex,
    v1_portfolio: Any,
    use_leg_exit_fill: bool,
    exit_features: dict[str, Any] | None,
) -> tuple[float, dict[str, Any]]:
    pseudo = base.Holding(
        leg=holding.leg,
        lots=holding.lots,
        margin_locked=holding.margin_locked,
        entry_epoch=holding.entry_epoch,
        entry_fill_price=holding.entry_fill_price,
        entry_ltp_price=holding.entry_ltp_price,
        entry_score=holding.entry_score,
        entry_ram_10=fnum(holding.entry_features.get("ram_10")),
        entry_ram_60=fnum(holding.entry_features.get("ram_60")),
    )
    pnl, row = base.close_holding(
        holding=pseudo,
        exit_epoch=exit_epoch,
        reason=reason,
        index=index,
        v1_portfolio=v1_portfolio,
        use_leg_exit_fill=use_leg_exit_fill,
        scores={holding.leg.row_id: exit_features or {}},
    )
    row["entry_features"] = holding.entry_features
    row["exit_features"] = exit_features
    row["policy_entry_score"] = holding.entry_score
    return pnl, row


def enter_holding(
    *,
    leg: base.TrancheLeg,
    lots: int,
    clock_epoch: int,
    feature: dict[str, Any],
    index: base.QuoteIndex,
    v1_portfolio: Any,
) -> ResearchHolding:
    fill = base.execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")
    if fill is None:
        raise RuntimeError(f"missing entry fill for T2 {leg.symbol} {clock_epoch}")
    return ResearchHolding(
        leg=leg,
        lots=int(lots),
        margin_locked=float(leg.margin_per_lot) * int(lots),
        entry_epoch=int(clock_epoch),
        entry_fill_price=float(fill["fill_price"]),
        entry_ltp_price=fnum(fill.get("ltp_price"), float(fill["fill_price"])) or float(fill["fill_price"]),
        entry_score=float(feature["portfolio_score"]),
        entry_features=dict(feature),
    )


def summarize(transactions: list[dict[str, Any]], holdings: dict[str, ResearchHolding], cash: float, equity: float, peak_equity: float, max_drawdown: float, peak_margin: float, initial_capital: float) -> dict[str, Any]:
    exits = [row for row in transactions if row.get("event") == "portfolio_exit"]
    nets = [float(row.get("net_rupees") or 0.0) for row in exits]
    wins = [x for x in nets if x > 0]
    return {
        "closed_trades": len(exits),
        "wins": len(wins),
        "losses": len(exits) - len(wins),
        "success_rate_pct": (len(wins) / len(exits) * 100.0) if exits else None,
        "realized_net_rupees": sum(nets),
        "cash_rupees": cash,
        "ending_equity_rupees": equity,
        "return_on_initial_pct": ((equity / initial_capital) - 1.0) * 100.0 if initial_capital else None,
        "open_positions": len(holdings),
        "current_margin_rupees": sum(item.margin_locked for item in holdings.values()),
        "peak_margin_rupees": peak_margin,
        "peak_equity_rupees": peak_equity,
        "max_drawdown_rupees": max_drawdown,
        "max_drawdown_pct": (max_drawdown / peak_equity * 100.0) if peak_equity else None,
        "avg_net_rupees": statistics.mean(nets) if nets else None,
        "median_net_rupees": statistics.median(nets) if nets else None,
        "worst_trade_rupees": min(nets) if nets else None,
        "best_trade_rupees": max(nets) if nets else None,
    }


def simulate_policy(
    *,
    policy: Policy,
    legs: list[base.TrancheLeg],
    dates: list[date],
    panel: dict[int, dict[str, dict[str, Any]]],
    index: base.QuoteIndex,
    v1_portfolio: Any,
    initial_capital: float,
    max_positions: int,
) -> dict[str, Any]:
    legs_by_id = {leg.row_id: leg for leg in legs}
    cash = float(initial_capital)
    holdings: dict[str, ResearchHolding] = {}
    transactions: list[dict[str, Any]] = []
    diagnostics = {
        "evaluated_clocks": 0,
        "eligible_features": 0,
        "entries": 0,
        "underlying_exits": 0,
        "replacement_exits": 0,
        "blocked_policy": 0,
        "blocked_cash": 0,
        "missing_entry_fill": 0,
        "missing_exit_fill": 0,
        "replacement_blocked": 0,
    }
    peak_equity = cash
    peak_margin = 0.0
    max_drawdown = 0.0
    replacements_by_day: dict[str, int] = {}
    for day in dates:
        day_key = day.isoformat()
        replacements_by_day[day_key] = 0
        for clock_epoch in base.session_clock_epochs(day):
            diagnostics["evaluated_clocks"] += 1
            clock_features = panel.get(clock_epoch, {})
            # Forced exits happen before new allocation at this minute.
            for holding_key, holding in list(holdings.items()):
                leg = holding.leg
                if leg.exit_epoch is not None and leg.exit_epoch <= clock_epoch:
                    try:
                        pnl, row = close_holding(
                            holding=holding,
                            exit_epoch=leg.exit_epoch,
                            reason="underlying_tranche_exit",
                            index=index,
                            v1_portfolio=v1_portfolio,
                            use_leg_exit_fill=True,
                            exit_features=clock_features.get(holding_key),
                        )
                    except RuntimeError:
                        diagnostics["missing_exit_fill"] += 1
                        continue
                    cash += holding.margin_locked + pnl
                    transactions.append(row)
                    holdings.pop(holding_key, None)
                    diagnostics["underlying_exits"] += 1
            candidates: list[dict[str, Any]] = []
            for row_id, feature in clock_features.items():
                if row_id in holdings:
                    continue
                leg = legs_by_id.get(row_id)
                if leg is None:
                    continue
                score = blended_score(feature, policy.formula)
                if score is None:
                    diagnostics["blocked_policy"] += 1
                    continue
                feature = dict(feature)
                feature["portfolio_score"] = score
                if not candidate_passes_policy(feature, policy):
                    diagnostics["blocked_policy"] += 1
                    continue
                diagnostics["eligible_features"] += 1
                candidates.append(feature)
            candidates.sort(key=lambda row: (float(row["portfolio_score"]), row["symbol"], row["row_id"]), reverse=True)
            # Fill vacant slots.
            for feature in candidates:
                if len(holdings) >= max_positions:
                    break
                leg = legs_by_id[feature["row_id"]]
                equity = portfolio_equity(cash, holdings, index, v1_portfolio, clock_epoch)
                lots = choose_lots(cash, equity, leg.margin_per_lot)
                if lots <= 0:
                    diagnostics["blocked_cash"] += 1
                    continue
                try:
                    holding = enter_holding(
                        leg=leg,
                        lots=lots,
                        clock_epoch=clock_epoch,
                        feature=feature,
                        index=index,
                        v1_portfolio=v1_portfolio,
                    )
                except RuntimeError:
                    diagnostics["missing_entry_fill"] += 1
                    continue
                cash -= holding.margin_locked
                holdings[leg.row_id] = holding
                transactions.append(
                    {
                        "event": "portfolio_entry",
                        "portfolio": "T2",
                        "policy": policy.name,
                        "symbol": leg.symbol,
                        "side": leg.side,
                        "position_id": leg.position_id,
                        "row_id": leg.row_id,
                        "lots": holding.lots,
                        "lot_size": leg.lot_size,
                        "margin_locked": holding.margin_locked,
                        "entry_epoch": clock_epoch,
                        "entry_time": base.epoch_ist_iso(clock_epoch),
                        "entry_fill_price": holding.entry_fill_price,
                        "entry_ltp_price": holding.entry_ltp_price,
                        "entry_score": holding.entry_score,
                        "entry_features": feature,
                        "underlying_tranche_entry_epoch": leg.entry_epoch,
                        "underlying_tranche_exit_epoch": leg.exit_epoch,
                    }
                )
                diagnostics["entries"] += 1
            # Replace only deteriorated holdings, not the weakest rank blindly.
            if policy.allow_replacement and len(holdings) >= max_positions:
                for feature in candidates:
                    if feature["row_id"] in holdings:
                        continue
                    if policy.max_replacements_per_day is not None and replacements_by_day[day_key] >= policy.max_replacements_per_day:
                        break
                    held: list[tuple[float, str, ResearchHolding, dict[str, Any]]] = []
                    for key, holding in holdings.items():
                        held_feature = clock_features.get(key)
                        if held_feature is None:
                            continue
                        held_feature = dict(held_feature)
                        held_score = blended_score(held_feature, policy.formula)
                        if held_score is None:
                            continue
                        held_feature["portfolio_score"] = held_score
                        held.append((float(held_score), key, holding, held_feature))
                    if not held:
                        break
                    weakest_score, weakest_key, weakest_holding, weakest_feature = min(held, key=lambda item: (item[0], item[1]))
                    candidate_score = float(feature["portfolio_score"])
                    held_ret = fnum(weakest_feature.get("current_ret"), 0.0) or 0.0
                    if policy.replace_only_if_held_score_below is not None and weakest_score > policy.replace_only_if_held_score_below:
                        diagnostics["replacement_blocked"] += 1
                        continue
                    if policy.replace_only_if_held_ret_below is not None and held_ret > policy.replace_only_if_held_ret_below:
                        diagnostics["replacement_blocked"] += 1
                        continue
                    if clock_epoch - weakest_holding.entry_epoch < policy.min_hold_minutes * 60:
                        diagnostics["replacement_blocked"] += 1
                        continue
                    if candidate_score <= weakest_score + policy.replacement_gap:
                        diagnostics["replacement_blocked"] += 1
                        continue
                    leg = legs_by_id[feature["row_id"]]
                    equity = portfolio_equity(cash, holdings, index, v1_portfolio, clock_epoch)
                    lots = choose_lots(cash + weakest_holding.margin_locked, equity, leg.margin_per_lot)
                    if lots <= 0:
                        diagnostics["blocked_cash"] += 1
                        continue
                    try:
                        pnl, exit_row = close_holding(
                            holding=weakest_holding,
                            exit_epoch=clock_epoch,
                            reason="risk_replacement",
                            index=index,
                            v1_portfolio=v1_portfolio,
                            use_leg_exit_fill=False,
                            exit_features=weakest_feature,
                        )
                        new_holding = enter_holding(
                            leg=leg,
                            lots=lots,
                            clock_epoch=clock_epoch,
                            feature=feature,
                            index=index,
                            v1_portfolio=v1_portfolio,
                        )
                    except RuntimeError:
                        diagnostics["missing_entry_fill"] += 1
                        continue
                    cash += weakest_holding.margin_locked + pnl
                    holdings.pop(weakest_key, None)
                    transactions.append(exit_row)
                    diagnostics["replacement_exits"] += 1
                    replacements_by_day[day_key] += 1
                    cash -= new_holding.margin_locked
                    holdings[leg.row_id] = new_holding
                    transactions.append(
                        {
                            "event": "portfolio_entry",
                            "portfolio": "T2",
                            "policy": policy.name,
                            "symbol": leg.symbol,
                            "side": leg.side,
                            "position_id": leg.position_id,
                            "row_id": leg.row_id,
                            "lots": new_holding.lots,
                            "lot_size": leg.lot_size,
                            "margin_locked": new_holding.margin_locked,
                            "entry_epoch": clock_epoch,
                            "entry_time": base.epoch_ist_iso(clock_epoch),
                            "entry_fill_price": new_holding.entry_fill_price,
                            "entry_ltp_price": new_holding.entry_ltp_price,
                            "entry_score": new_holding.entry_score,
                            "entry_features": feature,
                            "underlying_tranche_entry_epoch": leg.entry_epoch,
                            "underlying_tranche_exit_epoch": leg.exit_epoch,
                        }
                    )
                    diagnostics["entries"] += 1
                    break
            equity = portfolio_equity(cash, holdings, index, v1_portfolio, clock_epoch)
            peak_equity = max(peak_equity, equity)
            max_drawdown = max(max_drawdown, peak_equity - equity)
            peak_margin = max(peak_margin, sum(item.margin_locked for item in holdings.values()))
    final_epoch = index.last_epoch() or (base.session_clock_epochs(dates[-1])[-1] if dates else int(time.time()))
    final_equity = portfolio_equity(cash, holdings, index, v1_portfolio, final_epoch)
    peak_equity = max(peak_equity, final_equity)
    max_drawdown = max(max_drawdown, peak_equity - final_equity)
    open_rows = [
        {
            "portfolio": "T2",
            "policy": policy.name,
            "symbol": holding.leg.symbol,
            "side": holding.leg.side,
            "position_id": holding.leg.position_id,
            "row_id": holding.leg.row_id,
            "lots": holding.lots,
            "lot_size": holding.leg.lot_size,
            "margin_locked": holding.margin_locked,
            "entry_epoch": holding.entry_epoch,
            "entry_time": base.epoch_ist_iso(holding.entry_epoch),
            "entry_fill_price": holding.entry_fill_price,
            "entry_score": holding.entry_score,
            "unrealized_net_rupees": holding_mtm(index, v1_portfolio, holding, final_epoch),
            "entry_features": holding.entry_features,
        }
        for holding in holdings.values()
    ]
    summary = summarize(transactions, holdings, cash, final_equity, peak_equity, max_drawdown, peak_margin, initial_capital)
    summary.update({"policy": policy.__dict__, "diagnostics": diagnostics})
    return {"summary": summary, "transactions": transactions, "open_positions": open_rows}


def policy_grid() -> list[Policy]:
    policies: list[Policy] = []
    for formula in ("risk_first", "smooth_survivor", "continuation", "cost_adjusted"):
        for min_age in (5.0, 15.0, 30.0):
            for max_age in (90.0, 180.0, 300.0):
                policies.append(
                    Policy(
                        name=f"hold_{formula}_age{int(min_age)}_max{int(max_age)}",
                        formula=formula,
                        min_score=0.70 if formula in {"risk_first", "smooth_survivor"} else 0.78,
                        min_age_minutes=min_age,
                        max_age_minutes=max_age,
                        min_current_ret=0.0,
                        min_mfe=0.0005,
                        max_mae_abs=0.008,
                        max_drawdown_from_mfe=None,
                        max_drawdown_to_mfe=0.80,
                        min_positive_ram_count=2,
                        max_spread_bps=20.0,
                        min_edge_cost_multiple=5.0,
                        min_minutes_to_session_end=45.0,
                        allow_replacement=False,
                        min_hold_minutes=0,
                        replacement_gap=999.0,
                        replace_only_if_held_score_below=None,
                        replace_only_if_held_ret_below=None,
                        max_replacements_per_day=0,
                    )
                )
    for formula in ("risk_first", "smooth_survivor", "cost_adjusted"):
        for held_score in (0.35, 0.45, 0.55):
            for held_ret in (-0.002, 0.0):
                policies.append(
                    Policy(
                        name=f"replace_bad_{formula}_held{held_score:.2f}_ret{held_ret:.3f}".replace(".", "p").replace("-", "m"),
                        formula=formula,
                        min_score=0.78,
                        min_age_minutes=10.0,
                        max_age_minutes=240.0,
                        min_current_ret=0.0,
                        min_mfe=0.0005,
                        max_mae_abs=0.0075,
                        max_drawdown_from_mfe=None,
                        max_drawdown_to_mfe=0.75,
                        min_positive_ram_count=2,
                        max_spread_bps=20.0,
                        min_edge_cost_multiple=8.0,
                        min_minutes_to_session_end=60.0,
                        allow_replacement=True,
                        min_hold_minutes=90,
                        replacement_gap=0.18,
                        replace_only_if_held_score_below=held_score,
                        replace_only_if_held_ret_below=held_ret,
                        max_replacements_per_day=3,
                    )
                )
    for max_mae in (0.004, 0.006):
        policies.append(
            Policy(
                name=f"capital_preservation_mae{max_mae:.3f}".replace(".", "p"),
                formula="risk_first",
                min_score=0.65,
                min_age_minutes=15.0,
                max_age_minutes=240.0,
                min_current_ret=0.0005,
                min_mfe=0.001,
                max_mae_abs=max_mae,
                max_drawdown_from_mfe=0.004,
                max_drawdown_to_mfe=0.60,
                min_positive_ram_count=2,
                max_spread_bps=15.0,
                min_edge_cost_multiple=8.0,
                min_minutes_to_session_end=60.0,
                allow_replacement=True,
                min_hold_minutes=120,
                replacement_gap=0.20,
                replace_only_if_held_score_below=0.50,
                replace_only_if_held_ret_below=0.0,
                max_replacements_per_day=2,
            )
        )
    return policies


def objective(summary: dict[str, Any]) -> float:
    ret = fnum(summary.get("return_on_initial_pct"), -999.0) or -999.0
    dd = fnum(summary.get("max_drawdown_pct"), 100.0) or 100.0
    success = fnum(summary.get("success_rate_pct"), 0.0) or 0.0
    worst = fnum(summary.get("worst_trade_rupees"), 0.0) or 0.0
    trades = int(summary.get("closed_trades") or 0)
    trade_bonus = min(trades, 80) * 0.02
    return ret - (1.25 * dd) + (0.04 * success) + trade_bonus + max(-5.0, worst / 10000.0)


def write_summary_csv(path: Path, summaries: list[dict[str, Any]]) -> None:
    fields = [
        "policy_name",
        "objective",
        "closed_trades",
        "success_rate_pct",
        "ending_equity_rupees",
        "return_on_initial_pct",
        "realized_net_rupees",
        "max_drawdown_pct",
        "max_drawdown_rupees",
        "worst_trade_rupees",
        "best_trade_rupees",
        "open_positions",
        "current_margin_rupees",
        "peak_margin_rupees",
        "entries",
        "underlying_exits",
        "replacement_exits",
        "blocked_policy",
        "blocked_cash",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for summary in summaries:
            row = {
                "policy_name": summary["policy"]["name"],
                "objective": summary.get("objective"),
                "entries": (summary.get("diagnostics") or {}).get("entries"),
                "underlying_exits": (summary.get("diagnostics") or {}).get("underlying_exits"),
                "replacement_exits": (summary.get("diagnostics") or {}).get("replacement_exits"),
                "blocked_policy": (summary.get("diagnostics") or {}).get("blocked_policy"),
                "blocked_cash": (summary.get("diagnostics") or {}).get("blocked_cash"),
            }
            row.update({key: summary.get(key) for key in fields if key not in row})
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-25")
    parser.add_argument("--initial-capital", type=float, default=2_000_000.0)
    parser.add_argument("--max-positions", type=int, default=5)
    parser.add_argument("--risk-floor", type=float, default=0.001)
    parser.add_argument("--max-policies", type=int, default=0)
    parser.add_argument("--scan-yield-every-lines", type=int, default=0)
    parser.add_argument("--scan-yield-seconds", type=float, default=0.0)
    parser.add_argument("--quote-index-cache-dir", type=Path, default=None)
    parser.add_argument("--no-quote-index-cache", action="store_true")
    parser.add_argument("--rebuild-quote-index-cache", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    root = args.root
    base.add_paths(root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    start = base.parse_date(args.start_date)
    end = base.parse_date(args.end_date)
    loaded = base.load_rows(root)
    manifest = base.load_contract_manifest(root)
    margins = base.load_margin_lookup(root)
    legs_by_tranche = base.build_legs(loaded.get("rows_by_tranche") or {}, manifest, margins)
    legs = legs_by_tranche["T2"]
    required_keys: set[str] = set()
    for leg in legs:
        required_keys.add(leg.signal_key)
        required_keys.add(leg.execution_key)
    stream_paths = base.discover_stream_paths(root, start, end)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = None
    if not args.no_quote_index_cache:
        cache_dir = args.quote_index_cache_dir or (root / "state" / "research_cache" / "t2_quote_index")
    if args.scan_yield_every_lines > 0 and args.scan_yield_seconds > 0:
        quote_index, input_report = load_quote_index_throttled(
            root,
            stream_paths,
            required_keys,
            progress_path=args.output_dir / "scan_progress.json",
            yield_every_lines=int(args.scan_yield_every_lines),
            yield_seconds=float(args.scan_yield_seconds),
            cache_dir=cache_dir,
            rebuild_cache=bool(args.rebuild_quote_index_cache),
        )
    else:
        quote_index, input_report = load_quote_index_throttled(
            root,
            stream_paths,
            required_keys,
            progress_path=args.output_dir / "scan_progress.json",
            yield_every_lines=0,
            yield_seconds=0.0,
            cache_dir=cache_dir,
            rebuild_cache=bool(args.rebuild_quote_index_cache),
        )
    dates = trading_dates(start, end, stream_paths)
    write_json(
        args.output_dir / "phase_progress.json",
        {
            "phase": "quote_index_loaded",
            "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "input_stream": input_report,
            "t2_leg_count": len(legs),
            "required_key_count": len(required_keys),
        },
    )
    panel, feature_report = build_feature_panel(
        legs=legs,
        dates=dates,
        index=quote_index,
        v1_portfolio=v1_portfolio,
        risk_floor=float(args.risk_floor),
    )
    policies = policy_grid()
    if args.max_policies and args.max_policies > 0:
        policies = policies[: args.max_policies]
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    write_json(
        output / "phase_progress.json",
        {
            "phase": "feature_panel_built",
            "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "input_stream": input_report,
            "feature_panel": feature_report,
            "policy_count": len(policies),
            "t2_leg_count": len(legs),
            "required_key_count": len(required_keys),
        },
    )
    manifest_report = {
        "schema": "obvfutport_v2.t2_portfolio_policy_research.v1",
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "root": str(root),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "initial_capital": args.initial_capital,
        "max_positions": args.max_positions,
        "policy_count": len(policies),
        "t2_leg_count": len(legs),
        "required_key_count": len(required_keys),
        "input_stream": input_report,
        "feature_panel": feature_report,
        "input_rule": {
            "portfolio": "T2 only",
            "entry_clocks": "1-minute clocks 09:16-15:30 IST",
            "score_cutoff": "all feature inputs use target-stream quotes at or before clock minus one minute; entry fill uses current clock quote",
            "capital": "margin sizing, max 20% of current equity per selected position, integer futures lots, multi-lot allowed",
            "mutation": "read-only research output; no v2/Matrix/v1/Compass production writes",
        },
    }
    write_json(output / "input_manifest.json", manifest_report)
    summaries: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None
    started = time.monotonic()
    for idx, policy in enumerate(policies, start=1):
        result = simulate_policy(
            policy=policy,
            legs=legs,
            dates=dates,
            panel=panel,
            index=quote_index,
            v1_portfolio=v1_portfolio,
            initial_capital=float(args.initial_capital),
            max_positions=int(args.max_positions),
        )
        summary = result["summary"]
        summary["objective"] = objective(summary)
        summaries.append(summary)
        slug = policy.name
        write_json(output / f"{slug}_summary.json", summary)
        write_jsonl(output / f"{slug}_transactions.jsonl", result["transactions"])
        write_json(output / f"{slug}_open_positions.json", result["open_positions"])
        if best is None or float(summary["objective"]) > float(best["objective"]):
            best = summary
        if idx % 10 == 0 or idx == len(policies):
            write_json(
                output / "progress.json",
                {
                    "completed_policies": idx,
                    "total_policies": len(policies),
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "best_policy": best["policy"]["name"] if best else None,
                    "best_objective": best.get("objective") if best else None,
                    "best_return_pct": best.get("return_on_initial_pct") if best else None,
                    "best_max_drawdown_pct": best.get("max_drawdown_pct") if best else None,
                },
            )
    summaries.sort(key=lambda item: float(item.get("objective") or -10**9), reverse=True)
    write_summary_csv(output / "t2_policy_research_summary.csv", summaries)
    write_json(output / "best_policy.json", summaries[0] if summaries else {})
    write_json(
        output / "final_report.json",
        {
            "manifest": manifest_report,
            "best_policy": summaries[0] if summaries else None,
            "top_10": summaries[:10],
            "elapsed_seconds": round(time.monotonic() - started, 3),
        },
    )
    print(json.dumps({"output_dir": str(output), "best_policy": summaries[0] if summaries else None, "top_5": summaries[:5]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
