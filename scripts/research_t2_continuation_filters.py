#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_portfolio_rules as portfolio_rules  # noqa: E402


IST = ZoneInfo("Asia/Kolkata")
SCHEMA = "obvfutport_v2.t2_continuation_filter_research.v1"


@dataclass(frozen=True)
class ContinuationPolicy:
    name: str
    formula: str
    min_score: float
    min_age_minutes: float
    max_age_minutes: float | None
    min_current_ret: float
    min_mfe: float
    max_mae_abs: float
    max_drawdown_to_mfe: float
    min_positive_ram_count: int
    max_spread_bps: float
    min_edge_cost_multiple: float
    min_minutes_to_session_end: float


def write_json(path: Path, payload: Any) -> None:
    base.write_json(path, payload)


def period_bounds(start: date, end: date) -> tuple[int, int]:
    start_epoch = int(datetime.combine(start, dt_time(0, 0), tzinfo=IST).timestamp())
    end_epoch = int(datetime.combine(end, dt_time(23, 59, 59), tzinfo=IST).timestamp())
    return start_epoch, end_epoch


def finite_float(value: Any) -> float | None:
    return base.as_float(value)


def metric_stats(series: pd.Series) -> dict[str, Any]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
    return {
        "count": int(values.shape[0]),
        "min": round(float(values.min()), 6),
        "max": round(float(values.max()), 6),
        "mean": round(float(values.mean()), 6),
        "median": round(float(values.median()), 6),
    }


def safe_slug(text: str) -> str:
    out = []
    for ch in text:
        if ch.isalnum() or ch in {"_", "-"}:
            out.append(ch)
        elif ch == ".":
            out.append("p")
        else:
            out.append("_")
    return "".join(out)


def continuation_policy_grid() -> list[ContinuationPolicy]:
    formulas = ("risk_first", "smooth_survivor", "continuation", "cost_adjusted")
    score_levels = (0.70, 0.75, 0.80, 0.85, 0.90)
    age_windows = (
        (5.0, 60.0),
        (5.0, 120.0),
        (15.0, 90.0),
        (15.0, 180.0),
        (30.0, 90.0),
        (30.0, 180.0),
        (30.0, 300.0),
        (45.0, 180.0),
        (60.0, 240.0),
        (90.0, 300.0),
    )
    runway_levels = (0.0, 45.0)
    profiles = (
        {
            "label": "mild",
            "min_current_ret": 0.0,
            "min_mfe": 0.0005,
            "max_mae_abs": 0.0080,
            "max_drawdown_to_mfe": 0.80,
            "min_positive_ram_count": 2,
            "max_spread_bps": 20.0,
            "min_edge_cost_multiple": 3.0,
        },
        {
            "label": "balanced",
            "min_current_ret": 0.0005,
            "min_mfe": 0.0010,
            "max_mae_abs": 0.0060,
            "max_drawdown_to_mfe": 0.65,
            "min_positive_ram_count": 2,
            "max_spread_bps": 20.0,
            "min_edge_cost_multiple": 5.0,
        },
        {
            "label": "strict",
            "min_current_ret": 0.0010,
            "min_mfe": 0.0020,
            "max_mae_abs": 0.0050,
            "max_drawdown_to_mfe": 0.50,
            "min_positive_ram_count": 3,
            "max_spread_bps": 15.0,
            "min_edge_cost_multiple": 5.0,
        },
        {
            "label": "edge_strict",
            "min_current_ret": 0.0015,
            "min_mfe": 0.0030,
            "max_mae_abs": 0.0040,
            "max_drawdown_to_mfe": 0.40,
            "min_positive_ram_count": 3,
            "max_spread_bps": 15.0,
            "min_edge_cost_multiple": 8.0,
        },
        {
            "label": "tight_risk",
            "min_current_ret": 0.0005,
            "min_mfe": 0.0015,
            "max_mae_abs": 0.0030,
            "max_drawdown_to_mfe": 0.50,
            "min_positive_ram_count": 2,
            "max_spread_bps": 12.0,
            "min_edge_cost_multiple": 5.0,
        },
    )
    out: list[ContinuationPolicy] = []
    seen: set[str] = set()
    for formula in formulas:
        for min_score in score_levels:
            for min_age, max_age in age_windows:
                for runway in runway_levels:
                    for profile in profiles:
                        name = (
                            f"{formula}_{profile['label']}"
                            f"_score{min_score:.2f}_age{int(min_age)}to{int(max_age)}_runway{int(runway)}"
                        ).replace(".", "p")
                        if name in seen:
                            continue
                        seen.add(name)
                        out.append(
                            ContinuationPolicy(
                                name=name,
                                formula=formula,
                                min_score=min_score,
                                min_age_minutes=min_age,
                                max_age_minutes=max_age,
                                min_current_ret=float(profile["min_current_ret"]),
                                min_mfe=float(profile["min_mfe"]),
                                max_mae_abs=float(profile["max_mae_abs"]),
                                max_drawdown_to_mfe=float(profile["max_drawdown_to_mfe"]),
                                min_positive_ram_count=int(profile["min_positive_ram_count"]),
                                max_spread_bps=float(profile["max_spread_bps"]),
                                min_edge_cost_multiple=float(profile["min_edge_cost_multiple"]),
                                min_minutes_to_session_end=runway,
                            )
                        )
    anchor = ContinuationPolicy(
        name="anchor_hold_risk_first_age30_max180",
        formula="risk_first",
        min_score=0.70,
        min_age_minutes=30.0,
        max_age_minutes=180.0,
        min_current_ret=0.0,
        min_mfe=0.0005,
        max_mae_abs=0.008,
        max_drawdown_to_mfe=0.80,
        min_positive_ram_count=2,
        max_spread_bps=20.0,
        min_edge_cost_multiple=5.0,
        min_minutes_to_session_end=45.0,
    )
    return [anchor] + [policy for policy in out if policy.name != anchor.name]


def load_quote_index_from_cache(
    *,
    root: Path,
    stream_paths: list[tuple[str, Path]],
    required_keys: set[str],
    cache_dir: Path,
) -> tuple[base.QuoteIndex, dict[str, Any]]:
    index = base.QuoteIndex()
    key_hash = portfolio_rules.required_key_hash(required_keys)
    per_day: dict[str, Any] = {}
    total_rows = 0
    missing: list[dict[str, Any]] = []
    for trade_date, stream_path in stream_paths:
        source = portfolio_rules.stream_fingerprint(stream_path)
        cache_file, meta_file = portfolio_rules.cache_paths(cache_dir, trade_date, key_hash)
        cache_status = "hit"
        if not cache_file.exists() or not meta_file.exists():
            candidates = sorted(cache_dir.glob(f"{trade_date}_*.quote_index_meta.json"))
            fallback: tuple[Path, Path, dict[str, Any]] | None = None
            for candidate_meta_file in candidates:
                candidate_meta = base.read_json(candidate_meta_file, {})
                if candidate_meta.get("schema") != portfolio_rules.QUOTE_INDEX_CACHE_SCHEMA:
                    continue
                if candidate_meta.get("trade_date") != trade_date:
                    continue
                if candidate_meta.get("source") != source:
                    continue
                candidate_cache = candidate_meta_file.with_name(candidate_meta_file.name.replace("_meta.json", ".pkl.gz"))
                if not candidate_cache.exists():
                    continue
                fallback = (candidate_cache, candidate_meta_file, candidate_meta)
                break
            if fallback is None:
                missing.append({"trade_date": trade_date, "cache_file": str(cache_file), "meta_file": str(meta_file)})
                continue
            cache_file, meta_file, meta = fallback
            cache_status = "compatible_fallback"
        else:
            meta = base.read_json(meta_file, {})
            if not portfolio_rules.cache_meta_matches(meta, trade_date=trade_date, key_hash=key_hash, source=source):
                missing.append({"trade_date": trade_date, "cache_file": str(cache_file), "reason": "stale_or_signature_mismatch"})
                continue
        payload = portfolio_rules.load_day_quote_cache(cache_file)
        rows = portfolio_rules.merge_day_cache(index, payload)
        total_rows += rows
        missing_required_keys = len(required_keys.difference(payload.keys()))
        per_day[trade_date] = {
            **source,
            "cache_file": str(cache_file),
            "cache_meta_file": str(meta_file),
            "kept_target_rows": rows,
            "cache_status": cache_status,
            "payload_key_count": len(payload),
            "missing_required_key_count": missing_required_keys,
        }
    if missing:
        raise RuntimeError(f"missing quote-index caches: {json.dumps(missing[:5], sort_keys=True)}")
    index.finalize()
    return index, {
        "cache_dir": str(cache_dir),
        "required_key_hash": key_hash,
        "cache_hits": len(per_day),
        "loaded_cache_rows": total_rows,
        "key_count": index.key_count(),
        "row_count": index.row_count(),
        "per_day": per_day,
    }


def load_quote_index(
    *,
    root: Path,
    stream_paths: list[tuple[str, Path]],
    required_keys: set[str],
    cache_dir: Path,
    cache_only: bool,
    progress_path: Path | None,
) -> tuple[base.QuoteIndex, dict[str, Any]]:
    if cache_only:
        return load_quote_index_from_cache(root=root, stream_paths=stream_paths, required_keys=required_keys, cache_dir=cache_dir)
    return portfolio_rules.load_quote_index_throttled(
        root,
        stream_paths,
        required_keys,
        progress_path=progress_path,
        yield_every_lines=0,
        yield_seconds=0.0,
        cache_dir=cache_dir,
        rebuild_cache=False,
    )


def build_outcome_frame(
    *,
    panel: dict[int, dict[str, dict[str, Any]]],
    legs: list[base.TrancheLeg],
    index: base.QuoteIndex,
    v1_portfolio: Any,
    period_end_epoch: int,
    include_open_after_period: bool = False,
    period_mark_epoch: int | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    legs_by_id = {leg.row_id: leg for leg in legs}
    exit_fill_by_id: dict[str, dict[str, Any] | None] = {}
    skip_counts = {
        "open_after_period": 0,
        "missing_leg": 0,
        "missing_exit_fill": 0,
        "missing_entry_fill": 0,
        "non_positive_entry_fill": 0,
        "non_positive_margin": 0,
    }
    records: list[dict[str, Any]] = []
    formula_names = ("risk_first", "smooth_survivor", "continuation", "cost_adjusted")
    for clock_epoch in sorted(panel):
        for row_id, feature in panel[clock_epoch].items():
            leg = legs_by_id.get(row_id)
            if leg is None:
                skip_counts["missing_leg"] += 1
                continue
            open_after_period = leg.exit_epoch is None or leg.exit_epoch > period_end_epoch
            if open_after_period and not include_open_after_period:
                skip_counts["open_after_period"] += 1
                continue
            effective_exit_epoch = int(period_mark_epoch or period_end_epoch) if open_after_period else int(leg.exit_epoch or 0)
            if effective_exit_epoch <= int(clock_epoch):
                skip_counts["open_after_period" if open_after_period else "missing_exit_fill"] += 1
                continue
            if leg.margin_per_lot <= 0:
                skip_counts["non_positive_margin"] += 1
                continue
            exit_fill = exit_fill_by_id.get(row_id)
            if row_id not in exit_fill_by_id:
                exit_fill = base.execution_fill(index, v1_portfolio, leg, effective_exit_epoch, phase="exit")
                exit_fill_by_id[row_id] = exit_fill
            if exit_fill is None:
                skip_counts["missing_exit_fill"] += 1
                continue
            entry_fill = base.execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")
            if entry_fill is None:
                skip_counts["missing_entry_fill"] += 1
                continue
            entry_price = finite_float(entry_fill.get("fill_price"))
            exit_price = finite_float(exit_fill.get("fill_price"))
            if entry_price is None or exit_price is None or entry_price <= 0:
                skip_counts["non_positive_entry_fill"] += 1
                continue
            acct = base.accounting(v1_portfolio, leg, entry_price, exit_price, 1)
            net_rupees = float(acct.get("net_rupees") or 0.0)
            gross_rupees = float(acct.get("gross_rupees") or 0.0)
            charges_rupees = float(acct.get("charges_rupees") or 0.0)
            direction = base.signed_direction(leg.side)
            gross_return_pct = direction * ((exit_price / entry_price) - 1.0) * 100.0
            record: dict[str, Any] = {
                "row_id": row_id,
                "symbol": leg.symbol,
                "side": leg.side,
                "clock_epoch": int(clock_epoch),
                "clock_time": base.epoch_ist_iso(clock_epoch),
                "t2_entry_epoch": int(leg.entry_epoch),
                "t2_entry_time": base.epoch_ist_iso(leg.entry_epoch),
                "t2_exit_epoch": int(effective_exit_epoch),
                "t2_exit_time": base.epoch_ist_iso(effective_exit_epoch),
                "t2_actual_exit_epoch": int(leg.exit_epoch) if leg.exit_epoch is not None else None,
                "t2_actual_exit_time": base.epoch_ist_iso(leg.exit_epoch) if leg.exit_epoch is not None else None,
                "t2_status": "open_at_period_end" if open_after_period else "closed",
                "open_at_period_end": bool(open_after_period),
                "entry_fill_price": entry_price,
                "exit_fill_price": exit_price,
                "gross_return_pct": gross_return_pct,
                "net_rupees": net_rupees,
                "gross_rupees": gross_rupees,
                "charges_rupees": charges_rupees,
                "net_return_on_margin_pct": (net_rupees / float(leg.margin_per_lot)) * 100.0,
                "hold_minutes_after_clock": (float(effective_exit_epoch) - float(clock_epoch)) / 60.0,
                "margin_per_lot": float(leg.margin_per_lot),
                "lot_size": int(leg.lot_size or 1),
                "signal_source": leg.signal_source,
            }
            for key, value in feature.items():
                if key in {"row_id", "symbol", "side"}:
                    continue
                number = finite_float(value)
                if number is not None:
                    record[key] = number
            record["positive_ram_count"] = portfolio_rules.positive_ram_count(feature)
            for formula in formula_names:
                record[f"score_{formula}"] = portfolio_rules.blended_score(feature, formula)
            records.append(record)
    frame = pd.DataFrame.from_records(records)
    if not frame.empty:
        frame.sort_values(["clock_epoch", "row_id"], inplace=True, kind="mergesort")
        frame.reset_index(drop=True, inplace=True)
    report = {
        "rows": int(frame.shape[0]),
        "unique_legs_with_forward_rows": int(frame["row_id"].nunique()) if not frame.empty else 0,
        "skip_counts": skip_counts,
        "include_open_after_period": bool(include_open_after_period),
        "period_mark_epoch": int(period_mark_epoch) if period_mark_epoch is not None else None,
        "period_mark_time": base.epoch_ist_iso(period_mark_epoch) if period_mark_epoch is not None else None,
    }
    return frame, report


def summarize_selection(selected: pd.DataFrame, policy: ContinuationPolicy, *, min_trades_for_quality: int) -> dict[str, Any]:
    if selected.empty:
        return {
            "policy_name": policy.name,
            "formula": policy.formula,
            "selected_legs": 0,
            "quality_pass": False,
            "quality_reason": "no_selected_legs",
            "objective": -10**9,
            **policy.__dict__,
        }
    wins = selected["net_rupees"] > 0
    gross_wins = selected["gross_return_pct"] > 0
    success_rate = float(wins.mean() * 100.0)
    gross_success_rate = float(gross_wins.mean() * 100.0)
    net_return_margin = pd.to_numeric(selected["net_return_on_margin_pct"], errors="coerce")
    net_rupees = pd.to_numeric(selected["net_rupees"], errors="coerce")
    gross_return = pd.to_numeric(selected["gross_return_pct"], errors="coerce")
    dates = pd.to_datetime(selected["clock_time"], errors="coerce").dt.strftime("%Y-%m-%d")
    day_stats = []
    for day, group in selected.assign(_day=dates).groupby("_day", dropna=True):
        day_wins = group["net_rupees"] > 0
        day_stats.append(
            {
                "date": day,
                "count": int(group.shape[0]),
                "success_rate_pct": round(float(day_wins.mean() * 100.0), 4) if not group.empty else None,
                "net_rupees": round(float(group["net_rupees"].sum()), 4),
                "median_net_return_on_margin_pct": round(float(group["net_return_on_margin_pct"].median()), 6),
            }
        )
    positive_days = sum(1 for item in day_stats if float(item["net_rupees"]) > 0)
    mean_margin = float(net_return_margin.mean())
    median_margin = float(net_return_margin.median())
    worst_margin = float(net_return_margin.min())
    total_net = float(net_rupees.sum())
    selected_count = int(selected.shape[0])
    quality_reasons: list[str] = []
    if selected_count < min_trades_for_quality:
        quality_reasons.append("too_few_trades")
    if success_rate < 50.0:
        quality_reasons.append("success_below_50")
    if mean_margin <= 0:
        quality_reasons.append("non_positive_mean_margin_return")
    if median_margin <= 0:
        quality_reasons.append("non_positive_median_margin_return")
    if positive_days < max(2, math.ceil(len(day_stats) * 0.40)):
        quality_reasons.append("weak_day_consistency")
    quality_pass = not quality_reasons
    # Positive objective requires positive forward expectancy. Count bonus is deliberately modest.
    objective = (
        (success_rate - 50.0) * 0.75
        + median_margin * 2.0
        + mean_margin * 1.25
        + min(3.0, math.log1p(selected_count) * 0.4)
        + (positive_days / max(1, len(day_stats))) * 1.5
        - abs(min(0.0, worst_margin)) * 0.35
    )
    return {
        "policy_name": policy.name,
        "formula": policy.formula,
        "selected_legs": selected_count,
        "wins": int(wins.sum()),
        "success_rate_pct": round(success_rate, 4),
        "gross_wins": int(gross_wins.sum()),
        "gross_success_rate_pct": round(gross_success_rate, 4),
        "total_net_rupees_per_lot": round(total_net, 4),
        "avg_net_rupees_per_lot": round(float(net_rupees.mean()), 4),
        "median_net_rupees_per_lot": round(float(net_rupees.median()), 4),
        "worst_net_rupees_per_lot": round(float(net_rupees.min()), 4),
        "best_net_rupees_per_lot": round(float(net_rupees.max()), 4),
        "mean_net_return_on_margin_pct": round(mean_margin, 6),
        "median_net_return_on_margin_pct": round(median_margin, 6),
        "min_net_return_on_margin_pct": round(worst_margin, 6),
        "max_net_return_on_margin_pct": round(float(net_return_margin.max()), 6),
        "mean_gross_return_pct": round(float(gross_return.mean()), 6),
        "median_gross_return_pct": round(float(gross_return.median()), 6),
        "min_gross_return_pct": round(float(gross_return.min()), 6),
        "max_gross_return_pct": round(float(gross_return.max()), 6),
        "mean_hold_minutes_after_clock": round(float(selected["hold_minutes_after_clock"].mean()), 4),
        "median_hold_minutes_after_clock": round(float(selected["hold_minutes_after_clock"].median()), 4),
        "long_count": int((selected["side"] == "long").sum()),
        "short_count": int((selected["side"] == "short").sum()),
        "active_days": int(len(day_stats)),
        "positive_days": int(positive_days),
        "quality_pass": quality_pass,
        "quality_reason": ",".join(quality_reasons) if quality_reasons else "pass",
        "objective": round(objective, 6),
        "day_stats": day_stats,
        **policy.__dict__,
    }


def first_passing_rows(frame: pd.DataFrame, policy: ContinuationPolicy) -> pd.DataFrame:
    if frame.empty:
        return frame
    score = pd.to_numeric(frame[f"score_{policy.formula}"], errors="coerce")
    mask = (
        (score >= policy.min_score)
        & (frame["age_minutes"] >= policy.min_age_minutes)
        & (frame["age_minutes"] <= float(policy.max_age_minutes if policy.max_age_minutes is not None else 10**9))
        & (frame["current_ret"] >= policy.min_current_ret)
        & (frame["mfe"] >= policy.min_mfe)
        & (frame["mae_abs"] <= policy.max_mae_abs)
        & (frame["drawdown_to_mfe"] <= policy.max_drawdown_to_mfe)
        & (frame["positive_ram_count"] >= policy.min_positive_ram_count)
        & (frame["spread_bps"] <= policy.max_spread_bps)
        & (frame["edge_to_cost_multiple"] >= policy.min_edge_cost_multiple)
        & (frame["minutes_to_session_end"] >= policy.min_minutes_to_session_end)
    )
    passed = frame.loc[mask].copy()
    if passed.empty:
        return passed
    passed["portfolio_score"] = pd.to_numeric(passed[f"score_{policy.formula}"], errors="coerce")
    return passed.drop_duplicates("row_id", keep="first")


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = [
        "policy_name",
        "objective",
        "quality_pass",
        "quality_reason",
        "selected_legs",
        "wins",
        "success_rate_pct",
        "mean_net_return_on_margin_pct",
        "median_net_return_on_margin_pct",
        "min_net_return_on_margin_pct",
        "max_net_return_on_margin_pct",
        "total_net_rupees_per_lot",
        "avg_net_rupees_per_lot",
        "median_net_rupees_per_lot",
        "worst_net_rupees_per_lot",
        "best_net_rupees_per_lot",
        "mean_gross_return_pct",
        "median_gross_return_pct",
        "long_count",
        "short_count",
        "active_days",
        "positive_days",
        "formula",
        "min_score",
        "min_age_minutes",
        "max_age_minutes",
        "min_current_ret",
        "min_mfe",
        "max_mae_abs",
        "max_drawdown_to_mfe",
        "min_positive_ram_count",
        "max_spread_bps",
        "min_edge_cost_multiple",
        "min_minutes_to_session_end",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def select_period_legs(
    legs: list[base.TrancheLeg],
    *,
    start_epoch: int,
    end_epoch: int,
    scope: str,
) -> list[base.TrancheLeg]:
    if scope == "entry":
        return [leg for leg in legs if start_epoch <= leg.entry_epoch <= end_epoch]
    if scope == "overlap":
        return [
            leg
            for leg in legs
            if leg.entry_epoch <= end_epoch and (leg.exit_epoch is None or leg.exit_epoch >= start_epoch)
        ]
    raise ValueError(f"unsupported leg scope: {scope}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-25")
    parser.add_argument("--risk-floor", type=float, default=0.001)
    parser.add_argument("--min-trades-for-quality", type=int, default=20)
    parser.add_argument("--max-policies", type=int, default=0)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--quote-index-cache-dir", type=Path, default=None)
    parser.add_argument("--allow-raw-scan", action="store_true")
    parser.add_argument(
        "--include-open-after-period",
        action="store_true",
        help="Include T2 legs still open at the period end and mark them to the last available clock.",
    )
    parser.add_argument(
        "--leg-scope",
        choices=("entry", "overlap"),
        default="entry",
        help=(
            "entry keeps the historical behavior: include T2 legs whose original entry is inside the date range. "
            "overlap includes carry-in legs whose lifetime overlaps the date range, useful for incremental extensions."
        ),
    )
    args = parser.parse_args()

    started = time.monotonic()
    root = args.root
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    base.add_paths(root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    start = base.parse_date(args.start_date)
    end = base.parse_date(args.end_date)
    start_epoch, end_epoch = period_bounds(start, end)
    loaded = base.load_rows(root)
    manifest = base.load_contract_manifest(root)
    margins = base.load_margin_lookup(root)
    all_t2_legs = base.build_legs(loaded.get("rows_by_tranche") or {}, manifest, margins)["T2"]
    period_legs = select_period_legs(all_t2_legs, start_epoch=start_epoch, end_epoch=end_epoch, scope=args.leg_scope)
    closed_period_legs = [leg for leg in period_legs if leg.exit_epoch is not None and leg.exit_epoch <= end_epoch]
    carry_in_legs = [leg for leg in period_legs if leg.entry_epoch < start_epoch]
    open_after_period_legs = [leg for leg in period_legs if leg.exit_epoch is None or leg.exit_epoch > end_epoch]
    required_keys: set[str] = set()
    for leg in period_legs:
        required_keys.add(leg.signal_key)
        required_keys.add(leg.execution_key)
    stream_paths = base.discover_stream_paths(root, start, end)
    dates = portfolio_rules.trading_dates(start, end, stream_paths)
    cache_dir = args.quote_index_cache_dir or (root / "state" / "research_cache" / "t2_quote_index")
    index, input_report = load_quote_index(
        root=root,
        stream_paths=stream_paths,
        required_keys=required_keys,
        cache_dir=cache_dir,
        cache_only=not bool(args.allow_raw_scan),
        progress_path=output_dir / "scan_progress.json",
    )
    write_json(
        output_dir / "phase_progress.json",
        {
            "phase": "quote_index_loaded",
            "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "input_stream": input_report,
            "period_t2_leg_count": len(period_legs),
            "closed_period_t2_leg_count": len(closed_period_legs),
            "carry_in_t2_leg_count": len(carry_in_legs),
            "open_after_period_t2_leg_count": len(open_after_period_legs),
            "required_key_count": len(required_keys),
            "leg_scope": args.leg_scope,
        },
    )
    panel, feature_report = portfolio_rules.build_feature_panel(
        legs=period_legs,
        dates=dates,
        index=index,
        v1_portfolio=v1_portfolio,
        risk_floor=float(args.risk_floor),
    )
    panel_clocks = [int(clock) for clock in panel.keys() if int(clock) <= int(end_epoch)]
    period_mark_epoch = max(panel_clocks) if panel_clocks else end_epoch
    frame, outcome_report = build_outcome_frame(
        panel=panel,
        legs=period_legs,
        index=index,
        v1_portfolio=v1_portfolio,
        period_end_epoch=end_epoch,
        include_open_after_period=bool(args.include_open_after_period),
        period_mark_epoch=period_mark_epoch,
    )
    frame_path = output_dir / "continuation_opportunities.parquet"
    frame_storage = "parquet"
    try:
        frame.to_parquet(frame_path, index=False)
    except Exception as exc:
        frame_path = output_dir / "continuation_opportunities.pkl.gz"
        frame.to_pickle(frame_path, compression="gzip")
        frame_storage = f"pickle_gzip_after_parquet_error:{type(exc).__name__}"
    write_json(
        output_dir / "phase_progress.json",
        {
            "phase": "outcome_frame_built",
            "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "input_stream": input_report,
            "feature_panel": feature_report,
            "outcome_frame": outcome_report,
            "opportunity_frame_path": str(frame_path),
            "opportunity_frame_storage": frame_storage,
            "period_t2_leg_count": len(period_legs),
            "closed_period_t2_leg_count": len(closed_period_legs),
            "carry_in_t2_leg_count": len(carry_in_legs),
            "open_after_period_t2_leg_count": len(open_after_period_legs),
            "required_key_count": len(required_keys),
            "leg_scope": args.leg_scope,
            "include_open_after_period": bool(args.include_open_after_period),
            "period_mark_epoch": int(period_mark_epoch),
            "period_mark_time": base.epoch_ist_iso(period_mark_epoch),
        },
    )
    policies = continuation_policy_grid()
    if args.max_policies and args.max_policies > 0:
        policies = policies[: args.max_policies]
    summaries: list[dict[str, Any]] = []
    best_quality: dict[str, Any] | None = None
    best_objective: dict[str, Any] | None = None
    policy_started = time.monotonic()
    for idx, policy in enumerate(policies, start=1):
        selected = first_passing_rows(frame, policy)
        summary = summarize_selection(selected, policy, min_trades_for_quality=int(args.min_trades_for_quality))
        summaries.append(summary)
        if best_objective is None or float(summary.get("objective") or -10**9) > float(best_objective.get("objective") or -10**9):
            best_objective = summary
        if summary.get("quality_pass") and (
            best_quality is None or float(summary.get("objective") or -10**9) > float(best_quality.get("objective") or -10**9)
        ):
            best_quality = summary
        if idx % 100 == 0 or idx == len(policies):
            write_json(
                output_dir / "progress.json",
                {
                    "phase": "policy_scoring",
                    "completed_policies": idx,
                    "total_policies": len(policies),
                    "elapsed_seconds": round(time.monotonic() - policy_started, 3),
                    "best_quality_policy": best_quality.get("policy_name") if best_quality else None,
                    "best_quality_objective": best_quality.get("objective") if best_quality else None,
                    "best_objective_policy": best_objective.get("policy_name") if best_objective else None,
                    "best_objective": best_objective.get("objective") if best_objective else None,
                },
            )
    summaries.sort(key=lambda item: float(item.get("objective") or -10**9), reverse=True)
    quality_summaries = [row for row in summaries if row.get("quality_pass")]
    write_summary_csv(output_dir / "t2_continuation_filter_summary.csv", summaries)
    write_summary_csv(output_dir / "t2_continuation_quality_pass_summary.csv", quality_summaries)
    top_payload = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "root": str(root),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "definition": (
            "For each policy, each underlying T2 leg is entered once at its first qualifying 1-minute clock, "
            "then exited at that leg's actual T2 exit using the same futures fill/accounting model."
        ),
        "input_rule": {
            "source": "v2 quote-valid compact target stream cache",
            "score_cutoff": "all feature inputs use quotes at or before clock minus one minute; entry fill uses current clock",
            "portfolio": "none; standalone continuation filter only",
            "mutation": "read-only research; no production v2, Matrix, v1, or Compass writes",
        },
        "period_t2_leg_count": len(period_legs),
        "closed_period_t2_leg_count": len(closed_period_legs),
        "carry_in_t2_leg_count": len(carry_in_legs),
        "open_after_period_t2_leg_count": len(open_after_period_legs),
        "required_key_count": len(required_keys),
        "leg_scope": args.leg_scope,
        "include_open_after_period": bool(args.include_open_after_period),
        "period_mark_epoch": int(period_mark_epoch),
        "period_mark_time": base.epoch_ist_iso(period_mark_epoch),
        "policy_count": len(policies),
        "quality_pass_count": len(quality_summaries),
        "input_stream": input_report,
        "feature_panel": feature_report,
        "outcome_frame": outcome_report,
        "best_quality_policy": quality_summaries[0] if quality_summaries else None,
        "best_objective_policy": summaries[0] if summaries else None,
        "top_10": summaries[:10],
        "top_10_quality_pass": quality_summaries[:10],
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    write_json(output_dir / "final_report.json", top_payload)
    print(json.dumps(top_payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
