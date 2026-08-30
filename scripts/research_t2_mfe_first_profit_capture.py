#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_continuation_filters as continuation  # noqa: E402


SCHEMA = "obvfutport_v2.t2_mfe_first_profit_capture_research.v1"
SCORE_COLUMNS = ("score_risk_first", "score_smooth_survivor", "score_continuation", "score_cost_adjusted")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def metric_stats(values: list[float] | pd.Series) -> dict[str, Any]:
    clean = pd.Series(values, dtype="float64").dropna()
    clean = clean[clean.map(math.isfinite)]
    if clean.empty:
        return {"count": 0, "min": None, "p10": None, "median": None, "mean": None, "p90": None, "max": None}
    return {
        "count": int(clean.shape[0]),
        "sum": round(float(clean.sum()), 8),
        "min": round(float(clean.min()), 8),
        "p10": round(float(clean.quantile(0.10)), 8),
        "median": round(float(clean.median()), 8),
        "mean": round(float(clean.mean()), 8),
        "p90": round(float(clean.quantile(0.90)), 8),
        "max": round(float(clean.max()), 8),
    }


def truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def side_direction(side: str) -> float:
    return 1.0 if str(side).lower() == "long" else -1.0


def price_factor_from_directional_return(current_ret: float, side: str) -> float | None:
    direction = side_direction(side)
    factor = 1.0 + (float(current_ret) / direction)
    if not math.isfinite(factor) or factor <= 0:
        return None
    return factor


def directional_return_from_entry(current_ret: float, entry_current_ret: float, side: str) -> float | None:
    entry_factor = price_factor_from_directional_return(entry_current_ret, side)
    current_factor = price_factor_from_directional_return(current_ret, side)
    if entry_factor is None or current_factor is None:
        return None
    return side_direction(side) * ((current_factor / entry_factor) - 1.0)


def reconstructed_exit_price(entry_fill_price: float, side: str, forward_return: float) -> float:
    direction = side_direction(side)
    return float(entry_fill_price) * (1.0 + (float(forward_return) / direction))


def build_path_lookup(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    needed = [
        "row_id",
        "symbol",
        "side",
        "clock_epoch",
        "clock_time",
        "current_ret",
        *SCORE_COLUMNS,
    ]
    slim = frame.loc[:, [col for col in needed if col in frame.columns]].copy()
    slim["row_id"] = slim["row_id"].astype(str)
    slim["clock_epoch"] = pd.to_numeric(slim["clock_epoch"], errors="coerce")
    slim["current_ret"] = pd.to_numeric(slim["current_ret"], errors="coerce")
    slim = slim.dropna(subset=["clock_epoch", "current_ret"]).sort_values(["row_id", "clock_epoch"], kind="mergesort")
    return {row_id: group.reset_index(drop=True) for row_id, group in slim.groupby("row_id", sort=False)}


def forward_window(row: Any, path_lookup: dict[str, pd.DataFrame]) -> pd.DataFrame:
    row_id = str(row.row_id)
    path = path_lookup.get(row_id)
    if path is None or path.empty:
        return pd.DataFrame()
    entry_epoch = int(row.clock_epoch)
    exit_epoch = int(row.t2_exit_epoch)
    if exit_epoch <= entry_epoch:
        return pd.DataFrame()
    window = path.loc[(path["clock_epoch"] >= entry_epoch) & (path["clock_epoch"] <= exit_epoch)].copy()
    if window.empty:
        return window
    entry_ret = float(row.current_ret)
    side = str(row.side)
    forward_returns: list[float | None] = []
    for item in window.itertuples(index=False):
        forward_returns.append(directional_return_from_entry(float(item.current_ret), entry_ret, side))
    window["forward_return"] = forward_returns
    window = window.loc[pd.to_numeric(window["forward_return"], errors="coerce").notna()].copy()
    return window


def path_metrics(
    row: Any,
    path_lookup: dict[str, pd.DataFrame],
    mae_floor: float,
    *,
    include_window: bool,
) -> dict[str, Any]:
    window = forward_window(row, path_lookup)
    if window.empty:
        return {"ok": False, "reason": "missing_forward_window"}
    returns = pd.to_numeric(window["forward_return"], errors="coerce")
    if returns.empty:
        return {"ok": False, "reason": "missing_forward_returns"}
    max_value = float(returns.max())
    min_value = float(returns.min())
    mfe = max(0.0, max_value)
    mae_abs = max(0.0, -min_value)
    max_rows = window.loc[returns == max_value]
    min_rows = window.loc[returns == min_value]
    mfe_epoch = int(max_rows.iloc[0]["clock_epoch"]) if mfe > 0 and not max_rows.empty else None
    mae_epoch = int(min_rows.iloc[0]["clock_epoch"]) if mae_abs > 0 and not min_rows.empty else None
    if mfe <= 0:
        ordering = "no_mfe"
        mfe_before_mae = False
    elif mae_abs <= 0:
        ordering = "mfe_no_mae"
        mfe_before_mae = True
    elif mfe_epoch is not None and mae_epoch is not None and mfe_epoch < mae_epoch:
        ordering = "mfe_before_mae"
        mfe_before_mae = True
    elif mfe_epoch is not None and mae_epoch is not None and mfe_epoch == mae_epoch:
        ordering = "mfe_mae_same_clock"
        mfe_before_mae = False
    else:
        ordering = "mae_before_mfe"
        mfe_before_mae = False
    out = {
        "ok": True,
        "row_id": str(row.row_id),
        "symbol": str(row.symbol),
        "side": str(row.side),
        "qualification_epoch": int(row.clock_epoch),
        "t2_exit_epoch": int(row.t2_exit_epoch),
        "t2_actual_exit_epoch": base.as_int(getattr(row, "t2_actual_exit_epoch", None)),
        "t2_status": str(getattr(row, "t2_status", "closed") or "closed"),
        "open_at_period_end": truthy_flag(getattr(row, "open_at_period_end", False)),
        "entry_fill_price": float(row.entry_fill_price),
        "margin_per_lot": float(row.margin_per_lot),
        "lot_size": int(row.lot_size or 1),
        "mfe": mfe,
        "mae_abs": mae_abs,
        "mfe_mae_ratio": mfe / max(mae_abs, mae_floor),
        "mfe_epoch": mfe_epoch,
        "mae_epoch": mae_epoch,
        "mfe_before_mae": mfe_before_mae,
        "mfe_mae_ordering": ordering,
        "exit_return": float(returns.iloc[-1]),
        "duration_minutes": (int(window.iloc[-1]["clock_epoch"]) - int(row.clock_epoch)) / 60.0,
    }
    if include_window:
        out["window"] = window
    return out


def policy_path_rows(
    frame: pd.DataFrame,
    path_lookup: dict[str, pd.DataFrame],
    policy: continuation.ContinuationPolicy,
    mae_floor: float,
    *,
    include_window: bool,
) -> list[dict[str, Any]]:
    selected = continuation.first_passing_rows(frame, policy)
    rows: list[dict[str, Any]] = []
    for row in selected.itertuples(index=False):
        metrics = path_metrics(row, path_lookup, mae_floor, include_window=include_window)
        if metrics.get("ok"):
            rows.append(metrics)
    return rows


def summarize_path_rows(policy: continuation.ContinuationPolicy, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "policy_name": policy.name,
            "selected_legs": 0,
            "measured_legs": 0,
            **asdict(policy),
        }
    mfe_first_count = sum(1 for row in rows if row.get("mfe_before_mae"))
    ordering_counts: dict[str, int] = {}
    for row in rows:
        ordering = str(row.get("mfe_mae_ordering") or "unknown")
        ordering_counts[ordering] = ordering_counts.get(ordering, 0) + 1
    return {
        "policy_name": policy.name,
        "selected_legs": len(rows),
        "measured_legs": len(rows),
        "unique_symbols": len({str(row["symbol"]) for row in rows}),
        "mfe_before_mae_count": mfe_first_count,
        "mfe_before_mae_rate_pct": round(mfe_first_count / len(rows) * 100.0, 4),
        "ordering_counts": ordering_counts,
        "mfe": metric_stats([float(row["mfe"]) for row in rows]),
        "mae_abs": metric_stats([float(row["mae_abs"]) for row in rows]),
        "mfe_mae_ratio": metric_stats([float(row["mfe_mae_ratio"]) for row in rows]),
        "exit_return": metric_stats([float(row["exit_return"]) for row in rows]),
        "duration_minutes": metric_stats([float(row["duration_minutes"]) for row in rows]),
        **asdict(policy),
    }


def overlay_configs() -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = [{"name": "hold_to_t2_exit", "kind": "hold"}]
    for target in (0.0025, 0.0050, 0.0075, 0.0100, 0.0150):
        configs.append({"name": f"profit_{int(target * 10000)}bps", "kind": "profit", "target": target})
    for target in (0.0025, 0.0050, 0.0075, 0.0100):
        for giveback in (0.25, 0.40, 0.50):
            configs.append(
                {
                    "name": f"profit_{int(target * 10000)}bps_trail_giveback{int(giveback * 100)}",
                    "kind": "profit_trail",
                    "target": target,
                    "giveback": giveback,
                }
            )
    for target in (0.0050, 0.0075, 0.0100):
        for decay_drop in (0.10, 0.20):
            for min_hold in (15, 30):
                configs.append(
                    {
                        "name": f"profit_{int(target * 10000)}bps_or_score_decay{int(decay_drop * 100)}_h{min_hold}",
                        "kind": "profit_or_decay",
                        "target": target,
                        "decay_drop": decay_drop,
                        "min_hold_minutes": min_hold,
                    }
                )
    for target in (0.0050, 0.0075, 0.0100):
        for giveback in (0.40, 0.50):
            for decay_drop in (0.10, 0.20):
                configs.append(
                    {
                        "name": (
                            f"profit_{int(target * 10000)}bps_trail{int(giveback * 100)}"
                            f"_or_decay{int(decay_drop * 100)}"
                        ),
                        "kind": "profit_trail_or_decay",
                        "target": target,
                        "giveback": giveback,
                        "decay_drop": decay_drop,
                        "min_hold_minutes": 15,
                    }
                )
    for target in (0.0025, 0.0050):
        for wait in (15, 30, 60, 90):
            configs.append(
                {
                    "name": f"profit_{int(target * 10000)}bps_timeout{wait}m",
                    "kind": "profit_or_timeout",
                    "target": target,
                    "max_wait_minutes": wait,
                }
            )
            for floor in (0.0, target * 0.25):
                floor_label = "flat" if floor <= 0 else "quarter_target"
                configs.append(
                    {
                        "name": f"profit_{int(target * 10000)}bps_fail{wait}m_{floor_label}",
                        "kind": "profit_or_failure",
                        "target": target,
                        "max_wait_minutes": wait,
                        "failure_floor": floor,
                    }
                )
            for stop in (0.0025, 0.0050):
                configs.append(
                    {
                        "name": f"profit_{int(target * 10000)}bps_stop{int(stop * 10000)}bps_fail{wait}m_flat",
                        "kind": "profit_stop_or_failure",
                        "target": target,
                        "max_wait_minutes": wait,
                        "failure_floor": 0.0,
                        "hard_stop": stop,
                    }
                )
    return configs


def choose_overlay_exit(row: dict[str, Any], config: dict[str, Any], score_column: str, min_score: float) -> dict[str, Any]:
    window = row["window"]
    returns = pd.to_numeric(window["forward_return"], errors="coerce")
    clocks = pd.to_numeric(window["clock_epoch"], errors="coerce")
    entry_epoch = int(row["qualification_epoch"])
    peak = 0.0
    armed = False
    target = float(config.get("target") or 0.0)
    giveback = float(config.get("giveback") or 0.0)
    decay_floor = max(0.0, float(min_score) - float(config.get("decay_drop") or 0.0))
    min_hold_seconds = int(config.get("min_hold_minutes") or 0) * 60
    max_wait_seconds = int(config.get("max_wait_minutes") or 0) * 60
    failure_floor = float(config.get("failure_floor") or 0.0)
    hard_stop = config.get("hard_stop")
    hard_stop_value = float(hard_stop) if hard_stop is not None else None
    exit_idx = len(window) - 1
    exit_reason = "underlying_t2_exit"
    for idx, (clock, ret) in enumerate(zip(clocks, returns)):
        if not math.isfinite(float(ret)):
            continue
        clock_int = int(clock)
        ret_float = float(ret)
        peak = max(peak, ret_float)
        kind = str(config["kind"])
        if kind == "hold":
            continue
        if kind == "profit" and ret_float >= target:
            exit_idx = idx
            exit_reason = "profit_capture"
            break
        if kind in {"profit_or_timeout", "profit_or_failure", "profit_stop_or_failure"}:
            if ret_float >= target:
                exit_idx = idx
                exit_reason = "profit_capture"
                break
            if hard_stop_value is not None and ret_float <= -hard_stop_value:
                exit_idx = idx
                exit_reason = "adverse_stop"
                break
            if max_wait_seconds > 0 and clock_int - entry_epoch >= max_wait_seconds:
                if kind == "profit_or_timeout" or ret_float <= failure_floor:
                    exit_idx = idx
                    exit_reason = "target_timeout_failure"
                    break
        if kind in {"profit_trail", "profit_trail_or_decay"}:
            if peak >= target:
                armed = True
            if armed and ret_float <= peak * (1.0 - giveback):
                exit_idx = idx
                exit_reason = "profit_trail_giveback"
                break
        if kind in {"profit_or_decay", "profit_trail_or_decay"}:
            if ret_float >= target:
                exit_idx = idx
                exit_reason = "profit_capture"
                break
            if clock_int - entry_epoch >= min_hold_seconds and score_column in window.columns:
                score = base.as_float(window.iloc[idx].get(score_column))
                if score is not None and score <= decay_floor:
                    exit_idx = idx
                    exit_reason = "score_decay"
                    break
    exit_row = window.iloc[exit_idx]
    exit_return = float(exit_row["forward_return"])
    return {
        "exit_epoch": int(exit_row["clock_epoch"]),
        "exit_reason": exit_reason,
        "exit_return": exit_return,
        "exit_duration_minutes": (int(exit_row["clock_epoch"]) - entry_epoch) / 60.0,
        "exit_price": reconstructed_exit_price(float(row["entry_fill_price"]), str(row["side"]), exit_return),
    }


def overlay_accounting(v1_portfolio: Any, row: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    entry = float(row["entry_fill_price"])
    exit_price = float(overlay["exit_price"])
    side = str(row["side"])
    lot_size = int(row.get("lot_size") or 1)
    acct = v1_portfolio.futures_trade_accounting(
        side=side,
        entry_fill_price=entry,
        exit_fill_price=exit_price,
        lot_size=lot_size,
        lots=1,
        point_config=None,
    )
    net = float(acct.get("net_rupees") or 0.0)
    gross = float(acct.get("gross_rupees") or 0.0)
    charges = float(acct.get("charges_rupees") or 0.0)
    margin = float(row.get("margin_per_lot") or 0.0)
    return {
        "net_rupees": net,
        "gross_rupees": gross,
        "charges_rupees": charges,
        "net_return_on_margin_pct": (net / margin * 100.0) if margin > 0 else None,
    }


def summarize_overlay(
    policy_summary: dict[str, Any],
    rows: list[dict[str, Any]],
    config: dict[str, Any],
    policy: continuation.ContinuationPolicy,
    v1_portfolio: Any,
    *,
    oracle_mfe_first_only: bool,
) -> dict[str, Any]:
    usable = [row for row in rows if (row.get("mfe_before_mae") or not oracle_mfe_first_only)]
    if not usable:
        return {
            "policy_name": policy.name,
            "overlay": config["name"],
            "oracle_mfe_first_only": oracle_mfe_first_only,
            "trade_count": 0,
        }
    score_column = f"score_{policy.formula}"
    exits = [choose_overlay_exit(row, config, score_column, policy.min_score) for row in usable]
    accounts = [overlay_accounting(v1_portfolio, row, exit_row) for row, exit_row in zip(usable, exits)]
    net_values = [float(item["net_rupees"]) for item in accounts]
    margin_values = [float(item["net_return_on_margin_pct"]) for item in accounts if item.get("net_return_on_margin_pct") is not None]
    exit_returns = [float(item["exit_return"]) for item in exits]
    exit_reasons: dict[str, int] = {}
    for item in exits:
        reason = str(item.get("exit_reason") or "unknown")
        exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    wins = sum(1 for value in net_values if value > 0)
    return {
        "policy_name": policy.name,
        "overlay": config["name"],
        "oracle_mfe_first_only": oracle_mfe_first_only,
        "trade_count": len(usable),
        "unique_symbols": len({str(row["symbol"]) for row in usable}),
        "mfe_before_mae_rate_pct": policy_summary.get("mfe_before_mae_rate_pct"),
        "policy_median_mfe_mae_ratio": (policy_summary.get("mfe_mae_ratio") or {}).get("median"),
        "policy_p10_mfe_mae_ratio": (policy_summary.get("mfe_mae_ratio") or {}).get("p10"),
        "success_rate_pct": round(wins / len(usable) * 100.0, 4),
        "net_rupees": metric_stats(net_values),
        "net_return_on_margin_pct": metric_stats(margin_values),
        "exit_return": metric_stats(exit_returns),
        "exit_duration_minutes": metric_stats([float(item["exit_duration_minutes"]) for item in exits]),
        "exit_reasons": exit_reasons,
        **{f"overlay_{key}": value for key, value in config.items() if key != "name"},
    }


def flatten_policy(row: dict[str, Any]) -> dict[str, Any]:
    ratio = row.get("mfe_mae_ratio") or {}
    mfe = row.get("mfe") or {}
    mae = row.get("mae_abs") or {}
    exit_return = row.get("exit_return") or {}
    return {
        "policy_name": row.get("policy_name"),
        "measured_legs": row.get("measured_legs"),
        "unique_symbols": row.get("unique_symbols"),
        "mfe_before_mae_rate_pct": row.get("mfe_before_mae_rate_pct"),
        "mfe_before_mae_count": row.get("mfe_before_mae_count"),
        "mean_mfe_mae_ratio": ratio.get("mean"),
        "median_mfe_mae_ratio": ratio.get("median"),
        "p10_mfe_mae_ratio": ratio.get("p10"),
        "min_mfe_mae_ratio": ratio.get("min"),
        "mean_mfe": mfe.get("mean"),
        "median_mfe": mfe.get("median"),
        "mean_mae_abs": mae.get("mean"),
        "median_mae_abs": mae.get("median"),
        "median_exit_return": exit_return.get("median"),
        "formula": row.get("formula"),
        "min_score": row.get("min_score"),
        "min_age_minutes": row.get("min_age_minutes"),
        "max_age_minutes": row.get("max_age_minutes"),
        "min_current_ret": row.get("min_current_ret"),
        "min_mfe": row.get("min_mfe"),
        "max_mae_abs": row.get("max_mae_abs"),
        "max_drawdown_to_mfe": row.get("max_drawdown_to_mfe"),
        "min_positive_ram_count": row.get("min_positive_ram_count"),
        "max_spread_bps": row.get("max_spread_bps"),
        "min_edge_cost_multiple": row.get("min_edge_cost_multiple"),
        "min_minutes_to_session_end": row.get("min_minutes_to_session_end"),
        "ordering_counts": json.dumps(row.get("ordering_counts") or {}, sort_keys=True),
    }


def flatten_overlay(row: dict[str, Any]) -> dict[str, Any]:
    net = row.get("net_rupees") or {}
    margin = row.get("net_return_on_margin_pct") or {}
    exit_ret = row.get("exit_return") or {}
    duration = row.get("exit_duration_minutes") or {}
    return {
        "policy_name": row.get("policy_name"),
        "overlay": row.get("overlay"),
        "oracle_mfe_first_only": row.get("oracle_mfe_first_only"),
        "trade_count": row.get("trade_count"),
        "unique_symbols": row.get("unique_symbols"),
        "mfe_before_mae_rate_pct": row.get("mfe_before_mae_rate_pct"),
        "policy_median_mfe_mae_ratio": row.get("policy_median_mfe_mae_ratio"),
        "policy_p10_mfe_mae_ratio": row.get("policy_p10_mfe_mae_ratio"),
        "success_rate_pct": row.get("success_rate_pct"),
        "total_net_rupees": net.get("sum"),
        "mean_net_rupees": net.get("mean"),
        "median_net_rupees": net.get("median"),
        "min_net_rupees": net.get("min"),
        "mean_net_return_on_margin_pct": margin.get("mean"),
        "median_net_return_on_margin_pct": margin.get("median"),
        "min_net_return_on_margin_pct": margin.get("min"),
        "mean_exit_return": exit_ret.get("mean"),
        "median_exit_return": exit_ret.get("median"),
        "min_exit_return": exit_ret.get("min"),
        "mean_exit_duration_minutes": duration.get("mean"),
        "median_exit_duration_minutes": duration.get("median"),
        "exit_reasons": json.dumps(row.get("exit_reasons") or {}, sort_keys=True),
        "overlay_kind": row.get("overlay_kind"),
        "overlay_target": row.get("overlay_target"),
        "overlay_giveback": row.get("overlay_giveback"),
        "overlay_decay_drop": row.get("overlay_decay_drop"),
        "overlay_min_hold_minutes": row.get("overlay_min_hold_minutes"),
        "overlay_max_wait_minutes": row.get("overlay_max_wait_minutes"),
        "overlay_failure_floor": row.get("overlay_failure_floor"),
        "overlay_hard_stop": row.get("overlay_hard_stop"),
    }


def write_csv(path: Path, rows: list[dict[str, Any]], flattener: Any) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    flat = [flattener(row) for row in rows]
    columns = list(flat[0].keys())
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat)
    tmp.replace(path)


def policy_rank_key(row: dict[str, Any]) -> tuple[float, float, float, float, int]:
    ratio = row.get("mfe_mae_ratio") or {}
    return (
        float(row.get("mfe_before_mae_rate_pct") or -1e9),
        float(ratio.get("p10") or -1e9),
        float(ratio.get("median") or -1e9),
        float(ratio.get("mean") or -1e9),
        int(row.get("measured_legs") or 0),
    )


def overlay_rank_key(row: dict[str, Any]) -> tuple[float, float, float, float, int]:
    margin = row.get("net_return_on_margin_pct") or {}
    return (
        float(row.get("success_rate_pct") or -1e9),
        float(margin.get("median") or -1e9),
        float(margin.get("mean") or -1e9),
        float((row.get("net_rupees") or {}).get("sum") or -1e18),
        int(row.get("trade_count") or 0),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mae-floor", type=float, default=0.0005)
    parser.add_argument("--min-overlay-legs", type=int, default=20)
    parser.add_argument("--max-overlay-policies", type=int, default=80)
    parser.add_argument("--max-policies", type=int, default=0)
    args = parser.parse_args()

    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    base.add_paths(args.root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    frame = pd.read_parquet(args.opportunity_frame)
    path_lookup = build_path_lookup(frame)
    policies = continuation.continuation_policy_grid()
    if args.max_policies > 0:
        policies = policies[: args.max_policies]
    policy_summaries: list[dict[str, Any]] = []
    policy_by_name: dict[str, continuation.ContinuationPolicy] = {}
    for idx, policy in enumerate(policies, start=1):
        rows = policy_path_rows(frame, path_lookup, policy, args.mae_floor, include_window=False)
        summary = summarize_path_rows(policy, rows)
        policy_summaries.append(summary)
        policy_by_name[policy.name] = policy
        if idx % 100 == 0 or idx == len(policies):
            write_json(
                args.output_dir / "progress.json",
                {
                    "phase": "mfe_first_policy_ranking",
                    "completed_policies": idx,
                    "total_policies": len(policies),
                    "latest_policy": policy.name,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                },
            )
    ranked_policies = sorted(
        [row for row in policy_summaries if int(row.get("measured_legs") or 0) >= args.min_overlay_legs],
        key=policy_rank_key,
        reverse=True,
    )
    overlay_policy_names = [row["policy_name"] for row in ranked_policies[: args.max_overlay_policies]]
    configs = overlay_configs()
    overlay_rows: list[dict[str, Any]] = []
    overlay_started = time.monotonic()
    total_overlay = len(overlay_policy_names) * len(configs) * 2
    done_overlay = 0
    summary_by_policy = {row["policy_name"]: row for row in policy_summaries}
    for policy_name in overlay_policy_names:
        policy = policy_by_name[policy_name]
        rows = policy_path_rows(frame, path_lookup, policy, args.mae_floor, include_window=True)
        summary = summary_by_policy[policy_name]
        for config in configs:
            for oracle in (False, True):
                overlay_rows.append(
                    summarize_overlay(
                        summary,
                        rows,
                        config,
                        policy,
                        v1_portfolio,
                        oracle_mfe_first_only=oracle,
                    )
                )
                done_overlay += 1
        write_json(
            args.output_dir / "progress.json",
            {
                "phase": "profit_capture_overlay",
                "completed_overlay_cases": done_overlay,
                "total_overlay_cases": total_overlay,
                "latest_policy": policy_name,
                "elapsed_seconds": round(time.monotonic() - overlay_started, 3),
            },
        )
    overlay_rows = [row for row in overlay_rows if int(row.get("trade_count") or 0) > 0]
    ranked_overlays_all = sorted(
        [row for row in overlay_rows if not row.get("oracle_mfe_first_only") and int(row.get("trade_count") or 0) >= args.min_overlay_legs],
        key=overlay_rank_key,
        reverse=True,
    )
    ranked_overlays_oracle = sorted(
        [row for row in overlay_rows if row.get("oracle_mfe_first_only") and int(row.get("trade_count") or 0) >= args.min_overlay_legs],
        key=overlay_rank_key,
        reverse=True,
    )
    write_csv(args.output_dir / "t2_mfe_first_policy_summary.csv", policy_summaries, flatten_policy)
    write_csv(args.output_dir / "t2_profit_capture_overlay_summary.csv", overlay_rows, flatten_overlay)
    report = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "opportunity_frame": str(args.opportunity_frame),
        "output_dir": str(args.output_dir),
        "definition": {
            "selection": "first clock where a T2 leg passes a tested continuation policy; this uses point-in-time features only",
            "mfe_before_mae": "true when forward MFE occurs before forward MAE after qualification, or when there is MFE with no adverse excursion",
            "overlay_all_selected": "live-implementable backtest of profit/decay exits on every selected leg under that policy",
            "overlay_mfe_first_only": "oracle diagnostic on the historical MFE-first subset; useful for edge discovery, not directly live-implementable",
            "accounting": "per-lot futures accounting using reconstructed exit price from direction-adjusted compact path returns",
        },
        "input_rows": int(frame.shape[0]),
        "input_unique_legs": int(frame["row_id"].nunique()) if "row_id" in frame else None,
        "policy_count": len(policies),
        "overlay_policy_count": len(overlay_policy_names),
        "overlay_config_count": len(configs),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "top_policies_mfe_first_min20": [flatten_policy(row) for row in ranked_policies[:20]],
        "top_live_implementable_overlays": [flatten_overlay(row) for row in ranked_overlays_all[:20]],
        "top_oracle_mfe_first_overlays": [flatten_overlay(row) for row in ranked_overlays_oracle[:20]],
    }
    write_json(args.output_dir / "final_report.json", report)
    print(
        json.dumps(
            {
                "schema": SCHEMA,
                "output_dir": str(args.output_dir),
                "policy_count": len(policies),
                "overlay_policy_count": len(overlay_policy_names),
                "overlay_config_count": len(configs),
                "elapsed_seconds": report["elapsed_seconds"],
                "top_policy": report["top_policies_mfe_first_min20"][:1],
                "top_live_overlay": report["top_live_implementable_overlays"][:3],
                "top_oracle_overlay": report["top_oracle_mfe_first_overlays"][:3],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
