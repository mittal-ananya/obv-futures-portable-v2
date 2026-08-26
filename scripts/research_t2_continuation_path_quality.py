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

import research_t2_continuation_filters as continuation  # noqa: E402


SCHEMA = "obvfutport_v2.t2_continuation_path_quality.v1"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def metric_stats(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return {"count": 0, "min": None, "p10": None, "median": None, "mean": None, "p90": None, "max": None}
    return {
        "count": int(clean.shape[0]),
        "min": round(float(clean.min()), 8),
        "p10": round(float(clean.quantile(0.10)), 8),
        "median": round(float(clean.median()), 8),
        "mean": round(float(clean.mean()), 8),
        "p90": round(float(clean.quantile(0.90)), 8),
        "max": round(float(clean.max()), 8),
    }


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


def build_path_lookup(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    needed = ["row_id", "side", "clock_epoch", "clock_time", "current_ret"]
    slim = frame.loc[:, needed].copy()
    slim["row_id"] = slim["row_id"].astype(str)
    slim["clock_epoch"] = pd.to_numeric(slim["clock_epoch"], errors="coerce")
    slim["current_ret"] = pd.to_numeric(slim["current_ret"], errors="coerce")
    slim = slim.dropna(subset=["clock_epoch", "current_ret"]).sort_values(["row_id", "clock_epoch"], kind="mergesort")
    return {row_id: group.reset_index(drop=True) for row_id, group in slim.groupby("row_id", sort=False)}


def forward_path_quality(row: Any, path_lookup: dict[str, pd.DataFrame], mae_floor: float) -> dict[str, Any]:
    row_id = str(row.row_id)
    path = path_lookup.get(row_id)
    if path is None or path.empty:
        return {"ok": False, "reason": "missing_path"}
    entry_epoch = int(row.clock_epoch)
    exit_epoch = int(row.t2_exit_epoch)
    if exit_epoch <= entry_epoch:
        return {"ok": False, "reason": "non_positive_forward_window"}
    window = path.loc[(path["clock_epoch"] >= entry_epoch) & (path["clock_epoch"] <= exit_epoch)]
    if window.empty:
        return {"ok": False, "reason": "empty_forward_window"}
    entry_ret = float(row.current_ret)
    side = str(row.side)
    returns: list[float] = []
    for item in window.itertuples(index=False):
        value = directional_return_from_entry(float(item.current_ret), entry_ret, side)
        if value is not None and math.isfinite(value):
            returns.append(value)
    if not returns:
        return {"ok": False, "reason": "missing_forward_returns"}
    mfe = max(0.0, max(returns))
    mae_abs = max(0.0, -min(returns))
    ratio = mfe / max(mae_abs, mae_floor)
    return {
        "ok": True,
        "forward_mfe": mfe,
        "forward_mae_abs": mae_abs,
        "forward_mfe_mae_ratio": ratio,
        "forward_exit_return": returns[-1],
        "forward_point_count": len(returns),
        "forward_duration_minutes": (exit_epoch - entry_epoch) / 60.0,
        "zero_forward_mae": mae_abs <= 0.0,
    }


def summarize_policy(
    frame: pd.DataFrame,
    path_lookup: dict[str, pd.DataFrame],
    policy: continuation.ContinuationPolicy,
    mae_floor: float,
) -> dict[str, Any]:
    selected = continuation.first_passing_rows(frame, policy)
    if selected.empty:
        return {
            "policy_name": policy.name,
            "selected_legs": 0,
            "quality_reason": "no_selected_legs",
            **asdict(policy),
        }
    forward_rows: list[dict[str, Any]] = []
    skip_reasons: dict[str, int] = {}
    for row in selected.itertuples(index=False):
        quality = forward_path_quality(row, path_lookup, mae_floor)
        if not quality.get("ok"):
            reason = str(quality.get("reason") or "unknown")
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        else:
            forward_rows.append(quality)
    if not forward_rows:
        return {
            "policy_name": policy.name,
            "selected_legs": int(selected.shape[0]),
            "forward_measured_legs": 0,
            "quality_reason": "no_forward_paths",
            "forward_skip_reasons": skip_reasons,
            **asdict(policy),
        }
    forward = pd.DataFrame.from_records(forward_rows)
    mfe = pd.to_numeric(forward["forward_mfe"], errors="coerce")
    mae_abs = pd.to_numeric(forward["forward_mae_abs"], errors="coerce")
    ratio = pd.to_numeric(forward["forward_mfe_mae_ratio"], errors="coerce")
    exit_ret = pd.to_numeric(forward["forward_exit_return"], errors="coerce")
    duration = pd.to_numeric(forward["forward_duration_minutes"], errors="coerce")
    no_mae_count = int((mae_abs <= 0).sum())
    closed_wins = pd.to_numeric(selected["net_rupees"], errors="coerce") > 0
    return {
        "policy_name": policy.name,
        "selected_legs": int(selected.shape[0]),
        "forward_measured_legs": int(forward.shape[0]),
        "forward_skip_reasons": skip_reasons,
        "unique_symbols": int(selected["symbol"].nunique()) if "symbol" in selected else None,
        "long_count": int((selected["side"] == "long").sum()) if "side" in selected else None,
        "short_count": int((selected["side"] == "short").sum()) if "side" in selected else None,
        "closed_success_rate_pct": round(float(closed_wins.mean() * 100.0), 4),
        "forward_mfe": metric_stats(mfe),
        "forward_mae_abs": metric_stats(mae_abs),
        "forward_exit_return": metric_stats(exit_ret),
        "forward_duration_minutes": metric_stats(duration),
        "forward_mfe_mae_ratio": metric_stats(ratio),
        "forward_min_mfe_mae_ratio": round(float(ratio.min()), 8),
        "forward_p10_mfe_mae_ratio": round(float(ratio.quantile(0.10)), 8),
        "ratio_ge_1_pct": round(float((ratio >= 1.0).mean() * 100.0), 4),
        "ratio_ge_2_pct": round(float((ratio >= 2.0).mean() * 100.0), 4),
        "ratio_ge_3_pct": round(float((ratio >= 3.0).mean() * 100.0), 4),
        "zero_forward_mae_count": no_mae_count,
        "mean_net_return_on_margin_pct": round(
            float(pd.to_numeric(selected["net_return_on_margin_pct"], errors="coerce").mean()), 8
        ),
        "median_net_return_on_margin_pct": round(
            float(pd.to_numeric(selected["net_return_on_margin_pct"], errors="coerce").median()), 8
        ),
        "total_net_rupees_per_lot": round(float(pd.to_numeric(selected["net_rupees"], errors="coerce").sum()), 4),
        **asdict(policy),
    }


def flatten(row: dict[str, Any]) -> dict[str, Any]:
    ratio = row.get("forward_mfe_mae_ratio") or {}
    mfe = row.get("forward_mfe") or {}
    mae = row.get("forward_mae_abs") or {}
    exit_ret = row.get("forward_exit_return") or {}
    duration = row.get("forward_duration_minutes") or {}
    return {
        "policy_name": row.get("policy_name"),
        "selected_legs": row.get("selected_legs"),
        "forward_measured_legs": row.get("forward_measured_legs"),
        "unique_symbols": row.get("unique_symbols"),
        "closed_success_rate_pct": row.get("closed_success_rate_pct"),
        "mean_mfe_mae_ratio": ratio.get("mean"),
        "median_mfe_mae_ratio": ratio.get("median"),
        "min_mfe_mae_ratio": ratio.get("min"),
        "p10_mfe_mae_ratio": ratio.get("p10"),
        "p90_mfe_mae_ratio": ratio.get("p90"),
        "max_mfe_mae_ratio": ratio.get("max"),
        "mean_mfe": mfe.get("mean"),
        "median_mfe": mfe.get("median"),
        "min_mfe": mfe.get("min"),
        "mean_mae_abs": mae.get("mean"),
        "median_mae_abs": mae.get("median"),
        "max_mae_abs": mae.get("max"),
        "mean_forward_exit_return": exit_ret.get("mean"),
        "median_forward_exit_return": exit_ret.get("median"),
        "min_forward_exit_return": exit_ret.get("min"),
        "mean_forward_duration_minutes": duration.get("mean"),
        "median_forward_duration_minutes": duration.get("median"),
        "ratio_ge_1_pct": row.get("ratio_ge_1_pct"),
        "ratio_ge_2_pct": row.get("ratio_ge_2_pct"),
        "ratio_ge_3_pct": row.get("ratio_ge_3_pct"),
        "zero_forward_mae_count": row.get("zero_forward_mae_count"),
        "mean_net_return_on_margin_pct": row.get("mean_net_return_on_margin_pct"),
        "median_net_return_on_margin_pct": row.get("median_net_return_on_margin_pct"),
        "total_net_rupees_per_lot": row.get("total_net_rupees_per_lot"),
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
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = list(flatten(rows[0]).keys()) if rows else []
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(flatten(row))
    tmp.replace(path)


def top_by(rows: list[dict[str, Any]], stat_key: str, min_legs: int, limit: int) -> list[dict[str, Any]]:
    def score(row: dict[str, Any]) -> tuple[float, float, float, int]:
        ratio = row.get("forward_mfe_mae_ratio") or {}
        value = ratio.get(stat_key)
        if value is None:
            value = -1e9
        median_ret = row.get("median_net_return_on_margin_pct")
        if median_ret is None:
            median_ret = -1e9
        success = row.get("closed_success_rate_pct")
        if success is None:
            success = -1e9
        return (float(value), float(median_ret), float(success), int(row.get("selected_legs") or 0))

    selected = [row for row in rows if int(row.get("forward_measured_legs") or 0) >= min_legs]
    return [flatten(row) for row in sorted(selected, key=score, reverse=True)[:limit]]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mae-floor", type=float, default=0.0005)
    parser.add_argument("--report-min-legs", default="10,20,50,100")
    parser.add_argument("--max-policies", type=int, default=0)
    args = parser.parse_args()

    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.read_parquet(args.opportunity_frame)
    path_lookup = build_path_lookup(frame)
    policies = continuation.continuation_policy_grid()
    if args.max_policies > 0:
        policies = policies[: args.max_policies]
    rows: list[dict[str, Any]] = []
    for idx, policy in enumerate(policies, start=1):
        rows.append(summarize_policy(frame, path_lookup, policy, args.mae_floor))
        if idx % 100 == 0 or idx == len(policies):
            write_json(
                args.output_dir / "progress.json",
                {
                    "phase": "continuation_path_quality",
                    "completed_policies": idx,
                    "total_policies": len(policies),
                    "latest_policy": policy.name,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                },
            )
    rows.sort(
        key=lambda row: (
            (row.get("qualification_mfe_mae_ratio") or {}).get("mean") or -1e9,
            (row.get("qualification_mfe_mae_ratio") or {}).get("min") or -1e9,
            int(row.get("selected_legs") or 0),
        ),
        reverse=True,
    )
    write_csv(args.output_dir / "t2_continuation_path_quality_summary.csv", rows)
    min_legs = [int(part.strip()) for part in args.report_min_legs.split(",") if part.strip()]
    report = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "opportunity_frame": str(args.opportunity_frame),
        "output_dir": str(args.output_dir),
        "definition": {
            "qualification": "first 1-minute clock where the T2 leg passes the continuation policy",
            "forward_mfe_mae_ratio": "MFE after qualification / max(MAE_abs after qualification, mae_floor)",
            "mae_floor": args.mae_floor,
            "scope": "point-in-time selection rule diagnostic; future path is used only for historical outcome measurement, not for selection",
        },
        "input_rows": int(frame.shape[0]),
        "input_unique_legs": int(frame["row_id"].nunique()) if "row_id" in frame else None,
        "policy_count": len(policies),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "rankings": {},
    }
    for min_count in min_legs:
        report["rankings"][f"min_legs_{min_count}"] = {
            "top_by_mean_mfe_mae": top_by(rows, "mean", min_count, 15),
            "top_by_median_mfe_mae": top_by(rows, "median", min_count, 15),
            "top_by_min_mfe_mae": top_by(rows, "min", min_count, 15),
            "top_by_p10_mfe_mae": top_by(rows, "p10", min_count, 15),
        }
    write_json(args.output_dir / "final_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
