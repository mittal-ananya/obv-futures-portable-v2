#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_continuation_filters as continuation  # noqa: E402
import research_t2_mfe_first_profit_capture as overlay_research  # noqa: E402


SCHEMA = "obvfutport_v2.selected_t2_custom_exit_candidate_audit.v1"
IST = timezone(timedelta(hours=5, minutes=30))


POLICIES = [
    "smooth_survivor_mild_score0p90_age60to240_runway0",
    "smooth_survivor_balanced_score0p90_age30to300_runway45",
    "smooth_survivor_strict_score0p90_age30to300_runway45",
    "risk_first_edge_strict_score0p80_age90to300_runway45",
]

CUSTOM_EXITS = [
    {"name": "armed20_floor80", "kind": "armed_peak_floor", "arm_target": 0.0020, "floor_fraction": 0.80},
    {
        "name": "armed20_floor80_stop100",
        "kind": "armed_peak_floor",
        "arm_target": 0.0020,
        "floor_fraction": 0.80,
        "hard_stop": 0.0100,
    },
    {
        "name": "armed20_floor80_stop100_fail90",
        "kind": "armed_peak_floor",
        "arm_target": 0.0020,
        "floor_fraction": 0.80,
        "hard_stop": 0.0100,
        "max_wait_minutes": 90,
        "failure_floor": 0.0,
    },
    {"name": "armed20_floor65", "kind": "armed_peak_floor", "arm_target": 0.0020, "floor_fraction": 0.65},
    {
        "name": "armed20_floor65_stop100",
        "kind": "armed_peak_floor",
        "arm_target": 0.0020,
        "floor_fraction": 0.65,
        "hard_stop": 0.0100,
    },
    {"name": "armed50_floor50", "kind": "armed_peak_floor", "arm_target": 0.0050, "floor_fraction": 0.50},
    {
        "name": "armed80_floor50_stop150",
        "kind": "armed_peak_floor",
        "arm_target": 0.0080,
        "floor_fraction": 0.50,
        "hard_stop": 0.0150,
    },
]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def metric_stats(values: list[float]) -> dict[str, Any]:
    clean = pd.Series(values, dtype="float64").dropna()
    clean = clean[clean.map(math.isfinite)]
    if clean.empty:
        return {"count": 0, "sum": None, "min": None, "p10": None, "median": None, "mean": None, "p90": None, "max": None}
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


def epoch_to_ist(epoch: Any) -> str:
    try:
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S%z")
    except Exception:
        return ""


def choose_custom_exit(row: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    window = row["window"]
    returns = pd.to_numeric(window["forward_return"], errors="coerce")
    entry_epoch = int(row["qualification_epoch"])
    arm_target = float(config["arm_target"])
    floor_fraction = float(config["floor_fraction"])
    hard_stop = config.get("hard_stop")
    hard_stop_value = float(hard_stop) if hard_stop is not None else None
    max_wait_seconds = int(config.get("max_wait_minutes") or 0) * 60
    failure_floor = float(config.get("failure_floor") or 0.0)
    armed = False
    peak = 0.0
    active_floor: float | None = None
    exit_idx = len(window) - 1
    exit_reason = "open_at_period_end" if row.get("open_at_period_end") else "underlying_t2_exit"
    exit_return_override: float | None = None
    for idx, ret in enumerate(returns):
        if not math.isfinite(float(ret)):
            continue
        ret_float = float(ret)
        clock_epoch = int(window.iloc[idx]["clock_epoch"])
        if hard_stop_value is not None and ret_float <= -hard_stop_value:
            exit_idx = idx
            exit_reason = "adverse_stop"
            break
        peak = max(peak, ret_float)
        if not armed and ret_float >= arm_target:
            armed = True
        if armed:
            active_floor = max(0.0, peak * floor_fraction)
            if ret_float <= active_floor:
                exit_idx = idx
                exit_reason = "armed_peak_floor"
                exit_return_override = active_floor
                break
        if max_wait_seconds > 0 and clock_epoch - entry_epoch >= max_wait_seconds and ret_float <= failure_floor:
            exit_idx = idx
            exit_reason = "target_timeout_failure"
            break
    exit_row = window.iloc[exit_idx]
    exit_return = float(exit_return_override if exit_return_override is not None else exit_row["forward_return"])
    return {
        "exit_epoch": int(exit_row["clock_epoch"]),
        "exit_reason": exit_reason,
        "exit_return": exit_return,
        "exit_duration_minutes": (int(exit_row["clock_epoch"]) - entry_epoch) / 60.0,
        "exit_price": overlay_research.reconstructed_exit_price(float(row["entry_fill_price"]), str(row["side"]), exit_return),
        "armed": armed,
        "peak_return": peak,
        "active_floor_return": active_floor,
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [row for row in rows if not row.get("open_at_period_end")]
    net_values = [float(row["net_rupees_per_lot"]) for row in closed]
    margin_values = [float(row["net_return_on_margin_pct"]) for row in closed if row.get("net_return_on_margin_pct") is not None]
    hold_values = [float(row["hold_minutes"]) for row in closed if row.get("hold_minutes") is not None]
    marked_net_values = [float(row["net_rupees_per_lot"]) for row in rows]
    marked_margin_values = [float(row["net_return_on_margin_pct"]) for row in rows if row.get("net_return_on_margin_pct") is not None]
    reasons: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("exit_reason") or "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "trade_count": len(rows),
        "closed_count": len(closed),
        "open_at_period_end_count": len(rows) - len(closed),
        "success_rate_pct": round(sum(1 for value in net_values if value > 0) / len(net_values) * 100.0, 4) if net_values else None,
        "net_rupees_per_lot": metric_stats(net_values),
        "net_return_on_margin_pct": metric_stats(margin_values),
        "marked_success_rate_pct": (
            round(sum(1 for value in marked_net_values if value > 0) / len(marked_net_values) * 100.0, 4)
            if marked_net_values
            else None
        ),
        "marked_net_rupees_per_lot": metric_stats(marked_net_values),
        "marked_net_return_on_margin_pct": metric_stats(marked_margin_values),
        "hold_minutes": metric_stats(hold_values),
        "exit_reasons": reasons,
    }


def flatten(row: dict[str, Any]) -> dict[str, Any]:
    full = row["full"]
    train = row["train_aug10_aug27"]
    aug28 = row["val_aug28"]
    aug31 = row["val_aug31"]
    return {
        "policy_name": row["policy_name"],
        "exit": row["exit"],
        "trade_count": full["trade_count"],
        "success_rate_pct": full["success_rate_pct"],
        "total_net_rupees_per_lot": full["net_rupees_per_lot"]["sum"],
        "median_net_rupees_per_lot": full["net_rupees_per_lot"]["median"],
        "worst_net_rupees_per_lot": full["net_rupees_per_lot"]["min"],
        "marked_success_rate_pct": full["marked_success_rate_pct"],
        "marked_total_net_rupees_per_lot": full["marked_net_rupees_per_lot"]["sum"],
        "marked_median_net_rupees_per_lot": full["marked_net_rupees_per_lot"]["median"],
        "marked_worst_net_rupees_per_lot": full["marked_net_rupees_per_lot"]["min"],
        "mean_margin_return_pct": full["net_return_on_margin_pct"]["mean"],
        "median_margin_return_pct": full["net_return_on_margin_pct"]["median"],
        "marked_mean_margin_return_pct": full["marked_net_return_on_margin_pct"]["mean"],
        "marked_median_margin_return_pct": full["marked_net_return_on_margin_pct"]["median"],
        "median_hold_minutes": full["hold_minutes"]["median"],
        "exit_reasons": json.dumps(full["exit_reasons"], sort_keys=True),
        "train_success_rate_pct": train["success_rate_pct"],
        "train_total_net_rupees_per_lot": train["net_rupees_per_lot"]["sum"],
        "aug28_trade_count": aug28["trade_count"],
        "aug28_total_net_rupees_per_lot": aug28["net_rupees_per_lot"]["sum"],
        "aug31_trade_count": aug31["trade_count"],
        "aug31_total_net_rupees_per_lot": aug31["net_rupees_per_lot"]["sum"],
    }


def split(rows: list[dict[str, Any]], dates: set[str]) -> dict[str, Any]:
    return summarize([row for row in rows if str(row.get("entry_date")) in dates])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mae-floor", type=float, default=0.0005)
    args = parser.parse_args()

    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    base.add_paths(args.root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    frame = pd.read_parquet(args.opportunity_frame)
    path_lookup = overlay_research.build_path_lookup(frame)
    policies = {policy.name: policy for policy in continuation.continuation_policy_grid()}
    reports: list[dict[str, Any]] = []
    for policy_name in POLICIES:
        policy = policies[policy_name]
        rows = overlay_research.policy_path_rows(frame, path_lookup, policy, args.mae_floor, include_window=True)
        for custom_exit in CUSTOM_EXITS:
            trades: list[dict[str, Any]] = []
            for row in rows:
                exit_row = choose_custom_exit(row, custom_exit)
                account = overlay_research.overlay_accounting(v1_portfolio, row, exit_row)
                entry_epoch = int(row["qualification_epoch"])
                exit_epoch = int(exit_row["exit_epoch"])
                trades.append(
                    {
                        "policy_name": policy_name,
                        "exit": custom_exit["name"],
                        "row_id": row.get("row_id"),
                        "symbol": row.get("symbol"),
                        "side": row.get("side"),
                        "entry_epoch": entry_epoch,
                        "entry_time": epoch_to_ist(entry_epoch),
                        "entry_date": epoch_to_ist(entry_epoch)[:10],
                        "exit_epoch": exit_epoch,
                        "exit_time": epoch_to_ist(exit_epoch),
                        "exit_date": epoch_to_ist(exit_epoch)[:10],
                        "exit_reason": exit_row.get("exit_reason"),
                        "hold_minutes": exit_row.get("exit_duration_minutes"),
                        "exit_return": exit_row.get("exit_return"),
                        "net_rupees_per_lot": account.get("net_rupees"),
                        "gross_rupees_per_lot": account.get("gross_rupees"),
                        "charges_rupees_per_lot": account.get("charges_rupees"),
                        "net_return_on_margin_pct": account.get("net_return_on_margin_pct"),
                        "open_at_period_end": row.get("open_at_period_end") and exit_row.get("exit_reason") == "open_at_period_end",
                    }
                )
            dates = {str(row["entry_date"]) for row in trades}
            report = {
                "policy_name": policy_name,
                "exit": custom_exit["name"],
                "full": summarize(trades),
                "train_aug10_aug27": split(trades, {d for d in dates if "2026-08-10" <= d <= "2026-08-27"}),
                "val_aug28": split(trades, {"2026-08-28"}),
                "val_aug31": split(trades, {"2026-08-31"}),
            }
            reports.append(report)
    ranked = sorted(
        reports,
        key=lambda row: (
            float(row["full"]["success_rate_pct"] or 0.0),
            float(row["full"]["net_rupees_per_lot"]["sum"] or -1e9),
            float(row["val_aug31"]["net_rupees_per_lot"]["sum"] or -1e9),
            float(row["full"]["net_rupees_per_lot"]["min"] or -1e9),
        ),
        reverse=True,
    )
    write_csv(args.output_dir / "summary.csv", [flatten(row) for row in ranked])
    write_json(
        args.output_dir / "final_report.json",
        {
            "schema": SCHEMA,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "top": [flatten(row) for row in ranked[:30]],
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
