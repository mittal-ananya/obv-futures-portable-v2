#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_continuation_filters as continuation  # noqa: E402
import research_t2_mfe_first_profit_capture as overlay_research  # noqa: E402


SCHEMA = "obvfutport_v2.t2_overlay_variant_compare.v1"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
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


def choose_exit(row: dict[str, Any], config: dict[str, Any], score_column: str, min_score: float) -> dict[str, Any]:
    if config["kind"] not in {"armed_profit_floor", "armed_peak_floor"}:
        return overlay_research.choose_overlay_exit(row, config, score_column, min_score)

    window = row["window"]
    returns = pd.to_numeric(window["forward_return"], errors="coerce")
    arm_target = float(config["arm_target"])
    floor_return = config.get("floor_return")
    floor_fraction = config.get("floor_fraction")
    floor_return_value = float(floor_return) if floor_return is not None else None
    floor_fraction_value = float(floor_fraction) if floor_fraction is not None else None
    hard_stop = config.get("hard_stop")
    hard_stop_value = float(hard_stop) if hard_stop is not None else None
    max_wait_seconds = int(config.get("max_wait_minutes") or 0) * 60
    failure_floor = float(config.get("failure_floor") or 0.0)
    exit_idx = len(window) - 1
    exit_reason = "underlying_t2_exit"
    exit_return_override: float | None = None
    armed = False
    peak = 0.0
    last_idx = 0
    last_date: str | None = None
    entry_epoch = int(row["qualification_epoch"])
    for idx, ret in enumerate(returns):
        if not math.isfinite(float(ret)):
            continue
        ret_float = float(ret)
        clock_date = str(window.iloc[idx].get("clock_time") or "")[:10]
        if armed and last_date is not None and clock_date and clock_date != last_date:
            exit_idx = last_idx
            exit_reason = "armed_session_close"
            break
        if hard_stop_value is not None and ret_float <= -hard_stop_value:
            exit_idx = idx
            exit_reason = "adverse_stop"
            break
        peak = max(peak, ret_float)
        if not armed and ret_float >= arm_target:
            armed = True
        if armed and floor_return_value is not None:
            active_floor = floor_return_value
        elif armed and floor_fraction_value is not None:
            active_floor = max(0.0, peak * floor_fraction_value)
        else:
            active_floor = None
        if armed and active_floor is not None and ret_float <= active_floor:
            exit_idx = idx
            exit_reason = "armed_profit_floor" if floor_return_value is not None else "armed_peak_floor"
            exit_return_override = active_floor
            break
        clock_int = int(window.iloc[idx]["clock_epoch"])
        if max_wait_seconds > 0 and clock_int - entry_epoch >= max_wait_seconds and ret_float <= failure_floor:
            exit_idx = idx
            exit_reason = "target_timeout_failure"
            break
        last_idx = idx
        last_date = clock_date or last_date
    exit_row = window.iloc[exit_idx]
    exit_return = float(exit_return_override if exit_return_override is not None else exit_row["forward_return"])
    return {
        "exit_epoch": int(exit_row["clock_epoch"]),
        "exit_reason": exit_reason,
        "exit_return": exit_return,
        "exit_duration_minutes": (int(exit_row["clock_epoch"]) - entry_epoch) / 60.0,
        "exit_price": overlay_research.reconstructed_exit_price(float(row["entry_fill_price"]), str(row["side"]), exit_return),
    }


def summarize_variant(
    variant_name: str,
    policy: continuation.ContinuationPolicy,
    config: dict[str, Any],
    frame: pd.DataFrame,
    path_lookup: dict[str, pd.DataFrame],
    v1_portfolio: Any,
    mae_floor: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = overlay_research.policy_path_rows(frame, path_lookup, policy, mae_floor, include_window=True)
    score_column = f"score_{policy.formula}"
    trade_rows: list[dict[str, Any]] = []
    for row in rows:
        exit_row = choose_exit(row, config, score_column, policy.min_score)
        account = overlay_research.overlay_accounting(v1_portfolio, row, exit_row)
        net = float(account.get("net_rupees") or 0.0)
        margin_return = account.get("net_return_on_margin_pct")
        trade_rows.append(
            {
                "variant": variant_name,
                "policy_name": policy.name,
                "overlay": config["name"],
                "row_id": row.get("row_id"),
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "qualification_epoch": row.get("qualification_epoch"),
                "t2_exit_epoch": row.get("t2_exit_epoch"),
                "exit_epoch": exit_row.get("exit_epoch"),
                "exit_reason": exit_row.get("exit_reason"),
                "entry_fill_price": row.get("entry_fill_price"),
                "exit_price": exit_row.get("exit_price"),
                "exit_return": exit_row.get("exit_return"),
                "exit_duration_minutes": exit_row.get("exit_duration_minutes"),
                "margin_per_lot": row.get("margin_per_lot"),
                "lot_size": row.get("lot_size"),
                "net_rupees_per_lot": net,
                "gross_rupees_per_lot": account.get("gross_rupees"),
                "charges_rupees_per_lot": account.get("charges_rupees"),
                "net_return_on_margin_pct": margin_return,
                "mfe": row.get("mfe"),
                "mae_abs": row.get("mae_abs"),
                "mfe_before_mae": row.get("mfe_before_mae"),
                "mfe_mae_ordering": row.get("mfe_mae_ordering"),
            }
        )
    net_values = [float(row["net_rupees_per_lot"]) for row in trade_rows]
    margin_values = [float(row["net_return_on_margin_pct"]) for row in trade_rows if row.get("net_return_on_margin_pct") is not None]
    exit_returns = [float(row["exit_return"]) for row in trade_rows if row.get("exit_return") is not None]
    exit_durations = [float(row["exit_duration_minutes"]) for row in trade_rows if row.get("exit_duration_minutes") is not None]
    mfe_values = [float(row["mfe"]) for row in trade_rows if row.get("mfe") is not None]
    mae_values = [float(row["mae_abs"]) for row in trade_rows if row.get("mae_abs") is not None]
    exit_reasons: dict[str, int] = {}
    for row in trade_rows:
        reason = str(row.get("exit_reason") or "unknown")
        exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    wins = sum(1 for value in net_values if value > 0)
    summary = {
        "variant": variant_name,
        "policy_name": policy.name,
        "overlay": config["name"],
        "trade_count": len(trade_rows),
        "unique_symbols": len({str(row["symbol"]) for row in trade_rows}),
        "success_rate_pct": round(wins / len(trade_rows) * 100.0, 4) if trade_rows else None,
        "net_rupees_per_lot": metric_stats(net_values),
        "net_return_on_margin_pct": metric_stats(margin_values),
        "exit_return": metric_stats(exit_returns),
        "exit_duration_minutes": metric_stats(exit_durations),
        "mfe": metric_stats(mfe_values),
        "mae_abs": metric_stats(mae_values),
        "mfe_before_mae_rate_pct": (
            round(sum(1 for row in trade_rows if row.get("mfe_before_mae")) / len(trade_rows) * 100.0, 4)
            if trade_rows
            else None
        ),
        "exit_reasons": exit_reasons,
        **{f"overlay_{key}": value for key, value in config.items() if key != "name"},
    }
    return summary, trade_rows


def flatten_summary(row: dict[str, Any]) -> dict[str, Any]:
    net = row.get("net_rupees_per_lot") or {}
    margin = row.get("net_return_on_margin_pct") or {}
    exit_ret = row.get("exit_return") or {}
    duration = row.get("exit_duration_minutes") or {}
    mfe = row.get("mfe") or {}
    mae = row.get("mae_abs") or {}
    return {
        "variant": row.get("variant"),
        "policy_name": row.get("policy_name"),
        "overlay": row.get("overlay"),
        "trade_count": row.get("trade_count"),
        "unique_symbols": row.get("unique_symbols"),
        "success_rate_pct": row.get("success_rate_pct"),
        "total_net_rupees_per_lot": net.get("sum"),
        "mean_net_rupees_per_lot": net.get("mean"),
        "median_net_rupees_per_lot": net.get("median"),
        "min_net_rupees_per_lot": net.get("min"),
        "p10_net_rupees_per_lot": net.get("p10"),
        "p90_net_rupees_per_lot": net.get("p90"),
        "mean_margin_return_pct": margin.get("mean"),
        "median_margin_return_pct": margin.get("median"),
        "min_margin_return_pct": margin.get("min"),
        "p10_margin_return_pct": margin.get("p10"),
        "p90_margin_return_pct": margin.get("p90"),
        "mean_exit_return": exit_ret.get("mean"),
        "median_exit_return": exit_ret.get("median"),
        "min_exit_return": exit_ret.get("min"),
        "mean_exit_duration_minutes": duration.get("mean"),
        "median_exit_duration_minutes": duration.get("median"),
        "mfe_before_mae_rate_pct": row.get("mfe_before_mae_rate_pct"),
        "median_mfe": mfe.get("median"),
        "median_mae_abs": mae.get("median"),
        "exit_reasons": json.dumps(row.get("exit_reasons") or {}, sort_keys=True),
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    if fields is None:
        fields = list(rows[0].keys())
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


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
    variants = [
        (
            "risk_first_tight_profit25_stop50_fail90",
            "risk_first_tight_risk_score0p85_age60to240_runway45",
            {
                "name": "profit_25bps_stop50bps_fail90m_flat",
                "kind": "profit_stop_or_failure",
                "target": 0.0025,
                "max_wait_minutes": 90,
                "failure_floor": 0.0,
                "hard_stop": 0.0050,
            },
        ),
        (
            "smooth_survivor_profit50",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {"name": "profit_50bps", "kind": "profit", "target": 0.0050},
        ),
        (
            "smooth_survivor_profit30",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {"name": "profit_30bps", "kind": "profit", "target": 0.0030},
        ),
        (
            "smooth_survivor_profit25",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {"name": "profit_25bps", "kind": "profit", "target": 0.0025},
        ),
        (
            "smooth_survivor_armed50_floor25",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {"name": "trailing_profit_25bps_armed_at_50bps", "kind": "armed_profit_floor", "arm_target": 0.0050, "floor_return": 0.0025},
        ),
        (
            "smooth_survivor_profit50_stop50_fail90",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {
                "name": "profit_50bps_stop50bps_fail90m_flat",
                "kind": "profit_stop_or_failure",
                "target": 0.0050,
                "max_wait_minutes": 90,
                "failure_floor": 0.0,
                "hard_stop": 0.0050,
            },
        ),
        (
            "smooth_survivor_armed20_floor80",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {
                "name": "armed20bps_floor80pct_peak",
                "kind": "armed_peak_floor",
                "arm_target": 0.0020,
                "floor_fraction": 0.80,
            },
        ),
        (
            "smooth_survivor_armed20_floor80_stop100",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {
                "name": "armed20bps_floor80pct_peak_stop100bps",
                "kind": "armed_peak_floor",
                "arm_target": 0.0020,
                "floor_fraction": 0.80,
                "hard_stop": 0.0100,
            },
        ),
        (
            "smooth_survivor_armed20_floor80_stop100_fail90",
            "smooth_survivor_tight_risk_score0p80_age60to240_runway0",
            {
                "name": "armed20bps_floor80pct_peak_stop100bps_fail90m_flat",
                "kind": "armed_peak_floor",
                "arm_target": 0.0020,
                "floor_fraction": 0.80,
                "hard_stop": 0.0100,
                "max_wait_minutes": 90,
                "failure_floor": 0.0,
            },
        ),
    ]
    summaries: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for variant_name, policy_name, config in variants:
        policy = policies[policy_name]
        summary, rows = summarize_variant(variant_name, policy, config, frame, path_lookup, v1_portfolio, float(args.mae_floor))
        summaries.append(summary)
        trades.extend(rows)

    summary_rows = [flatten_summary(row) for row in summaries]
    write_csv(args.output_dir / "variant_summary.csv", summary_rows)
    write_csv(args.output_dir / "variant_trades.csv", trades)
    report = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "opportunity_frame": str(args.opportunity_frame),
        "output_dir": str(args.output_dir),
        "definition": {
            "unit": "one T2 continuation leg, one futures lot",
            "entry": "first clock where the policy qualifies the already-open T2 leg",
            "exit": "overlay exit or underlying T2 exit, whichever comes first",
            "trailing_profit_25bps_armed_at_50bps": "arm when forward return first reaches +50bps, then exit if forward return falls back to +25bps; otherwise use underlying T2 exit",
            "profit50_plus_best_strict_failure": "profit target +50bps; adverse stop -50bps; after 90 minutes exit if return is <= flat; otherwise underlying T2 exit",
            "armed20_floor80": "arm when forward return first reaches +20bps, then exit if return falls to 80% of the best favorable return seen after arming; if armed into session close, exit at session close",
        },
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "summary": summary_rows,
    }
    write_json(args.output_dir / "final_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
