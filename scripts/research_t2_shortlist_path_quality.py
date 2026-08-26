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

import research_t2_shortlist_hysteresis as hysteresis  # noqa: E402
import research_t2_shortlist_momentum as shortlist  # noqa: E402


SCHEMA = "obvfutport_v2.t2_shortlist_path_quality.v1"
DEFAULT_SCORES = [
    "score_mom10",
    "score_mom30",
    "score_mom60",
    "score_mom_blend",
    "score_ram10",
    "score_ram30",
    "score_ram60",
    "score_ram_blend",
    "score_mom_ram_blend",
    "score_risk_first_existing",
    "score_continuation_existing",
    "score_cost_adjusted_existing",
]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def parse_csv_ints(text: str) -> list[int]:
    return [int(part.strip()) for part in str(text).split(",") if part.strip()]


def parse_csv_scores(text: str) -> list[str]:
    return [part.strip() for part in str(text).split(",") if part.strip()]


def metric_stats(values: list[float]) -> dict[str, Any]:
    clean = pd.Series([value for value in values if math.isfinite(float(value))], dtype="float64")
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


def spell_path_quality(spell: dict[str, Any], path_lookup: dict[str, pd.DataFrame], mae_floor: float) -> dict[str, Any]:
    row_id = str(spell.get("row_id") or "")
    path = path_lookup.get(row_id)
    if path is None or path.empty:
        return {"ok": False, "reason": "missing_path"}
    entry_epoch = int(spell["entry_epoch"])
    exit_epoch = int(spell["exit_epoch"])
    if exit_epoch <= entry_epoch:
        return {"ok": False, "reason": "non_positive_duration"}
    window = path.loc[(path["clock_epoch"] >= entry_epoch) & (path["clock_epoch"] <= exit_epoch)]
    if window.empty:
        return {"ok": False, "reason": "empty_window"}
    entry_rows = window.loc[window["clock_epoch"] >= entry_epoch]
    if entry_rows.empty:
        return {"ok": False, "reason": "missing_entry_row"}
    entry_row = entry_rows.iloc[0]
    entry_ret = float(entry_row["current_ret"])
    side = str(spell.get("side") or entry_row.get("side") or "")
    returns: list[float] = []
    for row in window.itertuples(index=False):
        value = directional_return_from_entry(float(row.current_ret), entry_ret, side)
        if value is not None and math.isfinite(value):
            returns.append(float(value))
    if not returns:
        return {"ok": False, "reason": "missing_returns"}
    mfe = max(0.0, max(returns))
    mae_abs = max(0.0, -min(returns))
    end_return = returns[-1]
    ratio_floor = mfe / max(mae_abs, mae_floor)
    ratio_raw = None if mae_abs <= 0 else (mfe / mae_abs)
    return {
        "ok": True,
        "row_id": row_id,
        "symbol": spell.get("symbol"),
        "side": side,
        "entry_epoch": entry_epoch,
        "exit_epoch": exit_epoch,
        "duration_minutes": (exit_epoch - entry_epoch) / 60.0,
        "exit_reason": spell.get("exit_reason"),
        "entry_score": spell.get("entry_score"),
        "last_score": spell.get("last_score"),
        "mfe": mfe,
        "mae_abs": mae_abs,
        "mfe_mae_ratio_floor": ratio_floor,
        "mfe_mae_ratio_raw": ratio_raw,
        "end_return": end_return,
        "positive_end_return": end_return > 0,
        "zero_mae": mae_abs <= 0,
        "path_points": len(returns),
    }


def summarize_rule(rule: hysteresis.HysteresisRule, rows: list[dict[str, Any]], skips: dict[str, int]) -> dict[str, Any]:
    good = [row for row in rows if row.get("ok")]
    ratios = [float(row["mfe_mae_ratio_floor"]) for row in good]
    raw_ratios = [float(row["mfe_mae_ratio_raw"]) for row in good if row.get("mfe_mae_ratio_raw") is not None]
    mfe = [float(row["mfe"]) for row in good]
    mae = [float(row["mae_abs"]) for row in good]
    end_returns = [float(row["end_return"]) for row in good]
    durations = [float(row["duration_minutes"]) for row in good]
    wins = sum(1 for row in good if row.get("positive_end_return"))
    zero_mae = sum(1 for row in good if row.get("zero_mae"))
    exit_reasons: dict[str, int] = {}
    for row in good:
        reason = str(row.get("exit_reason") or "unknown")
        exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    ratio_stats = metric_stats(ratios)
    return {
        "rule": rule.name,
        "score_column": rule.score_column,
        "mode": rule.mode,
        "entry_value": rule.entry_value,
        "exit_value": rule.exit_value,
        "refresh_minutes": rule.refresh_minutes,
        "entry_persist": rule.entry_persist,
        "exit_persist": rule.exit_persist,
        "min_hold_minutes": rule.min_hold_minutes,
        "spell_count": len(good),
        "spell_unique_legs": len({str(row.get("row_id")) for row in good}),
        "path_skip_count": sum(skips.values()),
        "path_skip_reasons": skips,
        "success_rate_pct": round((wins / len(good) * 100.0), 4) if good else None,
        "zero_mae_spell_count": zero_mae,
        "mfe": metric_stats(mfe),
        "mae_abs": metric_stats(mae),
        "mfe_mae_ratio_floor": ratio_stats,
        "mfe_mae_ratio_raw": metric_stats(raw_ratios),
        "end_return": metric_stats(end_returns),
        "duration_minutes": metric_stats(durations),
        "ratio_ge_1_pct": round(sum(1 for value in ratios if value >= 1.0) / len(ratios) * 100.0, 4) if ratios else None,
        "ratio_ge_2_pct": round(sum(1 for value in ratios if value >= 2.0) / len(ratios) * 100.0, 4) if ratios else None,
        "ratio_ge_3_pct": round(sum(1 for value in ratios if value >= 3.0) / len(ratios) * 100.0, 4) if ratios else None,
        "spell_exit_reasons": exit_reasons,
    }


def flatten_summary(row: dict[str, Any]) -> dict[str, Any]:
    ratio = row.get("mfe_mae_ratio_floor") or {}
    raw_ratio = row.get("mfe_mae_ratio_raw") or {}
    mfe = row.get("mfe") or {}
    mae = row.get("mae_abs") or {}
    end_return = row.get("end_return") or {}
    duration = row.get("duration_minutes") or {}
    return {
        "rule": row.get("rule"),
        "score_column": row.get("score_column"),
        "mode": row.get("mode"),
        "entry_value": row.get("entry_value"),
        "exit_value": row.get("exit_value"),
        "refresh_minutes": row.get("refresh_minutes"),
        "entry_persist": row.get("entry_persist"),
        "exit_persist": row.get("exit_persist"),
        "min_hold_minutes": row.get("min_hold_minutes"),
        "spell_count": row.get("spell_count"),
        "success_rate_pct": row.get("success_rate_pct"),
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
        "mean_end_return": end_return.get("mean"),
        "median_end_return": end_return.get("median"),
        "min_end_return": end_return.get("min"),
        "mean_duration_minutes": duration.get("mean"),
        "median_duration_minutes": duration.get("median"),
        "ratio_ge_1_pct": row.get("ratio_ge_1_pct"),
        "ratio_ge_2_pct": row.get("ratio_ge_2_pct"),
        "ratio_ge_3_pct": row.get("ratio_ge_3_pct"),
        "zero_mae_spell_count": row.get("zero_mae_spell_count"),
        "raw_ratio_count": raw_ratio.get("count"),
        "path_skip_count": row.get("path_skip_count"),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = [
        "rule",
        "score_column",
        "mode",
        "entry_value",
        "exit_value",
        "refresh_minutes",
        "entry_persist",
        "exit_persist",
        "min_hold_minutes",
        "spell_count",
        "success_rate_pct",
        "mean_mfe_mae_ratio",
        "median_mfe_mae_ratio",
        "min_mfe_mae_ratio",
        "p10_mfe_mae_ratio",
        "p90_mfe_mae_ratio",
        "max_mfe_mae_ratio",
        "mean_mfe",
        "median_mfe",
        "min_mfe",
        "mean_mae_abs",
        "median_mae_abs",
        "max_mae_abs",
        "mean_end_return",
        "median_end_return",
        "min_end_return",
        "mean_duration_minutes",
        "median_duration_minutes",
        "ratio_ge_1_pct",
        "ratio_ge_2_pct",
        "ratio_ge_3_pct",
        "zero_mae_spell_count",
        "raw_ratio_count",
        "path_skip_count",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def top_by(rows: list[dict[str, Any]], metric_path: tuple[str, str], min_spells: int, limit: int) -> list[dict[str, Any]]:
    outer, inner = metric_path

    def score(row: dict[str, Any]) -> tuple[float, float, float, int]:
        stats = row.get(outer) or {}
        value = stats.get(inner)
        if value is None:
            value = -1e9
        success = row.get("success_rate_pct")
        if success is None:
            success = -1e9
        median_end = (row.get("end_return") or {}).get("median")
        if median_end is None:
            median_end = -1e9
        return (float(value), float(median_end), float(success), int(row.get("spell_count") or 0))

    filtered = [row for row in rows if int(row.get("spell_count") or 0) >= int(min_spells)]
    return [flatten_summary(row) for row in sorted(filtered, key=score, reverse=True)[:limit]]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--refresh-minutes", default="1,5,10,15,30,60,120")
    parser.add_argument("--entry-persists", default="2,3")
    parser.add_argument("--exit-persists", default="1")
    parser.add_argument("--min-holds", default="15,30,60")
    parser.add_argument("--score-columns", default="")
    parser.add_argument("--mode-family", choices=["all", "rank_pct", "top_n"], default="all")
    parser.add_argument("--max-rules", type=int, default=0)
    parser.add_argument("--mae-floor", type=float, default=0.0005)
    parser.add_argument("--report-min-spells", default="10,20,50,100")
    args = parser.parse_args()

    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    raw = shortlist.load_frame(args.opportunity_frame)
    frame = shortlist.add_shortlist_scores(raw)
    scores = parse_csv_scores(args.score_columns) if args.score_columns else DEFAULT_SCORES
    missing = [score for score in scores if score not in frame.columns]
    if missing:
        raise ValueError(f"missing score columns: {missing}")

    rules = hysteresis.build_rules(
        score_columns=scores,
        refresh_minutes=parse_csv_ints(args.refresh_minutes),
        entry_persists=parse_csv_ints(args.entry_persists),
        exit_persists=parse_csv_ints(args.exit_persists),
        min_holds=parse_csv_ints(args.min_holds),
        include_rank_pct=args.mode_family in {"all", "rank_pct"},
        include_top_n=args.mode_family in {"all", "top_n"},
    )
    if args.max_rules > 0:
        rules = rules[: args.max_rules]

    path_lookup = build_path_lookup(frame)
    anchor_minutes = shortlist.parse_anchor_minutes("09:16")
    refresh_frames: dict[int, pd.DataFrame] = {}
    ranked_frames: dict[tuple[int, str], pd.DataFrame] = {}
    summaries: list[dict[str, Any]] = []
    flat_rows: list[dict[str, Any]] = []
    loop_started = time.monotonic()

    for idx, rule in enumerate(rules, start=1):
        refresh_frame = refresh_frames.get(rule.refresh_minutes)
        if refresh_frame is None:
            refresh_frame = shortlist.filter_refresh_frame(frame, rule.refresh_minutes, anchor_minutes)
            refresh_frames[rule.refresh_minutes] = refresh_frame
        ranked_key = (rule.refresh_minutes, rule.score_column)
        ranked_frame = ranked_frames.get(ranked_key)
        if ranked_frame is None:
            ranked_frame = hysteresis.ranked_score_frame(refresh_frame, rule.score_column)
            ranked_frames[ranked_key] = ranked_frame
        rule_frame = hysteresis.score_rule_frame(ranked_frame, rule)
        spells = hysteresis.build_hysteresis_spells(rule_frame, rule)
        path_rows: list[dict[str, Any]] = []
        skips: dict[str, int] = {}
        for spell in spells:
            path_row = spell_path_quality(spell, path_lookup, args.mae_floor)
            if not path_row.get("ok"):
                reason = str(path_row.get("reason") or "unknown")
                skips[reason] = skips.get(reason, 0) + 1
            path_rows.append(path_row)
        summary = summarize_rule(rule, path_rows, skips)
        summaries.append(summary)
        flat_rows.append(flatten_summary(summary))
        if idx % 100 == 0 or idx == len(rules):
            write_csv(args.output_dir / "t2_shortlist_path_quality_summary.csv", flat_rows)
            write_json(
                args.output_dir / "progress.json",
                {
                    "phase": "path_quality_scoring",
                    "completed_rules": idx,
                    "total_rules": len(rules),
                    "latest_rule": rule.name,
                    "elapsed_seconds": round(time.monotonic() - loop_started, 3),
                },
            )

    min_spell_values = parse_csv_ints(args.report_min_spells)
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "opportunity_frame": str(args.opportunity_frame),
        "output_dir": str(args.output_dir),
        "rule_count": len(rules),
        "input_rows": int(frame.shape[0]),
        "input_unique_legs": int(frame["row_id"].nunique()) if not frame.empty else 0,
        "mae_floor": args.mae_floor,
        "definition": {
            "shortlist_spells": "same hysteresis/min-hold rules already tested for T2",
            "path_mfe_mae": "direction-adjusted return path from shortlist-entry clock through shortlist-exit clock, using opportunity-frame current_ret snapshots",
            "mfe_mae_ratio_floor": "mfe / max(mae_abs, mae_floor), default floor 5 bps to avoid infinite ratios from zero-MAE micro spells",
            "min_mfe_mae_ratio": "minimum spell-level floored ratio within the rule",
            "live_implementability": "ranking uses only previously computed opportunity rows and current-clock scores; no future data is used for entry/exit decisions, only for historical outcome measurement",
        },
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "rankings": {},
    }
    for min_spells in min_spell_values:
        report["rankings"][f"min_spells_{min_spells}"] = {
            "top_by_mean_mfe_mae": top_by(summaries, ("mfe_mae_ratio_floor", "mean"), min_spells, 15),
            "top_by_median_mfe_mae": top_by(summaries, ("mfe_mae_ratio_floor", "median"), min_spells, 15),
            "top_by_min_mfe_mae": top_by(summaries, ("mfe_mae_ratio_floor", "min"), min_spells, 15),
            "top_by_p10_mfe_mae": top_by(summaries, ("mfe_mae_ratio_floor", "p10"), min_spells, 15),
        }
    write_csv(args.output_dir / "t2_shortlist_path_quality_summary.csv", flat_rows)
    write_json(args.output_dir / "final_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
