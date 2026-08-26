#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import research_t2_shortlist_momentum as shortlist  # noqa: E402


SCHEMA = "obvfutport_v2.t2_shortlist_hysteresis_research.v1"


@dataclass(frozen=True)
class HysteresisRule:
    name: str
    score_column: str
    mode: str
    entry_value: float
    exit_value: float
    refresh_minutes: int
    entry_persist: int
    exit_persist: int
    min_hold_minutes: int


def parse_csv_ints(text: str) -> list[int]:
    values: list[int] = []
    for part in str(text).split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value <= 0:
            raise ValueError(f"expected positive integer, got {value}")
        values.append(value)
    return values


def parse_csv_scores(text: str) -> list[str]:
    return [part.strip() for part in str(text).split(",") if part.strip()]


def rule_name(rule: HysteresisRule) -> str:
    if rule.mode == "rank_pct":
        entry = int(round(rule.entry_value * 100))
        exit_ = int(round(rule.exit_value * 100))
        core = f"{rule.score_column}_rank{entry}_exit{exit_}"
    elif rule.mode == "top_n":
        core = f"{rule.score_column}_top{int(rule.entry_value)}_exit{int(rule.exit_value)}"
    else:
        core = f"{rule.score_column}_{rule.mode}_{rule.entry_value:g}_exit{rule.exit_value:g}"
    return (
        f"{core}_r{rule.refresh_minutes}m"
        f"_p{rule.entry_persist}"
        f"_x{rule.exit_persist}"
        f"_h{rule.min_hold_minutes}"
    )


def build_rules(
    *,
    score_columns: list[str],
    refresh_minutes: list[int],
    entry_persists: list[int],
    exit_persists: list[int],
    min_holds: list[int],
    include_rank_pct: bool,
    include_top_n: bool,
) -> list[HysteresisRule]:
    rank_pairs = [
        (0.95, 0.90),
        (0.95, 0.80),
        (0.90, 0.80),
        (0.90, 0.70),
        (0.80, 0.70),
    ]
    top_pairs = [
        (5, 10),
        (5, 20),
        (10, 20),
        (10, 30),
        (20, 30),
        (20, 50),
        (30, 50),
    ]
    rules: list[HysteresisRule] = []
    for refresh in refresh_minutes:
        for score in score_columns:
            pairs: list[tuple[str, float, float]] = []
            if include_rank_pct:
                pairs.extend(("rank_pct", entry, exit_) for entry, exit_ in rank_pairs)
            if include_top_n:
                pairs.extend(("top_n", float(entry), float(exit_)) for entry, exit_ in top_pairs)
            for mode, entry_value, exit_value in pairs:
                for entry_persist in entry_persists:
                    for exit_persist in exit_persists:
                        for min_hold in min_holds:
                            draft = HysteresisRule(
                                name="",
                                score_column=score,
                                mode=mode,
                                entry_value=float(entry_value),
                                exit_value=float(exit_value),
                                refresh_minutes=int(refresh),
                                entry_persist=int(entry_persist),
                                exit_persist=int(exit_persist),
                                min_hold_minutes=int(min_hold),
                            )
                            rules.append(
                                HysteresisRule(
                                    name=rule_name(draft),
                                    score_column=draft.score_column,
                                    mode=draft.mode,
                                    entry_value=draft.entry_value,
                                    exit_value=draft.exit_value,
                                    refresh_minutes=draft.refresh_minutes,
                                    entry_persist=draft.entry_persist,
                                    exit_persist=draft.exit_persist,
                                    min_hold_minutes=draft.min_hold_minutes,
                                )
                            )
    return rules


def ranked_score_frame(refresh_frame: pd.DataFrame, score_column: str) -> pd.DataFrame:
    cols = [
        "row_id",
        "symbol",
        "side",
        "clock_epoch",
        "clock_time",
        "t2_entry_epoch",
        "t2_entry_time",
        "t2_exit_epoch",
        "t2_exit_time",
        score_column,
    ]
    full = refresh_frame.loc[:, cols].copy()
    full.rename(columns={score_column: "shortlist_score"}, inplace=True)
    full["score_rank_pct"] = None
    full["score_rank_n"] = None
    usable = full.loc[pd.to_numeric(full["shortlist_score"], errors="coerce").notna()].copy()
    if usable.empty:
        return full
    usable.sort_values(
        ["clock_epoch", "shortlist_score", "symbol", "row_id"],
        ascending=[True, False, True, True],
        inplace=True,
    )
    usable["score_rank_pct"] = usable.groupby("clock_epoch")["shortlist_score"].rank(method="first", pct=True)
    usable["score_rank_n"] = usable.groupby("clock_epoch", sort=False).cumcount() + 1
    ranks = usable[["row_id", "clock_epoch", "score_rank_pct", "score_rank_n"]]
    full.drop(columns=["score_rank_pct", "score_rank_n"], inplace=True)
    return full.merge(ranks, on=["row_id", "clock_epoch"], how="left")


def score_rule_frame(ranked_frame: pd.DataFrame, rule: HysteresisRule) -> pd.DataFrame:
    full = ranked_frame.copy()
    if rule.mode == "rank_pct":
        rank_pct = pd.to_numeric(full["score_rank_pct"], errors="coerce")
        full["entry_eligible"] = rank_pct >= rule.entry_value
        full["stay_eligible"] = rank_pct >= rule.exit_value
    elif rule.mode == "top_n":
        rank_n = pd.to_numeric(full["score_rank_n"], errors="coerce")
        full["entry_eligible"] = rank_n <= int(rule.entry_value)
        full["stay_eligible"] = rank_n <= int(rule.exit_value)
    else:
        raise ValueError(f"unknown mode: {rule.mode}")
    full["entry_eligible"] = full["entry_eligible"].fillna(False).astype(bool)
    full["stay_eligible"] = full["stay_eligible"].fillna(False).astype(bool)
    return full


def build_hysteresis_spells(rule_frame: pd.DataFrame, rule: HysteresisRule) -> list[dict[str, Any]]:
    if rule_frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    entry_ids = set(rule_frame.loc[rule_frame["entry_eligible"], "row_id"].astype(str).unique())
    if not entry_ids:
        return rows
    frame = rule_frame.loc[rule_frame["row_id"].astype(str).isin(entry_ids)].sort_values(
        ["row_id", "clock_epoch"],
        kind="mergesort",
    )
    expected_gap = int(rule.refresh_minutes * 60)
    for row_id, group in frame.groupby("row_id", sort=False):
        group = group.sort_values("clock_epoch", kind="mergesort")
        active: dict[str, Any] | None = None
        entry_streak = 0
        exit_streak = 0
        last_clock: int | None = None
        last_score: float | None = None
        last_active_clock: int | None = None
        for row in group.itertuples(index=False):
            clock = int(row.clock_epoch)
            consecutive = last_clock is not None and clock - last_clock == expected_gap
            if not consecutive:
                entry_streak = 0
                if active is not None:
                    exit_streak = 0
            score = shortlist.base.as_float(row.shortlist_score)
            entry_eligible = bool(row.entry_eligible)
            stay_eligible = bool(row.stay_eligible)

            if active is None:
                entry_streak = entry_streak + 1 if entry_eligible and consecutive else (1 if entry_eligible else 0)
                if entry_streak >= rule.entry_persist:
                    active = {
                        "row_id": row_id,
                        "symbol": row.symbol,
                        "side": row.side,
                        "entry_epoch": clock,
                        "entry_time": row.clock_time,
                        "t2_entry_epoch": int(row.t2_entry_epoch),
                        "t2_entry_time": row.t2_entry_time,
                        "t2_exit_epoch": int(row.t2_exit_epoch),
                        "t2_exit_time": row.t2_exit_time,
                        "entry_score": score,
                        "entry_persist": rule.entry_persist,
                        "exit_persist": rule.exit_persist,
                        "min_hold_minutes": rule.min_hold_minutes,
                        "refresh_minutes": rule.refresh_minutes,
                    }
                    exit_streak = 0
                    last_active_clock = clock
                    last_score = score
            else:
                last_active_clock = clock
                last_score = score
                min_hold_met = clock - int(active["entry_epoch"]) >= rule.min_hold_minutes * 60
                if stay_eligible:
                    exit_streak = 0
                elif min_hold_met:
                    exit_streak = exit_streak + 1 if consecutive else 1
                else:
                    exit_streak = 0
                if min_hold_met and exit_streak >= rule.exit_persist:
                    exit_epoch = min(clock, int(active["t2_exit_epoch"]))
                    if exit_epoch > int(active["entry_epoch"]):
                        active["last_shortlist_epoch"] = last_active_clock
                        active["last_shortlist_time"] = shortlist.base.epoch_ist_iso(int(last_active_clock))
                        active["exit_epoch"] = exit_epoch
                        active["exit_time"] = shortlist.base.epoch_ist_iso(exit_epoch)
                        active["exit_reason"] = (
                            "underlying_t2_exit" if exit_epoch >= int(active["t2_exit_epoch"]) else "score_exit_threshold"
                        )
                        active["last_score"] = last_score
                        rows.append(active)
                    active = None
                    entry_streak = 0
                    exit_streak = 0
                    last_active_clock = None
                    last_score = None
            last_clock = clock
        if active is not None:
            exit_epoch = int(active["t2_exit_epoch"])
            if exit_epoch > int(active["entry_epoch"]):
                active["last_shortlist_epoch"] = last_active_clock
                active["last_shortlist_time"] = shortlist.base.epoch_ist_iso(int(last_active_clock or active["entry_epoch"]))
                active["exit_epoch"] = exit_epoch
                active["exit_time"] = shortlist.base.epoch_ist_iso(exit_epoch)
                active["exit_reason"] = "underlying_t2_exit"
                active["last_score"] = last_score
                rows.append(active)
    return rows


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
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
        "spell_unique_legs",
        "spell_success_rate_pct",
        "spell_mean_margin_pct",
        "spell_median_margin_pct",
        "spell_total_net",
        "spell_median_duration_min",
        "spell_score_exit_threshold",
        "spell_underlying_t2_exit",
        "spell_accounting_missing_entry_fill",
        "spell_accounting_missing_exit_fill",
        "elapsed_seconds",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def flatten(rule: HysteresisRule, spell: dict[str, Any], skips: dict[str, int], elapsed_seconds: float) -> dict[str, Any]:
    def stat(payload: dict[str, Any], key: str, field: str) -> Any:
        value = payload.get(key)
        if isinstance(value, dict):
            return value.get(field)
        return None

    reasons = spell.get("spell_exit_reasons") or {}
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
        "spell_count": spell.get("spell_count"),
        "spell_unique_legs": spell.get("spell_unique_legs"),
        "spell_success_rate_pct": spell.get("spell_success_rate_pct"),
        "spell_mean_margin_pct": stat(spell, "spell_net_return_on_margin_pct", "mean"),
        "spell_median_margin_pct": stat(spell, "spell_net_return_on_margin_pct", "median"),
        "spell_total_net": stat(spell, "spell_net_rupees", "sum"),
        "spell_median_duration_min": stat(spell, "spell_duration_minutes", "median"),
        "spell_score_exit_threshold": reasons.get("score_exit_threshold", 0),
        "spell_underlying_t2_exit": reasons.get("underlying_t2_exit", 0),
        "spell_accounting_missing_entry_fill": skips.get("missing_entry_fill", 0),
        "spell_accounting_missing_exit_fill": skips.get("missing_exit_fill", 0),
        "elapsed_seconds": round(float(elapsed_seconds), 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-25")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--refresh-minutes", default="1,5,10,15,30,60,120")
    parser.add_argument("--entry-persists", default="2,3")
    parser.add_argument("--exit-persists", default="1")
    parser.add_argument("--min-holds", default="15,30,60")
    parser.add_argument("--score-columns", default="")
    parser.add_argument("--mode-family", choices=["all", "rank_pct", "top_n"], default="all")
    parser.add_argument("--max-rules", type=int, default=0)
    args = parser.parse_args()

    started = time.monotonic()
    root = args.root
    shortlist.base.add_paths(root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    raw = shortlist.load_frame(args.opportunity_frame)
    frame = shortlist.add_shortlist_scores(raw)
    default_scores = [
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
    score_columns = parse_csv_scores(args.score_columns) if args.score_columns else default_scores
    missing_scores = [score for score in score_columns if score not in frame.columns]
    if missing_scores:
        raise ValueError(f"missing score columns: {missing_scores}")

    rules = build_rules(
        score_columns=score_columns,
        refresh_minutes=parse_csv_ints(args.refresh_minutes),
        entry_persists=parse_csv_ints(args.entry_persists),
        exit_persists=parse_csv_ints(args.exit_persists),
        min_holds=parse_csv_ints(args.min_holds),
        include_rank_pct=args.mode_family in {"all", "rank_pct"},
        include_top_n=args.mode_family in {"all", "top_n"},
    )
    if args.max_rules and args.max_rules > 0:
        rules = rules[: args.max_rules]

    leg_lookup = shortlist.build_leg_lookup(root, args.start_date, args.end_date)
    required_keys = set()
    for leg in leg_lookup.values():
        required_keys.add(leg.signal_key)
        required_keys.add(leg.execution_key)
    index = shortlist.load_quote_index_for_period(root, args.start_date, args.end_date, required_keys)

    summaries: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    refresh_frames: dict[int, pd.DataFrame] = {}
    ranked_frames: dict[tuple[int, str], pd.DataFrame] = {}
    anchor_minutes = shortlist.parse_anchor_minutes("09:16")
    loop_started = time.monotonic()
    for idx, rule in enumerate(rules, start=1):
        rule_started = time.monotonic()
        refresh_frame = refresh_frames.get(rule.refresh_minutes)
        if refresh_frame is None:
            refresh_frame = shortlist.filter_refresh_frame(frame, rule.refresh_minutes, anchor_minutes)
            refresh_frames[rule.refresh_minutes] = refresh_frame
        ranked_key = (rule.refresh_minutes, rule.score_column)
        ranked_frame = ranked_frames.get(ranked_key)
        if ranked_frame is None:
            ranked_frame = ranked_score_frame(refresh_frame, rule.score_column)
            ranked_frames[ranked_key] = ranked_frame
        rule_frame = score_rule_frame(ranked_frame, rule)
        spell_rows = build_hysteresis_spells(rule_frame, rule)
        spell_frame = shortlist.account_spells(
            spells=spell_rows,
            leg_lookup=leg_lookup,
            index=index,
            v1_portfolio=v1_portfolio,
        )
        spell_summary = shortlist.summarize_spells(spell_frame)
        flat = flatten(rule, spell_summary, dict(spell_frame.attrs.get("skips") or {}), time.monotonic() - rule_started)
        summaries.append(flat)
        details.append(
            {
                "rule": rule.__dict__,
                "spells": spell_summary,
                "spell_accounting_skips": dict(spell_frame.attrs.get("skips") or {}),
            }
        )
        if idx % 50 == 0 or idx == len(rules):
            write_summary_csv(output_dir / "t2_shortlist_hysteresis_summary.csv", summaries)
            shortlist.write_json(
                output_dir / "progress.json",
                {
                    "phase": "shortlist_hysteresis_scoring",
                    "completed_rules": idx,
                    "total_rules": len(rules),
                    "elapsed_seconds": round(time.monotonic() - loop_started, 3),
                    "latest_rule": rule.name,
                },
            )

    ranked_median = sorted(
        summaries,
        key=lambda row: (
            float(row.get("spell_median_margin_pct") or -999),
            float(row.get("spell_mean_margin_pct") or -999),
            float(row.get("spell_success_rate_pct") or -999),
            int(row.get("spell_count") or 0),
        ),
        reverse=True,
    )
    ranked_mean = sorted(
        summaries,
        key=lambda row: (
            float(row.get("spell_mean_margin_pct") or -999),
            float(row.get("spell_median_margin_pct") or -999),
            float(row.get("spell_success_rate_pct") or -999),
            int(row.get("spell_count") or 0),
        ),
        reverse=True,
    )
    ranked_success = sorted(
        summaries,
        key=lambda row: (
            float(row.get("spell_success_rate_pct") or -999),
            float(row.get("spell_mean_margin_pct") or -999),
            float(row.get("spell_median_margin_pct") or -999),
            int(row.get("spell_count") or 0),
        ),
        reverse=True,
    )
    write_summary_csv(output_dir / "t2_shortlist_hysteresis_summary.csv", summaries)
    shortlist.write_json(output_dir / "t2_shortlist_hysteresis_details.json", details)
    report = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "root": str(root),
        "opportunity_frame": str(args.opportunity_frame),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "rule_count": len(rules),
        "input_rows": int(frame.shape[0]),
        "input_unique_legs": int(frame["row_id"].nunique()) if not frame.empty else 0,
        "definition": {
            "shortlist_entry": "entry occurs at the refresh clock where a leg has satisfied entry threshold for entry_persist consecutive refreshes",
            "shortlist_exit": "exit occurs at underlying T2 exit, or after minimum hold when the score is below the weaker exit threshold for exit_persist refreshes",
            "direction_adjustment": "ret_* and ram_* are signed by T2 direction, so long and short legs compete on the same score scale",
            "live_implementability": "all entry/exit decisions use only score values available at the current refresh clock",
        },
        "top_20_by_spell_median": ranked_median[:20],
        "top_20_by_spell_mean": ranked_mean[:20],
        "top_20_by_spell_success": ranked_success[:20],
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    shortlist.write_json(output_dir / "final_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
