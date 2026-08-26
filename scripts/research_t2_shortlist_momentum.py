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
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_continuation_filters as continuation  # noqa: E402
import research_t2_portfolio_rules as portfolio_rules  # noqa: E402


IST = ZoneInfo("Asia/Kolkata")
SCHEMA = "obvfutport_v2.t2_shortlist_momentum_research.v1"


@dataclass(frozen=True)
class ShortlistRule:
    name: str
    score_column: str
    mode: str
    value: float
    refresh_minutes: int


def write_json(path: Path, payload: Any) -> None:
    base.write_json(path, payload)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    base.write_jsonl(path, rows)


def metric_stats(series: pd.Series) -> dict[str, Any]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
    return {
        "count": int(values.shape[0]),
        "sum": round(float(values.sum()), 6),
        "min": round(float(values.min()), 6),
        "max": round(float(values.max()), 6),
        "mean": round(float(values.mean()), 6),
        "median": round(float(values.median()), 6),
    }


def period_bounds(start_text: str, end_text: str) -> tuple[int, int]:
    start = base.parse_date(start_text)
    end = base.parse_date(end_text)
    start_epoch = int(datetime.combine(start, dt_time(0, 0), tzinfo=IST).timestamp())
    end_epoch = int(datetime.combine(end, dt_time(23, 59, 59), tzinfo=IST).timestamp())
    return start_epoch, end_epoch


def load_frame(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_pickle(path, compression="gzip")


def add_shortlist_scores(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["score_mom10"] = out["rank_ret_10"]
    out["score_mom30"] = out["rank_ret_30"]
    out["score_mom60"] = out["rank_ret_60"]
    out["score_mom_blend"] = (
        0.35 * out["rank_ret_10"] + 0.25 * out["rank_ret_30"] + 0.40 * out["rank_ret_60"]
    )
    out["score_ram10"] = out["rank_ram_10"]
    out["score_ram30"] = out["rank_ram_30"]
    out["score_ram60"] = out["rank_ram_60"]
    out["score_ram_blend"] = (
        0.35 * out["rank_ram_10"] + 0.25 * out["rank_ram_30"] + 0.40 * out["rank_ram_60"]
    )
    out["score_mom_ram_blend"] = (
        0.25 * out["rank_ret_10"]
        + 0.25 * out["rank_ret_60"]
        + 0.25 * out["rank_ram_10"]
        + 0.25 * out["rank_ram_60"]
    )
    out["score_risk_first_existing"] = out["score_risk_first"]
    out["score_continuation_existing"] = out["score_continuation"]
    out["score_cost_adjusted_existing"] = out["score_cost_adjusted"]
    return out


def parse_csv_ints(text: str) -> list[int]:
    values: list[int] = []
    for part in str(text).split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value <= 0:
            raise ValueError(f"refresh interval must be positive: {value}")
        values.append(value)
    return values or [1]


def parse_anchor_minutes(text: str) -> int:
    hour_text, minute_text = str(text).strip().split(":", 1)
    return int(hour_text) * 60 + int(minute_text)


def filter_refresh_frame(frame: pd.DataFrame, refresh_minutes: int, anchor_minutes: int) -> pd.DataFrame:
    if refresh_minutes <= 1:
        return frame.copy()
    clock_time = pd.to_datetime(frame["clock_time"], errors="coerce")
    minute_of_day = clock_time.dt.hour * 60 + clock_time.dt.minute
    mask = ((minute_of_day - int(anchor_minutes)) % int(refresh_minutes)) == 0
    return frame.loc[mask].copy()


def shortlist_rules(score_columns: list[str], refresh_minutes: list[int]) -> list[ShortlistRule]:
    rules: list[ShortlistRule] = []
    for refresh in refresh_minutes:
        for score in score_columns:
            suffix = f"_r{refresh}m"
            for n in (5, 10, 20, 30, 50):
                rules.append(
                    ShortlistRule(
                        name=f"{score}_top{n}{suffix}",
                        score_column=score,
                        mode="top_n",
                        value=float(n),
                        refresh_minutes=int(refresh),
                    )
                )
            for pct in (0.95, 0.90, 0.80):
                rules.append(
                    ShortlistRule(
                        name=f"{score}_rankpct{int(pct * 100)}{suffix}",
                        score_column=score,
                        mode="rank_pct",
                        value=float(pct),
                        refresh_minutes=int(refresh),
                    )
                )
    return rules


def select_shortlist(frame: pd.DataFrame, rule: ShortlistRule) -> pd.DataFrame:
    usable = frame.loc[pd.to_numeric(frame[rule.score_column], errors="coerce").notna()].copy()
    if usable.empty:
        return usable
    usable.sort_values(["clock_epoch", rule.score_column, "symbol", "row_id"], ascending=[True, False, True, True], inplace=True)
    if rule.mode == "top_n":
        selected = usable.groupby("clock_epoch", sort=False, group_keys=False).head(int(rule.value)).copy()
    elif rule.mode == "rank_pct":
        usable["_score_rank_pct"] = usable.groupby("clock_epoch")[rule.score_column].rank(method="first", pct=True)
        selected = usable.loc[usable["_score_rank_pct"] >= rule.value].copy()
    else:
        raise ValueError(f"unknown shortlist rule mode: {rule.mode}")
    selected["shortlist_score"] = selected[rule.score_column]
    return selected


def summarize_rows(rows: pd.DataFrame, *, label: str) -> dict[str, Any]:
    if rows.empty:
        return {
            f"{label}_count": 0,
            f"{label}_success_rate_pct": None,
            f"{label}_net_return_on_margin_pct": metric_stats(pd.Series(dtype=float)),
            f"{label}_net_rupees": metric_stats(pd.Series(dtype=float)),
        }
    wins = rows["net_rupees"] > 0
    return {
        f"{label}_count": int(rows.shape[0]),
        f"{label}_unique_legs": int(rows["row_id"].nunique()) if "row_id" in rows else None,
        f"{label}_wins": int(wins.sum()),
        f"{label}_success_rate_pct": round(float(wins.mean() * 100.0), 4),
        f"{label}_net_return_on_margin_pct": metric_stats(rows["net_return_on_margin_pct"]),
        f"{label}_net_rupees": metric_stats(rows["net_rupees"]),
    }


def summarize_first_hits(selected: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if selected.empty:
        return selected, summarize_rows(selected, label="first_hit")
    first = selected.sort_values(["row_id", "clock_epoch"], kind="mergesort").drop_duplicates("row_id", keep="first").copy()
    return first, summarize_rows(first, label="first_hit")


def build_leg_lookup(root: Path, start_date: str, end_date: str) -> dict[str, base.TrancheLeg]:
    start_epoch, end_epoch = period_bounds(start_date, end_date)
    loaded = base.load_rows(root)
    manifest = base.load_contract_manifest(root)
    margins = base.load_margin_lookup(root)
    all_t2 = base.build_legs(loaded.get("rows_by_tranche") or {}, manifest, margins)["T2"]
    return {leg.row_id: leg for leg in all_t2 if start_epoch <= leg.entry_epoch <= end_epoch}


def load_quote_index_for_period(root: Path, start_date: str, end_date: str, required_keys: set[str]) -> base.QuoteIndex:
    start = base.parse_date(start_date)
    end = base.parse_date(end_date)
    stream_paths = base.discover_stream_paths(root, start, end)
    cache_dir = root / "state" / "research_cache" / "t2_quote_index"
    index, _input_report = continuation.load_quote_index_from_cache(
        root=root,
        stream_paths=stream_paths,
        required_keys=required_keys,
        cache_dir=cache_dir,
    )
    return index


def spell_records(selected: pd.DataFrame, refresh_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if selected.empty:
        return []
    rows: list[dict[str, Any]] = []
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
    ]
    ever_selected = set(selected["row_id"].astype(str).unique())
    slim = refresh_frame.loc[refresh_frame["row_id"].astype(str).isin(ever_selected), cols].copy()
    selected_scores = selected[["row_id", "clock_epoch", "shortlist_score"]].copy()
    slim = slim.merge(selected_scores, on=["row_id", "clock_epoch"], how="left")
    slim["selected_flag"] = pd.to_numeric(slim["shortlist_score"], errors="coerce").notna()
    slim = slim.sort_values(["row_id", "clock_epoch"], kind="mergesort")
    for row_id, group in slim.groupby("row_id", sort=False):
        group = group.sort_values("clock_epoch", kind="mergesort")
        current: dict[str, Any] | None = None
        last_epoch: int | None = None
        last_score: float | None = None
        for row in group.itertuples(index=False):
            clock = int(row.clock_epoch)
            selected_now = bool(row.selected_flag)
            score = float(row.shortlist_score) if selected_now else None
            if selected_now and current is None:
                current = {
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
                }
            elif not selected_now and current is not None:
                if last_epoch is not None:
                    current["last_shortlist_epoch"] = last_epoch
                    current["last_shortlist_time"] = base.epoch_ist_iso(last_epoch)
                    current["exit_epoch"] = min(clock, int(current["t2_exit_epoch"]))
                    current["exit_time"] = base.epoch_ist_iso(current["exit_epoch"])
                    current["exit_reason"] = (
                        "underlying_t2_exit" if current["exit_epoch"] >= int(current["t2_exit_epoch"]) else "left_shortlist"
                    )
                    current["last_score"] = last_score
                    rows.append(current)
                current = None
            if selected_now:
                last_epoch = clock
                last_score = score
        if current is not None and last_epoch is not None:
            current["last_shortlist_epoch"] = last_epoch
            current["last_shortlist_time"] = base.epoch_ist_iso(last_epoch)
            current["exit_epoch"] = int(current["t2_exit_epoch"])
            current["exit_time"] = base.epoch_ist_iso(current["exit_epoch"])
            current["exit_reason"] = "underlying_t2_exit"
            current["last_score"] = last_score
            rows.append(current)
    return rows


def account_spells(
    *,
    spells: list[dict[str, Any]],
    leg_lookup: dict[str, base.TrancheLeg],
    index: base.QuoteIndex,
    v1_portfolio: Any,
) -> pd.DataFrame:
    out: list[dict[str, Any]] = []
    skips = {"missing_leg": 0, "missing_entry_fill": 0, "missing_exit_fill": 0}
    for spell in spells:
        leg = leg_lookup.get(str(spell["row_id"]))
        if leg is None:
            skips["missing_leg"] += 1
            continue
        entry_fill = base.execution_fill(index, v1_portfolio, leg, int(spell["entry_epoch"]), phase="entry")
        exit_fill = base.execution_fill(index, v1_portfolio, leg, int(spell["exit_epoch"]), phase="exit")
        if entry_fill is None:
            skips["missing_entry_fill"] += 1
            continue
        if exit_fill is None:
            skips["missing_exit_fill"] += 1
            continue
        entry_price = base.as_float(entry_fill.get("fill_price"))
        exit_price = base.as_float(exit_fill.get("fill_price"))
        if entry_price is None or exit_price is None or entry_price <= 0:
            continue
        acct = base.accounting(v1_portfolio, leg, entry_price, exit_price, 1)
        net = float(acct.get("net_rupees") or 0.0)
        gross = float(acct.get("gross_rupees") or 0.0)
        charges = float(acct.get("charges_rupees") or 0.0)
        direction = base.signed_direction(leg.side)
        row = dict(spell)
        row.update(
            {
                "entry_fill_price": entry_price,
                "exit_fill_price": exit_price,
                "net_rupees": net,
                "gross_rupees": gross,
                "charges_rupees": charges,
                "gross_return_pct": direction * ((exit_price / entry_price) - 1.0) * 100.0,
                "net_return_on_margin_pct": (net / float(leg.margin_per_lot)) * 100.0 if leg.margin_per_lot else None,
                "duration_minutes": (float(spell["exit_epoch"]) - float(spell["entry_epoch"])) / 60.0,
                "margin_per_lot": float(leg.margin_per_lot),
            }
        )
        out.append(row)
    frame = pd.DataFrame.from_records(out)
    frame.attrs["skips"] = skips
    return frame


def summarize_spells(spells: pd.DataFrame) -> dict[str, Any]:
    if spells.empty:
        return {
            "spell_count": 0,
            "spell_success_rate_pct": None,
            "spell_net_return_on_margin_pct": metric_stats(pd.Series(dtype=float)),
            "spell_net_rupees": metric_stats(pd.Series(dtype=float)),
            "spell_exit_reasons": {},
        }
    wins = spells["net_rupees"] > 0
    return {
        "spell_count": int(spells.shape[0]),
        "spell_unique_legs": int(spells["row_id"].nunique()),
        "spell_wins": int(wins.sum()),
        "spell_success_rate_pct": round(float(wins.mean() * 100.0), 4),
        "spell_net_return_on_margin_pct": metric_stats(spells["net_return_on_margin_pct"]),
        "spell_net_rupees": metric_stats(spells["net_rupees"]),
        "spell_gross_return_pct": metric_stats(spells["gross_return_pct"]),
        "spell_duration_minutes": metric_stats(spells["duration_minutes"]),
        "spell_exit_reasons": {str(k): int(v) for k, v in spells["exit_reason"].value_counts().to_dict().items()},
    }


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = [
        "rule",
        "score_column",
        "mode",
        "value",
        "refresh_minutes",
        "snapshot_count",
        "snapshot_unique_legs",
        "snapshot_success_rate_pct",
        "snapshot_mean_margin_pct",
        "snapshot_median_margin_pct",
        "first_hit_count",
        "first_hit_unique_legs",
        "first_hit_success_rate_pct",
        "first_hit_mean_margin_pct",
        "first_hit_median_margin_pct",
        "first_hit_total_net",
        "spell_count",
        "spell_unique_legs",
        "spell_success_rate_pct",
        "spell_mean_margin_pct",
        "spell_median_margin_pct",
        "spell_total_net",
        "spell_median_duration_min",
        "spell_left_shortlist",
        "spell_underlying_t2_exit",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def flatten_summary(rule: ShortlistRule, snapshot: dict[str, Any], first: dict[str, Any], spell: dict[str, Any]) -> dict[str, Any]:
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
        "value": rule.value,
        "refresh_minutes": rule.refresh_minutes,
        "snapshot_count": snapshot.get("snapshot_count"),
        "snapshot_unique_legs": snapshot.get("snapshot_unique_legs"),
        "snapshot_success_rate_pct": snapshot.get("snapshot_success_rate_pct"),
        "snapshot_mean_margin_pct": stat(snapshot, "snapshot_net_return_on_margin_pct", "mean"),
        "snapshot_median_margin_pct": stat(snapshot, "snapshot_net_return_on_margin_pct", "median"),
        "first_hit_count": first.get("first_hit_count"),
        "first_hit_unique_legs": first.get("first_hit_unique_legs"),
        "first_hit_success_rate_pct": first.get("first_hit_success_rate_pct"),
        "first_hit_mean_margin_pct": stat(first, "first_hit_net_return_on_margin_pct", "mean"),
        "first_hit_median_margin_pct": stat(first, "first_hit_net_return_on_margin_pct", "median"),
        "first_hit_total_net": stat(first, "first_hit_net_rupees", "sum"),
        "spell_count": spell.get("spell_count"),
        "spell_unique_legs": spell.get("spell_unique_legs"),
        "spell_success_rate_pct": spell.get("spell_success_rate_pct"),
        "spell_mean_margin_pct": stat(spell, "spell_net_return_on_margin_pct", "mean"),
        "spell_median_margin_pct": stat(spell, "spell_net_return_on_margin_pct", "median"),
        "spell_total_net": stat(spell, "spell_net_rupees", "sum"),
        "spell_median_duration_min": stat(spell, "spell_duration_minutes", "median"),
        "spell_left_shortlist": reasons.get("left_shortlist", 0),
        "spell_underlying_t2_exit": reasons.get("underlying_t2_exit", 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-25")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-rules", type=int, default=0)
    parser.add_argument(
        "--refresh-minutes",
        default="1,5,10,15,30",
        help="Comma-separated shortlist revision intervals anchored to --session-anchor.",
    )
    parser.add_argument(
        "--session-anchor",
        default="09:16",
        help="HH:MM IST anchor for interval refreshes, matching the first portfolio evaluation clock.",
    )
    args = parser.parse_args()

    started = time.monotonic()
    root = args.root
    base.add_paths(root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    raw = load_frame(args.opportunity_frame)
    frame = add_shortlist_scores(raw)
    score_columns = [
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
    refresh_minutes = parse_csv_ints(args.refresh_minutes)
    anchor_minutes = parse_anchor_minutes(args.session_anchor)
    rules = shortlist_rules(score_columns, refresh_minutes)
    if args.max_rules and args.max_rules > 0:
        rules = rules[: args.max_rules]
    leg_lookup = build_leg_lookup(root, args.start_date, args.end_date)
    required_keys = set()
    for leg in leg_lookup.values():
        required_keys.add(leg.signal_key)
        required_keys.add(leg.execution_key)
    index = load_quote_index_for_period(root, args.start_date, args.end_date, required_keys)
    summaries: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    rule_started = time.monotonic()
    refresh_frames: dict[int, pd.DataFrame] = {}
    for idx, rule in enumerate(rules, start=1):
        refresh_frame = refresh_frames.get(rule.refresh_minutes)
        if refresh_frame is None:
            refresh_frame = filter_refresh_frame(frame, rule.refresh_minutes, anchor_minutes)
            refresh_frames[rule.refresh_minutes] = refresh_frame
        selected = select_shortlist(refresh_frame, rule)
        snapshot_summary = summarize_rows(selected, label="snapshot")
        first_hits, first_summary = summarize_first_hits(selected)
        spells = spell_records(selected, refresh_frame)
        spell_frame = account_spells(spells=spells, leg_lookup=leg_lookup, index=index, v1_portfolio=v1_portfolio)
        spell_summary = summarize_spells(spell_frame)
        flat = flatten_summary(rule, snapshot_summary, first_summary, spell_summary)
        summaries.append(flat)
        details.append(
            {
                "rule": rule.__dict__,
                "snapshot": snapshot_summary,
                "first_hit": first_summary,
                "spells": spell_summary,
                "spell_accounting_skips": dict(spell_frame.attrs.get("skips") or {}),
            }
        )
        if idx % 20 == 0 or idx == len(rules):
            write_json(
                output_dir / "progress.json",
                {
                    "phase": "shortlist_scoring",
                    "completed_rules": idx,
                    "total_rules": len(rules),
                    "elapsed_seconds": round(time.monotonic() - rule_started, 3),
                    "latest_rule": rule.name,
                },
            )
    # Rank primarily by spell behavior because that is the actual enter/exit-shortlist interpretation.
    ranked_spell = sorted(
        summaries,
        key=lambda row: (
            float(row.get("spell_median_margin_pct") or -999),
            float(row.get("spell_mean_margin_pct") or -999),
            float(row.get("spell_success_rate_pct") or -999),
            int(row.get("spell_count") or 0),
        ),
        reverse=True,
    )
    ranked_first = sorted(
        summaries,
        key=lambda row: (
            float(row.get("first_hit_median_margin_pct") or -999),
            float(row.get("first_hit_mean_margin_pct") or -999),
            float(row.get("first_hit_success_rate_pct") or -999),
            int(row.get("first_hit_count") or 0),
        ),
        reverse=True,
    )
    write_summary_csv(output_dir / "t2_shortlist_momentum_summary.csv", summaries)
    write_json(output_dir / "t2_shortlist_momentum_details.json", details)
    report = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "root": str(root),
        "opportunity_frame": str(args.opportunity_frame),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "rule_count": len(rules),
        "refresh_minutes": refresh_minutes,
        "session_anchor": args.session_anchor,
        "input_rows": int(frame.shape[0]),
        "input_unique_legs": int(frame["row_id"].nunique()) if not frame.empty else 0,
        "definition": {
            "direction_adjustment": "ret_* and ram_* are already signed by T2 direction, so long and short legs compete on the same score scale",
            "snapshot": "each selected clock row is treated as a standalone entry at that clock and exit at actual T2 exit",
            "first_hit": "each T2 leg is counted once at its first shortlist appearance and exits at actual T2 exit",
            "spell": "each contiguous shortlist membership interval is treated as an entry at spell start and exit when the leg leaves the shortlist or reaches T2 exit",
        },
        "top_10_by_spell": ranked_spell[:10],
        "top_10_by_first_hit": ranked_first[:10],
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    write_json(output_dir / "final_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
