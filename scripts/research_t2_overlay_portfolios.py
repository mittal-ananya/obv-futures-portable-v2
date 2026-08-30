#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_continuation_filters as continuation  # noqa: E402
import research_t2_mfe_first_profit_capture as overlay_research  # noqa: E402
import research_t2_overlay_variant_compare as compare  # noqa: E402


SCHEMA = "obvfutport_v2.t2_overlay_portfolio_research.v1"
IST = ZoneInfo("Asia/Kolkata")


@dataclass
class Candidate:
    variant: str
    policy_name: str
    overlay: str
    row_id: str
    symbol: str
    side: str
    entry_epoch: int
    exit_epoch: int
    exit_reason: str
    entry_fill_price: float
    exit_fill_price: float
    margin_per_lot: float
    lot_size: int
    score: float
    score_column: str
    window: pd.DataFrame


@dataclass
class PortfolioHolding:
    candidate: Candidate
    lots: int
    margin_locked: float


@dataclass(frozen=True)
class ReplacementPolicy:
    name: str
    enabled: bool
    score_gap_pct: float = 0.0
    min_hold_minutes: int = 0


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def metric_stats(values: list[float]) -> dict[str, Any]:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return {"count": 0, "sum": None, "min": None, "p10": None, "median": None, "mean": None, "p90": None, "max": None}
    s = pd.Series(clean, dtype="float64")
    return {
        "count": int(s.shape[0]),
        "sum": round(float(s.sum()), 8),
        "min": round(float(s.min()), 8),
        "p10": round(float(s.quantile(0.10)), 8),
        "median": round(float(s.median()), 8),
        "mean": round(float(s.mean()), 8),
        "p90": round(float(s.quantile(0.90)), 8),
        "max": round(float(s.max()), 8),
    }


def source_position_id(row_id: str) -> str:
    parts = str(row_id or "").split("|")
    if len(parts) >= 3 and parts[1] == "T2":
        return parts[2]
    return str(row_id or "")


def candidate_source_exit_epoch(candidate: "Candidate") -> int:
    try:
        values = pd.to_numeric(candidate.window.get("t2_exit_epoch"), errors="coerce").dropna()
        if not values.empty:
            return int(values.iloc[0])
    except Exception:
        pass
    return int(candidate.exit_epoch)


def dedupe_candidates(candidates: list["Candidate"]) -> list["Candidate"]:
    by_key: dict[tuple[str, str, str, int], Candidate] = {}
    for candidate in candidates:
        key = (
            candidate.variant,
            candidate.symbol.upper(),
            source_position_id(candidate.row_id),
            int(candidate.entry_epoch),
        )
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = candidate
            continue
        existing_rank = (candidate_source_exit_epoch(existing), existing.exit_epoch, -existing.score, existing.row_id)
        candidate_rank = (candidate_source_exit_epoch(candidate), candidate.exit_epoch, -candidate.score, candidate.row_id)
        if candidate_rank < existing_rank:
            by_key[key] = candidate
    return sorted(by_key.values(), key=lambda item: (item.entry_epoch, -item.score, item.symbol, item.row_id))


def portfolio_variants() -> list[tuple[str, str, dict[str, Any]]]:
    smooth = "smooth_survivor_tight_risk_score0p80_age60to240_runway0"
    return [
        (
            "smooth_survivor_armed20_floor80",
            smooth,
            {
                "name": "armed20bps_floor80pct_peak",
                "kind": "armed_peak_floor",
                "arm_target": 0.0020,
                "floor_fraction": 0.80,
            },
        ),
        ("smooth_survivor_profit25", smooth, {"name": "profit_25bps", "kind": "profit", "target": 0.0025}),
        ("smooth_survivor_profit30", smooth, {"name": "profit_30bps", "kind": "profit", "target": 0.0030}),
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
        ("smooth_survivor_profit50", smooth, {"name": "profit_50bps", "kind": "profit", "target": 0.0050}),
    ]


def candidate_account(v1_portfolio: Any, candidate: Candidate, exit_fill_price: float, lots: int) -> dict[str, Any]:
    return v1_portfolio.futures_trade_accounting(
        side=candidate.side,
        entry_fill_price=float(candidate.entry_fill_price),
        exit_fill_price=float(exit_fill_price),
        lot_size=int(candidate.lot_size or 1),
        lots=int(lots or 1),
        point_config=None,
    )


def candidate_return_at(candidate: Candidate, epoch: int) -> float | None:
    window = candidate.window
    clocks = pd.to_numeric(window["clock_epoch"], errors="coerce")
    eligible = window.loc[clocks <= int(epoch)]
    if eligible.empty:
        return None
    value = base.as_float(eligible.iloc[-1].get("forward_return"))
    return value


def candidate_score_at(candidate: Candidate, epoch: int, *, max_age_seconds: int = 90) -> float | None:
    window = candidate.window
    clocks = pd.to_numeric(window["clock_epoch"], errors="coerce")
    eligible = window.loc[clocks <= int(epoch)]
    if eligible.empty:
        return None
    row = eligible.iloc[-1]
    row_epoch = base.as_int(row.get("clock_epoch"))
    if row_epoch is None or int(epoch) - int(row_epoch) > max_age_seconds:
        return None
    return base.as_float(row.get(candidate.score_column))


def replacement_exit_price(candidate: Candidate, epoch: int) -> float | None:
    ret = candidate_return_at(candidate, epoch)
    if ret is None:
        return None
    return float(overlay_research.reconstructed_exit_price(candidate.entry_fill_price, candidate.side, ret))


def candidate_mtm(v1_portfolio: Any, holding: PortfolioHolding, epoch: int) -> float:
    ret = candidate_return_at(holding.candidate, epoch)
    if ret is None:
        return 0.0
    exit_price = overlay_research.reconstructed_exit_price(holding.candidate.entry_fill_price, holding.candidate.side, ret)
    acct = candidate_account(v1_portfolio, holding.candidate, exit_price, holding.lots)
    return float(acct.get("net_rupees") or 0.0)


def portfolio_equity(cash: float, holdings: dict[str, PortfolioHolding], v1_portfolio: Any, epoch: int) -> float:
    locked = sum(item.margin_locked for item in holdings.values())
    mtm = sum(candidate_mtm(v1_portfolio, item, epoch) for item in holdings.values())
    return float(cash) + locked + mtm


def choose_lots(
    cash: float,
    equity: float,
    max_positions: int,
    margin_per_lot: float,
    *,
    sizing_mode: str,
    fixed_entry_margin: float | None,
) -> int:
    if sizing_mode == "fixed_entry_margin_unconstrained":
        budget = float(fixed_entry_margin or 0.0)
    else:
        budget = min(max(0.0, cash), max(0.0, equity) / float(max_positions))
    if margin_per_lot <= 0 or budget < margin_per_lot:
        return 0
    return int(math.floor(budget / margin_per_lot))


def max_drawdown(equity_values: list[float]) -> tuple[float, float]:
    peak = None
    max_dd = 0.0
    max_dd_pct = 0.0
    for value in equity_values:
        if peak is None or value > peak:
            peak = value
        if peak and peak > 0:
            dd = peak - value
            dd_pct = dd / peak * 100.0
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = dd_pct
    return max_dd, max_dd_pct


def replacement_policies(names: str) -> list[ReplacementPolicy]:
    result: list[ReplacementPolicy] = []
    for item in str(names or "none").split(","):
        name = item.strip()
        if not name:
            continue
        if name == "none":
            result.append(ReplacementPolicy(name="none", enabled=False))
        elif name == "gap5_hold30":
            result.append(ReplacementPolicy(name=name, enabled=True, score_gap_pct=0.05, min_hold_minutes=30))
        elif name == "gap10_hold30":
            result.append(ReplacementPolicy(name=name, enabled=True, score_gap_pct=0.10, min_hold_minutes=30))
        elif name == "gap5_hold60":
            result.append(ReplacementPolicy(name=name, enabled=True, score_gap_pct=0.05, min_hold_minutes=60))
        elif name == "gap10_hold60":
            result.append(ReplacementPolicy(name=name, enabled=True, score_gap_pct=0.10, min_hold_minutes=60))
        else:
            raise ValueError(f"unknown replacement policy {name!r}")
    return result or [ReplacementPolicy(name="none", enabled=False)]


def build_candidates(
    *,
    frame: pd.DataFrame,
    path_lookup: dict[str, pd.DataFrame],
    policies: dict[str, continuation.ContinuationPolicy],
    v1_portfolio: Any,
    mae_floor: float,
) -> dict[str, list[Candidate]]:
    out: dict[str, list[Candidate]] = {}
    for variant_name, policy_name, config in portfolio_variants():
        policy = policies[policy_name]
        score_column = f"score_{policy.formula}"
        rows = overlay_research.policy_path_rows(frame, path_lookup, policy, mae_floor, include_window=True)
        candidates: list[Candidate] = []
        for row in rows:
            score = base.as_float(row["window"].iloc[0].get(score_column))
            if score is None:
                continue
            exit_row = compare.choose_exit(row, config, score_column, policy.min_score)
            entry_epoch = int(row["qualification_epoch"])
            exit_epoch = int(exit_row["exit_epoch"])
            if exit_epoch <= entry_epoch:
                continue
            exit_reason = str(exit_row["exit_reason"])
            if overlay_research.truthy_flag(row.get("open_at_period_end")) and exit_reason == "underlying_t2_exit":
                exit_reason = "open_at_period_end"
            candidates.append(
                Candidate(
                    variant=variant_name,
                    policy_name=policy_name,
                    overlay=str(config["name"]),
                    row_id=str(row["row_id"]),
                    symbol=str(row["symbol"]),
                    side=str(row["side"]),
                    entry_epoch=entry_epoch,
                    exit_epoch=exit_epoch,
                    exit_reason=exit_reason,
                    entry_fill_price=float(row["entry_fill_price"]),
                    exit_fill_price=float(exit_row["exit_price"]),
                    margin_per_lot=float(row["margin_per_lot"]),
                    lot_size=int(row.get("lot_size") or 1),
                    score=float(score),
                    score_column=score_column,
                    window=row["window"],
                )
            )
        out[variant_name] = dedupe_candidates(candidates)
    return out


def run_portfolio(
    *,
    variant: str,
    candidates: list[Candidate],
    max_positions: int,
    initial_capital: float,
    v1_portfolio: Any,
    sizing_mode: str,
    fixed_entry_margin: float | None,
    replacement_policy: ReplacementPolicy,
) -> dict[str, Any]:
    cash = float(initial_capital)
    holdings: dict[str, PortfolioHolding] = {}
    transactions: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    diagnostics = {
        "entry_candidates": len(candidates),
        "entered": 0,
        "skipped_slot_full": 0,
        "skipped_cash_or_entry_budget": 0,
        "skipped_symbol_already_held": 0,
        "replacement_exits": 0,
        "replacement_blocked_min_hold": 0,
        "replacement_blocked_score_gap": 0,
        "replacement_blocked_no_current_score": 0,
        "replacement_blocked_no_current_exit_price": 0,
        "closed": 0,
    }
    entries_by_epoch: dict[int, list[Candidate]] = {}
    event_epochs: set[int] = set()
    for candidate in candidates:
        entries_by_epoch.setdefault(candidate.entry_epoch, []).append(candidate)
        event_epochs.add(candidate.entry_epoch)
        event_epochs.add(candidate.exit_epoch)
        event_epochs.update(int(epoch) for epoch in pd.to_numeric(candidate.window["clock_epoch"], errors="coerce").dropna().astype(int))
    peak_margin = 0.0
    peak_equity = float(initial_capital)
    for epoch in sorted(event_epochs):
        for row_id, holding in list(holdings.items()):
            candidate = holding.candidate
            if candidate.exit_reason == "open_at_period_end":
                continue
            if candidate.exit_epoch <= epoch:
                acct = candidate_account(v1_portfolio, candidate, candidate.exit_fill_price, holding.lots)
                pnl = float(acct.get("net_rupees") or 0.0)
                cash += holding.margin_locked + pnl
                transactions.append(
                    {
                        "event": "exit",
                        "variant": variant,
                        "max_positions": max_positions,
                        "symbol": candidate.symbol,
                        "side": candidate.side,
                        "row_id": candidate.row_id,
                        "lots": holding.lots,
                        "lot_size": candidate.lot_size,
                        "entry_epoch": candidate.entry_epoch,
                        "entry_time": base.epoch_ist_iso(candidate.entry_epoch),
                        "exit_epoch": candidate.exit_epoch,
                        "exit_time": base.epoch_ist_iso(candidate.exit_epoch),
                        "exit_reason": candidate.exit_reason,
                        "entry_score": candidate.score,
                        "entry_fill_price": candidate.entry_fill_price,
                        "exit_fill_price": candidate.exit_fill_price,
                        "margin_locked": holding.margin_locked,
                        "gross_rupees": acct.get("gross_rupees"),
                        "charges_rupees": acct.get("charges_rupees"),
                        "net_rupees": pnl,
                        "net_pct_margin": (pnl / holding.margin_locked * 100.0) if holding.margin_locked else None,
                    }
                )
                holdings.pop(row_id, None)
                diagnostics["closed"] += 1
        entries = sorted(entries_by_epoch.get(epoch, []), key=lambda item: (item.score, item.symbol, item.row_id), reverse=True)
        for candidate in entries:
            if any(holding.candidate.symbol == candidate.symbol for holding in holdings.values()):
                diagnostics["skipped_symbol_already_held"] += 1
                continue
            replaced_row: dict[str, Any] | None = None
            forced_lots: int | None = None
            if len(holdings) >= max_positions:
                if not replacement_policy.enabled:
                    diagnostics["skipped_slot_full"] += 1
                    continue
                replaceable: list[tuple[float, str, PortfolioHolding]] = []
                blocked_min_hold = 0
                blocked_no_score = 0
                for held_row_id, holding in holdings.items():
                    held_age = int(epoch) - int(holding.candidate.entry_epoch)
                    if held_age < replacement_policy.min_hold_minutes * 60:
                        blocked_min_hold += 1
                        continue
                    held_score = candidate_score_at(holding.candidate, epoch)
                    if held_score is None:
                        blocked_no_score += 1
                        continue
                    replaceable.append((float(held_score), held_row_id, holding))
                if not replaceable:
                    diagnostics["replacement_blocked_min_hold"] += blocked_min_hold
                    diagnostics["replacement_blocked_no_current_score"] += blocked_no_score
                    diagnostics["skipped_slot_full"] += 1
                    continue
                weakest_score, weakest_row_id, weakest = min(replaceable, key=lambda item: (item[0], item[1]))
                if candidate.score <= weakest_score * (1.0 + replacement_policy.score_gap_pct):
                    diagnostics["replacement_blocked_score_gap"] += 1
                    diagnostics["skipped_slot_full"] += 1
                    continue
                exit_price = replacement_exit_price(weakest.candidate, epoch)
                if exit_price is None:
                    diagnostics["replacement_blocked_no_current_exit_price"] += 1
                    diagnostics["skipped_slot_full"] += 1
                    continue
                acct = candidate_account(v1_portfolio, weakest.candidate, exit_price, weakest.lots)
                pnl = float(acct.get("net_rupees") or 0.0)
                cash_after_close = cash + weakest.margin_locked + pnl
                equity_before_close = portfolio_equity(cash, holdings, v1_portfolio, epoch)
                forced_lots = choose_lots(
                    cash_after_close,
                    equity_before_close,
                    max_positions,
                    candidate.margin_per_lot,
                    sizing_mode=sizing_mode,
                    fixed_entry_margin=fixed_entry_margin,
                )
                if forced_lots <= 0:
                    diagnostics["skipped_cash_or_entry_budget"] += 1
                    diagnostics["skipped_slot_full"] += 1
                    continue
                cash = cash_after_close
                holdings.pop(weakest_row_id, None)
                replaced_row = {
                    "event": "exit",
                    "exit_reason": "portfolio_replacement",
                    "replaced_by_row_id": candidate.row_id,
                    "replaced_by_symbol": candidate.symbol,
                    "replacement_policy": replacement_policy.name,
                    "replacement_held_score": weakest_score,
                    "replacement_new_score": candidate.score,
                    "variant": variant,
                    "max_positions": max_positions,
                    "symbol": weakest.candidate.symbol,
                    "side": weakest.candidate.side,
                    "row_id": weakest.candidate.row_id,
                    "lots": weakest.lots,
                    "lot_size": weakest.candidate.lot_size,
                    "entry_epoch": weakest.candidate.entry_epoch,
                    "entry_time": base.epoch_ist_iso(weakest.candidate.entry_epoch),
                    "exit_epoch": int(epoch),
                    "exit_time": base.epoch_ist_iso(epoch),
                    "entry_score": weakest.candidate.score,
                    "entry_fill_price": weakest.candidate.entry_fill_price,
                    "exit_fill_price": exit_price,
                    "margin_locked": weakest.margin_locked,
                    "gross_rupees": acct.get("gross_rupees"),
                    "charges_rupees": acct.get("charges_rupees"),
                    "net_rupees": pnl,
                    "net_pct_margin": (pnl / weakest.margin_locked * 100.0) if weakest.margin_locked else None,
                }
                transactions.append(replaced_row)
                diagnostics["replacement_exits"] += 1
                diagnostics["closed"] += 1
            equity = portfolio_equity(cash, holdings, v1_portfolio, epoch)
            lots = forced_lots or choose_lots(
                cash,
                equity,
                max_positions,
                candidate.margin_per_lot,
                sizing_mode=sizing_mode,
                fixed_entry_margin=fixed_entry_margin,
            )
            if lots <= 0:
                diagnostics["skipped_cash_or_entry_budget"] += 1
                continue
            margin_locked = float(candidate.margin_per_lot) * lots
            cash -= margin_locked
            holdings[candidate.row_id] = PortfolioHolding(candidate=candidate, lots=lots, margin_locked=margin_locked)
            diagnostics["entered"] += 1
            transactions.append(
                {
                    "event": "entry",
                    "variant": variant,
                    "max_positions": max_positions,
                    "replacement_policy": replacement_policy.name,
                    "entry_after_replacement": bool(replaced_row),
                    "symbol": candidate.symbol,
                    "side": candidate.side,
                    "row_id": candidate.row_id,
                    "lots": lots,
                    "lot_size": candidate.lot_size,
                    "entry_epoch": candidate.entry_epoch,
                    "entry_time": base.epoch_ist_iso(candidate.entry_epoch),
                    "entry_score": candidate.score,
                    "entry_fill_price": candidate.entry_fill_price,
                    "margin_locked": margin_locked,
                    "policy_name": candidate.policy_name,
                    "overlay": candidate.overlay,
                    "planned_exit_epoch": candidate.exit_epoch,
                    "planned_exit_time": base.epoch_ist_iso(candidate.exit_epoch),
                    "planned_exit_reason": candidate.exit_reason,
                }
            )
        equity = portfolio_equity(cash, holdings, v1_portfolio, epoch)
        margin_used = sum(item.margin_locked for item in holdings.values())
        peak_margin = max(peak_margin, margin_used)
        peak_equity = max(peak_equity, equity)
        snapshots.append(
            {
                "variant": variant,
                "max_positions": max_positions,
                "replacement_policy": replacement_policy.name,
                "portfolio_id": portfolio_id(variant, max_positions, sizing_mode, fixed_entry_margin, replacement_policy.name),
                "epoch": int(epoch),
                "time": base.epoch_ist_iso(epoch),
                "cash": cash,
                "equity": equity,
                "margin_used": margin_used,
                "open_positions": len(holdings),
            }
        )
    final_epoch = max(event_epochs) if event_epochs else int(time.time())
    final_equity = portfolio_equity(cash, holdings, v1_portfolio, final_epoch)
    equity_values = [float(row["equity"]) for row in snapshots]
    max_dd, max_dd_pct = max_drawdown(equity_values)
    exits = [row for row in transactions if row["event"] == "exit"]
    net_values = [float(row["net_rupees"]) for row in exits]
    net_pct_values = [float(row["net_pct_margin"]) for row in exits if row.get("net_pct_margin") is not None]
    wins = [value for value in net_values if value > 0]
    avg_margin = statistics.mean(float(row["margin_used"]) for row in snapshots) if snapshots else 0.0
    avg_open = statistics.mean(float(row["open_positions"]) for row in snapshots) if snapshots else 0.0
    summary = {
        "portfolio_id": portfolio_id(variant, max_positions, sizing_mode, fixed_entry_margin, replacement_policy.name),
        "variant": variant,
        "max_positions": max_positions,
        "initial_capital": initial_capital,
        "ending_equity": final_equity,
        "return_on_initial_pct": ((final_equity / initial_capital) - 1.0) * 100.0 if initial_capital else None,
        "closed_trades": len(exits),
        "wins": len(wins),
        "losses": len(exits) - len(wins),
        "success_rate_pct": (len(wins) / len(exits) * 100.0) if exits else None,
        "realized_net_rupees": sum(net_values),
        "avg_net_rupees": statistics.mean(net_values) if net_values else None,
        "median_net_rupees": statistics.median(net_values) if net_values else None,
        "worst_trade_rupees": min(net_values) if net_values else None,
        "best_trade_rupees": max(net_values) if net_values else None,
        "avg_net_pct_margin": statistics.mean(net_pct_values) if net_pct_values else None,
        "median_net_pct_margin": statistics.median(net_pct_values) if net_pct_values else None,
        "worst_net_pct_margin": min(net_pct_values) if net_pct_values else None,
        "max_drawdown_rupees": max_dd,
        "max_drawdown_pct": max_dd_pct,
        "current_open_positions": len(holdings),
        "current_margin_rupees": sum(item.margin_locked for item in holdings.values()),
        "peak_margin_rupees": peak_margin,
        "avg_margin_rupees": avg_margin,
        "peak_margin_to_initial_pct": (peak_margin / initial_capital * 100.0) if initial_capital else None,
        "avg_margin_to_initial_pct": (avg_margin / initial_capital * 100.0) if initial_capital else None,
        "return_on_peak_margin_pct": (sum(net_values) / peak_margin * 100.0) if peak_margin else None,
        "drawdown_on_peak_margin_pct": (max_dd / peak_margin * 100.0) if peak_margin else None,
        "return_to_drawdown_on_peak_margin": (sum(net_values) / max_dd) if max_dd else None,
        "avg_open_positions": avg_open,
        "peak_equity_rupees": peak_equity,
        "sizing_mode": sizing_mode,
        "fixed_entry_margin": fixed_entry_margin,
        "replacement_policy": replacement_policy.name,
        "replacement_score_gap_pct": replacement_policy.score_gap_pct,
        "replacement_min_hold_minutes": replacement_policy.min_hold_minutes,
        "diagnostics": diagnostics,
    }
    return {"summary": summary, "transactions": transactions, "snapshots": snapshots}


def flatten_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "portfolio_id": row.get("portfolio_id"),
        "variant": row.get("variant"),
        "max_positions": row.get("max_positions"),
        "replacement_policy": row.get("replacement_policy"),
        "replacement_score_gap_pct": row.get("replacement_score_gap_pct"),
        "replacement_min_hold_minutes": row.get("replacement_min_hold_minutes"),
        "initial_capital": row.get("initial_capital"),
        "ending_equity": row.get("ending_equity"),
        "return_on_initial_pct": row.get("return_on_initial_pct"),
        "closed_trades": row.get("closed_trades"),
        "wins": row.get("wins"),
        "losses": row.get("losses"),
        "success_rate_pct": row.get("success_rate_pct"),
        "realized_net_rupees": row.get("realized_net_rupees"),
        "avg_net_rupees": row.get("avg_net_rupees"),
        "median_net_rupees": row.get("median_net_rupees"),
        "worst_trade_rupees": row.get("worst_trade_rupees"),
        "best_trade_rupees": row.get("best_trade_rupees"),
        "avg_net_pct_margin": row.get("avg_net_pct_margin"),
        "median_net_pct_margin": row.get("median_net_pct_margin"),
        "worst_net_pct_margin": row.get("worst_net_pct_margin"),
        "max_drawdown_rupees": row.get("max_drawdown_rupees"),
        "max_drawdown_pct": row.get("max_drawdown_pct"),
        "current_open_positions": row.get("current_open_positions"),
        "current_margin_rupees": row.get("current_margin_rupees"),
        "peak_margin_rupees": row.get("peak_margin_rupees"),
        "avg_margin_rupees": row.get("avg_margin_rupees"),
        "peak_margin_to_initial_pct": row.get("peak_margin_to_initial_pct"),
        "avg_margin_to_initial_pct": row.get("avg_margin_to_initial_pct"),
        "return_on_peak_margin_pct": row.get("return_on_peak_margin_pct"),
        "drawdown_on_peak_margin_pct": row.get("drawdown_on_peak_margin_pct"),
        "return_to_drawdown_on_peak_margin": row.get("return_to_drawdown_on_peak_margin"),
        "avg_open_positions": row.get("avg_open_positions"),
        "peak_equity_rupees": row.get("peak_equity_rupees"),
        "sizing_mode": row.get("sizing_mode"),
        "fixed_entry_margin": row.get("fixed_entry_margin"),
        "diagnostics": json.dumps(row.get("diagnostics") or {}, sort_keys=True),
    }


def portfolio_id(
    variant: str,
    max_positions: int,
    sizing_mode: str,
    fixed_entry_margin: float | None,
    replacement_policy: str,
) -> str:
    fixed = "na" if fixed_entry_margin is None else str(int(round(float(fixed_entry_margin))))
    return f"{sizing_mode}|fixed{fixed}|{replacement_policy}|{variant}|cap{max_positions}"


def daily_returns_from_snapshots(summary: dict[str, Any], snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = {}
    for row in snapshots:
        day = datetime.fromtimestamp(int(row["epoch"]), IST).date().isoformat()
        current = by_day.get(day)
        if current is None or int(row["epoch"]) >= int(current["epoch"]):
            by_day[day] = row
    out: list[dict[str, Any]] = []
    previous_equity = float(summary["initial_capital"])
    for day in sorted(by_day):
        row = by_day[day]
        equity = float(row["equity"])
        pnl = equity - previous_equity
        out.append(
            {
                "portfolio_id": summary["portfolio_id"],
                "variant": summary["variant"],
                "max_positions": summary["max_positions"],
                "replacement_policy": summary["replacement_policy"],
                "sizing_mode": summary["sizing_mode"],
                "fixed_entry_margin": summary["fixed_entry_margin"],
                "trade_date": day,
                "eod_epoch": int(row["epoch"]),
                "eod_equity": equity,
                "daily_pnl_rupees": pnl,
                "daily_return_pct": (pnl / previous_equity * 100.0) if previous_equity else None,
                "eod_margin_used": row["margin_used"],
                "eod_open_positions": row["open_positions"],
            }
        )
        previous_equity = equity
    return out


def write_daily_correlations(output_dir: Path, daily_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not daily_rows:
        return {"count": 0}
    daily = pd.DataFrame(daily_rows)
    pivot = daily.pivot_table(index="trade_date", columns="portfolio_id", values="daily_return_pct", aggfunc="last")
    corr = pivot.corr(min_periods=2)
    corr.to_csv(output_dir / "daily_return_correlation_matrix.csv")
    pairs: list[dict[str, Any]] = []
    columns = list(corr.columns)
    for i, left in enumerate(columns):
        for right in columns[i + 1 :]:
            value = corr.loc[left, right]
            if pd.isna(value):
                continue
            pairs.append({"portfolio_a": left, "portfolio_b": right, "correlation": float(value)})
    pairs.sort(key=lambda row: row["correlation"])
    write_csv(output_dir / "daily_return_pairwise_correlations.csv", pairs)
    values = [row["correlation"] for row in pairs]
    return {
        "portfolio_count": len(columns),
        "trade_dates": list(pivot.index),
        "pair_count": len(pairs),
        "min_correlation": min(values) if values else None,
        "median_correlation": statistics.median(values) if values else None,
        "mean_correlation": statistics.mean(values) if values else None,
        "max_correlation": max(values) if values else None,
        "lowest_pairs": pairs[:10],
        "highest_pairs": pairs[-10:],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--initial-capital", type=float, default=2_000_000.0)
    parser.add_argument("--max-position-grid", default="3,4,5,6,7,8,10")
    parser.add_argument("--mae-floor", type=float, default=0.0005)
    parser.add_argument(
        "--sizing-mode",
        choices=["dynamic_equity_cash_constrained", "fixed_entry_margin_unconstrained"],
        default="dynamic_equity_cash_constrained",
    )
    parser.add_argument("--fixed-entry-margin", type=float, default=None)
    parser.add_argument(
        "--replacement-policy-grid",
        default="none",
        help="comma-separated: none,gap5_hold30,gap10_hold30,gap5_hold60,gap10_hold60",
    )
    args = parser.parse_args()

    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    base.add_paths(args.root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    frame = pd.read_parquet(args.opportunity_frame)
    path_lookup = overlay_research.build_path_lookup(frame)
    policies = {policy.name: policy for policy in continuation.continuation_policy_grid()}
    candidates_by_variant = build_candidates(
        frame=frame,
        path_lookup=path_lookup,
        policies=policies,
        v1_portfolio=v1_portfolio,
        mae_floor=float(args.mae_floor),
    )
    caps = [int(item.strip()) for item in str(args.max_position_grid).split(",") if item.strip()]
    all_summaries: list[dict[str, Any]] = []
    all_transactions: list[dict[str, Any]] = []
    all_daily_returns: list[dict[str, Any]] = []
    repl_policies = replacement_policies(args.replacement_policy_grid)
    for variant, candidates in candidates_by_variant.items():
        for repl in repl_policies:
            for cap in caps:
                result = run_portfolio(
                    variant=variant,
                    candidates=candidates,
                    max_positions=cap,
                    initial_capital=float(args.initial_capital),
                    v1_portfolio=v1_portfolio,
                    sizing_mode=str(args.sizing_mode),
                    fixed_entry_margin=args.fixed_entry_margin,
                    replacement_policy=repl,
                )
                all_summaries.append(result["summary"])
                all_transactions.extend(result["transactions"])
                all_daily_returns.extend(daily_returns_from_snapshots(result["summary"], result["snapshots"]))
                write_json(
                    args.output_dir / "progress.json",
                    {
                        "phase": "portfolio_grid",
                        "latest_variant": variant,
                        "latest_replacement_policy": repl.name,
                        "latest_max_positions": cap,
                        "completed_cases": len(all_summaries),
                        "total_cases": len(candidates_by_variant) * len(caps) * len(repl_policies),
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    },
                )
    summary_rows = [flatten_summary(row) for row in all_summaries]
    write_csv(args.output_dir / "portfolio_summary.csv", summary_rows)
    write_csv(args.output_dir / "portfolio_transactions.csv", all_transactions)
    write_csv(args.output_dir / "portfolio_daily_returns.csv", all_daily_returns)
    correlation_summary = write_daily_correlations(args.output_dir, all_daily_returns)
    ranked = sorted(
        summary_rows,
        key=lambda row: (
            float(row.get("return_on_peak_margin_pct") or -1e9),
            -float(row.get("drawdown_on_peak_margin_pct") or 1e9),
            float(row.get("success_rate_pct") or -1e9),
        ),
        reverse=True,
    )
    risk_ranked = sorted(
        summary_rows,
        key=lambda row: (
            -float(row.get("drawdown_on_peak_margin_pct") or 1e9),
            float(row.get("return_on_peak_margin_pct") or -1e9),
            float(row.get("success_rate_pct") or -1e9),
        ),
        reverse=True,
    )
    report = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "opportunity_frame": str(args.opportunity_frame),
        "output_dir": str(args.output_dir),
        "definition": {
            "capital": "margin-based futures portfolio; unused capital remains cash",
            "position_sizing": (
                "new position can use fixed_entry_margin regardless of available cash"
                if args.sizing_mode == "fixed_entry_margin_unconstrained"
                else "new position can use at most current portfolio equity / max_positions, further capped by available cash"
            ),
            "replacement": (
                "controlled replacement enabled according to replacement_policy_grid"
                if any(policy.enabled for policy in repl_policies)
                else "disabled in this pass; exits are only natural overlay exits"
            ),
            "controlled_replacement": (
                "when enabled, a new current entry can replace the weakest held leg only after min hold and only if its score exceeds the held score by the configured gap"
            ),
            "entry_priority": "same-minute candidates sorted by point-in-time policy score, descending",
            "same_symbol": "only one open position per symbol per portfolio",
            "accounting": "same net futures per-lot accounting and overlay exits used in the standalone T2 overlay comparison",
        },
        "sizing_mode": str(args.sizing_mode),
        "fixed_entry_margin": args.fixed_entry_margin,
        "initial_capital": float(args.initial_capital),
        "max_position_grid": caps,
        "replacement_policy_grid": [policy.__dict__ for policy in repl_policies],
        "candidate_counts": {key: len(value) for key, value in candidates_by_variant.items()},
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "top_by_return": ranked[:10],
        "top_by_risk": risk_ranked[:10],
        "correlation_summary": correlation_summary,
    }
    write_json(args.output_dir / "final_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
