#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import sys
import tempfile
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_continuation_filters as continuation  # noqa: E402
import research_t2_mfe_first_profit_capture as overlay_research  # noqa: E402
import research_t2_overlay_variant_compare as overlay_compare  # noqa: E402
import research_t2_overlay_portfolios as portfolio_research  # noqa: E402
import run_v2matrix_overlay as live_overlay  # noqa: E402
from backfill_v2matrix_history import enriched_matrix_events, load_matrix_module, now_ist_iso, write_json, write_jsonl  # noqa: E402


SCHEMA = "obvfutport_v2.v2matrix_research_portfolio_install.v1"
DEFAULT_VARIANTS = live_overlay.PORTFOLIO_VARIANTS


def json_clean(value: Any) -> Any:
    return live_overlay.json_clean(value)


def finite_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def source_position_id(row_id: str) -> str:
    parts = str(row_id or "").split("|")
    if len(parts) >= 3 and parts[1] == "T2":
        return parts[2]
    return str(row_id or "")


def live_row_id(row_id: str, *, symbol: str, entry_epoch: int) -> str:
    position_id = source_position_id(row_id)
    if position_id:
        return f"{str(symbol).upper()}|T2|{position_id}|{int(entry_epoch)}"
    return str(row_id)


def epoch_ist_iso(epoch: Any) -> str | None:
    value = base.as_int(epoch)
    return base.epoch_ist_iso(value) if value is not None else None


def portfolio_key(variant: str) -> str:
    return live_overlay.portfolio_key(variant)


def variant_overlay_name(variant: str) -> str:
    return str(live_overlay.variant_config(variant).get("name") or variant)


def expected_summary_from_csv(path: Path, *, variants: set[str], max_positions: int | None) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    out: dict[str, dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            variant = str(row.get("variant") or "")
            if variant not in variants:
                continue
            if max_positions is not None and str(row.get("max_positions") or "") != str(max_positions):
                continue
            repl = str(row.get("replacement_policy") or "none")
            if repl != "none":
                continue
            out[variant] = dict(row)
    return out


def numeric_close(left: Any, right: Any, *, tolerance: float) -> bool:
    lnum = finite_float(left)
    rnum = finite_float(right)
    if lnum is None and rnum is None:
        return True
    if lnum is None or rnum is None:
        return False
    return abs(lnum - rnum) <= tolerance


def int_equal(left: Any, right: Any) -> bool:
    try:
        return int(float(left)) == int(float(right))
    except (TypeError, ValueError):
        return left == right


def compare_summary_rows(
    *,
    variant: str,
    computed: dict[str, Any],
    expected: dict[str, Any],
    tolerance_rupees: float,
) -> list[dict[str, Any]]:
    checks = {
        "closed_trades": int_equal,
        "wins": int_equal,
        "losses": int_equal,
        "current_open_positions": int_equal,
        "realized_net_rupees": lambda a, b: numeric_close(a, b, tolerance=tolerance_rupees),
        "peak_margin_rupees": lambda a, b: numeric_close(a, b, tolerance=tolerance_rupees),
        "return_on_initial_pct": lambda a, b: numeric_close(a, b, tolerance=1e-6),
        "success_rate_pct": lambda a, b: numeric_close(a, b, tolerance=1e-6),
    }
    mismatches: list[dict[str, Any]] = []
    for field, checker in checks.items():
        if field not in expected:
            continue
        if not checker(computed.get(field), expected.get(field)):
            mismatches.append(
                {
                    "variant": variant,
                    "field": field,
                    "computed": computed.get(field),
                    "expected": expected.get(field),
                }
            )
    return mismatches


def portfolio_state_from_research_result(
    *,
    variant: str,
    result: dict[str, Any],
    initial_capital: float,
) -> dict[str, Any]:
    definition = live_overlay.portfolio_def(variant)
    key = portfolio_key(variant)
    portfolio = {
        "portfolio_id": key,
        "source_research_portfolio_id": result["summary"].get("portfolio_id"),
        "variant": variant,
        "label": definition.label,
        "rule": (
            f"fixed Rs 5L per entry / no replacement / max {definition.max_positions}"
            f" / requalify={definition.requalify}"
            f" / cooldown={definition.cooldown_minutes}m"
            f" / max_entries_per_t2_leg={definition.max_entries_per_t2_leg}"
        ),
        "max_positions": definition.max_positions,
        "fixed_entry_margin_rupees": definition.fixed_entry_margin,
        "requalify": definition.requalify,
        "cooldown_minutes": definition.cooldown_minutes,
        "max_entries_per_t2_leg": definition.max_entries_per_t2_leg,
        "cash_rupees": float(initial_capital),
        "peak_margin_rupees": 0.0,
        "holdings": {},
        "transactions": [],
        "diagnostics": result["summary"].get("diagnostics") or {},
        "installed_from_research": True,
    }
    holdings = portfolio["holdings"]
    for raw in result.get("transactions", []):
        row = dict(raw)
        research_row_id = str(row.get("row_id") or "")
        symbol = str(row.get("symbol") or "").upper()
        entry_epoch = base.as_int(row.get("entry_epoch")) or 0
        mapped_row_id = live_row_id(research_row_id, symbol=symbol, entry_epoch=entry_epoch)
        overlay_key_value = live_overlay.overlay_key(variant, mapped_row_id, entry_epoch)
        row["portfolio_id"] = key
        row["source_research_portfolio_id"] = result["summary"].get("portfolio_id")
        row["research_row_id"] = research_row_id
        row["row_id"] = mapped_row_id
        row["overlay_key"] = overlay_key_value
        row["source_t2_position_id"] = source_position_id(research_row_id)
        row["history_backfilled"] = True
        row["installed_from_research"] = True
        if row.get("event") == "entry":
            margin = float(row.get("margin_locked") or 0.0)
            portfolio["cash_rupees"] = float(portfolio["cash_rupees"]) - margin
            holdings[overlay_key_value] = {
                "overlay_key": overlay_key_value,
                "row_id": mapped_row_id,
                "research_row_id": research_row_id,
                "position_id": row["source_t2_position_id"],
                "symbol": symbol,
                "side": row.get("side"),
                "lots": int(float(row.get("lots") or 0)),
                "lot_size": int(float(row.get("lot_size") or 1)),
                "margin_locked": margin,
                "entry_epoch": entry_epoch,
                "entry_time": row.get("entry_time") or epoch_ist_iso(entry_epoch),
                "entry_fill_price": float(row.get("entry_fill_price") or 0.0),
                "entry_ltp_price": float(row.get("entry_fill_price") or 0.0),
                "entry_score": float(row.get("entry_score") or 0.0),
            }
            portfolio["last_event_at_ist"] = row.get("entry_time")
        elif row.get("event") == "exit":
            margin = float(row.get("margin_locked") or 0.0)
            pnl = float(row.get("net_rupees") or 0.0)
            portfolio["cash_rupees"] = float(portfolio["cash_rupees"]) + margin + pnl
            holdings.pop(overlay_key_value, None)
            portfolio["last_event_at_ist"] = row.get("exit_time")
        portfolio["transactions"].append(row)
        current_margin = sum(float(item.get("margin_locked") or 0.0) for item in holdings.values())
        portfolio["peak_margin_rupees"] = max(float(portfolio["peak_margin_rupees"]), current_margin)
    portfolio["peak_margin_rupees"] = max(
        float(portfolio["peak_margin_rupees"]),
        float(result["summary"].get("peak_margin_rupees") or 0.0),
    )
    return portfolio


def all_qualified_signal_summary(
    *,
    variant: str,
    candidates: list[portfolio_research.Candidate],
    v1_portfolio: Any,
    reference: str,
) -> dict[str, Any]:
    net_values: list[float] = []
    for candidate in candidates:
        account = portfolio_research.candidate_account(v1_portfolio, candidate, candidate.exit_fill_price, 1)
        net_values.append(float(account.get("net_rupees") or 0.0))
    wins = [value for value in net_values if value > 0]
    return {
        "variant": variant,
        "all_qualified_signal_trade_count": len(net_values),
        "all_qualified_signal_win_count": len(wins),
        "all_qualified_signal_loss_count": len(net_values) - len(wins),
        "all_qualified_signal_success_rate_pct": (len(wins) / len(net_values) * 100.0) if net_values else None,
        "all_qualified_signal_net_rupees_per_lot": sum(net_values),
        "all_qualified_signal_reference": reference,
    }


def matrix_payload_from_candidate(
    *,
    candidate: portfolio_research.Candidate,
    event_type: str,
    event_epoch: int,
    event_price: float,
    position: live_overlay.OverlayPosition,
    exit_reason: str | None = None,
) -> dict[str, Any]:
    row_id = live_row_id(candidate.row_id, symbol=candidate.symbol, entry_epoch=candidate.entry_epoch)
    position_id = source_position_id(candidate.row_id)
    side = "long" if str(candidate.side).lower() == "long" else "short"
    return {
        "event_id": f"V2MATRIX:{candidate.variant}:{event_type}:{position_id}:{event_epoch}",
        "source_strategy": "OBVFUTPORT_V2_T2_SMOOTH_SURVIVOR",
        "source_model_version": "v2matrix_overlay_research_install_v1",
        "instrument_id": candidate.symbol,
        "instrument_name": candidate.symbol,
        "event_type": event_type,
        "side": side,
        "tranche": "T2_OVERLAY",
        "position_closed": event_type != "paper_entry",
        "trigger_time_ist": epoch_ist_iso(event_epoch),
        "trigger_price_underlying": event_price,
        "trigger_price_source": "historical_research_execution_fill_proxy",
        "trigger_price_instrument_key": "",
        "current_price_underlying": event_price,
        "current_time_ist": epoch_ist_iso(event_epoch),
        "current_price_source": "historical_research_execution_fill_proxy",
        "signal_source": "",
        "signal_instrument_key": "",
        "execution_instrument_key": "",
        "display_price_source": "historical_research_execution_fill_proxy",
        "execution_price_source": "v2_futures_execution_contract",
        "matrix_selected_leg": "T2_smooth_survivor_overlay",
        "matrix_selection_rule": candidate.variant,
        "overlay_variant": candidate.variant,
        "overlay_policy": candidate.policy_name,
        "overlay_score": candidate.score,
        "overlay_row_id": row_id,
        "research_row_id": candidate.row_id,
        "position_id": f"{candidate.variant}:{position_id}",
        "signal_id": position_id,
        "source_t2_position_id": position_id,
        "matrix_entry_time_ist": position.entry_time,
        "matrix_entry_price_underlying": position.entry_ltp_price,
        "matrix_entry_price_source": "historical_research_execution_fill_proxy",
        "exit_reason": exit_reason,
        "created_at_ist": now_ist_iso(),
    }


def numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([math.nan] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def passing_rows(frame: pd.DataFrame, policy: live_overlay.portfolio_rules.Policy) -> pd.DataFrame:
    if frame.empty:
        return frame
    score_column = f"score_{policy.formula}"
    score = numeric_series(frame, score_column)
    mask = score >= policy.min_score
    mask &= numeric_series(frame, "age_minutes") >= policy.min_age_minutes
    if policy.max_age_minutes is not None:
        mask &= numeric_series(frame, "age_minutes") <= float(policy.max_age_minutes)
    if policy.min_current_ret is not None:
        mask &= numeric_series(frame, "current_ret") >= float(policy.min_current_ret)
    if policy.min_mfe is not None:
        mask &= numeric_series(frame, "mfe") >= float(policy.min_mfe)
    if policy.max_mae_abs is not None:
        mask &= numeric_series(frame, "mae_abs") <= float(policy.max_mae_abs)
    if policy.max_drawdown_to_mfe is not None:
        mask &= numeric_series(frame, "drawdown_to_mfe") <= float(policy.max_drawdown_to_mfe)
    if policy.min_positive_ram_count:
        mask &= numeric_series(frame, "positive_ram_count") >= int(policy.min_positive_ram_count)
    if policy.max_spread_bps is not None:
        mask &= numeric_series(frame, "spread_bps") <= float(policy.max_spread_bps)
    if policy.min_edge_cost_multiple is not None:
        mask &= numeric_series(frame, "edge_to_cost_multiple") >= float(policy.min_edge_cost_multiple)
    if policy.min_minutes_to_session_end is not None:
        mask &= numeric_series(frame, "minutes_to_session_end") >= float(policy.min_minutes_to_session_end)
    passed = frame.loc[mask].copy()
    if passed.empty:
        return passed
    passed["portfolio_score"] = score.loc[passed.index]
    return passed.sort_values(["row_id", "clock_epoch"], kind="mergesort")


def candidate_from_metrics(
    *,
    variant: str,
    policy: live_overlay.portfolio_rules.Policy,
    config: dict[str, Any],
    row: dict[str, Any],
) -> portfolio_research.Candidate | None:
    score_column = f"score_{policy.formula}"
    score = base.as_float(row["window"].iloc[0].get(score_column))
    if score is None:
        return None
    exit_row = overlay_compare.choose_exit(row, config, score_column, policy.min_score)
    entry_epoch = int(row["qualification_epoch"])
    exit_epoch = int(exit_row["exit_epoch"])
    if exit_epoch <= entry_epoch:
        return None
    return portfolio_research.Candidate(
        variant=variant,
        policy_name=policy.name,
        overlay=str(config["name"]),
        row_id=str(row["row_id"]),
        symbol=str(row["symbol"]),
        side=str(row["side"]),
        entry_epoch=entry_epoch,
        exit_epoch=exit_epoch,
        exit_reason=str(exit_row["exit_reason"]),
        entry_fill_price=float(row["entry_fill_price"]),
        exit_fill_price=float(exit_row["exit_price"]),
        margin_per_lot=float(row["margin_per_lot"]),
        lot_size=int(row.get("lot_size") or 1),
        score=float(score),
        score_column=score_column,
        window=row["window"],
    )


def build_candidates_for_definition(
    *,
    frame: pd.DataFrame,
    path_lookup: dict[str, pd.DataFrame],
    definition: live_overlay.PortfolioVariantDefinition,
    mae_floor: float,
) -> list[portfolio_research.Candidate]:
    passed = passing_rows(frame, definition.policy)
    if passed.empty:
        return []
    config = dict(definition.overlay)
    candidates: list[portfolio_research.Candidate] = []
    if not definition.requalify:
        first = passed.drop_duplicates("row_id", keep="first")
        for raw in first.itertuples(index=False):
            metrics = overlay_research.path_metrics(raw, path_lookup, mae_floor, include_window=True)
            if not metrics.get("ok"):
                continue
            candidate = candidate_from_metrics(variant=definition.name, policy=definition.policy, config=config, row=metrics)
            if candidate is not None:
                candidates.append(candidate)
        return portfolio_research.dedupe_candidates(candidates)

    cooldown_seconds = int(definition.cooldown_minutes) * 60
    for _row_id, group in passed.groupby("row_id", sort=False):
        next_allowed_epoch = -1
        entry_count = 0
        for raw in group.itertuples(index=False):
            clock_epoch = base.as_int(getattr(raw, "clock_epoch", None))
            if clock_epoch is None:
                continue
            if clock_epoch < next_allowed_epoch:
                continue
            if entry_count >= int(definition.max_entries_per_t2_leg):
                break
            metrics = overlay_research.path_metrics(raw, path_lookup, mae_floor, include_window=True)
            if not metrics.get("ok"):
                continue
            candidate = candidate_from_metrics(variant=definition.name, policy=definition.policy, config=config, row=metrics)
            if candidate is None:
                continue
            candidates.append(candidate)
            entry_count += 1
            next_allowed_epoch = int(candidate.exit_epoch) + cooldown_seconds
    return portfolio_research.dedupe_candidates(candidates)


def build_live_variant_candidates(
    *,
    frame: pd.DataFrame,
    path_lookup: dict[str, pd.DataFrame],
    variants: tuple[str, ...],
    mae_floor: float,
) -> dict[str, list[portfolio_research.Candidate]]:
    out: dict[str, list[portfolio_research.Candidate]] = {}
    for variant in variants:
        definition = live_overlay.portfolio_def(variant)
        out[variant] = build_candidates_for_definition(
            frame=frame,
            path_lookup=path_lookup,
            definition=definition,
            mae_floor=mae_floor,
        )
    return out


def overlay_state_from_candidates(
    *,
    candidates_by_variant: dict[str, list[portfolio_research.Candidate]],
    variants: tuple[str, ...],
    primary_variant: str,
    final_epoch: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    state: dict[str, Any] = {
        "schema": live_overlay.SCHEMA,
        "created_at_ist": now_ist_iso(),
        "primary_variant": primary_variant,
        "portfolio_variants": list(variants),
        "active_overlay": {},
        "completed_overlay_keys": [],
        "completed_overlay_history": {},
        "posted_event_ids": [],
        "installed_from_research": True,
    }
    overlay_events: list[dict[str, Any]] = []
    matrix_payloads: list[dict[str, Any]] = []
    completed: set[str] = set()
    active = state["active_overlay"]
    for variant in variants:
        for candidate in candidates_by_variant.get(variant, []):
            row_id = live_row_id(candidate.row_id, symbol=candidate.symbol, entry_epoch=candidate.entry_epoch)
            key = live_overlay.overlay_key(variant, row_id, int(candidate.entry_epoch))
            position_id = source_position_id(candidate.row_id)
            position = live_overlay.OverlayPosition(
                variant=variant,
                row_id=row_id,
                position_id=position_id,
                symbol=candidate.symbol,
                side=candidate.side,
                entry_epoch=int(candidate.entry_epoch),
                entry_time=epoch_ist_iso(candidate.entry_epoch),
                entry_fill_price=float(candidate.entry_fill_price),
                entry_ltp_price=float(candidate.entry_fill_price),
                entry_score=float(candidate.score),
                entry_features={
                    "portfolio_score": float(candidate.score),
                    "policy_name": candidate.policy_name,
                    "overlay": candidate.overlay,
                    "research_row_id": candidate.row_id,
                },
            )
            entry_event = {
                "schema": live_overlay.SCHEMA,
                "event": "overlay_entry",
                "overlay_key": key,
                "variant": variant,
                "policy": candidate.policy_name,
                "source_t2_position_id": position_id,
                "row_id": row_id,
                "research_row_id": candidate.row_id,
                "symbol": candidate.symbol,
                "side": candidate.side,
                "entry_epoch": int(candidate.entry_epoch),
                "entry_time": epoch_ist_iso(candidate.entry_epoch),
                "entry_score": float(candidate.score),
                "entry_fill_price": float(candidate.entry_fill_price),
                "entry_ltp_price": float(candidate.entry_fill_price),
                "history_backfilled": True,
                "installed_from_research": True,
                "created_at_ist": now_ist_iso(),
            }
            overlay_events.append(entry_event)
            if variant == primary_variant:
                matrix_payloads.append(
                    matrix_payload_from_candidate(
                        candidate=candidate,
                        event_type="paper_entry",
                        event_epoch=int(candidate.entry_epoch),
                        event_price=float(candidate.entry_fill_price),
                        position=position,
                    )
                )
            if int(candidate.exit_epoch) <= int(final_epoch):
                completed.add(key)
                live_overlay.record_completed_overlay(
                    state,
                    variant=variant,
                    row_id=row_id,
                    key=key,
                    exit_epoch=int(candidate.exit_epoch),
                    exit_reason=candidate.exit_reason,
                )
                exit_event = {
                    "schema": live_overlay.SCHEMA,
                    "event": "overlay_exit",
                    "overlay_key": key,
                    "variant": variant,
                    "policy": candidate.policy_name,
                    "source_t2_position_id": position_id,
                    "row_id": row_id,
                    "research_row_id": candidate.row_id,
                    "symbol": candidate.symbol,
                    "side": candidate.side,
                    "exit_epoch": int(candidate.exit_epoch),
                    "exit_time": epoch_ist_iso(candidate.exit_epoch),
                    "exit_reason": candidate.exit_reason,
                    "entry_epoch": int(candidate.entry_epoch),
                    "entry_time": epoch_ist_iso(candidate.entry_epoch),
                    "entry_fill_price": float(candidate.entry_fill_price),
                    "exit_fill_price": float(candidate.exit_fill_price),
                    "exit_ltp_price": float(candidate.exit_fill_price),
                    "history_backfilled": True,
                    "installed_from_research": True,
                    "created_at_ist": now_ist_iso(),
                }
                overlay_events.append(exit_event)
                if variant == primary_variant:
                    matrix_payloads.append(
                        matrix_payload_from_candidate(
                            candidate=candidate,
                            event_type="tranche2_exit",
                            event_epoch=int(candidate.exit_epoch),
                            event_price=float(candidate.exit_fill_price),
                            position=position,
                            exit_reason=candidate.exit_reason,
                        )
                    )
            else:
                returns = pd.to_numeric(candidate.window.get("forward_return"), errors="coerce").dropna()
                peak = float(returns.max()) if not returns.empty else 0.0
                position.peak_return = max(0.0, peak)
                config = live_overlay.variant_config(variant)
                if str(config.get("kind")) == "armed_peak_floor":
                    position.armed = position.peak_return >= float(config.get("arm_target") or 0.0)
                active[key] = asdict(position)
    state["completed_overlay_keys"] = sorted(completed)
    state["posted_event_ids"] = sorted(str(row.get("event_id") or "") for row in matrix_payloads if row.get("event_id"))
    state["updated_at_ist"] = now_ist_iso()
    state["last_clock"] = {
        "clock_epoch": final_epoch,
        "clock_time_ist": epoch_ist_iso(final_epoch),
        "in_session": False,
        "history_backfilled": True,
        "installed_from_research": True,
        "created_overlay_events": len(overlay_events),
        "matrix_events": len(matrix_payloads),
        "active_overlay_count": len(active),
        "completed_overlay_count": len(completed),
    }
    return state, overlay_events, matrix_payloads, {
        "overlay_events": len(overlay_events),
        "matrix_payloads": len(matrix_payloads),
        "active_overlay_count": len(active),
        "completed_overlay_count": len(completed),
    }


def summarize_installed_portfolio(portfolio: dict[str, Any]) -> dict[str, Any]:
    transactions = [row for row in portfolio.get("transactions", []) if isinstance(row, dict)]
    exits = [row for row in transactions if row.get("event") == "exit"]
    wins = [row for row in exits if float(row.get("net_rupees") or 0.0) > 0]
    realized = sum(float(row.get("net_rupees") or 0.0) for row in exits)
    peak_margin = float(portfolio.get("peak_margin_rupees") or 0.0)
    portfolio_success = (len(wins) / len(exits) * 100.0) if exits else None
    variant = str(portfolio.get("variant") or "")
    definition = live_overlay.portfolio_def(variant) if variant in live_overlay.PORTFOLIO_DEFINITIONS else None
    return {
        "portfolio_id": portfolio.get("portfolio_id"),
        "variant": variant,
        "label": definition.label if definition is not None else portfolio.get("label"),
        "rule": portfolio.get("rule"),
        "max_positions": definition.max_positions if definition is not None else portfolio.get("max_positions"),
        "fixed_entry_margin_rupees": definition.fixed_entry_margin if definition is not None else portfolio.get("fixed_entry_margin_rupees"),
        "requalify": definition.requalify if definition is not None else portfolio.get("requalify"),
        "cooldown_minutes": definition.cooldown_minutes if definition is not None else portfolio.get("cooldown_minutes"),
        "max_entries_per_t2_leg": definition.max_entries_per_t2_leg if definition is not None else portfolio.get("max_entries_per_t2_leg"),
        "open_positions": len(portfolio.get("holdings") or {}),
        "closed_trades": len(exits),
        "wins": len(wins),
        "losses": len(exits) - len(wins),
        "success_rate_pct": portfolio_success,
        "portfolio_closed_success_rate_pct": portfolio_success,
        "portfolio_closed_trade_count": len(exits),
        "portfolio_closed_win_count": len(wins),
        "all_qualified_signal_success_rate_pct": portfolio.get("all_qualified_signal_success_rate_pct"),
        "all_qualified_signal_trade_count": portfolio.get("all_qualified_signal_trade_count"),
        "all_qualified_signal_win_count": portfolio.get("all_qualified_signal_win_count"),
        "all_qualified_signal_reference": portfolio.get("all_qualified_signal_reference"),
        "realized_net_rupees": realized,
        "unrealized_net_rupees": 0.0,
        "total_net_rupees": realized,
        "current_margin_rupees": sum(float(item.get("margin_locked") or 0.0) for item in (portfolio.get("holdings") or {}).values() if isinstance(item, dict)),
        "peak_margin_rupees": peak_margin,
        "return_on_peak_margin_pct": (realized / peak_margin * 100.0) if peak_margin else None,
        "installed_from_research": True,
    }


def install_state(
    *,
    overlay_root: Path,
    overlay_state: dict[str, Any],
    overlay_events: list[dict[str, Any]],
    matrix_state: dict[str, Any],
    matrix_events: list[dict[str, Any]],
    portfolio_state: dict[str, Any],
    report: dict[str, Any],
) -> Path:
    state_dir = overlay_root / "state"
    backup_dir = state_dir / "backups" / f"pre_research_portfolio_install_{datetime.now(tz=live_overlay.IST).strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    for name in ("overlay_state.json", "overlay_events.jsonl", "portfolio_state.json", "matrix_state.json", "matrix_events.jsonl", "overlay_status.json"):
        src = state_dir / name
        if src.exists():
            shutil.copy2(src, backup_dir / name)
    write_json(state_dir / "overlay_state.json", overlay_state)
    write_jsonl(state_dir / "overlay_events.jsonl", overlay_events)
    write_json(state_dir / "portfolio_state.json", portfolio_state)
    write_json(state_dir / "matrix_state.json", matrix_state)
    write_jsonl(state_dir / "matrix_events.jsonl", matrix_events)
    write_json(
        state_dir / "overlay_status.json",
        {
            "ok": True,
            "schema": "obvfutport_v2.v2matrix_overlay_status.v1",
            "updated_at_ist": now_ist_iso(),
            "stream": {"skipped": True, "reason": "research_portfolio_history_installed"},
            "clock": overlay_state.get("last_clock"),
            "research_portfolio_install": {
                "installed": True,
                "report_path": str(state_dir / "v2matrix_research_portfolio_install_report.json"),
            },
        },
    )
    write_json(state_dir / "v2matrix_research_portfolio_install_report.json", {**report, "backup_dir": str(backup_dir)})
    return backup_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--overlay-root", type=Path, default=Path("/opt/cloud-deploy-candidates/v2matrix"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--expected-summary-csv", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--variants", default=",".join(DEFAULT_VARIANTS))
    parser.add_argument("--primary-variant", default=live_overlay.PRIMARY_VARIANT)
    parser.add_argument("--max-positions", type=int, default=live_overlay.MAX_PORTFOLIO_POSITIONS)
    parser.add_argument("--initial-capital", type=float, default=2_000_000.0)
    parser.add_argument("--fixed-entry-margin", type=float, default=live_overlay.FIXED_ENTRY_MARGIN)
    parser.add_argument("--mae-floor", type=float, default=0.0005)
    parser.add_argument("--summary-tolerance-rupees", type=float, default=1.0)
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()

    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    variants = tuple(item.strip() for item in str(args.variants).split(",") if item.strip())
    if args.primary_variant not in variants:
        raise RuntimeError(f"primary variant {args.primary_variant!r} must be included in --variants")
    base.add_paths(args.root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    frame = pd.read_parquet(args.opportunity_frame)
    path_lookup = overlay_research.build_path_lookup(frame)
    candidates_by_variant = build_live_variant_candidates(
        frame=frame,
        path_lookup=path_lookup,
        variants=variants,
        mae_floor=float(args.mae_floor),
    )
    replacement = portfolio_research.ReplacementPolicy(name="none", enabled=False)
    portfolio_results: dict[str, dict[str, Any]] = {}
    portfolio_summaries: list[dict[str, Any]] = []
    portfolios: dict[str, dict[str, Any]] = {}
    for variant in variants:
        definition = live_overlay.portfolio_def(variant)
        result = portfolio_research.run_portfolio(
            variant=variant,
            candidates=candidates_by_variant.get(variant, []),
            max_positions=int(definition.max_positions),
            initial_capital=float(args.initial_capital),
            v1_portfolio=v1_portfolio,
            sizing_mode="fixed_entry_margin_unconstrained",
            fixed_entry_margin=float(definition.fixed_entry_margin),
            replacement_policy=replacement,
        )
        portfolio_results[variant] = result
        portfolio = portfolio_state_from_research_result(
            variant=variant,
            result=result,
            initial_capital=float(args.initial_capital),
        )
        signal_summary = all_qualified_signal_summary(
            variant=variant,
            candidates=candidates_by_variant.get(variant, []),
            v1_portfolio=v1_portfolio,
            reference=str(args.opportunity_frame),
        )
        portfolio.update(signal_summary)
        portfolios[portfolio_key(variant)] = portfolio
        summary = {
            **summarize_installed_portfolio(portfolio),
            "source_research_portfolio_id": result["summary"].get("portfolio_id"),
            "research_summary": result["summary"],
            "all_qualified_signal_summary": signal_summary,
        }
        portfolio_summaries.append(summary)

    final_epoch = 0
    all_candidates = [candidate for items in candidates_by_variant.values() for candidate in items]
    if all_candidates:
        final_epoch = max(int(candidate.exit_epoch) for candidate in all_candidates)
    overlay_state, overlay_events, matrix_payloads, simulation_report = overlay_state_from_candidates(
        candidates_by_variant=candidates_by_variant,
        variants=variants,
        primary_variant=args.primary_variant,
        final_epoch=final_epoch,
    )
    overlay_state["portfolios"] = portfolios
    overlay_state["portfolio_summaries"] = portfolio_summaries
    matrix_app = load_matrix_module(args.overlay_root)
    matrix_state, matrix_events = enriched_matrix_events(matrix_app, matrix_payloads)
    portfolio_state = {
        "schema": "obvfutport_v2.v2matrix_portfolios.v1",
        "updated_at_ist": now_ist_iso(),
        "definition": {
            "source": "research candidate universe from T2 continuation opportunity frame",
            "sizing": "fixed Rs 5L max margin per entry, multi-lot, no cash constraint",
            "max_positions": int(args.max_positions),
            "replacement": "none",
            "install_gate": "installed summaries must match research summaries before state replacement",
        },
        "summaries": portfolio_summaries,
        "portfolios": portfolios,
    }

    expected = expected_summary_from_csv(
        args.expected_summary_csv,
        variants=set(variants),
        max_positions=None,
    ) if args.expected_summary_csv else {}
    summary_mismatches: list[dict[str, Any]] = []
    for variant, expected_row in expected.items():
        computed = portfolio_results[variant]["summary"]
        summary_mismatches.extend(
            compare_summary_rows(
                variant=variant,
                computed=computed,
                expected=expected_row,
                tolerance_rupees=float(args.summary_tolerance_rupees),
            )
        )
    if expected and set(expected) != set(variants):
        summary_mismatches.append(
            {
                "field": "expected_variants",
                "computed": sorted(variants),
                "expected": sorted(expected),
                "reason": "expected summary CSV did not contain every selected variant",
            }
        )
    report = {
        "schema": SCHEMA,
        "created_at_ist": now_ist_iso(),
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": "research_t2_overlay_portfolios canonical candidate universe",
        "opportunity_frame": str(args.opportunity_frame),
        "expected_summary_csv": str(args.expected_summary_csv) if args.expected_summary_csv else None,
        "variants": list(variants),
        "primary_variant": args.primary_variant,
        "portfolio_definition": {
            "initial_capital_rupees": float(args.initial_capital),
            "fixed_entry_margin_rupees": "per_portfolio_variant",
            "max_positions": "per_portfolio_variant",
            "cash_constraint": False,
            "replacement": "none",
            "portfolio_variants": [asdict(live_overlay.portfolio_def(variant)) for variant in variants],
        },
        "candidate_counts": {variant: len(candidates_by_variant.get(variant, [])) for variant in variants},
        "portfolio_summaries": portfolio_summaries,
        "summary_gate": {
            "ok": not summary_mismatches,
            "mismatches": summary_mismatches,
            "expected_summary_loaded": bool(expected),
        },
        "simulation_report": simulation_report,
        "matrix_event_count": len(matrix_events),
        "matrix_symbol_count": len(matrix_state.get("instruments") or {}),
        "installed": bool(args.install and not summary_mismatches),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    write_json(args.output_dir / "v2matrix_research_portfolio_install_report.json", report)
    write_json(args.output_dir / "overlay_state.json", overlay_state)
    write_jsonl(args.output_dir / "overlay_events.jsonl", overlay_events)
    write_json(args.output_dir / "matrix_state.json", matrix_state)
    write_jsonl(args.output_dir / "matrix_events.jsonl", matrix_events)
    write_json(args.output_dir / "portfolio_state.json", portfolio_state)
    if summary_mismatches:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2
    if args.install:
        backup_dir = install_state(
            overlay_root=args.overlay_root,
            overlay_state=overlay_state,
            overlay_events=overlay_events,
            matrix_state=matrix_state,
            matrix_events=matrix_events,
            portfolio_state=portfolio_state,
            report=report,
        )
        report["backup_dir"] = str(backup_dir)
        write_json(args.output_dir / "v2matrix_research_portfolio_install_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
