#!/usr/bin/env python3
"""Build v2-only adaptive symbol overrides from the joint T1/T2 report."""

from __future__ import annotations

import argparse
import copy
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
PRIMARY_SUMMARY_KEY = "summary_three_lot"


def as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def scale_dynamic_point_config(config: dict[str, Any], kind: str, scale: float) -> dict[str, Any]:
    out = copy.deepcopy(config or {})
    target = out.setdefault(kind, {})
    if not isinstance(target, dict):
        target = {}
        out[kind] = target
    if not math.isfinite(scale) or abs(scale - 1.0) < 1e-12:
        return out
    for key in (
        "multiplier",
        "floor_points",
        "cap_points",
        "fallback_points",
        "floor_bps",
        "cap_bps",
        "fallback_bps",
    ):
        value = as_float(target.get(key))
        if value is not None:
            target[key] = value * scale
    target[f"adaptive_{kind}_scale"] = scale
    return out


def build_signal_overrides(combo: dict[str, Any]) -> dict[str, Any]:
    return {
        "primary_obv_short_abs_threshold": float(combo["primary_abs"]),
        "fresh_breakout": {"multiplier": float(combo["fresh_multiplier"])},
        "fresh_long_price_strength_pct": float(combo["long_pct"]),
        "fresh_short_price_weakness_pct": float(combo["short_pct"]),
    }


def build_execution_overrides(exit_combo: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    out = scale_dynamic_point_config(out, "hard_sl", float(exit_combo.get("hard_sl_scale") or 1.0))
    out = scale_dynamic_point_config(out, "trail_activation", float(exit_combo.get("trail_activation_scale") or 1.0))
    profile = {
        "short_exit_pct": float(exit_combo["short_exit_pct"]),
        "long_exit_pct": float(exit_combo["long_exit_pct"]),
        "min_exit_age_sessions": int(exit_combo["min_exit_age_sessions"]),
        "trail_activation_r_multiple": float(exit_combo["trail_activation_r_multiple"]),
        "trail_giveback_fraction": float(exit_combo["trail_giveback_fraction"]),
    }
    mfe_r = exit_combo.get("min_profit_or_mfe_r")
    if mfe_r is not None:
        profile["min_profit_or_mfe_r"] = float(mfe_r)
    out["exit_profile"] = profile
    out.update(
        {
            "two_lot_ttsl_enabled": True,
            "two_lot_ttsl_activation_clocks": int(exit_combo["t2_activation_clocks"]),
            "two_lot_ttsl_tighten_pct": float(exit_combo["t2_tighten_pct"]),
            "two_lot_ttsl_sync_with_base_stop": True,
        }
    )
    return out


def build_tranche3_overrides(tranche3_combo: dict[str, Any]) -> dict[str, Any]:
    entry_mode = str(tranche3_combo.get("entry_mode") or "momentum").strip().lower()
    if entry_mode not in {"momentum", "pullback"}:
        entry_mode = "momentum"
    pullback_r = as_float(tranche3_combo.get("pullback_r_multiple"))
    return {
        "tranche3_enabled": bool(tranche3_combo.get("enabled", True)),
        "tranche3_activation_clocks": int(tranche3_combo.get("activation_clocks") or 16),
        "tranche3_entry_r_multiple": float(
            tranche3_combo.get("entry_r_multiple")
            if tranche3_combo.get("entry_r_multiple") is not None
            else 0.75
        ),
        "tranche3_entry_mode": entry_mode,
        "tranche3_pullback_r_multiple": float(pullback_r) if pullback_r is not None else None,
    }


def summary_value(item: dict[str, Any] | None, key: str) -> float | None:
    if not isinstance(item, dict):
        return None
    return as_float((item.get(PRIMARY_SUMMARY_KEY) or {}).get(key))


def classify(symbol_item: dict[str, Any]) -> tuple[str | None, list[str], str]:
    current = symbol_item.get("current_deployed") if isinstance(symbol_item.get("current_deployed"), dict) else None
    best = symbol_item.get("best_risk_first_candidate") if isinstance(symbol_item.get("best_risk_first_candidate"), dict) else None
    if not best:
        return None, ["no_best_candidate"], "no_adoption"

    cur_net = summary_value(current, "total_net_rupees")
    best_net = summary_value(best, "total_net_rupees")
    cur_success = summary_value(current, "success_rate_pct")
    best_success = summary_value(best, "success_rate_pct")
    cur_worst = summary_value(current, "min_net_pct_margin")
    best_worst = summary_value(best, "min_net_pct_margin")
    cur_dd = summary_value(current, "max_drawdown_rupees")
    best_dd = summary_value(best, "max_drawdown_rupees")
    reject = str(best.get("rejected_reason") or "")
    promotion = symbol_item.get("promotion") if isinstance(symbol_item.get("promotion"), dict) else {}

    net_delta = None if cur_net is None or best_net is None else best_net - cur_net
    success_delta = None if cur_success is None or best_success is None else best_success - cur_success
    worst_delta = None if cur_worst is None or best_worst is None else best_worst - cur_worst
    drawdown_delta = None if cur_dd is None or best_dd is None else best_dd - cur_dd

    tags: list[str] = []
    if reject:
        tags.append(reject)
    if bool(symbol_item.get("threshold_synthesized")):
        tags.append("synthesized_threshold_source")
    else:
        tags.append("v1_runtime_threshold_source")

    if promotion.get("decision") == "promote_candidate_for_review":
        return "tier1_formal_promotion_candidate", tags + ["strict_gate_passed"], "adopt"

    if net_delta is None or net_delta <= 0:
        return None, tags + ["net_not_improved"], "hold_back"
    if worst_delta is not None and worst_delta < -1e-9:
        return None, tags + ["worst_loss_worsened"], "hold_back"
    if drawdown_delta is not None and drawdown_delta < -1e-9:
        return None, tags + ["drawdown_worsened"], "hold_back"

    all_four = (
        success_delta is not None
        and success_delta > 0
        and worst_delta is not None
        and worst_delta > 0
        and drawdown_delta is not None
        and drawdown_delta > 0
    )
    if all_four:
        return "tier2_provisional_all_metrics_improved", tags + ["net_success_worst_drawdown_improved"], "adopt"

    return "tier3_watchlist_improved_non_worsening_risk", tags + ["net_improved_risk_not_worse"], "adopt"


def deltas(current: dict[str, Any] | None, best: dict[str, Any] | None) -> dict[str, Any]:
    keys = {
        "net_delta_rupees": "total_net_rupees",
        "success_rate_delta_pct": "success_rate_pct",
        "worst_loss_pct_delta": "min_net_pct_margin",
        "drawdown_delta_rupees": "max_drawdown_rupees",
        "trade_count_delta": "trade_count",
        "closed_count_delta": "closed_count",
    }
    out: dict[str, Any] = {}
    for out_key, metric_key in keys.items():
        cur = summary_value(current, metric_key)
        best_value = summary_value(best, metric_key)
        out[out_key] = None if cur is None or best_value is None else best_value - cur
    return out


def run(args: argparse.Namespace) -> dict[str, Any]:
    source = read_json(Path(args.report))
    symbols = source.get("symbols") if isinstance(source.get("symbols"), dict) else {}
    adopted: dict[str, Any] = {}
    held_back: dict[str, Any] = {}
    counts: dict[str, int] = {}
    for symbol, item in sorted(symbols.items()):
        if not isinstance(item, dict):
            continue
        tier, tags, action = classify(item)
        best = item.get("best_risk_first_candidate") if isinstance(item.get("best_risk_first_candidate"), dict) else None
        current = item.get("current_deployed") if isinstance(item.get("current_deployed"), dict) else None
        if action == "adopt" and tier and best:
            combo = best.get("combo") if isinstance(best.get("combo"), dict) else {}
            exit_combo = best.get("exit_combo") if isinstance(best.get("exit_combo"), dict) else {}
            tranche3_combo = best.get("tranche3_combo") if isinstance(best.get("tranche3_combo"), dict) else {}
            if not combo or not exit_combo or not tranche3_combo:
                held_back[symbol] = {"status": "hold_back", "reason": "missing_combo_exit_or_tranche3_combo", "tags": tags}
                counts["hold_back_missing_combo_exit_or_tranche3_combo"] = counts.get("hold_back_missing_combo_exit_or_tranche3_combo", 0) + 1
                continue
            adopted[symbol] = {
                "adopted": True,
                "tier": tier,
                "status": "adopted",
                "tags": tags,
                "source_report": str(args.report),
                "source_run": args.source_run or str(Path(args.report).parent),
                "candidate_kind": "best_risk_first_candidate",
                "threshold_source_before": item.get("threshold_source"),
                "threshold_synthesized_before": bool(item.get("threshold_synthesized")),
                "combo_label": best.get("combo_label"),
                "exit_combo_label": best.get("exit_combo_label"),
                "tranche3_combo_label": best.get("tranche3_combo_label"),
                "joint_label": best.get("joint_label"),
                "entry_combo": combo,
                "exit_combo": exit_combo,
                "tranche3_combo": tranche3_combo,
                "overrides": {
                    "signal_point_config": build_signal_overrides(combo),
                    "execution_point_config": build_execution_overrides(exit_combo),
                    "tranche3_config": build_tranche3_overrides(tranche3_combo),
                },
                "metrics": {
                    "primary_summary": PRIMARY_SUMMARY_KEY,
                    "current": current.get(PRIMARY_SUMMARY_KEY) if current else None,
                    "candidate": best.get(PRIMARY_SUMMARY_KEY),
                    "deltas": deltas(current, best),
                    "rejected_reason": best.get("rejected_reason"),
                    "promotion": item.get("promotion"),
                },
                "generated_at_ist": datetime.now(tz=IST).replace(microsecond=0).isoformat(),
            }
            counts[tier] = counts.get(tier, 0) + 1
        else:
            held_back[symbol] = {
                "status": "hold_back",
                "reason": tags[-1] if tags else "not_adopted",
                "tags": tags,
                "metrics": {
                    "current": current.get(PRIMARY_SUMMARY_KEY) if current else None,
                    "candidate": best.get(PRIMARY_SUMMARY_KEY) if best else None,
                    "deltas": deltas(current, best),
                },
            }
            counts[f"hold_back_{held_back[symbol]['reason']}"] = counts.get(f"hold_back_{held_back[symbol]['reason']}", 0) + 1

    payload = {
        "schema": "obvfutport_v2.adaptive_symbol_overrides.v1",
        "generated_at_ist": datetime.now(tz=IST).replace(microsecond=0).isoformat(),
        "source_report": str(args.report),
        "source_run": args.source_run or str(Path(args.report).parent),
        "policy": {
            "adopted_tiers": [
                "tier1_formal_promotion_candidate",
                "tier2_provisional_all_metrics_improved",
                "tier3_watchlist_improved_non_worsening_risk",
            ],
            "held_back": "net not improved, or worst loss/drawdown worsened, or candidate missing",
            "scope": "OBVFUTPORT-v2 only",
            "baseline_for_next_incremental_recalibration": True,
        },
        "counts": {
            "symbols_seen": len(symbols),
            "adopted": len(adopted),
            "held_back": len(held_back),
            **counts,
        },
        "symbols": adopted,
        "held_back_symbols": held_back,
    }
    atomic_write_json(Path(args.output), payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--source-run")
    args = parser.parse_args()
    payload = run(args)
    print(json.dumps(payload["counts"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
