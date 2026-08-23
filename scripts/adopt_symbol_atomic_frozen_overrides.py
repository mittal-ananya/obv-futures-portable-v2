#!/usr/bin/env python3
"""Merge replay-proven symbol-atomic frozen candidates into v2 overrides.

This updates only the requested symbols. It is intended for adopting candidates
produced by v2_symbol_atomic_recalibration_runner.py after their per-symbol
replay proof has passed.
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from adopt_recalibration_overrides import (
    PRIMARY_SUMMARY_KEY,
    atomic_write_json,
    build_execution_overrides,
    build_signal_overrides,
    build_tranche3_overrides,
)


IST = ZoneInfo("Asia/Kolkata")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_symbol_tiers(raw: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in raw:
        if "=" not in item:
            raise SystemExit(f"Invalid --symbol-tier value {item!r}; expected SYMBOL=tier")
        symbol, tier = item.split("=", 1)
        symbol = symbol.strip().upper()
        tier = tier.strip()
        if not symbol or not tier:
            raise SystemExit(f"Invalid --symbol-tier value {item!r}; expected SYMBOL=tier")
        out[symbol] = tier
    return out


def normalize_tags(raw: list[str]) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()
    for tag in raw:
        clean = str(tag or "").strip()
        if clean and clean not in seen:
            tags.append(clean)
            seen.add(clean)
    return tags


def summary_delta(current: dict[str, Any] | None, candidate: dict[str, Any] | None, key: str) -> float | None:
    if not isinstance(current, dict) or not isinstance(candidate, dict):
        return None
    try:
        cur = current.get(key)
        nxt = candidate.get(key)
        if cur is None or nxt is None:
            return None
        return float(nxt) - float(cur)
    except (TypeError, ValueError):
        return None


def build_deltas(current: dict[str, Any] | None, candidate: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "net_delta_rupees": summary_delta(current, candidate, "total_net_rupees"),
        "success_rate_delta_pct": summary_delta(current, candidate, "success_rate_pct"),
        "worst_loss_pct_delta": summary_delta(current, candidate, "min_net_pct_margin"),
        "drawdown_delta_rupees": summary_delta(current, candidate, "max_drawdown_rupees"),
        "trade_count_delta": summary_delta(current, candidate, "trade_count"),
        "closed_count_delta": summary_delta(current, candidate, "closed_count"),
    }


def build_override(
    *,
    artifact: dict[str, Any],
    tier: str,
    source_run: str,
    extra_tags: list[str],
    previous: dict[str, Any] | None,
) -> dict[str, Any]:
    symbol = str(artifact.get("symbol") or "").upper()
    best = artifact.get("best_candidate") if isinstance(artifact.get("best_candidate"), dict) else {}
    current = artifact.get("current_candidate") if isinstance(artifact.get("current_candidate"), dict) else {}
    combo = best.get("combo") if isinstance(best.get("combo"), dict) else {}
    exit_combo = best.get("exit_combo") if isinstance(best.get("exit_combo"), dict) else {}
    tranche3_combo = best.get("tranche3_combo") if isinstance(best.get("tranche3_combo"), dict) else {}
    if not symbol or not combo or not exit_combo or not tranche3_combo:
        raise SystemExit(f"{symbol or '<unknown>'}: missing best combo/exit/tranche3 details")

    classification = artifact.get("classification") if isinstance(artifact.get("classification"), dict) else {}
    rejected_reason = best.get("rejected_reason")
    quality_tier = classification.get("quality_tier")
    candidate_summary = best.get(PRIMARY_SUMMARY_KEY)
    current_summary = current.get(PRIMARY_SUMMARY_KEY)
    previous_audit = None
    if isinstance(previous, dict):
        previous_audit = {
            "tier": previous.get("tier"),
            "joint_label": previous.get("joint_label"),
            "source_run": previous.get("source_run"),
            "metrics": previous.get("metrics"),
        }

    tags = normalize_tags(
        [
            tier,
            str(quality_tier or "symbol_atomic_replay_proven"),
            "symbol_atomic_kernel",
            "aug10_aug21_compact_quote_valid_stream",
            *extra_tags,
            str(rejected_reason) if rejected_reason else "",
        ]
    )

    return {
        "adopted": True,
        "tier": tier,
        "status": "adopted",
        "tags": tags,
        "source_report": str(artifact.get("run_signature", {}).get("signature_hash") or ""),
        "source_run": source_run,
        "candidate_kind": "symbol_atomic_best_candidate",
        "threshold_source_before": previous.get("threshold_source_before") if isinstance(previous, dict) else None,
        "threshold_synthesized_before": previous.get("threshold_synthesized_before") if isinstance(previous, dict) else None,
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
            "current": current_summary,
            "candidate": candidate_summary,
            "deltas": build_deltas(current_summary, candidate_summary),
            "rejected_reason": rejected_reason,
            "classification": classification,
            "previous_override": previous_audit,
        },
        "generated_at_ist": datetime.now(tz=IST).replace(microsecond=0).isoformat(),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    base_path = Path(args.base)
    output_path = Path(args.output)
    frozen_dir = Path(args.frozen_dir)
    tiers = parse_symbol_tiers(args.symbol_tier)
    if not tiers:
        raise SystemExit("At least one --symbol-tier SYMBOL=tier is required")

    payload = read_json(base_path)
    if not isinstance(payload, dict):
        raise SystemExit(f"{base_path}: expected JSON object")
    symbols = payload.setdefault("symbols", {})
    if not isinstance(symbols, dict):
        raise SystemExit(f"{base_path}: symbols must be an object")

    timestamp = datetime.now(tz=IST).strftime("%Y%m%d_%H%M%S")
    backup_path = None
    if args.backup_dir:
        backup_dir = Path(args.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"{output_path.name}.pre_symbol_atomic_5_{timestamp}.json"
        shutil.copy2(base_path, backup_path)

    adopted: dict[str, Any] = {}
    for symbol, tier in sorted(tiers.items()):
        artifact_path = frozen_dir / f"{symbol}.json"
        if not artifact_path.exists():
            raise SystemExit(f"Missing frozen artifact for {symbol}: {artifact_path}")
        artifact = read_json(artifact_path)
        if artifact.get("freeze_status") != "frozen":
            raise SystemExit(f"{artifact_path}: freeze_status is not frozen")
        previous = symbols.get(symbol) if isinstance(symbols.get(symbol), dict) else None
        symbols[symbol] = build_override(
            artifact=artifact,
            tier=tier,
            source_run=args.source_run or str(frozen_dir.parent),
            extra_tags=args.extra_tag,
            previous=previous,
        )
        adopted[symbol] = {
            "tier": tier,
            "joint_label": symbols[symbol].get("joint_label"),
            "rejected_reason": symbols[symbol].get("metrics", {}).get("rejected_reason"),
            "candidate": symbols[symbol].get("metrics", {}).get("candidate"),
        }

    payload["generated_at_ist"] = datetime.now(tz=IST).replace(microsecond=0).isoformat()
    payload["source_run"] = args.source_run or str(frozen_dir.parent)
    policy = payload.setdefault("policy", {})
    if isinstance(policy, dict):
        policy["latest_update"] = {
            "scope": "OBVFUTPORT-v2 only",
            "method": "symbol_atomic_replay_proven_override_merge",
            "symbols": sorted(tiers),
            "updated_at_ist": payload["generated_at_ist"],
            "backup_path": str(backup_path) if backup_path else None,
        }
    counts = payload.setdefault("counts", {})
    if isinstance(counts, dict):
        counts["symbols_seen"] = len(symbols)
        counts["adopted"] = len(symbols)
        counts["symbol_atomic_5_latest_update"] = len(tiers)

    atomic_write_json(output_path, payload)
    return {"output": str(output_path), "backup": str(backup_path) if backup_path else None, "adopted": adopted}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--frozen-dir", required=True)
    parser.add_argument("--source-run", default="")
    parser.add_argument("--backup-dir", default="")
    parser.add_argument("--symbol-tier", action="append", default=[])
    parser.add_argument("--extra-tag", action="append", default=[])
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
