#!/usr/bin/env python3
"""Checkpointed symbol-atomic v2 joint T1/T2/T3 recalibration runner.

This runner is intentionally non-production. It writes frozen per-symbol
candidate artifacts only after the chosen candidate has been replay-proven by
the same canonical kernel used for scoring.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import sys
import time
import traceback
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = PACKAGE_ROOT / "scripts"
SRC_ROOT = PACKAGE_ROOT / "src"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import v2_canonical_joint_gate as gate  # noqa: E402
from obvfut_portable_v2.passive_runner import (  # noqa: E402
    atomic_write_json,
    epoch_ist_iso,
    json_clean,
    read_json,
)


RUNNER_SCHEMA = "obvfutport_v2.symbol_atomic_joint_recalibration.v1"
FROZEN_SCHEMA = "obvfutport_v2.frozen_symbol_candidate.v1"


def sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_symbols(raw: str | None) -> list[str]:
    return gate.parse_csv(raw)


def process_health() -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    try:
        load_1m, load_5m, load_15m = os.getloadavg()
    except OSError:
        load_1m = load_5m = load_15m = None
    return {
        "rss_mb": round(float(usage.ru_maxrss) / 1024.0, 2),
        "user_cpu_seconds": round(float(usage.ru_utime), 2),
        "system_cpu_seconds": round(float(usage.ru_stime), 2),
        "load_1m": load_1m,
        "load_5m": load_5m,
        "load_15m": load_15m,
    }


def append_progress(output_dir: Path, event: dict[str, Any]) -> None:
    payload = {
        "ts_epoch": time.time(),
        "ts_ist": epoch_ist_iso(time.time()),
        **event,
    }
    progress_path = output_dir / "progress.jsonl"
    with progress_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(json_clean(payload), sort_keys=True) + "\n")
    print(json.dumps(json_clean(payload), sort_keys=True), flush=True)


def write_status(output_dir: Path, status: dict[str, Any]) -> None:
    payload = {
        "schema": f"{RUNNER_SCHEMA}.status",
        "updated_at_epoch": time.time(),
        "updated_at_ist": epoch_ist_iso(time.time()),
        **status,
        "process_health": process_health(),
    }
    atomic_write_json(output_dir / "run_status.json", json_clean(payload))


def write_checkpoint(output_dir: Path, checkpoint: dict[str, Any]) -> None:
    payload = {
        "schema": f"{RUNNER_SCHEMA}.checkpoint",
        "updated_at_epoch": time.time(),
        "updated_at_ist": epoch_ist_iso(time.time()),
        **checkpoint,
    }
    atomic_write_json(output_dir / "checkpoint.json", json_clean(payload))


def load_checkpoint(output_dir: Path) -> dict[str, Any]:
    path = output_dir / "checkpoint.json"
    return read_json(path, {}) if path.exists() else {}


def run_signature(args: argparse.Namespace) -> dict[str, Any]:
    config_path = Path(args.config)
    scorer_path = SCRIPT_ROOT / "score_t1_t2_exit_candidates_risk_first.py"
    gate_path = SCRIPT_ROOT / "v2_canonical_joint_gate.py"
    runner_path = Path(__file__).resolve()
    payload = {
        "schema": f"{RUNNER_SCHEMA}.signature.v1",
        "config_path": str(config_path),
        "config_sha256": sha256_file(config_path),
        "code_sha256": {
            "symbol_atomic_runner": sha256_file(runner_path),
            "canonical_gate": sha256_file(gate_path),
            "joint_scorer": sha256_file(scorer_path),
        },
        "date_range": {"start_date": args.start_date, "end_date": args.end_date},
        "contract_as_of_iso": args.contract_as_of_iso,
        "skip_weekends": not args.no_skip_weekends,
        "grid": {
            "primary_short_thresholds": args.primary_short_thresholds,
            "fresh_breakout_multipliers": args.fresh_breakout_multipliers,
            "long_strength_pcts": args.long_strength_pcts,
            "short_weakness_pcts": args.short_weakness_pcts,
            "exit_combo_labels": args.exit_combo_labels,
            "tranche3_combo_labels": args.tranche3_combo_labels,
        },
        "risk_gate": {
            "min_trades": args.min_trades,
            "min_closed_trades": args.min_closed_trades,
            "min_success_rate_pct": args.min_success_rate_pct,
            "max_worst_loss_pct_deterioration": args.max_worst_loss_pct_deterioration,
            "min_worst_loss_pct_improvement": args.min_worst_loss_pct_improvement,
            "max_success_rate_deterioration_pct": args.max_success_rate_deterioration_pct,
            "min_net_preservation_ratio": args.min_net_preservation_ratio,
            "max_single_win_share": args.max_single_win_share,
        },
        "index_root": args.index_root,
        "require_index": bool(args.require_index),
    }
    payload["signature_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return payload


def symbol_input_fingerprint(
    *,
    index_root: Path,
    dates: list[str],
    meta: Any,
) -> dict[str, Any]:
    keys = sorted({str(meta.signal_key), str(meta.execution_key)})
    files: list[dict[str, Any]] = []
    for trade_date in dates:
        for key in keys:
            path = gate.stream_index_file(index_root, trade_date, key) if index_root else Path("")
            exists = path.exists() if index_root else False
            stat = path.stat() if exists else None
            files.append(
                {
                    "trade_date": trade_date,
                    "target_key": key,
                    "path": str(path) if index_root else None,
                    "exists": bool(exists),
                    "size_bytes": int(stat.st_size) if stat else 0,
                    "mtime_ns": int(stat.st_mtime_ns) if stat else None,
                }
            )
    digest = hashlib.sha256(json.dumps(files, sort_keys=True).encode("utf-8")).hexdigest()
    return {
        "signal_key": str(meta.signal_key),
        "execution_key": str(meta.execution_key),
        "files": files,
        "fingerprint_hash": digest,
    }


def artifact_signature_matches(path: Path, signature: dict[str, Any], input_fingerprint: dict[str, Any]) -> bool:
    if not path.exists():
        return False
    artifact = read_json(path, {})
    return (
        artifact.get("run_signature", {}).get("signature_hash") == signature.get("signature_hash")
        and artifact.get("input_fingerprint", {}).get("fingerprint_hash")
        == input_fingerprint.get("fingerprint_hash")
    )


def frozen_artifact_matches(path: Path, signature: dict[str, Any], input_fingerprint: dict[str, Any]) -> bool:
    if not artifact_signature_matches(path, signature, input_fingerprint):
        return False
    return read_json(path, {}).get("freeze_status") == "frozen"


def pending_symbols(symbols: list[str], completed: list[str], skipped: list[dict[str, Any]]) -> list[str]:
    handled = set(completed)
    handled.update(str(item.get("symbol")) for item in skipped if item.get("symbol"))
    return [symbol for symbol in symbols if symbol not in handled]


def gate_args_for_symbol(args: argparse.Namespace, symbol: str, output_dir: Path) -> argparse.Namespace:
    return argparse.Namespace(
        config=args.config,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=str(output_dir),
        symbols=symbol,
        max_symbols=None,
        sample_rows_per_day=args.sample_rows_per_day,
        no_skip_weekends=args.no_skip_weekends,
        index_root=args.index_root,
        reuse_index=args.reuse_index,
        require_index=args.require_index,
        contract_as_of_iso=args.contract_as_of_iso,
        require_branch_coverage=False,
        targeted_branch_proof=False,
        current_runtime_combos_only=False,
        exit_combo_labels=args.exit_combo_labels,
        tranche3_combo_labels=args.tranche3_combo_labels,
        primary_short_thresholds=args.primary_short_thresholds,
        fresh_breakout_multipliers=args.fresh_breakout_multipliers,
        long_strength_pcts=args.long_strength_pcts,
        short_weakness_pcts=args.short_weakness_pcts,
        signal_quote_max_age_seconds=args.signal_quote_max_age_seconds,
        current_long_strength_pct=args.current_long_strength_pct,
        current_short_weakness_pct=args.current_short_weakness_pct,
        min_trades=args.min_trades,
        min_closed_trades=args.min_closed_trades,
        min_success_rate_pct=args.min_success_rate_pct,
        max_worst_loss_pct_deterioration=args.max_worst_loss_pct_deterioration,
        min_worst_loss_pct_improvement=args.min_worst_loss_pct_improvement,
        max_success_rate_deterioration_pct=args.max_success_rate_deterioration_pct,
        min_net_preservation_ratio=args.min_net_preservation_ratio,
        max_single_win_share=args.max_single_win_share,
    )


def classify_result(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("status") == "missing_context":
        return {"action": "skip", "reason": "missing_context"}
    if item.get("status") == "no_scores":
        return {"action": "skip", "reason": "no_scores"}
    best = item.get("best")
    if not isinstance(best, dict):
        return {"action": "skip", "reason": "no_best_candidate"}
    proof = item.get("proof") if isinstance(item.get("proof"), dict) else None
    if not proof:
        return {"action": "stop", "reason": "missing_proof"}
    comparison = proof.get("comparison") if isinstance(proof.get("comparison"), dict) else {}
    if not comparison.get("ok"):
        return {"action": "stop", "reason": "proof_comparison_failed", "details": comparison}
    current_comparison = proof.get("current_comparison")
    if isinstance(current_comparison, dict) and not current_comparison.get("ok"):
        return {"action": "stop", "reason": "current_runtime_proof_comparison_failed", "details": current_comparison}
    invariants = proof.get("invariants") if isinstance(proof.get("invariants"), dict) else {}
    if not invariants.get("ok"):
        return {"action": "stop", "reason": "invariant_failed", "details": invariants}
    accepted_count = int(item.get("accepted_combo_count") or 0)
    rejected_reason = best.get("rejected_reason")
    if accepted_count > 0 and not rejected_reason:
        quality_tier = "risk_gate_accepted"
        promotion_eligible = True
    elif rejected_reason:
        quality_tier = "replay_proven_safety_rejected"
        promotion_eligible = False
    else:
        quality_tier = "replay_proven_no_strict_acceptance"
        promotion_eligible = False
    return {
        "action": "freeze",
        "reason": "best_candidate_replay_proven",
        "quality_tier": quality_tier,
        "promotion_eligible": promotion_eligible,
        "candidate_rejected_reason": rejected_reason,
        "accepted_combo_count": accepted_count,
    }


def compact_symbol_summary(item: dict[str, Any]) -> dict[str, Any]:
    best = item.get("best") or {}
    current = item.get("current") or {}
    return {
        "symbol": item.get("symbol"),
        "status": item.get("status"),
        "candidate_entries": item.get("candidate_entries"),
        "entry_combo_count": item.get("entry_combo_count"),
        "exit_combo_count": item.get("exit_combo_count"),
        "tranche3_combo_count": item.get("tranche3_combo_count"),
        "combo_count": item.get("combo_count"),
        "valid_combo_count": item.get("valid_combo_count"),
        "accepted_combo_count": item.get("accepted_combo_count"),
        "duration_seconds": item.get("duration_seconds"),
        "best_joint_label": best.get("joint_label"),
        "best_rejected_reason": best.get("rejected_reason"),
        "best_summary_one_lot": best.get("summary_one_lot"),
        "best_summary_two_lot": best.get("summary_two_lot"),
        "best_summary_three_lot": best.get("summary_three_lot"),
        "current_joint_label": current.get("joint_label"),
        "current_summary_three_lot": current.get("summary_three_lot"),
    }


def write_frozen_candidate(
    *,
    output_dir: Path,
    symbol: str,
    item: dict[str, Any],
    signature: dict[str, Any],
    input_fingerprint: dict[str, Any],
    classification: dict[str, Any],
) -> Path:
    path = output_dir / "frozen_candidates" / f"{symbol}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "schema": FROZEN_SCHEMA,
        "symbol": symbol,
        "freeze_status": "frozen",
        "frozen_at_epoch": time.time(),
        "frozen_at_ist": epoch_ist_iso(time.time()),
        "classification": classification,
        "run_signature": signature,
        "input_fingerprint": input_fingerprint,
        "symbol_summary": compact_symbol_summary(item),
        "best_candidate": item.get("best"),
        "current_candidate": item.get("current"),
        "proof": item.get("proof"),
        "top_risk": item.get("top_risk"),
    }
    atomic_write_json(path, json_clean(artifact))
    return path


def write_collection(output_dir: Path, name: str, rows: list[dict[str, Any]]) -> None:
    atomic_write_json(output_dir / name, json_clean(rows))


def should_pause_or_stop(output_dir: Path) -> str | None:
    if (output_dir / "stop.request").exists():
        return "stop_requested"
    if (output_dir / "pause.request").exists():
        return "pause_requested"
    return None


def cleanup_symbol_work(symbol_dir: Path, *, keep: bool) -> None:
    if keep:
        return
    runner_state = symbol_dir / "_runner_state"
    if runner_state.exists():
        shutil.rmtree(runner_state, ignore_errors=True)


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    frozen_dir = output_dir / "frozen_candidates"
    skipped_dir = output_dir / "skipped_symbols"
    bug_dir = output_dir / "bugs_found"
    for directory in (frozen_dir, skipped_dir, bug_dir, output_dir / "_symbol_work"):
        directory.mkdir(parents=True, exist_ok=True)

    started = time.time()
    signature = run_signature(args)
    config_path = Path(args.config)
    config = read_json(config_path, {})
    dates = gate.date_range(args.start_date, args.end_date, skip_weekends=not args.no_skip_weekends)

    root_runner_dir = output_dir / "_run_setup"
    root_runner = gate.prepare_runner(
        config_path,
        root_runner_dir,
        retain_seconds=False,
        contract_as_of_iso=args.contract_as_of_iso,
    )
    requested_symbols = parse_symbols(args.symbols)
    metas = gate.selected_metas(root_runner, requested_symbols, args.max_symbols)
    symbols = [meta.symbol for meta in metas]
    meta_by_symbol = {meta.symbol: meta for meta in metas}

    report: dict[str, Any] = {
        "schema": RUNNER_SCHEMA,
        "started_at_epoch": started,
        "started_at_ist": epoch_ist_iso(started),
        "config": str(config_path),
        "output_dir": str(output_dir),
        "dates": dates,
        "symbol_count": len(symbols),
        "symbols": symbols,
        "run_signature": signature,
        "stop_gates": [
            "proof_comparison_failed",
            "current_runtime_proof_comparison_failed",
            "invariant_failed",
            "missing_proof",
            "unexpected_exception",
            "targeted_branch_proof_failed",
        ],
        "skip_gates": [
            "missing_context",
            "no_scores",
            "no_best_candidate",
            "symbol_specific_data_gap",
            "explicit_resume_existing_frozen",
        ],
    }
    atomic_write_json(output_dir / "run_manifest.json", json_clean(report))
    append_progress(output_dir, {"event": "run_started", "symbol_count": len(symbols), "dates": dates})

    branch_proof = {"enabled": False, "ok": None}
    if not args.skip_targeted_branch_proof:
        branch_runner = gate.prepare_runner(
            config_path,
            output_dir / "_branch_proof",
            retain_seconds=False,
            contract_as_of_iso=args.contract_as_of_iso,
        )
        branch_proof = gate.targeted_branch_proof(branch_runner)
        atomic_write_json(output_dir / "targeted_branch_proof.json", json_clean(branch_proof))
        append_progress(output_dir, {"event": "targeted_branch_proof", "ok": bool(branch_proof.get("ok"))})
        if not branch_proof.get("ok"):
            bug = {"symbol": None, "reason": "targeted_branch_proof_failed", "details": branch_proof}
            atomic_write_json(bug_dir / "targeted_branch_proof_failed.json", json_clean(bug))
            write_status(
                output_dir,
                {
                    "state": "stopped",
                    "reason": "targeted_branch_proof_failed",
                    "completed_symbols": 0,
                    "total_symbols": len(symbols),
                },
            )
            return {**report, "ok": False, "failed_reason": "targeted_branch_proof_failed"}

    checkpoint = load_checkpoint(output_dir) if args.resume else {}
    completed: list[str] = list(checkpoint.get("completed_symbols") or [])
    skipped: list[dict[str, Any]] = list(checkpoint.get("skipped_symbols") or [])
    bugs: list[dict[str, Any]] = list(checkpoint.get("bugs_found") or [])

    for ordinal, symbol in enumerate(symbols, start=1):
        meta = meta_by_symbol[symbol]
        input_fingerprint = symbol_input_fingerprint(index_root=Path(args.index_root), dates=dates, meta=meta)
        frozen_path = frozen_dir / f"{symbol}.json"
        skipped_path = skipped_dir / f"{symbol}.json"
        if (
            args.resume
            and args.trust_checkpoint_completed
            and not args.force
            and symbol in completed
            and frozen_path.exists()
        ):
            append_progress(
                output_dir,
                {
                    "event": "symbol_resume_skip",
                    "symbol": symbol,
                    "ordinal": ordinal,
                    "reason": "checkpoint_completed_trusted",
                    "artifact": str(frozen_path),
                },
            )
            continue
        if (
            args.resume
            and not args.force
            and frozen_artifact_matches(frozen_path, signature, input_fingerprint)
        ):
            if symbol not in completed:
                completed.append(symbol)
            append_progress(
                output_dir,
                {"event": "symbol_resume_skip", "symbol": symbol, "ordinal": ordinal, "reason": "existing_frozen_artifact"},
            )
            continue
        if (
            args.resume
            and not args.force
            and artifact_signature_matches(skipped_path, signature, input_fingerprint)
        ):
            existing_skip = read_json(skipped_path, {})
            if not any(item.get("symbol") == symbol for item in skipped):
                skipped.append(existing_skip)
            append_progress(
                output_dir,
                {"event": "symbol_resume_skip", "symbol": symbol, "ordinal": ordinal, "reason": "existing_skipped_artifact"},
            )
            continue

        control = should_pause_or_stop(output_dir)
        if control:
            write_checkpoint(
                output_dir,
                {
                    "state": "paused" if control == "pause_requested" else "stopped",
                    "reason": control,
                    "completed_symbols": completed,
                    "skipped_symbols": skipped,
                    "bugs_found": bugs,
                    "pending_symbols": symbols[ordinal - 1 :],
                    "run_signature": signature,
                },
            )
            write_status(
                output_dir,
                {
                    "state": "paused" if control == "pause_requested" else "stopped",
                    "reason": control,
                    "current_symbol": symbol,
                    "completed_symbols": len(completed),
                    "skipped_symbols": len(skipped),
                    "bug_count": len(bugs),
                    "total_symbols": len(symbols),
                },
            )
            append_progress(output_dir, {"event": control, "symbol": symbol, "ordinal": ordinal})
            return {**report, "ok": control != "stop_requested", "reason": control}

        symbol_started = time.perf_counter()
        append_progress(
            output_dir,
            {
                "event": "symbol_started",
                "symbol": symbol,
                "ordinal": ordinal,
                "total_symbols": len(symbols),
                "completed_symbols": len(completed),
                "skipped_symbols": len(skipped),
                "bug_count": len(bugs),
            },
        )
        write_status(
            output_dir,
            {
                "state": "running",
                "current_symbol": symbol,
                "ordinal": ordinal,
                "total_symbols": len(symbols),
                "completed_symbols": len(completed),
                "skipped_symbols": len(skipped),
                "bug_count": len(bugs),
            },
        )
        symbol_dir = output_dir / "_symbol_work" / symbol
        try:
            symbol_args = gate_args_for_symbol(args, symbol, symbol_dir)
            runner = gate.prepare_runner(
                config_path,
                symbol_dir,
                retain_seconds=True,
                contract_as_of_iso=args.contract_as_of_iso,
            )
            contexts = gate.build_symbol_contexts(
                runner=runner,
                config=config,
                metas=[meta],
                dates=dates,
                args=symbol_args,
            )
            v1 = gate.scorer.load_v1_portfolio_module(runner.config)
            context = contexts.get(symbol)
            if context is None:
                item = {"symbol": symbol, "status": "missing_context"}
            else:
                item = gate.score_symbol(runner=runner, v1=v1, context=context, args=symbol_args)
            classification = classify_result(item)
            duration = round(time.perf_counter() - symbol_started, 4)
            item["symbol_atomic_duration_seconds"] = duration

            if classification["action"] == "freeze":
                artifact_path = write_frozen_candidate(
                    output_dir=output_dir,
                    symbol=symbol,
                    item=item,
                    signature=signature,
                    input_fingerprint=input_fingerprint,
                    classification=classification,
                )
                if symbol not in completed:
                    completed.append(symbol)
                append_progress(
                    output_dir,
                    {
                        "event": "symbol_frozen",
                        "symbol": symbol,
                        "ordinal": ordinal,
                        "duration_seconds": duration,
                        "artifact": str(artifact_path),
                        "candidate_entries": item.get("candidate_entries"),
                        "combo_count": item.get("combo_count"),
                        "accepted_combo_count": item.get("accepted_combo_count"),
                        "best_joint_label": (item.get("best") or {}).get("joint_label"),
                    },
                )
            elif classification["action"] == "skip":
                skip_item = {
                    "symbol": symbol,
                    "ordinal": ordinal,
                    "duration_seconds": duration,
                    "classification": classification,
                    "symbol_summary": compact_symbol_summary(item),
                    "run_signature": signature,
                    "input_fingerprint": input_fingerprint,
                }
                atomic_write_json(skipped_dir / f"{symbol}.json", json_clean(skip_item))
                skipped.append(skip_item)
                append_progress(output_dir, {"event": "symbol_skipped", **skip_item})
            else:
                bug = {
                    "symbol": symbol,
                    "ordinal": ordinal,
                    "duration_seconds": duration,
                    "classification": classification,
                    "symbol_summary": compact_symbol_summary(item),
                    "run_signature": signature,
                    "input_fingerprint": input_fingerprint,
                }
                atomic_write_json(bug_dir / f"{symbol}.json", json_clean(bug))
                bugs.append(bug)
                append_progress(output_dir, {"event": "symbol_bug_stop", **bug})
                write_checkpoint(
                    output_dir,
                    {
                        "state": "stopped",
                        "reason": classification.get("reason"),
                        "completed_symbols": completed,
                        "skipped_symbols": skipped,
                        "bugs_found": bugs,
                        "pending_symbols": symbols[ordinal:],
                        "run_signature": signature,
                    },
                )
                write_status(
                    output_dir,
                    {
                        "state": "stopped",
                        "reason": classification.get("reason"),
                        "current_symbol": symbol,
                        "completed_symbols": len(completed),
                        "skipped_symbols": len(skipped),
                        "bug_count": len(bugs),
                        "total_symbols": len(symbols),
                    },
                )
                return {**report, "ok": False, "failed_symbol": symbol, "failed_reason": classification.get("reason")}
        except SystemExit as exc:
            duration = round(time.perf_counter() - symbol_started, 4)
            skip_item = {
                "symbol": symbol,
                "ordinal": ordinal,
                "duration_seconds": duration,
                "classification": {"action": "skip", "reason": "symbol_specific_data_gap", "details": str(exc)},
                "run_signature": signature,
                "input_fingerprint": input_fingerprint,
            }
            atomic_write_json(skipped_dir / f"{symbol}.json", json_clean(skip_item))
            skipped.append(skip_item)
            append_progress(output_dir, {"event": "symbol_skipped", **skip_item})
        except Exception as exc:  # noqa: BLE001
            duration = round(time.perf_counter() - symbol_started, 4)
            bug = {
                "symbol": symbol,
                "ordinal": ordinal,
                "duration_seconds": duration,
                "classification": {
                    "action": "stop",
                    "reason": "unexpected_exception",
                    "exception": repr(exc),
                    "traceback": traceback.format_exc(limit=30),
                },
                "run_signature": signature,
                "input_fingerprint": input_fingerprint,
            }
            atomic_write_json(bug_dir / f"{symbol}.json", json_clean(bug))
            bugs.append(bug)
            append_progress(output_dir, {"event": "symbol_exception_stop", **bug})
            write_checkpoint(
                output_dir,
                {
                    "state": "stopped",
                    "reason": "unexpected_exception",
                    "completed_symbols": completed,
                    "skipped_symbols": skipped,
                    "bugs_found": bugs,
                    "pending_symbols": symbols[ordinal:],
                    "run_signature": signature,
                },
            )
            write_status(
                output_dir,
                {
                    "state": "stopped",
                    "reason": "unexpected_exception",
                    "current_symbol": symbol,
                    "completed_symbols": len(completed),
                    "skipped_symbols": len(skipped),
                    "bug_count": len(bugs),
                    "total_symbols": len(symbols),
                },
            )
            return {**report, "ok": False, "failed_symbol": symbol, "failed_reason": "unexpected_exception"}
        finally:
            cleanup_symbol_work(symbol_dir, keep=bool(args.keep_symbol_work))

        write_collection(output_dir, "skipped_symbols.json", skipped)
        write_collection(output_dir, "bugs_found.json", bugs)
        write_checkpoint(
            output_dir,
            {
                "state": "running",
                "completed_symbols": completed,
                "skipped_symbols": skipped,
                "bugs_found": bugs,
                "pending_symbols": pending_symbols(symbols, completed, skipped),
                "run_signature": signature,
            },
        )

    final_report = {
        **report,
        "ok": not bugs,
        "completed_at_epoch": time.time(),
        "completed_at_ist": epoch_ist_iso(time.time()),
        "completed_symbols": completed,
        "completed_count": len(completed),
        "skipped_symbols": skipped,
        "skipped_count": len(skipped),
        "bugs_found": bugs,
        "bug_count": len(bugs),
        "targeted_branch_proof": branch_proof,
    }
    atomic_write_json(output_dir / "symbol_atomic_recalibration_report.json", json_clean(final_report))
    write_status(
        output_dir,
        {
            "state": "completed" if not bugs else "completed_with_bugs",
            "completed_symbols": len(completed),
            "skipped_symbols": len(skipped),
            "bug_count": len(bugs),
            "total_symbols": len(symbols),
            "output_dir": str(output_dir),
        },
    )
    write_checkpoint(
        output_dir,
        {
            "state": "completed" if not bugs else "completed_with_bugs",
            "completed_symbols": completed,
            "skipped_symbols": skipped,
            "bugs_found": bugs,
            "pending_symbols": [],
            "run_signature": signature,
        },
    )
    append_progress(
        output_dir,
        {
            "event": "run_completed",
            "ok": final_report["ok"],
            "completed_count": len(completed),
            "skipped_count": len(skipped),
            "bug_count": len(bugs),
            "duration_seconds": round(time.time() - started, 4),
        },
    )
    return final_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--sample-rows-per-day", type=int, default=5000)
    parser.add_argument("--no-skip-weekends", action="store_true")
    parser.add_argument("--index-root", default="")
    parser.add_argument("--reuse-index", action="store_true")
    parser.add_argument("--require-index", action="store_true")
    parser.add_argument(
        "--contract-as-of-iso",
        default="",
        help="Pin contract lifecycle selection for historical proof/recalibration runs.",
    )
    parser.add_argument("--skip-targeted-branch-proof", action="store_true")
    parser.add_argument("--keep-symbol-work", action="store_true")
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--trust-checkpoint-completed",
        action="store_true",
        help="On resume, skip symbols already marked completed in checkpoint when their frozen artifact exists.",
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--exit-combo-labels", default="")
    parser.add_argument("--tranche3-combo-labels", default="")
    parser.add_argument("--primary-short-thresholds", default="1.5,1.75,2.0")
    parser.add_argument("--fresh-breakout-multipliers", default="1,1.2,1.4,1.6")
    parser.add_argument("--long-strength-pcts", default="90,95")
    parser.add_argument("--short-weakness-pcts", default="1,5,10")
    parser.add_argument("--signal-quote-max-age-seconds", type=float, default=None)
    parser.add_argument("--current-long-strength-pct", type=float, default=95.0)
    parser.add_argument("--current-short-weakness-pct", type=float, default=1.0)
    parser.add_argument("--min-trades", type=int, default=1)
    parser.add_argument("--min-closed-trades", type=int, default=1)
    parser.add_argument("--min-success-rate-pct", type=float, default=0.0)
    parser.add_argument("--max-worst-loss-pct-deterioration", type=float, default=0.25)
    parser.add_argument("--min-worst-loss-pct-improvement", type=float, default=0.25)
    parser.add_argument("--max-success-rate-deterioration-pct", type=float, default=5.0)
    parser.add_argument("--min-net-preservation-ratio", type=float, default=0.80)
    parser.add_argument("--max-single-win-share", type=float, default=0.60)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = run(args)
    return 0 if report.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
