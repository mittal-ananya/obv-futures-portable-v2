#!/usr/bin/env python3
"""Regression gate for OBVFUTPORT-v2 adaptive recalibration.

This gate is deliberately isolated. It validates scorer/replay invariants,
then optionally runs current-runtime smoke gates in temporary folders. It must
pass before any broad 212-symbol recalibration/reseed is launched.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PACKAGE_ROOT / "src"
SCRIPT_ROOT = PACKAGE_ROOT / "scripts"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from obvfut_portable_v2.passive_runner import atomic_write_json, epoch_ist_iso, json_clean, read_json  # noqa: E402

import score_t1_t2_exit_candidates_risk_first as scorer  # noqa: E402


DEFAULT_SYMBOLS = ["ABCAPITAL", "OFSS", "RELIANCE", "SOLARINDS", "HDFCAMC"]


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip().upper() for part in str(raw).split(",") if part.strip()]


def resolve_path(root: Path, value: str | None) -> Path:
    path = Path(str(value or ""))
    return path if path.is_absolute() else root / path


def write_json(path: Path, payload: Any) -> None:
    atomic_write_json(path, json_clean(payload))


def check(condition: bool, name: str, detail: Any = None) -> dict[str, Any]:
    return {"name": name, "ok": bool(condition), "detail": detail}


def static_source_checks(root: Path) -> list[dict[str, Any]]:
    scorer_path = root / "scripts" / "score_t1_t2_exit_candidates_risk_first.py"
    smoke_path = root / "scripts" / "run_v2_joint_smoke_gate.py"
    runner_path = root / "src" / "obvfut_portable_v2" / "passive_runner.py"
    scorer_text = scorer_path.read_text(encoding="utf-8")
    smoke_text = smoke_path.read_text(encoding="utf-8")
    runner_text = runner_path.read_text(encoding="utf-8")
    return [
        check(
            "combo[\"hard_sl_scale\"] = 1.0" in scorer_text
            and "combo[\"trail_activation_scale\"] = 1.0" in scorer_text,
            "current_runtime_no_double_exit_scaling",
        ),
        check(
            "mfe_r is not None and hard_sl_points is not None" in scorer_text,
            "mfe_points_not_zeroed_without_trade_hard_sl",
        ),
        check(
            "second_row_retention_seconds=None" in scorer_text,
            "scorer_retains_selected_second_rows_for_compact_update",
        ),
        check(
            "_compact_update_tranche3_for_candidate" in scorer_text
            and "_compact_model_clock_position_update" in runner_text,
            "scorer_uses_compact_open_position_update",
        ),
        check(
            "--chronological-combo-simulation" in scorer_text and "--include-score-rows" in scorer_text,
            "scorer_exposes_chronological_and_row_output_flags",
        ),
        check(
            "--chronological-combo-simulation" in smoke_text and "--include-score-rows" in smoke_text,
            "smoke_gate_can_call_chronological_scorer",
        ),
        check(
            "embedded[symbol]" in smoke_text and "current.get(\"rows\")" in smoke_text,
            "smoke_gate_reads_embedded_current_rows",
        ),
        check(
            "archive_replay_event_time_checkpoints_enabled" in smoke_text,
            "installed_smoke_uses_event_time_checkpoints",
        ),
        check(
            "archive_replay_disable_live_stale_entry_marking" in smoke_text,
            "installed_smoke_disables_live_stale_marking",
        ),
        check(
            "_filter_valid_tranche3_events" in runner_text
            and "_tranche3_close_allowed" in runner_text,
            "runner_filters_invalid_t3_events",
        ),
        check(
            "lower_bound = latest_epoch if latest_epoch > entry_epoch" in runner_text
            and "existing_mfe = max" in runner_text,
            "compact_clock_summary_is_incremental",
        ),
        check(
            "def _rows_between_epochs" in runner_text
            and "return self._rows_between_epochs(second_rows" in runner_text,
            "active_position_rows_use_indexed_epoch_slice",
        ),
        check(
            "day_source = signal_clock_state" in runner_text,
            "compact_exhaustion_age_uses_full_signal_clock_calendar",
        ),
    ]


def invariant_checks() -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    no_entry_position = {"tranche3": {"status": "not_entered"}}
    entered_position = {"tranche3": {"status": "open", "entry_epoch": 1000}}
    t2_closed_position = {
        "two_lot_ttsl": {"tranche2": {"exit_epoch": 1500}},
        "tranche3": {"status": "open", "entry_epoch": 1000},
    }
    checks.append(
        check(
            not scorer.tranche3_close_allowed(no_entry_position, 1200),
            "t3_cannot_exit_without_entry",
        )
    )
    checks.append(
        check(
            not scorer.tranche3_close_allowed(entered_position, 999)
            and scorer.tranche3_close_allowed(entered_position, 1000),
            "t3_exit_epoch_must_be_at_or_after_t3_entry",
        )
    )
    checks.append(
        check(
            scorer.resolve_tranche3_final_epoch(t2_closed_position, 1800) == 1499,
            "t3_final_epoch_capped_before_selected_t2_exit",
        )
    )
    exit_combo = {
        "short_exit_pct": 5.0,
        "long_exit_pct": 95.0,
        "min_exit_age_sessions": 2,
        "trail_activation_r_multiple": 2.5,
        "trail_giveback_fraction": 0.8,
        "min_profit_or_mfe_r": 0.5,
        "t2_activation_clocks": 12,
        "t2_tighten_pct": 10.0,
    }
    pc_without_hard_sl = scorer.point_config_for_exit_combo({}, exit_combo, hard_sl_points=None)
    pc_with_hard_sl = scorer.point_config_for_exit_combo({}, exit_combo, hard_sl_points=20.0)
    checks.append(
        check(
            "min_profit_or_mfe_points" not in pc_without_hard_sl.get("exit_profile", {}),
            "mfe_points_not_materialized_without_trade_hard_sl",
        )
    )
    checks.append(
        check(
            pc_with_hard_sl.get("exit_profile", {}).get("min_profit_or_mfe_points") == 10.0,
            "mfe_points_materialized_from_trade_hard_sl_when_available",
        )
    )
    checks.append(
        check(
            pc_with_hard_sl.get("two_lot_ttsl_activation_clocks") == 12
            and pc_with_hard_sl.get("two_lot_ttsl_tighten_pct") == 10.0
            and pc_with_hard_sl.get("two_lot_ttsl_sync_with_base_stop") is True,
            "t2_v21_config_materialized",
        )
    )
    fake_meta = SimpleNamespace(
        adaptive_calibration={
            "adopted": True,
            "exit_combo": {
                "label": "exit_test",
                "hard_sl_scale": 0.8,
                "trail_activation_scale": 0.75,
                "trail_activation_r_multiple": 2.0,
                "trail_giveback_fraction": 0.7,
                "short_exit_pct": 10.0,
                "long_exit_pct": 90.0,
                "min_exit_age_sessions": 1,
                "min_profit_or_mfe_r": None,
                "t2_activation_clocks": 8,
                "t2_tighten_pct": 12.0,
            },
        }
    )
    current = scorer.current_exit_combo(fake_meta, argparse.Namespace())
    checks.append(
        check(
            current.get("hard_sl_scale") == 1.0 and current.get("trail_activation_scale") == 1.0,
            "current_runtime_exit_combo_never_double_scales_adaptive_points",
            {"current": current},
        )
    )
    return checks


def run_command(command: list[str], *, cwd: Path, env: dict[str, str], timeout_seconds: int) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        result = subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
        return {
            "command": command,
            "returncode": result.returncode,
            "duration_seconds": round(time.perf_counter() - started, 4),
            "stdout_tail": result.stdout[-12000:],
            "stderr_tail": result.stderr[-12000:],
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": command,
            "returncode": None,
            "duration_seconds": round(time.perf_counter() - started, 4),
            "stdout_tail": (exc.stdout or "")[-12000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-12000:] if isinstance(exc.stderr, str) else "",
            "timed_out": True,
        }


def smoke_gate(
    *,
    root: Path,
    config: Path,
    output_dir: Path,
    python: str,
    symbols: list[str],
    start_date: str,
    end_date: str,
    timeout_seconds: int,
    reuse_filtered_stream: bool,
) -> dict[str, Any]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    command = [
        python,
        str(root / "scripts" / "run_v2_joint_smoke_gate.py"),
        "--root",
        str(root),
        "--config",
        str(config),
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--symbols",
        ",".join(symbols),
        "--output-dir",
        str(output_dir),
        "--python",
        python,
        "--force",
        "--chronological-combo-simulation",
    ]
    if reuse_filtered_stream:
        command.append("--reuse-filtered-stream")
    result = run_command(command, cwd=root, env=env, timeout_seconds=timeout_seconds)
    report_path = output_dir / "smoke_gate_report.json"
    payload = read_json(report_path, {}) if report_path.exists() else {}
    result["report_path"] = str(report_path)
    result["ok"] = bool(payload.get("ok")) and result.get("returncode") == 0
    result["comparison"] = payload.get("comparison")
    result["impossible_t3_rows"] = payload.get("impossible_t3_rows")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(PACKAGE_ROOT))
    parser.add_argument("--config", default="config/runtime.json")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-19")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--runtime-smoke", action="store_true")
    parser.add_argument("--reuse-filtered-stream", action="store_true")
    parser.add_argument("--one-symbol-timeout-seconds", type=int, default=240)
    parser.add_argument("--five-symbol-timeout-seconds", type=int, default=900)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    config = resolve_path(root, args.config)
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = root / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    symbols = parse_csv(args.symbols) or DEFAULT_SYMBOLS
    report: dict[str, Any] = {
        "schema": "obvfutport_v2.recalibration_regression_gate.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "root": str(root),
        "config": str(config),
        "output_dir": str(output_dir),
        "symbols": symbols,
        "runtime_smoke_requested": bool(args.runtime_smoke),
    }
    report["static_checks"] = static_source_checks(root)
    report["invariant_checks"] = invariant_checks()
    static_ok = all(item.get("ok") for item in report["static_checks"])
    invariant_ok = all(item.get("ok") for item in report["invariant_checks"])
    report["static_ok"] = static_ok
    report["invariant_ok"] = invariant_ok
    report["smoke"] = {}
    if static_ok and invariant_ok and args.runtime_smoke:
        report["smoke"]["one_symbol"] = smoke_gate(
            root=root,
            config=config,
            output_dir=output_dir / "one_symbol",
            python=args.python,
            symbols=[symbols[0]],
            start_date=args.start_date,
            end_date=args.end_date,
            timeout_seconds=int(args.one_symbol_timeout_seconds),
            reuse_filtered_stream=bool(args.reuse_filtered_stream),
        )
        if report["smoke"]["one_symbol"].get("ok"):
            report["smoke"]["five_symbol"] = smoke_gate(
                root=root,
                config=config,
                output_dir=output_dir / "five_symbol",
                python=args.python,
                symbols=symbols,
                start_date=args.start_date,
                end_date=args.end_date,
                timeout_seconds=int(args.five_symbol_timeout_seconds),
                reuse_filtered_stream=bool(args.reuse_filtered_stream),
            )
    report["ok"] = bool(static_ok and invariant_ok) and (
        not args.runtime_smoke
        or bool(report.get("smoke", {}).get("one_symbol", {}).get("ok"))
        and bool(report.get("smoke", {}).get("five_symbol", {}).get("ok"))
    )
    report["completed_at_ist"] = epoch_ist_iso(time.time())
    write_json(output_dir / "regression_gate_report.json", report)
    print(
        json.dumps(
            json_clean(
                {
                    "ok": report["ok"],
                    "static_ok": static_ok,
                    "invariant_ok": invariant_ok,
                    "smoke": report["smoke"],
                    "report_path": str(output_dir / "regression_gate_report.json"),
                }
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
