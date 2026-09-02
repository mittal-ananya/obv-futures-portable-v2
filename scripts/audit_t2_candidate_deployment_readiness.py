#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import audit_selected_t2_custom_exit_candidates as custom_exit_audit  # noqa: E402
import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_continuation_filters as continuation  # noqa: E402
import research_t2_mfe_first_profit_capture as overlay_research  # noqa: E402
import research_t2_portfolio_rules as portfolio_rules  # noqa: E402
import run_v2matrix_overlay as live_overlay  # noqa: E402


SCHEMA = "obvfutport_v2.t2_candidate_deployment_readiness_audit.v1"
OPEN_CARRY_CONTRACT_SCHEMA = "obvfutport_v2.t2_overlay_open_carry_contract.v1"
FROZEN_PROVENANCE_SCHEMA = "obvfutport_v2.t2_candidate_frozen_provenance.v1"


OPEN_CARRY_REQUIRED_STATE_FIELDS = (
    "entry_epoch",
    "entry_fill_price",
    "peak_return",
    "armed",
    "source_t2_position_id",
    "quote_history_mode",
    "ram_60_available_from",
)


OPEN_CARRY_CONTRACT = {
    "schema": OPEN_CARRY_CONTRACT_SCHEMA,
    "positions_remain_open_across_days": True,
    "required_state_fields": list(OPEN_CARRY_REQUIRED_STATE_FIELDS),
    "closed_pnl_excludes_open_marks": True,
    "marked_open_pnl_reported_separately": True,
    "closed_trade_success_rate_excludes_open_marks": True,
    "live_and_replay_continue_same_state_until": "overlay_exit_or_underlying_t2_exit",
    "exit_precedence": [
        "overlay_adverse_stop",
        "overlay_profit_capture",
        "overlay_armed_peak_floor",
        "overlay_session_close_when_configured",
        "underlying_t2_exit",
    ],
}


PROVENANCE_SOURCE_KEYS = (
    "opportunity_frame",
    "phase_report",
    "t2_export_manifest",
    "t2_ledger",
    "t2_open_state",
    "runtime_config",
    "contract_manifest",
    "adaptive_override",
    "hurst_universe_manifest",
)


PROVENANCE_REQUIRED_SOURCE_KEYS = (
    "opportunity_frame",
    "t2_export_manifest",
    "t2_ledger",
    "t2_open_state",
    "runtime_config",
    "contract_manifest",
    "adaptive_override",
)


THRESHOLD_FIELDS = (
    "primary_obv_short_abs_threshold",
    "entry_threshold",
    "hard_sl_points",
    "trail_activation_points",
    "trail_activation_effective_points",
    "t2_hard_sl_points",
    "t2_trail_activation_points",
    "threshold_source",
    "threshold_synthesized",
    "adaptive_combo_label",
    "adaptive_exit_combo_label",
)


CANDIDATES: list[dict[str, Any]] = [
    {
        "name": "smooth_survivor_balanced_score0p90_age30to300_runway45 + armed20_floor80_stop100",
        "policy_name": "smooth_survivor_balanced_score0p90_age30to300_runway45",
        "exit_engine": "custom",
        "exit": {
            "name": "armed20_floor80_stop100",
            "kind": "armed_peak_floor",
            "arm_target": 0.0020,
            "floor_fraction": 0.80,
            "hard_stop": 0.0100,
        },
    },
    {
        "name": "smooth_survivor_balanced_score0p90_age30to300_runway45 + armed20_floor80",
        "policy_name": "smooth_survivor_balanced_score0p90_age30to300_runway45",
        "exit_engine": "custom",
        "exit": {
            "name": "armed20_floor80",
            "kind": "armed_peak_floor",
            "arm_target": 0.0020,
            "floor_fraction": 0.80,
        },
    },
    {
        "name": "risk_first_edge_strict_score0p80_age90to300_runway45 + armed20_floor80",
        "policy_name": "risk_first_edge_strict_score0p80_age90to300_runway45",
        "exit_engine": "custom",
        "exit": {
            "name": "armed20_floor80",
            "kind": "armed_peak_floor",
            "arm_target": 0.0020,
            "floor_fraction": 0.80,
        },
    },
    {
        "name": "risk_first_edge_strict_score0p80_age90to300_runway45 + armed20_floor65",
        "policy_name": "risk_first_edge_strict_score0p80_age90to300_runway45",
        "exit_engine": "custom",
        "exit": {
            "name": "armed20_floor65",
            "kind": "armed_peak_floor",
            "arm_target": 0.0020,
            "floor_fraction": 0.65,
        },
    },
    {
        "name": "smooth_survivor_mild_score0p90_age60to240_runway0 + profit25",
        "policy_name": "smooth_survivor_mild_score0p90_age60to240_runway0",
        "exit_engine": "overlay",
        "exit": {"name": "profit_25bps", "kind": "profit", "target": 0.0025},
    },
]


def stable_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(stable_json_dumps(payload).encode("utf-8")).hexdigest()


def sha256_file(path: Path | None) -> str | None:
    if path is None or not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_root_path(root: Path, raw: Any) -> Path | None:
    if raw is None:
        return None
    path = Path(str(raw))
    if path.is_absolute():
        return path
    return root / path


def source_record(root: Path, path: Path | None, *, required: bool) -> dict[str, Any]:
    resolved = resolve_root_path(root, path) if path is not None else None
    exists = bool(resolved and resolved.exists() and resolved.is_file())
    return {
        "path": str(resolved) if resolved is not None else None,
        "required": required,
        "exists": exists,
        "size_bytes": int(resolved.stat().st_size) if exists else None,
        "sha256": sha256_file(resolved) if exists else None,
    }


def first_existing(paths: list[Path | None]) -> Path | None:
    for path in paths:
        if path is not None and path.exists() and path.is_file():
            return path
    return None


def newest_existing(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists() and path.is_file()]
    if not existing:
        return None
    return sorted(existing, key=lambda path: (path.stat().st_mtime, str(path)))[-1]


def find_string_paths(payload: Any, fragments: tuple[str, ...]) -> list[str]:
    out: list[str] = []
    if isinstance(payload, dict):
        for value in payload.values():
            out.extend(find_string_paths(value, fragments))
    elif isinstance(payload, list):
        for value in payload:
            out.extend(find_string_paths(value, fragments))
    elif isinstance(payload, str):
        text = payload
        if any(fragment in text for fragment in fragments):
            out.append(text)
    return out


def resolve_manifest_path(root: Path, manifest: dict[str, Any], fragments: tuple[str, ...], fallback_glob: str) -> Path | None:
    candidates = [resolve_root_path(root, value) for value in find_string_paths(manifest, fragments)]
    found = first_existing(candidates)
    if found is not None:
        return found
    return newest_existing(sorted((root / "state" / "canonical_exports").glob(fallback_glob)))


def latest_export_manifest(root: Path) -> Path | None:
    return newest_existing(sorted((root / "state" / "canonical_exports").glob("*/manifest.json")))


def runtime_config_path(root: Path) -> Path | None:
    return first_existing(
        [
            root / "runtime.json",
            root / "config" / "runtime.json",
            root / "config" / "obvfutport_v2_runtime.json",
            root / "config" / "obvfutport_v2_runtime_config.json",
            root / "state" / "runtime.json",
        ]
    ) or newest_existing(sorted((root / "config").glob("*runtime*.json")))


def adaptive_override_path(root: Path, runtime_config: dict[str, Any]) -> Path | None:
    from_runtime = [
        resolve_root_path(root, runtime_config.get("adaptive_calibration_path")),
        resolve_root_path(root, runtime_config.get("adaptive_calibration_path_local")),
        resolve_root_path(root, runtime_config.get("adaptive_override")),
    ]
    return first_existing(from_runtime) or first_existing(
        [
            root / "state" / "adaptive_calibration" / "v2_symbol_overrides_latest.json",
            root / "config" / "v2_symbol_overrides_latest.json",
        ]
    ) or newest_existing(sorted((root / "state").glob("**/*adaptive*override*.json")))


def hurst_universe_manifest_path(root: Path, runtime_config: dict[str, Any]) -> Path | None:
    from_runtime = [
        resolve_root_path(root, runtime_config.get("hurst_universe_manifest_path")),
        resolve_root_path(root, runtime_config.get("hurst_universe_manifest_path_local")),
    ]
    return first_existing(from_runtime) or newest_existing(sorted((root / "config").glob("*hurst*manifest*.json")))


def git_head(path: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            capture_output=True,
            check=False,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    value = proc.stdout.strip()
    return value or None


def module_file_record(module: Any) -> dict[str, Any]:
    module_file = getattr(module, "__file__", None)
    path = Path(module_file).resolve() if module_file else None
    return source_record(Path("/"), path, required=True)


def iter_jsonl_records(path: Path | None, *, limit: int | None = None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
                if limit is not None and len(rows) >= limit:
                    break
    return rows


def threshold_source_from_row(row: dict[str, Any]) -> str:
    raw = row.get("threshold_source")
    if raw:
        return str(raw)
    if row.get("threshold_synthesized") is True:
        return "hurst_manifest_synthesized"
    if row.get("threshold_synthesized") is False:
        return "v1_runtime"
    adaptive = row.get("adaptive_calibration") if isinstance(row.get("adaptive_calibration"), dict) else {}
    if adaptive.get("threshold_source"):
        return str(adaptive.get("threshold_source"))
    if adaptive.get("threshold_synthesized") is True:
        return "hurst_manifest_synthesized"
    if adaptive.get("threshold_synthesized") is False:
        return "v1_runtime"
    return "unknown"


def threshold_snapshot_from_ledger(ledger_path: Path | None) -> dict[str, Any]:
    rows = iter_jsonl_records(ledger_path)
    by_symbol: dict[str, dict[str, Any]] = {}
    source_counts: Counter[str] = Counter()
    hard_sl_exact_80_count = 0
    trail_activation_exact_180_count = 0
    event_count_by_type: Counter[str] = Counter()
    for row in rows:
        event_type = str(row.get("event_type") or row.get("event") or row.get("type") or "")
        if event_type:
            event_count_by_type[event_type] += 1
        symbol = str(row.get("symbol") or row.get("instrument_id") or row.get("instrument_name") or "").strip()
        if not symbol:
            continue
        source = threshold_source_from_row(row)
        source_counts[source] += 1
        hard_sl = finite(row.get("hard_sl_points"))
        trail_activation = finite(row.get("trail_activation_points"))
        if hard_sl is not None and abs(hard_sl - 80.0) <= 1e-12:
            hard_sl_exact_80_count += 1
        if trail_activation is not None and abs(trail_activation - 180.0) <= 1e-12:
            trail_activation_exact_180_count += 1
        existing = by_symbol.setdefault(symbol, {"symbol": symbol, "threshold_source": source})
        if existing.get("threshold_source") == "unknown" and source != "unknown":
            existing["threshold_source"] = source
        for field in THRESHOLD_FIELDS:
            value = row.get(field)
            if value is not None and field not in existing:
                existing[field] = value
    symbol_source_counts: Counter[str] = Counter(str(row.get("threshold_source") or "unknown") for row in by_symbol.values())
    return {
        "ledger_row_count": len(rows),
        "event_count_by_type": dict(sorted(event_count_by_type.items())),
        "effective_symbol_count": len(by_symbol),
        "event_threshold_source_counts": dict(sorted(source_counts.items())),
        "symbol_threshold_source_counts": dict(sorted(symbol_source_counts.items())),
        "hard_sl_exact_80_count": hard_sl_exact_80_count,
        "trail_activation_exact_180_count": trail_activation_exact_180_count,
        "effective_by_symbol": {symbol: by_symbol[symbol] for symbol in sorted(by_symbol)},
        "effective_by_symbol_hash": stable_hash({symbol: by_symbol[symbol] for symbol in sorted(by_symbol)}),
    }


def build_frozen_provenance(
    *,
    root: Path,
    opportunity_frame: Path | None,
    phase_report: Path | None,
    candidates: list[dict[str, Any]],
    policies: dict[str, continuation.ContinuationPolicy],
) -> dict[str, Any]:
    export_manifest_path = latest_export_manifest(root)
    export_manifest = base.read_json(export_manifest_path, {}) if export_manifest_path else {}
    t2_ledger_path = resolve_manifest_path(
        root,
        export_manifest,
        ("t2_ledger", "T2 ledger", "obvfutport_v2_t2_ledger"),
        "*/obvfutport_v2_t2_ledger_*.jsonl",
    )
    t2_open_state_path = resolve_manifest_path(
        root,
        export_manifest,
        ("t2_open_state", "T2 open", "obvfutport_v2_t2_open_state"),
        "*/obvfutport_v2_t2_open_state_*.json",
    )
    runtime_path = runtime_config_path(root)
    runtime_config = base.read_json(runtime_path, {}) if runtime_path else {}
    contract_path = root / "config" / "obvfutport_v2_contract_chain_manifest.json"
    adaptive_path = adaptive_override_path(root, runtime_config)
    hurst_path = hurst_universe_manifest_path(root, runtime_config)
    selected_policy_payload = {name: asdict(policy) for name, policy in policies.items() if name in {c["policy_name"] for c in candidates}}
    script_hashes = {
        "audit_t2_candidate_deployment_readiness": module_file_record(sys.modules[__name__]),
        "audit_selected_t2_custom_exit_candidates": module_file_record(custom_exit_audit),
        "backtest_tranche_portfolio_overlay": module_file_record(base),
        "research_t2_continuation_filters": module_file_record(continuation),
        "research_t2_mfe_first_profit_capture": module_file_record(overlay_research),
        "research_t2_portfolio_rules": module_file_record(portfolio_rules),
        "run_v2matrix_overlay": module_file_record(live_overlay),
    }
    sources = {
        "opportunity_frame": source_record(root, opportunity_frame, required=True),
        "phase_report": source_record(root, phase_report, required=False),
        "t2_export_manifest": source_record(root, export_manifest_path, required=True),
        "t2_ledger": source_record(root, t2_ledger_path, required=True),
        "t2_open_state": source_record(root, t2_open_state_path, required=True),
        "runtime_config": source_record(root, runtime_path, required=True),
        "contract_manifest": source_record(root, contract_path, required=True),
        "adaptive_override": source_record(root, adaptive_path, required=True),
        "hurst_universe_manifest": source_record(root, hurst_path, required=False),
    }
    missing_required = [key for key in PROVENANCE_REQUIRED_SOURCE_KEYS if not bool(sources.get(key, {}).get("exists"))]
    threshold_snapshot = threshold_snapshot_from_ledger(t2_ledger_path)
    payload = {
        "schema": FROZEN_PROVENANCE_SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "root": str(root),
        "git_commit": git_head(root),
        "sources": sources,
        "missing_required_sources": missing_required,
        "candidate_rules": {
            "candidates": candidates,
            "selected_policies": selected_policy_payload,
            "hash": stable_hash({"candidates": candidates, "selected_policies": selected_policy_payload}),
        },
        "thresholds": threshold_snapshot,
        "script_hashes": script_hashes,
        "script_hash": stable_hash({name: record.get("sha256") for name, record in script_hashes.items()}),
    }
    payload["hash"] = stable_hash(
        {
            "sources": {key: (value or {}).get("sha256") for key, value in sources.items()},
            "candidate_rules_hash": payload["candidate_rules"]["hash"],
            "threshold_hash": threshold_snapshot.get("effective_by_symbol_hash"),
            "script_hash": payload["script_hash"],
        }
    )
    return payload


def load_expected_provenance(path: Path) -> dict[str, Any]:
    payload = base.read_json(path, {})
    if isinstance(payload, dict) and isinstance(payload.get("frozen_provenance"), dict):
        return payload["frozen_provenance"]
    return payload if isinstance(payload, dict) else {}


def nested_get(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = payload
    for part in path:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def compare_frozen_provenance(current: dict[str, Any], expected: dict[str, Any]) -> list[dict[str, Any]]:
    paths: list[tuple[str, ...]] = [("hash",), ("candidate_rules", "hash"), ("thresholds", "effective_by_symbol_hash"), ("script_hash",)]
    for source_key in PROVENANCE_SOURCE_KEYS:
        paths.append(("sources", source_key, "sha256"))
    mismatches: list[dict[str, Any]] = []
    for path in paths:
        current_value = nested_get(current, path)
        expected_value = nested_get(expected, path)
        if current_value != expected_value:
            mismatches.append(
                {
                    "field": ".".join(path),
                    "current": current_value,
                    "expected": expected_value,
                }
            )
    return mismatches


def position_id_from_row_id(row_id: Any) -> str | None:
    parts = str(row_id or "").split("|")
    return parts[2] if len(parts) >= 3 and parts[2] else None


def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def path_peak_return(row: dict[str, Any], exit_epoch: int) -> float | None:
    window = row.get("window")
    if not isinstance(window, pd.DataFrame) or window.empty or "forward_return" not in window.columns:
        return None
    subset = window.loc[pd.to_numeric(window["clock_epoch"], errors="coerce") <= int(exit_epoch)]
    returns = pd.to_numeric(subset["forward_return"], errors="coerce").dropna()
    returns = returns[returns.map(math.isfinite)]
    if returns.empty:
        return None
    return float(returns.max())


def open_carry_state_coverage(open_rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_by_field: dict[str, int] = {field: 0 for field in OPEN_CARRY_REQUIRED_STATE_FIELDS}
    row_issues: list[dict[str, Any]] = []
    for row in open_rows:
        missing_fields: list[str] = []
        for field in OPEN_CARRY_REQUIRED_STATE_FIELDS:
            value = row.get(field)
            if value is None:
                missing_fields.append(field)
            elif isinstance(value, str) and not value.strip():
                missing_fields.append(field)
            elif isinstance(value, float) and math.isnan(value):
                missing_fields.append(field)
        for field in missing_fields:
            missing_by_field[field] += 1
        if missing_fields:
            row_issues.append(
                {
                    "candidate": row.get("candidate"),
                    "row_id": row.get("row_id"),
                    "symbol": row.get("symbol"),
                    "missing_fields": missing_fields,
                }
            )
    return {
        "required_fields": list(OPEN_CARRY_REQUIRED_STATE_FIELDS),
        "open_row_count": len(open_rows),
        "rows_with_missing_required_fields": len(row_issues),
        "missing_by_field": {key: value for key, value in missing_by_field.items() if value},
        "row_issues": row_issues[:50],
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        seen: list[str] = []
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.append(key)
        fieldnames = seen
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def finite(value: Any) -> float | None:
    return base.as_float(value)


def as_int(value: Any) -> int | None:
    return base.as_int(value)


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def epoch_date(epoch: Any) -> str | None:
    epoch_int = as_int(epoch)
    if epoch_int is None:
        return None
    return datetime.fromtimestamp(epoch_int, tz=base.IST).date().isoformat()


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


def summarize_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [row for row in trades if not row.get("open_at_period_end")]
    marked = list(trades)
    closed_net = [float(row["net_rupees_per_lot"]) for row in closed if finite(row.get("net_rupees_per_lot")) is not None]
    marked_net = [float(row["net_rupees_per_lot"]) for row in marked if finite(row.get("net_rupees_per_lot")) is not None]
    closed_margin = [
        float(row["net_return_on_margin_pct"]) for row in closed if finite(row.get("net_return_on_margin_pct")) is not None
    ]
    marked_margin = [
        float(row["net_return_on_margin_pct"]) for row in marked if finite(row.get("net_return_on_margin_pct")) is not None
    ]
    hold = [float(row["hold_minutes"]) for row in closed if finite(row.get("hold_minutes")) is not None]
    reasons: dict[str, int] = {}
    for row in trades:
        reason = str(row.get("exit_reason") or "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "trade_count": len(trades),
        "closed_count": len(closed),
        "open_at_period_end_count": len(trades) - len(closed),
        "success_rate_pct": round(sum(1 for value in closed_net if value > 0) / len(closed_net) * 100.0, 4)
        if closed_net
        else None,
        "marked_success_rate_pct": round(sum(1 for value in marked_net if value > 0) / len(marked_net) * 100.0, 4)
        if marked_net
        else None,
        "net_rupees_per_lot": metric_stats(closed_net),
        "marked_net_rupees_per_lot": metric_stats(marked_net),
        "net_return_on_margin_pct": metric_stats(closed_margin),
        "marked_net_return_on_margin_pct": metric_stats(marked_margin),
        "hold_minutes": metric_stats(hold),
        "exit_reasons": reasons,
    }


def split_summary(trades: list[dict[str, Any]], start_day: str, end_day: str) -> dict[str, Any]:
    return summarize_trades([row for row in trades if start_day <= str(row.get("entry_date") or "") <= end_day])


def exact_day_summary(trades: list[dict[str, Any]], day: str) -> dict[str, Any]:
    return summarize_trades([row for row in trades if str(row.get("entry_date") or "") == day])


def policy_mask(frame: pd.DataFrame, policy: continuation.ContinuationPolicy) -> pd.Series:
    score = pd.to_numeric(frame[f"score_{policy.formula}"], errors="coerce")
    max_age = float(policy.max_age_minutes if policy.max_age_minutes is not None else 10**9)
    return (
        (score >= policy.min_score)
        & (pd.to_numeric(frame["age_minutes"], errors="coerce") >= policy.min_age_minutes)
        & (pd.to_numeric(frame["age_minutes"], errors="coerce") <= max_age)
        & (pd.to_numeric(frame["current_ret"], errors="coerce") >= policy.min_current_ret)
        & (pd.to_numeric(frame["mfe"], errors="coerce") >= policy.min_mfe)
        & (pd.to_numeric(frame["mae_abs"], errors="coerce") <= policy.max_mae_abs)
        & (pd.to_numeric(frame["drawdown_to_mfe"], errors="coerce") <= policy.max_drawdown_to_mfe)
        & (pd.to_numeric(frame["positive_ram_count"], errors="coerce") >= policy.min_positive_ram_count)
        & (pd.to_numeric(frame["spread_bps"], errors="coerce") <= policy.max_spread_bps)
        & (pd.to_numeric(frame["edge_to_cost_multiple"], errors="coerce") >= policy.min_edge_cost_multiple)
        & (pd.to_numeric(frame["minutes_to_session_end"], errors="coerce") >= policy.min_minutes_to_session_end)
    )


def candidate_entry_passes(row: dict[str, Any], policy: continuation.ContinuationPolicy) -> tuple[bool, list[str]]:
    issues: list[str] = []
    checks = {
        f"score_{policy.formula}": (finite(row.get(f"score_{policy.formula}")), ">=", policy.min_score),
        "age_minutes": (finite(row.get("age_minutes")), ">=", policy.min_age_minutes),
        "current_ret": (finite(row.get("current_ret")), ">=", policy.min_current_ret),
        "mfe": (finite(row.get("mfe")), ">=", policy.min_mfe),
        "mae_abs": (finite(row.get("mae_abs")), "<=", policy.max_mae_abs),
        "drawdown_to_mfe": (finite(row.get("drawdown_to_mfe")), "<=", policy.max_drawdown_to_mfe),
        "positive_ram_count": (finite(row.get("positive_ram_count")), ">=", policy.min_positive_ram_count),
        "spread_bps": (finite(row.get("spread_bps")), "<=", policy.max_spread_bps),
        "edge_to_cost_multiple": (finite(row.get("edge_to_cost_multiple")), ">=", policy.min_edge_cost_multiple),
        "minutes_to_session_end": (finite(row.get("minutes_to_session_end")), ">=", policy.min_minutes_to_session_end),
    }
    if policy.max_age_minutes is not None:
        checks["age_minutes_max"] = (finite(row.get("age_minutes")), "<=", float(policy.max_age_minutes))
    for key, (actual, op, expected) in checks.items():
        if actual is None:
            issues.append(f"{key}:missing")
        elif op == ">=" and actual < float(expected):
            issues.append(f"{key}:{actual}<min{expected}")
        elif op == "<=" and actual > float(expected):
            issues.append(f"{key}:{actual}>max{expected}")
    return not issues, issues


def load_t2_leg_maps(root: Path) -> tuple[dict[str, base.TrancheLeg], dict[tuple[str, str, int], base.TrancheLeg]]:
    loaded = base.load_rows(root)
    manifest = base.load_contract_manifest(root)
    margins = base.load_margin_lookup(root)
    legs = base.build_legs(loaded.get("rows_by_tranche") or {}, manifest, margins)["T2"]
    by_row = {leg.row_id: leg for leg in legs}
    by_identity = {(leg.symbol, leg.position_id, int(leg.entry_epoch)): leg for leg in legs}
    return by_row, by_identity


def leg_for_row(row: dict[str, Any], by_row: dict[str, base.TrancheLeg], by_identity: dict[tuple[str, str, int], base.TrancheLeg]) -> base.TrancheLeg | None:
    row_id = str(row.get("row_id") or "")
    if row_id in by_row:
        return by_row[row_id]
    parts = row_id.split("|")
    if len(parts) >= 4:
        entry_epoch = as_int(parts[3])
        if entry_epoch is not None:
            return by_identity.get((parts[0], parts[2], entry_epoch))
    return None


def make_candidate_trades(
    *,
    frame: pd.DataFrame,
    path_lookup: dict[str, pd.DataFrame],
    policy: continuation.ContinuationPolicy,
    candidate: dict[str, Any],
    v1_portfolio: Any,
    mae_floor: float,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    selected = continuation.first_passing_rows(frame, policy).copy()
    selected_by_row_id = {str(row.row_id): row._asdict() for row in selected.itertuples(index=False)}
    rows = overlay_research.policy_path_rows(frame, path_lookup, policy, mae_floor, include_window=True)
    trades: list[dict[str, Any]] = []
    for row in rows:
        if candidate["exit_engine"] == "custom":
            exit_row = custom_exit_audit.choose_custom_exit(row, candidate["exit"])
        else:
            exit_row = overlay_research.choose_overlay_exit(
                row,
                candidate["exit"],
                f"score_{policy.formula}",
                policy.min_score,
            )
        account = overlay_research.overlay_accounting(v1_portfolio, row, exit_row)
        entry_epoch = int(row["qualification_epoch"])
        exit_epoch = int(exit_row["exit_epoch"])
        peak_return = first_non_empty(exit_row.get("peak_return"), path_peak_return(row, exit_epoch), row.get("mfe"))
        armed = exit_row.get("armed")
        if armed is None:
            armed = False
        source_open_at_period_end = truthy(row.get("open_at_period_end"))
        open_at_period_end = source_open_at_period_end and str(exit_row.get("exit_reason") or "") == "open_at_period_end"
        exit_reason = "open_at_period_end" if open_at_period_end else exit_row.get("exit_reason")
        source = selected_by_row_id.get(str(row.get("row_id")), {})
        trades.append(
            {
                "candidate": candidate["name"],
                "policy_name": policy.name,
                "exit": candidate["exit"]["name"],
                "row_id": row.get("row_id"),
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "source_t2_position_id": first_non_empty(
                    row.get("position_id"),
                    source.get("position_id"),
                    position_id_from_row_id(row.get("row_id")),
                ),
                "entry_epoch": entry_epoch,
                "entry_time": base.epoch_ist_iso(entry_epoch),
                "entry_date": epoch_date(entry_epoch),
                "t2_entry_epoch": source.get("t2_entry_epoch"),
                "t2_entry_time": source.get("t2_entry_time"),
                "t2_exit_epoch": row.get("t2_exit_epoch"),
                "t2_exit_time": base.epoch_ist_iso(as_int(row.get("t2_exit_epoch"))),
                "t2_actual_exit_epoch": row.get("t2_actual_exit_epoch"),
                "t2_status": row.get("t2_status"),
                "exit_epoch": exit_epoch,
                "exit_time": base.epoch_ist_iso(exit_epoch),
                "exit_date": epoch_date(exit_epoch),
                "exit_reason": exit_reason,
                "exit_return": exit_row.get("exit_return"),
                "hold_minutes": exit_row.get("exit_duration_minutes"),
                "entry_fill_price": row.get("entry_fill_price"),
                "exit_price": exit_row.get("exit_price"),
                "peak_return": peak_return,
                "armed": bool(armed),
                "active_floor_return": exit_row.get("active_floor_return"),
                "net_rupees_per_lot": account.get("net_rupees"),
                "gross_rupees_per_lot": account.get("gross_rupees"),
                "charges_rupees_per_lot": account.get("charges_rupees"),
                "net_return_on_margin_pct": account.get("net_return_on_margin_pct"),
                "margin_per_lot": row.get("margin_per_lot"),
                "lot_size": row.get("lot_size"),
                "quote_history_mode": first_non_empty(source.get("quote_history_mode"), row.get("quote_history_mode")),
                "quote_history_key_scope": first_non_empty(
                    source.get("quote_history_key_scope"),
                    row.get("quote_history_key_scope"),
                ),
                "ram_60_available_from_epoch": first_non_empty(
                    source.get("ram_60_available_from_epoch"),
                    row.get("ram_60_available_from_epoch"),
                ),
                "ram_60_available_from": first_non_empty(
                    source.get("ram_60_available_from"),
                    row.get("ram_60_available_from"),
                    base.epoch_ist_iso(as_int(first_non_empty(source.get("ram_60_available_from_epoch"), row.get("ram_60_available_from_epoch")))),
                ),
                "open_at_period_end": open_at_period_end,
            }
        )
    return selected, trades


def recompute_clock_scores(frame: pd.DataFrame, selected: pd.DataFrame, policies: dict[str, continuation.ContinuationPolicy]) -> dict[tuple[int, str], dict[str, Any]]:
    if selected.empty:
        return {}
    clocks = {int(clock) for clock in pd.to_numeric(selected["clock_epoch"], errors="coerce").dropna()}
    subset = frame.loc[pd.to_numeric(frame["clock_epoch"], errors="coerce").isin(clocks)].copy()
    out: dict[tuple[int, str], dict[str, Any]] = {}
    for clock_epoch, group in subset.groupby("clock_epoch", sort=False):
        feature_rows = group.to_dict("records")
        portfolio_rules.add_ranks(feature_rows)
        for feature in feature_rows:
            row_id = str(feature.get("row_id") or "")
            if not row_id:
                continue
            scores = {}
            for formula in {policy.formula for policy in policies.values()}:
                scores[formula] = portfolio_rules.blended_score(feature, formula)
            out[(int(clock_epoch), row_id)] = scores
    return out


def audit_no_lookahead(
    *,
    frame: pd.DataFrame,
    selected: pd.DataFrame,
    policy: continuation.ContinuationPolicy,
    candidate_name: str,
    recomputed_scores: dict[tuple[int, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []
    if selected.empty:
        return traces
    frame_by_row: dict[str, pd.DataFrame] = {
        row_id: group.sort_values("clock_epoch", kind="mergesort")
        for row_id, group in frame.loc[frame["row_id"].isin(set(selected["row_id"].astype(str)))].groupby("row_id", sort=False)
    }
    used_fields = [
        f"score_{policy.formula}",
        "age_minutes",
        "current_ret",
        "mfe",
        "mae_abs",
        "drawdown_to_mfe",
        "positive_ram_count",
        "spread_bps",
        "edge_to_cost_multiple",
        "minutes_to_session_end",
        "ram_10",
        "ram_30",
        "ram_60",
        "ret_10",
        "ret_30",
        "ret_60",
        "entry_fill_price",
    ]
    for raw in selected.to_dict("records"):
        row = {str(k): v for k, v in raw.items()}
        row_id = str(row.get("row_id") or "")
        clock_epoch = as_int(row.get("clock_epoch"))
        issues: list[str] = []
        if clock_epoch is None:
            issues.append("clock_epoch:missing")
            clock_epoch = 0
        t2_entry_epoch = as_int(row.get("t2_entry_epoch"))
        t2_exit_epoch = as_int(row.get("t2_exit_epoch"))
        t2_actual_exit_epoch = as_int(row.get("t2_actual_exit_epoch"))
        if t2_entry_epoch is None or t2_entry_epoch > clock_epoch:
            issues.append("t2_entry_after_candidate_clock")
        if t2_exit_epoch is not None and t2_exit_epoch <= clock_epoch:
            issues.append("t2_exit_not_after_candidate_clock")
        if t2_actual_exit_epoch is not None and t2_actual_exit_epoch <= clock_epoch:
            issues.append("t2_actual_exit_not_after_candidate_clock")
        ok_policy, policy_issues = candidate_entry_passes(row, policy)
        if not ok_policy:
            issues.extend(f"policy:{item}" for item in policy_issues)
        for field in used_fields:
            if finite(row.get(field)) is None:
                issues.append(f"{field}:missing_or_nonfinite")
        input_cutoff = clock_epoch - 60
        ram_end = as_int(row.get("ram_60_window_end_epoch"))
        ram_start = as_int(row.get("ram_60_window_start_epoch"))
        ram_available = as_int(row.get("ram_60_available_from_epoch"))
        hist_start = as_int(row.get("signal_key_history_earliest_epoch"))
        hist_end = as_int(row.get("signal_key_history_latest_epoch"))
        if ram_end is None or ram_end > input_cutoff:
            issues.append("ram60_window_end_after_input_cutoff")
        if ram_start is None or (ram_end is not None and ram_start > ram_end):
            issues.append("ram60_window_start_invalid")
        if ram_available is None or ram_available > clock_epoch:
            issues.append("ram60_not_available_by_entry_clock")
        if hist_start is None or (ram_start is not None and hist_start > ram_start):
            issues.append("signal_history_starts_after_ram60_window")
        if hist_end is None or (ram_end is not None and hist_end < ram_end):
            issues.append("signal_history_ends_before_ram60_window")
        path = frame_by_row.get(row_id)
        earlier_pass_count = 0
        first_prior_pass_epoch = None
        if path is not None and not path.empty:
            earlier = path.loc[pd.to_numeric(path["clock_epoch"], errors="coerce") < clock_epoch]
            if not earlier.empty:
                prior_passes = earlier.loc[policy_mask(earlier, policy)]
                earlier_pass_count = int(prior_passes.shape[0])
                if earlier_pass_count:
                    first_prior_pass_epoch = int(prior_passes.iloc[0]["clock_epoch"])
                    issues.append("not_first_passing_clock")
        recomputed = recomputed_scores.get((clock_epoch, row_id), {}).get(policy.formula)
        stored = finite(row.get(f"score_{policy.formula}"))
        if stored is None or recomputed is None or abs(float(stored) - float(recomputed)) > 1e-9:
            issues.append("stored_score_recompute_mismatch")
        age = finite(row.get("age_minutes"))
        if age is not None and t2_entry_epoch is not None:
            expected_age = max(0.0, (clock_epoch - t2_entry_epoch) / 60.0)
            if abs(age - expected_age) > 1e-6:
                issues.append("age_minutes_mismatch")
        traces.append(
            {
                "candidate": candidate_name,
                "policy_name": policy.name,
                "row_id": row_id,
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "entry_epoch": clock_epoch,
                "entry_time": base.epoch_ist_iso(clock_epoch),
                "input_cutoff_epoch": input_cutoff,
                "input_cutoff_time": base.epoch_ist_iso(input_cutoff),
                "t2_entry_epoch": t2_entry_epoch,
                "t2_entry_time": row.get("t2_entry_time"),
                "t2_exit_epoch": t2_exit_epoch,
                "t2_exit_time": row.get("t2_exit_time"),
                "score": stored,
                "score_recomputed": recomputed,
                "ram_60_window_start_epoch": ram_start,
                "ram_60_window_start": row.get("ram_60_window_start"),
                "ram_60_window_end_epoch": ram_end,
                "ram_60_window_end": row.get("ram_60_window_end"),
                "ram_60_available_from_epoch": ram_available,
                "ram_60_available_from": row.get("ram_60_available_from"),
                "quote_history_mode": row.get("quote_history_mode"),
                "quote_history_key_scope": row.get("quote_history_key_scope"),
                "earlier_policy_pass_count": earlier_pass_count,
                "first_prior_pass_epoch": first_prior_pass_epoch,
                "audit_status": "PASS" if not issues else "FAIL",
                "issues": ";".join(issues),
            }
        )
    return traces


def edge_coverage(frame: pd.DataFrame, selected_all: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    edge = pd.to_numeric(frame.get("edge_to_cost_multiple"), errors="coerce")
    out["universe_rows"] = int(frame.shape[0])
    out["universe_edge_missing_rows"] = int(edge.isna().sum())
    ret_missing = {}
    for col in ("ret_10", "ret_30", "ret_60"):
        ret_missing[col] = int(pd.to_numeric(frame.get(col), errors="coerce").isna().sum()) if col in frame.columns else int(frame.shape[0])
    out["universe_missing_ret_counts"] = ret_missing
    selected_edge = pd.to_numeric(selected_all.get("edge_to_cost_multiple"), errors="coerce") if not selected_all.empty else pd.Series(dtype="float64")
    out["selected_entry_rows"] = int(selected_all.shape[0])
    out["selected_edge_missing_rows"] = int(selected_edge.isna().sum()) if not selected_all.empty else 0
    return out


def load_cache_payload(cache_file: str | None) -> dict[str, list[tuple[int, float, float, float | None, float | None]]]:
    if not cache_file:
        return {}
    path = Path(cache_file)
    if not path.exists():
        return {}
    return portfolio_rules.load_day_quote_cache(path)


def phase_day_order(phase_report: dict[str, Any]) -> list[str]:
    per_day = (((phase_report or {}).get("input_stream") or {}).get("per_day") or {})
    return sorted(str(day) for day in per_day.keys())


def previous_trade_day(day_order: list[str], day: str) -> str | None:
    if day not in day_order:
        return None
    idx = day_order.index(day)
    return day_order[idx - 1] if idx > 0 else None


def live_like_index_for_entry(
    *,
    phase_report: dict[str, Any],
    entry_day: str,
    clock_epoch: int,
    keys: set[str],
    max_rows_per_key: int,
) -> tuple[base.QuoteIndex, dict[str, Any]]:
    per_day = (((phase_report or {}).get("input_stream") or {}).get("per_day") or {})
    day_order = phase_day_order(phase_report)
    prior_day = previous_trade_day(day_order, entry_day)
    index = base.QuoteIndex()
    loaded: list[dict[str, Any]] = []
    for day in [prior_day, entry_day]:
        if day is None:
            continue
        payload = load_cache_payload((per_day.get(day) or {}).get("cache_file"))
        loaded_keys = 0
        loaded_rows = 0
        for key in keys:
            rows = payload.get(key) or []
            if day == entry_day:
                selected = [row for row in rows if len(row) >= 5 and int(row[0]) <= clock_epoch]
            else:
                selected = list(rows)
            selected = selected[-max_rows_per_key:]
            if selected:
                loaded_keys += 1
            for minute, event_epoch, price, bid, ask in selected:
                index.add(str(key), float(event_epoch), float(price), finite(bid), finite(ask))
                loaded_rows += 1
        loaded.append(
            {
                "trade_date": day,
                "cache_file": (per_day.get(day) or {}).get("cache_file"),
                "payload_key_count": len(payload),
                "requested_key_count": len(keys),
                "loaded_key_count": loaded_keys,
                "loaded_row_count": loaded_rows,
            }
        )
    index.finalize()
    return index, {"entry_day": entry_day, "prior_day": prior_day, "loaded": loaded}


def audit_quote_history(
    *,
    selected_rows: pd.DataFrame,
    phase_report: dict[str, Any],
    by_row: dict[str, base.TrancheLeg],
    by_identity: dict[tuple[str, str, int], base.TrancheLeg],
    risk_floor: float,
    live_required_keys: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    per_day = (((phase_report or {}).get("input_stream") or {}).get("per_day") or {})
    cache_payload_by_day: dict[str, dict[str, list[tuple[int, float, float, float | None, float | None]]]] = {}
    for raw in selected_rows.to_dict("records"):
        row = {str(k): v for k, v in raw.items()}
        clock_epoch = as_int(row.get("clock_epoch"))
        entry_day = epoch_date(clock_epoch)
        leg = leg_for_row(row, by_row, by_identity)
        issues: list[str] = []
        if leg is None:
            issues.append("source_leg_not_found")
        if clock_epoch is None or entry_day is None:
            issues.append("entry_clock_missing")
        candidate_keys: set[str] = set()
        if leg is not None:
            candidate_keys = {leg.signal_key, leg.execution_key}
            if not candidate_keys <= live_required_keys:
                issues.append("candidate_key_not_in_live_potential_scope")
        day_payload = cache_payload_by_day.get(entry_day or "")
        if day_payload is None and entry_day in per_day:
            day_payload = load_cache_payload((per_day.get(entry_day) or {}).get("cache_file"))
            cache_payload_by_day[entry_day] = day_payload
        missing_candidate_keys_in_day_cache = sorted(candidate_keys.difference(day_payload.keys() if day_payload else set()))
        if missing_candidate_keys_in_day_cache:
            issues.append("candidate_key_missing_from_entry_day_cache")
        ram60_value_match = None
        ret60_value_match = None
        ram_window_match = None
        live_ram60_available_from_epoch = None
        live_ram60_window_start_epoch = None
        live_ram60_window_end_epoch = None
        live_quote_report: dict[str, Any] = {}
        if leg is not None and clock_epoch is not None and entry_day is not None:
            index, live_quote_report = live_like_index_for_entry(
                phase_report=phase_report,
                entry_day=entry_day,
                clock_epoch=clock_epoch,
                keys=candidate_keys,
                max_rows_per_key=live_overlay.QUOTE_RING_STATE_ROWS_PER_KEY,
            )
            live_ram60 = base.risk_adjusted_momentum(index, leg, clock_epoch, 60, risk_floor=risk_floor)
            live_ret60 = base.directional_return(index, leg, clock_epoch, 60)
            live_ram60_available_from_epoch = index.ram_available_from_epoch(leg.signal_key, 60)
            bounds = index.lookback_window_bounds(leg.signal_key, clock_epoch - 60, 60)
            if bounds:
                live_ram60_window_start_epoch, live_ram60_window_end_epoch = bounds
            frame_ram60 = finite(row.get("ram_60"))
            frame_ret60 = finite(row.get("ret_60"))
            ram60_value_match = (
                live_ram60 is not None and frame_ram60 is not None and abs(float(live_ram60) - float(frame_ram60)) <= 1e-9
            )
            ret60_value_match = live_ret60 is not None and frame_ret60 is not None and abs(float(live_ret60) - float(frame_ret60)) <= 1e-12
            ram_window_match = (
                live_ram60_window_start_epoch == as_int(row.get("ram_60_window_start_epoch"))
                and live_ram60_window_end_epoch == as_int(row.get("ram_60_window_end_epoch"))
            )
            if not ram60_value_match:
                issues.append("live_like_ram60_value_mismatch")
            if not ret60_value_match:
                issues.append("live_like_ret60_value_mismatch")
            if not ram_window_match:
                issues.append("live_like_ram60_window_mismatch")
        rows.append(
            {
                "row_id": row.get("row_id"),
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "entry_epoch": clock_epoch,
                "entry_time": base.epoch_ist_iso(clock_epoch),
                "entry_day": entry_day,
                "signal_key": leg.signal_key if leg else None,
                "execution_key": leg.execution_key if leg else None,
                "quote_history_mode": row.get("quote_history_mode"),
                "quote_history_key_scope": row.get("quote_history_key_scope"),
                "frame_ram_60": finite(row.get("ram_60")),
                "frame_ret_60": finite(row.get("ret_60")),
                "live_like_ram60_available_from_epoch": live_ram60_available_from_epoch,
                "frame_ram60_available_from_epoch": as_int(row.get("ram_60_available_from_epoch")),
                "live_like_ram60_window_start_epoch": live_ram60_window_start_epoch,
                "live_like_ram60_window_end_epoch": live_ram60_window_end_epoch,
                "frame_ram60_window_start_epoch": as_int(row.get("ram_60_window_start_epoch")),
                "frame_ram60_window_end_epoch": as_int(row.get("ram_60_window_end_epoch")),
                "ram60_value_match": ram60_value_match,
                "ret60_value_match": ret60_value_match,
                "ram60_window_match": ram_window_match,
                "missing_candidate_keys_in_day_cache": ",".join(missing_candidate_keys_in_day_cache),
                "live_quote_loaded": json.dumps(live_quote_report, sort_keys=True),
                "audit_status": "PASS" if not issues else "FAIL",
                "issues": ";".join(issues),
            }
        )
    return rows


def cache_key_coverage(
    *,
    phase_report: dict[str, Any],
    required_keys: set[str],
    selected_rows: pd.DataFrame,
    by_row: dict[str, base.TrancheLeg],
    by_identity: dict[tuple[str, str, int], base.TrancheLeg],
) -> list[dict[str, Any]]:
    per_day = (((phase_report or {}).get("input_stream") or {}).get("per_day") or {})
    candidate_keys_by_day: dict[str, set[str]] = {}
    for raw in selected_rows.to_dict("records"):
        row = {str(k): v for k, v in raw.items()}
        day = epoch_date(row.get("clock_epoch"))
        leg = leg_for_row(row, by_row, by_identity)
        if not day or leg is None:
            continue
        candidate_keys_by_day.setdefault(day, set()).update({leg.signal_key, leg.execution_key})
    rows: list[dict[str, Any]] = []
    for day, meta in sorted(per_day.items()):
        payload = load_cache_payload(meta.get("cache_file"))
        payload_keys = set(payload.keys())
        missing_required = sorted(required_keys.difference(payload_keys))
        candidate_keys = candidate_keys_by_day.get(str(day), set())
        missing_candidate = sorted(candidate_keys.difference(payload_keys))
        rows.append(
            {
                "trade_date": day,
                "cache_file": meta.get("cache_file"),
                "cache_status": meta.get("cache_status"),
                "required_key_count": len(required_keys),
                "payload_key_count": len(payload_keys),
                "missing_required_key_count": len(missing_required),
                "candidate_entry_key_count": len(candidate_keys),
                "missing_candidate_entry_key_count": len(missing_candidate),
                "missing_required_samples": ",".join(missing_required[:10]),
                "missing_candidate_key_samples": ",".join(missing_candidate[:10]),
            }
        )
    return rows


def open_carry_rows(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "candidate": row.get("candidate"),
            "policy_name": row.get("policy_name"),
            "exit": row.get("exit"),
            "row_id": row.get("row_id"),
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "source_t2_position_id": row.get("source_t2_position_id"),
            "entry_epoch": row.get("entry_epoch"),
            "entry_time": row.get("entry_time"),
            "entry_fill_price": row.get("entry_fill_price"),
            "peak_return": row.get("peak_return"),
            "armed": row.get("armed"),
            "active_floor_return": row.get("active_floor_return"),
            "quote_history_mode": row.get("quote_history_mode"),
            "quote_history_key_scope": row.get("quote_history_key_scope"),
            "ram_60_available_from_epoch": row.get("ram_60_available_from_epoch"),
            "ram_60_available_from": row.get("ram_60_available_from"),
            "period_mark_time": row.get("exit_time"),
            "exit_reason": row.get("exit_reason"),
            "marked_net_rupees_per_lot": row.get("net_rupees_per_lot"),
            "marked_net_return_on_margin_pct": row.get("net_return_on_margin_pct"),
            "t2_status": row.get("t2_status"),
            "t2_actual_exit_epoch": row.get("t2_actual_exit_epoch"),
        }
        for row in trades
        if row.get("open_at_period_end")
    ]


def flatten_summary(candidate: str, summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate": candidate,
        "trade_count": summary.get("trade_count"),
        "closed_count": summary.get("closed_count"),
        "open_at_period_end_count": summary.get("open_at_period_end_count"),
        "success_rate_pct": summary.get("success_rate_pct"),
        "marked_success_rate_pct": summary.get("marked_success_rate_pct"),
        "total_net_rupees_per_lot": (summary.get("net_rupees_per_lot") or {}).get("sum"),
        "marked_total_net_rupees_per_lot": (summary.get("marked_net_rupees_per_lot") or {}).get("sum"),
        "median_net_rupees_per_lot": (summary.get("net_rupees_per_lot") or {}).get("median"),
        "worst_net_rupees_per_lot": (summary.get("net_rupees_per_lot") or {}).get("min"),
        "median_margin_return_pct": (summary.get("net_return_on_margin_pct") or {}).get("median"),
        "worst_margin_return_pct": (summary.get("net_return_on_margin_pct") or {}).get("min"),
        "median_hold_minutes": (summary.get("hold_minutes") or {}).get("median"),
        "exit_reasons": json.dumps(summary.get("exit_reasons") or {}, sort_keys=True),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--opportunity-frame", type=Path, required=True)
    parser.add_argument("--phase-report", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--candidate-regex", default=None)
    parser.add_argument("--mae-floor", type=float, default=0.0005)
    parser.add_argument("--risk-floor", type=float, default=0.001)
    parser.add_argument("--expected-provenance", type=Path, default=None)
    parser.add_argument(
        "--accept-open-carry-contract",
        action="store_true",
        help="Record explicit acceptance that open overlay trades carry forward and are excluded from closed-trade success.",
    )
    args = parser.parse_args()

    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    base.add_paths(args.root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    frame = pd.read_parquet(args.opportunity_frame)
    phase_report = base.read_json(args.phase_report, {}) if args.phase_report else {}
    policies = {policy.name: policy for policy in continuation.continuation_policy_grid()}
    candidates = CANDIDATES
    if args.candidate_regex:
        pattern = re.compile(args.candidate_regex)
        candidates = [candidate for candidate in CANDIDATES if pattern.search(candidate["name"])]
    frozen_provenance = build_frozen_provenance(
        root=args.root,
        opportunity_frame=args.opportunity_frame,
        phase_report=args.phase_report,
        candidates=candidates,
        policies=policies,
    )
    expected_provenance: dict[str, Any] = {}
    provenance_mismatches: list[dict[str, Any]] = []
    if args.expected_provenance:
        expected_provenance = load_expected_provenance(args.expected_provenance)
        provenance_mismatches = compare_frozen_provenance(frozen_provenance, expected_provenance)
    by_row, by_identity = load_t2_leg_maps(args.root)
    live_required_keys = live_overlay.potential_t2_required_keys(args.root, {leg.row_id: leg for leg in by_row.values()})
    path_lookup = overlay_research.build_path_lookup(frame)

    selected_frames: list[pd.DataFrame] = []
    candidate_reports: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    all_open_carry: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = []

    policy_names = {candidate["policy_name"] for candidate in candidates}
    recompute_source_selected: list[pd.DataFrame] = []
    for policy_name in policy_names:
        recompute_source_selected.append(continuation.first_passing_rows(frame, policies[policy_name]).copy())
    selected_for_recompute = pd.concat(recompute_source_selected, ignore_index=True) if recompute_source_selected else pd.DataFrame()
    recomputed_scores = recompute_clock_scores(frame, selected_for_recompute, {name: policies[name] for name in policy_names})

    for candidate in candidates:
        policy = policies[candidate["policy_name"]]
        selected, trades = make_candidate_trades(
            frame=frame,
            path_lookup=path_lookup,
            policy=policy,
            candidate=candidate,
            v1_portfolio=v1_portfolio,
            mae_floor=float(args.mae_floor),
        )
        selected["_candidate"] = candidate["name"]
        selected_frames.append(selected)
        traces = audit_no_lookahead(
            frame=frame,
            selected=selected,
            policy=policy,
            candidate_name=candidate["name"],
            recomputed_scores=recomputed_scores,
        )
        trace_rows.extend(traces)
        all_trades.extend(trades)
        carry = open_carry_rows(trades)
        all_open_carry.extend(carry)
        full_summary = summarize_trades(trades)
        candidate_reports.append(
            {
                "candidate": candidate["name"],
                "policy": asdict(policy),
                "exit": candidate["exit"],
                "full": full_summary,
                "train_aug10_aug21": split_summary(trades, "2026-08-10", "2026-08-21"),
                "rollover_aug24": exact_day_summary(trades, "2026-08-24"),
                "validation_aug25_aug28": split_summary(trades, "2026-08-25", "2026-08-28"),
                "test_aug31": exact_day_summary(trades, "2026-08-31"),
                "train_aug10_aug27": split_summary(trades, "2026-08-10", "2026-08-27"),
                "validation_aug28": exact_day_summary(trades, "2026-08-28"),
                "open_carry_rows": carry,
                "no_lookahead": {
                    "entry_rows": len(traces),
                    "fail_rows": sum(1 for row in traces if row.get("audit_status") != "PASS"),
                    "issue_counts": {
                        issue: sum(1 for row in traces if issue in str(row.get("issues") or "").split(";"))
                        for issue in sorted({issue for row in traces for issue in str(row.get("issues") or "").split(";") if issue})
                    },
                },
            }
        )

    selected_all = pd.concat(selected_frames, ignore_index=True) if selected_frames else pd.DataFrame()
    quote_rows = audit_quote_history(
        selected_rows=selected_all,
        phase_report=phase_report,
        by_row=by_row,
        by_identity=by_identity,
        risk_floor=float(args.risk_floor),
        live_required_keys=live_required_keys,
    )

    period_required_keys: set[str] = set()
    for raw in frame[["row_id", "clock_epoch"]].drop_duplicates("row_id").to_dict("records"):
        leg = leg_for_row(raw, by_row, by_identity)
        if leg is not None:
            period_required_keys.update({leg.signal_key, leg.execution_key})
    key_rows = cache_key_coverage(
        phase_report=phase_report,
        required_keys=period_required_keys,
        selected_rows=selected_all,
        by_row=by_row,
        by_identity=by_identity,
    )
    edge_report = edge_coverage(frame, selected_all)
    open_carry_state_report = open_carry_state_coverage(all_open_carry)
    open_carry_contract_report = {
        **OPEN_CARRY_CONTRACT,
        "accepted_for_this_audit": bool(args.accept_open_carry_contract),
        "state_field_coverage": open_carry_state_report,
    }
    deployment_blockers: list[str] = []
    if any(row.get("audit_status") != "PASS" for row in trace_rows):
        deployment_blockers.append("no_lookahead_trace_failures")
    if any(row.get("audit_status") != "PASS" for row in quote_rows):
        deployment_blockers.append("quote_history_live_equivalence_failures")
    if any(int(row.get("missing_candidate_entry_key_count") or 0) > 0 for row in key_rows):
        deployment_blockers.append("candidate_keys_missing_from_day_cache")
    if int(open_carry_state_report.get("rows_with_missing_required_fields") or 0) > 0:
        deployment_blockers.append("open_carry_state_fields_missing")
    if any(report["full"].get("open_at_period_end_count") for report in candidate_reports) and not args.accept_open_carry_contract:
        deployment_blockers.append("open_carry_contract_requires_explicit_acceptance")
    if int(edge_report.get("selected_edge_missing_rows") or 0) > 0:
        deployment_blockers.append("selected_entries_missing_edge_calculation")
    if frozen_provenance.get("missing_required_sources"):
        deployment_blockers.append("frozen_provenance_missing_required_sources")
    if args.expected_provenance and not expected_provenance:
        deployment_blockers.append("expected_provenance_unreadable")
    if provenance_mismatches:
        deployment_blockers.append("frozen_provenance_mismatch")

    write_csv(args.output_dir / "candidate_summary.csv", [flatten_summary(row["candidate"], row["full"]) for row in candidate_reports])
    write_csv(
        args.output_dir / "walk_forward_summary.csv",
        [
            {
                "candidate": report["candidate"],
                "split": split_name,
                **flatten_summary(report["candidate"], report[split_name]),
            }
            for report in candidate_reports
            for split_name in (
                "train_aug10_aug21",
                "rollover_aug24",
                "validation_aug25_aug28",
                "test_aug31",
                "train_aug10_aug27",
                "validation_aug28",
            )
        ],
    )
    write_csv(args.output_dir / "no_lookahead_trace.csv", trace_rows)
    write_csv(args.output_dir / "quote_history_live_equivalence.csv", quote_rows)
    write_csv(args.output_dir / "cache_key_coverage.csv", key_rows)
    write_csv(args.output_dir / "open_carry_rows.csv", all_open_carry)
    write_csv(args.output_dir / "trade_rows.csv", all_trades)
    write_json(args.output_dir / "frozen_provenance.json", frozen_provenance)
    write_json(args.output_dir / "open_carry_contract.json", open_carry_contract_report)

    report = {
        "schema": SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "root": str(args.root),
        "opportunity_frame": str(args.opportunity_frame),
        "phase_report": str(args.phase_report) if args.phase_report else None,
        "output_dir": str(args.output_dir),
        "candidate_count": len(candidates),
        "frame_rows": int(frame.shape[0]),
        "frame_unique_legs": int(frame["row_id"].nunique()) if "row_id" in frame.columns else None,
        "phase_feature_panel": (phase_report or {}).get("feature_panel"),
        "phase_outcome_frame": (phase_report or {}).get("outcome_frame"),
        "phase_input_stream": (phase_report or {}).get("input_stream"),
        "edge_coverage": edge_report,
        "cache_key_coverage": {
            "day_count": len(key_rows),
            "days_with_missing_required_keys": [
                row for row in key_rows if int(row.get("missing_required_key_count") or 0) > 0
            ],
            "days_with_missing_candidate_keys": [
                row for row in key_rows if int(row.get("missing_candidate_entry_key_count") or 0) > 0
            ],
        },
        "quote_history_live_equivalence": {
            "rows": len(quote_rows),
            "fail_rows": sum(1 for row in quote_rows if row.get("audit_status") != "PASS"),
            "mode_counts": selected_all["quote_history_mode"].value_counts(dropna=False).to_dict()
            if "quote_history_mode" in selected_all.columns and not selected_all.empty
            else {},
            "cross_session_ram60_window_rows": sum(
                1
                for row in quote_rows
                if epoch_date(row.get("frame_ram60_window_start_epoch")) is not None
                and row.get("entry_day") is not None
                and epoch_date(row.get("frame_ram60_window_start_epoch")) != row.get("entry_day")
            ),
        },
        "no_lookahead_trace": {
            "rows": len(trace_rows),
            "fail_rows": sum(1 for row in trace_rows if row.get("audit_status") != "PASS"),
        },
        "open_carry": {
            "rows": len(all_open_carry),
            "by_candidate": {
                report["candidate"]: int(report["full"].get("open_at_period_end_count") or 0) for report in candidate_reports
            },
            "contract": open_carry_contract_report,
        },
        "frozen_provenance": frozen_provenance,
        "expected_provenance": str(args.expected_provenance) if args.expected_provenance else None,
        "frozen_provenance_mismatches": provenance_mismatches,
        "candidate_reports": candidate_reports,
        "deployment_blockers": sorted(set(deployment_blockers)),
        "deployment_readiness": "PASS" if not deployment_blockers else "BLOCKED",
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    write_json(args.output_dir / "deployment_readiness_report.json", report)
    print(json.dumps({k: report[k] for k in ("deployment_readiness", "deployment_blockers", "elapsed_seconds")}, indent=2))
    return 0 if not deployment_blockers else 2


if __name__ == "__main__":
    raise SystemExit(main())
