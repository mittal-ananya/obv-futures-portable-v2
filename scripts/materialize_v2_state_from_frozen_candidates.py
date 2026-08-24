#!/usr/bin/env python3
"""Build an installable OBVFUTPORT-v2 state from frozen selected candidates.

This path is intentionally not a strategy replay. It materializes the exact
selected candidate rows already proven by the atomic recalibration runner, so
the dashboard and Matrix cannot drift from the frozen candidate report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


IST_OFFSET_SECONDS = 5 * 3600 + 30 * 60


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""), encoding="utf-8")
    tmp.replace(path)


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    rows.append(item)
    except OSError:
        return []
    return rows


def copytree_replace(source: Path, target: Path) -> int:
    if not source.exists():
        return 0
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target)
    return 1


def copy_file_replace(source: Path, target: Path) -> int:
    if not source.exists():
        return 0
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return 1


def as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def as_epoch(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def normalize_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    if status in {"open", "open_mark", "marked_open", "mark", "live_open"}:
        return "open"
    if status == "closed":
        return "closed"
    return "open" if status else "closed"


def row_is_closed(row: dict[str, Any]) -> bool:
    return normalize_status(row.get("status")) == "closed"


def fmt_epoch(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone().isoformat()


def trade_date_from_epoch(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch + IST_OFFSET_SECONDS, tz=timezone.utc).date().isoformat()


def trade_dates(start_date: str, end_date: str) -> list[str]:
    current = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    out: list[str] = []
    while current <= final:
        if current.weekday() < 5:
            out.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return out


def candidate_dirs(raw_roots: list[str]) -> list[Path]:
    roots: list[Path] = []
    for raw in raw_roots:
        path = Path(raw)
        if (path / "frozen_candidates").is_dir():
            path = path / "frozen_candidates"
        if path.is_dir():
            roots.append(path)
    return roots


def load_candidate_artifacts(roots: list[Path], symbols: set[str] | None = None) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    for root in roots:
        for path in sorted(root.glob("*.json")):
            payload = read_json(path, {})
            symbol = str(payload.get("symbol") or path.stem).upper()
            if symbols is not None and symbol not in symbols:
                continue
            if payload.get("freeze_status") != "frozen":
                continue
            best = payload.get("best_candidate")
            if not isinstance(best, dict) or not isinstance(best.get("rows"), list):
                continue
            artifacts[symbol] = payload
    return artifacts


def load_override_symbols(path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json(path, {})
    records = payload.get("symbols") if isinstance(payload, dict) else {}
    return {str(k).upper(): v for k, v in records.items() if isinstance(v, dict)} if isinstance(records, dict) else {}


def selected_rows(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    best = artifact.get("best_candidate") if isinstance(artifact.get("best_candidate"), dict) else {}
    rows = best.get("rows") if isinstance(best.get("rows"), list) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def stable_id(symbol: str, side: Any, signal_epoch: Any, candidate_id: Any, joint_label: str) -> str:
    raw = f"{symbol}|{side}|{signal_epoch}|{candidate_id}|{joint_label}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"OBVFUTPORT_V2_SELECTED:{symbol}:{str(side or '').lower()}:{signal_epoch}:{digest}"


def event_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("event_epoch")
        or row.get("exit_epoch")
        or row.get("entry_epoch")
        or row.get("signal_epoch")
        or 0,
        row.get("event") or "",
        row.get("symbol") or "",
        row.get("position_id") or "",
    )


def adaptive_event_fields(adaptive: dict[str, Any]) -> dict[str, Any]:
    metrics = adaptive.get("metrics") if isinstance(adaptive.get("metrics"), dict) else {}
    deltas = metrics.get("deltas") if isinstance(metrics.get("deltas"), dict) else {}
    return {
        "adaptive_calibration": adaptive,
        "adaptive_combo_label": adaptive.get("combo_label"),
        "adaptive_exit_combo_label": adaptive.get("exit_combo_label"),
        "adaptive_tranche3_combo_label": adaptive.get("tranche3_combo_label"),
        "adaptive_tier": adaptive.get("tier"),
        "adaptive_tags": adaptive.get("tags"),
        "adaptive_net_delta_rupees": deltas.get("net_delta_rupees"),
        "adaptive_success_rate_delta_pct": deltas.get("success_rate_delta_pct"),
        "adaptive_worst_loss_pct_delta": deltas.get("worst_loss_pct_delta"),
        "adaptive_drawdown_delta_rupees": deltas.get("drawdown_delta_rupees"),
    }


def tranche2_state(row: dict[str, Any], parent: dict[str, Any]) -> dict[str, Any]:
    exit_epoch = as_epoch(row.get("t2_exit_epoch"))
    if exit_epoch is None and row_is_closed(row):
        exit_epoch = as_epoch(row.get("exit_epoch"))
    closed = exit_epoch is not None
    return {
        "status": "closed" if closed else "open",
        "symbol": parent.get("symbol"),
        "side": parent.get("side"),
        "position_id": parent.get("position_id"),
        "signal_id": parent.get("signal_id"),
        "signal_epoch": parent.get("signal_epoch"),
        "signal_time": parent.get("signal_time"),
        "signal_source": parent.get("signal_source"),
        "signal_instrument_key": parent.get("signal_instrument_key"),
        "instrument_key": parent.get("instrument_key"),
        "entry_epoch": parent.get("entry_epoch"),
        "entry_time": parent.get("entry_time"),
        "entry_fill_price": parent.get("entry_fill_price"),
        "entry_price": parent.get("entry_price"),
        "exit_epoch": exit_epoch,
        "exit_time": (row.get("t2_exit_time") or row.get("exit_time") or fmt_epoch(exit_epoch)) if closed else None,
        "exit_fill_price": (row.get("t2_exit_fill_price") or row.get("exit_fill_price")) if closed else None,
        "exit_price": (row.get("t2_exit_ltp_price") or row.get("exit_ltp_price")) if closed else None,
        "exit_reason": (row.get("t2_exit_source") or row.get("exit_reason")) if closed else None,
        "net_rupees": row.get("t2_net_rupees"),
        "margin_rupees": row.get("one_lot_margin_rupees") or row.get("two_lot_margin_rupees"),
        "current_one_lot_margin_rupees": row.get("one_lot_margin_rupees"),
    }


def tranche3_state(row: dict[str, Any], parent: dict[str, Any]) -> dict[str, Any]:
    entry_epoch = as_epoch(row.get("t3_entry_epoch"))
    exit_epoch = as_epoch(row.get("t3_exit_epoch"))
    return {
        "status": "closed" if exit_epoch is not None else "open",
        "symbol": parent.get("symbol"),
        "side": parent.get("side"),
        "position_id": parent.get("position_id"),
        "signal_id": parent.get("signal_id"),
        "signal_epoch": parent.get("signal_epoch"),
        "signal_time": parent.get("signal_time"),
        "signal_source": parent.get("signal_source"),
        "signal_instrument_key": parent.get("signal_instrument_key"),
        "instrument_key": parent.get("instrument_key"),
        "entry_epoch": entry_epoch,
        "entry_time": row.get("t3_entry_time") or fmt_epoch(entry_epoch),
        "entry_fill_price": row.get("t3_entry_fill_price"),
        "entry_price": row.get("t3_entry_ltp_price") or row.get("t3_entry_fill_price"),
        "exit_epoch": exit_epoch,
        "exit_time": row.get("t3_exit_time") or fmt_epoch(exit_epoch),
        "exit_fill_price": row.get("t3_exit_fill_price"),
        "exit_price": row.get("t3_exit_ltp_price") or row.get("t3_exit_fill_price"),
        "exit_reason": row.get("t3_exit_reason"),
        "net_rupees": row.get("t3_net_rupees"),
        "margin_rupees": row.get("one_lot_margin_rupees"),
        "current_one_lot_margin_rupees": row.get("one_lot_margin_rupees"),
        "tranche3_entry_mode": row.get("t3_entry_mode"),
        "tranche3_combo_label": row.get("tranche3_combo_label"),
    }


def base_position(row: dict[str, Any], adaptive: dict[str, Any], joint_label: str) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").upper()
    side = str(row.get("side") or "").lower()
    signal_epoch = as_epoch(row.get("signal_epoch"))
    pid = stable_id(symbol, side, signal_epoch, row.get("candidate_id"), joint_label)
    signal_id = pid.replace(":position", "")
    position = {
        **row,
        "symbol": symbol,
        "side": side,
        "position_id": pid,
        "signal_id": signal_id,
        "strategy_id": "OBVFUTPORT_V2_PASSIVE",
        "model_version": "joint_adaptive_v2_selected_candidate_materialized",
        "architecture_version": "v2_selected_candidate_materialized",
        "signal_epoch": signal_epoch,
        "signal_time": row.get("signal_time") or fmt_epoch(signal_epoch),
        "signal_instrument_key": row.get("signal_key"),
        "instrument_key": row.get("execution_key"),
        "entry_epoch": as_epoch(row.get("entry_epoch")),
        "entry_time": row.get("entry_time") or fmt_epoch(as_epoch(row.get("entry_epoch"))),
        "entry_price": row.get("entry_ltp_price") or row.get("entry_fill_price"),
        "entry_fill_price": row.get("entry_fill_price"),
        "exit_epoch": as_epoch(row.get("exit_epoch")),
        "exit_time": row.get("exit_time") or fmt_epoch(as_epoch(row.get("exit_epoch"))),
        "exit_price": row.get("exit_ltp_price") or row.get("exit_fill_price"),
        "exit_fill_price": row.get("exit_fill_price"),
        "net_rupees": row.get("t1_net_rupees"),
        "gross_rupees": row.get("t1_gross_rupees"),
        "margin_rupees": row.get("one_lot_margin_rupees"),
        "current_one_lot_margin_rupees": row.get("one_lot_margin_rupees"),
        "entry_margin_used_rupees": row.get("one_lot_margin_rupees"),
        "contract_label": row.get("contract_label") or "august_main",
        "source": row.get("module"),
        "status": normalize_status(row.get("status")),
        **adaptive_event_fields(adaptive),
    }
    t2 = tranche2_state(row, position)
    position["two_lot_ttsl"] = {"tranche2": t2}
    if truthy(row.get("t3_entered")):
        position["tranche3"] = tranche3_state(row, position)
    return position


def events_for_row(row: dict[str, Any], adaptive: dict[str, Any], joint_label: str) -> list[dict[str, Any]]:
    position = base_position(row, adaptive, joint_label)
    common = {
        "schema": "obvfutport_v2.selected_candidate_event.v1",
        "strategy_id": "OBVFUTPORT_V2_PASSIVE",
        "source_runtime": "selected_candidate_materializer",
        "symbol": position["symbol"],
        "position_id": position["position_id"],
        "signal_id": position["signal_id"],
        "created_at_ist": datetime.now().astimezone().isoformat(),
        **adaptive_event_fields(adaptive),
    }
    events: list[dict[str, Any]] = [
        {
            **common,
            "event": "paper_entry",
            "event_epoch": position.get("entry_epoch"),
            "entry_epoch": position.get("entry_epoch"),
            "entry_time": position.get("entry_time"),
            "position": position,
        }
    ]
    if truthy(row.get("t3_entered")):
        t3 = position.get("tranche3") if isinstance(position.get("tranche3"), dict) else {}
        if as_epoch(t3.get("entry_epoch")) is not None:
            events.append({**common, **t3, "event": "tranche3_entry", "event_epoch": t3.get("entry_epoch")})
        if as_epoch(t3.get("exit_epoch")) is not None:
            events.append({**common, **t3, "event": "tranche3_exit", "event_epoch": t3.get("exit_epoch")})
    t2 = position.get("two_lot_ttsl", {}).get("tranche2") if isinstance(position.get("two_lot_ttsl"), dict) else {}
    # Emit a separate T2 exit only when it exits before the base T1 exit. Matrix
    # otherwise uses the base exit as the selected-leg exit, just like Compass.
    if isinstance(t2, dict) and as_epoch(t2.get("exit_epoch")) is not None and as_epoch(t2.get("exit_epoch")) != as_epoch(position.get("exit_epoch")):
        events.append({**common, **t2, "event": "tranche2_exit", "event_epoch": t2.get("exit_epoch")})
    if as_epoch(position.get("exit_epoch")) is not None and row_is_closed(row):
        events.append(
            {
                **common,
                **position,
                "event": "paper_exit",
                "event_epoch": position.get("exit_epoch"),
                "position": position,
            }
        )
    return sorted(events, key=event_sort_key)


def materialize_symbol(symbol: str, artifact: dict[str, Any], adaptive: dict[str, Any], output_state: Path) -> dict[str, Any]:
    rows = selected_rows(artifact)
    joint_label = str((artifact.get("best_candidate") or {}).get("joint_label") or adaptive.get("joint_label") or "")
    events: list[dict[str, Any]] = []
    open_position: dict[str, Any] | None = None
    last_closed: dict[str, Any] | None = None
    for row in rows:
        row_events = events_for_row(row, adaptive, joint_label)
        events.extend(row_events)
        position = row_events[0].get("position") if row_events and isinstance(row_events[0].get("position"), dict) else None
        if isinstance(position, dict):
            if normalize_status(position.get("status")) != "closed":
                open_position = position
            else:
                last_closed = next((event for event in reversed(row_events) if event.get("event") == "paper_exit"), last_closed)
    events = sorted(events, key=event_sort_key)
    symbol_dir = output_state / "instruments" / symbol
    write_jsonl(symbol_dir / "ledger.jsonl", events)
    model_state = {
        "schema": "obvfutport_v2.selected_candidate_model_state.v1",
        "symbol": symbol,
        "position": open_position or None,
        "last_closed_trade": last_closed,
        "selected_candidate_materialized": True,
        "selected_candidate_rows": len(rows),
        "selected_candidate_events": len(events),
        "updated_at_ist": datetime.now().astimezone().isoformat(),
    }
    atomic_write_json(symbol_dir / "model_state.json", model_state)
    return {"symbol": symbol, "rows": len(rows), "events": len(events), "open": bool(open_position)}


def copy_baseline_symbol(symbol: str, baseline_state: Path, output_state: Path) -> bool:
    source = baseline_state / "instruments" / symbol
    target = output_state / "instruments" / symbol
    return bool(copytree_replace(source, target))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-root", action="append", required=True)
    parser.add_argument("--override", required=True)
    parser.add_argument("--output-state", required=True)
    parser.add_argument("--baseline-state", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--quarantined-symbols", default="IOC,MAXHEALTH,WAAREEENER")
    parser.add_argument("--require-symbol-count", type=int, default=212)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    output_state = Path(args.output_state)
    if output_state.exists():
        if not args.force:
            raise SystemExit(f"output state exists; pass --force to replace: {output_state}")
        shutil.rmtree(output_state)
    output_state.mkdir(parents=True, exist_ok=True)

    roots = candidate_dirs(args.candidate_root)
    requested = {item.strip().upper() for item in args.symbols.split(",") if item.strip()} or None
    quarantined = {item.strip().upper() for item in args.quarantined_symbols.split(",") if item.strip()}
    overrides = load_override_symbols(Path(args.override))
    universe_symbols = sorted(requested or set(overrides))
    artifacts = load_candidate_artifacts(roots, set(universe_symbols))
    baseline_state = Path(args.baseline_state)

    copied_baseline: list[str] = []
    materialized: list[dict[str, Any]] = []
    missing: list[str] = []
    for symbol in universe_symbols:
        if symbol in quarantined:
            if copy_baseline_symbol(symbol, baseline_state, output_state):
                copied_baseline.append(symbol)
            else:
                missing.append(symbol)
            continue
        artifact = artifacts.get(symbol)
        adaptive = overrides.get(symbol, {})
        if not artifact or not adaptive:
            if copy_baseline_symbol(symbol, baseline_state, output_state):
                copied_baseline.append(symbol)
            else:
                missing.append(symbol)
            continue
        materialized.append(materialize_symbol(symbol, artifact, adaptive, output_state))

    dates = trade_dates(args.start_date, args.end_date)
    events_by_date: dict[str, list[dict[str, Any]]] = {trade_date: [] for trade_date in dates}
    for ledger in sorted((output_state / "instruments").glob("*/ledger.jsonl")):
        for event in iter_jsonl(ledger):
            event_date = trade_date_from_epoch(as_epoch(event.get("event_epoch") or event.get("entry_epoch") or event.get("exit_epoch")))
            if event_date in events_by_date:
                events_by_date[event_date].append(event)
    for trade_date, events in events_by_date.items():
        if events:
            write_jsonl(output_state / "decision_events" / f"decision_events_{trade_date}.jsonl", sorted(events, key=event_sort_key))

    copytree_replace(baseline_state / "bootstrap_state", output_state / "bootstrap_state")
    for name in ("bootstrap_status.json", "status.json", "target_stream_consumer_pointer.json"):
        copy_file_replace(baseline_state / name, output_state / name)
    copy_file_replace(Path(args.override), output_state / "adaptive_calibration" / "v2_symbol_overrides_latest.json")

    symbol_count = len([path for path in (output_state / "instruments").glob("*") if path.is_dir()])
    ok = symbol_count >= int(args.require_symbol_count) and not missing
    archive_report = {
        "schema": "obvfutport_v2.selected_candidate_materialized_archive_report.v1",
        "ok": ok,
        "partial": not ok,
        "source": "selected_frozen_candidate_materialized",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "symbols": symbol_count,
        "materialized_symbols": len(materialized),
        "baseline_copied_symbols": copied_baseline,
        "missing_symbols": missing,
        "date_reports": [
            {
                "trade_date": trade_date,
                "trade_state_events": len(events_by_date.get(trade_date, [])),
                "source": "selected_frozen_candidate_materialized",
            }
            for trade_date in dates
        ],
        "updated_at_ist": datetime.now().astimezone().isoformat(),
    }
    atomic_write_json(output_state / "archive_replay_report.json", archive_report)
    report = {
        "schema": "obvfutport_v2.selected_candidate_materializer.v1",
        "ok": ok,
        "output_state": str(output_state),
        "candidate_roots": [str(path) for path in roots],
        "override": str(Path(args.override)),
        "baseline_state": str(baseline_state),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "symbols": symbol_count,
        "materialized": materialized,
        "materialized_count": len(materialized),
        "copied_baseline_count": len(copied_baseline),
        "copied_baseline_symbols": copied_baseline,
        "missing_symbols": missing,
        "decision_events_total": sum(len(events) for events in events_by_date.values()),
        "updated_epoch": time.time(),
    }
    atomic_write_json(output_state / "selected_candidate_materializer_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
