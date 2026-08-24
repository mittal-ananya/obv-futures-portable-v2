from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            handle.write("\n")
    tmp.replace(path)


def as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def value(row: dict[str, Any], key: str) -> Any:
    position = row.get("position") if isinstance(row.get("position"), dict) else {}
    return position.get(key) or row.get(key)


def current_id(row: dict[str, Any]) -> tuple[str, str]:
    position = row.get("position") if isinstance(row.get("position"), dict) else {}
    position_id = str(position.get("position_id") or row.get("position_id") or "").strip()
    signal_id = str(position.get("signal_id") or row.get("signal_id") or "").strip()
    return position_id or signal_id, signal_id or position_id


def set_identity(row: dict[str, Any], position_id: str, signal_id: str) -> bool:
    changed = False
    for key, expected in (("position_id", position_id), ("signal_id", signal_id)):
        if row.get(key) != expected:
            row[key] = expected
            changed = True
    position = row.get("position") if isinstance(row.get("position"), dict) else None
    if position is not None:
        for key, expected in (("position_id", position_id), ("signal_id", signal_id)):
            if position.get(key) != expected:
                position[key] = expected
                changed = True
    return changed


def set_if_missing(row: dict[str, Any], key: str, expected: Any) -> bool:
    if expected is None:
        return False
    if row.get(key) in (None, ""):
        row[key] = expected
        return True
    return False


def selected_id_for_entry(rows: list[dict[str, Any]], row_index: int, *, side: str, entry_epoch: int | None, entry_time: str) -> tuple[str, str]:
    for prior in reversed(rows[:row_index]):
        if prior.get("event") != "paper_entry":
            continue
        prior_side = str(value(prior, "side") or "").lower()
        if prior_side and side and prior_side != side.lower():
            continue
        prior_epoch = as_int(value(prior, "entry_epoch"))
        prior_time = str(value(prior, "entry_time") or "")
        if entry_epoch is not None and prior_epoch == entry_epoch:
            position_id, signal_id = current_id(prior)
            if position_id or signal_id:
                return position_id or signal_id, signal_id or position_id
        if entry_time and prior_time == entry_time:
            position_id, signal_id = current_id(prior)
            if position_id or signal_id:
                return position_id or signal_id, signal_id or position_id
    return "", ""


def rolled_identity(*, strategy_id: str, symbol: str, side: str, entry_epoch: int, rollover_id: str, from_position_id: str) -> tuple[str, str]:
    digest = hashlib.sha1(
        "|".join([strategy_id, symbol, side, str(entry_epoch), rollover_id, from_position_id]).encode("utf-8")
    ).hexdigest()[:16]
    signal_id = f"{strategy_id}:{symbol}:{side}:{entry_epoch}:roll:{digest}"
    return f"{signal_id}:position", signal_id


def patch_ledger_rows(symbol: str, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, int]]:
    counts = {
        "lifecycle_exit_rows": 0,
        "rollover_entry_rows": 0,
        "rollover_summary_rows": 0,
        "post_roll_exit_rows": 0,
    }
    rollovers: dict[str, dict[str, Any]] = {}
    latest_strategy_id = "OBVFUTPORT_V2_PASSIVE"
    for row in rows:
        if row.get("strategy_id"):
            latest_strategy_id = str(row.get("strategy_id"))

    for index, row in enumerate(rows):
        event = str(row.get("event") or "")
        rollover_id = str(row.get("rollover_id") or "")
        side = str(value(row, "side") or "").lower()
        entry_epoch = as_int(value(row, "entry_epoch"))
        exit_epoch = as_int(row.get("exit_epoch"))
        entry_time = str(value(row, "entry_time") or "")
        if event == "paper_exit" and row.get("exit_reason") == "lifecycle_rollover" and rollover_id:
            old_position_id, old_signal_id = current_id(row)
            if not old_position_id or not old_signal_id:
                old_position_id, old_signal_id = selected_id_for_entry(
                    rows,
                    index,
                    side=side,
                    entry_epoch=entry_epoch,
                    entry_time=entry_time,
                )
            if old_position_id and old_signal_id:
                if set_identity(row, old_position_id, old_signal_id):
                    counts["lifecycle_exit_rows"] += 1
                if set_if_missing(row, "event_epoch", exit_epoch):
                    counts["lifecycle_exit_rows"] += 1
                rollovers.setdefault(rollover_id, {}).update(
                    {
                        "from_position_id": old_position_id,
                        "from_signal_id": old_signal_id,
                        "side": side,
                        "exit_epoch": exit_epoch,
                    }
                )
            continue

        if event == "paper_entry" and rollover_id:
            position = row.get("position") if isinstance(row.get("position"), dict) else {}
            side = str(position.get("side") or side or "").lower()
            entry_epoch = as_int(position.get("entry_epoch") or row.get("entry_epoch"))
            roll = rollovers.setdefault(rollover_id, {})
            from_position_id = str(roll.get("from_position_id") or "")
            if side and entry_epoch is not None and from_position_id:
                position_id, signal_id = rolled_identity(
                    strategy_id=latest_strategy_id,
                    symbol=symbol,
                    side=side,
                    entry_epoch=entry_epoch,
                    rollover_id=rollover_id,
                    from_position_id=from_position_id,
                )
                if set_identity(row, position_id, signal_id):
                    counts["rollover_entry_rows"] += 1
                for key in ("side", "source", "instrument_key", "contract_label", "signal_source", "signal_instrument_key", "signal_contract_label", "entry_epoch", "entry_time"):
                    if key in position:
                        counts["rollover_entry_rows"] += int(set_if_missing(row, key, position.get(key)))
                counts["rollover_entry_rows"] += int(set_if_missing(row, "event_epoch", entry_epoch))
                roll.update(
                    {
                        "to_position_id": position_id,
                        "to_signal_id": signal_id,
                        "to_entry_epoch": entry_epoch,
                        "to_entry_time": position.get("entry_time") or row.get("entry_time"),
                        "side": side,
                    }
                )
            continue

        if event == "paper_rollover" and rollover_id:
            roll = rollovers.get(rollover_id) or {}
            to_position_id = str(roll.get("to_position_id") or "")
            to_signal_id = str(roll.get("to_signal_id") or "")
            if to_position_id and to_signal_id:
                if set_identity(row, to_position_id, to_signal_id):
                    counts["rollover_summary_rows"] += 1
                for key in ("from_position_id", "from_signal_id", "to_position_id", "to_signal_id", "side"):
                    counts["rollover_summary_rows"] += int(set_if_missing(row, key, roll.get(key)))
                counts["rollover_summary_rows"] += int(set_if_missing(row, "event_epoch", roll.get("to_entry_epoch")))
                counts["rollover_summary_rows"] += int(set_if_missing(row, "entry_epoch", roll.get("to_entry_epoch")))
                counts["rollover_summary_rows"] += int(set_if_missing(row, "exit_epoch", roll.get("exit_epoch")))
            continue

        if event in {"paper_exit", "tranche2_exit", "tranche3_exit"} and not current_id(row)[0]:
            for roll in rollovers.values():
                if entry_epoch is not None and entry_epoch == roll.get("to_entry_epoch"):
                    to_position_id = str(roll.get("to_position_id") or "")
                    to_signal_id = str(roll.get("to_signal_id") or "")
                    if to_position_id and to_signal_id:
                        if set_identity(row, to_position_id, to_signal_id):
                            counts["post_roll_exit_rows"] += 1
                        counts["post_roll_exit_rows"] += int(set_if_missing(row, "event_epoch", exit_epoch))
                    break
    return rows, rollovers, counts


def repair_model_state(symbol: str, state: dict[str, Any], rollovers: dict[str, dict[str, Any]]) -> tuple[dict[str, Any], int]:
    position = state.get("position") if isinstance(state.get("position"), dict) else None
    if not position:
        return state, 0
    if position.get("position_id") and position.get("signal_id"):
        return state, 0
    rollover_id = str(position.get("source_rollover_id") or state.get("last_rollover_id") or "")
    roll = rollovers.get(rollover_id) if rollover_id else None
    if not roll:
        entry_epoch = as_int(position.get("entry_epoch"))
        for candidate in rollovers.values():
            if entry_epoch is not None and entry_epoch == candidate.get("to_entry_epoch"):
                roll = candidate
                break
    if not roll:
        return state, 0
    position_id = str(roll.get("to_position_id") or "")
    signal_id = str(roll.get("to_signal_id") or "")
    if not position_id or not signal_id:
        return state, 0
    changed = int(set_identity(position, position_id, signal_id))
    changed += int(set_if_missing(position, "entry_epoch", roll.get("to_entry_epoch")))
    changed += int(set_if_missing(position, "event_epoch", roll.get("to_entry_epoch")))
    changed += int(set_if_missing(position, "roll_from_position_id", roll.get("from_position_id")))
    changed += int(set_if_missing(position, "roll_from_signal_id", roll.get("from_signal_id")))
    state["position"] = position
    return state, changed


def patch_decision_events(path: Path, rollovers_by_symbol: dict[str, dict[str, dict[str, Any]]]) -> tuple[int, int]:
    rows = iter_jsonl(path)
    changed = 0
    for row in rows:
        symbol = str(row.get("symbol") or "")
        rollovers = rollovers_by_symbol.get(symbol) or {}
        if not rollovers:
            continue
        event = str(row.get("event") or "")
        rollover_id = str(row.get("rollover_id") or "")
        entry_epoch = as_int(value(row, "entry_epoch"))
        exit_epoch = as_int(row.get("exit_epoch"))
        if rollover_id and rollover_id in rollovers:
            roll = rollovers[rollover_id]
            if event == "paper_exit" and row.get("exit_reason") == "lifecycle_rollover":
                changed += int(set_identity(row, str(roll.get("from_position_id") or ""), str(roll.get("from_signal_id") or "")))
                changed += int(set_if_missing(row, "event_epoch", row.get("exit_epoch")))
            elif event == "paper_entry":
                changed += int(set_identity(row, str(roll.get("to_position_id") or ""), str(roll.get("to_signal_id") or "")))
                changed += int(set_if_missing(row, "event_epoch", roll.get("to_entry_epoch")))
            elif event == "paper_rollover":
                changed += int(set_identity(row, str(roll.get("to_position_id") or ""), str(roll.get("to_signal_id") or "")))
                for key in ("from_position_id", "from_signal_id", "to_position_id", "to_signal_id", "side"):
                    changed += int(set_if_missing(row, key, roll.get(key)))
                changed += int(set_if_missing(row, "event_epoch", roll.get("to_entry_epoch")))
                changed += int(set_if_missing(row, "entry_epoch", roll.get("to_entry_epoch")))
                changed += int(set_if_missing(row, "exit_epoch", roll.get("exit_epoch")))
        elif event in {"paper_exit", "tranche2_exit", "tranche3_exit"} and not current_id(row)[0]:
            for roll in rollovers.values():
                if entry_epoch is not None and entry_epoch == roll.get("to_entry_epoch"):
                    changed += int(set_identity(row, str(roll.get("to_position_id") or ""), str(roll.get("to_signal_id") or "")))
                    changed += int(set_if_missing(row, "event_epoch", exit_epoch))
                    break
    if changed:
        write_jsonl(path, rows)
    return len(rows), changed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=Path("state"))
    parser.add_argument("--trade-date", default="2026-08-24")
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()

    state_dir = args.state_dir
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_root = state_dir / "backups" / f"rollover_identity_repair_{stamp}"
    rollovers_by_symbol: dict[str, dict[str, dict[str, Any]]] = {}
    symbol_reports: list[dict[str, Any]] = []

    for ledger_path in sorted((state_dir / "instruments").glob("*/ledger.jsonl")):
        symbol = ledger_path.parent.name
        original_rows = iter_jsonl(ledger_path)
        rows = json.loads(json.dumps(original_rows))
        patched_rows, rollovers, counts = patch_ledger_rows(symbol, rows)
        model_path = ledger_path.parent / "model_state.json"
        model_state = read_json(model_path, {})
        patched_model, model_changes = repair_model_state(symbol, json.loads(json.dumps(model_state)), rollovers)
        row_changes = sum(counts.values())
        if row_changes or model_changes:
            symbol_reports.append(
                {
                    "symbol": symbol,
                    "ledger_changes": counts,
                    "model_state_changes": model_changes,
                    "rollover_ids": sorted(rollovers),
                }
            )
            rollovers_by_symbol[symbol] = rollovers
            if args.install:
                dest = backup_root / "instruments" / symbol
                dest.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ledger_path, dest / "ledger.jsonl")
                if model_path.exists():
                    shutil.copy2(model_path, dest / "model_state.json")
                write_jsonl(ledger_path, patched_rows)
                write_json(model_path, patched_model)

    decision_report = {"rows": 0, "changes": 0}
    decision_path = state_dir / "decision_events" / f"decision_events_{args.trade_date}.jsonl"
    if rollovers_by_symbol and decision_path.exists() and args.install:
        backup_root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(decision_path, backup_root / decision_path.name)
        rows, changes = patch_decision_events(decision_path, rollovers_by_symbol)
        decision_report = {"rows": rows, "changes": changes}

    report = {
        "schema": "obvfutport_v2.rollover_identity_repair.v1",
        "trade_date": args.trade_date,
        "install": bool(args.install),
        "state_dir": str(state_dir),
        "backup_root": str(backup_root) if args.install and symbol_reports else None,
        "symbols_changed": len(symbol_reports),
        "symbol_reports": symbol_reports,
        "decision_events": decision_report,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    report_path = state_dir / "reports" / f"rollover_identity_repair_{args.trade_date.replace('-', '')}_{stamp}.json"
    write_json(report_path, report)
    latest_path = state_dir / "reports" / f"rollover_identity_repair_{args.trade_date.replace('-', '')}_latest.json"
    write_json(latest_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
