from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def add_strategy_paths(root: Path) -> None:
    v2_src = root / "src"
    v1_src = Path("/opt/cloud-deploy-candidates/obv-futures-portable-v1/src")
    for path in (v2_src, v1_src):
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


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
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            handle.write("\n")
    tmp.replace(path)


def as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        out = int(float(value))
    except (TypeError, ValueError):
        return None
    return out


def as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def position_key(row: dict[str, Any]) -> str:
    return str(row.get("position_id") or row.get("signal_id") or "").strip()


def lot_size_for(symbol: str, instrument_key: str, manifest: dict[str, Any]) -> int:
    symbol_payload = (manifest.get("symbols") or {}).get(symbol)
    if not isinstance(symbol_payload, dict):
        return 1
    for contract in symbol_payload.get("contracts") or []:
        if isinstance(contract, dict) and str(contract.get("instrument_key") or "") == instrument_key:
            return int(contract.get("lot_size") or 1)
    for contract in symbol_payload.get("contracts") or []:
        if isinstance(contract, dict) and contract.get("lot_size"):
            return int(contract.get("lot_size") or 1)
    return 1


def signed_points(side: str, entry: float, exit_: float) -> float:
    return exit_ - entry if str(side).lower() == "long" else entry - exit_


def build_t3_exit(
    *,
    symbol: str,
    t3_entry: dict[str, Any],
    parent_exit: dict[str, Any],
    manifest: dict[str, Any],
    v1_portfolio: Any,
) -> dict[str, Any]:
    side = str(t3_entry.get("side") or parent_exit.get("side") or "").lower()
    entry_fill = as_float(t3_entry.get("entry_fill_price") or t3_entry.get("entry_price"))
    entry_price = as_float(t3_entry.get("entry_price") or entry_fill)
    exit_fill = as_float(parent_exit.get("exit_fill_price") or parent_exit.get("exit_price"))
    exit_price = as_float(parent_exit.get("exit_price") or parent_exit.get("exit_ltp_price") or exit_fill)
    if not side or entry_fill is None or entry_price is None or exit_fill is None or exit_price is None:
        raise ValueError(f"missing T3 accounting fields for {symbol} {position_key(t3_entry)}")
    instrument_key = str(parent_exit.get("instrument_key") or t3_entry.get("instrument_key") or "")
    lot_size = lot_size_for(symbol, instrument_key, manifest)
    accounting = v1_portfolio.futures_trade_accounting(
        side=side,
        entry_fill_price=entry_fill,
        exit_fill_price=exit_fill,
        lot_size=lot_size,
        point_config=None,
    )
    exit_epoch = as_int(parent_exit.get("exit_epoch"))
    if exit_epoch is None:
        raise ValueError(f"missing parent exit_epoch for {symbol} {position_key(t3_entry)}")

    row = dict(t3_entry)
    row.update(
        {
            "event": "tranche3_exit",
            "event_epoch": exit_epoch,
            "status": "closed",
            "exit_reason": parent_exit.get("exit_reason"),
            "exit_source": "base_strategy",
            "base_exit_reason": parent_exit.get("exit_reason"),
            "base_exit_event_epoch": parent_exit.get("event_epoch") or parent_exit.get("exit_epoch"),
            "base_exit_position_id": parent_exit.get("position_id"),
            "exit_epoch": exit_epoch,
            "exit_time": parent_exit.get("exit_time"),
            "exit_row_time": parent_exit.get("exit_row_time"),
            "exit_price": exit_price,
            "exit_ltp_price": parent_exit.get("exit_ltp_price") or exit_price,
            "exit_fill_price": exit_fill,
            "instrument_key": instrument_key or t3_entry.get("instrument_key"),
            "contract_label": parent_exit.get("contract_label") or t3_entry.get("contract_label"),
            "accounting_model": parent_exit.get("accounting_model") or "bid_ask_proxy_slippage_zerodha_futures",
            "lot_size": lot_size,
            "quantity": accounting.get("quantity"),
            "model_gross_points": signed_points(side, entry_price, exit_price),
            "gross_points": accounting.get("gross_points"),
            "gross_rupees": accounting.get("gross_rupees"),
            "charges_rupees": accounting.get("charges_rupees"),
            "charges_points": accounting.get("charges_points"),
            "charge_breakdown": accounting.get("charge_breakdown"),
            "net_points": accounting.get("net_points"),
            "net_rupees": accounting.get("net_rupees"),
            "restated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "restatement_reason": "missing_explicit_tranche3_exit_from_parent_base_exit",
        }
    )
    tranche3 = dict(row.get("tranche3") or {})
    tranche3.update(
        {
            "status": "closed",
            "exit_reason": row["exit_reason"],
            "exit_source": row["exit_source"],
            "exit_epoch": row["exit_epoch"],
            "exit_time": row["exit_time"],
            "exit_price": row["exit_price"],
            "exit_ltp_price": row["exit_ltp_price"],
            "exit_fill_price": row["exit_fill_price"],
            "gross_points": row["gross_points"],
            "gross_rupees": row["gross_rupees"],
            "charges_rupees": row["charges_rupees"],
            "charges_points": row["charges_points"],
            "charge_breakdown": row["charge_breakdown"],
            "net_points": row["net_points"],
            "net_rupees": row["net_rupees"],
        }
    )
    row["tranche3"] = tranche3
    return row


def find_missing_t3(rows: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    t3_entries: dict[str, dict[str, Any]] = {}
    t3_exits: set[str] = set()
    parent_exits: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = position_key(row)
        if not key:
            continue
        event = row.get("event")
        if event == "tranche3_entry":
            t3_entries[key] = row
        elif event == "tranche3_exit":
            t3_exits.add(key)
        elif event == "paper_exit":
            parent_exits.setdefault(key, []).append(row)
    missing: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for key, entry in t3_entries.items():
        if key in t3_exits:
            continue
        entry_epoch = as_int(entry.get("entry_epoch")) or 0
        candidates = [
            row
            for row in parent_exits.get(key, [])
            if (as_int(row.get("exit_epoch")) or 0) >= entry_epoch
        ]
        if candidates:
            missing.append((entry, sorted(candidates, key=lambda row: as_int(row.get("exit_epoch")) or 0)[0]))
    return missing


def patch_parent_exit_epochs(rows: list[dict[str, Any]]) -> int:
    changed = 0
    for row in rows:
        if row.get("event") == "paper_exit" and row.get("exit_epoch") and not row.get("event_epoch"):
            row["event_epoch"] = row.get("exit_epoch")
            changed += 1
    return changed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--trade-date", default="2026-08-24")
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()

    add_strategy_paths(args.root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    state_dir = args.root / "state"
    manifest = read_json(args.root / "config" / "obvfutport_v2_contract_chain_manifest.json", {})
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_root = state_dir / "backups" / f"missing_t3_base_exit_repair_{stamp}"
    decision_path = state_dir / "decision_events" / f"decision_events_{args.trade_date}.jsonl"
    decision_rows = iter_jsonl(decision_path)
    decision_extra: list[dict[str, Any]] = []
    changed_symbols: list[dict[str, Any]] = []
    total_parent_epoch_repairs = 0

    for ledger_path in sorted((state_dir / "instruments").glob("*/ledger.jsonl")):
        symbol = ledger_path.parent.name
        rows = iter_jsonl(ledger_path)
        missing = find_missing_t3(rows)
        parent_epoch_repairs = patch_parent_exit_epochs(rows)
        new_rows: list[dict[str, Any]] = []
        for t3_entry, parent_exit in missing:
            new_rows.append(
                build_t3_exit(
                    symbol=symbol,
                    t3_entry=t3_entry,
                    parent_exit=parent_exit,
                    manifest=manifest,
                    v1_portfolio=v1_portfolio,
                )
            )
        if not new_rows and not parent_epoch_repairs:
            continue
        changed_symbols.append(
            {
                "symbol": symbol,
                "missing_t3_exit_rows_added": len(new_rows),
                "parent_paper_exit_event_epochs_repaired": parent_epoch_repairs,
                "position_ids": [position_key(row) for row in new_rows],
            }
        )
        total_parent_epoch_repairs += parent_epoch_repairs
        if args.install:
            dest = backup_root / "instruments" / symbol
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ledger_path, dest / "ledger.jsonl")
            rows.extend(new_rows)
            write_jsonl(ledger_path, rows)
            decision_extra.extend(new_rows)

    decision_epoch_repairs = 0
    if args.install and decision_path.exists():
        backup_root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(decision_path, backup_root / decision_path.name)
        decision_epoch_repairs = patch_parent_exit_epochs(decision_rows)
        existing_keys = {
            (row.get("event"), row.get("position_id") or row.get("signal_id"), row.get("entry_epoch"), row.get("exit_epoch"))
            for row in decision_rows
        }
        for row in decision_extra:
            key = (row.get("event"), row.get("position_id") or row.get("signal_id"), row.get("entry_epoch"), row.get("exit_epoch"))
            if key not in existing_keys:
                decision_rows.append(row)
                existing_keys.add(key)
        write_jsonl(decision_path, decision_rows)

    report = {
        "schema": "obvfutport_v2.missing_t3_base_exit_repair.v1",
        "trade_date": args.trade_date,
        "install": bool(args.install),
        "changed_symbol_count": len(changed_symbols),
        "missing_t3_exit_rows_added": sum(item["missing_t3_exit_rows_added"] for item in changed_symbols),
        "parent_paper_exit_event_epochs_repaired": total_parent_epoch_repairs,
        "decision_event_epochs_repaired": decision_epoch_repairs,
        "decision_t3_exit_rows_appended": len(decision_extra) if args.install else 0,
        "backup_root": str(backup_root) if args.install and changed_symbols else None,
        "changed_symbols": changed_symbols,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    report_path = state_dir / "reports" / f"missing_t3_base_exit_repair_{args.trade_date.replace('-', '')}_{stamp}.json"
    write_json(report_path, report)
    write_json(state_dir / "reports" / f"missing_t3_base_exit_repair_{args.trade_date.replace('-', '')}_latest.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
