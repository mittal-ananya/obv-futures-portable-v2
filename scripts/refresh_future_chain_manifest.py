#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{datetime.now(tz=IST).strftime('%Y%m%d%H%M%S%f')}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def resolve_path(config: dict[str, Any], primary: str, local: str | None = None) -> Path:
    primary_path = Path(str(config.get(primary) or ""))
    if primary_path.exists():
        return primary_path
    if local:
        local_path = Path(str(config.get(local) or ""))
        if local_path.exists():
            return local_path
    return primary_path


def load_holiday_dates(path: Path) -> set[date]:
    if not path.exists():
        return set()
    holidays: set[date] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        try:
            reader = csv.DictReader(handle)
            for row in reader:
                values = []
                for key in ("date", "holiday_date", "trading_holiday", "day"):
                    if row.get(key):
                        values.append(str(row[key]))
                values.extend(str(value) for value in row.values() if value)
                for value in values:
                    text = value.strip()[:10]
                    try:
                        holidays.add(date.fromisoformat(text))
                        break
                    except Exception:
                        continue
        except Exception:
            for line in sample.splitlines():
                text = line.strip().split(",", 1)[0][:10]
                try:
                    holidays.add(date.fromisoformat(text))
                except Exception:
                    continue
    return holidays


def previous_trading_day(day: date, holidays: set[date] | None = None) -> date:
    holiday_dates = holidays or set()
    current = day - timedelta(days=1)
    while current.weekday() >= 5 or current in holiday_dates:
        current -= timedelta(days=1)
    return current


def label_from_expiry(expiry: str, *, is_current: bool) -> str:
    try:
        month = date.fromisoformat(expiry).strftime("%B").lower()
    except Exception:
        month = "future"
    return f"{month}_main" if is_current else f"{month}_shadow"


def load_nfo_futures(master_path: Path) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with master_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("exchange") or "") != "NFO":
                continue
            if str(row.get("instrument_type") or "") != "FUT":
                continue
            if not str(row.get("tradingsymbol") or "").endswith("FUT"):
                continue
            expiry_text = str(row.get("expiry") or "")
            try:
                expiry = date.fromisoformat(expiry_text)
            except Exception:
                continue
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            key = f"NFO:{row['tradingsymbol']}"
            out[name].append(
                {
                    "instrument_key": key,
                    "tradingsymbol": row.get("tradingsymbol"),
                    "expiry_date": expiry.isoformat(),
                    "lot_size": int(float(row.get("lot_size") or 0)),
                    "tick_size": float(row.get("tick_size") or 0.0),
                    "instrument_token": row.get("instrument_token"),
                    "exchange_token": row.get("exchange_token"),
                }
            )
    return {name: sorted(rows, key=lambda item: item["expiry_date"]) for name, rows in out.items()}


def contract_baseline_start(
    *,
    active_rows: list[dict[str, Any]],
    all_rows: list[dict[str, Any]],
    active_index: int,
    config: dict[str, Any],
    holidays: set[date],
) -> str | None:
    if active_index > 0:
        return previous_trading_day(date.fromisoformat(str(active_rows[active_index - 1]["expiry_date"])), holidays).isoformat()
    current_expiry = date.fromisoformat(str(active_rows[0]["expiry_date"]))
    previous_contracts = [
        row for row in all_rows if date.fromisoformat(str(row["expiry_date"])) < current_expiry
    ]
    if previous_contracts:
        return previous_trading_day(date.fromisoformat(str(previous_contracts[-1]["expiry_date"])), holidays).isoformat()
    return str(config.get("new_symbol_baseline_start_date") or "2026-08-10")


def build_manifests(
    *,
    runtime_config: Path,
    nfo_master: Path,
    holiday_calendar: Path,
    as_of: date,
    contracts_per_symbol: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    config = read_json(runtime_config, {})
    universe_path = resolve_path(config, "hurst_universe_manifest_path", "hurst_universe_manifest_path_local")
    universe = read_json(universe_path, {})
    by_name = load_nfo_futures(nfo_master)
    holidays = load_holiday_dates(holiday_calendar)
    symbols: dict[str, Any] = {}
    extra_keys: list[str] = []
    unavailable: list[dict[str, Any]] = []
    inactive_lifecycle: list[dict[str, Any]] = []

    for entry in universe.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol") or "")
        manifest_fut_key = str(entry.get("fut_key") or "")
        rows_all = by_name.get(symbol, [])
        rows = [
            row
            for row in rows_all
            if date.fromisoformat(str(row["expiry_date"])) >= as_of
        ][:contracts_per_symbol]
        if not symbol or not rows:
            unavailable.append({"symbol": symbol, "reason": "no_future_contracts_in_nfo_master"})
            continue
        contracts: list[dict[str, Any]] = []
        for idx, row in enumerate(rows):
            lifecycle_start = contract_baseline_start(
                active_rows=rows,
                all_rows=rows_all,
                active_index=idx,
                config=config,
                holidays=holidays,
            )
            if idx > 0 and lifecycle_start and date.fromisoformat(lifecycle_start) > as_of:
                continue
            contract = dict(row)
            contract["label"] = label_from_expiry(str(row["expiry_date"]), is_current=idx == 0)
            contract["roll_date"] = previous_trading_day(
                date.fromisoformat(str(row["expiry_date"])),
                holidays,
            ).isoformat()
            if lifecycle_start:
                if idx == 0:
                    contract["baseline_start_date"] = lifecycle_start
                else:
                    contract["lifecycle_start_date"] = lifecycle_start
            contracts.append(contract)
            if idx == 0 and str(row["instrument_key"]) != manifest_fut_key:
                extra_keys.append(str(row["instrument_key"]))
            elif idx > 0:
                extra_keys.append(str(row["instrument_key"]))
        inactive_contracts = len(rows) - len(contracts)
        if inactive_contracts:
            inactive_lifecycle.append(
                {
                    "symbol": symbol,
                    "reason": "future_contracts_available_but_not_lifecycle_active",
                    "active_contracts": len(contracts),
                    "candidate_contracts": len(rows),
                    "as_of_date": as_of.isoformat(),
                }
            )
        base_key = str(contracts[0].get("instrument_key") or entry.get("fut_key") or "")
        symbols[symbol] = {
            "cash_key": entry.get("cash_key"),
            "base_fut_key": base_key,
            "contracts": contracts,
        }

    generated_at = datetime.now(tz=IST).isoformat()
    chain_manifest = {
        "schema": "obvfutport_v2.contract_chain_manifest.v1",
        "owner": "OBVFUTPORT_V2",
        "purpose": "Automatic lifecycle-gated futures chain for v2 target-stream and rollover. Includes current contract plus only next contracts whose lifecycle_start_date is active.",
        "runtime_config": str(runtime_config),
        "nfo_master": str(nfo_master),
        "holiday_calendar": str(holiday_calendar),
        "holiday_count": len(holidays),
        "universe_manifest": str(universe_path),
        "as_of_date": as_of.isoformat(),
        "max_contracts_scanned_per_symbol": contracts_per_symbol,
        "symbol_count": len(symbols),
        "unavailable": unavailable,
        "inactive_lifecycle_contracts": inactive_lifecycle,
        "generated_at_ist": generated_at,
        "symbols": symbols,
    }
    extra_manifest = {
        "schema": "stock_ws_pullback_reclaim.extra_target_keys.v1",
        "owner": "OBVFUTPORT_V2",
        "purpose": "Automatically generated v2 futures shadow keys for compact target-stream extraction.",
        "generated_at_ist": generated_at,
        "generated_from": str(nfo_master),
        "holiday_calendar": str(holiday_calendar),
        "holiday_count": len(holidays),
        "target_key_count": len(sorted(set(extra_keys))),
        "target_keys": sorted(set(extra_keys)),
        "unavailable_target_key_count": len(unavailable),
        "unavailable_target_keys": unavailable,
        "inactive_lifecycle_contract_count": len(inactive_lifecycle),
        "inactive_lifecycle_contracts": inactive_lifecycle,
    }
    return chain_manifest, extra_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh OBVFUTPORT-v2 futures contract chain and extra target-key manifests.")
    parser.add_argument("--runtime-config", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/config/runtime.json")
    parser.add_argument("--nfo-master", default="/opt/cloud-deploy-candidates/intraday-short-straddle-v1/config/runtime/zerodha_nfo_master.csv")
    parser.add_argument("--holiday-calendar", default="/opt/cloud-deploy-candidates/intraday-short-straddle-v1/strategy/data/runtime/nse_holidays.csv")
    parser.add_argument("--output-chain", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/config/obvfutport_v2_contract_chain_manifest.json")
    parser.add_argument("--output-extra-keys", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/config/obvfutport_v2_extra_target_keys.json")
    parser.add_argument("--as-of-date", default=datetime.now(tz=IST).date().isoformat())
    parser.add_argument("--contracts-per-symbol", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    chain, extra = build_manifests(
        runtime_config=Path(args.runtime_config),
        nfo_master=Path(args.nfo_master),
        holiday_calendar=Path(args.holiday_calendar),
        as_of=date.fromisoformat(args.as_of_date),
        contracts_per_symbol=max(2, int(args.contracts_per_symbol)),
    )
    atomic_write_json(Path(args.output_chain), chain)
    atomic_write_json(Path(args.output_extra_keys), extra)
    print(
        json.dumps(
            {
                "ok": True,
                "chain_symbol_count": chain["symbol_count"],
                "extra_target_key_count": extra["target_key_count"],
                "unavailable_count": len(chain["unavailable"]),
                "output_chain": args.output_chain,
                "output_extra_keys": args.output_extra_keys,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
