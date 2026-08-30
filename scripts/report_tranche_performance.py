#!/usr/bin/env python3
"""Summarize OBVFUTPORT-v2 T1/T2/T3 performance from installed ledgers."""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


IST_OFFSET_SECONDS = 5 * 3600 + 30 * 60


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        return default


def parse_epoch(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def pct(net: float | None, margin: float | None) -> float | None:
    if net is None or margin is None or margin == 0:
        return None
    return 100.0 * net / margin


def load_margin_lookup(paths: list[str]) -> dict[str, dict[str, float]]:
    lookup: dict[str, dict[str, float]] = {}
    for raw_path in paths:
        if not raw_path:
            continue
        data = read_json(Path(raw_path), {})
        for entry in data.get("entries") or []:
            symbol = str(entry.get("symbol") or "")
            if not symbol:
                continue
            item = lookup.setdefault(symbol, {})
            for source_key, target_key in (("margin_long", "long"), ("margin_short", "short")):
                value = safe_float(entry.get(source_key))
                if value is not None:
                    item[target_key] = value
    return lookup


def lookup_margin(margins: dict[str, dict[str, float]], symbol: str, side: Any) -> float | None:
    item = margins.get(str(symbol)) or {}
    side_key = str(side or "").lower()
    if side_key in item:
        return item[side_key]
    return item.get("long") or item.get("short")


def start_epoch_from_date(trade_date: str) -> int:
    dt = datetime.fromisoformat(f"{trade_date}T00:00:00+05:30")
    return int(dt.timestamp())


def row_from_event(
    symbol: str,
    event: dict[str, Any],
    tranche: str,
    margins: dict[str, dict[str, float]],
) -> dict[str, Any]:
    margin = (
        safe_float(event.get("entry_margin_used_rupees"))
        or safe_float(event.get("margin_rupees"))
        or safe_float(event.get("current_one_lot_margin_rupees"))
        or lookup_margin(margins, symbol, event.get("side"))
    )
    net = safe_float(event.get("net_rupees"))
    gross = safe_float(event.get("gross_rupees"))
    charges = safe_float(event.get("charges_rupees"))
    return {
        "tranche": tranche,
        "symbol": symbol,
        "status": "closed",
        "side": event.get("side"),
        "signal_source": event.get("signal_source"),
        "position_id": event.get("position_id"),
        "entry_time": event.get("entry_time"),
        "exit_time": event.get("exit_time"),
        "exit_reason": event.get("exit_reason"),
        "entry_epoch": parse_epoch(event.get("entry_epoch")),
        "exit_epoch": parse_epoch(event.get("exit_epoch")),
        "gross_rupees": gross,
        "charges_rupees": charges,
        "net_rupees": net,
        "margin_rupees": margin,
        "net_pct_margin": pct(net, margin),
    }


def row_from_nested(
    *,
    symbol: str,
    parent: dict[str, Any],
    nested: dict[str, Any],
    tranche: str,
    open_mark: bool,
    margins: dict[str, dict[str, float]],
) -> dict[str, Any]:
    margin = (
        safe_float(nested.get("current_one_lot_margin_rupees"))
        or safe_float(parent.get("entry_margin_used_rupees"))
        or safe_float(parent.get("margin_rupees"))
        or lookup_margin(margins, symbol, nested.get("side") or parent.get("side"))
    )
    net = safe_float(nested.get("net_rupees"))
    gross = safe_float(nested.get("gross_rupees"))
    charges = safe_float(nested.get("charges_rupees"))
    return {
        "tranche": tranche,
        "symbol": symbol,
        "status": "open_mark" if open_mark else "closed",
        "side": nested.get("side") or parent.get("side"),
        "signal_source": parent.get("signal_source"),
        "position_id": parent.get("position_id"),
        "entry_time": nested.get("entry_time") or parent.get("entry_time"),
        "exit_time": nested.get("exit_time") or nested.get("mark_time") or parent.get("latest_time"),
        "exit_reason": nested.get("exit_reason") or nested.get("exit_source") or parent.get("source_exit_reason"),
        "entry_epoch": parse_epoch(nested.get("entry_epoch") or parent.get("entry_epoch")),
        "exit_epoch": parse_epoch(nested.get("exit_epoch") or parent.get("latest_epoch")),
        "gross_rupees": gross,
        "charges_rupees": charges,
        "net_rupees": net,
        "margin_rupees": margin,
        "net_pct_margin": pct(net, margin),
    }


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [row for row in rows if row.get("status") == "closed"]
    open_rows = [row for row in rows if row.get("status") == "open_mark"]
    net_values = [float(row["net_rupees"]) for row in rows if row.get("net_rupees") is not None]
    closed_net_values = [float(row["net_rupees"]) for row in closed if row.get("net_rupees") is not None]
    pct_values = [float(row["net_pct_margin"]) for row in rows if row.get("net_pct_margin") is not None]
    closed_pct_values = [float(row["net_pct_margin"]) for row in closed if row.get("net_pct_margin") is not None]
    wins = [row for row in closed if (safe_float(row.get("net_rupees")) or 0.0) > 0]
    losses = [row for row in closed if (safe_float(row.get("net_rupees")) or 0.0) <= 0]
    return {
        "rows": len(rows),
        "closed": len(closed),
        "open_mark": len(open_rows),
        "closed_wins": len(wins),
        "closed_losses": len(losses),
        "closed_success_rate_pct": (100.0 * len(wins) / len(closed)) if closed else None,
        "closed_net_rupees": sum(closed_net_values),
        "open_net_rupees": sum(float(row["net_rupees"]) for row in open_rows if row.get("net_rupees") is not None),
        "total_net_rupees": sum(net_values),
        "gross_rupees": sum(float(row["gross_rupees"]) for row in rows if row.get("gross_rupees") is not None),
        "charges_rupees": sum(float(row["charges_rupees"]) for row in rows if row.get("charges_rupees") is not None),
        "avg_net_pct_margin": statistics.mean(pct_values) if pct_values else None,
        "median_net_pct_margin": statistics.median(pct_values) if pct_values else None,
        "min_net_pct_margin": min(pct_values) if pct_values else None,
        "max_net_pct_margin": max(pct_values) if pct_values else None,
        "avg_closed_net_pct_margin": statistics.mean(closed_pct_values) if closed_pct_values else None,
        "median_closed_net_pct_margin": statistics.median(closed_pct_values) if closed_pct_values else None,
    }


def row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("tranche"),
        row.get("symbol"),
        row.get("position_id"),
        parse_epoch(row.get("entry_epoch")),
        parse_epoch(row.get("exit_epoch")),
        row.get("status"),
    )


def lifecycle_row_key(row: dict[str, Any]) -> tuple[Any, ...] | None:
    position_id = str(row.get("position_id") or "")
    entry_epoch = parse_epoch(row.get("entry_epoch"))
    tranche = str(row.get("tranche") or "")
    symbol = str(row.get("symbol") or "")
    if not position_id or entry_epoch is None or not tranche or not symbol:
        return None
    return (tranche, symbol, position_id, entry_epoch)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", default="/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
    parser.add_argument("--margin-manifest", action="append", default=[])
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-19")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    state_dir = Path(args.state_dir)
    default_margin_manifests = [
        str(state_dir.parent / "config" / "universe_v1_overlap50_aug10_flat_parity.json"),
        "/opt/cloud-deploy-candidates/stock-ws-pullback-reclaim-v0-1/config/universe_broad212.json",
    ]
    margins = load_margin_lookup([*default_margin_manifests, *args.margin_manifest])
    start_epoch = start_epoch_from_date(args.start_date)
    rows_by_tranche: dict[str, list[dict[str, Any]]] = {"T1": [], "T2": [], "T3": []}
    t2_seen: set[tuple[Any, ...]] = set()
    t3_seen: set[tuple[Any, ...]] = set()
    seen: set[tuple[Any, ...]] = set()
    lifecycle_rows: dict[tuple[Any, ...], dict[str, Any]] = {}

    def add(row: dict[str, Any]) -> None:
        key = row_key(row)
        if key in seen:
            return
        lifecycle_key = lifecycle_row_key(row)
        status = str(row.get("status") or "")
        if lifecycle_key is not None:
            existing = lifecycle_rows.get(lifecycle_key)
            existing_status = str(existing.get("status") or "") if existing else ""
            if existing is not None:
                if existing_status != "closed" and status == "closed":
                    existing_tranche = str(existing.get("tranche") or "")
                    try:
                        rows_by_tranche.setdefault(existing_tranche, []).remove(existing)
                    except ValueError:
                        pass
                    seen.add(key)
                    lifecycle_rows[lifecycle_key] = row
                    rows_by_tranche.setdefault(str(row.get("tranche")), []).append(row)
                return
        seen.add(key)
        if lifecycle_key is not None:
            lifecycle_rows[lifecycle_key] = row
        rows_by_tranche.setdefault(str(row.get("tranche")), []).append(row)

    for instrument_dir in sorted((state_dir / "instruments").glob("*")):
        if not instrument_dir.is_dir():
            continue
        symbol = instrument_dir.name
        ledger = instrument_dir / "ledger.jsonl"
        if ledger.exists():
            for raw in ledger.read_text().splitlines():
                if not raw.strip():
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                entry_epoch = parse_epoch(event.get("entry_epoch"))
                if entry_epoch is None or entry_epoch < start_epoch:
                    continue
                if event.get("event") == "paper_exit":
                    add(row_from_event(symbol, event, "T1", margins))
                    ttsl = event.get("two_lot_ttsl") if isinstance(event.get("two_lot_ttsl"), dict) else {}
                    tr2 = ttsl.get("tranche2") if isinstance(ttsl.get("tranche2"), dict) else {}
                    if tr2:
                        key = (
                            symbol,
                            event.get("position_id"),
                            parse_epoch(tr2.get("entry_epoch") or event.get("entry_epoch")),
                            parse_epoch(tr2.get("exit_epoch") or event.get("exit_epoch")),
                        )
                        if key not in t2_seen:
                            t2_seen.add(key)
                            add(
                                row_from_nested(
                                    symbol=symbol,
                                    parent=event,
                                    nested=tr2,
                                    tranche="T2",
                                    open_mark=False,
                                    margins=margins,
                                )
                            )
                    tr3 = event.get("tranche3") if isinstance(event.get("tranche3"), dict) else {}
                    tr3_entry_epoch = parse_epoch(tr3.get("entry_epoch"))
                    if tr3 and tr3_entry_epoch is not None and tr3.get("status") == "closed":
                        key = (symbol, event.get("position_id"), tr3_entry_epoch, tr3.get("exit_epoch"))
                        if key not in t3_seen:
                            t3_seen.add(key)
                            rows_by_tranche["T3"].append(
                                row_from_nested(
                                    symbol=symbol,
                                    parent=event,
                                    nested=tr3,
                                    tranche="T3",
                                    open_mark=False,
                                    margins=margins,
                                )
                            )
                elif event.get("event") == "tranche2_exit":
                    key = (symbol, event.get("position_id"), entry_epoch, parse_epoch(event.get("exit_epoch")))
                    if key not in t2_seen:
                        t2_seen.add(key)
                        add(row_from_event(symbol, event, "T2", margins))
                elif event.get("event") == "tranche3_exit":
                    key = (symbol, event.get("position_id"), entry_epoch, event.get("exit_epoch"))
                    if key not in t3_seen:
                        t3_seen.add(key)
                        add(row_from_event(symbol, event, "T3", margins))

        model = read_json(instrument_dir / "model_state.json", {})
        position = model.get("position") if isinstance(model, dict) else None
        if isinstance(position, dict):
            entry_epoch = parse_epoch(position.get("entry_epoch"))
            if entry_epoch is not None and entry_epoch >= start_epoch:
                t1_row = {
                    "tranche": "T1",
                    "symbol": symbol,
                    "status": "open_mark",
                    "side": position.get("side"),
                    "signal_source": position.get("signal_source"),
                    "position_id": position.get("position_id"),
                    "entry_time": position.get("entry_time"),
                    "exit_time": position.get("latest_time"),
                    "exit_reason": "open_mark_to_market_if_closed_now",
                    "entry_epoch": entry_epoch,
                    "exit_epoch": parse_epoch(position.get("latest_epoch")),
                    "gross_rupees": safe_float(position.get("gross_rupees_if_closed")),
                    "charges_rupees": safe_float(position.get("charges_rupees_if_closed")),
                    "net_rupees": safe_float(position.get("net_rupees_if_closed")),
                    "margin_rupees": safe_float(position.get("entry_margin_used_rupees")),
                    "net_pct_margin": pct(
                        safe_float(position.get("net_rupees_if_closed")),
                        safe_float(position.get("entry_margin_used_rupees"))
                        or lookup_margin(margins, symbol, position.get("side")),
                    ),
                }
                t1_row["margin_rupees"] = t1_row["margin_rupees"] or lookup_margin(
                    margins, symbol, position.get("side")
                )
                add(t1_row)
                ttsl = position.get("two_lot_ttsl") if isinstance(position.get("two_lot_ttsl"), dict) else {}
                tr2 = ttsl.get("tranche2") if isinstance(ttsl.get("tranche2"), dict) else {}
                if tr2:
                    key = (
                        symbol,
                        position.get("position_id"),
                        parse_epoch(tr2.get("entry_epoch") or position.get("entry_epoch")),
                        parse_epoch(tr2.get("exit_epoch") or position.get("latest_epoch")),
                    )
                    if key not in t2_seen:
                        t2_seen.add(key)
                        add(
                            row_from_nested(
                                symbol=symbol,
                                parent=position,
                                nested=tr2,
                                tranche="T2",
                                open_mark=tr2.get("status") != "closed",
                                margins=margins,
                            )
                        )
                tr3 = position.get("tranche3") if isinstance(position.get("tranche3"), dict) else {}
                tr3_entry_epoch = parse_epoch(tr3.get("entry_epoch"))
                if tr3 and tr3_entry_epoch is not None and tr3_entry_epoch >= start_epoch:
                    key = (symbol, position.get("position_id"), tr3_entry_epoch, tr3.get("exit_epoch"))
                    if key not in t3_seen:
                        t3_seen.add(key)
                        add(
                            row_from_nested(
                                symbol=symbol,
                                parent=position,
                                nested=tr3,
                                tranche="T3",
                                open_mark=tr3.get("status") != "closed",
                                margins=margins,
                            )
                        )

    by_symbol: dict[str, dict[str, dict[str, Any]]] = {}
    for tranche, rows in rows_by_tranche.items():
        by_symbol[tranche] = {}
        for row in rows:
            symbol = str(row.get("symbol"))
            item = by_symbol[tranche].setdefault(symbol, {"rows": 0, "closed": 0, "open_mark": 0, "net": 0.0})
            item["rows"] += 1
            item["closed"] += int(row.get("status") == "closed")
            item["open_mark"] += int(row.get("status") == "open_mark")
            item["net"] += safe_float(row.get("net_rupees")) or 0.0

    out = {
        "schema": "obvfutport_v2.tranche_performance_report.v3",
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "state_dir": str(state_dir),
        "basis": f"entry_epoch >= {args.start_date}T00:00:00+05:30; open rows marked to latest model_state price",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "instrument_dirs": len([p for p in (state_dir / "instruments").glob("*") if p.is_dir()]),
        "open_positions": sum(
            1
            for p in (state_dir / "instruments").glob("*/model_state.json")
            if isinstance(read_json(p, {}).get("position"), dict)
        ),
        "summary": {tranche: summarize_rows(rows) for tranche, rows in rows_by_tranche.items()},
        "by_symbol": by_symbol,
        "rows_by_tranche": rows_by_tranche,
    }

    output = Path(args.output) if args.output else state_dir / "reports" / f"v2_tranche_performance_{args.start_date.replace('-', '')}_to_{args.end_date.replace('-', '')}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(out, indent=2, sort_keys=True))
    print(json.dumps({k: out[k] for k in ["schema", "state_dir", "start_date", "end_date", "instrument_dirs", "open_positions"]} | {"summary": out["summary"], "output": str(output)}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
