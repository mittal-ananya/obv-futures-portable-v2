#!/usr/bin/env python3
"""Restate v2Matrix historical display prices from the compact target stream.

This updates only v2Matrix display fields. Futures execution fields and v2
strategy ledgers are intentionally left untouched.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import gzip
import json
import os
import pickle
import shutil
import time
from bisect import bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any


PROXY = "historical_research_execution_fill_proxy"
DISPLAY_SOURCE = "v2matrix_historical_target_stream_restatement"
DISPLAY_CLASS = "cash_underlying_or_configured_signal_source"


def parse_time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value)


def epoch(value: str) -> float:
    return parse_time(value).timestamp()


def date_key(value: str) -> str:
    return value[:10]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def display_key(symbol: str) -> str:
    index_map = {
        "NIFTY": "NSE:NIFTY 50",
        "BANKNIFTY": "NSE:NIFTY BANK",
        "FINNIFTY": "NSE:NIFTY FIN SERVICE",
        "MIDCPNIFTY": "NSE:NIFTY MIDCAP SELECT",
    }
    return index_map.get(symbol, f"NSE:{symbol}")


def row_price(row: dict[str, Any]) -> float | None:
    for key in ("price", "last_price", "ltp", "close"):
        val = row.get(key)
        if val is None:
            continue
        try:
            f = float(val)
        except Exception:
            continue
        if f > 0:
            return f
    return None


def index_row_price(row: Any) -> float | None:
    if isinstance(row, dict):
        return row_price(row)
    if isinstance(row, (tuple, list)) and len(row) >= 3:
        try:
            val = float(row[2])
        except Exception:
            return None
        return val if val > 0 else None
    return None


def index_row_start(row: Any) -> float | None:
    try:
        if isinstance(row, dict):
            return float(row.get("exchange_epoch") or row.get("event_epoch") or row.get("start_epoch"))
        if isinstance(row, (tuple, list)) and row:
            return float(row[0])
    except Exception:
        return None
    return None


def index_row_end(row: Any) -> float | None:
    try:
        if isinstance(row, dict):
            return float(row.get("end_epoch") or row.get("exchange_epoch") or row.get("event_epoch"))
        if isinstance(row, (tuple, list)) and len(row) > 1:
            return float(row[1])
    except Exception:
        return None
    return None


def load_quote_index_day(
    day: str,
    by_key: dict[str, list[float]],
    quote_index_root: Path,
    max_stale_seconds: float,
) -> dict[tuple[str, float], dict[str, Any]]:
    candidates = sorted(
        quote_index_root.glob(f"{day}_*.quote_index.pkl.gz"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    best: dict[str, Any] | None = None
    best_path: Path | None = None
    best_coverage = -1
    for path in candidates:
        try:
            with gzip.open(path, "rb") as f:
                obj = pickle.load(f)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        coverage = sum(1 for key in by_key if key in obj)
        if coverage > best_coverage:
            best = obj
            best_path = path
            best_coverage = coverage
        if coverage == len(by_key):
            break
    if best is None:
        return {}

    assigned: dict[tuple[str, float], dict[str, Any]] = {}
    for key, queries in by_key.items():
        rows = best.get(key) or []
        starts = [index_row_start(r) for r in rows]
        clean_rows = [(s, r) for s, r in zip(starts, rows) if s is not None]
        clean_rows.sort(key=lambda x: x[0])
        start_vals = [s for s, _ in clean_rows]
        for q in queries:
            i = bisect_right(start_vals, q) - 1
            if i < 0:
                continue
            row = clean_rows[i][1]
            price = index_row_price(row)
            if price is None:
                continue
            end = index_row_end(row)
            if end is not None and q > end + max_stale_seconds:
                continue
            assigned[(key, q)] = {
                "key": key,
                "price": price,
                "exchange_epoch": clean_rows[i][0],
                "source_quote_index": str(best_path) if best_path else None,
            }
    return assigned


def collect_proxy_events(events_path: Path) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    events: list[dict[str, Any]] = []
    proxy_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            event = json.loads(line)
            events.append(event)
            if PROXY not in line:
                continue
            event_id = str(event.get("event_id") or "")
            if not event_id:
                continue
            proxy_events[event_id].append(event)
    return events, proxy_events


def build_queries(proxy_events: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, list[float]]]:
    queries: dict[str, dict[str, set[float]]] = defaultdict(lambda: defaultdict(set))
    for event_list in proxy_events.values():
        for event in event_list:
            symbol = str(event.get("instrument_id") or event.get("instrument_name") or "").strip()
            if not symbol:
                continue
            key = display_key(symbol)
            for field in ("matrix_entry_time_ist", "trigger_time_ist", "current_time_ist"):
                t = event.get(field)
                if not t:
                    continue
                queries[date_key(t)][key].add(epoch(t))
    return {
        day: {key: sorted(vals) for key, vals in by_key.items()}
        for day, by_key in queries.items()
    }


def scan_day(day: str, by_key: dict[str, list[float]], target_root: Path, max_stale_seconds: float) -> dict[tuple[str, float], dict[str, Any]]:
    stream = target_root / day / f"target_quotes_{day}.jsonl"
    if not stream.exists():
        return {}

    assigned: dict[tuple[str, float], dict[str, Any]] = {}
    idx = {key: 0 for key in by_key}
    last_by_key: dict[str, dict[str, Any]] = {}
    key_tokens = []
    for key in by_key:
        key_tokens.append(f'"key":"{key}"')
        key_tokens.append(f'"key": "{key}"')

    def assign_until(key: str, before_epoch: float | None = None, include_equal: bool = False) -> None:
        values = by_key[key]
        i = idx[key]
        last = last_by_key.get(key)
        while i < len(values):
            q = values[i]
            if before_epoch is not None:
                if include_equal:
                    if q > before_epoch:
                        break
                elif q >= before_epoch:
                    break
            if last:
                age = q - float(last.get("exchange_epoch") or last.get("event_epoch") or q)
                if age >= 0 and age <= max_stale_seconds:
                    assigned[(key, q)] = dict(last)
            i += 1
        idx[key] = i

    with stream.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if not any(token in line for token in key_tokens):
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            key = row.get("key")
            if key not in by_key:
                continue
            try:
                e = float(row.get("exchange_epoch") or row.get("event_epoch") or 0)
            except Exception:
                continue
            if e <= 0:
                continue
            assign_until(key, e, include_equal=False)
            price = row_price(row)
            if price is not None:
                last_by_key[key] = row
            assign_until(key, e, include_equal=True)
            if all(idx[k] >= len(v) for k, v in by_key.items()):
                break

    for key in by_key:
        assign_until(key, None, include_equal=True)
    return assigned


def restate_event(event: dict[str, Any], assigned: dict[tuple[str, float], dict[str, Any]]) -> tuple[dict[str, Any], bool, list[str]]:
    if PROXY not in json.dumps(event, separators=(",", ":"), default=str):
        return event, False, []
    symbol = str(event.get("instrument_id") or event.get("instrument_name") or "").strip()
    if not symbol:
        return event, False, ["missing_symbol"]
    key = display_key(symbol)
    missing: list[str] = []
    out = dict(event)

    def lookup(time_field: str) -> tuple[float | None, dict[str, Any] | None]:
        t = out.get(time_field)
        if not t:
            return None, None
        q = epoch(t)
        row = assigned.get((key, q))
        if not row:
            missing.append(time_field)
            return q, None
        return q, row

    _, entry_row = lookup("matrix_entry_time_ist")
    _, trigger_row = lookup("trigger_time_ist")
    _, current_row = lookup("current_time_ist")
    if entry_row:
        out["matrix_entry_price_underlying"] = row_price(entry_row)
        out["matrix_entry_price_source"] = DISPLAY_SOURCE
        out["matrix_entry_instrument_key"] = key
        out["signal_instrument_key"] = key
        out["signal_source"] = "cash"
    if trigger_row:
        out["trigger_price_underlying"] = row_price(trigger_row)
        out["trigger_price_source"] = DISPLAY_SOURCE
        out["trigger_price_instrument_key"] = key
        out["display_price_source"] = DISPLAY_CLASS
    if current_row:
        out["current_price_underlying"] = row_price(current_row)
        out["current_price_source"] = DISPLAY_SOURCE
    if entry_row or trigger_row or current_row:
        out["display_restatement_source"] = DISPLAY_SOURCE
        out["display_restatement_instrument_key"] = key
        out["display_restatement_at_ist"] = dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).isoformat()
    return out, bool(entry_row or trigger_row or current_row), missing


def patch_state_value(obj: Any, by_event_id: dict[str, dict[str, Any]]) -> Any:
    if isinstance(obj, list):
        return [patch_state_value(v, by_event_id) for v in obj]
    if not isinstance(obj, dict):
        return obj
    event_id = obj.get("event_id")
    if event_id in by_event_id:
        obj = dict(obj)
        obj.update(by_event_id[event_id])
    else:
        obj = {k: patch_state_value(v, by_event_id) for k, v in obj.items()}
    last = obj.get("last_event")
    if isinstance(last, dict):
        ev_id = last.get("event_id")
        if ev_id in by_event_id:
            ev = by_event_id[ev_id]
            obj["current_price_underlying"] = ev.get("current_price_underlying")
            obj["current_price_source"] = ev.get("current_price_source")
            obj["current_time_ist"] = ev.get("current_time_ist")
            obj["last_trade_event_price_underlying"] = ev.get("trigger_price_underlying")
            obj["last_trade_event_price_source"] = ev.get("trigger_price_source")
            obj["last_trade_event_entry_price_underlying"] = ev.get("matrix_entry_price_underlying")
            obj["last_trade_event_entry_price_source"] = ev.get("matrix_entry_price_source")
            obj["trigger_price_underlying"] = ev.get("trigger_price_underlying")
            obj["trigger_price_source"] = ev.get("trigger_price_source")
            obj["trigger_price_instrument_key"] = ev.get("trigger_price_instrument_key")
            obj["matrix_entry_price_underlying"] = ev.get("matrix_entry_price_underlying")
            obj["matrix_entry_price_source"] = ev.get("matrix_entry_price_source")
            obj["matrix_entry_instrument_key"] = ev.get("matrix_entry_instrument_key")
    return obj


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--v2matrix-state", type=Path, default=Path("/opt/cloud-deploy-candidates/v2matrix/state"))
    ap.add_argument("--target-stream-root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/target_stream"))
    ap.add_argument("--quote-index-root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/research_cache/t2_quote_index"))
    ap.add_argument("--no-quote-index", action="store_true")
    ap.add_argument("--max-stale-seconds", type=float, default=120.0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    state_dir = args.v2matrix_state
    events_path = state_dir / "matrix_events.jsonl"
    state_path = state_dir / "matrix_state.json"
    report_path = state_dir / "v2matrix_historical_display_restatement_report.json"

    events, proxy_events = collect_proxy_events(events_path)
    queries = build_queries(proxy_events)
    assigned: dict[tuple[str, float], dict[str, Any]] = {}
    query_source = "quote_index"
    for day, by_key in sorted(queries.items()):
        day_assigned = {}
        if not args.no_quote_index:
            day_assigned = load_quote_index_day(day, by_key, args.quote_index_root, args.max_stale_seconds)
        if len(day_assigned) < sum(len(v) for v in by_key.values()) and args.no_quote_index:
            query_source = "target_stream_scan"
            day_assigned.update(scan_day(day, by_key, args.target_stream_root, args.max_stale_seconds))
        assigned.update(day_assigned)

    restated_events: list[dict[str, Any]] = []
    by_event_id: dict[str, dict[str, Any]] = {}
    changed = 0
    missing_counts: dict[str, int] = defaultdict(int)
    for event in events:
        new_event, did_change, missing = restate_event(event, assigned)
        restated_events.append(new_event)
        if did_change:
            changed += 1
            event_id = str(new_event.get("event_id") or "")
            if event_id:
                by_event_id[event_id] = new_event
        for field in missing:
            missing_counts[field] += 1

    state = json.loads(state_path.read_text())
    new_state = patch_state_value(state, by_event_id)
    new_state["updated_at_ist"] = dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).isoformat()
    new_state["historical_display_restatement"] = {
        "source": DISPLAY_SOURCE,
        "restated_events": changed,
        "proxy_events_before": sum(len(v) for v in proxy_events.values()),
    }

    events_blob = "".join(json.dumps(e, separators=(",", ":"), ensure_ascii=False) + "\n" for e in restated_events)
    state_blob = json.dumps(new_state, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    proxy_after_events = events_blob.count(PROXY)
    proxy_after_state = state_blob.count(PROXY)

    report = {
        "schema": "v2matrix.historical_display_restatement.v1",
        "created_at_ist": dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).isoformat(),
        "dry_run": args.dry_run,
        "events_total": len(events),
        "proxy_events_before": sum(len(v) for v in proxy_events.values()),
        "events_restated": changed,
        "query_dates": sorted(queries),
        "query_key_count": sum(len(v) for v in queries.values()),
        "assigned_points": len(assigned),
        "query_source": query_source,
        "missing_counts": dict(sorted(missing_counts.items())),
        "proxy_marker_after_events": proxy_after_events,
        "proxy_marker_after_state": proxy_after_state,
        "events_sha256_before": sha256(events_path),
        "state_sha256_before": sha256(state_path),
    }

    if args.dry_run:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if proxy_after_events == 0 and proxy_after_state == 0 else 2

    stamp = time.strftime("%Y%m%d_%H%M%S")
    for path in (events_path, state_path):
        shutil.copy2(path, path.with_suffix(path.suffix + f".bak_hist_display_restate_{stamp}"))
    tmp_events = events_path.with_suffix(events_path.suffix + ".tmp_restate")
    tmp_state = state_path.with_suffix(state_path.suffix + ".tmp_restate")
    tmp_report = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp_events.write_text(events_blob, encoding="utf-8")
    tmp_state.write_text(state_blob, encoding="utf-8")
    report["events_sha256_after"] = hashlib.sha256(events_blob.encode("utf-8")).hexdigest()
    report["state_sha256_after"] = hashlib.sha256(state_blob.encode("utf-8")).hexdigest()
    tmp_report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp_events, events_path)
    os.replace(tmp_state, state_path)
    os.replace(tmp_report, report_path)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if proxy_after_events == 0 and proxy_after_state == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
