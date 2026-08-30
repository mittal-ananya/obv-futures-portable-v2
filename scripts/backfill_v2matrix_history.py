#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import os
import pickle
import shutil
import sys
import tempfile
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_portfolio_rules as portfolio_rules  # noqa: E402
import run_v2matrix_overlay as live_overlay  # noqa: E402


SCHEMA = "obvfutport_v2.v2matrix_history_backfill.v1"
QUOTE_CACHE_SCHEMA = portfolio_rules.QUOTE_INDEX_CACHE_SCHEMA


def now_ist_iso() -> str:
    return datetime.now(tz=live_overlay.IST).isoformat()


def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def json_clean(value: Any) -> Any:
    return live_overlay.json_clean(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        json.dump(json_clean(payload), handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")
        tmp_name = handle.name
    os.chmod(tmp_name, 0o644)
    os.replace(tmp_name, path)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        for row in rows:
            handle.write(json.dumps(json_clean(row), ensure_ascii=True, sort_keys=True))
            handle.write("\n")
        tmp_name = handle.name
    os.chmod(tmp_name, 0o644)
    os.replace(tmp_name, path)


def latest_stream_date(root: Path) -> date:
    latest: date | None = None
    for day_dir in sorted((root / "state" / "target_stream").glob("20??-??-??")):
        try:
            day = base.parse_date(day_dir.name)
        except ValueError:
            continue
        path = day_dir / f"target_quotes_{day_dir.name}.jsonl"
        if path.exists():
            latest = day if latest is None else max(latest, day)
    if latest is None:
        raise RuntimeError("no v2 target_stream files found")
    return latest


def cache_meta_compatible(meta: dict[str, Any], *, trade_date: str, source: dict[str, Any]) -> bool:
    return (
        meta.get("schema") == QUOTE_CACHE_SCHEMA
        and meta.get("trade_date") == trade_date
        and meta.get("source") == source
    )


def load_pickle_gz(path: Path) -> Any:
    with gzip.open(path, "rb") as handle:
        return pickle.load(handle)


def save_pickle_gz(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with gzip.open(tmp, "wb", compresslevel=3) as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)


def merge_payload_into_index(
    index: base.QuoteIndex,
    payload: dict[str, list[tuple[int, float, float, float | None, float | None]]],
) -> int:
    merged = 0
    raw = index._raw  # type: ignore[attr-defined]
    for key, rows in payload.items():
        by_minute = raw.setdefault(key, {})
        for minute, event_epoch, price, bid, ask in rows:
            old = by_minute.get(int(minute))
            if old is None or float(event_epoch) >= old.event_epoch:
                by_minute[int(minute)] = base.Quote(int(minute), float(event_epoch), float(price), bid, ask)
            merged += 1
    return merged


def find_compatible_cache(cache_dir: Path, trade_date: str, source: dict[str, Any]) -> tuple[Path, dict[str, Any]] | None:
    candidates = sorted(cache_dir.glob(f"{trade_date}_*.quote_index_meta.json"))
    for meta_path in reversed(candidates):
        meta = base.read_json(meta_path, {})
        if not isinstance(meta, dict) or not cache_meta_compatible(meta, trade_date=trade_date, source=source):
            continue
        cache_file = Path(str(meta_path).replace(".quote_index_meta.json", ".quote_index.pkl.gz"))
        if cache_file.exists():
            return cache_file, meta
    return None


def current_hash_cache_paths(cache_dir: Path, trade_date: str, required_keys: set[str]) -> tuple[Path, Path, str]:
    key_hash = portfolio_rules.required_key_hash(required_keys)
    cache_file, meta_file = portfolio_rules.cache_paths(cache_dir, trade_date, key_hash)
    return cache_file, meta_file, key_hash


def scan_stream_for_keys(
    *,
    path: Path,
    trade_date: str,
    required_keys: set[str],
    progress_path: Path,
    progress_prefix: str,
    yield_every_lines: int,
    yield_seconds: float,
) -> tuple[dict[str, list[tuple[int, float, float, float | None, float | None]]], dict[str, Any]]:
    from obvfut_portable_v2.passive_runner import row_from_target_stream_line  # type: ignore

    payload_by_minute: dict[str, dict[int, tuple[int, float, float, float | None, float | None]]] = {}
    total_lines = 0
    kept_rows = 0
    started = time.monotonic()
    last_progress = started
    size = path.stat().st_size
    with path.open("rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            total_lines += 1
            row = row_from_target_stream_line(raw_line, trade_date, required_keys)
            if row is None:
                if yield_every_lines > 0 and total_lines % yield_every_lines == 0 and yield_seconds > 0:
                    time.sleep(yield_seconds)
                continue
            price = base.as_float(row.get("price"))
            epoch = base.as_float(row.get("epoch"))
            key = str(row.get("target") or "")
            if price is None or epoch is None or not key:
                continue
            bid = base.as_float(row.get("bid"))
            ask = base.as_float(row.get("ask"))
            minute = base.minute_floor(epoch)
            by_minute = payload_by_minute.setdefault(key, {})
            old = by_minute.get(minute)
            if old is None or float(epoch) >= old[1]:
                by_minute[minute] = (int(minute), float(epoch), float(price), bid, ask)
            kept_rows += 1
            if yield_every_lines > 0 and total_lines % yield_every_lines == 0:
                now = time.monotonic()
                if now - last_progress >= 5:
                    try:
                        position = handle.tell()
                    except OSError:
                        position = None
                    write_json(
                        progress_path,
                        {
                            "schema": SCHEMA,
                            "phase": f"{progress_prefix}_scan",
                            "trade_date": trade_date,
                            "path": str(path),
                            "position": position,
                            "size_bytes": size,
                            "pct": (position / size * 100.0) if position is not None and size else None,
                            "lines": total_lines,
                            "kept_rows": kept_rows,
                            "target_key_count": len(required_keys),
                            "elapsed_seconds": round(now - started, 3),
                            "updated_at_ist": now_ist_iso(),
                        },
                    )
                    last_progress = now
                if yield_seconds > 0:
                    time.sleep(yield_seconds)
    payload = {key: list(rows.values()) for key, rows in payload_by_minute.items()}
    return payload, {
        "line_count": total_lines,
        "kept_target_rows": kept_rows,
        "minute_quote_rows": sum(len(rows) for rows in payload.values()),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def load_quote_index_lenient(
    *,
    root: Path,
    stream_paths: list[tuple[str, Path]],
    required_keys: set[str],
    cache_dir: Path,
    output_dir: Path,
    yield_every_lines: int,
    yield_seconds: float,
) -> tuple[base.QuoteIndex, dict[str, Any]]:
    index = base.QuoteIndex()
    per_day: dict[str, dict[str, Any]] = {}
    total_cache_rows = 0
    total_scan_rows = 0
    total_scanned_lines = 0
    started = time.monotonic()
    for trade_date, path in stream_paths:
        source = portfolio_rules.stream_fingerprint(path)
        cache_file, meta_file, key_hash = current_hash_cache_paths(cache_dir, trade_date, required_keys)
        day_payload: dict[str, list[tuple[int, float, float, float | None, float | None]]] = {}
        cache_status = "miss"
        loaded_cache_file: str | None = None
        missing_keys = set(required_keys)
        if cache_file.exists() and meta_file.exists():
            meta = base.read_json(meta_file, {})
            if isinstance(meta, dict) and portfolio_rules.cache_meta_matches(
                meta,
                trade_date=trade_date,
                key_hash=key_hash,
                source=source,
            ):
                payload = load_pickle_gz(cache_file)
                if isinstance(payload, dict):
                    day_payload = {key: rows for key, rows in payload.items() if key in required_keys}
                    missing_keys -= set(day_payload)
                    cache_status = "exact_hit"
                    loaded_cache_file = str(cache_file)
        if cache_status == "miss":
            compatible = find_compatible_cache(cache_dir, trade_date, source)
            if compatible is not None:
                older_cache_file, older_meta = compatible
                payload = load_pickle_gz(older_cache_file)
                if isinstance(payload, dict):
                    day_payload = {key: rows for key, rows in payload.items() if key in required_keys}
                    missing_keys -= set(day_payload)
                    cache_status = "compatible_partial_hit"
                    loaded_cache_file = str(older_cache_file)
        cache_rows = sum(len(rows) for rows in day_payload.values())
        scanned_report: dict[str, Any] | None = None
        if missing_keys:
            scanned_payload, scanned_report = scan_stream_for_keys(
                path=path,
                trade_date=trade_date,
                required_keys=missing_keys,
                progress_path=output_dir / "backfill_progress.json",
                progress_prefix="quote_index_missing_keys",
                yield_every_lines=yield_every_lines,
                yield_seconds=yield_seconds,
            )
            total_scanned_lines += int(scanned_report.get("line_count") or 0)
            total_scan_rows += int(scanned_report.get("minute_quote_rows") or 0)
            for key, rows in scanned_payload.items():
                day_payload[key] = rows
        total_cache_rows += cache_rows
        merge_payload_into_index(index, day_payload)
        if cache_status != "exact_hit" or missing_keys:
            save_pickle_gz(cache_file, day_payload)
            write_json(
                meta_file,
                {
                    "schema": QUOTE_CACHE_SCHEMA,
                    "created_at_utc": utc_iso(),
                    "trade_date": trade_date,
                    "required_key_hash": key_hash,
                    "required_key_count": len(required_keys),
                    "source": source,
                    "line_count": scanned_report.get("line_count") if scanned_report else None,
                    "kept_target_rows": scanned_report.get("kept_target_rows") if scanned_report else None,
                    "minute_quote_rows": sum(len(rows) for rows in day_payload.values()),
                    "cache_file": str(cache_file),
                    "derived_from_cache_file": loaded_cache_file,
                    "missing_key_scan_count": len(missing_keys),
                },
            )
        per_day[trade_date] = {
            **source,
            "cache_status": cache_status,
            "loaded_cache_file": loaded_cache_file,
            "written_cache_file": str(cache_file),
            "cache_rows_loaded": cache_rows,
            "missing_key_count": len(missing_keys),
            "missing_keys": sorted(missing_keys),
            "scanned": scanned_report,
            "minute_quote_rows": sum(len(rows) for rows in day_payload.values()),
        }
        write_json(
            output_dir / "backfill_progress.json",
            {
                "schema": SCHEMA,
                "phase": "quote_index_day_loaded",
                "trade_date": trade_date,
                "day": per_day[trade_date],
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "updated_at_ist": now_ist_iso(),
            },
        )
    index.finalize()
    return index, {
        "stream_days": per_day,
        "cache_rows_loaded": total_cache_rows,
        "raw_scan_rows_loaded": total_scan_rows,
        "raw_lines_scanned": total_scanned_lines,
        "quote_keys_loaded": index.key_count(),
        "minute_quote_rows": index.row_count(),
        "required_key_count": len(required_keys),
        "required_key_hash": portfolio_rules.required_key_hash(required_keys),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def load_matrix_module(overlay_root: Path) -> Any:
    src = overlay_root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    os.environ.setdefault("PACKAGE_ROOT", str(overlay_root))
    from v2matrix import app as matrix_app  # type: ignore

    return matrix_app


def event_sort_key(payload: dict[str, Any]) -> tuple[int, int, str, str]:
    event_type = str(payload.get("event_type") or "")
    rank = 0 if event_type in {"paper_entry", "long_entry", "short_entry"} else 1
    epoch = live_overlay.parse_epoch(payload.get("event_epoch") or payload.get("trigger_epoch")) or 0
    if not epoch:
        parsed = matrix_time_epoch(payload.get("trigger_time_ist") or payload.get("current_time_ist"))
        epoch = parsed or 0
    return (epoch, rank, str(payload.get("instrument_id") or ""), str(payload.get("event_id") or ""))


def matrix_time_epoch(value: Any) -> int | None:
    parsed = None
    if value:
        try:
            parsed = live_overlay.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=live_overlay.IST)
    return int(parsed.timestamp()) if parsed else None


def enriched_matrix_events(matrix_app: Any, payloads: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    state: dict[str, Any] = {"instruments": {}, "event_count": 0, "created_at_ist": now_ist_iso()}
    events: list[dict[str, Any]] = []
    for payload in sorted(payloads, key=event_sort_key):
        received_at = payload.get("trigger_time_ist") or now_ist_iso()
        payload = {
            **payload,
            "matrix_rebuild_event": True,
            "suppress_notification": True,
        }
        row = matrix_app.apply_event(payload, state=state, persist=False, received_at_ist=str(received_at))
        event = row.get("last_event") if isinstance(row, dict) and isinstance(row.get("last_event"), dict) else None
        if event is not None:
            events.append(event)
    state["schema"] = "obvfutport_v2.v2matrix_state.v1"
    state["history_backfilled"] = True
    state["history_backfill_schema"] = SCHEMA
    state["updated_at_ist"] = now_ist_iso()
    return state, events


def end_clock_for_summary(index: base.QuoteIndex, end_date: date) -> int:
    latest = None
    for minutes in index._keys.values():  # type: ignore[attr-defined]
        if minutes:
            latest = max(latest or minutes[-1], minutes[-1])
    if latest is not None:
        return int(latest)
    clocks = base.session_clock_epochs(end_date)
    return clocks[-1]


def simulate_overlay_history(
    *,
    root: Path,
    overlay_root: Path,
    start_date: date,
    end_date: date,
    index: base.QuoteIndex,
    panel: dict[int, dict[str, dict[str, Any]]],
    legs: dict[str, base.TrancheLeg],
    v1_portfolio: Any,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    state: dict[str, Any] = {
        "schema": live_overlay.SCHEMA,
        "created_at_ist": now_ist_iso(),
        "primary_variant": live_overlay.PRIMARY_VARIANT,
        "portfolio_variants": list(live_overlay.PORTFOLIO_VARIANTS),
        "active_overlay": {},
        "completed_overlay_keys": [],
        "posted_event_ids": [],
    }
    active_overlay = state.setdefault("active_overlay", {})
    completed_overlay: set[str] = set()
    overlay_events: list[dict[str, Any]] = []
    matrix_payloads: list[dict[str, Any]] = []
    portfolios = live_overlay.ensure_portfolios(state)
    processed_clocks = 0
    skipped_clocks_without_features = 0
    for day in (base.parse_date(day) for day, _path in base.discover_stream_paths(root, start_date, end_date)):
        for clock_epoch in base.session_clock_epochs(day):
            processed_clocks += 1
            features = panel.get(clock_epoch, {})
            if not features:
                skipped_clocks_without_features += 1

            for key, payload in list(active_overlay.items()):
                if not isinstance(payload, dict):
                    active_overlay.pop(key, None)
                    continue
                position = live_overlay.dict_to_overlay_position(payload)
                leg = legs.get(position.row_id)
                if leg is None:
                    continue
                should_exit, exit_reason, ret, exit_fill, fill = live_overlay.should_exit_overlay(
                    variant=position.variant,
                    leg=leg,
                    position=position,
                    index=index,  # type: ignore[arg-type]
                    v1_portfolio=v1_portfolio,
                    clock_epoch=clock_epoch,
                )
                active_overlay[key] = asdict(position)
                if not should_exit or exit_fill is None:
                    continue
                active_overlay.pop(key, None)
                completed_overlay.add(key)
                event = {
                    "schema": live_overlay.SCHEMA,
                    "event": "overlay_exit",
                    "overlay_key": key,
                    "variant": position.variant,
                    "policy": live_overlay.POLICY.name,
                    "source_t2_position_id": leg.position_id,
                    "row_id": leg.row_id,
                    "symbol": leg.symbol,
                    "side": leg.side,
                    "exit_epoch": clock_epoch,
                    "exit_time": live_overlay.epoch_ist_iso(clock_epoch),
                    "exit_reason": exit_reason,
                    "exit_return": ret,
                    "entry_epoch": position.entry_epoch,
                    "entry_time": position.entry_time,
                    "entry_fill_price": position.entry_fill_price,
                    "exit_fill_price": exit_fill,
                    "exit_ltp_price": live_overlay.safe_float((fill or {}).get("ltp_price")) or exit_fill,
                    "history_backfilled": True,
                    "created_at_ist": now_ist_iso(),
                }
                overlay_events.append(event)
                for portfolio in portfolios.values():
                    if isinstance(portfolio, dict) and portfolio.get("variant") == position.variant:
                        live_overlay.close_portfolio_holding(
                            portfolio=portfolio,
                            overlay_key_value=key,
                            exit_epoch=clock_epoch,
                            exit_fill_price=exit_fill,
                            exit_reason=exit_reason,
                            leg=leg,
                            v1_portfolio=v1_portfolio,
                        )
                if position.variant == live_overlay.PRIMARY_VARIANT:
                    signal_quote = index.quote_at_or_before(leg.signal_key, clock_epoch, max_age_seconds=300)
                    trigger_price = signal_quote.price if signal_quote else event["exit_ltp_price"]
                    matrix_payloads.append(
                        live_overlay.matrix_payload(
                            event_type="tranche2_exit",
                            variant=position.variant,
                            leg=leg,
                            position=position,
                            event_epoch_value=clock_epoch,
                            trigger_price=float(trigger_price),
                            trigger_source="v2matrix_overlay_signal_stream",
                            features=features.get(leg.row_id),
                            exit_reason=exit_reason,
                            position_closed=True,
                        )
                    )

            eligible = [feature for feature in features.values() if live_overlay.candidate_passes(feature)]
            eligible.sort(
                key=lambda row: (
                    float(row.get("portfolio_score") or -1.0),
                    str(row.get("symbol") or ""),
                    str(row.get("row_id") or ""),
                ),
                reverse=True,
            )
            for feature in eligible:
                row_id = str(feature["row_id"])
                leg = legs.get(row_id)
                if leg is None:
                    continue
                for variant in live_overlay.PORTFOLIO_VARIANTS:
                    key = live_overlay.overlay_key(variant, row_id)
                    if key in active_overlay or key in completed_overlay:
                        continue
                    fill = live_overlay.execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")  # type: ignore[arg-type]
                    if fill is None:
                        continue
                    entry_fill = live_overlay.safe_float(fill.get("fill_price"))
                    entry_ltp = live_overlay.safe_float(fill.get("ltp_price")) or entry_fill
                    if entry_fill is None or entry_ltp is None:
                        continue
                    signal_quote = index.quote_at_or_before(leg.signal_key, clock_epoch, max_age_seconds=300)
                    entry_display_price = float(signal_quote.price) if signal_quote else float(entry_ltp)
                    entry_display_source = "v2matrix_overlay_signal_stream" if signal_quote else "v2matrix_overlay_entry_ltp_fallback"
                    entry_display_key = leg.signal_key if signal_quote else leg.execution_key
                    entry_features = dict(feature)
                    entry_features.update(
                        {
                            "entry_display_price_underlying": entry_display_price,
                            "entry_display_price_source": entry_display_source,
                            "entry_display_instrument_key": entry_display_key,
                            "entry_execution_fill_price": float(entry_fill),
                            "entry_execution_ltp_price": float(entry_ltp),
                            "entry_execution_instrument_key": leg.execution_key,
                        }
                    )
                    position = live_overlay.OverlayPosition(
                        variant=variant,
                        row_id=row_id,
                        position_id=leg.position_id,
                        symbol=leg.symbol,
                        side=leg.side,
                        entry_epoch=clock_epoch,
                        entry_time=live_overlay.epoch_ist_iso(clock_epoch),
                        entry_fill_price=float(entry_fill),
                        entry_ltp_price=float(entry_ltp),
                        entry_score=float(feature.get("portfolio_score") or 0.0),
                        entry_features=entry_features,
                    )
                    active_overlay[key] = asdict(position)
                    event = {
                        "schema": live_overlay.SCHEMA,
                        "event": "overlay_entry",
                        "overlay_key": key,
                        "variant": variant,
                        "policy": live_overlay.POLICY.name,
                        "source_t2_position_id": leg.position_id,
                        "row_id": row_id,
                        "symbol": leg.symbol,
                        "side": leg.side,
                        "entry_epoch": clock_epoch,
                        "entry_time": position.entry_time,
                        "entry_score": position.entry_score,
                        "entry_features": entry_features,
                        "entry_fill_price": entry_fill,
                        "entry_ltp_price": entry_ltp,
                        "entry_display_price_underlying": entry_display_price,
                        "entry_display_price_source": entry_display_source,
                        "entry_display_instrument_key": entry_display_key,
                        "history_backfilled": True,
                        "created_at_ist": now_ist_iso(),
                    }
                    overlay_events.append(event)
                    portfolio = portfolios.get(live_overlay.portfolio_key(variant))
                    if isinstance(portfolio, dict):
                        live_overlay.open_portfolio_holding(portfolio=portfolio, variant=variant, position=position, leg=leg)
                    if variant == live_overlay.PRIMARY_VARIANT:
                        matrix_payloads.append(
                            live_overlay.matrix_payload(
                                event_type="paper_entry",
                                variant=variant,
                                leg=leg,
                                position=position,
                                event_epoch_value=clock_epoch,
                                trigger_price=entry_display_price,
                                trigger_source=entry_display_source,
                                features=entry_features,
                            )
                        )
    state["completed_overlay_keys"] = sorted(completed_overlay)
    state["posted_event_ids"] = sorted(str(row.get("event_id") or "") for row in matrix_payloads if row.get("event_id"))
    final_clock = end_clock_for_summary(index, end_date)
    summaries = []
    for portfolio in portfolios.values():
        if isinstance(portfolio, dict):
            summaries.append(live_overlay.portfolio_summary(portfolio, index, v1_portfolio, legs, final_clock))  # type: ignore[arg-type]
    state["portfolio_summaries"] = summaries
    state["quote_ring"] = {}
    state["updated_at_ist"] = now_ist_iso()
    state["last_clock"] = {
        "clock_epoch": final_clock,
        "clock_time_ist": live_overlay.epoch_ist_iso(final_clock),
        "in_session": False,
        "loaded_t2_legs": len(legs),
        "active_t2_legs": sum(1 for leg in legs.values() if leg.entry_epoch <= final_clock and (leg.exit_epoch is None or leg.exit_epoch > final_clock)),
        "processed_historical_clocks": processed_clocks,
        "skipped_clocks_without_features": skipped_clocks_without_features,
        "created_overlay_events": len(overlay_events),
        "matrix_events": len(matrix_payloads),
        "active_overlay_count": len(active_overlay),
        "quote_keys": index.key_count(),
        "quote_rows": index.row_count(),
        "history_backfilled": True,
    }
    return state, overlay_events, matrix_payloads, {
        "processed_clocks": processed_clocks,
        "skipped_clocks_without_features": skipped_clocks_without_features,
        "overlay_events": len(overlay_events),
        "matrix_payloads": len(matrix_payloads),
        "active_overlay_count": len(active_overlay),
        "completed_overlay_count": len(completed_overlay),
    }


def install_state(
    *,
    overlay_root: Path,
    overlay_state: dict[str, Any],
    overlay_events: list[dict[str, Any]],
    matrix_state: dict[str, Any],
    matrix_events: list[dict[str, Any]],
    report: dict[str, Any],
) -> None:
    state_dir = overlay_root / "state"
    backup_dir = state_dir / "backups" / f"pre_history_backfill_{datetime.now(tz=live_overlay.IST).strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    for name in ("overlay_state.json", "overlay_events.jsonl", "portfolio_state.json", "matrix_state.json", "matrix_events.jsonl", "overlay_status.json"):
        src = state_dir / name
        if src.exists():
            shutil.copy2(src, backup_dir / name)
    write_json(state_dir / "overlay_state.json", overlay_state)
    write_jsonl(state_dir / "overlay_events.jsonl", overlay_events)
    live_overlay.write_portfolio_files(overlay_root, overlay_state)
    write_json(state_dir / "matrix_state.json", matrix_state)
    write_jsonl(state_dir / "matrix_events.jsonl", matrix_events)
    status = {
        "ok": True,
        "schema": "obvfutport_v2.v2matrix_overlay_status.v1",
        "updated_at_ist": now_ist_iso(),
        "stream": {"skipped": True, "reason": "history_backfill_installed"},
        "clock": overlay_state.get("last_clock"),
        "history_backfill": {
            "installed": True,
            "report_path": str(state_dir / "v2matrix_history_backfill_report.json"),
        },
    }
    write_json(state_dir / "overlay_status.json", status)
    write_json(state_dir / "v2matrix_history_backfill_report.json", {**report, "backup_dir": str(backup_dir)})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--overlay-root", type=Path, default=Path("/opt/cloud-deploy-candidates/v2matrix"))
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--risk-floor", type=float, default=0.0005)
    parser.add_argument("--max-entry-staleness-seconds", type=float, default=5.0)
    parser.add_argument("--quote-index-cache-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scan-yield-every-lines", type=int, default=250000)
    parser.add_argument("--scan-yield-seconds", type=float, default=0.02)
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()

    root = args.root
    overlay_root = args.overlay_root
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    base.add_paths(root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    start = base.parse_date(args.start_date)
    end = base.parse_date(args.end_date) if args.end_date else latest_stream_date(root)
    stream_paths = base.discover_stream_paths(root, start, end)
    if not stream_paths:
        raise RuntimeError(f"no target stream paths found between {start} and {end}")
    legs = live_overlay.load_t2_legs(root, args.max_entry_staleness_seconds)
    required_keys = {leg.signal_key for leg in legs.values()} | {leg.execution_key for leg in legs.values()}
    cache_dir = args.quote_index_cache_dir or (root / "state" / "research_cache" / "t2_quote_index")
    write_json(
        output_dir / "backfill_progress.json",
        {
            "schema": SCHEMA,
            "phase": "started",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "stream_days": [day for day, _path in stream_paths],
            "t2_legs": len(legs),
            "required_keys": len(required_keys),
            "install": bool(args.install),
            "updated_at_ist": now_ist_iso(),
        },
    )
    quote_index, input_report = load_quote_index_lenient(
        root=root,
        stream_paths=stream_paths,
        required_keys=required_keys,
        cache_dir=cache_dir,
        output_dir=output_dir,
        yield_every_lines=args.scan_yield_every_lines,
        yield_seconds=args.scan_yield_seconds,
    )
    dates = [base.parse_date(day) for day, _path in stream_paths]
    write_json(
        output_dir / "backfill_progress.json",
        {
            "schema": SCHEMA,
            "phase": "quote_index_loaded",
            "input_report": input_report,
            "updated_at_ist": now_ist_iso(),
        },
    )
    panel, feature_report = portfolio_rules.build_feature_panel(
        legs=list(legs.values()),
        dates=dates,
        index=quote_index,
        v1_portfolio=v1_portfolio,
        risk_floor=float(args.risk_floor),
    )
    for clock_features in panel.values():
        for feature in clock_features.values():
            feature["portfolio_score"] = portfolio_rules.blended_score(feature, live_overlay.POLICY.formula)
    write_json(
        output_dir / "backfill_progress.json",
        {
            "schema": SCHEMA,
            "phase": "feature_panel_built",
            "feature_report": feature_report,
            "panel_clock_count": len(panel),
            "updated_at_ist": now_ist_iso(),
        },
    )
    overlay_state, overlay_events, matrix_payloads, simulation_report = simulate_overlay_history(
        root=root,
        overlay_root=overlay_root,
        start_date=start,
        end_date=end,
        index=quote_index,
        panel=panel,
        legs=legs,
        v1_portfolio=v1_portfolio,
    )
    matrix_app = load_matrix_module(overlay_root)
    matrix_state, matrix_events = enriched_matrix_events(matrix_app, matrix_payloads)
    report = {
        "schema": SCHEMA,
        "created_at_ist": now_ist_iso(),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "source": "OBVFUTPORT-v2 T2 ledger plus quote-valid compact target_stream",
        "policy": live_overlay.POLICY.name,
        "primary_variant": live_overlay.PRIMARY_VARIANT,
        "portfolio_variants": list(live_overlay.PORTFOLIO_VARIANTS),
        "portfolio_definition": {
            "fixed_entry_margin_rupees": live_overlay.FIXED_ENTRY_MARGIN,
            "max_positions": live_overlay.MAX_PORTFOLIO_POSITIONS,
            "cash_constraint": False,
            "replacement": "none",
        },
        "input_report": input_report,
        "feature_report": feature_report,
        "simulation_report": simulation_report,
        "matrix_event_count": len(matrix_events),
        "matrix_symbol_count": len(matrix_state.get("instruments") or {}),
        "portfolio_summaries": overlay_state.get("portfolio_summaries") or [],
        "installed": bool(args.install),
    }
    write_json(output_dir / "v2matrix_history_backfill_report.json", report)
    write_json(output_dir / "overlay_state.json", overlay_state)
    write_jsonl(output_dir / "overlay_events.jsonl", overlay_events)
    write_json(output_dir / "matrix_state.json", matrix_state)
    write_jsonl(output_dir / "matrix_events.jsonl", matrix_events)
    live_overlay.write_portfolio_files(output_dir, overlay_state)
    if args.install:
        install_state(
            overlay_root=overlay_root,
            overlay_state=overlay_state,
            overlay_events=overlay_events,
            matrix_state=matrix_state,
            matrix_events=matrix_events,
            report=report,
        )
    write_json(
        output_dir / "backfill_progress.json",
        {
            "schema": SCHEMA,
            "phase": "complete",
            "installed": bool(args.install),
            "report_path": str(output_dir / "v2matrix_history_backfill_report.json"),
            "summary": {
                "overlay_events": len(overlay_events),
                "matrix_events": len(matrix_events),
                "matrix_symbols": len(matrix_state.get("instruments") or {}),
                "active_overlay": simulation_report.get("active_overlay_count"),
            },
            "updated_at_ist": now_ist_iso(),
        },
    )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
