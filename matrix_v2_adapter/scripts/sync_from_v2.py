from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


IST_OFFSET_SECONDS = 5 * 3600 + 30 * 60
V2_STATE_ROOT = Path(os.environ.get("MATRIX_V2_STATE_ROOT", "/opt/cloud-deploy-candidates/obv-futures-portable-v2/state"))
MATRIX_ROOT = Path(os.environ.get("MATRIX_ROOT", "/opt/cloud-deploy-candidates/matrix-v1"))
BRIDGE_STATE_PATH = MATRIX_ROOT / "state" / "matrix_v2_bridge_state.json"
MATRIX_EVENTS_URL = os.environ.get("MATRIX_EVENTS_URL", "http://127.0.0.1:8097/api/matrix/v1/events")
MATRIX_MAX_ENTRY_STALENESS_SECONDS = float(os.environ.get("MATRIX_MAX_ENTRY_STALENESS_SECONDS", "5"))
LATEST_TICKS_PATH = Path(
    os.environ.get(
        "MATRIX_LATEST_TICKS_PATH",
        "/opt/cloud-deploy-candidates/intraday-short-straddle-v1/state/market_data/latest_ticks.json",
    )
)


def now_ist_iso() -> str:
    return datetime.fromtimestamp(time.time(), tz=timezone.utc).astimezone(
        timezone.utc
    ).astimezone().isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
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


def parse_epoch(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def live_tail_window_start_epoch() -> int:
    """Return the Matrix notification-window start for the current IST day."""
    now_epoch = time.time()
    ist_epoch = now_epoch + IST_OFFSET_SECONDS
    local_day_start = int(ist_epoch // 86400) * 86400
    six_am_ist_as_utc = local_day_start + 6 * 3600 - IST_OFFSET_SECONDS
    if now_epoch < six_am_ist_as_utc:
        six_am_ist_as_utc -= 86400
    return int(six_am_ist_as_utc)


def ledger_event_epoch(event: dict[str, Any], event_type: str) -> int | None:
    if event_type == "paper_entry":
        position = event.get("position") if isinstance(event.get("position"), dict) else {}
        return parse_epoch(position.get("entry_epoch") or event.get("entry_epoch") or event.get("event_epoch"))
    if event_type in {"tranche2_exit", "paper_exit"}:
        return parse_epoch(event.get("exit_epoch") or event.get("event_epoch"))
    return parse_epoch(event.get("event_epoch"))


def ledger_event_sort_rank(event: dict[str, Any], event_type: str) -> int:
    if event_type == "paper_exit" and event.get("exit_reason") == "lifecycle_rollover":
        return -2
    if event_type == "tranche2_exit" and event.get("rollover_id"):
        return -1
    if event_type == "paper_entry":
        return 0
    if event_type == "tranche2_exit":
        return 1
    if event_type == "paper_exit":
        return 2
    return 3


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "stale"}


def entry_event_is_stale(position: dict[str, Any], event: dict[str, Any]) -> bool:
    if truthy(position.get("entry_stale")) or truthy(event.get("entry_stale")):
        return True
    staleness_seconds = safe_float(
        position.get("entry_staleness_seconds")
        or event.get("entry_staleness_seconds")
        or position.get("fill_lag_seconds")
        or event.get("fill_lag_seconds")
    )
    return bool(staleness_seconds is not None and staleness_seconds > MATRIX_MAX_ENTRY_STALENESS_SECONDS)


def matrix_entry_event_id(position_key: str) -> str:
    return f"MATRIX:v2:selected_t2_entry:{position_key}"


def matrix_exit_event_id(position_key: str, exit_epoch: Any, selected_type: str) -> str:
    return f"MATRIX:v2:selected_{selected_type}_exit:{position_key}:{exit_epoch}"


def safe_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def latest_tick_for_key(key: str) -> dict[str, Any]:
    if not key:
        return {}
    payload = read_json(LATEST_TICKS_PATH, {})
    ticks = payload.get("ticks") if isinstance(payload, dict) else {}
    tick = ticks.get(key.upper()) if isinstance(ticks, dict) else None
    return tick if isinstance(tick, dict) else {}


def tick_price_time(key: str) -> tuple[float | None, str | None, str]:
    tick = latest_tick_for_key(key)
    price = safe_float(tick.get("last_price")) or safe_float(tick.get("price"))
    when = tick.get("exchange_timestamp") or tick.get("last_trade_time") or tick.get("received_at_ist")
    return price, when, "cash_stock_latest_ticks" if price is not None else "cash_stock_latest_ticks_unavailable"


def fallback_price(source: dict[str, Any], keys: tuple[str, ...]) -> tuple[float | None, str]:
    for key in keys:
        value = safe_float(source.get(key))
        if value is not None:
            return value, f"v2_ledger_{key}"
    return None, "unavailable"


def is_recent_epoch(epoch: int | None, max_age_seconds: int = 180) -> bool:
    if epoch is None:
        return False
    return abs(time.time() - epoch) <= max_age_seconds


def post_matrix(payload: dict[str, Any], dry_run: bool = False) -> bool:
    if dry_run:
        print(json.dumps(payload, sort_keys=True))
        return True
    data = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    req = urllib.request.Request(
        MATRIX_EVENTS_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"post_failed event_id={payload.get('event_id')} error={exc}", flush=True)
        return False


def post_matrix_batch(payloads: list[dict[str, Any]], dry_run: bool = False) -> bool:
    if not payloads:
        return True
    if len(payloads) == 1:
        return post_matrix(payloads[0], dry_run=dry_run)
    payload = {"events": payloads}
    if dry_run:
        print(json.dumps(payload, sort_keys=True))
        return True
    data = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    req = urllib.request.Request(
        MATRIX_EVENTS_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"post_batch_failed count={len(payloads)} error={exc}", flush=True)
        return False


def entry_payload(position: dict[str, Any], *, received_event: dict[str, Any] | None = None) -> dict[str, Any]:
    received_event = received_event or {}
    symbol = safe_symbol(position.get("symbol") or received_event.get("symbol"))
    side = str(position.get("side") or received_event.get("side") or "").lower()
    signal_key = str(position.get("signal_instrument_key") or received_event.get("signal_instrument_key") or "").strip()
    entry_epoch = parse_epoch(position.get("entry_epoch") or received_event.get("entry_epoch"))
    entry_price, entry_source = fallback_price(
        position,
        (
            "matrix_entry_price_underlying",
            "compass_entry_price_underlying",
            "signal_entry_price",
            "signal_price",
        ),
    )
    if is_recent_epoch(entry_epoch):
        live_price, live_time, live_source = tick_price_time(signal_key)
        if live_price is not None:
            entry_price, entry_source = live_price, live_source
    current_price, current_time, current_source = tick_price_time(signal_key)
    position_id = str(position.get("position_id") or received_event.get("position_id") or "")
    signal_id = str(position.get("signal_id") or received_event.get("signal_id") or position_id)
    return {
        "event_id": f"MATRIX:v2:selected_t2_entry:{position_id or signal_id}",
        "source_strategy": "OBVFUTPORT_V2_PASSIVE",
        "source_model_version": position.get("model_version") or received_event.get("model_version"),
        "instrument_id": symbol,
        "instrument_name": symbol,
        "event_type": "paper_entry",
        "side": side,
        "tranche": "T2",
        "trigger_time_ist": position.get("entry_time") or received_event.get("entry_time"),
        "trigger_price_underlying": entry_price,
        "trigger_price_source": entry_source if entry_source.startswith("cash_stock") else f"cash_stock_{entry_source}",
        "trigger_price_instrument_key": signal_key,
        "current_price_underlying": current_price or entry_price,
        "current_time_ist": current_time or position.get("entry_time") or received_event.get("entry_time"),
        "current_price_source": current_source,
        "signal_source": position.get("signal_source") or received_event.get("signal_source"),
        "signal_instrument_key": signal_key,
        "execution_instrument_key": position.get("instrument_key") or received_event.get("instrument_key"),
        "display_price_source": "cash_underlying",
        "execution_price_source": "v2_futures_execution_contract",
        "matrix_selected_leg": "T2",
        "matrix_selection_rule": "t2_exit_else_t1_base_exit",
        "position_id": position_id,
        "signal_id": signal_id,
        "created_at_ist": received_event.get("created_at_ist") or received_event.get("recorded_at_ist"),
        "entry_stale": bool(position.get("entry_stale") or received_event.get("entry_stale")),
        "entry_stale_reason": position.get("entry_stale_reason") or received_event.get("entry_stale_reason"),
        "entry_staleness_seconds": position.get("entry_staleness_seconds") or received_event.get("entry_staleness_seconds"),
    }


def exit_payload(event: dict[str, Any], entry: dict[str, Any], *, selected_type: str) -> dict[str, Any]:
    symbol = safe_symbol(event.get("symbol") or entry.get("symbol"))
    side = str(event.get("side") or entry.get("side") or "").lower()
    signal_key = str(event.get("signal_instrument_key") or entry.get("signal_instrument_key") or "").strip()
    exit_epoch = parse_epoch(event.get("exit_epoch"))
    exit_price, exit_source = fallback_price(
        event,
        (
            "matrix_exit_price_underlying",
            "compass_exit_price_underlying",
            "signal_exit_price",
            "transition_reference_price",
            "exit_price",
        ),
    )
    if is_recent_epoch(exit_epoch):
        live_price, live_time, live_source = tick_price_time(signal_key)
        if live_price is not None:
            exit_price, exit_source = live_price, live_source
    entry_price, entry_source = fallback_price(
        entry,
        (
            "matrix_entry_price_underlying",
            "compass_entry_price_underlying",
            "signal_entry_price",
            "signal_price",
        ),
    )
    position_id = str(event.get("position_id") or entry.get("position_id") or "")
    event_type = "tranche2_exit" if selected_type == "t2" else "base_exit"
    return {
        "event_id": f"MATRIX:v2:selected_{selected_type}_exit:{position_id}:{event.get('exit_epoch')}",
        "source_strategy": "OBVFUTPORT_V2_PASSIVE",
        "source_model_version": event.get("model_version") or entry.get("model_version"),
        "instrument_id": symbol,
        "instrument_name": symbol,
        "event_type": event_type,
        "side": side,
        "tranche": "T2" if selected_type == "t2" else "T1",
        "position_closed": True,
        "trigger_time_ist": event.get("exit_time"),
        "trigger_price_underlying": exit_price,
        "trigger_price_source": exit_source if exit_source.startswith("cash_stock") else f"cash_stock_{exit_source}",
        "trigger_price_instrument_key": signal_key,
        "current_price_underlying": exit_price,
        "current_time_ist": event.get("exit_time"),
        "current_price_source": exit_source,
        "matrix_entry_time_ist": entry.get("entry_time"),
        "matrix_entry_price_underlying": entry_price,
        "matrix_entry_price_source": entry_source if entry_source.startswith("cash_stock") else f"cash_stock_{entry_source}",
        "signal_source": event.get("signal_source") or entry.get("signal_source"),
        "signal_instrument_key": signal_key,
        "execution_instrument_key": event.get("instrument_key") or entry.get("instrument_key"),
        "display_price_source": "cash_underlying",
        "execution_price_source": "v2_futures_execution_contract",
        "matrix_selected_leg": "T2" if selected_type == "t2" else "T1_base_exit",
        "matrix_selection_rule": "t2_exit_else_t1_base_exit",
        "position_id": position_id,
        "signal_id": event.get("signal_id") or entry.get("signal_id"),
        "exit_reason": event.get("exit_reason") or event.get("tranche2_exit_source") or event.get("tranche1_exit_source"),
        "created_at_ist": event.get("created_at_ist") or event.get("recorded_at_ist"),
    }


def collect_events() -> list[tuple[int, int, int, str, dict[str, Any]]]:
    collected: list[tuple[int, int, int, str, dict[str, Any]]] = []
    sequence = 0
    for ledger_path in sorted((V2_STATE_ROOT / "instruments").glob("*/ledger.jsonl")):
        for event in iter_jsonl(ledger_path):
            event_type = str(event.get("event") or "")
            if event_type == "paper_entry":
                position = event.get("position") if isinstance(event.get("position"), dict) else {}
                epoch = parse_epoch(position.get("entry_epoch") or event.get("entry_epoch")) or 0
                collected.append((epoch, ledger_event_sort_rank(event, event_type), sequence, event_type, event))
                sequence += 1
            elif event_type == "tranche2_exit":
                epoch = parse_epoch(event.get("exit_epoch")) or 0
                collected.append((epoch, ledger_event_sort_rank(event, event_type), sequence, event_type, event))
                sequence += 1
            elif event_type == "paper_exit":
                epoch = parse_epoch(event.get("exit_epoch")) or 0
                collected.append((epoch, ledger_event_sort_rank(event, event_type), sequence, event_type, event))
                sequence += 1
    return sorted(collected, key=lambda item: (item[0], item[1], item[2]))


def sync_once(*, dry_run: bool = False) -> dict[str, Any]:
    bridge_state = read_json(BRIDGE_STATE_PATH, {})
    if not isinstance(bridge_state, dict):
        bridge_state = {}
    posted = set(bridge_state.get("posted_event_ids") or [])
    selected_exited = set(bridge_state.get("selected_exited_position_ids") or [])
    suppressed_stale_entries = set(bridge_state.get("suppressed_stale_entry_ids") or [])
    entries: dict[str, dict[str, Any]] = bridge_state.get("entries_by_position_id") if isinstance(bridge_state.get("entries_by_position_id"), dict) else {}
    active_by_symbol: dict[str, str] = (
        bridge_state.get("active_position_by_symbol")
        if isinstance(bridge_state.get("active_position_by_symbol"), dict)
        else {}
    )
    events = collect_events()
    t2_selected_keys = {
        str(event.get("position_id") or event.get("signal_id") or "")
        for _, _, _, event_type, event in events
        if event_type == "tranche2_exit" and str(event.get("position_id") or event.get("signal_id") or "")
    }
    accepted = 0
    skipped = 0
    skipped_stale_entries = 0
    skipped_unmatched_exits = 0
    failed = 0
    pending_payloads: list[dict[str, Any]] = []
    for _, _, _, event_type, event in events:
        if event_type == "paper_entry":
            position = event.get("position") if isinstance(event.get("position"), dict) else {}
            position_key = str(position.get("position_id") or event.get("position_id") or position.get("signal_id") or event.get("signal_id") or "")
            if not position_key:
                skipped += 1
                continue
            if entry_event_is_stale(position, event):
                suppressed_stale_entries.add(position_key)
                posted.add(matrix_entry_event_id(position_key))
                skipped_stale_entries += 1
                continue
            entries[position_key] = {
                **position,
                "symbol": position.get("symbol") or event.get("symbol"),
                "model_version": position.get("model_version") or event.get("model_version"),
            }
            symbol = safe_symbol(entries[position_key].get("symbol") or event.get("symbol"))
            if symbol:
                active_by_symbol[symbol] = position_key
            payload = entry_payload(entries[position_key], received_event=event)
            payload["matrix_rebuild_event"] = True
            payload["suppress_notification"] = True
        elif event_type == "tranche2_exit":
            position_key = str(event.get("position_id") or event.get("signal_id") or "")
            if not position_key:
                skipped += 1
                continue
            if position_key in suppressed_stale_entries:
                skipped += 1
                continue
            entry = entries.get(position_key) or event
            if position_key not in entries:
                entries[position_key] = entry
            symbol = safe_symbol(event.get("symbol") or entry.get("symbol"))
            if symbol and active_by_symbol.get(symbol) not in {None, "", position_key}:
                posted.add(matrix_exit_event_id(position_key, event.get("exit_epoch"), "t2"))
                skipped_unmatched_exits += 1
                selected_exited.add(position_key)
                continue
            payload = exit_payload(event, entry, selected_type="t2")
            payload["matrix_rebuild_event"] = True
            payload["suppress_notification"] = True
            selected_exited.add(position_key)
            if symbol and active_by_symbol.get(symbol) == position_key:
                active_by_symbol.pop(symbol, None)
        elif event_type == "paper_exit":
            position_key = str(event.get("position_id") or event.get("signal_id") or "")
            if not position_key or position_key in selected_exited or position_key in t2_selected_keys:
                skipped += 1
                continue
            if position_key in suppressed_stale_entries:
                skipped += 1
                continue
            entry = entries.get(position_key) or event
            if position_key not in entries:
                entries[position_key] = entry
            symbol = safe_symbol(event.get("symbol") or entry.get("symbol"))
            if symbol and active_by_symbol.get(symbol) not in {None, "", position_key}:
                posted.add(matrix_exit_event_id(position_key, event.get("exit_epoch"), "base"))
                skipped_unmatched_exits += 1
                selected_exited.add(position_key)
                continue
            payload = exit_payload(event, entry, selected_type="base")
            payload["matrix_rebuild_event"] = True
            payload["suppress_notification"] = True
            selected_exited.add(position_key)
            if symbol and active_by_symbol.get(symbol) == position_key:
                active_by_symbol.pop(symbol, None)
        else:
            continue
        event_id = str(payload.get("event_id") or "")
        if not event_id or event_id in posted:
            skipped += 1
            continue
        pending_payloads.append(payload)
    if post_matrix_batch(pending_payloads, dry_run=dry_run):
        posted.update(str(payload.get("event_id") or "") for payload in pending_payloads)
        accepted += len(pending_payloads)
    else:
        failed += len(pending_payloads)
    bridge_state.update(
        {
            "schema": "matrix_v1.v2_bridge_state.v1",
            "updated_at_epoch": time.time(),
            "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "posted_event_ids": sorted(posted),
            "selected_exited_position_ids": sorted(selected_exited),
            "suppressed_stale_entry_ids": sorted(suppressed_stale_entries),
            "entries_by_position_id": entries,
            "active_position_by_symbol": dict(sorted(active_by_symbol.items())),
            "last_result": {
                "accepted": accepted,
                "skipped": skipped,
                "skipped_stale_entries": skipped_stale_entries,
                "skipped_unmatched_exits": skipped_unmatched_exits,
                "failed": failed,
            },
        }
    )
    if not dry_run:
        write_json(BRIDGE_STATE_PATH, bridge_state)
    return bridge_state["last_result"]


def iter_new_ledger_events(path: Path, offset: int) -> tuple[int, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    try:
        size = path.stat().st_size
        if size < offset:
            offset = 0
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(offset)
            for line in handle:
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    rows.append(item)
            return handle.tell(), rows
    except OSError:
        return offset, []


def sync_tail_once(*, dry_run: bool = False) -> dict[str, Any]:
    bridge_state = read_json(BRIDGE_STATE_PATH, {})
    if not isinstance(bridge_state, dict):
        bridge_state = {}
    posted = set(bridge_state.get("posted_event_ids") or [])
    selected_exited = set(bridge_state.get("selected_exited_position_ids") or [])
    suppressed_stale_entries = set(bridge_state.get("suppressed_stale_entry_ids") or [])
    entries: dict[str, dict[str, Any]] = bridge_state.get("entries_by_position_id") if isinstance(bridge_state.get("entries_by_position_id"), dict) else {}
    active_by_symbol: dict[str, str] = (
        bridge_state.get("active_position_by_symbol")
        if isinstance(bridge_state.get("active_position_by_symbol"), dict)
        else {}
    )
    offsets = bridge_state.get("ledger_offsets") if isinstance(bridge_state.get("ledger_offsets"), dict) else {}
    ledger_paths = sorted((V2_STATE_ROOT / "instruments").glob("*/ledger.jsonl"))

    if not offsets and posted:
        offsets = {str(path): path.stat().st_size for path in ledger_paths if path.exists()}
        result = {"accepted": 0, "skipped": 0, "failed": 0, "initialized_offsets": len(offsets)}
        bridge_state.update(
            {
                "schema": "matrix_v1.v2_bridge_state.v1",
                "updated_at_epoch": time.time(),
                "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "posted_event_ids": sorted(posted),
                "selected_exited_position_ids": sorted(selected_exited),
                "suppressed_stale_entry_ids": sorted(suppressed_stale_entries),
                "entries_by_position_id": entries,
                "active_position_by_symbol": dict(sorted(active_by_symbol.items())),
                "ledger_offsets": offsets,
                "last_result": result,
            }
        )
        if not dry_run:
            write_json(BRIDGE_STATE_PATH, bridge_state)
        return result

    accepted = 0
    skipped = 0
    failed = 0
    skipped_historical = 0
    skipped_stale_entries = 0
    skipped_unmatched_exits = 0
    offsets_changed = False
    pending_payloads: list[dict[str, Any]] = []
    window_start_epoch = live_tail_window_start_epoch()
    for path in ledger_paths:
        old_offset = int(offsets.get(str(path), 0) or 0)
        new_offset, events = iter_new_ledger_events(path, old_offset)
        if new_offset != old_offset:
            offsets_changed = True
        offsets[str(path)] = new_offset
        for event in events:
            event_type = str(event.get("event") or "")
            payload: dict[str, Any] | None = None
            event_epoch = ledger_event_epoch(event, event_type)
            if event_epoch is not None and event_epoch < window_start_epoch:
                skipped_historical += 1
                continue
            if event_type == "paper_entry":
                position = event.get("position") if isinstance(event.get("position"), dict) else {}
                position_key = str(position.get("position_id") or event.get("position_id") or position.get("signal_id") or event.get("signal_id") or "")
                if not position_key:
                    skipped += 1
                    continue
                if entry_event_is_stale(position, event):
                    suppressed_stale_entries.add(position_key)
                    posted.add(matrix_entry_event_id(position_key))
                    skipped_stale_entries += 1
                    continue
                entries[position_key] = {
                    **position,
                    "symbol": position.get("symbol") or event.get("symbol"),
                    "model_version": position.get("model_version") or event.get("model_version"),
                }
                symbol = safe_symbol(entries[position_key].get("symbol") or event.get("symbol"))
                if symbol:
                    active_by_symbol[symbol] = position_key
                payload = entry_payload(entries[position_key], received_event=event)
            elif event_type == "tranche2_exit":
                position_key = str(event.get("position_id") or event.get("signal_id") or "")
                if not position_key:
                    skipped += 1
                    continue
                if position_key in suppressed_stale_entries:
                    skipped += 1
                    continue
                entry = entries.get(position_key) or event
                entries.setdefault(position_key, entry)
                symbol = safe_symbol(event.get("symbol") or entry.get("symbol"))
                if symbol and active_by_symbol.get(symbol) not in {None, "", position_key}:
                    posted.add(matrix_exit_event_id(position_key, event.get("exit_epoch"), "t2"))
                    skipped_unmatched_exits += 1
                    selected_exited.add(position_key)
                    continue
                payload = exit_payload(event, entry, selected_type="t2")
                selected_exited.add(position_key)
                if symbol and active_by_symbol.get(symbol) == position_key:
                    active_by_symbol.pop(symbol, None)
            elif event_type == "paper_exit":
                position_key = str(event.get("position_id") or event.get("signal_id") or "")
                if not position_key or position_key in selected_exited:
                    skipped += 1
                    continue
                if position_key in suppressed_stale_entries:
                    skipped += 1
                    continue
                entry = entries.get(position_key) or event
                entries.setdefault(position_key, entry)
                symbol = safe_symbol(event.get("symbol") or entry.get("symbol"))
                if symbol and active_by_symbol.get(symbol) not in {None, "", position_key}:
                    posted.add(matrix_exit_event_id(position_key, event.get("exit_epoch"), "base"))
                    skipped_unmatched_exits += 1
                    selected_exited.add(position_key)
                    continue
                payload = exit_payload(event, entry, selected_type="base")
                selected_exited.add(position_key)
                if symbol and active_by_symbol.get(symbol) == position_key:
                    active_by_symbol.pop(symbol, None)
            else:
                skipped += 1
                continue
            event_id = str((payload or {}).get("event_id") or "")
            if not payload or not event_id or event_id in posted:
                skipped += 1
                continue
            pending_payloads.append(payload)

    if post_matrix_batch(pending_payloads, dry_run=dry_run):
        posted.update(str(payload.get("event_id") or "") for payload in pending_payloads)
        accepted += len(pending_payloads)
    else:
        failed += len(pending_payloads)

    result = {
        "accepted": accepted,
        "skipped": skipped,
        "failed": failed,
        "skipped_historical": skipped_historical,
        "skipped_stale_entries": skipped_stale_entries,
        "skipped_unmatched_exits": skipped_unmatched_exits,
        "tailed_files": len(ledger_paths),
    }
    state_dirty = bool(accepted or failed or skipped or skipped_historical or offsets_changed)
    if not state_dirty:
        result["state_write_skipped"] = True
        return result
    bridge_state.update(
        {
            "schema": "matrix_v1.v2_bridge_state.v1",
            "updated_at_epoch": time.time(),
            "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "posted_event_ids": sorted(posted),
            "selected_exited_position_ids": sorted(selected_exited),
            "suppressed_stale_entry_ids": sorted(suppressed_stale_entries),
            "entries_by_position_id": entries,
            "active_position_by_symbol": dict(sorted(active_by_symbol.items())),
            "ledger_offsets": offsets,
            "last_result": result,
        }
    )
    if not dry_run:
        write_json(BRIDGE_STATE_PATH, bridge_state)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=15.0)
    args = parser.parse_args()
    while True:
        started = time.time()
        result = sync_once(dry_run=args.dry_run) if args.once else sync_tail_once(dry_run=args.dry_run)
        result["elapsed_seconds"] = round(time.time() - started, 3)
        print(json.dumps({"event": "matrix_v2_bridge_sync", **result, "time": datetime.now(timezone.utc).isoformat()}), flush=True)
        if args.once:
            return 0 if result.get("failed", 0) == 0 else 1
        time.sleep(max(1.0, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
