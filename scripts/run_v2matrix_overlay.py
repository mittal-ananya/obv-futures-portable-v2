#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import json
import math
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import backtest_tranche_portfolio_overlay as base  # noqa: E402
import research_t2_portfolio_rules as portfolio_rules  # noqa: E402


IST = ZoneInfo("Asia/Kolkata")
SCHEMA = "obvfutport_v2.v2matrix_overlay_state.v1"
PRIMARY_VARIANT = "smooth_survivor_armed20_floor80"
PORTFOLIO_VARIANTS = ("smooth_survivor_armed20_floor80", "smooth_survivor_profit25")
FIXED_ENTRY_MARGIN = 500_000.0
MAX_PORTFOLIO_POSITIONS = 3
POLICY = portfolio_rules.Policy(
    name="smooth_survivor_tight_risk_score0p80_age60to240_runway0",
    formula="smooth_survivor",
    min_score=0.80,
    min_age_minutes=60.0,
    max_age_minutes=240.0,
    min_current_ret=0.0005,
    min_mfe=0.0015,
    max_mae_abs=0.0030,
    max_drawdown_from_mfe=None,
    max_drawdown_to_mfe=0.50,
    min_positive_ram_count=2,
    max_spread_bps=12.0,
    min_edge_cost_multiple=5.0,
    min_minutes_to_session_end=0.0,
    allow_replacement=False,
    min_hold_minutes=0,
    replacement_gap=0.0,
    replace_only_if_held_score_below=None,
    replace_only_if_held_ret_below=None,
    max_replacements_per_day=None,
)


@dataclass
class OverlayPosition:
    variant: str
    row_id: str
    position_id: str
    symbol: str
    side: str
    entry_epoch: int
    entry_time: str | None
    entry_fill_price: float
    entry_ltp_price: float
    entry_score: float
    entry_features: dict[str, Any]
    peak_return: float = 0.0
    armed: bool = False


@dataclass
class PortfolioHolding:
    overlay_key: str
    row_id: str
    position_id: str
    symbol: str
    side: str
    lots: int
    lot_size: int
    margin_locked: float
    entry_epoch: int
    entry_time: str | None
    entry_fill_price: float
    entry_ltp_price: float
    entry_score: float


class QuoteRingIndex:
    def __init__(self, retention_seconds: int = 28_800) -> None:
        self.retention_seconds = int(retention_seconds)
        self._raw: dict[str, dict[int, base.Quote]] = {}
        self._keys: dict[str, list[int]] = {}
        self._values: dict[str, list[base.Quote]] = {}
        self.earliest_epoch: int | None = None
        self.latest_epoch: int | None = None

    def add(self, key: str, epoch: float, price: float, bid: float | None, ask: float | None) -> None:
        if not key or price <= 0:
            return
        minute = base.minute_floor(epoch)
        quote = base.Quote(minute, float(epoch), float(price), bid, ask)
        by_minute = self._raw.setdefault(key, {})
        old = by_minute.get(minute)
        if old is None or quote.event_epoch >= old.event_epoch:
            by_minute[minute] = quote
        self.earliest_epoch = minute if self.earliest_epoch is None else min(self.earliest_epoch, minute)
        self.latest_epoch = minute if self.latest_epoch is None else max(self.latest_epoch, minute)

    def prune(self, now_epoch: int) -> None:
        cutoff = base.minute_floor(now_epoch - self.retention_seconds)
        for key in list(self._raw):
            rows = self._raw[key]
            for minute in list(rows):
                if minute < cutoff:
                    rows.pop(minute, None)
            if not rows:
                self._raw.pop(key, None)
        self.finalize()

    def finalize(self) -> None:
        self._keys = {}
        self._values = {}
        earliest: int | None = None
        latest: int | None = None
        for key, rows in self._raw.items():
            minutes = sorted(rows)
            self._keys[key] = minutes
            self._values[key] = [rows[minute] for minute in minutes]
            if minutes:
                earliest = minutes[0] if earliest is None else min(earliest, minutes[0])
                latest = minutes[-1] if latest is None else max(latest, minutes[-1])
        self.earliest_epoch = earliest
        self.latest_epoch = latest

    def quote_at_or_before(self, key: str, epoch: int | float, *, max_age_seconds: int | None = None) -> base.Quote | None:
        minutes = self._keys.get(key)
        if not minutes:
            return None
        target = base.minute_floor(epoch)
        idx = bisect.bisect_right(minutes, target) - 1
        if idx < 0:
            return None
        quote = self._values[key][idx]
        if max_age_seconds is not None and target - quote.minute_epoch > max_age_seconds:
            return None
        return quote

    def quote_by_trading_offset(self, key: str, epoch: int | float, offset_minutes: int) -> base.Quote | None:
        minutes = self._keys.get(key)
        if not minutes:
            return None
        target = base.minute_floor(epoch)
        idx = bisect.bisect_right(minutes, target) - 1 - int(offset_minutes)
        if idx < 0:
            return None
        return self._values[key][idx]

    def price_window(self, key: str, epoch: int | float, lookback_minutes: int) -> list[float]:
        minutes = self._keys.get(key)
        values = self._values.get(key)
        if not minutes or not values:
            return []
        target = base.minute_floor(epoch)
        idx = bisect.bisect_right(minutes, target) - 1
        start = idx - int(lookback_minutes)
        if idx < 0 or start < 0:
            return []
        return [quote.price for quote in values[start : idx + 1]]

    def key_count(self) -> int:
        return len(self._keys)

    def row_count(self) -> int:
        return sum(len(rows) for rows in self._values.values())


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def epoch_ist_iso(epoch: int | float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), IST).isoformat()


def session_clock(epoch: int | None = None) -> int:
    value = time.time() if epoch is None else float(epoch)
    return int(value // 60) * 60


def in_session(epoch: int) -> bool:
    current = datetime.fromtimestamp(epoch, IST)
    if current.weekday() >= 5:
        return False
    start = datetime.combine(current.date(), dt_time(9, 16), tzinfo=IST)
    end = datetime.combine(current.date(), dt_time(15, 30), tzinfo=IST)
    return int(start.timestamp()) <= epoch <= int(end.timestamp())


def json_clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_clean(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_clean(item) for item in value]
    if isinstance(value, tuple):
        return [json_clean(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        json.dump(json_clean(payload), handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")
        tmp_name = handle.name
    os.chmod(tmp_name, 0o644)
    os.replace(tmp_name, path)


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(json_clean(payload), ensure_ascii=True, sort_keys=True))
        handle.write("\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
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


def safe_float(value: Any) -> float | None:
    return base.as_float(value)


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "stale"}


def event_epoch(event: dict[str, Any]) -> int:
    event_type = str(event.get("event") or "")
    if event_type == "paper_entry":
        position = event.get("position") if isinstance(event.get("position"), dict) else {}
        return parse_epoch(position.get("entry_epoch") or event.get("entry_epoch") or event.get("event_epoch")) or 0
    if event_type in {"tranche2_exit", "tranche3_exit", "paper_exit"}:
        return parse_epoch(event.get("exit_epoch") or event.get("event_epoch")) or 0
    return parse_epoch(event.get("event_epoch")) or 0


def event_sort_rank(event: dict[str, Any]) -> int:
    event_type = str(event.get("event") or "")
    if event_type == "paper_entry":
        return 0
    if event_type == "tranche2_exit":
        return 1
    if event_type == "paper_exit":
        return 2
    return 3


def entry_is_stale(position: dict[str, Any], event: dict[str, Any], max_seconds: float) -> bool:
    if truthy(position.get("entry_stale")) or truthy(event.get("entry_stale")):
        return True
    lag = safe_float(
        position.get("entry_staleness_seconds")
        or event.get("entry_staleness_seconds")
        or position.get("fill_lag_seconds")
        or event.get("fill_lag_seconds")
    )
    return bool(lag is not None and lag > max_seconds)


def safe_state_name(value: Any) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(value or "").strip())


def selected_t2_exit_from_paper_exit(event: dict[str, Any]) -> dict[str, Any]:
    two_lot = event.get("two_lot_ttsl") if isinstance(event.get("two_lot_ttsl"), dict) else {}
    tranche2 = two_lot.get("tranche2") if isinstance(two_lot.get("tranche2"), dict) else {}
    if str(event.get("t2_status") or tranche2.get("status") or "").lower() != "closed":
        return event
    selected = dict(event)
    for key, candidates in {
        "exit_epoch": (tranche2.get("exit_epoch"), event.get("t2_exit_epoch"), event.get("tranche2_exit_epoch"), event.get("exit_epoch")),
        "exit_time": (tranche2.get("exit_time"), event.get("t2_exit_time"), event.get("tranche2_exit_time"), event.get("exit_time")),
        "exit_price": (tranche2.get("exit_price"), event.get("t2_exit_ltp_price"), event.get("tranche2_exit_ltp_price"), event.get("exit_price")),
        "exit_fill_price": (
            tranche2.get("exit_fill_price"),
            event.get("t2_exit_fill_price"),
            event.get("tranche2_exit_fill_price"),
            event.get("exit_fill_price"),
        ),
        "instrument_key": (tranche2.get("instrument_key"), event.get("instrument_key")),
        "exit_reason": (tranche2.get("exit_reason"), event.get("tranche2_exit_source"), event.get("t2_exit_source"), event.get("exit_reason")),
    }.items():
        for value in candidates:
            if value not in {None, ""}:
                selected[key] = value
                break
    return selected


def load_t2_legs(root: Path, max_entry_staleness_seconds: float) -> dict[str, base.TrancheLeg]:
    events: list[dict[str, Any]] = []
    for path in sorted((root / "state" / "instruments").glob("*/ledger.jsonl")):
        events.extend(read_jsonl(path))
    events.sort(key=lambda item: (event_epoch(item), event_sort_rank(item)))
    entries: dict[str, dict[str, Any]] = {}
    exits: dict[str, dict[str, Any]] = {}
    stale_entries: set[str] = set()
    for event in events:
        event_type = str(event.get("event") or "")
        if event_type == "paper_entry":
            position = event.get("position") if isinstance(event.get("position"), dict) else {}
            position_id = str(position.get("position_id") or event.get("position_id") or position.get("signal_id") or event.get("signal_id") or "")
            if not position_id:
                continue
            if entry_is_stale(position, event, max_entry_staleness_seconds):
                stale_entries.add(position_id)
                continue
            entries[position_id] = {**event, "position": position}
        elif event_type == "tranche2_exit":
            position_id = str(event.get("position_id") or event.get("signal_id") or "")
            if position_id and position_id not in exits:
                exits[position_id] = event
        elif event_type == "paper_exit":
            position_id = str(event.get("position_id") or event.get("signal_id") or "")
            if position_id and position_id not in exits:
                exits[position_id] = selected_t2_exit_from_paper_exit(event)

    manifest = base.load_contract_manifest(root)
    margins = base.load_margin_lookup(root)
    out: dict[str, base.TrancheLeg] = {}
    for position_id, event in entries.items():
        if position_id in stale_entries:
            continue
        position = event.get("position") if isinstance(event.get("position"), dict) else {}
        symbol = str(position.get("symbol") or event.get("symbol") or "").strip().upper()
        side = str(position.get("side") or event.get("side") or "").strip().lower()
        entry_epoch = parse_epoch(position.get("entry_epoch") or event.get("entry_epoch"))
        if not symbol or side not in {"long", "short"} or entry_epoch is None:
            continue
        exit_event = exits.get(position_id)
        exit_epoch = parse_epoch((exit_event or {}).get("exit_epoch"))
        row = {
            **position,
            "symbol": symbol,
            "tranche": "T2",
            "side": side,
            "position_id": position_id,
            "signal_id": position.get("signal_id") or event.get("signal_id") or position_id,
            "entry_epoch": entry_epoch,
            "entry_time": position.get("entry_time") or event.get("entry_time"),
            "exit_epoch": exit_epoch,
            "exit_time": (exit_event or {}).get("exit_time"),
            "status": "closed" if exit_epoch else "open",
            "signal_source": position.get("signal_source") or event.get("signal_source"),
            "signal_instrument_key": position.get("signal_instrument_key") or event.get("signal_instrument_key"),
            "instrument_key": position.get("instrument_key") or event.get("instrument_key"),
            "entry_fill_price": position.get("entry_fill_price") or event.get("entry_fill_price"),
            "exit_fill_price": (exit_event or {}).get("exit_fill_price") or (exit_event or {}).get("exit_price"),
            "exit_price": (exit_event or {}).get("exit_price"),
            "margin_rupees": position.get("margin_rupees") or event.get("margin_rupees"),
            "source_event": event,
            "exit_event": exit_event,
        }
        execution_key = str(row.get("instrument_key") or base.fallback_execution_key(manifest, symbol))
        signal_key = base.fallback_signal_key(manifest, row)
        margin = base.margin_for(row, margins)
        if not execution_key or not signal_key or margin is None or margin <= 0:
            continue
        row_id = f"{symbol}|T2|{position_id}|{entry_epoch}"
        out[row_id] = base.TrancheLeg(
            row_id=row_id,
            symbol=symbol,
            tranche="T2",
            side=side,
            entry_epoch=int(entry_epoch),
            exit_epoch=int(exit_epoch) if exit_epoch else None,
            position_id=position_id,
            signal_source=str(row.get("signal_source") or ""),
            signal_key=signal_key,
            execution_key=execution_key,
            entry_fill_price=safe_float(row.get("entry_fill_price")),
            exit_fill_price=safe_float(row.get("exit_fill_price")),
            margin_per_lot=float(margin),
            lot_size=base.lot_size_for(manifest, symbol, execution_key),
            source_row=row,
        )
    return out


def target_stream_path(root: Path, trade_date: date) -> Path:
    day = trade_date.isoformat()
    return root / "state" / "target_stream" / day / f"target_quotes_{day}.jsonl"


def update_quote_index(
    *,
    root: Path,
    state: dict[str, Any],
    index: QuoteRingIndex,
    required_keys: set[str],
    initial_stream_mode: str,
) -> dict[str, Any]:
    from obvfut_portable_v2.passive_runner import row_from_target_stream_line  # type: ignore

    today = now_ist().date()
    path = target_stream_path(root, today)
    stream_offsets = state.setdefault("stream_offsets", {})
    path_key = str(path)
    if not path.exists():
        return {"path": path_key, "exists": False, "rows": 0, "offset": int(stream_offsets.get(path_key, 0) or 0)}
    size = path.stat().st_size
    offset_raw = stream_offsets.get(path_key)
    if offset_raw is None:
        current_time = now_ist().time()
        early_session_start = current_time <= dt_time(9, 30)
        offset = 0 if initial_stream_mode == "beginning" or early_session_start else size
    else:
        offset = int(offset_raw or 0)
        if offset > size:
            offset = 0
    rows = 0
    kept = 0
    with path.open("rb") as handle:
        handle.seek(offset)
        for raw_line in handle:
            if not raw_line.strip():
                continue
            rows += 1
            row = row_from_target_stream_line(raw_line, today.isoformat(), required_keys)
            if row is None:
                continue
            price = safe_float(row.get("price"))
            epoch = safe_float(row.get("epoch"))
            key = str(row.get("target") or "")
            if price is None or epoch is None or not key:
                continue
            index.add(key, epoch, price, safe_float(row.get("bid")), safe_float(row.get("ask")))
            kept += 1
        offset = handle.tell()
    stream_offsets[path_key] = offset
    index.prune(session_clock())
    return {"path": path_key, "exists": True, "rows": rows, "kept": kept, "size": size, "offset": offset}


def hydrate_quote_index_from_state(index: QuoteRingIndex, state: dict[str, Any]) -> None:
    payload = state.get("quote_ring")
    if not isinstance(payload, dict):
        return
    for key, rows in payload.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue
            minute, event_epoch_value, price, bid, ask = row[:5]
            if (price_value := safe_float(price)) is None:
                continue
            index.add(str(key), safe_float(event_epoch_value) or float(minute), price_value, safe_float(bid), safe_float(ask))
    index.finalize()


def quote_index_to_state(index: QuoteRingIndex) -> dict[str, list[list[Any]]]:
    out: dict[str, list[list[Any]]] = {}
    for key, values in index._values.items():
        out[key] = [[q.minute_epoch, q.event_epoch, q.price, q.bid, q.ask] for q in values[-520:]]
    return out


def execution_fill(index: QuoteRingIndex, v1_portfolio: Any, leg: base.TrancheLeg, epoch: int, *, phase: str) -> dict[str, Any] | None:
    quote = index.quote_at_or_before(leg.execution_key, epoch, max_age_seconds=300)
    if quote is None:
        return None
    row = {"price": quote.price, "bid": quote.bid, "ask": quote.ask, "epoch_second": quote.minute_epoch}
    fill = v1_portfolio.execution_fill_from_row(row, side=leg.side, phase=phase, point_config=None)
    return fill if safe_float(fill.get("fill_price")) is not None else None


def accounting(v1_portfolio: Any, leg: base.TrancheLeg, entry_fill: float, exit_fill: float, lots: int) -> dict[str, Any]:
    return v1_portfolio.futures_trade_accounting(
        side=leg.side,
        entry_fill_price=float(entry_fill),
        exit_fill_price=float(exit_fill),
        lot_size=int(leg.lot_size or 1),
        lots=int(lots or 1),
        point_config=None,
    )


def directional_return(index: QuoteRingIndex, leg: base.TrancheLeg, clock_epoch: int, lookback_minutes: int) -> float | None:
    ref_epoch = clock_epoch - 60
    end = index.quote_by_trading_offset(leg.signal_key, ref_epoch, 0)
    start = index.quote_by_trading_offset(leg.signal_key, ref_epoch, lookback_minutes)
    if end is None or start is None or start.price <= 0:
        return None
    return base.signed_direction(leg.side) * ((end.price / start.price) - 1.0)


def risk_adjusted_momentum(index: QuoteRingIndex, leg: base.TrancheLeg, clock_epoch: int, lookback_minutes: int, risk_floor: float) -> float | None:
    ref_epoch = clock_epoch - 60
    window = index.price_window(leg.signal_key, ref_epoch, lookback_minutes)
    if len(window) < lookback_minutes:
        return None
    returns = [(cur / prev) - 1.0 for prev, cur in zip(window, window[1:]) if prev > 0]
    if len(returns) < max(2, lookback_minutes // 2):
        return None
    direction = directional_return(index, leg, clock_epoch, lookback_minutes)
    if direction is None:
        return None
    realized_risk = sample_std(returns) * math.sqrt(max(1, len(returns)))
    return direction / max(risk_floor, realized_risk)


def sample_std(values: list[float]) -> float:
    """Population standard deviation used by the historical portfolio scorer."""
    if len(values) < 2:
        return 0.0
    helper = getattr(portfolio_rules, "sample_std", None)
    if helper is not None:
        return float(helper(values))
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def calc_ram_bundle(index: QuoteRingIndex, leg: base.TrancheLeg, clock_epoch: int, risk_floor: float) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for lookback in (10, 30, 60):
        out[f"ram_{lookback}"] = risk_adjusted_momentum(index, leg, clock_epoch, lookback, risk_floor)
        out[f"ret_{lookback}"] = directional_return(index, leg, clock_epoch, lookback)
    return out


def estimate_edge(index: QuoteRingIndex, v1_portfolio: Any, leg: base.TrancheLeg, clock_epoch: int, feature: dict[str, Any]) -> dict[str, Any]:
    rets = [safe_float(feature.get("ret_10")), safe_float(feature.get("ret_30")), safe_float(feature.get("ret_60"))]
    if any(value is None for value in rets):
        return {"ok": False, "reason": "missing_directional_return"}
    edge_return = max(0.0, 0.35 * rets[0] + 0.25 * rets[1] + 0.40 * rets[2])  # type: ignore[operator]
    if edge_return <= 0:
        return {"ok": False, "reason": "non_positive_edge_return", "edge_return": edge_return}
    fill = execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")
    if fill is None:
        return {"ok": False, "reason": "missing_entry_fill_for_edge"}
    entry_fill = float(fill["fill_price"])
    projected_exit = entry_fill * (1.0 + edge_return) if leg.side == "long" else entry_fill * (1.0 - edge_return)
    acct = accounting(v1_portfolio, leg, entry_fill, projected_exit, 1)
    gross = abs(float(acct.get("gross_rupees") or 0.0))
    charges = float(acct.get("charges_rupees") or 0.0)
    net = float(acct.get("net_rupees") or 0.0)
    return {
        "ok": net > 0 and gross > 0,
        "reason": "ok" if net > 0 and gross > 0 else "non_positive_projected_net",
        "edge_return": edge_return,
        "gross_edge_rupees_per_lot": gross,
        "estimated_charges_rupees_per_lot": charges,
        "estimated_net_edge_rupees_per_lot": net,
        "edge_to_cost_multiple": (gross / charges) if charges > 0 else None,
    }


def load_leg_runtime(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = state.get("leg_runtime")
    return value if isinstance(value, dict) else {}


def build_features(
    *,
    active_legs: list[base.TrancheLeg],
    index: QuoteRingIndex,
    v1_portfolio: Any,
    clock_epoch: int,
    state: dict[str, Any],
    risk_floor: float,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    runtime = load_leg_runtime(state)
    stats = {
        "active_legs": len(active_legs),
        "missing_entry_ref": 0,
        "missing_ram": 0,
        "missing_execution_ref": 0,
        "missing_edge": 0,
        "incomplete_history": 0,
    }
    features: list[dict[str, Any]] = []
    ref_epoch = clock_epoch - 60
    session_end = datetime.combine(datetime.fromtimestamp(clock_epoch, IST).date(), dt_time(15, 30), tzinfo=IST)
    for leg in active_legs:
        row = runtime.setdefault(leg.row_id, {})
        if not row.get("entry_ref_price"):
            row["entry_ref_price"] = leg.entry_fill_price
        if not row.get("entry_ref_price"):
            quote = index.quote_at_or_before(leg.execution_key, leg.entry_epoch, max_age_seconds=300)
            row["entry_ref_price"] = quote.price if quote is not None else None
        entry_ref = safe_float(row.get("entry_ref_price"))
        if entry_ref is None or entry_ref <= 0:
            stats["missing_entry_ref"] += 1
            continue
        earliest_needed = leg.entry_epoch - 60
        if index.earliest_epoch is None or index.earliest_epoch > earliest_needed:
            row["history_incomplete"] = True
        if row.get("history_incomplete"):
            stats["incomplete_history"] += 1
            continue
        ram = calc_ram_bundle(index, leg, clock_epoch, risk_floor)
        if ram.get("ram_10") is None or ram.get("ram_60") is None:
            stats["missing_ram"] += 1
            continue
        exec_quote = index.quote_at_or_before(leg.execution_key, ref_epoch, max_age_seconds=300)
        entry_quote = index.quote_at_or_before(leg.execution_key, clock_epoch, max_age_seconds=300)
        if exec_quote is None or entry_quote is None:
            stats["missing_execution_ref"] += 1
            continue
        current_ret = base.signed_direction(leg.side) * ((exec_quote.price / entry_ref) - 1.0)
        previous_mfe = float(row.get("mfe") or 0.0)
        mfe = max(previous_mfe, current_ret)
        mae = min(float(row.get("mae") or 0.0), current_ret)
        if current_ret >= previous_mfe:
            row["mfe_epoch"] = exec_quote.minute_epoch
        row["mfe"] = mfe
        row["mae"] = mae
        row["current_ret"] = current_ret
        mae_abs = max(0.0, -mae)
        drawdown = max(0.0, mfe - current_ret)
        minutes_since_mfe = ((ref_epoch - int(row.get("mfe_epoch"))) / 60.0) if row.get("mfe_epoch") else 0.0
        spread_bps = None
        if entry_quote.bid is not None and entry_quote.ask is not None and entry_quote.price > 0:
            spread_bps = max(0.0, (float(entry_quote.ask) - float(entry_quote.bid)) / entry_quote.price * 10000.0)
        edge = estimate_edge(index, v1_portfolio, leg, clock_epoch, {**ram})
        if not edge.get("ok"):
            stats["missing_edge"] += 1
        features.append(
            {
                "row_id": leg.row_id,
                "symbol": leg.symbol,
                "side": leg.side,
                "entry_epoch": leg.entry_epoch,
                "exit_epoch": leg.exit_epoch,
                "clock_epoch": clock_epoch,
                "age_minutes": max(0.0, (clock_epoch - leg.entry_epoch) / 60.0),
                "minutes_to_session_end": (int(session_end.timestamp()) - clock_epoch) / 60.0,
                "current_ret": current_ret,
                "mfe": mfe,
                "mae": mae,
                "mae_abs": mae_abs,
                "drawdown_from_mfe": drawdown,
                "drawdown_to_mfe": (drawdown / max(mfe, risk_floor)) if mfe > 0 else 1.0,
                "path_quality": current_ret - drawdown - (0.5 * mae_abs),
                "minutes_since_mfe": minutes_since_mfe,
                "spread_bps": spread_bps,
                "edge_return": edge.get("edge_return"),
                "edge_to_cost_multiple": edge.get("edge_to_cost_multiple"),
                "edge_diagnostics": edge,
                **ram,
            }
        )
    if features:
        portfolio_rules.add_ranks(features)
    out: dict[str, dict[str, Any]] = {}
    for feature in features:
        score = portfolio_rules.blended_score(feature, POLICY.formula)
        feature["portfolio_score"] = score
        out[str(feature["row_id"])] = feature
    state["leg_runtime"] = runtime
    return out, stats


def candidate_passes(feature: dict[str, Any]) -> bool:
    return portfolio_rules.candidate_passes_policy(feature, POLICY)


def variant_config(variant: str) -> dict[str, Any]:
    if variant == "smooth_survivor_armed20_floor80":
        return {"name": "armed20bps_floor80pct_peak", "kind": "armed_peak_floor", "arm_target": 0.0020, "floor_fraction": 0.80}
    if variant == "smooth_survivor_profit25":
        return {"name": "profit_25bps", "kind": "profit", "target": 0.0025}
    raise ValueError(f"unknown overlay variant {variant!r}")


def overlay_key(variant: str, row_id: str) -> str:
    return f"{variant}|{row_id}"


def dict_to_overlay_position(payload: dict[str, Any]) -> OverlayPosition:
    return OverlayPosition(**{field: payload[field] for field in OverlayPosition.__dataclass_fields__ if field in payload})


def dict_to_portfolio_holding(payload: dict[str, Any]) -> PortfolioHolding:
    return PortfolioHolding(**{field: payload[field] for field in PortfolioHolding.__dataclass_fields__ if field in payload})


def entry_display_reference(position: OverlayPosition) -> tuple[float, str, str | None]:
    features = position.entry_features if isinstance(position.entry_features, dict) else {}
    price = safe_float(features.get("entry_display_price_underlying"))
    source = str(features.get("entry_display_price_source") or "").strip()
    key = str(features.get("entry_display_instrument_key") or "").strip()
    if price is not None:
        return float(price), source or "v2matrix_overlay_signal_stream", key or None
    return float(position.entry_ltp_price), "v2matrix_overlay_entry_ltp_fallback", None


def overlay_return(index: QuoteRingIndex, v1_portfolio: Any, leg: base.TrancheLeg, position: OverlayPosition, clock_epoch: int) -> tuple[float | None, float | None, dict[str, Any] | None]:
    fill = execution_fill(index, v1_portfolio, leg, clock_epoch, phase="exit")
    if fill is None:
        return None, None, None
    exit_fill = safe_float(fill.get("fill_price"))
    if exit_fill is None:
        return None, None, fill
    direction = base.signed_direction(leg.side)
    forward_return = direction * ((exit_fill / position.entry_fill_price) - 1.0)
    return forward_return, exit_fill, fill


def exit_price_from_return(entry_fill_price: float, side: str, forward_return: float) -> float:
    direction = base.signed_direction(side)
    return float(entry_fill_price) * (1.0 + (float(forward_return) / direction))


def is_session_close_clock(clock_epoch: int) -> bool:
    current = datetime.fromtimestamp(int(clock_epoch), IST).time()
    return current >= dt_time(15, 30)


def should_exit_overlay(
    *,
    variant: str,
    leg: base.TrancheLeg,
    position: OverlayPosition,
    index: QuoteRingIndex,
    v1_portfolio: Any,
    clock_epoch: int,
) -> tuple[bool, str, float | None, float | None, dict[str, Any] | None]:
    ret, exit_fill, fill = overlay_return(index, v1_portfolio, leg, position, clock_epoch)
    config = variant_config(variant)
    if ret is not None and config["kind"] == "profit":
        if ret >= float(config["target"]):
            return True, "profit_capture", ret, exit_fill, fill
    elif ret is not None and config["kind"] == "armed_peak_floor":
        position.peak_return = max(position.peak_return, ret)
        if not position.armed and ret >= float(config["arm_target"]):
            position.armed = True
        if position.armed:
            floor = max(0.0, position.peak_return * float(config["floor_fraction"]))
            if ret <= floor:
                return True, "armed_peak_floor", floor, exit_price_from_return(position.entry_fill_price, leg.side, floor), fill
            if is_session_close_clock(clock_epoch):
                return True, "armed_session_close", ret, exit_fill, fill
    if leg.exit_epoch is not None and leg.exit_epoch <= clock_epoch:
        if ret is not None and exit_fill is not None:
            return True, "underlying_t2_exit", ret, exit_fill, fill
        if leg.exit_fill_price is not None:
            direction = base.signed_direction(leg.side)
            fallback_ret = direction * ((float(leg.exit_fill_price) / position.entry_fill_price) - 1.0)
            return True, "underlying_t2_exit", fallback_ret, float(leg.exit_fill_price), None
        return True, "underlying_t2_exit", ret, exit_fill, fill
    if ret is None:
        return False, "missing_exit_quote", None, None, fill
    return False, "", ret, exit_fill, fill


def matrix_payload(
    *,
    event_type: str,
    variant: str,
    leg: base.TrancheLeg,
    position: OverlayPosition,
    event_epoch_value: int,
    trigger_price: float,
    trigger_source: str,
    features: dict[str, Any] | None,
    exit_reason: str | None = None,
    position_closed: bool = False,
) -> dict[str, Any]:
    regime_side = "long" if leg.side == "long" else "short"
    entry_display_price, entry_display_source, entry_display_key = entry_display_reference(position)
    return {
        "event_id": f"V2MATRIX:{variant}:{event_type}:{leg.position_id}:{event_epoch_value}",
        "source_strategy": "OBVFUTPORT_V2_T2_SMOOTH_SURVIVOR",
        "source_model_version": "v2matrix_overlay_v1",
        "instrument_id": leg.symbol,
        "instrument_name": leg.symbol,
        "event_type": event_type,
        "side": regime_side,
        "tranche": "T2_OVERLAY",
        "position_closed": position_closed,
        "trigger_time_ist": epoch_ist_iso(event_epoch_value),
        "trigger_price_underlying": trigger_price,
        "trigger_price_source": trigger_source,
        "trigger_price_instrument_key": leg.signal_key,
        "current_price_underlying": trigger_price,
        "current_time_ist": epoch_ist_iso(event_epoch_value),
        "current_price_source": trigger_source,
        "signal_source": leg.signal_source,
        "signal_instrument_key": leg.signal_key,
        "execution_instrument_key": leg.execution_key,
        "display_price_source": "cash_underlying_or_configured_signal_source",
        "execution_price_source": "v2_futures_execution_contract",
        "matrix_selected_leg": "T2_smooth_survivor_overlay",
        "matrix_selection_rule": variant,
        "overlay_variant": variant,
        "overlay_policy": POLICY.name,
        "overlay_score": position.entry_score if features is None else features.get("portfolio_score"),
        "overlay_row_id": leg.row_id,
        "position_id": f"{variant}:{leg.position_id}",
        "signal_id": leg.position_id,
        "source_t2_position_id": leg.position_id,
        "matrix_entry_time_ist": position.entry_time,
        "matrix_entry_price_underlying": entry_display_price,
        "matrix_entry_price_source": entry_display_source,
        "matrix_entry_instrument_key": entry_display_key,
        "exit_reason": exit_reason,
        "created_at_ist": now_ist().isoformat(),
    }


def post_matrix_events(url: str, payloads: list[dict[str, Any]], dry_run: bool) -> tuple[int, int]:
    if not payloads:
        return 0, 0
    if dry_run:
        for payload in payloads:
            print(json.dumps(payload, sort_keys=True))
        return len(payloads), 0
    data = json.dumps({"events": payloads}, ensure_ascii=True, sort_keys=True).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            if 200 <= response.status < 300:
                return len(payloads), 0
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"v2matrix_post_failed count={len(payloads)} error={exc}", flush=True)
    return 0, len(payloads)


def portfolio_key(variant: str) -> str:
    return f"fixed5L_no_replacement_max3_{variant}"


def portfolio_summary(portfolio: dict[str, Any], index: QuoteRingIndex, v1_portfolio: Any, legs: dict[str, base.TrancheLeg], clock_epoch: int) -> dict[str, Any]:
    holdings = {
        key: dict_to_portfolio_holding(value)
        for key, value in (portfolio.get("holdings") or {}).items()
        if isinstance(value, dict)
    }
    transactions = [row for row in (portfolio.get("transactions") or []) if isinstance(row, dict)]
    realized = sum(float(row.get("net_rupees") or 0.0) for row in transactions if row.get("event") == "exit")
    unrealized = 0.0
    for holding in holdings.values():
        leg = legs.get(holding.row_id)
        if leg is None:
            continue
        fill = execution_fill(index, v1_portfolio, leg, clock_epoch, phase="exit")
        if fill is None:
            continue
        acct = accounting(v1_portfolio, leg, holding.entry_fill_price, float(fill["fill_price"]), holding.lots)
        unrealized += float(acct.get("net_rupees") or 0.0)
    exits = [row for row in transactions if row.get("event") == "exit"]
    wins = [row for row in exits if float(row.get("net_rupees") or 0.0) > 0]
    peak_margin = max(float(portfolio.get("peak_margin_rupees") or 0.0), sum(h.margin_locked for h in holdings.values()))
    portfolio_success = (len(wins) / len(exits) * 100.0) if exits else None
    return {
        "portfolio_id": portfolio.get("portfolio_id"),
        "variant": portfolio.get("variant"),
        "rule": portfolio.get("rule"),
        "max_positions": MAX_PORTFOLIO_POSITIONS,
        "fixed_entry_margin_rupees": FIXED_ENTRY_MARGIN,
        "open_positions": len(holdings),
        "closed_trades": len(exits),
        "wins": len(wins),
        "losses": len(exits) - len(wins),
        "success_rate_pct": portfolio_success,
        "portfolio_closed_success_rate_pct": portfolio_success,
        "portfolio_closed_trade_count": len(exits),
        "portfolio_closed_win_count": len(wins),
        "all_qualified_signal_success_rate_pct": portfolio.get("all_qualified_signal_success_rate_pct"),
        "all_qualified_signal_trade_count": portfolio.get("all_qualified_signal_trade_count"),
        "all_qualified_signal_win_count": portfolio.get("all_qualified_signal_win_count"),
        "all_qualified_signal_reference": portfolio.get("all_qualified_signal_reference"),
        "realized_net_rupees": realized,
        "unrealized_net_rupees": unrealized,
        "total_net_rupees": realized + unrealized,
        "current_margin_rupees": sum(h.margin_locked for h in holdings.values()),
        "peak_margin_rupees": peak_margin,
        "return_on_peak_margin_pct": (realized / peak_margin * 100.0) if peak_margin else None,
    }


def close_portfolio_holding(
    *,
    portfolio: dict[str, Any],
    overlay_key_value: str,
    exit_epoch: int,
    exit_fill_price: float,
    exit_reason: str,
    leg: base.TrancheLeg,
    v1_portfolio: Any,
) -> None:
    holdings = portfolio.setdefault("holdings", {})
    raw = holdings.pop(overlay_key_value, None)
    if not isinstance(raw, dict):
        return
    holding = dict_to_portfolio_holding(raw)
    acct = accounting(v1_portfolio, leg, holding.entry_fill_price, exit_fill_price, holding.lots)
    pnl = float(acct.get("net_rupees") or 0.0)
    portfolio["cash_rupees"] = float(portfolio.get("cash_rupees") or 0.0) + holding.margin_locked + pnl
    tx = {
        "event": "exit",
        "portfolio_id": portfolio["portfolio_id"],
        "variant": portfolio["variant"],
        "symbol": holding.symbol,
        "side": holding.side,
        "source_t2_position_id": holding.position_id,
        "overlay_key": overlay_key_value,
        "lots": holding.lots,
        "lot_size": holding.lot_size,
        "entry_epoch": holding.entry_epoch,
        "entry_time": holding.entry_time,
        "exit_epoch": exit_epoch,
        "exit_time": epoch_ist_iso(exit_epoch),
        "exit_reason": exit_reason,
        "entry_score": holding.entry_score,
        "entry_fill_price": holding.entry_fill_price,
        "exit_fill_price": exit_fill_price,
        "margin_locked": holding.margin_locked,
        "gross_rupees": acct.get("gross_rupees"),
        "charges_rupees": acct.get("charges_rupees"),
        "net_rupees": pnl,
        "net_pct_margin": (pnl / holding.margin_locked * 100.0) if holding.margin_locked else None,
    }
    portfolio.setdefault("transactions", []).append(tx)
    portfolio["last_event_at_ist"] = epoch_ist_iso(exit_epoch)


def open_portfolio_holding(*, portfolio: dict[str, Any], variant: str, position: OverlayPosition, leg: base.TrancheLeg) -> bool:
    holdings = portfolio.setdefault("holdings", {})
    if len(holdings) >= MAX_PORTFOLIO_POSITIONS:
        portfolio.setdefault("diagnostics", {}).setdefault("skipped_slot_full", 0)
        portfolio["diagnostics"]["skipped_slot_full"] += 1
        return False
    if any(isinstance(item, dict) and item.get("symbol") == leg.symbol for item in holdings.values()):
        portfolio.setdefault("diagnostics", {}).setdefault("skipped_symbol_already_held", 0)
        portfolio["diagnostics"]["skipped_symbol_already_held"] += 1
        return False
    lots = int(math.floor(FIXED_ENTRY_MARGIN / float(leg.margin_per_lot))) if leg.margin_per_lot > 0 else 0
    if lots <= 0:
        portfolio.setdefault("diagnostics", {}).setdefault("skipped_margin_too_large", 0)
        portfolio["diagnostics"]["skipped_margin_too_large"] += 1
        return False
    key = overlay_key(variant, leg.row_id)
    margin_locked = float(leg.margin_per_lot) * lots
    portfolio["cash_rupees"] = float(portfolio.get("cash_rupees") or 0.0) - margin_locked
    holding = PortfolioHolding(
        overlay_key=key,
        row_id=leg.row_id,
        position_id=leg.position_id,
        symbol=leg.symbol,
        side=leg.side,
        lots=lots,
        lot_size=int(leg.lot_size or 1),
        margin_locked=margin_locked,
        entry_epoch=position.entry_epoch,
        entry_time=position.entry_time,
        entry_fill_price=position.entry_fill_price,
        entry_ltp_price=position.entry_ltp_price,
        entry_score=position.entry_score,
    )
    holdings[key] = asdict(holding)
    portfolio["peak_margin_rupees"] = max(float(portfolio.get("peak_margin_rupees") or 0.0), sum(float(item.get("margin_locked") or 0.0) for item in holdings.values() if isinstance(item, dict)))
    portfolio.setdefault("transactions", []).append(
        {
            "event": "entry",
            "portfolio_id": portfolio["portfolio_id"],
            "variant": variant,
            "symbol": leg.symbol,
            "side": leg.side,
            "source_t2_position_id": leg.position_id,
            "overlay_key": key,
            "lots": lots,
            "lot_size": int(leg.lot_size or 1),
            "entry_epoch": position.entry_epoch,
            "entry_time": position.entry_time,
            "entry_score": position.entry_score,
            "entry_fill_price": position.entry_fill_price,
            "margin_locked": margin_locked,
        }
    )
    portfolio["last_event_at_ist"] = position.entry_time
    return True


def ensure_portfolios(state: dict[str, Any]) -> dict[str, Any]:
    portfolios = state.setdefault("portfolios", {})
    for variant in PORTFOLIO_VARIANTS:
        key = portfolio_key(variant)
        portfolios.setdefault(
            key,
            {
                "portfolio_id": key,
                "variant": variant,
                "rule": f"fixed Rs 5L per entry / no replacement / max {MAX_PORTFOLIO_POSITIONS}",
                "cash_rupees": 2_000_000.0,
                "peak_margin_rupees": 0.0,
                "holdings": {},
                "transactions": [],
                "diagnostics": {},
            },
        )
    return portfolios


def run_clock(
    *,
    root: Path,
    overlay_root: Path,
    state: dict[str, Any],
    index: QuoteRingIndex,
    v1_portfolio: Any,
    matrix_events_url: str,
    dry_run: bool,
    risk_floor: float,
    max_entry_staleness_seconds: float,
) -> dict[str, Any]:
    clock_epoch = session_clock()
    legs = load_t2_legs(root, max_entry_staleness_seconds)
    active_legs = [leg for leg in legs.values() if leg.entry_epoch <= clock_epoch and (leg.exit_epoch is None or leg.exit_epoch > clock_epoch)]
    required_keys = {leg.signal_key for leg in active_legs} | {leg.execution_key for leg in active_legs}
    features, feature_stats = build_features(
        active_legs=active_legs,
        index=index,
        v1_portfolio=v1_portfolio,
        clock_epoch=clock_epoch,
        state=state,
        risk_floor=risk_floor,
    )
    active_overlay = state.setdefault("active_overlay", {})
    completed_overlay = set(state.setdefault("completed_overlay_keys", []))
    posted_event_ids = set(state.setdefault("posted_event_ids", []))
    overlay_events_path = overlay_root / "state" / "overlay_events.jsonl"
    posted_payloads: list[dict[str, Any]] = []
    created_events: list[dict[str, Any]] = []
    portfolios = ensure_portfolios(state)

    # Exits before new entries, matching the portfolio research lifecycle.
    for key, payload in list(active_overlay.items()):
        if not isinstance(payload, dict):
            active_overlay.pop(key, None)
            continue
        position = dict_to_overlay_position(payload)
        leg = legs.get(position.row_id)
        if leg is None:
            continue
        should_exit, exit_reason, ret, exit_fill, fill = should_exit_overlay(
            variant=position.variant,
            leg=leg,
            position=position,
            index=index,
            v1_portfolio=v1_portfolio,
            clock_epoch=clock_epoch,
        )
        active_overlay[key] = asdict(position)
        if not should_exit or exit_fill is None:
            continue
        active_overlay.pop(key, None)
        completed_overlay.add(key)
        event = {
            "schema": SCHEMA,
            "event": "overlay_exit",
            "overlay_key": key,
            "variant": position.variant,
            "policy": POLICY.name,
            "source_t2_position_id": leg.position_id,
            "row_id": leg.row_id,
            "symbol": leg.symbol,
            "side": leg.side,
            "exit_epoch": clock_epoch,
            "exit_time": epoch_ist_iso(clock_epoch),
            "exit_reason": exit_reason,
            "exit_return": ret,
            "entry_epoch": position.entry_epoch,
            "entry_time": position.entry_time,
            "entry_fill_price": position.entry_fill_price,
            "exit_fill_price": exit_fill,
            "exit_ltp_price": safe_float((fill or {}).get("ltp_price")) or exit_fill,
            "created_at_ist": now_ist().isoformat(),
        }
        append_jsonl(overlay_events_path, event)
        created_events.append(event)
        for portfolio in portfolios.values():
            if isinstance(portfolio, dict) and portfolio.get("variant") == position.variant:
                close_portfolio_holding(
                    portfolio=portfolio,
                    overlay_key_value=key,
                    exit_epoch=clock_epoch,
                    exit_fill_price=exit_fill,
                    exit_reason=exit_reason,
                    leg=leg,
                    v1_portfolio=v1_portfolio,
                )
        if position.variant == PRIMARY_VARIANT:
            signal_quote = index.quote_at_or_before(leg.signal_key, clock_epoch, max_age_seconds=300)
            trigger_price = signal_quote.price if signal_quote else event["exit_ltp_price"]
            posted_payloads.append(
                matrix_payload(
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

    eligible = [feature for feature in features.values() if candidate_passes(feature)]
    eligible.sort(key=lambda row: (float(row.get("portfolio_score") or -1.0), str(row.get("symbol") or ""), str(row.get("row_id") or "")), reverse=True)
    for feature in eligible:
        row_id = str(feature["row_id"])
        leg = legs.get(row_id)
        if leg is None:
            continue
        for variant in PORTFOLIO_VARIANTS:
            key = overlay_key(variant, row_id)
            if key in active_overlay or key in completed_overlay:
                continue
            fill = execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")
            if fill is None:
                continue
            entry_fill = safe_float(fill.get("fill_price"))
            entry_ltp = safe_float(fill.get("ltp_price")) or entry_fill
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
            position = OverlayPosition(
                variant=variant,
                row_id=row_id,
                position_id=leg.position_id,
                symbol=leg.symbol,
                side=leg.side,
                entry_epoch=clock_epoch,
                entry_time=epoch_ist_iso(clock_epoch),
                entry_fill_price=float(entry_fill),
                entry_ltp_price=float(entry_ltp),
                entry_score=float(feature.get("portfolio_score") or 0.0),
                entry_features=entry_features,
            )
            active_overlay[key] = asdict(position)
            event = {
                "schema": SCHEMA,
                "event": "overlay_entry",
                "overlay_key": key,
                "variant": variant,
                "policy": POLICY.name,
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
                "created_at_ist": now_ist().isoformat(),
            }
            append_jsonl(overlay_events_path, event)
            created_events.append(event)
            portfolio = portfolios.get(portfolio_key(variant))
            if isinstance(portfolio, dict):
                open_portfolio_holding(portfolio=portfolio, variant=variant, position=position, leg=leg)
            if variant == PRIMARY_VARIANT:
                posted_payloads.append(
                    matrix_payload(
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

    pending = [payload for payload in posted_payloads if str(payload.get("event_id") or "") not in posted_event_ids]
    posted, failed = post_matrix_events(matrix_events_url, pending, dry_run)
    for payload in pending[:posted]:
        posted_event_ids.add(str(payload.get("event_id") or ""))
    state["posted_event_ids"] = sorted(posted_event_ids)
    state["completed_overlay_keys"] = sorted(completed_overlay)
    summaries = []
    for portfolio in portfolios.values():
        if isinstance(portfolio, dict):
            summaries.append(portfolio_summary(portfolio, index, v1_portfolio, legs, clock_epoch))
    state["portfolio_summaries"] = summaries
    state["last_clock"] = {
        "clock_epoch": clock_epoch,
        "clock_time_ist": epoch_ist_iso(clock_epoch),
        "in_session": in_session(clock_epoch),
        "loaded_t2_legs": len(legs),
        "active_t2_legs": len(active_legs),
        "required_key_count": len(required_keys),
        "feature_stats": feature_stats,
        "eligible_count": len(eligible),
        "created_events": len(created_events),
        "matrix_posted": posted,
        "matrix_failed": failed,
        "active_overlay_count": len(active_overlay),
        "quote_keys": index.key_count(),
        "quote_rows": index.row_count(),
    }
    return state["last_clock"]


def write_portfolio_files(overlay_root: Path, state: dict[str, Any]) -> None:
    state_dir = overlay_root / "state"
    portfolios = ensure_portfolios(state)
    write_json(
        state_dir / "portfolio_state.json",
        {
            "schema": "obvfutport_v2.v2matrix_portfolios.v1",
            "updated_at_ist": now_ist().isoformat(),
            "definition": {
                "source": "canonical OBVFUTPORT-v2 T2 ledgers plus quote-valid target stream",
                "sizing": "fixed Rs 5L max margin per entry, multi-lot, no cash constraint",
                "max_positions": MAX_PORTFOLIO_POSITIONS,
                "replacement": "none",
            },
            "summaries": state.get("portfolio_summaries") or [],
            "portfolios": portfolios,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--overlay-root", type=Path, default=Path("/opt/cloud-deploy-candidates/v2matrix"))
    parser.add_argument("--matrix-events-url", default="http://127.0.0.1:8098/api/v2matrix/v1/events")
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--risk-floor", type=float, default=0.0005)
    parser.add_argument("--max-entry-staleness-seconds", type=float, default=5.0)
    parser.add_argument("--quote-retention-seconds", type=int, default=28_800)
    parser.add_argument("--initial-stream-mode", choices=["tail", "beginning"], default="tail")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base.add_paths(args.root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    state_path = args.overlay_root / "state" / "overlay_state.json"
    status_path = args.overlay_root / "state" / "overlay_status.json"
    state = read_json(state_path, {})
    if not isinstance(state, dict):
        state = {}
    state.setdefault("schema", SCHEMA)
    state.setdefault("created_at_ist", now_ist().isoformat())
    state.setdefault("primary_variant", PRIMARY_VARIANT)
    state.setdefault("portfolio_variants", list(PORTFOLIO_VARIANTS))
    index = QuoteRingIndex(args.quote_retention_seconds)
    hydrate_quote_index_from_state(index, state)
    consecutive_failures = 0

    while True:
        started = time.monotonic()
        status: dict[str, Any]
        try:
            clock_epoch = session_clock()
            session_active = in_session(clock_epoch)
            if session_active or args.once:
                active_legs_for_keys = [
                    leg
                    for leg in load_t2_legs(args.root, args.max_entry_staleness_seconds).values()
                    if leg.entry_epoch <= clock_epoch and (leg.exit_epoch is None or leg.exit_epoch > clock_epoch)
                ]
                required_keys = {leg.signal_key for leg in active_legs_for_keys} | {leg.execution_key for leg in active_legs_for_keys}
                stream_status = update_quote_index(
                    root=args.root,
                    state=state,
                    index=index,
                    required_keys=required_keys,
                    initial_stream_mode=args.initial_stream_mode,
                )
                clock_status = run_clock(
                    root=args.root,
                    overlay_root=args.overlay_root,
                    state=state,
                    index=index,
                    v1_portfolio=v1_portfolio,
                    matrix_events_url=args.matrix_events_url,
                    dry_run=args.dry_run,
                    risk_floor=args.risk_floor,
                    max_entry_staleness_seconds=args.max_entry_staleness_seconds,
                )
            else:
                stream_status = {"skipped": True, "reason": "outside_market_session_idle"}
                clock_status = {"clock_epoch": clock_epoch, "clock_time_ist": epoch_ist_iso(clock_epoch), "in_session": False}
            state["quote_ring"] = quote_index_to_state(index)
            state["updated_at_ist"] = now_ist().isoformat()
            write_portfolio_files(args.overlay_root, state)
            if not args.dry_run:
                write_json(state_path, state)
            status = {
                "ok": True,
                "schema": "obvfutport_v2.v2matrix_overlay_status.v1",
                "updated_at_ist": now_ist().isoformat(),
                "stream": stream_status,
                "clock": clock_status,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
            consecutive_failures = 0
        except Exception as exc:  # noqa: BLE001
            consecutive_failures += 1
            status = {
                "ok": False,
                "schema": "obvfutport_v2.v2matrix_overlay_status.v1",
                "updated_at_ist": now_ist().isoformat(),
                "error": repr(exc),
                "consecutive_failures": consecutive_failures,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
            print(json.dumps(status, sort_keys=True), flush=True)
            if args.once:
                write_json(status_path, status)
                return 1
        write_json(status_path, status)
        print(json.dumps(status, sort_keys=True), flush=True)
        if args.once:
            return 0 if status.get("ok") else 1
        sleep_seconds = max(1.0, float(args.interval_seconds))
        if not in_session(session_clock()):
            sleep_seconds = max(sleep_seconds, 60.0)
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
