from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import tempfile
import time as time_mod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse


IST = ZoneInfo("Asia/Kolkata")
PACKAGE_ROOT = Path(os.environ.get("PACKAGE_ROOT", "/opt/cloud-deploy-candidates/v2matrix"))
CONFIG_PATH = PACKAGE_ROOT / "config" / "runtime.json"
STATE_ROOT = PACKAGE_ROOT / "state"
STATE_PATH = STATE_ROOT / "matrix_state.json"
EVENTS_PATH = STATE_ROOT / "matrix_events.jsonl"
PORTABLE_STATE_ROOT = Path(
    os.environ.get("MATRIX_PORTFOLIO_STATE_ROOT", "/opt/cloud-deploy-candidates/obv-futures-portable-v2/state")
)
LATEST_TICKS_PATH = Path(
    os.environ.get(
        "MATRIX_LATEST_TICKS_PATH",
        "/opt/cloud-deploy-candidates/intraday-short-straddle-v1/state/market_data/latest_ticks.json",
    )
)

REGIME_BULLISH = "Bullish"
REGIME_BEARISH = "Bearish"
REGIME_NEUTRAL = "Neutral"
ENTRY_EVENT_TYPES = {"long_trigger", "short_trigger", "long_entry", "short_entry", "paper_entry"}
EXIT_EVENT_TYPES = {"tranche2_exit", "tranche1_exit", "base_exit", "full_exit", "paper_exit", "flat", "neutral"}
NOTIFICATION_EVENT_TYPES = ENTRY_EVENT_TYPES | {"tranche2_exit", "tranche1_exit", "base_exit", "full_exit", "paper_exit"}
MAX_NOTIFICATION_RECEIVED_AGE_SECONDS = 10 * 60
MAX_NOTIFICATION_TRIGGER_AGE_SECONDS = 20 * 60
INDEX_SPOT_KEYS = {
    "BANKNIFTY": "NSE:NIFTY BANK",
    "NIFTY": "NSE:NIFTY 50",
}
_PORTABLE_UNIVERSE_CACHE: tuple[float, list[dict[str, Any]]] | None = None

app = FastAPI(title="v2Matrix")


@app.middleware("http")
async def matrix_html_no_cache(request: Request, call_next):
    response = await call_next(request)
    if request.url.path in {"/v2Matrix", "/v2Matrix/", "/v2Matrix/v1", "/v2Matrix/v2", "/v2Matrix/notifications"}:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def notification_window_start(reference: datetime | None = None) -> datetime:
    """v2Matrix notifications reset at 06:00 IST, not midnight."""
    current = reference or now_ist()
    six_am = current.replace(hour=6, minute=0, second=0, microsecond=0)
    if current < six_am:
        six_am -= timedelta(days=1)
    return six_am


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
        return value if value == value and value not in {float("inf"), float("-inf")} else None
    return value


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        json.dump(json_clean(payload), handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")
        tmp_name = handle.name
    os.replace(tmp_name, path)


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(json_clean(payload), ensure_ascii=True, sort_keys=True))
        handle.write("\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def load_config() -> dict[str, Any]:
    data = read_json(CONFIG_PATH, {})
    return data if isinstance(data, dict) else {}


def portable_runtime_config() -> dict[str, Any]:
    data = read_json(PORTABLE_STATE_ROOT.parent / "config" / "runtime.json", {})
    return data if isinstance(data, dict) else {}


def portable_universe_entries() -> list[dict[str, Any]]:
    global _PORTABLE_UNIVERSE_CACHE
    config = portable_runtime_config()
    manifest = str(config.get("hurst_universe_manifest_path") or "").strip()
    if not manifest:
        return []
    path = Path(manifest)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return []
    if _PORTABLE_UNIVERSE_CACHE and _PORTABLE_UNIVERSE_CACHE[0] == mtime:
        return _PORTABLE_UNIVERSE_CACHE[1]
    data = read_json(path, {})
    entries = data.get("entries") if isinstance(data, dict) else data
    if not isinstance(entries, list):
        entries = []
    clean_entries = [entry for entry in entries if isinstance(entry, dict) and str(entry.get("symbol") or "").strip()]
    _PORTABLE_UNIVERSE_CACHE = (mtime, clean_entries)
    return clean_entries


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(IST)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)
    return parsed.astimezone(IST)


def age_label(trigger_time: str | None, asof_time: str | None = None) -> str | None:
    parsed = parse_time(trigger_time)
    if parsed is None:
        return None
    asof = parse_time(asof_time) or now_ist()
    seconds = max(0, int((asof - parsed).total_seconds()))
    minutes = seconds // 60
    if minutes <= 10:
        return "Now"
    if minutes < 60:
        bucket = max(10, (minutes // 5) * 5)
        return f"{bucket} mins ago"
    hours = minutes // 60
    if hours < 24:
        return "1 hour ago" if hours == 1 else f"{hours} hours ago"
    days = hours // 24
    return "1 day ago" if days == 1 else f"{days} days ago"


def date_label(trigger_time: str | None) -> str | None:
    parsed = parse_time(trigger_time)
    return parsed.strftime("%d %b %Y") if parsed else None


def time_label(trigger_time: str | None) -> str | None:
    parsed = parse_time(trigger_time)
    return parsed.strftime("%H:%M") if parsed else None


def datetime_label(trigger_time: str | None) -> str | None:
    parsed = parse_time(trigger_time)
    if parsed is None:
        return None
    hour = parsed.strftime("%I").lstrip("0") or "0"
    return f"{parsed.strftime('%d %b %Y')}, {hour}:{parsed.strftime('%M')} {parsed.strftime('%p').lower()}"


def datetime_label_24(trigger_time: str | None) -> str | None:
    parsed = parse_time(trigger_time)
    if parsed is None:
        return None
    return f"{parsed.strftime('%d %b %Y')}, {parsed.strftime('%H:%M:%S')} IST"


def notification_card_from_event(event: dict[str, Any]) -> dict[str, Any] | None:
    if event.get("suppress_notification") or event.get("matrix_rebuild_event") or event.get("matrix_state_ignored"):
        return None
    event_type = str(event.get("event_type") or "").lower()
    if event_type not in NOTIFICATION_EVENT_TYPES:
        return None
    received_at = parse_time(event.get("received_at_ist") or event.get("created_at_ist"))
    window_start = notification_window_start()
    if received_at is None or received_at < window_start:
        return None
    trigger_at = parse_time(event.get("trigger_time_ist") or event.get("current_time_ist") or event.get("created_at_ist"))
    if trigger_at is None or trigger_at < window_start:
        return None
    if (received_at - trigger_at).total_seconds() > MAX_NOTIFICATION_TRIGGER_AGE_SECONDS:
        return None
    symbol = str(event.get("instrument_name") or event.get("instrument_id") or "Unknown").strip()
    side = str(event.get("side") or "").lower()
    if event_type in ENTRY_EVENT_TYPES:
        from_regime = REGIME_NEUTRAL
        to_regime = transition_regime_for_side(side)
    else:
        from_regime = transition_regime_for_side(side)
        to_regime = REGIME_NEUTRAL
    price = first_float(event, ("trigger_price_underlying", "current_price_underlying", "matrix_entry_price_underlying"))
    event_id = str(event.get("event_id") or "").strip()
    if not event_id:
        event_id = "|".join(
            [
                symbol,
                event_type,
                received_at.isoformat(),
                str(price or ""),
            ]
        )
    return {
        "id": event_id,
        "symbol": symbol,
        "from_regime": from_regime,
        "to_regime": to_regime,
        "received_at_ist": received_at.isoformat(),
        "received_label": datetime_label_24(received_at.isoformat()),
        "price": price,
    }


def today_notification_events() -> list[dict[str, Any]]:
    cards = [card for event in read_jsonl(EVENTS_PATH) if (card := notification_card_from_event(event))]
    cards.sort(key=lambda item: parse_time(item.get("received_at_ist")) or datetime.min.replace(tzinfo=IST), reverse=True)
    return cards


def direction_sign(*, regime: Any = None, side: Any = None) -> int:
    side_text = str(side or "").lower()
    if side_text == "short":
        return -1
    if side_text == "long":
        return 1
    regime_text = str(regime or "").lower()
    if regime_text == "bearish":
        return -1
    return 1


def directional_return_pct(entry_price: float | None, mark_price: float | None, *, regime: Any = None, side: Any = None) -> float | None:
    if entry_price is None or mark_price is None or entry_price <= 0:
        return None
    return direction_sign(regime=regime, side=side) * ((mark_price - entry_price) / entry_price) * 100.0


def move_pct(row: dict[str, Any]) -> float | None:
    current = as_float(row.get("current_price_underlying"))
    anchor = as_float(row.get("trigger_price_underlying"))
    return directional_return_pct(anchor, current, regime=row.get("regime"), side=row.get("side"))


def first_float(payload: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = as_float(payload.get(key))
        if value is not None and value > 0:
            return value
    return None


def safe_state_name(value: str) -> str:
    return "".join(character if character.isalnum() or character in {"_", "-"} else "_" for character in value)


def transition_regime_for_side(side: Any) -> str:
    side_text = str(side or "").lower()
    if side_text == "long":
        return REGIME_BULLISH
    if side_text == "short":
        return REGIME_BEARISH
    return REGIME_NEUTRAL


def transition_detail(
    *,
    time_ist: Any,
    from_regime: str,
    to_regime: str,
    price: float | None,
    move: float | None = None,
) -> dict[str, Any]:
    parsed_time = parse_time(time_ist)
    iso_time = parsed_time.isoformat() if parsed_time else None
    return {
        "time_ist": iso_time,
        "datetime": datetime_label(iso_time),
        "date": date_label(iso_time),
        "time": time_label(iso_time),
        "from_regime": from_regime,
        "to_regime": to_regime,
        "transition": f"{from_regime.upper()} -> {to_regime.upper()}",
        "price": price,
        "move_pct": move,
    }


def last_closed_trade_for_instrument(instrument_id: Any) -> dict[str, Any]:
    # v2Matrix is an overlay view; do not backfill neutral detail from the base v2 T2 ledger.
    return {}


def neutral_expanded_rows(row: dict[str, Any]) -> tuple[list[dict[str, Any]], float | None]:
    side_regime = str(row.get("last_trade_event_entry_to_regime") or row.get("last_trade_event_from_regime") or "")
    if side_regime in {REGIME_BULLISH, REGIME_BEARISH}:
        entry_time = row.get("last_trade_event_entry_time_ist")
        exit_time = row.get("last_trade_event_time_ist") or row.get("trigger_time_ist")
        entry_price = first_float(row, ("last_trade_event_entry_price_underlying",))
        exit_price = first_float(row, ("last_trade_event_price_underlying", "trigger_price_underlying"))
        move = directional_return_pct(entry_price, exit_price, regime=side_regime, side=row.get("side"))
        if entry_time and exit_time and entry_price is not None and exit_price is not None:
            return [
                transition_detail(
                    time_ist=exit_time,
                    from_regime=side_regime,
                    to_regime=REGIME_NEUTRAL,
                    price=exit_price,
                    move=move,
                ),
                transition_detail(
                    time_ist=entry_time,
                    from_regime=REGIME_NEUTRAL,
                    to_regime=side_regime,
                    price=entry_price,
                    move=move,
                ),
            ], move

    trade = last_closed_trade_for_instrument(row.get("instrument_id"))
    if not trade:
        return [], None
    entry_decision = trade.get("entry_decision") if isinstance(trade.get("entry_decision"), dict) else {}
    side_regime = transition_regime_for_side(trade.get("side"))
    if side_regime == REGIME_NEUTRAL:
        return [], None

    entry_time = trade.get("signal_time") or entry_decision.get("signal_time") or trade.get("entry_time")
    exit_time = trade.get("exit_time") or trade.get("created_at_ist")
    entry_price = first_float(trade, ("matrix_entry_price_underlying",))
    entry_source = str(trade.get("matrix_entry_price_source") or "")
    signal_source = str(trade.get("signal_source") or "").lower()
    if entry_price is None and "unavailable" not in entry_source and signal_source == "cash":
        entry_price = first_float(trade, ("signal_price",)) or first_float(entry_decision, ("signal_price",))
    if entry_price is None and "unavailable" not in entry_source and signal_source != "futures":
        entry_price = first_float(trade, ("entry_price",))
    exit_price = first_float(
        trade,
        (
            "matrix_trigger_price_underlying",
            "signal_exit_price",
            "transition_reference_price",
            "exit_price",
        ),
    )
    move = directional_return_pct(entry_price, exit_price, regime=side_regime, side=trade.get("side"))
    return [
        transition_detail(
            time_ist=exit_time,
            from_regime=side_regime,
            to_regime=REGIME_NEUTRAL,
            price=exit_price,
            move=move,
        ),
        transition_detail(
            time_ist=entry_time,
            from_regime=REGIME_NEUTRAL,
            to_regime=side_regime,
            price=entry_price,
            move=move,
        ),
    ], move


def is_loopback(value: str | None) -> bool:
    if not value:
        return True
    host = value.split(",", 1)[0].strip()
    return host in {"127.0.0.1", "::1", "localhost"}


def verify_webhook_auth(request: Request, body: bytes) -> None:
    config = load_config()
    secret = str(os.environ.get("MATRIX_WEBHOOK_SECRET") or config.get("webhook_secret") or "").strip()
    if secret:
        signature = request.headers.get("x-matrix-signature", "")
        expected = "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=401, detail="invalid webhook signature")
        return

    client_host = request.client.host if request.client else None
    forwarded_host = request.headers.get("x-real-ip") or request.headers.get("x-forwarded-for")
    if not is_loopback(client_host) or not is_loopback(forwarded_host):
        raise HTTPException(status_code=403, detail="webhook secret not configured; external posts disabled")


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def event_id(payload: dict[str, Any]) -> str:
    raw = payload.get("event_id")
    if raw:
        return str(raw)
    basis = {
        "source": payload.get("source_strategy") or payload.get("strategy_id"),
        "instrument_id": payload.get("instrument_id") or payload.get("instrument"),
        "event_type": payload.get("event_type"),
        "trigger_time_ist": payload.get("trigger_time_ist") or payload.get("event_time_ist"),
        "side": payload.get("side"),
    }
    text = json.dumps(json_clean(basis), sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def infer_regime(payload: dict[str, Any], previous: dict[str, Any] | None) -> str:
    event_type = str(payload.get("event_type") or "").lower()
    side = str(payload.get("side") or payload.get("position_side") or "").lower()
    explicit = str(payload.get("regime") or "").strip().lower()
    if explicit in {"bullish", "bearish", "neutral"}:
        return explicit.title()
    if event_type in {"long_trigger", "long_entry", "paper_entry"} and side == "long":
        return REGIME_BULLISH
    if event_type in {"short_trigger", "short_entry", "paper_entry"} and side == "short":
        return REGIME_BEARISH
    if event_type in {"tranche2_exit", "tranche1_exit", "base_exit", "full_exit", "paper_exit", "flat", "neutral"}:
        return REGIME_NEUTRAL
    if event_type == "state_snapshot":
        previous_trade_type = str((previous or {}).get("last_trade_event_type") or "").lower()
        previous_trade_to = str((previous or {}).get("last_trade_event_to_regime") or "")
        if previous_trade_to == REGIME_NEUTRAL and previous_trade_type in {
            "tranche2_exit",
            "tranche1_exit",
            "base_exit",
            "full_exit",
            "paper_exit",
            "flat",
            "neutral",
        }:
            return REGIME_NEUTRAL
        position_state = str(payload.get("position_state") or "").lower()
        if position_state in {"flat", "none", "neutral"}:
            return REGIME_NEUTRAL
        if position_state == "long_open" or side == "long":
            return REGIME_BULLISH
        if position_state == "short_open" or side == "short":
            return REGIME_BEARISH
    return str((previous or {}).get("regime") or REGIME_NEUTRAL)


def fallback_transition_from(regime: str, side: Any) -> str:
    if regime in {REGIME_BULLISH, REGIME_BEARISH}:
        return REGIME_NEUTRAL
    side_text = str(side or "").lower()
    if side_text == "long":
        return REGIME_BULLISH
    if side_text == "short":
        return REGIME_BEARISH
    return REGIME_NEUTRAL


def trigger_time_from_payload(payload: dict[str, Any], regime: str, previous: dict[str, Any] | None) -> str | None:
    event_type = str(payload.get("event_type") or "").lower()
    if event_type == "state_snapshot":
        candidate = (
            payload.get("trigger_time_ist")
            or payload.get("signal_time")
            or payload.get("exit_time")
        )
    else:
        candidate = (
            payload.get("trigger_time_ist")
            or payload.get("signal_time")
            or payload.get("event_time_ist")
            or payload.get("exit_time")
            or payload.get("created_at_ist")
        )
    parsed = parse_time(candidate)
    if parsed:
        return parsed.isoformat()
    if event_type == "state_snapshot":
        return None
    if regime == REGIME_NEUTRAL and previous:
        return previous.get("trigger_time_ist")
    return None


def trigger_price_from_payload(payload: dict[str, Any], previous: dict[str, Any] | None) -> float | None:
    price_source = str(payload.get("trigger_price_source") or "").lower()
    if payload.get("trigger_price_missing_reason") or "unavailable" in price_source:
        return None
    for key in (
        "trigger_price_underlying",
        "signal_price",
        "signal_exit_price",
        "transition_reference_price",
        "exit_price",
        "price",
    ):
        value = as_float(payload.get(key))
        if value is not None and value > 0:
            return value
    if previous:
        previous_price = as_float(previous.get("trigger_price_underlying"))
        return previous_price if previous_price is not None and previous_price > 0 else None
    return None


def payload_has_matrix_entry_reference(payload: dict[str, Any]) -> bool:
    return any(
        key in payload
        for key in (
            "matrix_entry_time_ist",
            "matrix_entry_price_underlying",
            "matrix_entry_price_source",
            "matrix_entry_instrument_key",
        )
    )


def trade_entry_reference_fields(
    payload: dict[str, Any],
    previous: dict[str, Any],
    previous_regime: str,
) -> dict[str, Any]:
    if payload_has_matrix_entry_reference(payload):
        parsed_time = parse_time(payload.get("matrix_entry_time_ist"))
        return {
            "last_trade_event_entry_time_ist": parsed_time.isoformat() if parsed_time else None,
            "last_trade_event_entry_price_underlying": as_float(payload.get("matrix_entry_price_underlying")),
            "last_trade_event_entry_price_source": payload.get("matrix_entry_price_source"),
            "last_trade_event_entry_from_regime": previous.get("transition_from_regime") or REGIME_NEUTRAL,
            "last_trade_event_entry_to_regime": previous_regime,
        }
    return {
        "last_trade_event_entry_time_ist": previous.get("trigger_time_ist"),
        "last_trade_event_entry_price_underlying": previous.get("trigger_price_underlying"),
        "last_trade_event_entry_price_source": previous.get("trigger_price_source"),
        "last_trade_event_entry_from_regime": previous.get("transition_from_regime") or REGIME_NEUTRAL,
        "last_trade_event_entry_to_regime": previous_regime,
    }


def current_price_from_payload(payload: dict[str, Any], previous: dict[str, Any] | None) -> float | None:
    for key in (
        "current_price_underlying",
        "current_price",
        "latest_price_underlying",
        "latest_price",
        "mark_price",
        "ltp_price",
    ):
        value = as_float(payload.get(key))
        if value is not None and value > 0:
            return value
    if previous:
        previous_price = as_float(previous.get("current_price_underlying"))
        return previous_price if previous_price is not None and previous_price > 0 else None
    return None


def current_time_from_payload(payload: dict[str, Any], previous: dict[str, Any] | None) -> str | None:
    candidate = (
        payload.get("current_time_ist")
        or payload.get("latest_time_ist")
        or payload.get("latest_time")
        or payload.get("mark_time_ist")
    )
    parsed = parse_time(candidate)
    if parsed:
        return parsed.isoformat()
    if previous:
        return previous.get("current_time_ist")
    return None


def latest_tick_context_for_row(row: dict[str, Any], latest_ticks: dict[str, Any]) -> dict[str, Any]:
    ticks = latest_ticks.get("ticks") if isinstance(latest_ticks.get("ticks"), dict) else {}
    if not ticks:
        return {}

    def top_depth(tick: dict[str, Any]) -> tuple[float | None, float | None]:
        depth = tick.get("depth") if isinstance(tick.get("depth"), dict) else {}
        buy = depth.get("buy") if isinstance(depth.get("buy"), list) else []
        sell = depth.get("sell") if isinstance(depth.get("sell"), list) else []
        bid = as_float((buy[0] or {}).get("price")) if buy else None
        ask = as_float((sell[0] or {}).get("price")) if sell else None
        return bid, ask

    def tick_market_time(tick: dict[str, Any]) -> str | None:
        parsed = (
            parse_time(tick.get("last_trade_time"))
            or parse_time(tick.get("exchange_timestamp"))
            or parse_time(tick.get("received_at_ist"))
            or parse_time(latest_ticks.get("updated_at_ist"))
        )
        return parsed.isoformat() if parsed else None

    candidates: list[str] = []
    source = str(row.get("trigger_price_source") or row.get("signal_source") or "").lower()
    instrument_id = str(row.get("instrument_id") or "").upper()
    signal_key = str(row.get("signal_instrument_key") or "").strip()
    wants_cash_reference = source.startswith("cash_stock")
    wants_index_reference = source.startswith("index_spot")

    trigger_key = str(row.get("trigger_price_instrument_key") or "").strip()
    if trigger_key:
        candidates.append(trigger_key)
    if wants_index_reference:
        mapped = INDEX_SPOT_KEYS.get(instrument_id)
        if mapped:
            candidates.append(mapped)
        elif signal_key.startswith("NFO:BANKNIFTY"):
            candidates.append("NSE:NIFTY BANK")
        elif signal_key.startswith("NFO:NIFTY"):
            candidates.append("NSE:NIFTY 50")
    if wants_cash_reference and signal_key:
        candidates.append(signal_key)
    if signal_key:
        candidates.append(signal_key)
    execution_key = str(row.get("execution_instrument_key") or "").strip()
    if execution_key and not wants_cash_reference and not wants_index_reference:
        candidates.append(execution_key)

    seen: set[str] = set()
    for key in candidates:
        normalised = key.upper()
        if not normalised or normalised in seen:
            continue
        seen.add(normalised)
        tick = ticks.get(normalised)
        if not isinstance(tick, dict):
            continue
        price = as_float(tick.get("last_price")) or as_float(tick.get("price"))
        if price is None or price <= 0:
            continue
        return {
            "current_price_underlying": price,
            "current_time_ist": tick_market_time(tick),
            "current_price_source": "latest_ticks",
            "current_price_instrument_key": normalised,
        }
    return {}


def response_updated_at(state_updated_at: Any, latest_updated_at: Any) -> str | None:
    state_time = parse_time(state_updated_at)
    latest_time = parse_time(latest_updated_at)
    if state_time and latest_time:
        return max(state_time, latest_time).isoformat()
    if latest_time:
        return latest_time.isoformat()
    if state_time:
        return state_time.isoformat()
    return None


def matrix_instruments_with_universe_placeholders(state: dict[str, Any]) -> dict[str, Any]:
    raw = state.get("instruments") or {}
    instruments = dict(raw) if isinstance(raw, dict) else {}
    existing: set[str] = set()
    for key, item in instruments.items():
        existing.add(str(key or "").upper())
        existing.add(safe_state_name(str(key or "")).upper())
        if isinstance(item, dict):
            instrument_id = str(item.get("instrument_id") or "").strip()
            instrument_name = str(item.get("instrument_name") or "").strip()
            if instrument_id:
                existing.add(instrument_id.upper())
                existing.add(safe_state_name(instrument_id).upper())
            if instrument_name:
                existing.add(instrument_name.upper())
                existing.add(safe_state_name(instrument_name).upper())

    for entry in portable_universe_entries():
        symbol = str(entry.get("symbol") or "").strip()
        if not symbol:
            continue
        if symbol.upper() in existing or safe_state_name(symbol).upper() in existing:
            continue
        cash_key = str(entry.get("cash_key") or "").strip()
        price_source = "index_spot_latest_ticks" if symbol.upper() in INDEX_SPOT_KEYS else "cash_stock_latest_ticks"
        instruments[symbol] = {
            "instrument_id": symbol,
            "instrument_name": symbol,
            "regime": REGIME_NEUTRAL,
            "side": None,
            "signal_source": "index_spot" if symbol.upper() in INDEX_SPOT_KEYS else "cash",
            "signal_instrument_key": cash_key,
            "trigger_price_source": price_source,
            "trigger_time_ist": None,
            "trigger_price_underlying": None,
            "transition_from_regime": REGIME_NEUTRAL,
            "transition_to_regime": REGIME_NEUTRAL,
            "placeholder_reason": "no_v2matrix_overlay_event_yet",
            "margin_long": entry.get("margin_long"),
            "margin_short": entry.get("margin_short"),
            "lot_size": entry.get("lot_size"),
        }
        existing.add(symbol.upper())
        existing.add(safe_state_name(symbol).upper())
    return instruments


def suppress_stale_notification_fields(row: dict[str, Any], updated_at: Any) -> dict[str, Any]:
    event_type = str(
        row.get("last_trade_event_type")
        or row.get("trigger_event_type")
        or ((row.get("last_event") or {}).get("event_type") if isinstance(row.get("last_event"), dict) else "")
        or ""
    ).lower()
    if event_type not in NOTIFICATION_EVENT_TYPES:
        return row
    reference_time = parse_time(updated_at) or now_ist()
    last_event = row.get("last_event") if isinstance(row.get("last_event"), dict) else {}
    received_time = parse_time(last_event.get("received_at_ist") or row.get("last_webhook_at_ist") or row.get("event_created_at_ist"))
    trigger_time = parse_time(row.get("last_trade_event_time_ist") or row.get("trigger_time_ist"))
    received_stale = received_time is None or (reference_time - received_time).total_seconds() > MAX_NOTIFICATION_RECEIVED_AGE_SECONDS
    trigger_stale = trigger_time is None or (reference_time - trigger_time).total_seconds() > MAX_NOTIFICATION_TRIGGER_AGE_SECONDS
    if not received_stale and not trigger_stale:
        return row
    suppressed_type = f"historical_{event_type}"
    row["notification_suppressed"] = True
    row["notification_suppressed_reason"] = "stale_historical_event"
    row["notification_original_last_trade_event_type"] = row.get("last_trade_event_type")
    row["last_trade_event_type"] = suppressed_type
    if row.get("trigger_event_type") == event_type:
        row["trigger_event_type"] = suppressed_type
    if isinstance(last_event, dict):
        patched = dict(last_event)
        if str(patched.get("event_type") or "").lower() == event_type:
            patched["event_type"] = suppressed_type
        row["last_event"] = patched
    return row


def state_payload_for_response(state: dict[str, Any]) -> dict[str, Any]:
    latest_ticks = read_json(LATEST_TICKS_PATH, {})
    if not isinstance(latest_ticks, dict):
        latest_ticks = {}
    updated_at = response_updated_at(state.get("updated_at_ist"), latest_ticks.get("updated_at_ist"))
    instruments = []
    for item in matrix_instruments_with_universe_placeholders(state).values():
        row = dict(item)
        row.update(latest_tick_context_for_row(row, latest_ticks))
        row = suppress_stale_notification_fields(row, updated_at)
        row["trigger_age"] = age_label(row.get("trigger_time_ist"), updated_at)
        row["trigger_date"] = date_label(row.get("trigger_time_ist"))
        row["trigger_time"] = time_label(row.get("trigger_time_ist"))
        row["trigger_datetime"] = datetime_label(row.get("trigger_time_ist"))
        row["current_datetime"] = datetime_label(row.get("current_time_ist"))
        row["move_pct"] = move_pct(row)
        row["transition_to_regime"] = row.get("transition_to_regime") or row.get("regime") or REGIME_NEUTRAL
        row["transition_from_regime"] = row.get("transition_from_regime") or fallback_transition_from(
            str(row.get("transition_to_regime") or row.get("regime") or REGIME_NEUTRAL),
            row.get("side"),
        )
        if row.get("regime") == REGIME_NEUTRAL:
            expanded_rows, neutral_move = neutral_expanded_rows(row)
            row["expanded_rows"] = expanded_rows
            row["neutral_move_pct"] = neutral_move
        else:
            row["expanded_rows"] = [
                transition_detail(
                    time_ist=row.get("trigger_time_ist"),
                    from_regime=str(row.get("transition_from_regime") or REGIME_NEUTRAL),
                    to_regime=str(row.get("transition_to_regime") or row.get("regime") or REGIME_NEUTRAL),
                    price=as_float(row.get("trigger_price_underlying")),
                    move=row.get("move_pct"),
                )
            ]
        instruments.append(row)
    instruments.sort(key=lambda row: str(row.get("instrument_name") or row.get("instrument_id") or ""))
    return {
        "service": "v2Matrix",
        "version": "v1",
        "updated_at_ist": updated_at,
        "updated_label": datetime_label_24(updated_at),
        "event_count": state.get("event_count", 0),
        "last_event_at_ist": state.get("last_event_at_ist"),
        "last_event_age": age_label(state.get("last_event_at_ist"), updated_at),
        "instruments": instruments,
    }


def apply_event(
    payload: dict[str, Any],
    state: dict[str, Any] | None = None,
    *,
    persist: bool = True,
    received_at_ist: str | None = None,
) -> dict[str, Any]:
    state = state if state is not None else read_json(STATE_PATH, {"instruments": {}, "event_count": 0})
    if not isinstance(state, dict):
        state = {"instruments": {}, "event_count": 0}
    instruments = state.setdefault("instruments", {})
    if not isinstance(instruments, dict):
        instruments = {}
        state["instruments"] = instruments

    instrument_id = str(payload.get("instrument_id") or payload.get("instrument") or "").strip()
    if not instrument_id:
        raise HTTPException(status_code=400, detail="instrument_id is required")
    previous = instruments.get(instrument_id) if isinstance(instruments.get(instrument_id), dict) else {}
    previous_regime = str(previous.get("regime") or REGIME_NEUTRAL)
    payload_event_type = str(payload.get("event_type") or "").lower()
    payload_position_id = str(payload.get("position_id") or payload.get("signal_id") or "").strip()
    previous_active_position_id = str(previous.get("active_position_id") or "").strip()
    if (
        payload_event_type in EXIT_EVENT_TYPES
        and previous_regime in {REGIME_BULLISH, REGIME_BEARISH}
        and payload_position_id
        and previous_active_position_id
        and payload_position_id != previous_active_position_id
    ):
        ignored = {
            **payload,
            "event_id": event_id(payload),
            "received_at_ist": received_at_ist or now_ist().isoformat(),
            "matrix_state_ignored": True,
            "matrix_state_ignored_reason": "exit_position_id_does_not_match_active_position",
            "active_position_id": previous_active_position_id,
        }
        state["event_count"] = int(state.get("event_count") or 0) + 1
        state["last_event_at_ist"] = ignored["received_at_ist"]
        state["updated_at_ist"] = ignored["received_at_ist"]
        if persist:
            append_jsonl(EVENTS_PATH, ignored)
            atomic_write_json(STATE_PATH, state)
        return previous
    regime = infer_regime(payload, previous)
    event_trigger_time = trigger_time_from_payload(payload, regime, previous)
    event_trigger_price = trigger_price_from_payload(payload, previous)
    trigger_time = event_trigger_time
    trigger_price = event_trigger_price
    trigger_price_source = payload.get("trigger_price_source") or previous.get("trigger_price_source")
    trigger_price_instrument_key = payload.get("trigger_price_instrument_key") or previous.get("trigger_price_instrument_key")
    trigger_ltp_underlying = payload.get("trigger_ltp_underlying") or previous.get("trigger_ltp_underlying")
    trigger_bid = payload.get("trigger_bid") or previous.get("trigger_bid")
    trigger_ask = payload.get("trigger_ask") or previous.get("trigger_ask")
    trigger_quote_warning = payload.get("trigger_quote_warning") or previous.get("trigger_quote_warning")
    if payload_event_type == "state_snapshot":
        snapshot_position_state = str(payload.get("position_state") or "").lower()
        snapshot_flat_with_exit_ref = snapshot_position_state in {"flat", "none", "neutral"} and event_trigger_time
        if previous and not snapshot_flat_with_exit_ref:
            trigger_time = previous.get("trigger_time_ist")
            trigger_price = previous.get("trigger_price_underlying")
            trigger_price_source = previous.get("trigger_price_source")
            trigger_price_instrument_key = previous.get("trigger_price_instrument_key")
            trigger_ltp_underlying = previous.get("trigger_ltp_underlying")
            trigger_bid = previous.get("trigger_bid")
            trigger_ask = previous.get("trigger_ask")
            trigger_quote_warning = previous.get("trigger_quote_warning")
        elif not snapshot_flat_with_exit_ref:
            trigger_time = None
            trigger_price = None
            trigger_price_source = payload.get("trigger_price_source")
            trigger_price_instrument_key = payload.get("trigger_price_instrument_key")
            trigger_ltp_underlying = None
            trigger_bid = None
            trigger_ask = None
            trigger_quote_warning = None
    current_price = current_price_from_payload(payload, previous)
    current_time = current_time_from_payload(payload, previous)
    received_at = received_at_ist or now_ist().isoformat()
    active_position_id = previous_active_position_id or None
    row_position_id = previous.get("position_id")
    if payload_event_type in ENTRY_EVENT_TYPES and payload_position_id:
        active_position_id = payload_position_id
        row_position_id = payload_position_id
    elif payload_event_type in EXIT_EVENT_TYPES and payload_position_id and payload_position_id == previous_active_position_id:
        active_position_id = None
        row_position_id = payload_position_id
    trigger_changed = trigger_time != previous.get("trigger_time_ist") or trigger_price != previous.get("trigger_price_underlying")
    regime_changed = regime != previous_regime
    if trigger_changed or regime_changed:
        transition_from = previous_regime
    else:
        transition_from = previous.get("transition_from_regime") or fallback_transition_from(
            regime,
            payload.get("side") or previous.get("side"),
        )
    enriched = {
        **payload,
        "event_id": event_id(payload),
        "received_at_ist": received_at,
    }
    trade_event = payload_event_type not in {"", "state_snapshot"}
    last_trade_fields = {}
    if trade_event:
        last_trade_fields = {
            "last_trade_event_id": enriched["event_id"],
            "last_trade_event_type": payload.get("event_type"),
            "last_trade_event_time_ist": event_trigger_time,
            "last_trade_event_price_underlying": event_trigger_price,
            "last_trade_event_price_source": payload.get("trigger_price_source"),
            "last_trade_event_from_regime": previous_regime,
            "last_trade_event_to_regime": regime,
            "last_trade_event_tranche": payload.get("tranche"),
            "last_trade_event_position_closed": payload.get("position_closed"),
        }
        if regime == REGIME_NEUTRAL and previous_regime in {REGIME_BULLISH, REGIME_BEARISH}:
            last_trade_fields.update(trade_entry_reference_fields(payload, previous, previous_regime))
        else:
            last_trade_fields.update(
                {
                    "last_trade_event_entry_time_ist": None,
                    "last_trade_event_entry_price_underlying": None,
                    "last_trade_event_entry_price_source": None,
                    "last_trade_event_entry_from_regime": None,
                    "last_trade_event_entry_to_regime": None,
                }
            )
    elif payload_event_type == "state_snapshot" and str(payload.get("position_state") or "").lower() in {"flat", "none", "neutral"} and event_trigger_time:
        last_trade_fields = {
            "last_trade_event_id": enriched["event_id"],
            "last_trade_event_type": "state_snapshot_flat",
            "last_trade_event_time_ist": event_trigger_time,
            "last_trade_event_price_underlying": event_trigger_price,
            "last_trade_event_price_source": payload.get("trigger_price_source"),
            "last_trade_event_from_regime": fallback_transition_from(REGIME_NEUTRAL, payload.get("side") or previous.get("side")),
            "last_trade_event_to_regime": REGIME_NEUTRAL,
            "last_trade_event_tranche": None,
            "last_trade_event_position_closed": True,
            "last_trade_event_entry_time_ist": None,
            "last_trade_event_entry_price_underlying": None,
            "last_trade_event_entry_price_source": None,
            "last_trade_event_entry_from_regime": None,
            "last_trade_event_entry_to_regime": None,
        }
    else:
        last_trade_fields = {
            key: previous.get(key)
            for key in (
                "last_trade_event_id",
                "last_trade_event_type",
                "last_trade_event_time_ist",
                "last_trade_event_price_underlying",
                "last_trade_event_price_source",
                "last_trade_event_from_regime",
                "last_trade_event_to_regime",
                "last_trade_event_tranche",
                "last_trade_event_position_closed",
                "last_trade_event_entry_time_ist",
                "last_trade_event_entry_price_underlying",
                "last_trade_event_entry_price_source",
                "last_trade_event_entry_from_regime",
                "last_trade_event_entry_to_regime",
            )
        }
    row = {
        **previous,
        "instrument_id": instrument_id,
        "instrument_name": payload.get("instrument_name") or payload.get("display_name") or previous.get("instrument_name") or instrument_id,
        "regime": regime,
        "transition_from_regime": transition_from,
        "transition_to_regime": regime,
        "trigger_time_ist": trigger_time,
        "trigger_price_underlying": trigger_price,
        "trigger_price_source": trigger_price_source,
        "trigger_price_instrument_key": trigger_price_instrument_key,
        "trigger_ltp_underlying": trigger_ltp_underlying,
        "trigger_bid": trigger_bid,
        "trigger_ask": trigger_ask,
        "trigger_quote_warning": trigger_quote_warning,
        "current_price_underlying": current_price,
        "current_time_ist": current_time,
        "trigger_event_type": payload.get("event_type") or previous.get("trigger_event_type"),
        "side": payload.get("side") or previous.get("side"),
        "position_id": row_position_id,
        "active_position_id": active_position_id,
        "source_strategy": payload.get("source_strategy") or payload.get("strategy_id") or previous.get("source_strategy"),
        "source_model_version": payload.get("source_model_version") or payload.get("model_version") or previous.get("source_model_version"),
        "signal_source": payload.get("signal_source") or previous.get("signal_source"),
        "signal_instrument_key": payload.get("signal_instrument_key") or previous.get("signal_instrument_key"),
        "execution_instrument_key": payload.get("execution_instrument_key") or previous.get("execution_instrument_key"),
        "entry_stale": payload.get("entry_stale") if payload_event_type in ENTRY_EVENT_TYPES else None,
        "entry_stale_reason": payload.get("entry_stale_reason") if payload_event_type in ENTRY_EVENT_TYPES else None,
        "entry_staleness_seconds": (
            payload.get("entry_staleness_seconds")
            if payload_event_type in ENTRY_EVENT_TYPES and payload.get("entry_staleness_seconds") is not None
            else None
        ),
        "entry_evaluation_time_ist": payload.get("entry_evaluation_time_ist") if payload_event_type in ENTRY_EVENT_TYPES else None,
        "event_created_at_ist": payload.get("created_at_ist") or previous.get("event_created_at_ist"),
        **last_trade_fields,
        "last_event": enriched,
        "last_webhook_at_ist": received_at,
    }
    instruments[instrument_id] = row
    state["event_count"] = int(state.get("event_count") or 0) + 1
    state["last_event_at_ist"] = received_at
    state["updated_at_ist"] = received_at
    if persist:
        append_jsonl(EVENTS_PATH, enriched)
        atomic_write_json(STATE_PATH, state)
    return row


def apply_events(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state = read_json(STATE_PATH, {"instruments": {}, "event_count": 0})
    if not isinstance(state, dict):
        state = {"instruments": {}, "event_count": 0}
    rows: list[dict[str, Any]] = []
    enriched_events: list[dict[str, Any]] = []
    received_at = now_ist().isoformat()
    for payload in payloads:
        row = apply_event(payload, state=state, persist=False, received_at_ist=received_at)
        rows.append(row)
        last_event = row.get("last_event") if isinstance(row.get("last_event"), dict) else None
        if last_event:
            enriched_events.append(last_event)
    for event in enriched_events:
        append_jsonl(EVENTS_PATH, event)
    atomic_write_json(STATE_PATH, state)
    return rows


@app.get("/v2Matrix/v1", response_class=HTMLResponse)
def matrix_dashboard() -> HTMLResponse:
    return HTMLResponse(
        MATRIX_HTML,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/v2Matrix/v2", response_class=HTMLResponse)
def matrix_dashboard_v2() -> HTMLResponse:
    return HTMLResponse(
        MATRIX_V2_HTML,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/v2Matrix/notifications", response_class=HTMLResponse)
def matrix_notifications_page() -> HTMLResponse:
    return HTMLResponse(
        MATRIX_NOTIFICATIONS_HTML,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/v2Matrix", response_class=HTMLResponse)
@app.get("/v2Matrix/", response_class=HTMLResponse)
def matrix_dashboard_root() -> HTMLResponse:
    return matrix_dashboard_v2()


@app.head("/v2Matrix/v1")
def matrix_head() -> Response:
    return Response(status_code=200)


@app.head("/v2Matrix/v2")
def matrix_head_v2() -> Response:
    return Response(status_code=200)


@app.head("/v2Matrix/notifications")
def matrix_notifications_head() -> Response:
    return Response(status_code=200)


@app.head("/v2Matrix")
@app.head("/v2Matrix/")
def matrix_head_root() -> Response:
    return Response(status_code=200)


def matrix_status_response() -> JSONResponse:
    state = read_json(STATE_PATH, {"instruments": {}, "event_count": 0, "updated_at_ist": now_ist().isoformat()})
    return JSONResponse(
        json_clean(state_payload_for_response(state)),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/v2matrix/v1/status")
def matrix_status() -> JSONResponse:
    return matrix_status_response()


@app.get("/api/v2matrix/v2/status")
def matrix_status_v2() -> JSONResponse:
    return matrix_status_response()


@app.get("/api/v2matrix/v2/notifications/today")
def matrix_notifications_today() -> JSONResponse:
    current = now_ist()
    window_start = notification_window_start(current)
    return JSONResponse(
        json_clean(
            {
                "trade_date": current.date().isoformat(),
                "generated_at_ist": current.isoformat(),
                "window_start_ist": window_start.isoformat(),
                "notifications": today_notification_events(),
            }
        ),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/v2matrix/v2/stream")
async def matrix_status_stream(request: Request) -> StreamingResponse:
    async def events():
        last_key = ""
        keepalive_ticks = 0
        while True:
            if await request.is_disconnected():
                break
            state = read_json(STATE_PATH, {"instruments": {}, "event_count": 0, "updated_at_ist": now_ist().isoformat()})
            payload = {
                "event_count": state.get("event_count", 0) if isinstance(state, dict) else 0,
                "last_event_at_ist": state.get("last_event_at_ist") if isinstance(state, dict) else None,
                "updated_at_ist": state.get("updated_at_ist") if isinstance(state, dict) else None,
            }
            key = json.dumps(json_clean(payload), sort_keys=True, separators=(",", ":"))
            if key != last_key:
                last_key = key
                yield f"event: matrix-state\ndata: {key}\n\n"
                keepalive_ticks = 0
            else:
                keepalive_ticks += 1
                if keepalive_ticks >= 15:
                    keepalive_ticks = 0
                    yield ": keepalive\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/v2matrix/v1/events")
async def matrix_events(request: Request) -> JSONResponse:
    body = await request.body()
    verify_webhook_auth(request, body)
    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json") from exc
    events = payload.get("events") if isinstance(payload, dict) else None
    if isinstance(events, list):
        rows = apply_events([event for event in events if isinstance(event, dict)])
    elif isinstance(payload, dict):
        rows = [apply_event(payload)]
    else:
        raise HTTPException(status_code=400, detail="payload must be an object or {events: []}")
    return JSONResponse({"ok": True, "accepted": len(rows), "instruments": rows})


MATRIX_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>v2Matrix</title>
  <script>
    document.documentElement.dataset.theme = localStorage.getItem("v2matrix-theme") || "dark";
  </script>
  <style>
    :root {
      color-scheme: dark;
      --ink:#f5f7fb;
      --muted:#8f9bad;
      --line:#263141;
      --bg:#080b10;
      --panel:#0f141c;
      --panel-hover:#151c27;
      --panel-active:#1a2431;
      --bull:#55d69a;
      --bull-bg:#0e2b20;
      --bear:#ff7a70;
      --bear-bg:#351412;
      --neutral:#a9b2c2;
      --neutral-bg:#202733;
      --focus:#88a7ff;
      --header-line:#111722;
      --sort-bg:#111722;
      --detail-bg:#0b1017;
      --panel-strong:#151d29;
      --live:#48e38f;
      --shadow:0 12px 34px rgba(0, 0, 0, .24);
    }
    :root[data-theme="light"] {
      color-scheme: light;
      --ink:#10151f;
      --muted:#657184;
      --line:#d8e0eb;
      --bg:#f6f8fb;
      --panel:#ffffff;
      --panel-hover:#f1f5fa;
      --panel-active:#eaf0f7;
      --bull:#147a4c;
      --bull-bg:#ddf6ea;
      --bear:#b8342c;
      --bear-bg:#ffe4e1;
      --neutral:#526071;
      --neutral-bg:#edf1f6;
      --focus:#4b68d9;
      --header-line:#dce3ed;
      --sort-bg:#ffffff;
      --detail-bg:#f8fafc;
      --panel-strong:#edf3f9;
      --shadow:0 12px 34px rgba(36, 48, 65, .1);
    }
    * { box-sizing:border-box; }
    body { margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif; background:var(--bg); color:var(--ink); }
    header { position:sticky; top:0; z-index:30; padding:14px 14px 9px; border-bottom:1px solid var(--header-line); background:var(--bg); }
    .header-inner { max-width:600px; margin:0 auto; width:100%; display:flex; justify-content:space-between; gap:14px; align-items:center; }
    .header-actions { display:flex; align-items:center; gap:9px; flex-wrap:wrap; justify-content:flex-end; }
    h1 { margin:0; font-size:16px; font-weight:850; letter-spacing:0; }
    .subtitle { margin-top:1px; color:var(--muted); font-size:9px; font-weight:700; }
    .meta { color:var(--muted); font-size:9px; display:flex; gap:10px; flex-wrap:wrap; justify-content:flex-end; align-items:center; }
    .live-badge {
      display:inline-flex;
      align-items:center;
      gap:6px;
      color:var(--muted);
      font-size:9px;
      font-weight:750;
      padding:5px 9px;
      border:1px solid var(--line);
      border-radius:999px;
      background:var(--panel);
    }
    .live-dot {
      width:9px;
      height:9px;
      border-radius:50%;
      background:#7c8796;
      box-shadow:none;
    }
    .live-badge.live { color:var(--ink); }
    .live-badge.live .live-dot { background:var(--live); box-shadow:0 0 0 4px rgba(72, 227, 143, .13), 0 0 16px rgba(72, 227, 143, .85); }
    .theme-toggle {
      appearance:none;
      border:1px solid var(--line);
      color:var(--ink);
      background:var(--panel);
      border-radius:999px;
      padding:5px 9px;
      font-size:9px;
      font-weight:750;
      cursor:pointer;
    }
    main { padding:12px 14px 26px; }
    .board { max-width:600px; margin:0 auto; }
    .filterbar {
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:9px;
      margin:0 0 14px;
      padding:7px;
      border:1px solid var(--line);
      border-radius:8px;
      background:var(--detail-bg);
    }
    .filter-card {
      appearance:none;
      border:1px solid var(--line);
      background:var(--panel);
      color:var(--ink);
      border-radius:7px;
      padding:8px 10px;
      text-align:left;
      cursor:pointer;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:8px;
    }
    .filter-card.active { background:var(--panel-active); border-color:#40516a; }
    .filter-label { color:var(--muted); font-size:8px; text-transform:uppercase; font-weight:800; letter-spacing:.04em; }
    .filter-count { display:inline-block; margin-top:0; font-size:13px; font-weight:850; font-variant-numeric:tabular-nums; }
    .sortbar {
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:8px;
      margin:0 0 9px;
      align-items:center;
    }
    .sort {
      appearance:none;
      border:1px solid var(--line);
      background:var(--sort-bg);
      color:var(--muted);
      border-radius:6px;
      padding:6px 8px;
      font-size:8px;
      text-transform:uppercase;
      letter-spacing:.04em;
      font-weight:750;
      text-align:left;
      cursor:pointer;
    }
    .sort:nth-child(n+2) { text-align:center; }
    .sort.active { border-color:#40516a; background:var(--panel-active); color:var(--ink); }
    .sort-arrow { float:right; color:var(--focus); font-size:11px; }
    .sort:focus-visible, .card:focus-visible, .filter-card:focus-visible, .theme-toggle:focus-visible { outline:2px solid var(--focus); outline-offset:2px; }
    .cards { display:grid; gap:8px; }
    .card {
      width:100%;
      border:1px solid var(--line);
      border-radius:7px;
      background:var(--panel);
      color:inherit;
      padding:0;
      text-align:left;
      cursor:pointer;
      overflow:hidden;
      transition:background .18s ease, border-color .18s ease, transform .18s ease;
      box-shadow:var(--shadow);
    }
    .card:hover { background:var(--panel-hover); border-color:#344357; }
    .card-main {
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:8px;
      align-items:center;
      padding:9px 10px;
    }
    .symbol { font-weight:820; font-size:11px; line-height:1.15; }
    .small { color:var(--muted); font-size:12px; margin-top:3px; }
    .pill {
      display:inline-flex;
      align-items:center;
      width:max-content;
      min-width:62px;
      justify-content:center;
      padding:3px 7px;
      border-radius:999px;
      font-size:9px;
      font-weight:750;
      justify-self:center;
    }
    .pill.Bullish { background:var(--bull-bg); color:var(--bull); }
    .pill.Bearish { background:var(--bear-bg); color:var(--bear); }
    .pill.Neutral { background:var(--neutral-bg); color:var(--neutral); }
    .age, .move { color:var(--ink); font-size:9.5px; font-weight:720; font-variant-numeric:tabular-nums; text-align:center; }
    .move.positive { color:var(--bull); }
    .move.negative { color:var(--bear); }
    .card-detail {
      display:grid;
      grid-template-rows:0fr;
      padding:0 10px;
      border-top:0 solid transparent;
      background:var(--detail-bg);
      transition:grid-template-rows .22s ease, padding .22s ease, border-top-width .22s ease;
    }
    .card.expanded .card-detail { grid-template-rows:1fr; padding:9px 10px 10px; border-top-width:1px; border-top-color:var(--line); }
    .detail-inner { overflow:hidden; }
    .detail-grid {
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:9px;
      align-items:center;
    }
    .detail-grid.neutral-detail { grid-template-columns:repeat(4, minmax(0, 1fr)); }
    .detail-cell { font-size:9.5px; font-weight:750; color:var(--ink); min-width:0; }
    .detail-transition { display:flex; align-items:center; gap:6px; justify-content:center; color:var(--muted); letter-spacing:0; }
    .regime-chip {
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-width:44px;
      border-radius:999px;
      padding:3px 6px;
      font-size:8px;
      font-weight:850;
      line-height:1;
      white-space:nowrap;
    }
    .regime-chip.Bullish { background:var(--bull-bg); color:var(--bull); }
    .regime-chip.Bearish { background:var(--bear-bg); color:var(--bear); }
    .regime-chip.Neutral { background:var(--neutral-bg); color:var(--neutral); }
    .transition-arrow { color:var(--focus); font-size:10px; font-weight:850; }
    .price-box {
      display:flex;
      flex-direction:row;
      gap:6px;
      align-items:center;
      justify-content:center;
      font-variant-numeric:tabular-nums;
      white-space:nowrap;
    }
    .price-label { color:var(--muted); font-size:7px; font-weight:850; letter-spacing:.03em; }
    .detail-move {
      grid-row:1 / span 2;
      grid-column:4;
      align-self:stretch;
      display:flex;
      align-items:center;
      justify-content:center;
      border-left:1px solid var(--line);
      font-variant-numeric:tabular-nums;
      font-weight:850;
    }
    .price { font-variant-numeric:tabular-nums; font-weight:650; }
    .empty {
      padding:28px;
      color:var(--muted);
      text-align:center;
      background:var(--panel);
      border:1px solid var(--line);
      border-radius:8px;
    }
    .hidden { display:none; }
    @media (max-width: 720px) {
      header { padding:16px; }
      .header-inner { display:block; }
      .header-actions { justify-content:flex-start; margin-top:10px; }
      .meta { justify-content:flex-start; }
      main { padding:0 12px 18px; }
      .filterbar { grid-template-columns:repeat(2, minmax(0, 1fr)); gap:8px; }
    }
    @media (max-width: 440px) {
      header { padding:13px 12px 8px; }
      .filter-card { padding:9px 10px; }
      .sortbar, .card-main { grid-template-columns:repeat(4, minmax(0, 1fr)); gap:5px; }
      .detail-grid, .detail-grid.neutral-detail { grid-template-columns:repeat(4, minmax(0, 1fr)); gap:5px; }
      .sort { padding:6px 5px; font-size:8px; }
      .card-main { padding:8px 7px; }
      .symbol { font-size:10.5px; }
      .pill { min-width:50px; padding:3px 5px; font-size:8.5px; }
      .age, .move, .detail-cell { font-size:9px; }
      .regime-chip { min-width:34px; font-size:7.5px; padding:2px 4px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <div>
        <h1>v2Matrix</h1>
        <div class="subtitle">by Viveka</div>
      </div>
      <div class="header-actions">
        <div class="live-badge" id="liveBadge">
          <span class="live-dot"></span>
          <span id="marketState">Checking</span>
        </div>
        <button class="theme-toggle" id="themeToggle" type="button" aria-pressed="false">Day</button>
        <div class="meta">
          <span id="updated">Updated: Waiting</span>
          <span id="events">Symbols: 0</span>
        </div>
      </div>
    </div>
  </header>
  <main>
    <div class="board">
      <div class="filterbar" id="filters" aria-label="Filter instruments"></div>
      <div class="sortbar" aria-label="Sort instruments">
        <button class="sort active" type="button" data-sort="symbol">Symbol</button>
        <button class="sort" type="button" data-sort="status">Status</button>
        <button class="sort" type="button" data-sort="age">Age</button>
        <button class="sort" type="button" data-sort="move">Move</button>
      </div>
      <div id="cards" class="cards">
        <div class="empty">Waiting for webhook events</div>
      </div>
    </div>
  </main>
  <script>
    const expandedIds = new Set();
    let latestRows = [];
    let latestData = {};
    let sortKey = "symbol";
    let sortDir = 1;
    let filterKey = "All";
    const filters = ["All", "Bullish", "Bearish", "Neutral"];
    const statusRank = { Bullish: 0, Bearish: 1, Neutral: 2 };
    const themeToggle = document.getElementById("themeToggle");
    const missing = value => value === null || value === undefined || value === "" || value === "NA";
    const safe = (value, fallback = "Not available") => missing(value) ? fallback : String(value);
    const esc = value => safe(value).replace(/[&<>"']/g, character => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    }[character]));
    const numberOrNull = value => {
      const number = Number(value);
      return Number.isFinite(number) ? number : null;
    };
    const fmtPricePlain = value => {
      const number = Number(value);
      return Number.isFinite(number) && number > 0
        ? number.toFixed(2)
        : "Not available";
    };
    const fmtMove = value => {
      const number = numberOrNull(value);
      if (number === null) return "NA";
      const prefix = number > 0 ? "+" : "";
      return `${prefix}${number.toFixed(2)}%`;
    };
    const moveClass = value => {
      const number = numberOrNull(value);
      if (number === null || number === 0) return "";
      return number > 0 ? "positive" : "negative";
    };
    const directionSign = row => {
      const side = safe(row.side, "").toLowerCase();
      if (side === "short") return -1;
      if (side === "long") return 1;
      const regime = safe(row.regime, "Neutral").toLowerCase();
      return regime === "bearish" ? -1 : 1;
    };
    const moveValue = row => {
      if (safe(row.regime, "Neutral") === "Neutral") return null;
      const serverValue = numberOrNull(row.move_pct);
      if (serverValue !== null) return serverValue;
      const current = numberOrNull(row.current_price_underlying);
      const anchor = numberOrNull(row.trigger_price_underlying);
      return current !== null && anchor !== null && anchor > 0 ? directionSign(row) * ((current - anchor) / anchor) * 100 : null;
    };
    const ageEpoch = row => {
      const parsed = Date.parse(row.trigger_time_ist || "");
      return Number.isFinite(parsed) ? parsed : 0;
    };
    const rowId = row => safe(row.instrument_id || row.instrument_name, "unknown");
    const filteredRows = rows => filterKey === "All" ? rows : rows.filter(row => safe(row.regime, "Neutral") === filterKey);
    function fmtClockIst24(rawTime) {
      const raw = rawTime || "";
      const parts = raw.split(":");
      if (parts.length === 2) {
        const hour24 = Number(parts[0]);
        const minute = parts[1].padStart(2, "0");
        if (Number.isFinite(hour24)) {
          return `${String(hour24).padStart(2, "0")}:${minute} IST`;
        }
      }
      return null;
    }
    function detailDateTime(detail, row) {
      const date = safe(detail.date || row.trigger_date, "No trigger date");
      const clock = fmtClockIst24(detail.time || row.trigger_time);
      if (clock) return `${date}, ${clock}`;
      return safe(detail.datetime || row.trigger_datetime, "No trigger yet").replace(":", ".");
    }
    function fallbackExpandedRow(row) {
      const status = safe(row.transition_to_regime || row.regime, "Neutral").toUpperCase();
      const from = safe(row.transition_from_regime || "Neutral").toUpperCase();
      return {
        date: row.trigger_date,
        time: row.trigger_time,
        datetime: row.trigger_datetime,
        transition: `${from} -> ${status}`,
        price: row.trigger_price_underlying,
        move_pct: row.move_pct
      };
    }
    function normaliseRegime(value) {
      const text = safe(value, "Neutral").toLowerCase();
      if (text === "bullish") return "Bullish";
      if (text === "bearish") return "Bearish";
      return "Neutral";
    }
    function transitionHtml(value) {
      const parts = safe(value, "Neutral -> Neutral").split("->").map(part => normaliseRegime(part.trim()));
      const from = parts[0] || "Neutral";
      const to = parts[1] || "Neutral";
      return `
        <span class="regime-chip ${from}">${esc(from)}</span>
        <span class="transition-arrow">&rarr;</span>
        <span class="regime-chip ${to}">${esc(to)}</span>
      `;
    }
    function priceBoxHtml(label, value) {
      return `
        <div class="price-box">
          <span class="price-label">${esc(label)}</span>
          <span class="price">${esc(fmtPricePlain(value))}</span>
        </div>
      `;
    }
    function neutralPriceLabel(detail) {
      const transition = safe(detail.transition, "").toLowerCase();
      return transition.includes("-> neutral") ? "Exit" : "Entry";
    }
    function expandedRowsHtml(row) {
      const status = safe(row.regime, "Neutral");
      const details = Array.isArray(row.expanded_rows) && row.expanded_rows.length
        ? row.expanded_rows
        : [fallbackExpandedRow(row)];
      if (status === "Neutral" && details.length >= 2) {
        const move = numberOrNull(row.neutral_move_pct ?? details[0].move_pct);
        return `
          <div class="detail-grid neutral-detail">
            ${details.slice(0, 2).map((detail, index) => `
              <div class="detail-cell">${esc(detailDateTime(detail, row))}</div>
              <div class="detail-cell detail-transition">${transitionHtml(detail.transition || "NEUTRAL -> NEUTRAL")}</div>
              <div class="detail-cell">${priceBoxHtml(neutralPriceLabel(detail), detail.price)}</div>
              ${index === 0 ? `<div class="detail-cell detail-move ${moveClass(move)}">${esc(fmtMove(move))}</div>` : ""}
            `).join("")}
          </div>
        `;
      }
      const detail = details[0] || fallbackExpandedRow(row);
      return `
        <div class="detail-grid">
          <div class="detail-cell">${esc(detailDateTime(detail, row))}</div>
          <div class="detail-cell detail-transition">${transitionHtml(detail.transition || "NEUTRAL -> NEUTRAL")}</div>
          <div class="detail-cell">${priceBoxHtml("Entry", detail.price)}</div>
          <div class="detail-cell">${priceBoxHtml("Current", row.current_price_underlying)}</div>
        </div>
      `;
    }
    function sortedRows(rows) {
      return [...rows].sort((a, b) => {
        let left;
        let right;
        if (sortKey === "status") {
          left = statusRank[a.regime] ?? 99;
          right = statusRank[b.regime] ?? 99;
        } else if (sortKey === "age") {
          left = ageEpoch(a);
          right = ageEpoch(b);
        } else if (sortKey === "move") {
          left = moveValue(a);
          right = moveValue(b);
          if (left === null && right === null) return 0;
          if (left === null) return 1;
          if (right === null) return -1;
        } else {
          left = safe(a.instrument_name || a.instrument_id, "").toLowerCase();
          right = safe(b.instrument_name || b.instrument_id, "").toLowerCase();
        }
        if (left < right) return -1 * sortDir;
        if (left > right) return 1 * sortDir;
        return safe(a.instrument_name || a.instrument_id, "").localeCompare(safe(b.instrument_name || b.instrument_id, ""));
      });
    }
    function updateSortButtons() {
      document.querySelectorAll(".sort").forEach(button => {
        const active = button.dataset.sort === sortKey;
        button.classList.toggle("active", active);
        const base = button.dataset.sort === "symbol" ? "Symbol" : button.dataset.sort === "status" ? "Status" : button.dataset.sort === "move" ? "Move" : "Age";
        button.innerHTML = active ? `${base}<span class="sort-arrow">${sortDir > 0 ? "↑" : "↓"}</span>` : base;
      });
    }
    function renderFilters(rows) {
      const watchRows = watchlistFilterActive ? rows.filter(isWatchlisted) : rows;
      const counts = {
        All: watchRows.length,
        Bullish: watchRows.filter(row => row.regime === "Bullish").length,
        Bearish: watchRows.filter(row => row.regime === "Bearish").length,
        Neutral: watchRows.filter(row => row.regime === "Neutral").length
      };
      const watchButton = document.getElementById("watchlistFilter");
      const watchCount = rows.filter(isWatchlisted).length;
      watchButton.classList.toggle("active", watchlistFilterActive);
      watchButton.setAttribute("aria-pressed", watchlistFilterActive ? "true" : "false");
      document.getElementById("watchlistCount").textContent = watchCount;
      watchButton.onclick = () => {
        pulseHaptic();
        watchlistFilterActive = !watchlistFilterActive;
        visibleLimit = INITIAL_VISIBLE_LIMIT;
        saveWatchlist();
        render(latestData);
      };
      const holder = document.getElementById("filters");
      holder.innerHTML = filters.map(name => `
        <button class="filter-card ${filterKey === name ? "active" : ""}" type="button" data-filter="${name}">
          <span class="filter-label">${name}</span>
          <span class="filter-count">${counts[name] || 0}</span>
        </button>
      `).join("");
      holder.querySelectorAll(".filter-card").forEach(card => {
        card.addEventListener("click", () => {
          filterKey = card.dataset.filter || "All";
          render(latestData);
        });
      });
    }
    function cardHtml(row) {
      const id = rowId(row);
      const expanded = expandedIds.has(id);
      const status = safe(row.regime, "Neutral");
      const age = safe(row.trigger_age, "No trigger yet");
      const move = moveValue(row);
      const moveText = status === "Neutral" ? "-" : fmtMove(move);
      return `
        <button class="card ${expanded ? "expanded" : ""}" type="button" data-id="${esc(id)}" aria-expanded="${expanded ? "true" : "false"}">
          <div class="card-main">
            <div>
              <div class="symbol">${esc(row.instrument_name || row.instrument_id)}</div>
            </div>
            <span class="pill ${esc(status)}">${esc(status)}</span>
            <div class="age">${esc(age)}</div>
            <div class="move ${moveClass(move)}">${esc(moveText)}</div>
          </div>
          <div class="card-detail">
            <div class="detail-inner">
              ${expandedRowsHtml(row)}
            </div>
          </div>
        </button>
      `;
    }
    function parseIstParts(raw) {
      const match = String(raw || "").match(/^(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2})/);
      if (!match) return null;
      return {
        year: Number(match[1]),
        month: Number(match[2]),
        day: Number(match[3]),
        hour: Number(match[4]),
        minute: Number(match[5])
      };
    }
    function updateMarketState(data) {
      const badge = document.getElementById("liveBadge");
      const label = document.getElementById("marketState");
      const raw = data.last_event_at_ist || data.updated_at_ist;
      const parsed = Date.parse(raw || "");
      const parts = parseIstParts(raw);
      const ageSeconds = Number.isFinite(parsed) ? Math.max(0, (Date.now() - parsed) / 1000) : Infinity;
      const withinClock = parts && ((parts.hour > 9 || (parts.hour === 9 && parts.minute >= 15)) && (parts.hour < 15 || (parts.hour === 15 && parts.minute <= 35)));
      const weekday = parts ? new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay() : 0;
      const live = ageSeconds <= 180 && weekday >= 1 && weekday <= 5 && withinClock;
      badge.classList.toggle("live", live);
      label.textContent = live ? "Market live" : "Market idle";
    }
    function applyTheme(theme) {
      document.documentElement.dataset.theme = theme;
      localStorage.setItem("v2matrix-theme", theme);
      const next = theme === "dark" ? "Day" : "Night";
      themeToggle.textContent = next;
      themeToggle.setAttribute("aria-pressed", theme === "light" ? "true" : "false");
    }
    function render(data) {
      const rows = data.instruments || [];
      latestData = data;
      latestRows = rows;
      document.getElementById("updated").textContent = `Updated: ${safe(data.updated_label || data.updated_at_ist, "Waiting")}`;
      document.getElementById("events").textContent = `Symbols: ${rows.length}`;
      updateMarketState(data);
      renderFilters(rows);
      const body = document.getElementById("cards");
      if (!rows.length) {
        body.innerHTML = `<div class="empty">Waiting for webhook events</div>`;
        updateSortButtons();
        return;
      }
      const visibleRows = sortedRows(filteredRows(rows));
      if (!visibleRows.length) {
        body.innerHTML = `<div class="empty">No instruments in this filter</div>`;
        updateSortButtons();
        return;
      }
      body.innerHTML = visibleRows.map(cardHtml).join("");
      body.querySelectorAll(".card").forEach(card => {
        card.addEventListener("click", () => {
          const id = card.dataset.id;
          if (expandedIds.has(id)) expandedIds.delete(id);
          else expandedIds.add(id);
          render(latestData);
        });
      });
      updateSortButtons();
    }
    document.querySelectorAll(".sort").forEach(button => {
      button.addEventListener("click", () => {
        const next = button.dataset.sort;
        if (sortKey === next) sortDir *= -1;
        else {
          sortKey = next;
          sortDir = 1;
        }
        render(latestData);
      });
    });
    async function refresh() {
      try {
        const response = await fetch("/api/v2matrix/v1/status", {cache:"no-store"});
        render(await response.json());
      } catch (error) {
        document.getElementById("marketState").textContent = "API error";
        document.getElementById("liveBadge").classList.remove("live");
      }
    }
    applyTheme(document.documentElement.dataset.theme || "dark");
    themeToggle.addEventListener("click", () => {
      applyTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark");
    });
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""

MATRIX_NOTIFICATIONS_HTML = """
<!doctype html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>v2Matrix Alerts | Viveka</title>
  <style>
    :root {
      --bg:#212121;
      --surface:#2f2f2f;
      --surface-soft:#282828;
      --surface-strong:#383838;
      --ink:#ececec;
      --muted:#b4b4b4;
      --faint:#8a8a8a;
      --line:rgba(255,255,255,.12);
      --line-soft:rgba(255,255,255,.08);
      --primary:#10a37f;
      --primary-soft:rgba(16,163,127,.14);
      --success:#63d297;
      --danger:#ff6b6b;
      --warning:#f4bd50;
      --radius:8px;
      --shadow:0 10px 34px rgba(0,0,0,.20);
    }
    :root[data-theme="light"] {
      --bg:#f7f7f8;
      --surface:#ffffff;
      --surface-soft:#f2f2f2;
      --surface-strong:#eeeeee;
      --ink:#1f2328;
      --muted:#5f6368;
      --faint:#8b949e;
      --line:rgba(31,35,40,.13);
      --line-soft:rgba(31,35,40,.08);
      --shadow:0 10px 28px rgba(31,35,40,.08);
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      min-height:100vh;
      background:var(--bg);
      color:var(--ink);
      font-family:Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing:0;
    }
    a { color:inherit; text-decoration:none; }
    .shell-header {
      position:sticky;
      top:0;
      z-index:20;
      border-bottom:1px solid var(--line-soft);
      background:rgba(33,33,33,.92);
      backdrop-filter:blur(18px);
    }
    :root[data-theme="light"] .shell-header { background:rgba(247,247,248,.92); }
    .topbar {
      width:100%;
      max-width:980px;
      min-height:58px;
      margin:0 auto;
      padding:10px 16px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
    }
    .brand {
      min-width:0;
      display:flex;
      align-items:center;
      gap:10px;
    }
    .back-link {
      width:34px;
      height:34px;
      border:1px solid var(--line);
      border-radius:999px;
      display:inline-flex;
      align-items:center;
      justify-content:center;
      background:var(--surface);
      color:var(--muted);
    }
    .back-link svg,
    .bell-heading svg {
      width:17px;
      height:17px;
      stroke:currentColor;
      stroke-width:2;
      stroke-linecap:round;
      stroke-linejoin:round;
      fill:none;
    }
    .brand-copy {
      min-width:0;
      display:flex;
      flex-direction:column;
      gap:2px;
    }
    .brand-copy strong {
      font-size:15px;
      line-height:1.1;
    }
    .brand-copy span {
      font-size:11px;
      color:var(--faint);
    }
    .notification-toggle {
      appearance:none;
      border:1px solid var(--line);
      border-radius:999px;
      background:var(--surface);
      color:var(--ink);
      width:58px;
      height:28px;
      padding:0;
      font-size:10px;
      font-weight:750;
      cursor:pointer;
      display:inline-flex;
      align-items:center;
      justify-content:center;
      position:relative;
      overflow:hidden;
    }
    .page {
      width:100%;
      max-width:980px;
      margin:0 auto;
      padding:18px 16px 32px;
    }
    .hero {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
      margin-bottom:12px;
      min-height:36px;
    }
    .submeta {
      color:var(--muted);
      font-size:12px;
      font-weight:650;
    }
    .toggle-panel {
      flex:0 0 auto;
      min-width:0;
      border:1px solid var(--line);
      border-radius:999px;
      background:var(--surface);
      padding:3px 4px 3px 10px;
      display:inline-flex;
      align-items:center;
      gap:8px;
      box-shadow:none;
    }
    .toggle-label {
      color:var(--faint);
      font-size:10px;
      font-weight:800;
      margin:0;
    }
    .notification-toggle {
      border-radius:999px;
      background:var(--primary-soft);
      color:var(--primary);
      border-color:rgba(16,163,127,.28);
    }
    .notification-toggle.disabled {
      background:rgba(255,107,107,.10);
      color:var(--danger);
      border-color:rgba(255,107,107,.26);
    }
    .tabs {
      display:grid;
      grid-template-columns:repeat(2, minmax(0, 1fr));
      gap:4px;
      width:min(244px, 100%);
      height:44px;
      min-height:44px;
      padding:4px;
      border:1px solid var(--line);
      border-radius:999px;
      background:var(--surface-soft);
      margin-bottom:12px;
      overflow:hidden;
    }
    .tab-button {
      appearance:none;
      border:1px solid transparent;
      border-radius:999px;
      background:transparent;
      color:var(--muted);
      height:34px;
      min-height:34px;
      padding:6px 8px;
      font-size:11px;
      font-weight:800;
      cursor:pointer;
      display:flex;
      align-items:center;
      justify-content:center;
      gap:5px;
      white-space:nowrap;
    }
    .tab-button.active {
      background:var(--surface-strong);
      color:var(--ink);
      border-color:var(--line-soft);
    }
    .cards {
      display:flex;
      flex-direction:column;
      gap:8px;
    }
    .notification-card {
      border:1px solid var(--line-soft);
      border-radius:var(--radius);
      background:var(--surface);
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:8px;
      min-height:46px;
      padding:10px 12px;
      align-items:center;
      box-shadow:none;
    }
    .card-main {
      min-width:0;
      display:contents;
    }
    .card-symbol {
      min-width:0;
      font-size:12px;
      font-weight:850;
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
      text-align:left;
    }
    .transition {
      display:flex;
      align-items:center;
      gap:6px;
      flex-wrap:nowrap;
      justify-content:center;
      color:var(--muted);
      font-size:11px;
      font-weight:700;
      min-width:0;
    }
    .notification-card .transition-arrow {
      color:var(--faint);
      font-size:10px;
      font-weight:850;
      line-height:1;
      flex:0 0 auto;
    }
    .chip {
      min-width:58px;
      padding:4px 6px;
      border:1px solid var(--line);
      border-radius:999px;
      text-align:center;
      font-size:9px;
      color:var(--muted);
      white-space:nowrap;
    }
    .chip.Bullish { color:var(--success); background:rgba(99,210,151,.10); border-color:rgba(99,210,151,.20); }
    .chip.Bearish { color:var(--danger); background:rgba(255,107,107,.10); border-color:rgba(255,107,107,.22); }
    .card-meta {
      display:block;
      color:var(--faint);
      font-size:10px;
      text-align:left;
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
    }
    .price-pill {
      align-self:center;
      justify-self:end;
      width:100%;
      min-width:0;
      text-align:right;
      color:var(--primary);
      font-weight:850;
      font-size:12px;
      font-variant-numeric:tabular-nums;
    }
    .empty {
      border:1px solid var(--line-soft);
      border-radius:var(--radius);
      background:var(--surface);
      color:var(--muted);
      padding:28px;
      text-align:center;
    }
    .sample-toast {
      position:fixed;
      top:74px;
      left:50%;
      transform:translateX(-50%) translateY(-10px);
      z-index:80;
      min-width:min(360px, calc(100vw - 24px));
      border:1px solid rgba(16,163,127,.30);
      border-radius:var(--radius);
      background:var(--surface);
      color:var(--ink);
      box-shadow:var(--shadow);
      padding:11px 13px;
      opacity:0;
      pointer-events:none;
      transition:opacity .16s ease, transform .16s ease;
      font-size:12px;
      font-weight:750;
    }
    .sample-toast.show {
      opacity:1;
      transform:translateX(-50%) translateY(0);
    }
    @media (max-width: 640px) {
      .topbar { padding:9px 10px; }
      .page { padding:14px 10px 28px; }
      .hero {
        gap:8px;
      }
      .toggle-panel {
        min-width:0;
      }
      .notification-card {
        grid-template-columns:minmax(0, 1fr) auto;
        grid-template-areas:
          "time price"
          "symbol transition";
        column-gap:8px;
        row-gap:7px;
        min-height:58px;
        padding:10px 10px;
      }
      .card-symbol {
        grid-area:symbol;
        font-size:10.5px;
      }
      .transition {
        grid-area:transition;
        justify-content:flex-end;
        gap:4px;
        font-size:8.5px;
      }
      .notification-card .transition-arrow { font-size:9px; }
      .chip { min-width:48px; padding:3px 5px; font-size:7.5px; }
      .card-meta {
        grid-area:time;
        font-size:8.5px;
        max-width:100%;
      }
      .price-pill {
        grid-area:price;
        justify-self:end;
        width:auto;
        text-align:right;
        font-size:11px;
      }
      .tabs { width:min(210px, 100%); height:42px; min-height:42px; }
      .tab-button { height:32px; min-height:32px; font-size:9px; padding:5px 5px; }
      .toggle-label { display:none; }
      .notification-toggle { width:52px; height:26px; font-size:9px; }
    }
  </style>
</head>
<body>
  <header class="shell-header">
    <div class="topbar">
      <div class="brand">
        <a class="back-link" href="/v2Matrix/" aria-label="Back to Matrix">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m15 18-6-6 6-6"></path></svg>
        </a>
        <div class="brand-copy">
          <strong>v2Matrix Alerts</strong>
        </div>
      </div>
    </div>
  </header>
  <div class="sample-toast" id="sampleToast" role="status" aria-live="polite"></div>
  <main class="page">
    <section class="hero">
      <div class="submeta" id="notificationMeta">Loading today's alerts...</div>
      <aside class="toggle-panel">
        <div class="toggle-label">Alerts</div>
        <button class="notification-toggle disabled" id="notificationToggle" type="button" aria-label="Enable notifications">Off</button>
      </aside>
    </section>
    <nav class="tabs" aria-label="Notification filters">
      <button class="tab-button active" id="latestTab" type="button" data-tab="latest">Unread (0)</button>
      <button class="tab-button" id="readTab" type="button" data-tab="read">Read</button>
    </nav>
    <section class="cards" id="notificationCards">
      <div class="empty">Loading notifications...</div>
    </section>
  </main>
  <script>
    const LAST_OPENED_KEY = "v2matrix-v1-notifications-last-opened-ms";
    const LAST_OPENED_WINDOW_KEY = "v2matrix-v1-notifications-last-opened-window";
    const NOTIFICATION_ENABLED_KEY = "v2matrix-v1-notifications";
    const SOUND_ENABLED_KEY = "v2matrix-v1-sound";
    let activeTab = "latest";
    let previousOpenedAt = Number(localStorage.getItem(LAST_OPENED_KEY) || "0") || 0;
    let activeWindowStart = localStorage.getItem(LAST_OPENED_WINDOW_KEY) || "";
    let todayNotifications = [];
    const missing = value => value === null || value === undefined || value === "" || value === "NA";
    const esc = value => String(missing(value) ? "" : value).replace(/[&<>"']/g, character => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    }[character]));
    const priceText = value => {
      const number = Number(value);
      return Number.isFinite(number) && number > 0 ? number.toFixed(2) : "--";
    };
    const monthLookup = {
      Jan:0, Feb:1, Mar:2, Apr:3, May:4, Jun:5,
      Jul:6, Aug:7, Sep:8, Oct:9, Nov:10, Dec:11
    };
    const parseIstLabelMillis = value => {
      const match = String(value || "").match(/^(\\d{1,2})\\s+([A-Za-z]{3})\\s+(\\d{4}),\\s+(\\d{2}):(\\d{2}):(\\d{2})\\s+IST$/);
      if (!match) return 0;
      const [, day, month, year, hour, minute, second] = match;
      const monthIndex = monthLookup[month];
      if (monthIndex === undefined) return 0;
      return Date.UTC(Number(year), monthIndex, Number(day), Number(hour), Number(minute), Number(second)) - (330 * 60 * 1000);
    };
    const receivedMillis = item => {
      const parsed = Date.parse(item.received_at_ist || item.trigger_time_ist || "");
      return Number.isFinite(parsed) ? parsed : parseIstLabelMillis(item.received_label);
    };
    function notificationSupported() {
      return "Notification" in window;
    }
    function notificationsActive() {
      return localStorage.getItem(NOTIFICATION_ENABLED_KEY) === "true" || localStorage.getItem(SOUND_ENABLED_KEY) === "true";
    }
    function updateToggle() {
      const button = document.getElementById("notificationToggle");
      const enabled = notificationsActive();
      button.textContent = enabled ? "On" : "Off";
      button.setAttribute("aria-label", enabled ? "Disable notifications" : "Enable notifications");
      button.classList.toggle("disabled", !enabled);
    }
    function playSampleTone() {
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (!AudioCtx) return;
        const ctx = new AudioCtx();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = "sine";
        osc.frequency.setValueAtTime(880, ctx.currentTime);
        gain.gain.setValueAtTime(0.0001, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.16, ctx.currentTime + 0.02);
        gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.22);
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.start();
        osc.stop(ctx.currentTime + 0.24);
        setTimeout(() => ctx.close?.(), 500);
      } catch (error) {}
    }
    function pulseHaptic() {
      try {
        if (navigator.vibrate) navigator.vibrate([35, 30, 35]);
      } catch (error) {}
    }
    function showSampleToast(message) {
      const toast = document.getElementById("sampleToast");
      toast.textContent = message;
      toast.classList.add("show");
      clearTimeout(showSampleToast.timer);
      showSampleToast.timer = setTimeout(() => toast.classList.remove("show"), 2800);
    }
    function emitSampleConfirmation() {
      playSampleTone();
      pulseHaptic();
      showSampleToast("Notifications enabled. Sample alert delivered.");
      if (notificationSupported() && Notification.permission === "granted") {
        try {
          new Notification("v2Matrix notifications enabled", {
            body: "You will receive Matrix alerts here.",
            tag: "v2matrix-notifications-enabled-sample"
          });
        } catch (error) {}
      }
    }
    async function toggleNotifications() {
      if (notificationsActive()) {
        localStorage.setItem(NOTIFICATION_ENABLED_KEY, "false");
        localStorage.setItem(SOUND_ENABLED_KEY, "false");
        updateToggle();
        return;
      }
      localStorage.setItem(SOUND_ENABLED_KEY, "true");
      if (notificationSupported()) {
        if (Notification.permission === "default") {
          try { await Notification.requestPermission(); } catch (error) {}
        }
        localStorage.setItem(NOTIFICATION_ENABLED_KEY, Notification.permission === "granted" ? "true" : "false");
      } else {
        localStorage.setItem(NOTIFICATION_ENABLED_KEY, "false");
      }
      updateToggle();
      if (notificationsActive()) emitSampleConfirmation();
    }
    function regimeChip(name) {
      const safeName = ["Bullish", "Bearish", "Neutral"].includes(name) ? name : "Neutral";
      return `<span class="chip ${safeName}">${safeName}</span>`;
    }
    function cardHtml(item) {
      return `
        <article class="notification-card">
          <div class="card-main">
            <div class="card-meta">
              ${esc(item.received_label || item.received_at_ist || "")}
            </div>
            <div class="card-symbol">${esc(item.symbol || "Unknown")}</div>
            <div class="transition">
              ${regimeChip(item.from_regime)}
              <span class="transition-arrow">&rarr;</span>
              ${regimeChip(item.to_regime)}
            </div>
          </div>
          <div class="price-pill">${esc(priceText(item.price))}</div>
        </article>
      `;
    }
    function render() {
      const byNewest = (left, right) => receivedMillis(right) - receivedMillis(left);
      const latest = todayNotifications.filter(item => receivedMillis(item) > previousOpenedAt).sort(byNewest);
      const read = todayNotifications.filter(item => receivedMillis(item) <= previousOpenedAt).sort(byNewest);
      document.getElementById("latestTab").textContent = `Unread (${latest.length})`;
      document.getElementById("readTab").textContent = `Read (${read.length})`;
      document.querySelectorAll(".tab-button").forEach(button => button.classList.toggle("active", button.dataset.tab === activeTab));
      const rows = activeTab === "latest" ? latest : read;
      const body = document.getElementById("notificationCards");
      body.innerHTML = rows.length ? rows.map(cardHtml).join("") : `<div class="empty">${activeTab === "latest" ? "No unread Matrix alerts right now" : "No read Matrix alerts for today yet"}</div>`;
      document.getElementById("notificationMeta").textContent = `${todayNotifications.length} alerts generated today. ${latest.length} unread`;
    }
    async function loadNotifications() {
      try {
        const response = await fetch("/api/v2matrix/v2/notifications/today", {cache:"no-store"});
        const payload = await response.json();
        const payloadWindow = String(payload.window_start_ist || "");
        if (payloadWindow && payloadWindow !== activeWindowStart) {
          activeWindowStart = payloadWindow;
          previousOpenedAt = 0;
          localStorage.setItem(LAST_OPENED_WINDOW_KEY, activeWindowStart);
        }
        todayNotifications = Array.isArray(payload.notifications) ? payload.notifications : [];
        render();
        localStorage.setItem(LAST_OPENED_KEY, String(Date.now()));
      } catch (error) {
        document.getElementById("notificationCards").innerHTML = `<div class="empty">Unable to load v2Matrix notifications</div>`;
      }
    }
    document.getElementById("notificationToggle").addEventListener("click", toggleNotifications);
    document.querySelectorAll(".tab-button").forEach(button => {
      button.addEventListener("click", () => {
        activeTab = button.dataset.tab || "latest";
        render();
      });
    });
    document.documentElement.dataset.theme = localStorage.getItem("v2matrix-v1-theme") || "dark";
    updateToggle();
    loadNotifications();
    setInterval(loadNotifications, 60000);
  </script>
</body>
</html>
"""


MATRIX_V2_HTML = """
<!doctype html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>v2Matrix | Institutional Trading System</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <script>
    const storedMatrixTheme = localStorage.getItem("v2matrix-v1-theme");
    const matrixThemeUserSet = localStorage.getItem("v2matrix-v1-theme-user-set") === "true";
    document.documentElement.dataset.theme = matrixThemeUserSet && storedMatrixTheme ? storedMatrixTheme : "dark";
  </script>
  <style>
    :root {
      color-scheme: light;
      --bg:#f7f8fa;
      --surface:#ffffff;
      --surface-soft:#f0f3ff;
      --surface-strong:#e7eeff;
      --glass:rgba(255,255,255,.72);
      --ink:#111c2d;
      --muted:#464555;
      --faint:#777587;
      --line:#c7c4d8;
      --line-soft:#e2e8f0;
      --primary:#493ee5;
      --primary-hot:#635bff;
      --primary-soft:#e2dfff;
      --success:#00714d;
      --success-hot:#10b981;
      --success-soft:#d7f8e7;
      --danger:#ba1a1a;
      --danger-soft:#ffdad6;
      --warning:#a36700;
      --warning-soft:#ffddb8;
      --shadow:0 1px 2px rgba(0,0,0,.05);
      --shadow-hover:0 12px 24px -6px rgba(30,41,59,.08);
      --radius:10px;
    }
    :root[data-theme="dark"] {
      color-scheme: dark;
      --bg:#0e1523;
      --surface:#111c2d;
      --surface-soft:#182337;
      --surface-strong:#263143;
      --glass:rgba(17,28,45,.72);
      --ink:#ecf1ff;
      --muted:#cbd5e1;
      --faint:#94a3b8;
      --line:#334155;
      --line-soft:#263143;
      --primary:#c3c0ff;
      --primary-hot:#8d86ff;
      --primary-soft:#261f72;
      --success:#6ffbbe;
      --success-hot:#4edea3;
      --success-soft:#113d2b;
      --danger:#ffb4ab;
      --danger-soft:#4b1515;
      --warning:#ffb95f;
      --warning-soft:#3f290b;
      --shadow:0 1px 2px rgba(0,0,0,.18);
      --shadow-hover:0 16px 28px -8px rgba(0,0,0,.34);
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      min-height:100vh;
      background:var(--bg);
      color:var(--ink);
      font-family:Manrope, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      -webkit-font-smoothing:antialiased;
      display:flex;
      flex-direction:column;
    }
    .shell-header {
      position:sticky;
      top:0;
      z-index:50;
      background:var(--surface);
      border-bottom:1px solid var(--line);
      box-shadow:var(--shadow);
    }
    .topbar {
      max-width:980px;
      margin:0 auto;
      padding:12px 16px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:14px;
    }
    .brand {
      display:flex;
      align-items:center;
      gap:18px;
      min-width:0;
    }
    .brand-title {
      display:flex;
      flex-direction:column;
      line-height:1;
      color:var(--primary);
      font-family:Manrope, sans-serif;
      font-size:19px;
      font-weight:800;
      letter-spacing:0;
      white-space:nowrap;
    }
    .brand-title span:last-child {
      margin-top:2px;
      color:var(--primary-hot);
      font-family:Manrope, sans-serif;
      font-size:9px;
      font-weight:600;
      letter-spacing:0;
      opacity:.75;
    }
    .nav-pill {
      color:var(--primary);
      font-size:9px;
      font-weight:700;
      letter-spacing:.06em;
      text-transform:uppercase;
      border-bottom:2px solid var(--primary);
      padding:5px 0;
      white-space:nowrap;
    }
    .top-actions {
      display:flex;
      align-items:center;
      gap:8px;
      flex-wrap:wrap;
      justify-content:flex-end;
    }
    .live-chip, .theme-toggle, .notify-toggle, .smoke-toggle, .icon-chip {
      border:1px solid var(--line);
      background:var(--surface-soft);
      color:var(--muted);
      border-radius:999px;
      padding:5px 9px;
      font-size:9px;
      font-weight:700;
      display:inline-flex;
      align-items:center;
      gap:7px;
      white-space:nowrap;
    }
    .live-dot {
      width:7px;
      height:7px;
      border-radius:50%;
      background:var(--faint);
    }
    .live-chip.live .live-dot {
      background:var(--success-hot);
      box-shadow:0 0 0 4px rgba(16,185,129,.12), 0 0 16px rgba(16,185,129,.75);
    }
    .live-chip.closed .live-dot {
      background:var(--danger);
      box-shadow:0 0 0 4px rgba(186,26,26,.10), 0 0 14px rgba(186,26,26,.42);
    }
    .theme-toggle {
      cursor:pointer;
      border-radius:10px;
      color:#fff;
      background:var(--primary-hot);
      border-color:var(--primary-hot);
      padding:6px 12px;
    }
    .notify-toggle {
      cursor:pointer;
      border-radius:10px;
      background:var(--surface-soft);
      color:var(--primary);
      width:30px;
      height:30px;
      justify-content:center;
      padding:0;
      font-size:13px;
    }
    .notify-toggle svg {
      width:15px;
      height:15px;
      stroke:currentColor;
      stroke-width:2;
      stroke-linecap:round;
      stroke-linejoin:round;
      fill:none;
    }
    .notify-toggle.enabled {
      background:var(--success-soft);
      border-color:rgba(0,113,77,.28);
      color:var(--success);
    }
    .notify-toggle.blocked {
      background:var(--danger-soft);
      border-color:rgba(186,26,26,.24);
      color:var(--danger);
    }
    .smoke-toggle {
      cursor:pointer;
      border-radius:10px;
      color:var(--warning);
      background:var(--warning-soft);
      border-color:rgba(163,103,0,.28);
      width:30px;
      height:30px;
      justify-content:center;
      padding:0;
    }
    .smoke-toggle svg {
      width:15px;
      height:15px;
      stroke:currentColor;
      stroke-width:2;
      stroke-linecap:round;
      stroke-linejoin:round;
      fill:none;
    }
    .alert-stack {
      position:fixed;
      top:10px;
      left:50%;
      transform:translateX(-50%);
      z-index:1000;
      width:min(520px, calc(100vw - 24px));
      display:flex;
      flex-direction:column;
      gap:8px;
      pointer-events:none;
    }
    .instant-alert {
      width:100%;
      border:1px solid rgba(151,166,195,.25);
      border-radius:10px;
      background:rgba(12,16,23,.94);
      color:var(--ink);
      box-shadow:0 18px 52px rgba(0,0,0,.38);
      padding:11px 13px;
      overflow:hidden;
      pointer-events:auto;
      animation:instantAlertIn .22s cubic-bezier(.2,.8,.2,1) both;
      backdrop-filter:blur(18px);
    }
    :root[data-theme="light"] .instant-alert {
      background:rgba(255,255,255,.96);
      box-shadow:0 16px 38px rgba(22,30,44,.16);
    }
    .instant-alert.removing { animation:instantAlertOut .18s ease both; }
    .instant-alert.Bullish { border-color:rgba(0,113,77,.42); }
    .instant-alert.Bearish { border-color:rgba(186,26,26,.42); }
    .instant-alert.Neutral { border-color:rgba(148,163,184,.42); }
    .instant-alert-title {
      margin-bottom:6px;
      font-family:Manrope, sans-serif;
      font-size:10.5px;
      font-weight:850;
      letter-spacing:0;
    }
    .instant-alert-body {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      color:var(--muted);
      font-size:9px;
      font-weight:750;
      line-height:1.3;
    }
    .instant-alert-price {
      color:var(--ink);
      font-variant-numeric:tabular-nums;
      white-space:nowrap;
    }
    @keyframes instantAlertIn {
      from { opacity:0; transform:translateY(-18px) scale(.98); }
      to { opacity:1; transform:translateY(0) scale(1); }
    }
    @keyframes instantAlertOut {
      from { opacity:1; transform:translateY(0) scale(1); }
      to { opacity:0; transform:translateY(-16px) scale(.98); }
    }
    .icon-stack {
      display:flex;
      gap:7px;
      padding-left:10px;
      border-left:1px solid var(--line);
    }
    .icon-chip {
      width:22px;
      height:22px;
      justify-content:center;
      padding:0;
      border-radius:50%;
      font-size:10px;
      color:var(--primary);
      background:transparent;
    }
    .page {
      width:100%;
      max-width:980px;
      margin:0 auto;
      padding:14px 16px 0;
      display:grid;
      grid-template-columns:minmax(0, 1fr) 260px;
      gap:16px;
      flex:1 0 auto;
    }
    .content {
      min-width:0;
      display:flex;
      flex-direction:column;
      gap:12px;
    }
    .hero-row {
      display:flex;
      justify-content:space-between;
      gap:12px;
      align-items:flex-end;
      flex-wrap:wrap;
    }
    h1 {
      margin:0;
      font-family:Manrope, sans-serif;
      font-size:24px;
      line-height:1.12;
      letter-spacing:0;
      color:var(--ink);
    }
    .submeta {
      margin-top:7px;
      display:flex;
      gap:8px;
      align-items:center;
      color:var(--muted);
      font-size:10px;
      flex-wrap:wrap;
    }
    .submeta strong { color:var(--primary); }
    .meta-dot {
      width:4px;
      height:4px;
      border-radius:50%;
      background:var(--faint);
      display:inline-block;
    }
    .filterbar {
      display:inline-flex;
      align-items:center;
      gap:4px;
      padding:4px;
      border:1px solid var(--line);
      border-radius:var(--radius);
      background:var(--surface-soft);
      box-shadow:var(--shadow);
      max-width:100%;
      overflow:auto;
    }
    .filter-zone {
      display:flex;
      align-items:center;
      justify-content:flex-end;
      gap:8px;
      max-width:100%;
      min-width:0;
    }
    .filter-card {
      appearance:none;
      border:0;
      border-radius:var(--radius);
      background:transparent;
      color:var(--muted);
      cursor:pointer;
      min-width:92px;
      padding:6px 9px;
      display:flex;
      align-items:center;
      justify-content:center;
      gap:6px;
      font-size:9px;
      font-weight:800;
      letter-spacing:.06em;
      text-transform:uppercase;
      transition:all .18s ease;
    }
    .watchlist-filter {
      appearance:none;
      border:1px solid var(--line);
      border-radius:var(--radius);
      background:var(--surface);
      color:var(--muted);
      cursor:pointer;
      min-height:34px;
      padding:6px 10px;
      display:inline-flex;
      align-items:center;
      justify-content:center;
      gap:7px;
      font-size:9px;
      font-weight:800;
      letter-spacing:.06em;
      text-transform:uppercase;
      white-space:nowrap;
      box-shadow:var(--shadow);
      transition:all .18s ease;
    }
    .watchlist-filter.active {
      background:var(--warning-soft);
      color:var(--warning);
      border-color:rgba(154,103,0,.28);
    }
    .filter-card.active {
      background:var(--primary-hot);
      color:#fff;
      box-shadow:0 4px 12px rgba(99,91,255,.2);
    }
    .filter-count {
      min-width:20px;
      padding:2px 6px;
      border-radius:6px;
      background:rgba(99,91,255,.12);
      font-size:8px;
      line-height:1;
      font-family:Manrope, sans-serif;
      font-weight:600;
    }
    .filter-card.active .filter-count { background:rgba(255,255,255,.22); }
    .list-header {
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:10px;
      align-items:center;
      padding:8px 12px;
      border-radius:8px;
      background:var(--surface-strong);
      color:var(--muted);
      font-size:9px;
      font-weight:800;
      letter-spacing:.08em;
      text-transform:uppercase;
    }
    .list-header button {
      appearance:none;
      border:0;
      background:transparent;
      color:inherit;
      padding:0;
      font:inherit;
      text-align:center;
      cursor:pointer;
      display:flex;
      align-items:center;
      justify-content:center;
      gap:4px;
      text-transform:inherit;
      letter-spacing:inherit;
    }
    .list-header button:first-child { justify-content:flex-start; text-align:left; }
    .sort-arrow { color:var(--primary); font-size:11px; line-height:1; }
    .signals {
      display:flex;
      flex-direction:column;
      gap:7px;
    }
    .signal-card {
      border:1px solid var(--line-soft);
      border-radius:var(--radius);
      background:var(--glass);
      backdrop-filter:blur(20px);
      box-shadow:var(--shadow);
      overflow:hidden;
      transition:box-shadow .2s ease, transform .2s ease, border-color .2s ease;
    }
    .signal-card:hover {
      box-shadow:var(--shadow-hover);
      transform:translateY(-1px);
      border-color:var(--line);
    }
    .signal-card.signal-notified {
      border-color:rgba(141,134,255,.72);
      background:
        linear-gradient(90deg, rgba(99,91,255,.20), transparent 32%),
        var(--glass);
      box-shadow:0 0 0 1px rgba(141,134,255,.30), 0 0 22px rgba(141,134,255,.30);
    }
    .signal-card.signal-notified .row-main {
      background:rgba(141,134,255,.08);
    }
    .row-shell {
      position:relative;
    }
    .row-main {
      width:100%;
      appearance:none;
      border:0;
      background:transparent;
      color:inherit;
      cursor:pointer;
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:10px;
      align-items:center;
      padding:9px 52px 9px 12px;
      text-align:left;
    }
    .watchlist-toggle {
      position:absolute;
      top:0;
      right:0;
      bottom:0;
      z-index:3;
      width:40px;
      appearance:none;
      border:0;
      border-left:1px solid var(--line-soft);
      background:linear-gradient(180deg, rgba(154,103,0,.08), rgba(154,103,0,.16));
      color:var(--warning);
      cursor:pointer;
      display:flex;
      align-items:center;
      justify-content:center;
      font-size:18px;
      line-height:1;
      transition:background .16s ease, color .16s ease, transform .16s ease;
      touch-action:manipulation;
    }
    .watchlist-toggle:hover,
    .watchlist-toggle:focus-visible {
      background:var(--warning-soft);
      color:var(--warning);
      outline:0;
    }
    .watchlist-toggle.active {
      background:linear-gradient(180deg, rgba(154,103,0,.20), rgba(154,103,0,.34));
      color:#9a6700;
    }
    .symbol-cell {
      display:flex;
      align-items:center;
      justify-content:flex-start;
      gap:9px;
      min-width:0;
    }
    .row-main > div:not(.symbol-cell) {
      min-width:0;
      display:flex;
      align-items:center;
      justify-content:center;
      text-align:center;
    }
    .accent-line {
      width:3px;
      height:28px;
      border-radius:999px;
      background:var(--line);
      flex:0 0 auto;
    }
    .signal-card.Bullish .accent-line { background:var(--success); }
    .signal-card.Bearish .accent-line { background:var(--danger); }
    .symbol {
      min-width:0;
      overflow:hidden;
      text-overflow:ellipsis;
      font-family:Manrope, sans-serif;
      font-size:12px;
      line-height:1.15;
      font-weight:800;
      letter-spacing:0;
    }
    .status-chip, .regime-chip {
      display:inline-flex;
      align-items:center;
      justify-content:center;
      width:max-content;
      min-width:62px;
      padding:4px 8px;
      border-radius:999px;
      font-size:9px;
      font-weight:700;
      border:1px solid var(--line);
      background:var(--surface-strong);
      color:var(--muted);
    }
    .regime-chip {
      min-width:58px;
      padding:4px 7px;
      font-size:8.5px;
    }
    .status-chip.Bullish, .regime-chip.Bullish {
      background:var(--success-soft);
      color:var(--success);
      border-color:rgba(0,113,77,.2);
    }
    .status-chip.Bearish, .regime-chip.Bearish {
      background:var(--danger-soft);
      color:var(--danger);
      border-color:rgba(186,26,26,.22);
    }
    .age {
      color:var(--muted);
      font-size:10px;
      text-align:center;
      justify-content:center;
    }
    .move {
      text-align:center;
      justify-content:center;
      font-family:Manrope, sans-serif;
      font-size:10px;
      font-weight:600;
      color:var(--muted);
      white-space:nowrap;
    }
    .move.positive { color:var(--success); }
    .move.negative { color:var(--danger); }
    .detail-panel {
      display:grid;
      grid-template-rows:0fr;
      border-top:0 solid transparent;
      background:rgba(240,243,255,.48);
      transition:grid-template-rows .22s ease, padding .22s ease, border-top-width .22s ease;
      padding:0 12px;
    }
    :root[data-theme="dark"] .detail-panel { background:rgba(24,35,55,.62); }
    .signal-card.expanded .detail-panel {
      grid-template-rows:1fr;
      border-top-width:1px;
      border-top-color:var(--line-soft);
      padding:8px 12px 10px;
    }
    .detail-inner { overflow:hidden; }
    .detail-grid {
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      gap:10px;
      align-items:center;
    }
    .detail-row + .detail-row { margin-top:6px; }
    .detail-cell {
      min-width:0;
      display:flex;
      align-items:center;
      justify-content:center;
      text-align:center;
      font-size:8.5px;
      color:var(--muted);
    }
    .detail-grid > .detail-cell:nth-child(4n + 1) {
      justify-content:flex-start;
      text-align:left;
    }
    .detail-date-time {
      gap:4px;
      flex-wrap:wrap;
      line-height:1.2;
    }
    .detail-date::after { content:","; }
    .detail-time-clock { white-space:nowrap; }
    .detail-transition {
      display:flex;
      align-items:center;
      justify-content:center;
      gap:5px;
    }
    .transition-arrow { color:var(--primary); font-weight:800; }
    .price-box {
      display:flex;
      align-items:center;
      gap:5px;
      justify-content:center;
      white-space:nowrap;
    }
    .price-label {
      color:var(--faint);
      font-size:8.5px;
      font-weight:800;
      letter-spacing:0;
      text-transform:none;
    }
    .price {
      font-family:Manrope, sans-serif;
      font-size:8.5px;
      font-weight:600;
      color:var(--ink);
    }
    .neutral-move {
      grid-column:4;
      grid-row:1 / span 2;
      align-self:stretch;
      display:flex;
      align-items:center;
      justify-content:center;
      font-family:Manrope, sans-serif;
      font-size:8.5px;
      font-weight:600;
      border-left:1px solid var(--line-soft);
      padding-left:8px;
    }
    .neutral-move.positive, .performer-move.positive { color:var(--success); }
    .neutral-move.negative, .performer-move.negative { color:var(--danger); }
    .no-trigger-detail {
      display:flex;
      align-items:center;
      justify-content:center;
      min-height:34px;
      color:var(--faint);
      font-size:9px;
      font-weight:700;
      text-align:center;
    }
    .side-rail {
      display:flex;
      flex-direction:column;
      gap:12px;
      padding-top:2px;
    }
    .overview-card {
      border:1px solid var(--line);
      border-radius:var(--radius);
      background:linear-gradient(135deg, var(--surface-soft), var(--glass));
      backdrop-filter:blur(20px);
      box-shadow:var(--shadow-hover);
      padding:14px 12px;
    }
    .overview-card h2 {
      margin:0 0 8px;
      font-family:Manrope, sans-serif;
      color:var(--primary);
      font-size:15px;
      letter-spacing:0;
    }
    .overview-card p {
      margin:0 0 12px;
      color:var(--muted);
      font-size:10px;
      line-height:1.55;
    }
    .metric-row {
      display:flex;
      justify-content:space-between;
      gap:8px;
      color:var(--muted);
      font-size:9px;
      font-weight:700;
      margin:10px 0 6px;
    }
    .metric-value { color:var(--success); }
    .move-pair {
      display:grid;
      grid-template-columns:repeat(2, minmax(0, 1fr));
      gap:8px;
      margin-top:12px;
    }
    .move-metric {
      border:1px solid var(--line-soft);
      border-radius:var(--radius);
      background:rgba(99,91,255,.06);
      padding:8px;
      min-width:0;
    }
    .move-metric-label {
      color:var(--faint);
      font-size:8px;
      font-weight:800;
      letter-spacing:.05em;
      text-transform:uppercase;
    }
    .move-metric-value {
      margin-top:5px;
      font-size:14px;
      font-weight:800;
      color:var(--muted);
    }
    .move-metric-value.positive { color:var(--success); }
    .move-metric-value.negative { color:var(--danger); }
    .bar {
      height:6px;
      border-radius:999px;
      background:rgba(99,91,255,.08);
      overflow:hidden;
    }
    .bar span {
      display:block;
      height:100%;
      width:0%;
      border-radius:999px;
      background:var(--success);
      transition:width .25s ease;
    }
    .bar.warning span { background:var(--warning); }
    .performer-card {
      position:relative;
      min-height:138px;
      border:1px solid var(--line-soft);
      border-radius:var(--radius);
      overflow:hidden;
      background:
        radial-gradient(circle at 20% 10%, rgba(99,91,255,.14), transparent 32%),
        linear-gradient(160deg, rgba(255,255,255,.82), rgba(240,243,255,.68));
      box-shadow:var(--shadow-hover);
      padding:66px 12px 14px;
    }
    :root[data-theme="dark"] .performer-card {
      background:
        radial-gradient(circle at 20% 10%, rgba(99,91,255,.22), transparent 32%),
        linear-gradient(160deg, rgba(17,28,45,.92), rgba(24,35,55,.72));
    }
    .performer-card:before {
      content:"";
      position:absolute;
      inset:0;
      background:
        linear-gradient(100deg, transparent 12%, rgba(16,185,129,.24) 13%, transparent 14%, transparent 45%, rgba(163,103,0,.22) 46%, transparent 47%),
        repeating-linear-gradient(90deg, rgba(99,91,255,.06) 0 1px, transparent 1px 34px);
      mask-image:linear-gradient(to bottom, rgba(0,0,0,.76), transparent 66%);
      pointer-events:none;
    }
    .performer-eyebrow {
      color:var(--primary);
      font-size:9px;
      font-weight:800;
      letter-spacing:.1em;
      text-transform:uppercase;
      position:relative;
    }
    .performer-name {
      position:relative;
      margin-top:8px;
      font-family:Manrope, sans-serif;
      font-size:15px;
      font-weight:800;
      letter-spacing:0;
    }
    .performer-move {
      position:relative;
      margin-top:8px;
      font-family:Manrope, sans-serif;
      color:var(--success);
      font-weight:600;
    }
    .performer-position {
      position:relative;
      margin-top:6px;
      color:var(--muted);
      font-size:9px;
      font-weight:700;
      line-height:1.35;
    }
    .empty {
      border:1px solid var(--line-soft);
      border-radius:var(--radius);
      padding:18px;
      color:var(--muted);
      text-align:center;
      background:var(--glass);
    }
    .scroll-note {
      display:flex;
      justify-content:center;
      padding:24px 0 16px;
      color:var(--faint);
      font-style:italic;
    }
    footer {
      flex:0 0 auto;
      margin:28px 0 0;
      border-top:1px solid var(--line);
      padding:0;
    }
    .footer-inner {
      max-width:980px;
      margin:0 auto;
      padding:16px;
      display:flex;
      justify-content:space-between;
      gap:12px;
      color:var(--muted);
      font-size:9px;
      font-weight:700;
      letter-spacing:.04em;
      flex-wrap:wrap;
    }
    .footer-links { display:flex; gap:16px; flex-wrap:wrap; }
    @media (max-width: 1080px) {
      .page { grid-template-columns:1fr; }
      .side-rail { display:grid; grid-template-columns:1fr 1fr; }
      h1 { font-size:24px; }
    }
    @media (max-width: 760px) {
      .alert-stack { top:10px; }
      .topbar { padding:11px 14px; align-items:flex-start; }
      .brand { gap:12px; }
      .nav-pill, .icon-stack { display:none; }
      .page { padding:12px 12px 0; }
      .side-rail { order:-1; }
      .hero-row { align-items:flex-start; }
      h1 { font-size:22px; }
      .filterbar { width:100%; }
      .filter-card { min-width:88px; padding:6px 8px; }
      .list-header, .row-main, .detail-grid {
        grid-template-columns:repeat(4, minmax(0, 1fr));
        gap:7px;
        padding-left:10px;
        padding-right:10px;
      }
      .detail-panel, .signal-card.expanded .detail-panel { padding-left:10px; padding-right:10px; }
      .symbol { font-size:11px; }
      .status-chip { min-width:56px; font-size:8px; padding:4px 7px; }
      .regime-chip { min-width:42px; font-size:6.8px; padding:3px 4px; }
      .detail-transition { gap:3px; }
      .transition-arrow { font-size:8px; }
      .detail-date-time {
        flex-direction:column;
        align-items:flex-start;
        gap:1px;
      }
      .detail-date::after { content:""; }
      .detail-cell, .price-label, .price, .neutral-move { font-size:8px; }
      .age, .move { font-size:9px; }
      .side-rail { grid-template-columns:1fr; }
      .footer-inner { padding:16px 12px; }
    }
    @media (max-width: 520px) {
      .alert-stack { top:10px; width:calc(100vw - 20px); }
      .instant-alert { padding:9px 10px; border-radius:9px; }
      .instant-alert-body { align-items:flex-start; flex-direction:column; gap:6px; }
      .brand-title { font-size:18px; }
      .top-actions { gap:6px; }
      .live-chip { padding:5px 8px; }
      .theme-toggle { padding:5px 10px; }
      .notify-toggle, .smoke-toggle { width:28px; height:28px; font-size:12px; }
      .list-header, .row-main {
        grid-template-columns:repeat(4, minmax(0, 1fr));
      }
      .list-header { padding-top:8px; padding-bottom:8px; font-size:8px; }
      .row-main { padding-top:8px; padding-bottom:8px; }
      .symbol-cell { gap:7px; }
      .accent-line { height:24px; }
      .detail-grid { grid-template-columns:repeat(4, minmax(0, 1fr)); }
      .regime-chip { min-width:29px; font-size:5.4px; padding:2px; }
      .detail-transition { gap:1px; }
      .transition-arrow { font-size:6px; }
      .neutral-move {
        grid-column:4;
        grid-row:1 / span 2;
        justify-content:center;
      }
    }

    /* Matrix ChatGPT schema experiment: neutral surfaces, quiet hierarchy, OpenAI green accents. */
    :root {
      --bg:#f7f7f8;
      --surface:#ffffff;
      --surface-soft:#f4f4f4;
      --surface-strong:#ececf1;
      --glass:#ffffff;
      --ink:#0d0d0d;
      --muted:#5d5d66;
      --faint:#8e8ea0;
      --line:#d9d9e3;
      --line-soft:#ececf1;
      --primary:#10a37f;
      --primary-hot:#0d8f70;
      --primary-soft:#e7f8f2;
      --success:#0d8f70;
      --success-hot:#10a37f;
      --success-soft:#e7f8f2;
      --danger:#c2352b;
      --danger-soft:#fff1f0;
      --warning:#9a6700;
      --warning-soft:#fff7e6;
      --shadow:0 1px 2px rgba(0,0,0,.04);
      --shadow-hover:0 10px 28px rgba(0,0,0,.07);
      --radius:12px;
    }
    :root[data-theme="dark"] {
      --bg:#212121;
      --surface:#2f2f2f;
      --surface-soft:#262626;
      --surface-strong:#3a3a3a;
      --glass:#2f2f2f;
      --ink:#ececec;
      --muted:#c5c5d2;
      --faint:#8e8ea0;
      --line:#4a4a4a;
      --line-soft:#3a3a3a;
      --primary:#10a37f;
      --primary-hot:#19c37d;
      --primary-soft:#12382f;
      --success:#19c37d;
      --success-hot:#19c37d;
      --success-soft:#12382f;
      --danger:#ff7b72;
      --danger-soft:#3f2020;
      --warning:#f4bf75;
      --warning-soft:#3a2f18;
      --shadow:0 1px 2px rgba(0,0,0,.18);
      --shadow-hover:0 12px 30px rgba(0,0,0,.24);
    }
    body {
      background:var(--bg);
      font-family:Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing:0;
    }
    .shell-header {
      background:color-mix(in srgb, var(--surface) 92%, transparent);
      border-bottom:1px solid var(--line-soft);
      box-shadow:none;
      backdrop-filter:blur(18px);
    }
    .topbar, .page, .footer-inner {
      max-width:1180px;
    }
    .topbar {
      min-height:58px;
      padding:10px 18px;
    }
    .brand { gap:14px; }
    .brand-title {
      color:var(--ink);
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:18px;
      font-weight:700;
      letter-spacing:0;
    }
    .brand-title span:last-child {
      color:var(--faint);
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:10px;
      font-weight:500;
      opacity:1;
    }
    .nav-pill {
      color:var(--muted);
      border:1px solid var(--line-soft);
      border-radius:999px;
      background:var(--surface-soft);
      padding:7px 11px;
      font-size:11px;
      font-weight:600;
      letter-spacing:0;
      text-transform:none;
    }
    .top-actions { gap:8px; }
    .live-chip, .theme-toggle, .notify-toggle, .smoke-toggle, .icon-chip {
      border-color:var(--line);
      background:var(--surface);
      color:var(--muted);
      box-shadow:none;
      font-size:11px;
      font-weight:600;
      letter-spacing:0;
    }
    .theme-toggle {
      background:var(--ink);
      border-color:var(--ink);
      color:var(--surface);
      border-radius:999px;
      padding:7px 13px;
    }
    :root[data-theme="dark"] .theme-toggle {
      background:var(--surface-strong);
      border-color:var(--line);
      color:var(--ink);
    }
    .notify-toggle, .smoke-toggle {
      border-radius:999px;
      width:34px;
      height:34px;
      background:var(--surface);
      color:var(--muted);
    }
    .notify-toggle.enabled {
      background:var(--primary-soft);
      border-color:color-mix(in srgb, var(--primary) 30%, var(--line));
      color:var(--primary);
    }
    .smoke-toggle {
      background:var(--surface);
      border-color:var(--line);
      color:var(--muted);
    }
    .page {
      grid-template-columns:minmax(0, 1fr) 300px;
      gap:18px;
      padding:18px 18px 0;
    }
    .content { gap:12px; }
    h1 {
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:24px;
      font-weight:650;
      line-height:1.18;
      color:var(--ink);
    }
    .submeta {
      color:var(--faint);
      font-size:12px;
      font-weight:500;
    }
    .submeta strong { color:var(--primary); font-weight:650; }
    .filterbar {
      background:var(--surface);
      border-color:var(--line-soft);
      border-radius:999px;
      box-shadow:none;
      padding:4px;
    }
    .filter-card {
      min-width:86px;
      border-radius:999px;
      color:var(--muted);
      font-size:11px;
      font-weight:600;
      letter-spacing:0;
      text-transform:none;
      padding:7px 10px;
    }
    .filter-card.active {
      background:var(--surface-strong);
      color:var(--ink);
      box-shadow:none;
    }
    .filter-count {
      background:var(--surface-soft);
      border:1px solid var(--line-soft);
      color:var(--muted);
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:10px;
      font-weight:600;
    }
    .filter-card.active .filter-count {
      background:var(--surface);
      color:var(--muted);
    }
    .list-header {
      background:transparent;
      color:var(--faint);
      border-bottom:1px solid var(--line-soft);
      border-radius:0;
      padding:8px 14px;
      font-size:11px;
      font-weight:600;
      letter-spacing:0;
      text-transform:none;
    }
    .list-header button { font-weight:600; }
    .sort-arrow { color:var(--primary); }
    .signals { gap:8px; }
    .signal-card {
      background:var(--surface);
      border-color:var(--line-soft);
      border-radius:14px;
      box-shadow:none;
      backdrop-filter:none;
    }
    .signal-card:hover {
      transform:none;
      border-color:var(--line);
      box-shadow:var(--shadow-hover);
    }
    .signal-card.signal-notified {
      background:var(--surface);
      border-color:color-mix(in srgb, var(--primary) 45%, var(--line));
      box-shadow:0 0 0 1px color-mix(in srgb, var(--primary) 18%, transparent), 0 10px 28px color-mix(in srgb, var(--primary) 12%, transparent);
    }
    .signal-card.signal-notified .row-main {
      background:color-mix(in srgb, var(--primary) 7%, transparent);
    }
    .row-main {
      padding:12px 14px;
      gap:12px;
    }
    .accent-line {
      width:4px;
      height:30px;
      border-radius:999px;
      background:var(--line);
    }
    .symbol {
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:13px;
      font-weight:650;
    }
    .status-chip, .regime-chip {
      background:var(--surface-soft);
      border-color:var(--line-soft);
      color:var(--muted);
      font-size:11px;
      font-weight:600;
      min-width:68px;
      padding:5px 9px;
    }
    .regime-chip {
      min-width:62px;
      font-size:10px;
      padding:4px 8px;
    }
    .status-chip.Bullish, .regime-chip.Bullish {
      background:var(--success-soft);
      color:var(--success);
      border-color:color-mix(in srgb, var(--success) 24%, var(--line));
    }
    .status-chip.Bearish, .regime-chip.Bearish {
      background:var(--danger-soft);
      color:var(--danger);
      border-color:color-mix(in srgb, var(--danger) 24%, var(--line));
    }
    .age, .move {
      font-size:12px;
      font-weight:500;
      color:var(--muted);
    }
    .move {
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-weight:650;
    }
    .detail-panel {
      background:var(--surface-soft);
      padding:0 14px;
    }
    :root[data-theme="dark"] .detail-panel { background:var(--surface-soft); }
    .signal-card.expanded .detail-panel {
      border-top-color:var(--line-soft);
      padding:10px 14px 12px;
    }
    .detail-grid {
      gap:12px;
    }
    .detail-cell, .price-label, .price, .neutral-move {
      font-size:10.5px;
    }
    .detail-cell { color:var(--muted); }
    .price {
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-weight:650;
      color:var(--ink);
    }
    .price-label {
      color:var(--faint);
      font-weight:600;
    }
    .transition-arrow {
      color:var(--faint);
      font-weight:650;
    }
    .no-trigger-detail {
      min-height:40px;
      color:var(--faint);
      font-size:12px;
      font-weight:500;
    }
    .side-rail {
      gap:12px;
    }
    .overview-card, .performer-card {
      background:var(--surface);
      border-color:var(--line-soft);
      border-radius:14px;
      box-shadow:none;
      backdrop-filter:none;
      padding:16px;
    }
    .overview-card h2 {
      color:var(--ink);
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:16px;
      font-weight:650;
    }
    .overview-card p, .metric-row, .performer-position {
      color:var(--muted);
      font-size:12px;
      font-weight:500;
    }
    .move-metric {
      background:var(--surface-soft);
      border-color:var(--line-soft);
      border-radius:12px;
    }
    .move-metric-label {
      color:var(--faint);
      font-size:10px;
      font-weight:600;
      letter-spacing:0;
      text-transform:none;
    }
    .move-metric-value {
      font-size:18px;
      font-weight:650;
      color:var(--ink);
    }
    .bar {
      height:7px;
      background:var(--surface-strong);
    }
    .bar span {
      background:var(--primary);
    }
    .performer-card {
      min-height:unset;
      padding:16px;
    }
    .performer-card:before { display:none; }
    .performer-eyebrow {
      color:var(--faint);
      font-size:11px;
      font-weight:600;
      letter-spacing:0;
      text-transform:none;
    }
    .performer-name {
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:20px;
      font-weight:700;
    }
    .performer-move {
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-weight:650;
    }
    .empty {
      background:var(--surface);
      border-color:var(--line-soft);
      border-radius:14px;
      color:var(--muted);
    }
    .scroll-note {
      color:var(--faint);
      font-style:normal;
      font-size:12px;
    }
    footer {
      border-top-color:var(--line-soft);
      margin-top:28px;
    }
    .footer-inner {
      color:var(--faint);
      font-size:11px;
      font-weight:500;
      letter-spacing:0;
    }
    .instant-alert {
      background:var(--surface);
      border-color:var(--line);
      border-radius:14px;
      color:var(--ink);
      box-shadow:0 16px 42px rgba(0,0,0,.14);
      backdrop-filter:blur(18px);
    }
    :root[data-theme="dark"] .instant-alert {
      background:#2f2f2f;
      box-shadow:0 16px 42px rgba(0,0,0,.32);
    }
    .instant-alert-title {
      font-family:Inter, ui-sans-serif, system-ui, sans-serif;
      font-size:13px;
      font-weight:650;
    }
    .instant-alert-body {
      font-size:12px;
      font-weight:500;
    }
    .instant-alert-price {
      font-weight:650;
    }
    @media (max-width: 1080px) {
      .page { grid-template-columns:1fr; }
      .side-rail { order:-1; grid-template-columns:repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 760px) {
      .topbar { padding:10px 12px; }
      .page { padding:12px 12px 0; }
      .side-rail { grid-template-columns:1fr; }
      h1 { font-size:22px; }
      .filterbar { border-radius:14px; }
      .filter-card { min-width:82px; font-size:10.5px; }
      .list-header, .row-main, .detail-grid {
        gap:8px;
        padding-left:10px;
        padding-right:10px;
      }
      .symbol { font-size:12px; }
      .status-chip { min-width:58px; font-size:9.5px; padding:4px 7px; }
      .regime-chip { min-width:40px; font-size:7px; padding:3px 4px; }
      .detail-cell, .price-label, .price, .neutral-move { font-size:8.5px; }
    }
    @media (max-width: 520px) {
      .brand-title { font-size:17px; }
      .live-chip { font-size:10px; }
      .notify-toggle, .smoke-toggle { width:30px; height:30px; }
      .regime-chip { min-width:30px; font-size:5.8px; padding:2px; }
    }

    /* Matrix ChatGPT mobile schema experiment. */
    @media (max-width: 820px) {
      body {
        background:var(--bg);
      }
      .shell-header {
        position:sticky;
        top:0;
        background:color-mix(in srgb, var(--surface) 96%, transparent);
        border-bottom:1px solid var(--line-soft);
      }
      .topbar {
        min-height:52px;
        padding:8px 12px;
        align-items:center;
        gap:8px;
      }
      .brand {
        flex:1 1 auto;
        min-width:0;
        gap:8px;
      }
      .brand-title {
        font-size:17px;
        line-height:1.05;
      }
      .brand-title span:last-child {
        display:none;
      }
      .top-actions {
        flex:0 0 auto;
        gap:6px;
      }
      .live-chip {
        height:32px;
        max-width:124px;
        overflow:hidden;
        text-overflow:ellipsis;
        padding:6px 9px;
        font-size:10.5px;
      }
      .theme-toggle {
        height:32px;
        padding:6px 10px;
        font-size:10.5px;
      }
      .notify-toggle, .smoke-toggle {
        width:32px;
        height:32px;
      }
      .page {
        display:flex;
        flex-direction:column;
        width:100%;
        max-width:none;
        padding:10px 10px 0;
        gap:10px;
      }
      .content {
        order:2;
        gap:10px;
      }
      .side-rail {
        order:1;
        display:grid;
        grid-template-columns:1fr;
        gap:8px;
        padding-top:0;
      }
      .hero-row {
        display:flex;
        flex-direction:column;
        align-items:stretch;
        gap:10px;
      }
      h1 {
        font-size:20px;
        line-height:1.18;
        padding:0 2px;
      }
      .submeta {
        padding:0 2px;
        font-size:11px;
        gap:6px;
      }
      .filterbar {
        width:100%;
        display:flex;
        overflow-x:auto;
        overflow-y:hidden;
        gap:6px;
        padding:5px;
        border-radius:16px;
        scrollbar-width:none;
        -webkit-overflow-scrolling:touch;
      }
      .filterbar::-webkit-scrollbar {
        display:none;
      }
      .filter-card {
        flex:0 0 auto;
        min-width:76px;
        padding:8px 10px;
        font-size:11px;
        white-space:nowrap;
      }
      .list-header {
        display:none;
      }
      .signals {
        gap:8px;
      }
      .signal-card {
        border-radius:16px;
        overflow:hidden;
      }
      .row-main {
        grid-template-columns:minmax(0, 1fr) auto;
        grid-template-areas:
          "symbol status"
          "age move";
        gap:8px 10px;
        padding:12px;
        min-height:72px;
      }
      .symbol-cell {
        grid-area:symbol;
        justify-content:flex-start;
        gap:9px;
      }
      .row-main > div:nth-child(2) {
        grid-area:status;
        justify-content:flex-end;
        text-align:right;
      }
      .row-main > .age {
        grid-area:age;
        justify-content:flex-start;
        text-align:left;
        padding-left:13px;
      }
      .row-main > .move {
        grid-area:move;
        justify-content:flex-end;
        text-align:right;
      }
      .accent-line {
        width:4px;
        height:34px;
      }
      .symbol {
        font-size:14px;
        line-height:1.2;
      }
      .status-chip {
        min-width:70px;
        padding:5px 10px;
        font-size:11px;
      }
      .age, .move {
        font-size:11.5px;
      }
      .detail-panel, .signal-card.expanded .detail-panel {
        padding-left:12px;
        padding-right:12px;
      }
      .detail-grid {
        grid-template-columns:1fr;
        gap:8px;
        padding:0;
      }
      .detail-row + .detail-row {
        margin-top:10px;
        padding-top:10px;
        border-top:1px solid var(--line-soft);
      }
      .detail-cell {
        justify-content:space-between;
        text-align:left;
        font-size:11px;
        width:100%;
      }
      .detail-grid > .detail-cell:nth-child(4n + 1) {
        justify-content:space-between;
      }
      .detail-date-time {
        flex-direction:row;
        align-items:center;
        justify-content:flex-start;
        gap:6px;
      }
      .detail-date::after {
        content:",";
      }
      .detail-transition {
        justify-content:flex-start;
        gap:5px;
        overflow-x:auto;
        padding:2px 0;
      }
      .regime-chip {
        min-width:58px;
        font-size:10px;
        padding:4px 8px;
      }
      .transition-arrow {
        font-size:11px;
      }
      .price-box {
        width:100%;
        justify-content:space-between;
        gap:10px;
      }
      .price-label, .price {
        font-size:11px;
      }
      .neutral-move {
        grid-column:auto;
        grid-row:auto;
        align-self:auto;
        border-left:0;
        border-top:1px solid var(--line-soft);
        padding:8px 0 0;
        justify-content:space-between;
        font-size:11px;
      }
      .overview-card, .performer-card {
        border-radius:16px;
        padding:13px;
      }
      .overview-card h2 {
        font-size:15px;
        margin-bottom:6px;
      }
      .overview-card p {
        font-size:11px;
        margin-bottom:10px;
      }
      .metric-row {
        font-size:11px;
      }
      .move-pair {
        grid-template-columns:repeat(2, minmax(0, 1fr));
        gap:8px;
      }
      .move-metric {
        border-radius:14px;
        padding:9px;
      }
      .move-metric-label {
        font-size:9.5px;
      }
      .move-metric-value {
        font-size:16px;
      }
      .performer-name {
        font-size:18px;
      }
      .performer-move {
        margin-top:6px;
        font-size:13px;
      }
      .performer-position {
        font-size:11px;
      }
      .footer-inner {
        max-width:none;
        padding:14px 12px;
        flex-direction:column;
        align-items:flex-start;
        gap:8px;
      }
    }
    @media (max-width: 440px) {
      .topbar {
        padding:8px 10px;
      }
      .brand-title {
        font-size:16px;
      }
      .live-chip span:last-child {
        max-width:78px;
        overflow:hidden;
        text-overflow:ellipsis;
      }
      .theme-toggle {
        display:none;
      }
      h1 {
        font-size:19px;
      }
      .row-main {
        padding:11px;
      }
      .symbol {
        font-size:13px;
      }
      .status-chip {
        min-width:64px;
        font-size:10px;
        padding:4px 8px;
      }
      .move-pair {
        grid-template-columns:1fr;
      }
      .regime-chip {
        min-width:48px;
        font-size:8.8px;
        padding:4px 6px;
      }
      .detail-transition {
        gap:3px;
      }
      .price-box {
        flex-wrap:wrap;
      }
    }

    /* Matrix ChatGPT mobile sidebar + aligned signal table experiment. */
    .mobile-menu-toggle,
    .mobile-menu-close,
    .mobile-drawer-brand,
    .mobile-menu-backdrop {
      display:none;
    }
    .mobile-menu-toggle svg,
    .mobile-menu-close svg {
      width:18px;
      height:18px;
      stroke:currentColor;
      stroke-width:2;
      stroke-linecap:round;
      fill:none;
    }
    @media (max-width: 820px) {
      :root {
        --matrix-mobile-grid:minmax(0, 1.16fr) minmax(54px, .86fr) minmax(48px, .74fr) minmax(50px, .74fr);
      }
      body.mobile-menu-open {
        overflow:hidden;
      }
      .topbar {
        align-items:center;
        justify-content:flex-start;
      }
      .shell-header {
        z-index:220;
      }
      .shell-header .brand {
        display:none;
      }
      .mobile-menu-toggle {
        display:inline-flex;
        align-items:center;
        justify-content:center;
        width:34px;
        height:34px;
        border:1px solid var(--line);
        border-radius:999px;
        background:var(--surface);
        color:var(--ink);
        cursor:pointer;
        flex:0 0 auto;
      }
      .mobile-menu-backdrop {
        display:block;
        position:fixed;
        inset:0;
        z-index:180;
        background:rgba(0,0,0,.32);
        opacity:0;
        pointer-events:none;
        transition:opacity .18s ease;
      }
      body.mobile-menu-open .mobile-menu-backdrop {
        opacity:1;
        pointer-events:auto;
      }
      .top-actions {
        position:fixed;
        top:0;
        left:0;
        right:auto;
        bottom:0;
        z-index:260;
        width:min(320px, 86vw);
        padding:14px;
        display:flex;
        flex-direction:column;
        align-items:stretch;
        justify-content:flex-start;
        gap:10px;
        background:var(--surface);
        border-right:1px solid var(--line-soft);
        box-shadow:18px 0 42px rgba(0,0,0,.18);
        transform:translateX(-105%);
        transition:transform .22s cubic-bezier(.2,.8,.2,1);
        pointer-events:auto;
        touch-action:pan-y;
      }
      body.mobile-menu-open .top-actions {
        transform:translateX(0);
      }
      .mobile-drawer-brand {
        display:flex;
        align-items:flex-start;
        justify-content:space-between;
        gap:12px;
        padding:4px 0 12px;
        border-bottom:1px solid var(--line-soft);
        margin-bottom:4px;
      }
      .mobile-drawer-brand strong,
      .mobile-drawer-brand span {
        display:block;
        letter-spacing:0;
      }
      .mobile-drawer-brand strong {
        color:var(--ink);
        font-size:18px;
        line-height:1.1;
      }
      .mobile-drawer-brand span {
        margin-top:3px;
        color:var(--faint);
        font-size:12px;
      }
      .mobile-menu-close {
        display:inline-flex;
        align-items:center;
        justify-content:center;
        width:34px;
        height:34px;
        border:1px solid var(--line);
        border-radius:999px;
        background:var(--surface-soft);
        color:var(--muted);
        cursor:pointer;
        flex:0 0 auto;
      }
      .top-actions .live-chip,
      .top-actions .theme-toggle,
      .top-actions .notify-toggle,
      .top-actions .smoke-toggle {
        width:100%;
        height:42px;
        justify-content:center;
        border-radius:14px;
        font-size:12px;
        pointer-events:auto;
        touch-action:manipulation;
      }
      .top-actions .live-chip {
        max-width:none;
      }
      .top-actions .notify-toggle,
      .top-actions .smoke-toggle {
        padding:0;
        gap:8px;
      }
      .top-actions .cta-feedback {
        box-shadow:0 0 0 2px rgba(16,163,127,.18);
        border-color:rgba(16,163,127,.42);
      }
      .top-actions .notify-toggle::after {
        content:"Notifications";
        font-size:12px;
        font-weight:600;
      }
      .top-actions .smoke-toggle::after {
        content:"Smoke test";
        font-size:12px;
        font-weight:600;
      }
      .top-actions .icon-stack {
        display:none;
      }
      .filterbar {
        display:grid;
        grid-template-columns:repeat(4, minmax(0, 1fr));
        gap:5px;
        overflow:visible;
        padding:4px;
        border-radius:16px;
      }
      .filter-zone {
        width:100%;
        display:grid;
        grid-template-columns:minmax(92px, .62fr) minmax(0, 1fr);
        align-items:stretch;
        gap:6px;
      }
      .watchlist-filter {
        min-height:36px;
        padding:7px 5px;
        font-size:9px;
      }
      .filter-card {
        min-width:0;
        width:100%;
        flex:initial;
        padding:8px 4px;
        gap:4px;
        font-size:10.5px;
      }
      .filter-count {
        min-width:18px;
        padding:2px 4px;
        font-size:9px;
      }
      .list-header {
        display:grid;
        grid-template-columns:var(--matrix-mobile-grid);
        gap:6px;
        padding:8px 50px 8px 10px;
        font-size:9.5px;
        border-bottom:1px solid var(--line-soft);
      }
      .list-header button {
        justify-content:center;
        text-align:center;
        min-width:0;
      }
      .list-header button:first-child {
        justify-content:flex-start;
        text-align:left;
      }
      .row-main {
        display:grid;
        grid-template-columns:var(--matrix-mobile-grid);
        grid-template-areas:none;
        gap:6px;
        min-height:auto;
        padding:10px 50px 10px 10px;
      }
      .watchlist-toggle {
        width:38px;
        font-size:16px;
        z-index:3;
      }
      .symbol-cell {
        grid-area:auto;
        justify-content:flex-start;
        gap:7px;
      }
      .row-main > div:nth-child(2),
      .row-main > .age,
      .row-main > .move {
        grid-area:auto;
        justify-content:center;
        text-align:center;
        padding-left:0;
      }
      .row-main > .move {
        justify-content:center;
        text-align:center;
      }
      .accent-line {
        width:3px;
        height:26px;
      }
      .symbol {
        font-size:11.5px;
      }
      .status-chip {
        min-width:0;
        width:100%;
        max-width:68px;
        padding:4px 5px;
        font-size:8.8px;
      }
      .age, .move {
        font-size:9.2px;
        line-height:1.2;
      }
      .detail-grid {
        display:grid;
        grid-template-columns:var(--matrix-mobile-grid);
        gap:6px;
        align-items:center;
      }
      .detail-row + .detail-row {
        margin-top:7px;
        padding-top:7px;
      }
      .detail-cell,
      .detail-grid > .detail-cell:nth-child(4n + 1) {
        justify-content:center;
        text-align:center;
        font-size:8.2px;
      }
      .detail-grid > .detail-cell:first-child {
        justify-content:flex-start;
        text-align:left;
      }
      .detail-date-time {
        flex-direction:column;
        align-items:flex-start;
        justify-content:center;
        gap:1px;
        line-height:1.15;
      }
      .detail-date::after {
        content:"";
      }
      .detail-transition {
        justify-content:center;
        gap:2px;
        overflow:visible;
      }
      .regime-chip {
        min-width:0;
        width:31px;
        padding:2px 1px;
        font-size:5.8px;
      }
      .transition-arrow {
        font-size:6px;
      }
      .price-box {
        width:100%;
        justify-content:center;
        flex-direction:column;
        gap:1px;
      }
      .price-label,
      .price {
        font-size:8px;
        line-height:1.15;
      }
      .neutral-move {
        grid-column:4;
        grid-row:1 / span 2;
        align-self:stretch;
        border-top:0;
        border-left:1px solid var(--line-soft);
        padding:0 0 0 6px;
        display:flex;
        align-items:center;
        justify-content:center;
        text-align:center;
        font-size:8.2px;
      }
    }
    @media (max-width: 520px) {
      :root {
        --matrix-mobile-grid:minmax(0, 1.18fr) minmax(45px, .75fr) minmax(42px, .68fr) minmax(44px, .68fr);
      }
      .topbar {
        min-height:50px;
      }
      .top-actions .theme-toggle {
        display:inline-flex;
      }
      .brand-title {
        font-size:16px;
      }
      .filter-card {
        font-size:9.5px;
        padding:7px 3px;
      }
      .filter-count {
        min-width:16px;
        font-size:8px;
      }
      .list-header {
        padding:7px 46px 7px 8px;
        gap:4px;
        font-size:8px;
      }
      .row-main {
        padding:9px 46px 9px 8px;
        gap:4px;
      }
      .watchlist-toggle {
        width:36px;
        font-size:15px;
        z-index:3;
      }
      .symbol {
        font-size:10px;
      }
      .symbol-cell {
        gap:5px;
      }
      .accent-line {
        height:24px;
      }
      .status-chip {
        max-width:52px;
        font-size:7.4px;
        padding:3px 3px;
      }
      .age, .move {
        font-size:8px;
      }
      .detail-panel,
      .signal-card.expanded .detail-panel {
        padding-left:8px;
        padding-right:8px;
      }
      .detail-grid {
        gap:4px;
      }
      .detail-cell,
      .detail-grid > .detail-cell:nth-child(4n + 1),
      .neutral-move {
        font-size:7.3px;
      }
      .regime-chip {
        width:26px;
        font-size:5px;
      }
      .price-label,
      .price {
        font-size:7px;
      }
    }

    /* Matrix layout-stability hotfix: toggling Watchlist must not resize filters or overview cards. */
    .hero-row {
      display:grid;
      grid-template-columns:minmax(220px, 1fr) 548px;
      align-items:end;
      min-height:58px;
    }
    .filter-zone {
      width:100%;
      min-width:0;
      display:grid;
      grid-template-columns:118px 422px;
      align-items:stretch;
      justify-self:end;
      gap:8px;
      min-height:42px;
    }
    .watchlist-filter,
    .filter-card {
      width:100%;
      min-width:0;
      height:34px;
      box-sizing:border-box;
      overflow:hidden;
    }
    .filterbar {
      width:100%;
      min-width:0;
      display:grid;
      grid-template-columns:repeat(4, minmax(0, 1fr));
      overflow:hidden;
      box-sizing:border-box;
    }
    .filter-card span:first-child,
    .watchlist-filter span:first-child {
      flex:1 1 auto;
      min-width:0;
      overflow:hidden;
      text-align:center;
      text-overflow:ellipsis;
    }
    .filter-count {
      width:28px;
      min-width:28px;
      flex:0 0 28px;
      text-align:center;
      font-variant-numeric:tabular-nums;
    }
    @media (min-width: 641px) and (max-width: 1080px) {
      .hero-row {
        grid-template-columns:minmax(220px, 1fr) 548px;
      }
      .side-rail {
        display:grid;
        grid-template-columns:minmax(0, 1fr) minmax(0, 1fr);
        grid-auto-rows:154px;
        align-items:stretch;
        gap:10px;
        margin-bottom:0;
      }
      .overview-card,
      .performer-card {
        height:154px;
        min-height:154px;
        box-sizing:border-box;
        overflow:hidden;
        padding:14px;
      }
      .overview-card p,
      .performer-position {
        display:-webkit-box;
        -webkit-line-clamp:2;
        -webkit-box-orient:vertical;
        overflow:hidden;
      }
    }
    @media (max-width: 820px) {
      .shell-header {
        min-height:52px;
        z-index:320;
      }
      .topbar {
        display:grid;
        grid-template-columns:40px minmax(0, 1fr);
        justify-content:start;
        align-items:center;
      }
      .mobile-menu-toggle {
        display:inline-flex !important;
        visibility:visible;
        opacity:1;
        position:relative;
        z-index:340;
        grid-column:1;
      }
      .top-actions {
        z-index:360;
      }
      .top-actions .theme-toggle,
      .top-actions .notify-toggle,
      .top-actions .smoke-toggle,
      .top-actions .live-chip {
        display:inline-flex !important;
      }
      .hero-row {
        grid-template-columns:1fr;
        align-items:start;
        min-height:auto;
      }
      .filter-zone {
        justify-self:stretch;
        grid-template-columns:116px minmax(0, 1fr);
        gap:6px;
      }
    }
    @media (max-width: 640px) {
      .side-rail {
        display:grid;
        grid-template-columns:1fr;
        grid-auto-rows:116px;
        gap:8px;
        margin-bottom:0;
      }
      .overview-card,
      .performer-card {
        height:116px;
        min-height:116px;
        padding:12px;
        overflow:hidden;
        box-sizing:border-box;
      }
      .overview-card p,
      .metric-row,
      .performer-position {
        display:-webkit-box;
        -webkit-line-clamp:2;
        -webkit-box-orient:vertical;
        overflow:hidden;
      }
    }
    @media (max-width: 520px) {
      .filter-zone {
        grid-template-columns:100px minmax(0, 1fr);
        gap:5px;
      }
      .filter-count {
        width:20px;
        min-width:20px;
        flex-basis:20px;
      }
    }

    /* Matrix final ChatGPT-style responsive reset.
       This block intentionally sits last so older experimental media rules cannot fight the layout. */
    :root {
      --matrix-row-grid:minmax(94px, 1.12fr) minmax(70px, .86fr) minmax(62px, .74fr) minmax(64px, .74fr);
    }
    * { box-sizing:border-box; }
    html, body {
      width:100%;
      max-width:100%;
      overflow-x:hidden;
    }
    .shell-header {
      position:sticky !important;
      top:0 !important;
      z-index:340 !important;
      width:100% !important;
    }
    .topbar {
      width:100% !important;
      max-width:1180px !important;
      min-height:56px !important;
      margin:0 auto !important;
      padding:10px 16px !important;
      display:flex !important;
      align-items:center !important;
      justify-content:space-between !important;
      gap:12px !important;
    }
    .brand {
      min-width:0 !important;
      display:flex !important;
      align-items:center !important;
      gap:12px !important;
    }
    .top-actions {
      position:static !important;
      inset:auto !important;
      width:auto !important;
      min-width:0 !important;
      padding:0 !important;
      transform:none !important;
      display:flex !important;
      flex-direction:row !important;
      align-items:center !important;
      justify-content:flex-end !important;
      gap:8px !important;
      background:transparent !important;
      border:0 !important;
      box-shadow:none !important;
      z-index:auto !important;
      pointer-events:auto !important;
    }
    .mobile-menu-toggle,
    .mobile-menu-close,
    .mobile-drawer-brand,
    .mobile-menu-backdrop {
      display:none !important;
    }
    .page {
      width:100% !important;
      max-width:1180px !important;
      margin:0 auto !important;
      padding:14px 16px 0 !important;
      display:grid !important;
      grid-template-columns:minmax(0, 1fr) 280px !important;
      align-items:start !important;
      gap:16px !important;
    }
    .content {
      min-width:0 !important;
      order:0 !important;
    }
    .side-rail {
      min-width:0 !important;
      order:0 !important;
      display:grid !important;
      grid-template-columns:1fr !important;
      gap:12px !important;
      padding-top:2px !important;
      margin:0 !important;
      align-self:start !important;
    }
    .overview-card,
    .performer-card {
      min-height:158px !important;
      height:auto !important;
      padding:14px !important;
      overflow:hidden !important;
    }
    .overview-card {
      display:grid !important;
      grid-template-rows:auto minmax(0, auto) auto 6px auto !important;
      align-content:start !important;
      row-gap:8px !important;
    }
    .overview-card h2,
    .overview-card p,
    .metric-row,
    .move-pair,
    .performer-card > * {
      min-width:0 !important;
    }
    .overview-card h2,
    .overview-card p {
      margin:0 !important;
    }
    .overview-card p {
      display:block !important;
      overflow:visible !important;
      -webkit-line-clamp:unset !important;
      -webkit-box-orient:initial !important;
      line-height:1.45 !important;
    }
    .metric-row {
      display:grid !important;
      grid-template-columns:minmax(0, 1fr) auto !important;
      align-items:center !important;
      gap:10px !important;
      margin:0 !important;
    }
    .bar {
      margin:0 !important;
    }
    .move-pair {
      margin-top:0 !important;
      display:grid !important;
      grid-template-columns:repeat(2, minmax(0, 1fr)) !important;
      align-items:stretch !important;
    }
    .move-metric {
      min-width:0 !important;
      overflow:hidden !important;
    }
    .move-metric-label,
    .performer-eyebrow,
    .performer-position {
      overflow:hidden !important;
      text-overflow:ellipsis !important;
    }
    .performer-card {
      display:flex !important;
      flex-direction:column !important;
      align-items:flex-start !important;
      justify-content:flex-start !important;
      gap:8px !important;
    }
    .performer-name,
    .performer-move,
    .performer-position {
      max-width:100% !important;
    }
    .performer-name {
      overflow:hidden !important;
      text-overflow:ellipsis !important;
      white-space:nowrap !important;
    }
    .hero-row {
      width:100% !important;
      min-height:62px !important;
      display:grid !important;
      grid-template-columns:minmax(220px, 1fr) minmax(500px, 548px) !important;
      align-items:end !important;
      gap:12px !important;
    }
    .hero-row > div:first-child {
      min-width:0 !important;
    }
    .filter-zone {
      width:100% !important;
      max-width:548px !important;
      min-width:0 !important;
      min-height:44px !important;
      justify-self:end !important;
      display:grid !important;
      grid-template-columns:118px minmax(0, 1fr) !important;
      align-items:stretch !important;
      gap:8px !important;
    }
    .filterbar {
      width:100% !important;
      min-width:0 !important;
      height:44px !important;
      min-height:44px !important;
      display:grid !important;
      grid-template-columns:repeat(4, minmax(0, 1fr)) !important;
      align-items:stretch !important;
      gap:4px !important;
      padding:4px !important;
      overflow:hidden !important;
    }
    .watchlist-filter,
    .filter-card {
      width:100% !important;
      min-width:0 !important;
      height:34px !important;
      min-height:34px !important;
      padding:6px 6px !important;
      display:flex !important;
      align-items:center !important;
      justify-content:center !important;
      gap:5px !important;
      overflow:hidden !important;
      white-space:nowrap !important;
      letter-spacing:0 !important;
    }
    .watchlist-filter {
      height:44px !important;
      min-height:44px !important;
    }
    .filter-card span:first-child,
    .watchlist-filter span:first-child {
      min-width:0 !important;
      overflow:hidden !important;
      text-align:center !important;
      text-overflow:ellipsis !important;
    }
    .filter-count {
      flex:0 0 28px !important;
      width:28px !important;
      min-width:28px !important;
      padding:2px 0 !important;
      text-align:center !important;
      font-variant-numeric:tabular-nums !important;
    }
    .list-header,
    .row-main,
    .detail-grid {
      grid-template-columns:var(--matrix-row-grid) !important;
    }
    .list-header {
      display:grid !important;
      gap:8px !important;
      padding:8px 52px 8px 12px !important;
      align-items:center !important;
    }
    .list-header button,
    .row-main > div {
      min-width:0 !important;
    }
    .list-header button:first-child,
    .symbol-cell {
      justify-content:flex-start !important;
      text-align:left !important;
    }
    .row-main {
      display:grid !important;
      width:100% !important;
      min-height:46px !important;
      gap:8px !important;
      padding:10px 52px 10px 12px !important;
      align-items:center !important;
    }
    .row-main > div:not(.symbol-cell),
    .list-header button:not(:first-child) {
      justify-content:center !important;
      text-align:center !important;
    }
    .watchlist-toggle {
      width:40px !important;
      z-index:8 !important;
      border-radius:0 !important;
      touch-action:manipulation !important;
    }
    .symbol,
    .age,
    .move,
    .status-chip {
      min-width:0 !important;
    }
    @media (max-width: 1080px) {
      .topbar {
        max-width:100% !important;
      }
      .page {
        display:flex !important;
        flex-direction:column !important;
        gap:12px !important;
      }
      .content {
        order:2 !important;
        width:100% !important;
      }
      .side-rail {
        order:1 !important;
        width:100% !important;
        display:grid !important;
        grid-template-columns:repeat(2, minmax(0, 1fr)) !important;
        grid-auto-rows:190px !important;
        gap:10px !important;
        padding-top:0 !important;
        margin-top:4px !important;
      }
      .overview-card,
      .performer-card {
        height:190px !important;
        min-height:190px !important;
        padding:14px !important;
      }
      .overview-card p,
      .performer-position {
        display:-webkit-box !important;
        -webkit-line-clamp:2 !important;
        -webkit-box-orient:vertical !important;
        overflow:hidden !important;
      }
      .hero-row {
        grid-template-columns:minmax(220px, 1fr) minmax(460px, 548px) !important;
      }
      .filter-zone {
        max-width:548px !important;
      }
    }
    @media (max-width: 820px) {
      :root {
        --matrix-row-grid:minmax(86px, 1.12fr) minmax(60px, .82fr) minmax(50px, .72fr) minmax(54px, .72fr);
      }
      body.mobile-menu-open {
        overflow:hidden !important;
      }
      .shell-header {
        min-height:52px !important;
        z-index:1180 !important;
      }
      .topbar {
        min-height:52px !important;
        padding:8px 12px !important;
        justify-content:flex-start !important;
      }
      .shell-header .brand,
      .nav-pill {
        display:none !important;
      }
      .mobile-menu-toggle {
        display:inline-flex !important;
        align-items:center !important;
        justify-content:center !important;
        width:36px !important;
        height:36px !important;
        flex:0 0 36px !important;
        border:1px solid var(--line) !important;
        border-radius:999px !important;
        background:var(--surface) !important;
        color:var(--ink) !important;
        cursor:pointer !important;
        position:relative !important;
        z-index:1220 !important;
      }
      .mobile-menu-backdrop {
        display:block !important;
        position:fixed !important;
        inset:0 !important;
        z-index:1080 !important;
        background:rgba(0,0,0,.32) !important;
        opacity:0 !important;
        pointer-events:none !important;
        transition:opacity .18s ease !important;
      }
      body.mobile-menu-open .mobile-menu-backdrop {
        opacity:1 !important;
        pointer-events:auto !important;
      }
      .top-actions {
        position:fixed !important;
        top:0 !important;
        left:0 !important;
        right:auto !important;
        bottom:0 !important;
        z-index:1200 !important;
        width:min(318px, 86vw) !important;
        height:100vh !important;
        padding:14px !important;
        transform:translateX(-105%) !important;
        display:flex !important;
        flex-direction:column !important;
        align-items:stretch !important;
        justify-content:flex-start !important;
        gap:10px !important;
        background:var(--surface) !important;
        border-right:1px solid var(--line-soft) !important;
        border-left:0 !important;
        box-shadow:18px 0 42px rgba(0,0,0,.18) !important;
        pointer-events:auto !important;
        touch-action:pan-y !important;
        transition:transform .22s cubic-bezier(.2,.8,.2,1) !important;
      }
      body.mobile-menu-open .top-actions {
        transform:translateX(0) !important;
      }
      .mobile-drawer-brand {
        display:flex !important;
        align-items:flex-start !important;
        justify-content:space-between !important;
        gap:12px !important;
        padding:4px 0 12px !important;
        border-bottom:1px solid var(--line-soft) !important;
      }
      .mobile-drawer-brand strong,
      .mobile-drawer-brand span {
        display:block !important;
        letter-spacing:0 !important;
      }
      .mobile-drawer-brand strong {
        color:var(--ink) !important;
        font-size:18px !important;
        line-height:1.1 !important;
      }
      .mobile-drawer-brand span {
        margin-top:3px !important;
        color:var(--faint) !important;
        font-size:12px !important;
      }
      .mobile-menu-close {
        display:inline-flex !important;
        align-items:center !important;
        justify-content:center !important;
        width:34px !important;
        height:34px !important;
        flex:0 0 34px !important;
        border:1px solid var(--line) !important;
        border-radius:999px !important;
        background:var(--surface-soft) !important;
        color:var(--muted) !important;
        cursor:pointer !important;
        position:relative !important;
        z-index:1210 !important;
      }
      .top-actions .live-chip,
      .top-actions .theme-toggle,
      .top-actions .notify-toggle,
      .top-actions .smoke-toggle {
        width:100% !important;
        max-width:none !important;
        height:42px !important;
        display:inline-flex !important;
        justify-content:center !important;
        border-radius:14px !important;
        font-size:12px !important;
        pointer-events:auto !important;
      }
      .top-actions .icon-stack {
        display:none !important;
      }
      .page {
        padding:10px 10px 0 !important;
      }
      .hero-row {
        grid-template-columns:1fr !important;
        min-height:auto !important;
        align-items:start !important;
        gap:10px !important;
      }
      .filter-zone {
        max-width:none !important;
        grid-template-columns:104px minmax(0, 1fr) !important;
        justify-self:stretch !important;
        min-height:44px !important;
        gap:6px !important;
      }
      .filterbar {
        height:44px !important;
        min-height:44px !important;
        gap:4px !important;
      }
      .filter-card {
        height:34px !important;
        min-height:34px !important;
        padding:6px 4px !important;
        font-size:8.5px !important;
      }
      .watchlist-filter {
        height:44px !important;
        min-height:44px !important;
        padding:6px 4px !important;
        font-size:8.5px !important;
      }
      .filter-count {
        flex-basis:22px !important;
        width:22px !important;
        min-width:22px !important;
        font-size:8px !important;
      }
      .list-header {
        display:grid !important;
        gap:6px !important;
        padding:8px 48px 8px 10px !important;
        font-size:8.5px !important;
      }
      .row-main {
        min-height:44px !important;
        gap:6px !important;
        padding:10px 48px 10px 10px !important;
      }
      .symbol {
        font-size:10.5px !important;
      }
      .status-chip {
        min-width:54px !important;
        padding:4px 5px !important;
        font-size:8px !important;
      }
      .age,
      .move {
        font-size:8.5px !important;
      }
      .watchlist-toggle {
        width:38px !important;
        font-size:16px !important;
      }
      .detail-grid {
        gap:6px !important;
      }
      .detail-cell,
      .neutral-move,
      .price-label,
      .price {
        font-size:8px !important;
      }
      .side-rail {
        grid-template-columns:repeat(2, minmax(0, 1fr)) !important;
        grid-auto-rows:174px !important;
      }
      .overview-card,
      .performer-card {
        height:174px !important;
        min-height:174px !important;
        padding:12px !important;
      }
    }
    @media (max-width: 560px) {
      :root {
        --matrix-row-grid:minmax(74px, 1.08fr) minmax(52px, .82fr) minmax(42px, .68fr) minmax(46px, .68fr);
      }
      h1 {
        font-size:19px !important;
      }
      .submeta {
        font-size:9px !important;
      }
      .filter-zone {
        grid-template-columns:92px minmax(0, 1fr) !important;
        min-height:42px !important;
        gap:5px !important;
      }
      .filterbar {
        height:42px !important;
        min-height:42px !important;
      }
      .filter-card {
        height:32px !important;
        min-height:32px !important;
        padding:5px 3px !important;
        font-size:7.8px !important;
        gap:3px !important;
      }
      .watchlist-filter {
        height:42px !important;
        min-height:42px !important;
        padding:5px 3px !important;
        font-size:7.8px !important;
        gap:3px !important;
      }
      .filter-count {
        flex-basis:18px !important;
        width:18px !important;
        min-width:18px !important;
        font-size:7px !important;
      }
      .list-header {
        padding:8px 44px 8px 8px !important;
        gap:4px !important;
        font-size:7.5px !important;
      }
      .row-main {
        padding:9px 44px 9px 8px !important;
        gap:4px !important;
      }
      .watchlist-toggle {
        width:36px !important;
      }
      .accent-line {
        width:2px !important;
        height:24px !important;
      }
      .symbol-cell {
        gap:5px !important;
      }
      .symbol {
        font-size:9.2px !important;
      }
      .status-chip {
        min-width:46px !important;
        padding:3px 4px !important;
        font-size:7px !important;
      }
      .age,
      .move {
        font-size:7.5px !important;
      }
      .side-rail {
        grid-template-columns:1fr !important;
        grid-auto-rows:166px !important;
      }
      .overview-card,
      .performer-card {
        height:166px !important;
        min-height:166px !important;
      }
      .overview-card {
        row-gap:7px !important;
      }
      .overview-card h2 {
        font-size:14px !important;
      }
      .overview-card p,
      .metric-row,
      .performer-position {
        font-size:9px !important;
        line-height:1.35 !important;
      }
      .move-pair {
        gap:6px !important;
      }
      .move-metric {
        padding:7px !important;
      }
      .move-metric-label {
        font-size:7px !important;
        white-space:nowrap !important;
      }
      .move-metric-value {
        font-size:12px !important;
      }
      .performer-name {
        font-size:17px !important;
      }
      .performer-move {
        font-size:14px !important;
      }
      .detail-panel,
      .signal-card.expanded .detail-panel {
        padding-left:8px !important;
        padding-right:8px !important;
      }
      .detail-grid {
        gap:4px !important;
      }
      .detail-cell,
      .neutral-move,
      .price-label,
      .price {
        font-size:7px !important;
      }
    }
    @media (max-width: 1080px) {
      .overview-card {
        grid-template-rows:auto auto auto 6px auto !important;
      }
      .overview-card p {
        display:block !important;
        overflow:visible !important;
        -webkit-line-clamp:unset !important;
        -webkit-box-orient:initial !important;
        line-height:1.35 !important;
        min-height:1.35em !important;
      }
    }
    .alert-stack {
      z-index:1600 !important;
      pointer-events:none !important;
    }
    .instant-alert {
      pointer-events:none !important;
    }
    .header-notifications {
      position:relative !important;
      width:34px !important;
      height:34px !important;
      flex:0 0 34px !important;
      display:inline-flex !important;
      align-items:center !important;
      justify-content:center !important;
      border:1px solid var(--line) !important;
      border-radius:999px !important;
      background:var(--surface-soft) !important;
      color:var(--muted) !important;
      text-decoration:none !important;
      transition:background .16s ease, color .16s ease, border-color .16s ease !important;
    }
    .header-notifications:hover,
    .header-notifications:focus-visible {
      color:var(--primary) !important;
      border-color:rgba(99,91,255,.34) !important;
      background:rgba(99,91,255,.10) !important;
      outline:0 !important;
    }
    .header-notifications svg {
      width:16px !important;
      height:16px !important;
      stroke:currentColor !important;
      stroke-width:2 !important;
      stroke-linecap:round !important;
      stroke-linejoin:round !important;
      fill:none !important;
    }
    .notification-badge {
      position:absolute !important;
      top:-5px !important;
      right:-5px !important;
      min-width:17px !important;
      height:17px !important;
      padding:0 4px !important;
      border-radius:999px !important;
      display:inline-flex !important;
      align-items:center !important;
      justify-content:center !important;
      background:var(--danger) !important;
      color:#fff !important;
      font-size:8px !important;
      font-weight:850 !important;
      line-height:1 !important;
      font-variant-numeric:tabular-nums !important;
      box-shadow:0 0 0 2px var(--surface) !important;
    }
    .notification-badge.empty {
      display:none !important;
    }
    .sidebar-account {
      display:none !important;
    }
    @media (max-width: 820px) {
      .topbar {
        justify-content:space-between !important;
      }
      .header-notifications {
        margin-left:auto !important;
        margin-right:0 !important;
        z-index:560 !important;
      }
      body.mobile-menu-open .mobile-menu-toggle {
        opacity:0 !important;
        visibility:hidden !important;
        pointer-events:none !important;
        z-index:1000 !important;
      }
      .sidebar-account {
        margin-top:auto !important;
        display:block !important;
        border-top:1px solid var(--line-soft) !important;
        padding-top:12px !important;
      }
      .account-toggle {
        width:100% !important;
        min-height:52px !important;
        display:grid !important;
        grid-template-columns:34px minmax(0, 1fr) 18px !important;
        align-items:center !important;
        gap:10px !important;
        border:1px solid var(--line-soft) !important;
        border-radius:14px !important;
        background:var(--surface-soft) !important;
        color:var(--ink) !important;
        cursor:pointer !important;
        padding:8px !important;
        text-align:left !important;
      }
      .account-avatar {
        width:34px !important;
        height:34px !important;
        border-radius:999px !important;
        display:inline-flex !important;
        align-items:center !important;
        justify-content:center !important;
        background:var(--primary-hot) !important;
        color:#fff !important;
        font-weight:850 !important;
        font-size:12px !important;
      }
      .account-copy {
        min-width:0 !important;
        display:flex !important;
        flex-direction:column !important;
        gap:2px !important;
      }
      .account-copy strong,
      .account-copy small {
        min-width:0 !important;
        overflow:hidden !important;
        text-overflow:ellipsis !important;
        white-space:nowrap !important;
        letter-spacing:0 !important;
      }
      .account-copy strong {
        color:var(--ink) !important;
        font-size:12px !important;
      }
      .account-copy small {
        color:var(--faint) !important;
        font-size:10px !important;
      }
      .account-chevron {
        color:var(--faint) !important;
        font-size:13px !important;
        transform:rotate(0deg) !important;
        transition:transform .16s ease !important;
      }
      .sidebar-account.open .account-chevron {
        transform:rotate(180deg) !important;
      }
      .account-panel {
        display:none !important;
        grid-template-columns:1fr !important;
        gap:6px !important;
        margin-bottom:8px !important;
      }
      .sidebar-account.open .account-panel {
        display:grid !important;
      }
      .account-action {
        width:100% !important;
        min-height:38px !important;
        border:1px solid var(--line-soft) !important;
        border-radius:12px !important;
        background:transparent !important;
        color:var(--muted) !important;
        display:flex !important;
        align-items:center !important;
        justify-content:flex-start !important;
        padding:0 12px !important;
        text-decoration:none !important;
        font-size:12px !important;
        font-weight:700 !important;
        cursor:pointer !important;
      }
      .account-action:hover,
      .account-action:focus-visible {
        color:var(--ink) !important;
        background:var(--surface-soft) !important;
        outline:0 !important;
      }
      .sidebar-account.guest .logout-action {
        display:none !important;
      }
    }
    /* Matrix all-screen sidebar header reset.
       The header keeps only menu, centered logo on desktop, and notifications. */
    .topbar {
      display:grid !important;
      grid-template-columns:42px minmax(0, 1fr) 42px !important;
      align-items:center !important;
      justify-content:normal !important;
      gap:10px !important;
    }
    .mobile-menu-toggle {
      grid-column:1 !important;
      grid-row:1 !important;
      justify-self:start !important;
      display:inline-flex !important;
      align-items:center !important;
      justify-content:center !important;
      width:36px !important;
      height:36px !important;
      border:1px solid var(--line) !important;
      border-radius:999px !important;
      background:var(--surface) !important;
      color:var(--ink) !important;
      cursor:pointer !important;
      position:relative !important;
      z-index:1220 !important;
    }
    .brand {
      grid-column:2 !important;
      grid-row:1 !important;
      justify-self:center !important;
      display:flex !important;
      align-items:center !important;
      justify-content:center !important;
      min-width:0 !important;
      text-align:center !important;
    }
    .brand-title {
      align-items:center !important;
      text-align:center !important;
    }
    .nav-pill {
      display:none !important;
    }
    .header-notifications {
      grid-column:3 !important;
      grid-row:1 !important;
      justify-self:end !important;
      margin:0 !important;
      z-index:560 !important;
    }
    .mobile-menu-backdrop {
      display:block !important;
      position:fixed !important;
      inset:0 !important;
      z-index:1080 !important;
      background:rgba(0,0,0,.32) !important;
      opacity:0 !important;
      pointer-events:none !important;
      transition:opacity .18s ease !important;
    }
    body.mobile-menu-open {
      overflow:hidden !important;
      overscroll-behavior:none !important;
    }
    body.mobile-menu-open .shell-header {
      z-index:1300 !important;
    }
    body.mobile-menu-open .mobile-menu-backdrop {
      opacity:1 !important;
      pointer-events:auto !important;
    }
    body.mobile-menu-open .mobile-menu-toggle {
      opacity:0 !important;
      visibility:hidden !important;
      pointer-events:none !important;
    }
    .top-actions {
      position:fixed !important;
      top:0 !important;
      left:0 !important;
      right:auto !important;
      bottom:0 !important;
      z-index:1320 !important;
      width:min(320px, 86vw) !important;
      height:100vh !important;
      height:100dvh !important;
      max-height:100dvh !important;
      min-width:0 !important;
      padding:14px !important;
      overflow-y:auto !important;
      overscroll-behavior:contain !important;
      -webkit-overflow-scrolling:touch !important;
      transform:translateX(-105%) !important;
      display:flex !important;
      flex-direction:column !important;
      align-items:stretch !important;
      justify-content:flex-start !important;
      gap:10px !important;
      background:var(--surface) !important;
      border:0 !important;
      border-right:1px solid var(--line-soft) !important;
      box-shadow:18px 0 42px rgba(0,0,0,.18) !important;
      pointer-events:auto !important;
      touch-action:pan-y !important;
      transition:transform .22s cubic-bezier(.2,.8,.2,1) !important;
    }
    body.mobile-menu-open .top-actions {
      transform:translateX(0) !important;
    }
    .mobile-drawer-brand {
      display:flex !important;
      align-items:flex-start !important;
      justify-content:space-between !important;
      gap:12px !important;
      padding:4px 0 12px !important;
      border-bottom:1px solid var(--line-soft) !important;
      margin-bottom:4px !important;
    }
    .mobile-drawer-brand strong,
    .mobile-drawer-brand span {
      display:block !important;
      letter-spacing:0 !important;
    }
    .mobile-drawer-brand strong {
      color:var(--ink) !important;
      font-size:18px !important;
      line-height:1.1 !important;
    }
    .mobile-drawer-brand span {
      margin-top:3px !important;
      color:var(--faint) !important;
      font-size:12px !important;
    }
    .mobile-menu-close {
      display:inline-flex !important;
      align-items:center !important;
      justify-content:center !important;
      width:34px !important;
      height:34px !important;
      flex:0 0 34px !important;
      border:1px solid var(--line) !important;
      border-radius:999px !important;
      background:var(--surface-soft) !important;
      color:var(--muted) !important;
      cursor:pointer !important;
      position:relative !important;
      z-index:1210 !important;
    }
    .top-actions .live-chip {
      display:none !important;
    }
    .sidebar-toggle-row {
      width:100% !important;
      min-height:36px !important;
      display:flex !important;
      align-items:center !important;
      justify-content:space-between !important;
      gap:10px !important;
      border:1px solid var(--line-soft) !important;
      border-radius:999px !important;
      background:var(--surface-soft) !important;
      padding:4px 5px 4px 12px !important;
    }
    .sidebar-toggle-row span {
      color:var(--faint) !important;
      font-size:11px !important;
      font-weight:800 !important;
      letter-spacing:0 !important;
    }
    .top-actions .theme-toggle {
      width:58px !important;
      height:28px !important;
      max-width:58px !important;
      min-width:58px !important;
      padding:0 !important;
      display:inline-flex !important;
      align-items:center !important;
      justify-content:center !important;
      border-radius:999px !important;
      border:1px solid rgba(16,163,127,.28) !important;
      background:rgba(16,163,127,.14) !important;
      color:var(--primary) !important;
      font-size:10px !important;
      font-weight:800 !important;
      pointer-events:auto !important;
      gap:0 !important;
    }
    .top-actions .smoke-toggle {
      width:auto !important;
      max-width:none !important;
      height:32px !important;
      min-height:32px !important;
      align-self:flex-start !important;
      padding:0 12px !important;
      display:inline-flex !important;
      align-items:center !important;
      justify-content:center !important;
      border-radius:999px !important;
      font-size:11px !important;
      font-weight:800 !important;
      pointer-events:auto !important;
      gap:7px !important;
      border:1px solid var(--line) !important;
      background:var(--surface) !important;
      color:var(--muted) !important;
    }
    .top-actions .smoke-toggle::after {
      content:none !important;
    }
    .top-actions .smoke-toggle svg {
      width:13px !important;
      height:13px !important;
    }
    .top-actions .icon-stack {
      display:none !important;
    }
    .sidebar-account {
      margin-top:auto !important;
      display:block !important;
      border-top:1px solid var(--line-soft) !important;
      padding-top:12px !important;
    }
    .account-toggle {
      width:100% !important;
      min-height:52px !important;
      display:grid !important;
      grid-template-columns:34px minmax(0, 1fr) !important;
      align-items:center !important;
      gap:10px !important;
      border:1px solid var(--line-soft) !important;
      border-radius:14px !important;
      background:var(--surface-soft) !important;
      color:var(--ink) !important;
      cursor:pointer !important;
      padding:8px !important;
      text-align:left !important;
    }
    .account-avatar {
      width:34px !important;
      height:34px !important;
      border-radius:999px !important;
      display:inline-flex !important;
      align-items:center !important;
      justify-content:center !important;
      background:var(--primary-hot) !important;
      color:#fff !important;
      font-weight:850 !important;
      font-size:12px !important;
    }
    .account-copy {
      min-width:0 !important;
      display:flex !important;
      flex-direction:column !important;
      gap:2px !important;
    }
    .account-copy strong,
    .account-copy small {
      min-width:0 !important;
      overflow:hidden !important;
      text-overflow:ellipsis !important;
      white-space:nowrap !important;
      letter-spacing:0 !important;
    }
    .account-copy strong {
      color:var(--ink) !important;
      font-size:12px !important;
    }
    .account-copy small {
      color:var(--faint) !important;
      font-size:10px !important;
    }
    .account-panel {
      display:none !important;
      grid-template-columns:1fr !important;
      gap:6px !important;
      margin-bottom:8px !important;
    }
    .sidebar-account.open .account-panel {
      display:grid !important;
    }
    .account-action {
      width:100% !important;
      min-height:38px !important;
      border:1px solid var(--line-soft) !important;
      border-radius:12px !important;
      background:transparent !important;
      color:var(--muted) !important;
      display:flex !important;
      align-items:center !important;
      justify-content:flex-start !important;
      padding:0 12px !important;
      text-decoration:none !important;
      font-size:12px !important;
      font-weight:700 !important;
      cursor:pointer !important;
    }
    .account-action:hover,
    .account-action:focus-visible {
      color:var(--ink) !important;
      background:var(--surface-soft) !important;
      outline:0 !important;
    }
    .sidebar-account.guest .logout-action {
      display:none !important;
    }
    @media (max-width: 820px) {
      .brand {
        display:none !important;
      }
      .topbar {
        grid-template-columns:42px minmax(0, 1fr) 42px !important;
      }
    }
  </style>
</head>
<body>
  <div class="alert-stack" id="instantAlertStack" aria-live="polite" aria-atomic="false"></div>
  <header class="shell-header">
    <div class="topbar">
      <div class="brand">
        <div class="brand-title">
          <span>v2Matrix</span>
          <span>by Viveka</span>
        </div>
        <div class="nav-pill">Market Regimes</div>
      </div>
      <button class="mobile-menu-toggle" id="mobileMenuToggle" type="button" aria-label="Open controls" aria-expanded="false">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M4 7h16M4 12h16M4 17h16"></path>
        </svg>
      </button>
      <a class="header-notifications" id="headerNotifications" href="/v2Matrix/notifications" aria-label="Open notifications">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M10.27 21a2 2 0 0 0 3.46 0"></path>
          <path d="M3.26 15.33A1 1 0 0 0 4 17h16a1 1 0 0 0 .74-1.67C19.41 13.96 18 12.5 18 8a6 6 0 0 0-12 0c0 4.5-1.41 5.96-2.74 7.33"></path>
        </svg>
        <span class="notification-badge empty" id="headerUnreadCount">0</span>
      </a>
      <div class="top-actions">
        <div class="mobile-drawer-brand">
          <div>
            <strong>v2Matrix</strong>
            <span>by Viveka</span>
          </div>
          <button class="mobile-menu-close" id="mobileMenuClose" type="button" aria-label="Close controls">
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path d="M6 6l12 12M18 6 6 18"></path>
            </svg>
          </button>
        </div>
        <div class="live-chip" id="liveBadge"><span class="live-dot"></span><span id="marketState">Checking</span></div>
        <div class="sidebar-toggle-row">
          <span>Theme</span>
          <button class="theme-toggle" id="themeToggle" type="button">Day</button>
        </div>
        <button class="smoke-toggle" id="smokeToggle" type="button" aria-label="Smoke test alert" title="Smoke test alert">
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M13 2 4 14h7l-1 8 9-12h-7l1-8z"></path>
          </svg>
          <span>Smoke</span>
        </button>
        <div class="sidebar-account guest" id="sidebarAccount">
          <div class="account-panel" id="accountPanel">
            <a href="/v2Matrix/" class="account-action">Profile</a>
            <a href="/v2Matrix/notifications" class="account-action">Settings</a>
            <button class="account-action logout-action" id="logoutAction" type="button">Log Out</button>
          </div>
          <button class="account-toggle" id="accountToggle" type="button" aria-expanded="false">
            <span class="account-avatar" id="accountInitials">G</span>
            <span class="account-copy">
              <strong id="accountName">Guest</strong>
              <small id="accountRole">Not signed in</small>
            </span>
          </button>
        </div>
        <div class="icon-stack" aria-hidden="true">
          <span class="icon-chip">i</span>
          <span class="icon-chip">s</span>
          <span class="icon-chip">u</span>
        </div>
      </div>
    </div>
  </header>
  <div class="mobile-menu-backdrop" id="mobileMenuBackdrop" aria-hidden="true"></div>

  <main class="page">
    <section class="content">
      <div class="hero-row">
        <div>
          <h1>Market Regimes</h1>
          <div class="submeta">
            <span id="updated">Updated: Waiting</span>
            <span class="meta-dot"></span>
            <strong id="events">Symbols: 0</strong>
          </div>
        </div>
        <div class="filter-zone" aria-label="Filter instruments">
          <button class="watchlist-filter" id="watchlistFilter" type="button" aria-pressed="false">
            <span>Watchlist</span><span class="filter-count" id="watchlistCount">0</span>
          </button>
          <div class="filterbar" id="filters" aria-label="Regime filters"></div>
        </div>
      </div>

      <div class="list-header" aria-label="Sort instruments">
        <button type="button" data-sort="symbol">Symbol <span class="sort-arrow" id="sortSymbol"></span></button>
        <button type="button" data-sort="status">Status <span class="sort-arrow" id="sortStatus"></span></button>
        <button type="button" data-sort="age">Age <span class="sort-arrow" id="sortAge"></span></button>
        <button type="button" data-sort="move">Move (%) <span class="sort-arrow" id="sortMove"></span></button>
      </div>

      <div id="cards" class="signals">
        <div class="empty">Waiting for market regime data</div>
      </div>
      <div class="scroll-note" id="scrollNote"></div>
    </section>

    <aside class="side-rail">
      <section class="overview-card">
        <h2>Market Overview</h2>
        <p id="overviewText">Waiting for regime distribution.</p>
        <div class="metric-row"><span>Bullish Intensity</span><span class="metric-value" id="bullishIntensity">NA</span></div>
        <div class="bar"><span id="bullishBar"></span></div>
        <div class="move-pair">
          <div class="move-metric">
            <div class="move-metric-label">Avg Bullish Move</div>
            <div class="move-metric-value" id="avgBullishMove">--</div>
          </div>
          <div class="move-metric">
            <div class="move-metric-label">Avg Bearish Move</div>
            <div class="move-metric-value" id="avgBearishMove">--</div>
          </div>
        </div>
      </section>

      <section class="performer-card">
        <div class="performer-eyebrow">Top Performer</div>
        <div class="performer-name" id="topPerformer">NA</div>
        <div class="performer-move" id="topMove">NA</div>
        <div class="performer-position" id="topPosition">NA</div>
      </section>
    </aside>
  </main>

  <footer>
    <div class="footer-inner">
      <div>VIVEKA</div>
      <div>© 2026 Viveka Trading Systems. All rights reserved.</div>
      <div class="footer-links"><span>Terms of Service</span><span>Privacy Policy</span><span>Support</span></div>
    </div>
  </footer>

  <script>
    const expandedIds = new Set();
    let latestData = {};
    let sortKey = "age";
    let sortDir = -1;
    let filterKey = "All";
    let visibleLimit = Number.POSITIVE_INFINITY;
    let signalNotificationsPrimed = false;
    let notificationsEnabled = localStorage.getItem("v2matrix-v1-notifications") === "true";
    let soundEnabled = localStorage.getItem("v2matrix-v1-sound") === "true" || notificationsEnabled;
    let mobileNotificationPromptAttempted = false;
    let watchlistFilterActive = localStorage.getItem("v2matrix-v1-watchlist-filter") === "true";
    let watchlistIds = loadWatchlist();
    let seenSignalKeys = new Map();
    let highlightedSignals = loadSignalHighlights();
    let activeInstantAlertKeys = new Set();
    let pendingInstantAlerts = [];
    let alertAudioContext = null;
    const INITIAL_VISIBLE_LIMIT = Number.POSITIVE_INFINITY;
    const LOAD_MORE_COUNT = Number.POSITIVE_INFINITY;
    const SIGNAL_HIGHLIGHT_MS = 15 * 1000;
    const INSTANT_ALERT_MS = 5 * 1000;
    const HIGHLIGHT_STORAGE_KEY = "v2matrix-v1-signal-highlights";
    const NOTIFICATION_LAST_OPENED_KEY = "v2matrix-v1-notifications-last-opened-ms";
    const filters = ["All", "Bullish", "Bearish", "Neutral"];
    const statusRank = { Bullish: 0, Bearish: 1, Neutral: 2 };
    const missing = value => value === null || value === undefined || value === "" || value === "NA";
    const safe = (value, fallback = "Not available") => missing(value) ? fallback : String(value);
    const esc = value => safe(value).replace(/[&<>"']/g, character => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    }[character]));
    const numberOrNull = value => {
      const number = Number(value);
      return Number.isFinite(number) ? number : null;
    };
    const fmtPricePlain = value => {
      const number = Number(value);
      return Number.isFinite(number) && number > 0 ? number.toFixed(2) : "NA";
    };
    const fmtMove = value => {
      const number = numberOrNull(value);
      if (number === null) return "NA";
      const prefix = number > 0 ? "+" : "";
      return `${prefix}${number.toFixed(2)}%`;
    };
    const moveClass = value => {
      const number = numberOrNull(value);
      if (number === null || number === 0) return "";
      return number > 0 ? "positive" : "negative";
    };
    const moveValue = row => safe(row.regime, "Neutral") === "Neutral" ? null : numberOrNull(row.move_pct);
    const ageEpoch = row => {
      const parsed = Date.parse(row.trigger_time_ist || "");
      return Number.isFinite(parsed) ? parsed : 0;
    };
    const rowId = row => safe(row.instrument_id || row.instrument_name, "unknown");
    function loadWatchlist() {
      try {
        const raw = JSON.parse(localStorage.getItem("v2matrix-v1-watchlist") || "[]");
        return new Set(Array.isArray(raw) ? raw.map(value => String(value)) : []);
      } catch (error) {
        return new Set();
      }
    }
    function saveWatchlist() {
      localStorage.setItem("v2matrix-v1-watchlist", JSON.stringify([...watchlistIds].sort()));
      localStorage.setItem("v2matrix-v1-watchlist-filter", watchlistFilterActive ? "true" : "false");
    }
    function isWatchlisted(row) {
      return watchlistIds.has(rowId(row));
    }
    const filteredRows = rows => {
      const watchRows = watchlistFilterActive ? rows.filter(isWatchlisted) : rows;
      return filterKey === "All" ? watchRows : watchRows.filter(row => safe(row.regime, "Neutral") === filterKey);
    };
    function loadSignalHighlights() {
      try {
        const raw = JSON.parse(localStorage.getItem("v2matrix-v1-signal-highlights") || "{}");
        return new Map(Object.entries(raw).filter(([, until]) => Number(until) > Date.now()));
      } catch (error) {
        return new Map();
      }
    }
    function saveSignalHighlights() {
      const active = {};
      highlightedSignals.forEach((until, id) => {
        if (Number(until) > Date.now()) active[id] = until;
      });
      localStorage.setItem(HIGHLIGHT_STORAGE_KEY, JSON.stringify(active));
    }
    function isSignalHighlighted(id) {
      const until = highlightedSignals.get(id);
      if (!until) return false;
      if (Number(until) <= Date.now()) {
        highlightedSignals.delete(id);
        saveSignalHighlights();
        return false;
      }
      return true;
    }
    function markSignalHighlighted(row) {
      highlightedSignals.set(rowId(row), Date.now() + SIGNAL_HIGHLIGHT_MS);
      saveSignalHighlights();
    }
    function getAlertAudioContext() {
      const AudioContextClass = window.AudioContext || window.webkitAudioContext;
      if (!AudioContextClass) return null;
      if (!alertAudioContext) alertAudioContext = new AudioContextClass();
      return alertAudioContext;
    }
    function primeAlertAudio() {
      const context = getAlertAudioContext();
      if (context && context.state === "suspended") context.resume().catch(() => {});
    }
    function playAlertSound() {
      const context = getAlertAudioContext();
      if (!context) return;
      if (context.state === "suspended") {
        context.resume().then(playAlertSound).catch(() => {});
        return;
      }
      const now = context.currentTime;
      const gain = context.createGain();
      gain.gain.setValueAtTime(0.0001, now);
      gain.gain.exponentialRampToValueAtTime(0.12, now + 0.015);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.28);
      gain.connect(context.destination);
      [880, 1174].forEach((frequency, index) => {
        const oscillator = context.createOscillator();
        const start = now + index * 0.11;
        oscillator.type = "sine";
        oscillator.frequency.setValueAtTime(frequency, start);
        oscillator.connect(gain);
        oscillator.start(start);
        oscillator.stop(start + 0.12);
      });
    }
    function pulseHaptic() {
      primeAlertAudio();
      if ("vibrate" in navigator) navigator.vibrate(12);
    }
    function vibrateAlert() {
      if (!("vibrate" in navigator)) return false;
      try {
        return navigator.vibrate([80, 45, 80]);
      } catch (error) {
        return false;
      }
    }
    function notificationSupported() {
      return "Notification" in window;
    }
    function isMobileBrowser() {
      if (navigator.userAgentData && typeof navigator.userAgentData.mobile === "boolean") {
        return navigator.userAgentData.mobile;
      }
      const ua = navigator.userAgent || "";
      const touchMac = /Macintosh/i.test(ua) && Number(navigator.maxTouchPoints || 0) > 1;
      return touchMac || /Android|iPhone|iPad|iPod|Mobile|CriOS|FxiOS/i.test(ua);
    }
    async function promptMobileNotificationsOnOpen(force = false) {
      if (!isMobileBrowser()) return;
      if (mobileNotificationPromptAttempted && !force) return;
      mobileNotificationPromptAttempted = true;
      soundEnabled = true;
      localStorage.setItem("v2matrix-v1-sound", "true");
      if (!notificationSupported()) {
        notificationsEnabled = false;
        localStorage.setItem("v2matrix-v1-notifications", "false");
        updateNotifyButton();
        return;
      }
      if (Notification.permission === "default") {
        try {
          const permission = await Notification.requestPermission();
          notificationsEnabled = permission === "granted";
        } catch (error) {
          notificationsEnabled = false;
        }
      } else {
        notificationsEnabled = Notification.permission === "granted";
      }
      localStorage.setItem("v2matrix-v1-notifications", notificationsEnabled ? "true" : "false");
      updateNotifyButton();
    }
    function bellIconSvg(disabled = false) {
      return `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M10.27 21a2 2 0 0 0 3.46 0"></path>
          <path d="M3.26 15.33A1 1 0 0 0 4 17h16a1 1 0 0 0 .74-1.67C19.41 13.96 18 12.5 18 8a6 6 0 0 0-12 0c0 4.5-1.41 5.96-2.74 7.33"></path>
          ${disabled ? `<path d="m2 2 20 20"></path>` : ""}
        </svg>
      `;
    }
    function updateNotifyButton() {
      const button = document.getElementById("notifyToggle");
      if (!button) return;
      button.classList.remove("enabled", "blocked");
      if (!notificationSupported()) {
        button.innerHTML = bellIconSvg(!soundEnabled);
        button.setAttribute("aria-label", soundEnabled ? "Sound alerts enabled" : "Enable sound alerts");
        button.setAttribute("aria-pressed", soundEnabled ? "true" : "false");
        button.title = soundEnabled ? "Sound alerts enabled; browser notifications unavailable" : "Enable sound alerts";
        button.disabled = false;
        if (soundEnabled) button.classList.add("enabled");
        return;
      }
      if (Notification.permission === "granted" && notificationsEnabled && soundEnabled) {
        button.innerHTML = bellIconSvg(false);
        button.setAttribute("aria-label", "Notifications and sound enabled");
        button.setAttribute("aria-pressed", "true");
        button.title = "Notifications and sound enabled";
        button.classList.add("enabled");
      } else if (Notification.permission === "denied") {
        button.innerHTML = bellIconSvg(!soundEnabled);
        button.setAttribute("aria-label", soundEnabled ? "Sound alerts enabled; notifications blocked" : "Notifications blocked");
        button.setAttribute("aria-pressed", soundEnabled ? "true" : "false");
        button.title = soundEnabled ? "Sound alerts enabled; browser notifications blocked" : "Notifications blocked";
        button.classList.add("blocked");
        if (soundEnabled) button.classList.add("enabled");
      } else {
        button.innerHTML = bellIconSvg(!soundEnabled);
        button.setAttribute("aria-label", soundEnabled ? "Sound alerts enabled" : "Enable notifications and sound");
        button.setAttribute("aria-pressed", soundEnabled ? "true" : "false");
        button.title = soundEnabled ? "Sound alerts enabled; tap to enable browser notifications" : "Enable notifications and sound";
        if (soundEnabled) button.classList.add("enabled");
      }
    }
    function notificationReceivedMillis(item) {
      const parsed = Date.parse(item.received_at_ist || item.trigger_time_ist || "");
      return Number.isFinite(parsed) ? parsed : 0;
    }
    async function fetchTodayNotifications() {
      const response = await fetch("/api/v2matrix/v2/notifications/today", {cache:"no-store"});
      if (!response.ok) return [];
      const payload = await response.json();
      return Array.isArray(payload.notifications) ? payload.notifications : [];
    }
    async function updateHeaderUnreadBadge() {
      const badge = document.getElementById("headerUnreadCount");
      if (!badge) return;
      try {
        const lastOpened = Number(localStorage.getItem(NOTIFICATION_LAST_OPENED_KEY) || "0") || 0;
        const notifications = await fetchTodayNotifications();
        const unread = notifications.filter(item => notificationReceivedMillis(item) > lastOpened).length;
        badge.textContent = unread > 99 ? "99+" : String(unread);
        badge.classList.toggle("empty", unread <= 0);
        document.getElementById("headerNotifications")?.setAttribute("aria-label", unread > 0 ? `Open notifications, ${unread} unread` : "Open notifications");
      } catch (error) {
        badge.textContent = "0";
        badge.classList.add("empty");
      }
    }
    async function requestNotifications() {
      pulseHaptic();
      if ((notificationSupported() && Notification.permission === "granted" && notificationsEnabled) || (!notificationSupported() && soundEnabled)) {
        notificationsEnabled = false;
        soundEnabled = false;
        localStorage.setItem("v2matrix-v1-notifications", "false");
        localStorage.setItem("v2matrix-v1-sound", "false");
        updateNotifyButton();
        return;
      }
      soundEnabled = true;
      localStorage.setItem("v2matrix-v1-sound", "true");
      primeAlertAudio();
      if (!notificationSupported()) {
        localStorage.setItem("v2matrix-v1-notifications", "false");
        updateNotifyButton();
        return;
      }
      if (Notification.permission === "default") {
        await Notification.requestPermission();
      }
      notificationsEnabled = Notification.permission === "granted";
      localStorage.setItem("v2matrix-v1-notifications", notificationsEnabled ? "true" : "false");
      updateNotifyButton();
      if (soundEnabled) {
        playAlertSound();
        showInstantCustomAlert({
          key:`alerts-enabled-${Date.now()}`,
          title:"Viveka Alert: v2Matrix",
          transition:"Neutral -> Bullish",
          text:"Alerts enabled on this page"
        });
      }
      if (notificationsEnabled) {
        try {
          new Notification("v2Matrix notifications enabled", {
            body:"New regime signals will appear here with sound.",
            tag:"v2matrix-v1-notifications-enabled"
          });
        } catch (error) {}
      }
    }
    const NOTIFICATION_EVENT_TYPES = new Set([
      "long_trigger",
      "short_trigger",
      "long_entry",
      "short_entry",
      "paper_entry",
      "tranche2_exit",
      "tranche1_exit",
      "base_exit",
      "full_exit",
      "paper_exit"
    ]);
    const ENTRY_NOTIFICATION_EVENT_TYPES = new Set([
      "long_trigger",
      "short_trigger",
      "long_entry",
      "short_entry",
      "paper_entry"
    ]);
    const MAX_NOTIFICATION_RECEIVED_AGE_MS = 10 * 60 * 1000;
    const MAX_NOTIFICATION_TRIGGER_AGE_MS = 20 * 60 * 1000;
    function notificationEventType(row) {
      return safe(row.last_trade_event_type || row.trigger_event_type || (row.last_event || {}).event_type, "").toLowerCase();
    }
    function notificationWorthy(row) {
      return NOTIFICATION_EVENT_TYPES.has(notificationEventType(row));
    }
    function parseEventMillis(value) {
      const parsed = Date.parse(value || "");
      return Number.isFinite(parsed) ? parsed : 0;
    }
    function notificationEventIsRecent(row) {
      const nowMs = Date.now();
      const receivedMs = parseEventMillis((row.last_event || {}).received_at_ist || row.last_webhook_at_ist || row.event_created_at_ist);
      const triggerMs = parseEventMillis(row.last_trade_event_time_ist || row.trigger_time_ist);
      if (!receivedMs || nowMs - receivedMs > MAX_NOTIFICATION_RECEIVED_AGE_MS) return false;
      if (!triggerMs || nowMs - triggerMs > MAX_NOTIFICATION_TRIGGER_AGE_MS) return false;
      return true;
    }
    function signalKey(row) {
      if (!notificationWorthy(row)) return "";
      const tradeEventId = safe(row.last_trade_event_id, "");
      if (tradeEventId) return [rowId(row), tradeEventId].join("|");
      return [
        rowId(row),
        safe(row.regime, "Neutral"),
        safe(row.transition_from_regime, "Neutral"),
        safe(row.transition_to_regime, "Neutral"),
        safe(row.trigger_time_ist, ""),
        safe(row.trigger_price_underlying, "")
      ].join("|");
    }
    function signalAlertParts(row) {
      const status = safe(row.regime, "Neutral");
      const from = safe(row.last_trade_event_from_regime || row.transition_from_regime, "Neutral");
      const to = safe(row.last_trade_event_to_regime || row.transition_to_regime, status);
      const eventTime = row.last_trade_event_time_ist || row.trigger_time_ist;
      const clock = fmtClockFromIso(eventTime) || fmtClockIst24(row.trigger_time) || safe(row.trigger_datetime, "time unavailable");
      const price = fmtPricePlain(row.last_trade_event_price_underlying ?? row.trigger_price_underlying);
      return {
        title: `Viveka Alert: ${safe(row.instrument_name || row.instrument_id)}`,
        from: normaliseRegime(from),
        to: normaliseRegime(to),
        clock,
        price,
        body: `${from} -> ${to} at ${clock}, price ${price}`
      };
    }
    function showInstantCustomAlert(parts) {
      const stack = document.getElementById("instantAlertStack");
      if (!stack) return;
      const key = parts.key || `custom-${Date.now()}`;
      if (!key || activeInstantAlertKeys.has(key)) return;
      activeInstantAlertKeys.add(key);
      const status = normaliseRegime(parts.status || String(parts.transition || "").split("->").pop() || "Neutral");
      const alert = document.createElement("div");
      alert.className = `instant-alert ${status}`;
      alert.innerHTML = `
        <div class="instant-alert-title">${esc(parts.title)}</div>
        <div class="instant-alert-body">
          <span class="detail-transition">${transitionHtml(parts.transition || `${parts.from || "Neutral"} -> ${parts.to || status}`)}</span>
          <span class="instant-alert-price">${esc(parts.text || `${parts.clock}, price ${parts.price}`)}</span>
        </div>
      `;
      stack.prepend(alert);
      window.setTimeout(() => {
        alert.classList.add("removing");
        window.setTimeout(() => {
          alert.remove();
          activeInstantAlertKeys.delete(key);
        }, 190);
      }, INSTANT_ALERT_MS);
    }
    function showInstantSignalAlert(row, options = {}) {
      if (document.hidden && !options.fromPending) {
        pendingInstantAlerts.push({row, force:options.force === true});
        return;
      }
      const parts = signalAlertParts(row);
      showInstantCustomAlert({
        key: options.force === true ? `smoke-${Date.now()}` : signalKey(row),
        title: parts.title,
        transition: `${parts.from} -> ${parts.to}`,
        text: `${parts.clock}, price ${parts.price}`,
        status: parts.to
      });
    }
    function flushPendingInstantAlerts() {
      if (!pendingInstantAlerts.length) return;
      const alerts = pendingInstantAlerts.splice(0);
      alerts
        .sort((a, b) => signalEventMillis(a.row) - signalEventMillis(b.row))
        .forEach(item => showInstantSignalAlert(item.row, {force:item.force, fromPending:true}));
    }
    function notifySignal(row, options = {}) {
      const force = options.force === true;
      if (!force && !notificationWorthy(row)) return;
      if (!force && !notificationEventIsRecent(row)) return;
      markSignalHighlighted(row);
      showInstantSignalAlert(row, {force});
      if (soundEnabled) {
        playAlertSound();
        vibrateAlert();
      }
      if (!notificationSupported() || !notificationsEnabled || Notification.permission !== "granted") return;
      const parts = signalAlertParts(row);
      const eventType = notificationEventType(row);
      const isEntryNotification = ENTRY_NOTIFICATION_EVENT_TYPES.has(eventType);
      const staleSeconds = numberOrNull((row.last_event || {}).entry_staleness_seconds ?? row.entry_staleness_seconds);
      const confirmedClock = fmtClockFromIso(row.entry_evaluation_time_ist || row.event_created_at_ist || row.last_webhook_at_ist);
      const delayedSuffix = isEntryNotification && (row.entry_stale || (staleSeconds !== null && staleSeconds > 60))
        ? ` (delayed${confirmedClock ? `, confirmed ${confirmedClock}` : ""})`
        : "";
      try {
        new Notification(parts.title, {
          body: `${parts.body}${delayedSuffix}`,
          vibrate:[80, 45, 80],
          silent:false,
          tag: `v2matrix-v1-${rowId(row)}-${safe(row.last_trade_event_id || row.trigger_time_ist, Date.now())}`,
          renotify:true
        });
      } catch (error) {}
    }
    async function emitSmokeAlert() {
      pulseHaptic();
      const rows = sortedRows(filteredRows(latestData.instruments || []));
      const row = rows.find(item => !missing(item.trigger_time_ist) || !missing(item.trigger_time) || !missing(item.trigger_datetime)) || rows[0];
      if (!row) return;
      markSignalHighlighted(row);
      if (notificationSupported() && Notification.permission === "default") {
        await Notification.requestPermission();
      }
      if (notificationSupported() && Notification.permission === "granted") {
        notificationsEnabled = true;
        soundEnabled = true;
        localStorage.setItem("v2matrix-v1-notifications", "true");
        localStorage.setItem("v2matrix-v1-sound", "true");
        updateNotifyButton();
        notifySignal(row, {force:true});
      } else {
        soundEnabled = true;
        localStorage.setItem("v2matrix-v1-sound", "true");
        updateNotifyButton();
        notifySignal(row, {force:true});
      }
      render(latestData);
    }
    function signalEventMillis(row) {
      const parsed = Date.parse(row.last_trade_event_time_ist || row.trigger_time_ist || "");
      return Number.isFinite(parsed) ? parsed : 0;
    }
    function evaluateSignalNotifications(rows) {
      const nextKeys = new Map();
      const changedRows = [];
      const primed = signalNotificationsPrimed;
      rows.forEach(row => {
        const id = rowId(row);
        const key = signalKey(row);
        if (!key) return;
        const priorKey = seenSignalKeys.get(id);
        nextKeys.set(id, key);
        if (!primed || missing(row.trigger_time_ist)) return;
        if (!notificationEventIsRecent(row)) return;
        if (!priorKey || priorKey !== key) changedRows.push(row);
      });
      seenSignalKeys = nextKeys;
      signalNotificationsPrimed = true;
      changedRows
        .sort((a, b) => signalEventMillis(a) - signalEventMillis(b))
        .forEach(row => notifySignal(row));
    }
    function fmtClockIst24(rawTime) {
      const raw = rawTime || "";
      const parts = raw.split(":");
      if (parts.length === 2) {
        const hour24 = Number(parts[0]);
        const minute = parts[1].padStart(2, "0");
        if (Number.isFinite(hour24)) return `${String(hour24).padStart(2, "0")}:${minute} IST`;
      }
      return null;
    }
    function fmtClockFromIso(rawIso) {
      const parsed = Date.parse(rawIso || "");
      if (!Number.isFinite(parsed)) return null;
      const parts = new Intl.DateTimeFormat("en-GB", {
        timeZone:"Asia/Kolkata",
        hour:"2-digit",
        minute:"2-digit",
        hour12:false
      }).formatToParts(new Date(parsed)).reduce((carry, part) => {
        carry[part.type] = part.value;
        return carry;
      }, {});
      if (parts.hour && parts.minute) return `${parts.hour}:${parts.minute} IST`;
      return null;
    }
    function detailDateTimeHtml(detail, row) {
      const date = safe(detail.date || row.trigger_date, "No trigger date");
      const clock = fmtClockIst24(detail.time || row.trigger_time);
      if (clock) return `<span class="detail-date">${esc(date)}</span><span class="detail-time-clock">${esc(clock)}</span>`;
      return `<span class="detail-date">${esc(safe(detail.datetime || row.trigger_datetime, "No trigger yet").replace(":", "."))}</span>`;
    }
    function rowEntryDateTime(row) {
      const date = safe(row.trigger_date, "No trigger date");
      const clock = fmtClockIst24(row.trigger_time);
      if (clock) return `${date}, ${clock}`;
      return safe(row.trigger_datetime, "No trigger yet");
    }
    function fmtClockIstCompact(rawTime) {
      const raw = rawTime || "";
      const parts = raw.split(":");
      if (parts.length === 2) {
        const hour = Number(parts[0]);
        const minute = parts[1].padStart(2, "0");
        if (Number.isFinite(hour)) return `${hour}.${minute} IST`;
      }
      return null;
    }
    function rowPerformerDateTime(row) {
      const date = safe(row.trigger_date, "No trigger date");
      const clock = fmtClockIstCompact(row.trigger_time);
      if (clock) return `${date}, ${clock}`;
      return safe(row.trigger_datetime, "No trigger yet");
    }
    function normaliseRegime(value) {
      const text = safe(value, "Neutral").toLowerCase();
      if (text === "bullish") return "Bullish";
      if (text === "bearish") return "Bearish";
      return "Neutral";
    }
    function transitionHtml(value) {
      const parts = safe(value, "Neutral -> Neutral").split("->").map(part => normaliseRegime(part.trim()));
      const from = parts[0] || "Neutral";
      const to = parts[1] || "Neutral";
      return `<span class="regime-chip ${from}">${esc(from)}</span><span class="transition-arrow">-&gt;</span><span class="regime-chip ${to}">${esc(to)}</span>`;
    }
    function priceBoxHtml(label, value) {
      return `<div class="price-box"><span class="price-label">${esc(label)}</span><span class="price">${esc(fmtPricePlain(value))}</span></div>`;
    }
    function fallbackExpandedRow(row) {
      const status = safe(row.transition_to_regime || row.regime, "Neutral").toUpperCase();
      const from = safe(row.transition_from_regime || "Neutral").toUpperCase();
      return {
        date: row.trigger_date,
        time: row.trigger_time,
        datetime: row.trigger_datetime,
        transition: `${from} -> ${status}`,
        price: row.trigger_price_underlying,
        move_pct: row.move_pct
      };
    }
    function neutralPriceLabel(detail) {
      const transition = safe(detail.transition, "").toLowerCase();
      return transition.includes("-> neutral") ? "Exit" : "Entry";
    }
    function expandedRowsHtml(row) {
      const status = safe(row.regime, "Neutral");
      if (missing(row.trigger_time_ist) && missing(row.trigger_time) && missing(row.trigger_datetime)) {
        return `<div class="no-trigger-detail">No trigger yet</div>`;
      }
      const details = Array.isArray(row.expanded_rows) && row.expanded_rows.length ? row.expanded_rows : [fallbackExpandedRow(row)];
      if (status === "Neutral" && details.length >= 2) {
        const move = numberOrNull(row.neutral_move_pct ?? details[0].move_pct);
        return `
          <div class="detail-grid neutral-detail">
            ${details.slice(0, 2).map((detail, index) => `
              <div class="detail-cell detail-date-time">${detailDateTimeHtml(detail, row)}</div>
              <div class="detail-cell detail-transition">${transitionHtml(detail.transition || "NEUTRAL -> NEUTRAL")}</div>
              <div class="detail-cell">${priceBoxHtml(neutralPriceLabel(detail), detail.price)}</div>
              ${index === 0 ? `<div class="neutral-move ${moveClass(move)}">${esc(fmtMove(move))}</div>` : ""}
            `).join("")}
          </div>
        `;
      }
      const detail = details[0] || fallbackExpandedRow(row);
      return `
        <div class="detail-grid">
          <div class="detail-cell detail-date-time">${detailDateTimeHtml(detail, row)}</div>
          <div class="detail-cell detail-transition">${transitionHtml(detail.transition || "NEUTRAL -> NEUTRAL")}</div>
          <div class="detail-cell">${priceBoxHtml("Entry", detail.price)}</div>
          <div class="detail-cell">${priceBoxHtml("Current", row.current_price_underlying)}</div>
        </div>
      `;
    }
    function sortedRows(rows) {
      return [...rows].sort((a, b) => {
        let left;
        let right;
        if (sortKey === "status") {
          left = statusRank[a.regime] ?? 99;
          right = statusRank[b.regime] ?? 99;
        } else if (sortKey === "age") {
          left = ageEpoch(a);
          right = ageEpoch(b);
        } else if (sortKey === "move") {
          left = moveValue(a);
          right = moveValue(b);
          if (left === null && right === null) return 0;
          if (left === null) return 1;
          if (right === null) return -1;
        } else {
          left = safe(a.instrument_name || a.instrument_id, "").toLowerCase();
          right = safe(b.instrument_name || b.instrument_id, "").toLowerCase();
        }
        if (left < right) return -1 * sortDir;
        if (left > right) return 1 * sortDir;
        return safe(a.instrument_name || a.instrument_id, "").localeCompare(safe(b.instrument_name || b.instrument_id, ""));
      });
    }
    function updateSortButtons() {
      document.querySelectorAll(".list-header button").forEach(button => {
        const span = button.querySelector(".sort-arrow");
        span.textContent = button.dataset.sort === sortKey ? (sortDir > 0 ? "↑" : "↓") : "";
      });
    }
    function renderFilters(rows) {
      const watchRows = watchlistFilterActive ? rows.filter(isWatchlisted) : rows;
      const counts = {
        All: watchRows.length,
        Bullish: watchRows.filter(row => row.regime === "Bullish").length,
        Bearish: watchRows.filter(row => row.regime === "Bearish").length,
        Neutral: watchRows.filter(row => row.regime === "Neutral").length
      };
      const watchButton = document.getElementById("watchlistFilter");
      const watchCount = rows.filter(isWatchlisted).length;
      watchButton.classList.toggle("active", watchlistFilterActive);
      watchButton.setAttribute("aria-pressed", watchlistFilterActive ? "true" : "false");
      document.getElementById("watchlistCount").textContent = watchCount;
      watchButton.onclick = () => {
        pulseHaptic();
        watchlistFilterActive = !watchlistFilterActive;
        visibleLimit = INITIAL_VISIBLE_LIMIT;
        saveWatchlist();
        render(latestData);
      };
      const holder = document.getElementById("filters");
      holder.innerHTML = filters.map(name => `
        <button class="filter-card ${filterKey === name ? "active" : ""}" type="button" data-filter="${name}">
          <span>${name}</span><span class="filter-count">${counts[name] || 0}</span>
        </button>
      `).join("");
      holder.querySelectorAll(".filter-card").forEach(card => {
        card.addEventListener("click", () => {
          pulseHaptic();
          filterKey = card.dataset.filter || "All";
          visibleLimit = INITIAL_VISIBLE_LIMIT;
          render(latestData);
        });
      });
    }
    function cardHtml(row) {
      const id = rowId(row);
      const expanded = expandedIds.has(id);
      const highlighted = isSignalHighlighted(id);
      const status = safe(row.regime, "Neutral");
      const move = moveValue(row);
      const moveText = status === "Neutral" ? "-" : fmtMove(move);
      const watched = watchlistIds.has(id);
      return `
        <article class="signal-card ${esc(status)} ${expanded ? "expanded" : ""} ${highlighted ? "signal-notified" : ""} ${watched ? "watchlisted" : ""}" data-id="${esc(id)}">
          <div class="row-shell">
            <button class="row-main" type="button" data-id="${esc(id)}" aria-expanded="${expanded ? "true" : "false"}">
              <div class="symbol-cell"><span class="accent-line"></span><span class="symbol">${esc(row.instrument_name || row.instrument_id)}</span></div>
              <div><span class="status-chip ${esc(status)}">${esc(status)}</span></div>
              <div class="age">${esc(safe(row.trigger_age, "No trigger yet"))}</div>
              <div class="move ${moveClass(move)}">${esc(moveText)}</div>
            </button>
            <button class="watchlist-toggle ${watched ? "active" : ""}" type="button" data-watchlist-id="${esc(id)}" aria-label="${watched ? "Remove from Watchlist" : "Add to Watchlist"}" title="${watched ? "Remove from Watchlist" : "Add to Watchlist"}">
              <span aria-hidden="true">${watched ? "★" : "☆"}</span>
            </button>
          </div>
          <div class="detail-panel"><div class="detail-inner">${expandedRowsHtml(row)}</div></div>
        </article>
      `;
    }
    function updateMarketState(data) {
      const badge = document.getElementById("liveBadge");
      const label = document.getElementById("marketState");
      const parts = new Intl.DateTimeFormat("en-GB", {
        timeZone:"Asia/Kolkata",
        weekday:"short",
        hour:"2-digit",
        minute:"2-digit",
        hour12:false
      }).formatToParts(new Date()).reduce((carry, part) => {
        carry[part.type] = part.value;
        return carry;
      }, {});
      const minutes = Number(parts.hour) * 60 + Number(parts.minute);
      const weekday = parts.weekday || "";
      const open = !["Sat", "Sun"].includes(weekday) && minutes >= 555 && minutes <= 930;
      badge.classList.toggle("live", open);
      badge.classList.toggle("closed", !open);
      label.textContent = open ? "Market Live" : "Market Closed";
    }
    function renderOverview(rows) {
      const total = rows.length || 1;
      const bullish = rows.filter(row => row.regime === "Bullish").length;
      const bearish = rows.filter(row => row.regime === "Bearish").length;
      const neutral = rows.filter(row => row.regime === "Neutral").length;
      const bullishPct = Math.round((bullish / total) * 100);
      const leaning = bullish > bearish ? "Bullish" : bearish > bullish ? "Bearish" : "Neutral";
      document.getElementById("overviewText").textContent = `Aggregate sentiment is currently leaning ${leaning} (${bullishPct}%). Neutral count: ${neutral}.`;
      document.getElementById("bullishIntensity").textContent = bullishPct >= 45 ? "High" : bullishPct >= 25 ? "Medium" : "Low";
      document.getElementById("bullishBar").style.width = `${Math.min(100, bullishPct)}%`;
      const bullishMoves = rows.filter(row => row.regime === "Bullish").map(row => numberOrNull(row.move_pct)).filter(value => value !== null);
      const bearishMoves = rows.filter(row => row.regime === "Bearish").map(row => numberOrNull(row.move_pct)).filter(value => value !== null);
      const avg = values => values.length ? values.reduce((a, b) => a + b, 0) / values.length : null;
      const avgBullish = avg(bullishMoves);
      const avgBearish = avg(bearishMoves);
      const bullishNode = document.getElementById("avgBullishMove");
      const bearishNode = document.getElementById("avgBearishMove");
      bullishNode.textContent = avgBullish === null ? "--" : fmtMove(avgBullish);
      bearishNode.textContent = avgBearish === null ? "--" : fmtMove(avgBearish);
      bullishNode.className = `move-metric-value ${moveClass(avgBullish)}`;
      bearishNode.className = `move-metric-value ${moveClass(avgBearish)}`;
      const top = [...rows]
        .filter(row => ["Bullish", "Bearish"].includes(safe(row.regime, "Neutral")) && numberOrNull(row.move_pct) !== null)
        .sort((a, b) => (numberOrNull(b.move_pct) || 0) - (numberOrNull(a.move_pct) || 0))[0];
      document.getElementById("topPerformer").textContent = top ? safe(top.instrument_name || top.instrument_id) : "NA";
      document.getElementById("topMove").textContent = top ? `${fmtMove(top.move_pct)} since entry` : "NA";
      document.getElementById("topMove").className = `performer-move ${top ? moveClass(top.move_pct) : ""}`;
      document.getElementById("topPosition").textContent = top ? `${safe(top.regime, "Neutral")} since ${rowPerformerDateTime(top)}` : "No active position";
    }
    function render(data) {
      const rows = data.instruments || [];
      latestData = data;
      document.getElementById("updated").textContent = `Updated: ${safe(data.updated_label || data.updated_at_ist, "Waiting")}`;
      document.getElementById("events").textContent = `Symbols: ${rows.length}`;
      updateMarketState(data);
      renderFilters(rows);
      renderOverview(rows);
      const body = document.getElementById("cards");
      if (!rows.length) {
        body.innerHTML = `<div class="empty">Waiting for market regime data</div>`;
        document.getElementById("scrollNote").textContent = "";
        updateSortButtons();
        return;
      }
      const visibleRows = sortedRows(filteredRows(rows));
      visibleLimit = Math.min(Math.max(visibleLimit, INITIAL_VISIBLE_LIMIT), Math.max(visibleRows.length, INITIAL_VISIBLE_LIMIT));
      const renderedRows = visibleRows.slice(0, visibleLimit);
      const remaining = Math.max(0, visibleRows.length - renderedRows.length);
      body.innerHTML = renderedRows.length ? renderedRows.map(cardHtml).join("") : `<div class="empty">No instruments in this filter</div>`;
      document.getElementById("scrollNote").textContent = remaining ? `Scroll for ${remaining} more symbols...` : "";
      body.querySelectorAll(".row-main").forEach(button => {
        button.addEventListener("click", () => {
          pulseHaptic();
          const id = button.dataset.id;
          if (expandedIds.has(id)) expandedIds.delete(id);
          else expandedIds.add(id);
          render(latestData);
        });
      });
      body.querySelectorAll(".watchlist-toggle").forEach(button => {
        button.addEventListener("click", event => {
          event.preventDefault();
          event.stopPropagation();
          pulseHaptic();
          const id = String(button.dataset.watchlistId || "");
          if (!id) return;
          const previousScrollY = window.scrollY;
          if (watchlistIds.has(id)) watchlistIds.delete(id);
          else watchlistIds.add(id);
          saveWatchlist();
          render(latestData);
          window.requestAnimationFrame(() => window.scrollTo({top:previousScrollY, left:0, behavior:"auto"}));
        });
      });
      updateSortButtons();
    }
    document.querySelectorAll(".list-header button").forEach(button => {
      button.addEventListener("click", () => {
        pulseHaptic();
        const next = button.dataset.sort;
        if (sortKey === next) sortDir *= -1;
        else {
          sortKey = next;
          sortDir = 1;
        }
        visibleLimit = INITIAL_VISIBLE_LIMIT;
        render(latestData);
      });
    });
    function maybeLoadMore() {
      const rows = latestData.instruments || [];
      if (!rows.length) return;
      const total = filteredRows(rows).length;
      if (visibleLimit >= total) return;
      const nearBottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 260;
      if (!nearBottom) return;
      visibleLimit = Math.min(total, visibleLimit + LOAD_MORE_COUNT);
      render(latestData);
    }
    window.addEventListener("scroll", maybeLoadMore, {passive:true});
    function applyTheme(theme, userSelected = false) {
      document.documentElement.dataset.theme = theme;
      localStorage.setItem("v2matrix-v1-theme", theme);
      if (userSelected) localStorage.setItem("v2matrix-v1-theme-user-set", "true");
      document.getElementById("themeToggle").textContent = theme === "dark" ? "Day" : "Night";
    }
    function drawerAction(event, action) {
      event?.preventDefault();
      event?.stopPropagation();
      pulseHaptic();
      const button = event?.currentTarget;
      button?.classList.add("cta-feedback");
      window.setTimeout(() => button?.classList.remove("cta-feedback"), 450);
      try {
        const result = action();
        if (result && typeof result.catch === "function") {
          result.catch(error => console.warn("Matrix sidebar action failed", error));
        }
      } catch (error) {
        console.warn("Matrix sidebar action failed", error);
      }
    }
    document.getElementById("themeToggle")?.addEventListener("click", event => {
      drawerAction(event, () => applyTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark", true));
    });
    document.getElementById("smokeToggle")?.addEventListener("click", event => {
      drawerAction(event, emitSmokeAlert);
    });
    function initAccountDrawer() {
      const storedName = String(localStorage.getItem("v2matrix-v1-user-name") || "").trim();
      const loggedIn = storedName.length > 0;
      const name = loggedIn ? storedName : "Guest";
      const initials = loggedIn
        ? name.split(/\s+/).filter(Boolean).slice(0, 2).map(part => part[0]).join("").toUpperCase()
        : "G";
      document.getElementById("accountName").textContent = name;
      document.getElementById("accountInitials").textContent = initials || "G";
      document.getElementById("accountRole").textContent = loggedIn ? "v2Matrix account" : "Not signed in";
      document.getElementById("sidebarAccount")?.classList.toggle("guest", !loggedIn);
      document.getElementById("accountToggle")?.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        pulseHaptic();
        const account = document.getElementById("sidebarAccount");
        account?.classList.toggle("open");
        document.getElementById("accountToggle")?.setAttribute("aria-expanded", account?.classList.contains("open") ? "true" : "false");
      });
    }
    let lockedMenuScrollY = 0;
    function lockPageScrollForMenu() {
      lockedMenuScrollY = window.scrollY || document.documentElement.scrollTop || 0;
      document.body.style.position = "fixed";
      document.body.style.top = `-${lockedMenuScrollY}px`;
      document.body.style.left = "0";
      document.body.style.right = "0";
      document.body.style.width = "100%";
    }
    function unlockPageScrollForMenu() {
      document.body.style.position = "";
      document.body.style.top = "";
      document.body.style.left = "";
      document.body.style.right = "";
      document.body.style.width = "";
      window.scrollTo({top:lockedMenuScrollY, left:0, behavior:"auto"});
    }
    function openMobileMenu() {
      if (document.body.classList.contains("mobile-menu-open")) return;
      lockPageScrollForMenu();
      document.body.classList.add("mobile-menu-open");
      document.getElementById("mobileMenuToggle")?.setAttribute("aria-expanded", "true");
    }
    function closeMobileMenu() {
      if (!document.body.classList.contains("mobile-menu-open")) return;
      document.body.classList.remove("mobile-menu-open");
      document.getElementById("mobileMenuToggle")?.setAttribute("aria-expanded", "false");
      unlockPageScrollForMenu();
    }
    document.getElementById("mobileMenuToggle")?.addEventListener("click", event => {
      event.preventDefault();
      event.stopPropagation();
      pulseHaptic();
      if (document.body.classList.contains("mobile-menu-open")) closeMobileMenu();
      else openMobileMenu();
    });
    document.getElementById("mobileMenuClose")?.addEventListener("click", event => {
      event.preventDefault();
      event.stopPropagation();
      pulseHaptic();
      closeMobileMenu();
    });
    document.getElementById("mobileMenuBackdrop")?.addEventListener("click", event => {
      event.preventDefault();
      closeMobileMenu();
    });
    document.addEventListener("pointerdown", event => {
      if (!document.body.classList.contains("mobile-menu-open")) return;
      const drawer = document.querySelector(".top-actions");
      const toggle = document.getElementById("mobileMenuToggle");
      if (drawer?.contains(event.target) || toggle?.contains(event.target)) return;
      closeMobileMenu();
    }, true);
    document.querySelector(".top-actions")?.addEventListener("pointerdown", event => {
      event.stopPropagation();
    });
    document.querySelector(".top-actions")?.addEventListener("click", event => {
      event.stopPropagation();
    });
    document.addEventListener("touchmove", event => {
      if (!document.body.classList.contains("mobile-menu-open")) return;
      if (document.querySelector(".top-actions")?.contains(event.target)) return;
      event.preventDefault();
    }, {passive:false});
    document.addEventListener("wheel", event => {
      if (!document.body.classList.contains("mobile-menu-open")) return;
      if (document.querySelector(".top-actions")?.contains(event.target)) return;
      event.preventDefault();
    }, {passive:false});
    window.addEventListener("keydown", event => {
      if (event.key === "Escape") closeMobileMenu();
    });
    let refreshInFlight = false;
    let pendingRefresh = false;
    async function refresh() {
      if (refreshInFlight) {
        pendingRefresh = true;
        return;
      }
      refreshInFlight = true;
      try {
        const response = await fetch("/api/v2matrix/v2/status", {cache:"no-store"});
        const data = await response.json();
        evaluateSignalNotifications(data.instruments || []);
        render(data);
        updateHeaderUnreadBadge();
      } catch (error) {
        document.getElementById("marketState").textContent = "API error";
        document.getElementById("liveBadge").classList.remove("live");
        document.getElementById("liveBadge").classList.remove("closed");
      } finally {
        refreshInFlight = false;
        if (pendingRefresh) {
          pendingRefresh = false;
          setTimeout(refresh, 50);
        }
      }
    }
    function startMatrixEventStream() {
      if (!("EventSource" in window)) return;
      let source = null;
      let retryTimer = null;
      let lastStreamKey = "";
      const connect = () => {
        source = new EventSource("/api/v2matrix/v2/stream");
        source.addEventListener("matrix-state", event => {
          let payload = {};
          try {
            payload = JSON.parse(event.data || "{}");
          } catch (error) {
            payload = {};
          }
          const streamKey = `${payload.event_count || 0}|${payload.last_event_at_ist || ""}|${payload.updated_at_ist || ""}`;
          if (streamKey && streamKey !== lastStreamKey) {
            lastStreamKey = streamKey;
            refresh();
          }
        });
        source.onerror = () => {
          if (source) source.close();
          if (!retryTimer) {
            retryTimer = setTimeout(() => {
              retryTimer = null;
              connect();
            }, 5000);
          }
        };
      };
      connect();
    }
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) {
        flushPendingInstantAlerts();
        refresh();
      }
    });
    document.addEventListener("pointerdown", () => {
      if (soundEnabled) primeAlertAudio();
      if (isMobileBrowser() && notificationSupported() && Notification.permission === "default") {
        promptMobileNotificationsOnOpen(true);
      }
    }, {once:true, passive:true});
    document.addEventListener("touchstart", () => {
      if (soundEnabled) primeAlertAudio();
      if (isMobileBrowser() && notificationSupported() && Notification.permission === "default") {
        promptMobileNotificationsOnOpen(true);
      }
    }, {once:true, passive:true});
    window.addEventListener("focus", refresh);
    window.addEventListener("pageshow", refresh);
    window.addEventListener("online", refresh);
    applyTheme(document.documentElement.dataset.theme || "dark");
    initAccountDrawer();
    updateNotifyButton();
    promptMobileNotificationsOnOpen();
    refresh();
    startMatrixEventStream();
    setInterval(refresh, 5000);
    setInterval(updateHeaderUnreadBadge, 30000);
  </script>
</body>
</html>
"""
