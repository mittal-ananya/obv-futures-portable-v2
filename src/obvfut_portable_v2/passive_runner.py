from __future__ import annotations

import argparse
import base64
import bisect
import ctypes
import csv
import gc
import gzip
import hashlib
import importlib
import json
import math
import os
import re
import signal
import sys
import tarfile
import time
import traceback
from array import array
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
MIN_PRIOR_SECONDS = 5000
MIN_CLOCK_HISTORY = 20
_V1_PORTFOLIO_MODULE: Any | None = None
_V1_OBV_MODEL_MODULE: Any | None = None

SECOND_STATE_COLUMNS = [
    "trade_date",
    "epoch_second",
    "actual_time",
    "received_at_ist",
    "exchange_timestamp",
    "price",
    "bid_price",
    "ask_price",
    "volume_traded",
]

CLOCK_STATE_COLUMNS = [
    "trade_date",
    "clock_label",
    "clock_time",
    "has_clock_row",
    "actual_time",
    "received_at_ist",
    "exchange_timestamp",
    "epoch_second",
    "price",
    "prior_clock_vol_points",
]


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def parse_hhmm(raw: str) -> tuple[int, int]:
    hh, mm = str(raw).split(":", 1)
    return int(hh), int(mm)


def parse_hhmmss(raw: str) -> tuple[int, int, int]:
    parts = [int(part) for part in str(raw).split(":")]
    if len(parts) == 2:
        return parts[0], parts[1], 0
    return parts[0], parts[1], parts[2]


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def retention_seconds_from_config(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"", "none", "null", "unlimited"}:
        return None
    try:
        parsed = int(float(text))
    except (TypeError, ValueError):
        return None
    return max(0, parsed)


def json_clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_clean(v) for v in value]
    if isinstance(value, tuple):
        return [json_clean(v) for v in value]
    if isinstance(value, array):
        return [json_clean(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def float_array(values: Iterable[Any] = ()) -> array:
    out = array("d")
    for value in values:
        parsed = as_float(value)
        if parsed is not None:
            out.append(parsed)
    return out


def encode_float_array(values: array) -> dict[str, Any]:
    packed = array("d", values)
    if sys.byteorder != "little":
        packed.byteswap()
    return {
        "encoding": "array_f64_le_base64_v1",
        "count": len(packed),
        "data": base64.b64encode(packed.tobytes()).decode("ascii"),
    }


def decode_float_array(value: Any) -> array:
    if isinstance(value, dict) and value.get("encoding") == "array_f64_le_base64_v1":
        raw = base64.b64decode(str(value.get("data") or ""), validate=True)
        out = array("d")
        out.frombytes(raw)
        if sys.byteorder != "little":
            out.byteswap()
        expected = int(value.get("count") or 0)
        if expected and len(out) != expected:
            raise ValueError(f"array_f64_le_base64_v1 count mismatch: expected {expected}, got {len(out)}")
        return out
    return float_array(value or [])


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(json_clean(payload), indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(json_clean(payload), sort_keys=True))
        handle.write("\n")


def append_jsonl_many(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    payloads = [json.dumps(json_clean(row), sort_keys=True) for row in rows]
    if not payloads:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for payload in payloads:
            handle.write(payload)
            handle.write("\n")


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_holiday_dates(path: Path) -> set[date]:
    if not path.exists():
        return set()
    holidays: set[date] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                values: list[str] = []
                for key in ("date", "holiday_date", "trading_holiday", "day"):
                    if row.get(key):
                        values.append(str(row[key]))
                values.extend(str(value) for value in row.values() if value)
                for value in values:
                    try:
                        holidays.add(date.fromisoformat(value.strip()[:10]))
                        break
                    except Exception:
                        continue
    except Exception:
        return set()
    return holidays


def synthesized_september_future_key(fut_key: str) -> str | None:
    key = str(fut_key or "")
    if not key.endswith("26AUGFUT"):
        return None
    return f"{key[:-len('26AUGFUT')]}26SEPFUT"


def load_contract_chain_manifest(config: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    path = resolve_config_path(config, "contract_chain_manifest_path", "contract_chain_manifest_path_local")
    payload = read_json(path, {})
    raw_symbols = payload.get("symbols") if isinstance(payload, dict) else {}
    if not isinstance(raw_symbols, dict):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for symbol, item in raw_symbols.items():
        raw_contracts = item.get("contracts") if isinstance(item, dict) else item
        if not isinstance(raw_contracts, list):
            continue
        contracts: list[dict[str, Any]] = []
        for contract in raw_contracts:
            if not isinstance(contract, dict):
                continue
            key = str(contract.get("instrument_key") or "")
            expiry = str(contract.get("expiry_date") or contract.get("expiry") or "")
            if not key or not expiry:
                continue
            payload_contract = dict(contract)
            payload_contract["instrument_key"] = key
            payload_contract["expiry_date"] = expiry
            payload_contract["label"] = str(payload_contract.get("label") or contract_label_from_expiry(expiry))
            contracts.append(payload_contract)
        if contracts:
            out[str(symbol)] = sorted(contracts, key=lambda row: str(row.get("expiry_date") or ""))
    return out


def contract_label_from_expiry(expiry: str) -> str:
    try:
        month = date.fromisoformat(str(expiry)).strftime("%B").lower()
    except Exception:
        month = "future"
    return f"{month}_shadow"


def merge_contract_chain_with_manifest(
    chain: list[dict[str, Any]],
    manifest_chain: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not manifest_chain:
        return [dict(contract) for contract in chain if isinstance(contract, dict)]
    by_key: dict[str, dict[str, Any]] = {}
    for contract in chain:
        if not isinstance(contract, dict):
            continue
        key = str(contract.get("instrument_key") or "")
        if key:
            by_key[key] = dict(contract)
    for contract in manifest_chain:
        key = str(contract.get("instrument_key") or "")
        if not key:
            continue
        existing = by_key.get(key, {})
        merged = dict(contract)
        merged.update(existing)
        merged["instrument_key"] = key
        merged["expiry_date"] = str(merged.get("expiry_date") or contract.get("expiry_date") or "")
        merged["label"] = str(merged.get("label") or contract_label_from_expiry(str(merged.get("expiry_date") or "")))
        by_key[key] = merged
    return sorted(by_key.values(), key=lambda row: str(row.get("expiry_date") or ""))


def load_key_manifest(config: dict[str, Any], primary_key: str, local_key: str | None = None) -> set[str]:
    path = resolve_config_path(config, primary_key, local_key)
    payload = read_json(path, {})
    raw_keys = payload.get("target_keys") if isinstance(payload, dict) else []
    if not isinstance(raw_keys, list):
        return set()
    return {str(key) for key in raw_keys if key}


def read_json_gz(path: Path, default: Any) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def atomic_write_json_gz(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=5) as handle:
        json.dump(json_clean(payload), handle, sort_keys=True)
    tmp.replace(path)


def release_unused_process_memory() -> None:
    gc.collect()
    if not sys.platform.startswith("linux"):
        return
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def current_process_rss_mb() -> float | None:
    status_path = Path("/proc/self/status")
    if not status_path.exists():
        return None
    try:
        for line in status_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.startswith("VmRSS:"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                return float(parts[1]) / 1024.0
    except Exception:
        return None
    return None


def safe_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def resolve_config_path(config: dict[str, Any], primary_key: str, local_key: str | None = None) -> Path:
    primary = Path(str(config.get(primary_key) or ""))
    if primary.exists():
        return primary
    if local_key:
        local = Path(str(config.get(local_key) or ""))
        if local.exists():
            return local
    return primary


def load_v1_portfolio_module(config: dict[str, Any]) -> Any:
    global _V1_PORTFOLIO_MODULE
    if _V1_PORTFOLIO_MODULE is not None:
        return _V1_PORTFOLIO_MODULE
    _add_v1_src_paths(config)
    try:
        _V1_PORTFOLIO_MODULE = importlib.import_module("obvfut_portable_v1.portfolio")
    except ModuleNotFoundError as exc:
        if exc.name != "fastapi":
            raise
        _install_fastapi_import_stub()
        _V1_PORTFOLIO_MODULE = importlib.import_module("obvfut_portable_v1.portfolio")
    return _V1_PORTFOLIO_MODULE


def _add_v1_src_paths(config: dict[str, Any]) -> None:
    local_package_root = Path(__file__).resolve().parents[3] / "obv-futures-portable-v1"
    if "PACKAGE_ROOT" not in os.environ and local_package_root.exists():
        default_cloud_root = Path("/opt/cloud-deploy-candidates/obv-futures-portable-v1")
        if not default_cloud_root.exists():
            os.environ["PACKAGE_ROOT"] = str(local_package_root)
    candidates = [
        config.get("obvfut_v1_src_path"),
        config.get("obvfut_v1_src_path_local"),
        local_package_root / "src",
    ]
    for raw in candidates:
        if not raw:
            continue
        path = Path(str(raw))
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))


def _install_fastapi_import_stub() -> None:
    import types

    class _DummyFastAPI:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def get(self, *args: Any, **kwargs: Any) -> Any:
            def decorator(func: Any) -> Any:
                return func

            return decorator

        def post(self, *args: Any, **kwargs: Any) -> Any:
            def decorator(func: Any) -> Any:
                return func

            return decorator

        def head(self, *args: Any, **kwargs: Any) -> Any:
            def decorator(func: Any) -> Any:
                return func

            return decorator

    class _DummyHTTPException(Exception):
        def __init__(self, status_code: int = 500, detail: Any = None) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class _DummyResponse:
        def __init__(self, content: Any = None, status_code: int = 200, **kwargs: Any) -> None:
            self.content = content
            self.status_code = status_code
            self.kwargs = kwargs

    fastapi_module = types.ModuleType("fastapi")
    fastapi_module.FastAPI = _DummyFastAPI
    fastapi_module.HTTPException = _DummyHTTPException
    responses_module = types.ModuleType("fastapi.responses")
    responses_module.HTMLResponse = _DummyResponse
    responses_module.JSONResponse = _DummyResponse
    responses_module.Response = _DummyResponse
    sys.modules.setdefault("fastapi", fastapi_module)
    sys.modules.setdefault("fastapi.responses", responses_module)


def load_v1_obv_model_module(config: dict[str, Any]) -> Any:
    global _V1_OBV_MODEL_MODULE
    if _V1_OBV_MODEL_MODULE is not None:
        return _V1_OBV_MODEL_MODULE
    _add_v1_src_paths(config)
    _V1_OBV_MODEL_MODULE = importlib.import_module("obvfut_portable_v1.obv_model")
    return _V1_OBV_MODEL_MODULE


def epoch_ist_iso(epoch: Any) -> str | None:
    try:
        value = int(epoch)
    except Exception:
        return None
    if value <= 0:
        return None
    return datetime.fromtimestamp(value, IST).isoformat()


def epoch_ist_date(epoch: Any) -> str | None:
    parsed = as_float(epoch)
    if parsed is None:
        return None
    try:
        return datetime.fromtimestamp(int(parsed), IST).date().isoformat()
    except Exception:
        return None


def parse_market_timestamp_epoch(value: Any) -> float | None:
    if value is None:
        return None
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
    else:
        parsed = parsed.astimezone(IST)
    return float(parsed.timestamp())


def received_epoch_from_record(record: dict[str, Any], tick: dict[str, Any]) -> float | None:
    return (
        as_float(record.get("received_at_epoch"))
        or as_float(tick.get("received_at_epoch"))
        or parse_market_timestamp_epoch(record.get("received_at_ist"))
        or parse_market_timestamp_epoch(tick.get("received_at_ist"))
    )


def market_epoch_from_record(record: dict[str, Any], tick: dict[str, Any]) -> float | None:
    return (
        parse_market_timestamp_epoch(tick.get("exchange_timestamp"))
        or parse_market_timestamp_epoch(tick.get("last_trade_time"))
        or as_float(tick.get("exchange_timestamp_epoch"))
        or received_epoch_from_record(record, tick)
    )


def normalise_record(record: dict[str, Any], trade_date: str, target: str) -> dict[str, Any] | None:
    if record.get("instrument_key") != target:
        return None
    tick = record.get("tick") or {}
    received_epoch = received_epoch_from_record(record, tick)
    epoch = market_epoch_from_record(record, tick)
    price = as_float(tick.get("last_price"))
    volume = as_float(tick.get("volume_traded"))
    if epoch is None or price is None or volume is None:
        return None
    depth = tick.get("depth") or {}
    buy = depth.get("buy") or []
    sell = depth.get("sell") or []
    bid = as_float((buy[0] or {}).get("price")) if buy else None
    ask = as_float((sell[0] or {}).get("price")) if sell else None
    return {
        "trade_date": trade_date,
        "target": target,
        "epoch": epoch,
        "epoch_second": int(epoch),
        "received_at_ist": str(record.get("received_at_ist") or tick.get("received_at_ist") or ""),
        "exchange_timestamp": str(tick.get("exchange_timestamp") or epoch_ist_iso(epoch) or ""),
        "received_epoch": received_epoch,
        "market_data_latency_seconds": (received_epoch - epoch) if received_epoch is not None else None,
        "price": price,
        "volume_traded": volume,
        "bid": bid,
        "ask": ask,
        "spread": (ask - bid) if ask is not None and bid is not None else None,
    }


def normalise_or_pass_record(record: dict[str, Any], trade_date: str, target: str) -> dict[str, Any] | None:
    if record.get("target") == target and record.get("epoch_second") is not None:
        price = as_float(record.get("price"))
        volume = as_float(record.get("volume_traded"))
        epoch = as_float(record.get("epoch"))
        if price is None or volume is None or epoch is None:
            return None
        out = dict(record)
        out["trade_date"] = str(out.get("trade_date") or trade_date)
        out["target"] = target
        out["epoch"] = float(epoch)
        out["epoch_second"] = int(out.get("epoch_second") or epoch)
        return out
    return normalise_record(record, trade_date, target)


def json_object_end(text: str, start: int) -> int:
    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        char = text[idx]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return idx + 1
    return -1


def json_object_start_near_key(text: str, key_start: int) -> int:
    idx = key_start - 1
    while idx >= 0 and text[idx].isspace():
        idx -= 1
    if idx >= 0 and text[idx] == "{":
        return idx
    return text.rfind("{", max(0, key_start - 512), key_start)


def target_items_from_batch_line(line: str, target_set: set[str]) -> list[dict[str, Any]]:
    epoch_match = re.search(r'"received_at_epoch"\s*:\s*([0-9.]+)', line)
    ist_match = re.search(r'"received_at_ist"\s*:\s*"([^"]+)"', line)
    batch_epoch = as_float(epoch_match.group(1)) if epoch_match else None
    batch_ist = ist_match.group(1) if ist_match else None
    out: list[dict[str, Any]] = []
    for match in re.finditer(r'"instrument_key"\s*:\s*"([^"]+)"', line):
        key = match.group(1)
        if key not in target_set:
            continue
        start = json_object_start_near_key(line, match.start())
        end = json_object_end(line, start) if start >= 0 else -1
        if end <= start:
            continue
        try:
            item = json.loads(line[start:end])
        except json.JSONDecodeError:
            continue
        out.append({"instrument_key": key, "received_at_epoch": batch_epoch, "received_at_ist": batch_ist, "tick": item.get("tick") or item})
    return out


_RAW_INSTRUMENT_KEY_RE = re.compile(rb'"instrument_key"\s*:\s*"([^"]+)"')
_RAW_RECEIVED_EPOCH_RE = re.compile(rb'"received_at_epoch"\s*:\s*([0-9.]+)')
_RAW_RECEIVED_IST_RE = re.compile(rb'"received_at_ist"\s*:\s*"([^"]+)"')
_TARGET_STREAM_KEY_RE = re.compile(rb'"key"\s*:\s*"([^"]+)"')
_TARGET_STREAM_EXCHANGE_EPOCH_RE = re.compile(rb'"exchange_epoch"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_EVENT_EPOCH_RE = re.compile(rb'"event_epoch"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_RECEIVED_EPOCH_RE = re.compile(rb'"received_epoch"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_PRICE_RE = re.compile(rb'"price"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_LAST_PRICE_RE = re.compile(rb'"last_price"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_VOLUME_RE = re.compile(rb'"volume_traded"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_BID_RE = re.compile(rb'"bid"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_ASK_RE = re.compile(rb'"ask"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_SPREAD_RE = re.compile(rb'"spread"\s*:\s*([-0-9.eE]+|null)')
_TARGET_STREAM_EXCHANGE_TS_RE = re.compile(rb'"exchange_timestamp"\s*:\s*"([^"]*)"')
_TARGET_STREAM_RECEIVED_IST_RE = re.compile(rb'"received_at_ist"\s*:\s*"([^"]*)"')


def float_from_bytes_match(pattern: re.Pattern[bytes], raw_line: bytes) -> float | None:
    match = pattern.search(raw_line)
    if not match:
        return None
    value = match.group(1)
    if value == b"null":
        return None
    try:
        out = float(value)
    except ValueError:
        return None
    return out if math.isfinite(out) else None


def string_from_bytes_match(pattern: re.Pattern[bytes], raw_line: bytes) -> str:
    match = pattern.search(raw_line)
    if not match:
        return ""
    return match.group(1).decode("utf-8", "ignore")


def target_items_from_raw_line(raw_line: bytes, target_set: set[str], target_bytes: set[bytes] | None = None) -> list[dict[str, Any]]:
    if b'"ticks"' in raw_line:
        return target_items_from_batch_line(raw_line.decode("utf-8", "ignore"), target_set)
    match = _RAW_INSTRUMENT_KEY_RE.search(raw_line)
    if not match:
        return []
    key_bytes = match.group(1)
    if target_bytes is not None and key_bytes not in target_bytes:
        return []
    key = key_bytes.decode("utf-8", "ignore")
    if key not in target_set:
        return []
    try:
        record = json.loads(raw_line)
    except json.JSONDecodeError:
        return []
    epoch_match = _RAW_RECEIVED_EPOCH_RE.search(raw_line)
    ist_match = _RAW_RECEIVED_IST_RE.search(raw_line)
    batch_epoch = as_float(epoch_match.group(1).decode("ascii", "ignore")) if epoch_match else None
    batch_ist = ist_match.group(1).decode("utf-8", "ignore") if ist_match else None
    return [
        {
            "instrument_key": key,
            "received_at_epoch": record.get("received_at_epoch", batch_epoch),
            "received_at_ist": record.get("received_at_ist", batch_ist),
            "tick": record.get("tick") or record,
        }
    ]


def iter_history_target_items(path: Path, *, target_hints: Iterable[str] | None = None) -> Iterable[dict[str, Any]]:
    targets = set(target_hints or [])
    target_bytes = {target.encode("utf-8") for target in targets}
    with gzip.open(path, "rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            yield from target_items_from_raw_line(raw_line, targets, target_bytes)


def iter_live_batch_target_items(path: Path, *, target_hints: Iterable[str] | None = None) -> Iterable[dict[str, Any]]:
    targets = set(target_hints or [])
    target_bytes = {target.encode("utf-8") for target in targets}
    with path.open("rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            yield from target_items_from_raw_line(raw_line, targets, target_bytes)


def row_from_target_stream_row(row: dict[str, Any], trade_date: str, target_set: set[str] | None = None) -> dict[str, Any] | None:
    key = str(row.get("key") or row.get("target") or "")
    if not key or (target_set is not None and key not in target_set):
        return None
    epoch = as_float(row.get("exchange_epoch")) or as_float(row.get("event_epoch")) or parse_market_timestamp_epoch(row.get("exchange_timestamp"))
    price = as_float(row.get("price")) or as_float(row.get("last_price")) or as_float(row.get("mid"))
    volume = as_float(row.get("volume_traded"))
    if epoch is None or price is None or volume is None:
        return None
    if epoch_ist_date(epoch) != trade_date:
        return None
    received_epoch = as_float(row.get("received_epoch"))
    bid = as_float(row.get("bid"))
    ask = as_float(row.get("ask"))
    return {
        "trade_date": trade_date,
        "target": key,
        "epoch": epoch,
        "epoch_second": int(epoch),
        "received_at_ist": str(row.get("received_at_ist") or ""),
        "exchange_timestamp": str(row.get("exchange_timestamp") or epoch_ist_iso(epoch) or ""),
        "received_epoch": received_epoch,
        "market_data_latency_seconds": (received_epoch - epoch) if received_epoch is not None else None,
        "price": price,
        "volume_traded": volume,
        "bid": bid,
        "ask": ask,
        "spread": (ask - bid) if ask is not None and bid is not None else as_float(row.get("spread")),
    }


def row_from_target_stream_line(raw_line: bytes, trade_date: str, target_set: set[str] | None = None) -> dict[str, Any] | None:
    key_match = _TARGET_STREAM_KEY_RE.search(raw_line)
    if not key_match:
        try:
            compact = json.loads(raw_line)
        except json.JSONDecodeError:
            return None
        return row_from_target_stream_row(compact, trade_date, target_set)
    key = key_match.group(1).decode("utf-8", "ignore")
    if not key or (target_set is not None and key not in target_set):
        return None
    epoch = float_from_bytes_match(_TARGET_STREAM_EXCHANGE_EPOCH_RE, raw_line)
    if epoch is None:
        epoch = float_from_bytes_match(_TARGET_STREAM_EVENT_EPOCH_RE, raw_line)
    price = float_from_bytes_match(_TARGET_STREAM_PRICE_RE, raw_line)
    if price is None:
        price = float_from_bytes_match(_TARGET_STREAM_LAST_PRICE_RE, raw_line)
    volume = float_from_bytes_match(_TARGET_STREAM_VOLUME_RE, raw_line)
    if epoch is None or price is None or volume is None:
        try:
            compact = json.loads(raw_line)
        except json.JSONDecodeError:
            return None
        return row_from_target_stream_row(compact, trade_date, target_set)
    if epoch_ist_date(epoch) != trade_date:
        return None
    received_epoch = float_from_bytes_match(_TARGET_STREAM_RECEIVED_EPOCH_RE, raw_line)
    bid = float_from_bytes_match(_TARGET_STREAM_BID_RE, raw_line)
    ask = float_from_bytes_match(_TARGET_STREAM_ASK_RE, raw_line)
    spread = float_from_bytes_match(_TARGET_STREAM_SPREAD_RE, raw_line)
    return {
        "trade_date": trade_date,
        "target": key,
        "epoch": epoch,
        "epoch_second": int(epoch),
        "received_at_ist": string_from_bytes_match(_TARGET_STREAM_RECEIVED_IST_RE, raw_line),
        "exchange_timestamp": string_from_bytes_match(_TARGET_STREAM_EXCHANGE_TS_RE, raw_line) or epoch_ist_iso(epoch) or "",
        "received_epoch": received_epoch,
        "market_data_latency_seconds": (received_epoch - epoch) if received_epoch is not None else None,
        "price": price,
        "volume_traded": volume,
        "bid": bid,
        "ask": ask,
        "spread": (ask - bid) if ask is not None and bid is not None else spread,
    }


def iter_history_gzip(path: Path, *, target_hints: Iterable[str] | None = None) -> Iterable[dict[str, Any]]:
    byte_hints = [hint.encode("utf-8") for hint in (target_hints or [])]
    with gzip.open(path, "rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            if byte_hints and not any(hint in raw_line for hint in byte_hints):
                continue
            yield json.loads(raw_line)


def iter_archive_history(path: Path, trade_date: str, *, target_hints: Iterable[str] | None = None) -> Iterable[dict[str, Any]]:
    member_suffix = f"ticks_{trade_date}.jsonl.gz"
    byte_hints = [hint.encode("utf-8") for hint in (target_hints or [])]
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            if not member.name.endswith(member_suffix):
                continue
            if not member.isfile():
                return
            raw = archive.extractfile(member)
            if raw is None:
                return
            with gzip.GzipFile(fileobj=raw) as gz:
                for raw_line in gz:
                    if not raw_line.strip():
                        continue
                    if byte_hints and not any(hint in raw_line for hint in byte_hints):
                        continue
                    yield json.loads(raw_line)
            return


def accepted_archive_payload_member(name: str, trade_date: str) -> bool:
    base = Path(name).name
    accepted = {
        f"ticks_{trade_date}.jsonl.gz",
        f"ticks_{trade_date}.jsonl",
        f"batches_{trade_date}.jsonl.gz",
        f"batches_{trade_date}.jsonl",
        f"live_batches_{trade_date}.jsonl.gz",
        f"live_batches_{trade_date}.jsonl",
        f"market_data_ticks_{trade_date}.jsonl.gz",
        f"market_data_ticks_{trade_date}.jsonl",
        f"market_data_ticks_nse_nfo_{trade_date}.jsonl.gz",
        f"market_data_ticks_nse_nfo_{trade_date}.jsonl",
        f"market_data_live_batches_nse_nfo_{trade_date}.jsonl.gz",
        f"market_data_live_batches_nse_nfo_{trade_date}.jsonl",
    }
    if base in accepted:
        return True
    return (base.endswith(".jsonl.gz") or base.endswith(".jsonl")) and f"{trade_date}/" in name


def iter_archive_payload_lines(path: Path, trade_date: str) -> Iterable[bytes]:
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            if not member.isfile() or not accepted_archive_payload_member(member.name, trade_date):
                continue
            raw = archive.extractfile(member)
            if raw is None:
                return
            if member.name.endswith(".gz"):
                with gzip.GzipFile(fileobj=raw) as gz:
                    yield from gz
            else:
                yield from raw
            return


def iter_archive_target_items(path: Path, trade_date: str, *, target_hints: Iterable[str] | None = None) -> Iterable[dict[str, Any]]:
    targets = set(target_hints or [])
    target_bytes = {target.encode("utf-8") for target in targets}
    for raw_line in iter_archive_payload_lines(path, trade_date):
        if not raw_line.strip():
            continue
        yield from target_items_from_raw_line(raw_line, targets, target_bytes)


def candidate_history_sources(config: dict[str, Any], trade_date: str) -> list[tuple[Path, str]]:
    producer_root = Path(str(config["producer_root"]))
    roots = [producer_root]
    roots.extend(Path(str(root)) for root in config.get("external_archive_roots", []) if root)
    history_sources: list[tuple[Path, str]] = []
    archive_sources: list[tuple[Path, str]] = []
    for root in roots:
        history_sources.extend(
            [
                (root / "live_batches" / trade_date / f"batches_{trade_date}.jsonl", "live_batches_jsonl"),
                (root / "nse_nfo" / "live_batches" / trade_date / f"batches_{trade_date}.jsonl", "live_batches_jsonl"),
                (root / "history" / trade_date / f"ticks_{trade_date}.jsonl.gz", "history_gzip"),
                (root / "nse_nfo" / "history" / trade_date / f"ticks_{trade_date}.jsonl.gz", "history_gzip"),
            ]
        )
        archive_sources.extend(
            [
                (root / "archives" / trade_date / f"market_data_ticks_{trade_date}.tar.gz", "archive_tar"),
                (root / trade_date / f"market_data_ticks_{trade_date}.tar.gz", "archive_tar"),
                (root / "nse_nfo" / "archives" / trade_date / f"market_data_ticks_nse_nfo_{trade_date}.tar.gz", "archive_tar"),
                (root / "nse_nfo" / "archives" / trade_date / f"market_data_live_batches_nse_nfo_{trade_date}.tar.gz", "archive_tar"),
                (root / "nse_nfo" / "archives" / trade_date / f"market_data_ticks_{trade_date}.tar.gz", "archive_tar"),
                (root / "nse_nfo" / trade_date / f"market_data_ticks_nse_nfo_{trade_date}.tar.gz", "archive_tar"),
                (root / "nse_nfo" / trade_date / f"market_data_live_batches_nse_nfo_{trade_date}.tar.gz", "archive_tar"),
                (root / "nse_nfo" / trade_date / f"market_data_ticks_{trade_date}.tar.gz", "archive_tar"),
            ]
        )
    return archive_sources + history_sources if bool(config.get("prefer_archive_tar_for_replay", False)) else history_sources + archive_sources


def iter_bootstrap_records(config: dict[str, Any], trade_date: str, targets: list[str]) -> tuple[Iterable[dict[str, Any]], dict[str, Any]]:
    for path, source_type in candidate_history_sources(config, trade_date):
        if not path.exists():
            continue
        if source_type == "live_batches_jsonl":
            return iter_live_batch_target_items(path, target_hints=targets), {
                "trade_date": trade_date,
                "source": str(path),
                "source_type": source_type,
                "source_found": True,
            }
        if source_type == "history_gzip":
            return iter_history_target_items(path, target_hints=targets), {
                "trade_date": trade_date,
                "source": str(path),
                "source_type": source_type,
                "source_found": True,
            }
        return iter_archive_target_items(path, trade_date, target_hints=targets), {
            "trade_date": trade_date,
            "source": str(path),
            "source_type": source_type,
            "source_found": True,
        }
    return iter(()), {
        "trade_date": trade_date,
        "source": "",
        "source_type": "",
        "source_found": False,
        "error": "no_history_or_archive_source_found",
    }


def iter_target_stream_normalized_rows(
    path: Path,
    trade_date: str,
    targets: Iterable[str],
    *,
    offset: int = 0,
    max_bytes: int | None = None,
) -> Iterable[dict[str, Any]]:
    target_set = set(targets)
    with path.open("rb") as handle:
        if offset > 0:
            handle.seek(offset)
        start_offset = handle.tell()
        while True:
            pos = handle.tell()
            if max_bytes is not None and pos - start_offset >= max_bytes:
                break
            line = handle.readline()
            if not line:
                break
            if not line.endswith(b"\n"):
                break
            row = row_from_target_stream_line(line, trade_date, target_set)
            if row is not None:
                yield row


def compact_target_stream_row_from_normalized(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": "obvfutport_v2.target_quote.v1",
        "key": row["target"],
        "event_epoch": row["epoch"],
        "exchange_epoch": row["epoch"],
        "received_epoch": row.get("received_epoch"),
        "exchange_timestamp": row.get("exchange_timestamp"),
        "received_at_ist": row.get("received_at_ist"),
        "price": row.get("price"),
        "volume_traded": row.get("volume_traded"),
        "bid": row.get("bid"),
        "ask": row.get("ask"),
        "spread": row.get("spread"),
    }


def _deep_merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge_dicts(dict(out[key]), value)
        else:
            out[key] = value
    return out


def _config_payload(point_config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(point_config, dict):
        return {}
    payload = point_config.get("point_thresholds")
    return payload if isinstance(payload, dict) else point_config


def _ttsl_config_from_point_config(point_config: dict[str, Any] | None) -> dict[str, Any] | None:
    payload = _config_payload(point_config)
    keys = {
        "two_lot_ttsl_enabled",
        "two_lot_ttsl_activation_clocks",
        "two_lot_ttsl_tighten_fraction",
        "two_lot_ttsl_tighten_pct",
        "two_lot_ttsl_sync_with_base_stop",
        "ttsl_activation_clocks",
        "ttsl_tighten_pct",
    }
    out = {key: payload[key] for key in keys if key in payload}
    return out or None


def _tranche3_config_from_adaptive(adaptive: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(adaptive, dict):
        return None
    combo = adaptive.get("tranche3_combo") if isinstance(adaptive.get("tranche3_combo"), dict) else {}
    override = adaptive.get("overrides") if isinstance(adaptive.get("overrides"), dict) else {}
    config = override.get("tranche3_config") if isinstance(override.get("tranche3_config"), dict) else {}
    merged = _deep_merge_dicts(dict(combo), dict(config))
    out: dict[str, Any] = {}
    if "enabled" in merged:
        out["tranche3_enabled"] = bool(merged.get("enabled"))
    elif "tranche3_enabled" in merged:
        out["tranche3_enabled"] = bool(merged.get("tranche3_enabled"))
    activation = as_float(merged.get("activation_clocks"))
    if activation is None:
        activation = as_float(merged.get("tranche3_activation_clocks"))
    if activation is not None:
        out["tranche3_activation_clocks"] = max(1, int(activation))
    r_multiple = as_float(merged.get("entry_r_multiple"))
    if r_multiple is None:
        r_multiple = as_float(merged.get("tranche3_entry_r_multiple"))
    if r_multiple is not None:
        out["tranche3_entry_r_multiple"] = max(0.0, float(r_multiple))
    entry_mode = merged.get("entry_mode") or merged.get("tranche3_entry_mode")
    if entry_mode:
        out["tranche3_entry_mode"] = str(entry_mode).strip().lower()
    pullback_r = as_float(merged.get("pullback_r_multiple"))
    if pullback_r is None:
        pullback_r = as_float(merged.get("tranche3_pullback_r_multiple"))
    if pullback_r is not None:
        out["tranche3_pullback_r_multiple"] = max(0.0, float(pullback_r))
    return out or None


def _tranche3_entry_params(config: dict[str, Any] | None) -> dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    entry_mode = str(payload.get("tranche3_entry_mode") or payload.get("entry_mode") or "momentum").strip().lower()
    if entry_mode not in {"momentum", "pullback"}:
        entry_mode = "momentum"
    activation = as_float(payload.get("tranche3_activation_clocks"))
    if activation is None:
        activation = as_float(payload.get("activation_clocks"))
    entry_r = as_float(payload.get("tranche3_entry_r_multiple"))
    if entry_r is None:
        entry_r = as_float(payload.get("entry_r_multiple"))
    pullback_r = as_float(payload.get("tranche3_pullback_r_multiple"))
    if pullback_r is None:
        pullback_r = as_float(payload.get("pullback_r_multiple"))
    return {
        "entry_mode": entry_mode,
        "activation_clocks": max(1, int(activation or 16)),
        "entry_r_multiple": max(0.0, float(entry_r if entry_r is not None else 0.75)),
        "pullback_r_multiple": max(0.0, float(pullback_r if pullback_r is not None else 0.50)),
    }


def _block_tranche3_v2(
    position: dict[str, Any],
    t3: dict[str, Any],
    *,
    t2_exit_epoch: int | None,
    reason: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    t3.update(
        {
            "status": "blocked_t2_already_exited",
            "blocked_reason": reason,
            "blocked_t2_exit_epoch": t2_exit_epoch,
            "blocked_at_ist": now_ist().isoformat(),
        }
    )
    position["tranche3"] = t3
    return position, []


def _update_live_tranche3_v2(
    *,
    v1_portfolio: Any,
    position: dict[str, Any],
    path: Any,
    clock_state: Any | None,
    latest_exit_fill_price: float | None,
    latest_exit_time: Any,
    cost_points: float,
    lot_size: int,
    point_config: dict[str, Any] | None,
    config: dict[str, Any] | None,
    final_epoch: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """v2 T3 updater: v1 momentum behavior plus explicit pullback entries."""
    params = _tranche3_entry_params(config)
    if params["entry_mode"] != "pullback":
        return v1_portfolio._update_live_tranche3(
            position=position,
            path=path,
            clock_state=clock_state,
            latest_exit_fill_price=latest_exit_fill_price,
            latest_exit_time=latest_exit_time,
            cost_points=cost_points,
            lot_size=lot_size,
            point_config=point_config,
            config=config,
            final_epoch=final_epoch,
        )

    import pandas as pd  # type: ignore

    events: list[dict[str, Any]] = []
    position = v1_portfolio._ensure_live_tranche3(position, config=config, lot_size=lot_size)
    t3 = dict(position.get("tranche3") or {})
    t3.update(
        {
            "entry_mode": "pullback",
            "pullback_r_multiple": params["pullback_r_multiple"],
            "activation_clocks": params["activation_clocks"],
            "entry_r_multiple": params["pullback_r_multiple"],
        }
    )
    if not t3.get("enabled", True):
        t3["status"] = "disabled"
        position["tranche3"] = t3
        return position, events
    if t3.get("status") in {"open", "closed"}:
        position = v1_portfolio._refresh_live_tranche3_marks(
            position,
            latest_exit_fill_price=latest_exit_fill_price,
            latest_exit_time=latest_exit_time,
            lot_size=lot_size,
            point_config=point_config,
        )
        return position, events
    if t3.get("status") == "blocked_t2_already_exited":
        position["tranche3"] = t3
        return position, events

    t2_exit_epoch = _tranche2_selected_exit_epoch(position)
    side = str(position.get("side") or "").lower()
    entry_epoch = int(position.get("entry_epoch") or 0)
    entry_price = as_float(position.get("entry_price"))
    hard_sl = as_float(position.get("hard_sl_points"))
    if side not in {"long", "short"} or entry_epoch <= 0 or entry_price is None or hard_sl is None or hard_sl <= 0:
        t3["status"] = "missing_base_entry_inputs"
        position["tranche3"] = t3
        return position, events

    trigger_points = float(hard_sl) * float(params["pullback_r_multiple"])
    hard_sl_price = float(entry_price) - float(hard_sl) if side == "long" else float(entry_price) + float(hard_sl)
    trigger_price = float(entry_price) - trigger_points if side == "long" else float(entry_price) + trigger_points
    t3.update(
        {
            "entry_trigger_points": trigger_points,
            "entry_trigger_price": trigger_price,
            "hard_sl_price": hard_sl_price,
        }
    )

    latest_epoch = final_epoch
    if latest_epoch is None and hasattr(path, "empty") and not path.empty and "epoch_second" in path.columns:
        epochs = pd.to_numeric(path["epoch_second"], errors="coerce").dropna()
        latest_epoch = int(epochs.max()) if not epochs.empty else None
    if t2_exit_epoch is not None:
        latest_epoch = min(int(latest_epoch), max(0, int(t2_exit_epoch) - 1)) if latest_epoch is not None else max(0, int(t2_exit_epoch) - 1)

    activation_epoch = int(t3.get("activation_epoch") or 0) or None
    if activation_epoch is None and clock_state is not None:
        clock_df = clock_state if isinstance(clock_state, pd.DataFrame) else pd.DataFrame(clock_state)
        if not clock_df.empty and "epoch_second" in clock_df.columns:
            epochs = pd.to_numeric(clock_df["epoch_second"], errors="coerce")
            clock_rows = clock_df.loc[(epochs > entry_epoch) & (epochs <= (latest_epoch or int(epochs.max() or entry_epoch)))].copy()
            clock_rows = clock_rows[epochs.loc[clock_rows.index].notna()].sort_values("epoch_second")
        else:
            clock_rows = pd.DataFrame()
        if len(clock_rows) >= int(params["activation_clocks"]):
            activation_row = clock_rows.iloc[int(params["activation_clocks"]) - 1]
            activation_epoch = int(activation_row["epoch_second"])
            t3.update(
                {
                    "activation_epoch": activation_epoch,
                    "activation_time": epoch_ist_iso(activation_epoch),
                    "activation_price": as_float(activation_row.get("price")),
                    "clocks_since_entry": int(len(clock_rows)),
                }
            )
        else:
            if t2_exit_epoch is not None:
                return _block_tranche3_v2(
                    position,
                    t3,
                    t2_exit_epoch=t2_exit_epoch,
                    reason="t2_exited_before_tranche3_activation_clocks",
                )
            t3["status"] = "waiting_activation_clocks"
            t3["clocks_since_entry"] = int(len(clock_rows))
            position["tranche3"] = t3
            return position, events
    if activation_epoch is None:
        if t2_exit_epoch is not None:
            return _block_tranche3_v2(
                position,
                t3,
                t2_exit_epoch=t2_exit_epoch,
                reason="t2_exited_before_tranche3_activation_epoch_available",
            )
        t3["status"] = "waiting_activation_clocks"
        position["tranche3"] = t3
        return position, events
    if t2_exit_epoch is not None and int(activation_epoch) >= int(t2_exit_epoch):
        return _block_tranche3_v2(
            position,
            t3,
            t2_exit_epoch=t2_exit_epoch,
            reason="t2_exited_before_tranche3_activation_epoch",
        )

    path_df = path if isinstance(path, pd.DataFrame) else pd.DataFrame(path)
    if path_df.empty or "epoch_second" not in path_df.columns or "price" not in path_df.columns:
        t3["status"] = "armed_waiting_trigger"
        position["tranche3"] = t3
        return position, events
    last_checked = int(t3.get("last_checked_epoch") or activation_epoch)
    scan_start = max(last_checked, int(activation_epoch))
    scan_end = int(latest_epoch) if latest_epoch is not None else scan_start
    if t2_exit_epoch is not None and scan_start >= int(t2_exit_epoch):
        return _block_tranche3_v2(
            position,
            t3,
            t2_exit_epoch=t2_exit_epoch,
            reason="t2_already_exited_before_next_tranche3_scan",
        )
    epochs = pd.to_numeric(path_df["epoch_second"], errors="coerce")
    candidates = path_df.loc[(epochs > scan_start) & (epochs <= scan_end)].copy()
    if candidates.empty:
        t3["status"] = "armed_waiting_trigger"
        position["tranche3"] = t3
        return position, events
    candidates["_epoch_for_tranche3"] = pd.to_numeric(candidates["epoch_second"], errors="coerce")
    candidates = candidates[candidates["_epoch_for_tranche3"].notna()].sort_values("_epoch_for_tranche3")
    trigger_row = None
    trigger_pnl = None
    for _, row in candidates.iterrows():
        price = as_float(row.get("price"))
        if price is None:
            continue
        pnl_points = price - float(entry_price) if side == "long" else float(entry_price) - price
        if side == "long":
            beyond_hard_sl = price <= hard_sl_price
            trigger_hit = hard_sl_price < price <= trigger_price
        else:
            beyond_hard_sl = price >= hard_sl_price
            trigger_hit = hard_sl_price > price >= trigger_price
        if beyond_hard_sl:
            t3["status"] = "pullback_reached_hard_sl_zone"
            t3["last_checked_epoch"] = int(row["_epoch_for_tranche3"])
            position["tranche3"] = t3
            return position, events
        if trigger_hit:
            trigger_row = row
            trigger_pnl = pnl_points
            break
    if trigger_row is None:
        t3["status"] = "armed_waiting_pullback"
        t3["last_checked_epoch"] = int(candidates["_epoch_for_tranche3"].max())
        position["tranche3"] = t3
        return position, events

    entry_fill = v1_portfolio.execution_fill_from_row(
        trigger_row,
        side=side,
        phase="entry",
        point_config=point_config,
        fallback_round_trip_cost_points=cost_points,
    )
    entry_fill_price = as_float(entry_fill.get("fill_price"))
    entry_ltp_price = as_float(entry_fill.get("ltp_price"))
    if entry_fill_price is None or entry_ltp_price is None:
        t3["status"] = "entry_fill_unavailable"
        t3["last_checked_epoch"] = int(trigger_row["_epoch_for_tranche3"])
        position["tranche3"] = t3
        return position, events
    t3_entry_epoch = int(entry_fill.get("epoch_second") or trigger_row.get("epoch_second") or 0)
    t3.update(
        {
            "status": "open",
            "entry_reason": f"tranche3_pullback_{params['activation_clocks']}c_{params['pullback_r_multiple']:g}R",
            "entry_time": entry_fill.get("time") or epoch_ist_iso(t3_entry_epoch),
            "entry_epoch": t3_entry_epoch,
            "entry_price": entry_ltp_price,
            "entry_ltp_price": entry_ltp_price,
            "entry_fill_price": entry_fill_price,
            "entry_fill_quality": entry_fill.get("fill_quality"),
            "entry_pnl_from_base_points": trigger_pnl,
            "entry_R_from_base": trigger_pnl / float(hard_sl) if hard_sl else None,
            "last_checked_epoch": t3_entry_epoch,
        }
    )
    position["tranche3"] = t3
    position = v1_portfolio._refresh_live_tranche3_marks(
        position,
        latest_exit_fill_price=latest_exit_fill_price,
        latest_exit_time=latest_exit_time,
        lot_size=lot_size,
        point_config=point_config,
    )
    event = v1_portfolio._live_tranche3_entry_event(position, dict(position.get("tranche3") or {}))
    event["entry_reason"] = t3["entry_reason"]
    event["entry_mode"] = "pullback"
    event["pullback_r_multiple"] = params["pullback_r_multiple"]
    event["tranche3"] = dict(position.get("tranche3") or {})
    events.append(event)
    return position, events


def _tranche3_entry_epoch(position: dict[str, Any] | None) -> int | None:
    if not isinstance(position, dict):
        return None
    tranche3 = position.get("tranche3")
    if not isinstance(tranche3, dict):
        return None
    entry_epoch = as_float(tranche3.get("entry_epoch"))
    if entry_epoch is None or entry_epoch <= 0:
        return None
    return int(entry_epoch)


def _tranche3_close_allowed(position: dict[str, Any] | None, exit_epoch: Any) -> bool:
    entry_epoch = _tranche3_entry_epoch(position)
    parsed_exit = as_float(exit_epoch)
    if entry_epoch is None or parsed_exit is None:
        return False
    return int(parsed_exit) >= int(entry_epoch)


def _tranche2_selected_exit_epoch(position: dict[str, Any] | None) -> int | None:
    if not isinstance(position, dict):
        return None
    two_lot = position.get("two_lot_ttsl")
    if not isinstance(two_lot, dict):
        return None
    candidates: list[Any] = []
    tranche2 = two_lot.get("tranche2")
    if isinstance(tranche2, dict):
        candidates.append(tranche2.get("exit_epoch"))
    candidates.extend(
        [
            two_lot.get("partial_exit_epoch"),
            two_lot.get("tranche2_exit_epoch"),
            two_lot.get("ttsl_exit_epoch"),
        ]
    )
    parsed = [int(value) for value in (as_float(item) for item in candidates) if value is not None and value > 0]
    return min(parsed) if parsed else None


def _tranche3_final_epoch(position: dict[str, Any] | None, proposed_final_epoch: Any) -> int | None:
    proposed = as_float(proposed_final_epoch)
    cap = int(proposed) if proposed is not None and proposed > 0 else None
    tranche2_exit_epoch = _tranche2_selected_exit_epoch(position)
    if tranche2_exit_epoch is not None:
        tranche2_cap = max(0, int(tranche2_exit_epoch) - 1)
        cap = tranche2_cap if cap is None else min(cap, tranche2_cap)
    return cap


def _valid_tranche3_event(position: dict[str, Any] | None, event: dict[str, Any]) -> bool:
    if not isinstance(event, dict):
        return False
    if event.get("event") != "tranche3_exit":
        return True
    return _tranche3_close_allowed(position, event.get("exit_epoch"))


def _filter_valid_tranche3_events(
    position: dict[str, Any] | None,
    events: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [event for event in events if _valid_tranche3_event(position, event)]


def _scale_dynamic_point_config(
    point_config: dict[str, Any],
    *,
    kind: str,
    scale: float,
) -> dict[str, Any]:
    out = _deep_merge_dicts({}, point_config or {})
    if not math.isfinite(scale) or abs(scale - 1.0) < 1e-12:
        return out
    if isinstance(out.get("point_thresholds"), dict):
        payload = out["point_thresholds"]
    else:
        payload = out
    target = payload.get(kind) if isinstance(payload.get(kind), dict) else {}
    target = dict(target)
    for key in (
        "multiplier",
        "floor_points",
        "cap_points",
        "fallback_points",
        "floor_bps",
        "cap_bps",
        "fallback_bps",
    ):
        value = as_float(target.get(key))
        if value is not None:
            target[key] = float(value) * float(scale)
    target[f"adaptive_{kind}_scale"] = float(scale)
    payload[kind] = target
    return out


def _materialize_position_exit_profile(
    point_config: dict[str, Any] | None,
    position: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = _deep_merge_dicts({}, point_config or {})
    profile = payload.get("exit_profile") if isinstance(payload.get("exit_profile"), dict) else {}
    profile = dict(profile)
    mfe_r = as_float(profile.get("min_profit_or_mfe_r"))
    hard_sl = as_float((position or {}).get("hard_sl_points"))
    if mfe_r is not None and hard_sl is not None:
        profile["min_profit_or_mfe_points"] = float(mfe_r) * float(hard_sl)
    if profile:
        payload["exit_profile"] = profile
    return payload


def _adaptive_event_fields(meta: "InstrumentMeta") -> dict[str, Any]:
    adaptive = meta.adaptive_calibration if isinstance(meta.adaptive_calibration, dict) else {}
    if not adaptive:
        return {}
    metrics = adaptive.get("metrics") if isinstance(adaptive.get("metrics"), dict) else {}
    deltas = metrics.get("deltas") if isinstance(metrics.get("deltas"), dict) else {}
    return {
        "adaptive_calibration": adaptive,
        "adaptive_tier": adaptive.get("tier"),
        "adaptive_tags": adaptive.get("tags"),
        "adaptive_combo_label": adaptive.get("combo_label"),
        "adaptive_exit_combo_label": adaptive.get("exit_combo_label"),
        "adaptive_tranche3_combo_label": adaptive.get("tranche3_combo_label"),
        "adaptive_net_delta_rupees": deltas.get("net_delta_rupees"),
        "adaptive_success_rate_delta_pct": deltas.get("success_rate_delta_pct"),
        "adaptive_worst_loss_pct_delta": deltas.get("worst_loss_pct_delta"),
        "adaptive_drawdown_delta_rupees": deltas.get("drawdown_delta_rupees"),
    }


def _bps_points(price: float, bps: Any) -> float | None:
    value = as_float(bps)
    if value is None or not math.isfinite(price):
        return None
    return price * value / 10_000.0


def _floor_points(cfg: dict[str, Any], price: float, legacy: float) -> float:
    candidates: list[float] = []
    if "floor_points" in cfg:
        value = as_float(cfg.get("floor_points"))
        if value is not None:
            candidates.append(value)
    value = _bps_points(price, cfg.get("floor_bps"))
    if value is not None:
        candidates.append(value)
    return max(candidates) if candidates else legacy


def _cap_points(cfg: dict[str, Any], price: float, legacy: float) -> float:
    candidates: list[float] = []
    if "cap_points" in cfg:
        value = as_float(cfg.get("cap_points"))
        if value is not None:
            candidates.append(value)
    value = _bps_points(price, cfg.get("cap_bps"))
    if value is not None:
        candidates.append(value)
    return min(candidates) if candidates else legacy


def _fallback_points(cfg: dict[str, Any], price: float, legacy: float, floor: float) -> float:
    value = as_float(cfg.get("fallback_points"))
    if value is not None:
        return value
    bps_value = _bps_points(price, cfg.get("fallback_bps"))
    if bps_value is not None:
        return bps_value
    return floor if math.isfinite(floor) else legacy


def dynamic_points(
    unit: float,
    *,
    price: float,
    kind: str,
    point_config: dict[str, Any] | None,
    legacy_multiplier: float,
    legacy_floor: float,
    legacy_cap: float,
    legacy_fallback: float,
) -> float:
    cfg = None
    if isinstance(point_config, dict):
        payload = point_config.get("point_thresholds") if isinstance(point_config.get("point_thresholds"), dict) else point_config
        raw = payload.get(kind) if isinstance(payload, dict) else None
        cfg = dict(raw) if isinstance(raw, dict) else None
    if cfg is None:
        if math.isfinite(unit):
            return min(max(legacy_multiplier * unit, legacy_floor), legacy_cap)
        return legacy_fallback
    multiplier = as_float(cfg.get("multiplier"))
    if multiplier is None:
        multiplier = legacy_multiplier
    floor = _floor_points(cfg, price, legacy_floor)
    cap = _cap_points(cfg, price, legacy_cap)
    if cap < floor:
        cap = floor
    fallback = _fallback_points(cfg, price, legacy_fallback, floor)
    if math.isfinite(unit):
        return min(max(multiplier * unit, floor), cap)
    return fallback


def z_from_stats(value: float, stats: dict[str, float | int], min_periods: int = MIN_PRIOR_SECONDS) -> float:
    count = int(stats.get("count") or 0)
    total = float(stats.get("sum") or 0.0)
    sumsq = float(stats.get("sumsq") or 0.0)
    if not math.isfinite(value) or count < min_periods or count <= 1:
        return math.nan
    mean = total / count
    variance = (sumsq - (total * total / count)) / (count - 1.0)
    std = math.sqrt(max(variance, 0.0))
    return (value - mean) / std if std > 0 else math.nan


def update_stats(stats: dict[str, float | int], value: float) -> None:
    if not math.isfinite(value):
        return
    stats["count"] = int(stats.get("count") or 0) + 1
    stats["sum"] = float(stats.get("sum") or 0.0) + value
    stats["sumsq"] = float(stats.get("sumsq") or 0.0) + value * value


def sorted_quantile(values: list[float], q: float) -> float:
    if not values:
        return math.nan
    rank = (len(values) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return float(values[lower])
    return float(values[lower] + (values[upper] - values[lower]) * (rank - lower))


def clock_epochs_for_day(
    day: date,
    *,
    clock_start: str,
    clock_end: str,
    clock_step_minutes: int,
) -> list[int]:
    hh, mm = parse_hhmm(clock_start)
    eh, em = parse_hhmm(clock_end)
    current = datetime(day.year, day.month, day.day, hh, mm, tzinfo=IST)
    final = datetime(day.year, day.month, day.day, eh, em, tzinfo=IST)
    out: list[int] = []
    while current <= final:
        out.append(int(current.timestamp()))
        current += timedelta(minutes=clock_step_minutes)
    return out


def is_model_clock_epoch(epoch: int, clock_epochs: set[int]) -> bool:
    return int(epoch) in clock_epochs


def previous_trading_day(day: date, holidays: set[date] | None = None) -> date:
    holiday_dates = holidays or set()
    current = day - timedelta(days=1)
    while current.weekday() >= 5 or current in holiday_dates:
        current -= timedelta(days=1)
    return current


def contract_lifecycle_start(
    chain: list[dict[str, Any]],
    index: int,
    holidays: set[date] | None = None,
) -> str | None:
    item = chain[index]
    if item.get("baseline_start_date"):
        return str(item["baseline_start_date"])
    if item.get("lifecycle_start_date"):
        return str(item["lifecycle_start_date"])
    if index <= 0:
        return None
    expiry = chain[index - 1].get("expiry_date")
    return previous_trading_day(date.fromisoformat(str(expiry)), holidays).isoformat() if expiry else None


def contract_roll_datetime(
    chain: list[dict[str, Any]],
    index: int,
    roll_time_ist: str | None,
    holidays: set[date] | None = None,
) -> datetime:
    item = chain[index]
    roll_date = item.get("roll_date")
    if not roll_date:
        roll_date = previous_trading_day(date.fromisoformat(str(item["expiry_date"])), holidays).isoformat()
    hh, mm = parse_hhmm(str(roll_time_ist or "15:25"))
    return datetime(date.fromisoformat(str(roll_date)).year, date.fromisoformat(str(roll_date)).month, date.fromisoformat(str(roll_date)).day, hh, mm, tzinfo=IST)


def current_contract_index(
    chain: list[dict[str, Any]],
    when: datetime,
    roll_time_ist: str | None,
    holidays: set[date] | None = None,
) -> int:
    if not chain:
        return 0
    current = 0
    for index in range(len(chain) - 1):
        if when >= contract_roll_datetime(chain, index, roll_time_ist, holidays):
            current = index + 1
    return current


def lifecycle_status_from_meta(
    meta: "InstrumentMeta",
    when: datetime | None = None,
    holidays: set[date] | None = None,
) -> dict[str, Any]:
    when = when or now_ist()
    chain = list(meta.contract_chain or [])
    if not chain:
        return {"enabled": False}
    current_index = 0
    rollovers: list[dict[str, Any]] = []
    active_rollover: dict[str, Any] | None = None
    for index in range(len(chain) - 1):
        roll_dt = contract_roll_datetime(chain, index, meta.roll_execution_time_ist, holidays)
        roll = {
            "rollover_id": f"{chain[index]['instrument_key']}->{chain[index + 1]['instrument_key']}@{roll_dt.isoformat()}",
            "from_index": index,
            "to_index": index + 1,
            "from_instrument_key": chain[index]["instrument_key"],
            "to_instrument_key": chain[index + 1]["instrument_key"],
            "from_label": chain[index]["label"],
            "to_label": chain[index + 1]["label"],
            "from_expiry_date": chain[index].get("expiry_date"),
            "to_expiry_date": chain[index + 1].get("expiry_date"),
            "roll_datetime_ist": roll_dt.isoformat(),
            "roll_date": roll_dt.date().isoformat(),
            "roll_time_ist": roll_dt.strftime("%H:%M"),
            "next_lifecycle_start_date": contract_lifecycle_start(chain, index + 1, holidays),
            "status": "due" if when >= roll_dt else "pending",
        }
        rollovers.append(roll)
        if when >= roll_dt:
            current_index = index + 1
            active_rollover = roll
    shadow_index = current_index + 1 if current_index + 1 < len(chain) else None
    return {
        "enabled": True,
        "policy": "next_contract_baseline_start = previous NSE trading day before current_contract_expiry",
        "current_index": current_index,
        "shadow_index": shadow_index,
        "current_contract": chain[current_index]["instrument_key"],
        "shadow_contract": chain[shadow_index]["instrument_key"] if shadow_index is not None else None,
        "current_lifecycle_start_date": contract_lifecycle_start(chain, current_index, holidays),
        "shadow_lifecycle_start_date": contract_lifecycle_start(chain, shadow_index, holidays)
        if shadow_index is not None
        else None,
        "active_rollover": active_rollover,
        "rollovers": rollovers,
    }


@dataclass
class InstrumentMeta:
    symbol: str
    display_name: str
    signal_source: str
    signal_key: str
    execution_key: str
    cash_key: str | None
    lot_size: int
    margin_long: float | None
    margin_short: float | None
    point_config: dict[str, Any]
    signal_point_config: dict[str, Any]
    execution_point_config: dict[str, Any]
    signal_contract_label: str
    execution_contract_label: str
    lifecycle_start_date: str | None
    expiry_date: str | None
    round_trip_cost_points: float
    contract_chain: list[dict[str, Any]]
    current_contract_index: int
    shadow_execution_key: str | None
    shadow_signal_key: str | None
    roll_execution_time_ist: str | None
    target_keys: list[str]
    source: str
    synthesized: bool = False
    adaptive_calibration: dict[str, Any] = field(default_factory=dict)


@dataclass
class SecondSnapshot:
    epoch_second: int
    trade_date: str
    price: float
    volume: float
    bid: float | None
    ask: float | None
    spread: float | None
    received_at_ist: str | None
    exchange_timestamp: str | None
    received_epoch: float | None
    market_data_latency_seconds: float | None
    obv: float
    tick_rule_obv: float
    price_change_since_start: float
    obv_change_since_start: float
    tick_rule_obv_change_since_start: float
    price_prior_z: float
    obv_prior_z: float
    obv_minus_price_prior_z: float
    prior_percentile: float
    prior_p05: float
    prior_p10: float
    prior_p90: float
    prior_p95: float
    source_quote_epoch: float | None = None
    source_received_epoch: float | None = None


@dataclass
class OnlineObvState:
    key: str
    clock_epochs: set[int]
    min_prior_seconds: int = MIN_PRIOR_SECONDS
    second_row_retention_seconds: int | None = None
    obv: float = 0.0
    tick_rule_obv: float = 0.0
    last_price: float | None = None
    last_volume: float | None = None
    last_trade_date: str | None = None
    last_sign: int = 0
    baseline_price: float | None = None
    current_second: int | None = None
    current_snapshot: dict[str, Any] | None = None
    last_snapshot_payload: dict[str, Any] | None = None
    last_finalized_second: int | None = None
    latest_quote_epoch: float | None = None
    latest_received_epoch: float | None = None
    latest_price: float | None = None
    latest_bid: float | None = None
    latest_ask: float | None = None
    price_stats: dict[str, float | int] = field(default_factory=lambda: {"count": 0, "sum": 0.0, "sumsq": 0.0})
    obv_stats: dict[str, float | int] = field(default_factory=lambda: {"count": 0, "sum": 0.0, "sumsq": 0.0})
    sorted_spread_z: array = field(default_factory=lambda: array("d"))
    compute_non_clock_percentiles: bool = True
    spread_z_values_sorted: bool = True
    metric_by_clock_epoch: dict[int, SecondSnapshot] = field(default_factory=dict)
    previous_clock_prices: list[float] = field(default_factory=list)
    previous_clock_price_changes: list[float] = field(default_factory=list)
    second_rows: list[dict[str, Any]] = field(default_factory=list)
    clock_rows: list[dict[str, Any]] = field(default_factory=list)
    prev_range_by_clock_epoch: dict[int, float] = field(default_factory=dict)
    prev_range_history: list[float] = field(default_factory=list)
    interval_high: float | None = None
    interval_low: float | None = None
    processed_ticks: int = 0
    finalized_seconds: int = 0
    skipped_non_append_ticks: int = 0

    def ensure_spread_z_sorted(self) -> None:
        if not isinstance(self.sorted_spread_z, array):
            self.sorted_spread_z = float_array(self.sorted_spread_z)
        if self.spread_z_values_sorted:
            return
        self.sorted_spread_z = array("d", sorted(self.sorted_spread_z))
        self.spread_z_values_sorted = True

    def set_second_row_retention(self, retention_seconds: int | None) -> None:
        self.second_row_retention_seconds = retention_seconds
        self.trim_second_rows()

    def trim_second_rows(self, *, current_epoch: int | None = None) -> None:
        retention = self.second_row_retention_seconds
        if retention is None:
            return
        if retention <= 0:
            self.second_rows.clear()
            return
        anchor = current_epoch or self.last_finalized_second
        if anchor is None:
            return
        cutoff = int(anchor) - int(retention)
        drop = 0
        for row in self.second_rows:
            if int(row.get("epoch_second") or 0) >= cutoff:
                break
            drop += 1
        if drop:
            del self.second_rows[:drop]

    def process_row(self, row: dict[str, Any]) -> None:
        epoch_second = int(row["epoch_second"])
        if self.current_second is not None and epoch_second < self.current_second:
            self.skipped_non_append_ticks += 1
            return
        if self.current_second is None:
            self._apply_tick(row)
            self.current_second = epoch_second
            self.current_snapshot = self._snapshot_payload(row)
            self.processed_ticks += 1
            return
        if epoch_second == self.current_second:
            self._apply_tick(row)
            self.current_snapshot = self._snapshot_payload(row)
            self.processed_ticks += 1
            return
        self._flush_current()
        while self.last_finalized_second is not None and self.last_finalized_second + 1 < epoch_second:
            carry_epoch = self.last_finalized_second + 1
            carry = dict(self.last_snapshot_payload or {})
            if not carry:
                break
            carry["epoch_second"] = carry_epoch
            carry["epoch"] = float(carry_epoch)
            carry["received_at_ist"] = carry.get("received_at_ist")
            carry["exchange_timestamp"] = epoch_ist_iso(carry_epoch)
            carry["market_data_latency_seconds"] = None
            self._finalize_payload(carry, carried=True)
        self._apply_tick(row)
        self.current_second = epoch_second
        self.current_snapshot = self._snapshot_payload(row)
        self.processed_ticks += 1

    def _apply_tick(self, row: dict[str, Any]) -> None:
        trade_date = str(row["trade_date"])
        price = float(row["price"])
        volume = float(row["volume_traded"])
        if self.baseline_price is None:
            self.baseline_price = price
        same_day = self.last_trade_date == trade_date
        if same_day and self.last_volume is not None and volume >= self.last_volume:
            delta_volume = volume - self.last_volume
        else:
            delta_volume = 0.0
        sign = 0
        if same_day and self.last_price is not None:
            if price > self.last_price:
                sign = 1
            elif price < self.last_price:
                sign = -1
        if sign != 0:
            self.last_sign = sign
        self.obv += sign * delta_volume
        self.tick_rule_obv += (self.last_sign if sign == 0 else sign) * delta_volume
        self.last_price = price
        self.last_volume = volume
        self.last_trade_date = trade_date
        self.latest_quote_epoch = float(row["epoch"])
        self.latest_received_epoch = as_float(row.get("received_epoch"))
        self.latest_price = price
        self.latest_bid = as_float(row.get("bid"))
        self.latest_ask = as_float(row.get("ask"))

    def _snapshot_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            **row,
            "source_quote_epoch": as_float(row.get("source_quote_epoch")) or as_float(row.get("epoch")),
            "source_received_epoch": as_float(row.get("source_received_epoch")) or as_float(row.get("received_epoch")),
            "obv": self.obv,
            "tick_rule_obv": self.tick_rule_obv,
            "price_change_since_start": float(row["price"]) - float(self.baseline_price or row["price"]),
            "obv_change_since_start": self.obv,
            "tick_rule_obv_change_since_start": self.tick_rule_obv,
        }

    def _flush_current(self) -> None:
        if self.current_snapshot is None:
            return
        self._finalize_payload(self.current_snapshot, carried=False)
        self.current_snapshot = None
        self.current_second = None

    def flush_until_latest(self) -> None:
        self._flush_current()

    def finalize_until(self, epoch_second: int, *, max_gap_seconds: int = 300) -> int:
        target = int(epoch_second)
        if self.current_second is not None and self.current_second <= target:
            self._flush_current()
        if self.last_finalized_second is None or self.last_snapshot_payload is None:
            return 0
        gap = target - int(self.last_finalized_second)
        if gap <= 0 or gap > max_gap_seconds:
            return 0
        built = 0
        while self.last_finalized_second is not None and self.last_finalized_second < target:
            carry_epoch = int(self.last_finalized_second) + 1
            carry = dict(self.last_snapshot_payload or {})
            if not carry:
                break
            carry["epoch_second"] = carry_epoch
            carry["epoch"] = float(carry_epoch)
            carry["received_at_ist"] = carry.get("received_at_ist")
            carry["exchange_timestamp"] = epoch_ist_iso(carry_epoch)
            carry["market_data_latency_seconds"] = None
            self._finalize_payload(carry, carried=True)
            built += 1
        return built

    def _finalize_payload(self, payload: dict[str, Any], *, carried: bool) -> None:
        epoch_second = int(payload["epoch_second"])
        price = float(payload["price"])
        is_clock = is_model_clock_epoch(epoch_second, self.clock_epochs)
        if is_clock:
            if self.interval_high is None or self.interval_low is None:
                self.prev_range_by_clock_epoch[epoch_second] = math.nan
            else:
                self.prev_range_by_clock_epoch[epoch_second] = float(self.interval_high - self.interval_low)
            self.interval_high = price
            self.interval_low = price
        else:
            self.interval_high = price if self.interval_high is None else max(self.interval_high, price)
            self.interval_low = price if self.interval_low is None else min(self.interval_low, price)

        price_change = float(payload["price_change_since_start"])
        obv_change = float(payload["obv_change_since_start"])
        price_z = z_from_stats(price_change, self.price_stats, self.min_prior_seconds)
        obv_z = z_from_stats(obv_change, self.obv_stats, self.min_prior_seconds)
        spread_z = obv_z - price_z if math.isfinite(price_z) and math.isfinite(obv_z) else math.nan
        prior_len = len(self.sorted_spread_z)
        compute_percentiles = self.compute_non_clock_percentiles or is_clock
        if compute_percentiles and math.isfinite(spread_z) and prior_len >= self.min_prior_seconds:
            self.ensure_spread_z_sorted()
            prior_pct = 100.0 * bisect.bisect_right(self.sorted_spread_z, spread_z) / prior_len
        else:
            prior_pct = math.nan
        if is_clock and prior_len >= self.min_prior_seconds:
            self.ensure_spread_z_sorted()
            prior_p05 = sorted_quantile(self.sorted_spread_z, 0.05)
            prior_p10 = sorted_quantile(self.sorted_spread_z, 0.10)
            prior_p90 = sorted_quantile(self.sorted_spread_z, 0.90)
            prior_p95 = sorted_quantile(self.sorted_spread_z, 0.95)
        else:
            prior_p05 = prior_p10 = prior_p90 = prior_p95 = math.nan
        snapshot = SecondSnapshot(
            epoch_second=epoch_second,
            trade_date=str(payload["trade_date"]),
            price=price,
            volume=float(payload["volume_traded"]),
            bid=as_float(payload.get("bid")),
            ask=as_float(payload.get("ask")),
            spread=as_float(payload.get("spread")),
            received_at_ist=payload.get("received_at_ist"),
            exchange_timestamp=payload.get("exchange_timestamp"),
            received_epoch=as_float(payload.get("received_epoch")),
            market_data_latency_seconds=as_float(payload.get("market_data_latency_seconds")),
            obv=self.obv,
            tick_rule_obv=self.tick_rule_obv,
            price_change_since_start=price_change,
            obv_change_since_start=obv_change,
            tick_rule_obv_change_since_start=float(payload["tick_rule_obv_change_since_start"]),
            price_prior_z=price_z,
            obv_prior_z=obv_z,
            obv_minus_price_prior_z=spread_z,
            prior_percentile=prior_pct,
            prior_p05=prior_p05,
            prior_p10=prior_p10,
            prior_p90=prior_p90,
            prior_p95=prior_p95,
            source_quote_epoch=as_float(payload.get("source_quote_epoch")) or as_float(payload.get("epoch")),
            source_received_epoch=as_float(payload.get("source_received_epoch")) or as_float(payload.get("received_epoch")),
        )
        if self.second_row_retention_seconds != 0:
            self.second_rows.append(
                {
                    "trade_date": snapshot.trade_date,
                    "epoch_second": snapshot.epoch_second,
                    "epoch": float(snapshot.epoch_second),
                    "received_at_ist": snapshot.received_at_ist,
                    "exchange_timestamp": snapshot.exchange_timestamp or epoch_ist_iso(snapshot.epoch_second),
                    "received_epoch": snapshot.received_epoch,
                    "source_quote_epoch": snapshot.source_quote_epoch,
                    "source_received_epoch": snapshot.source_received_epoch,
                    "market_data_latency_seconds": snapshot.market_data_latency_seconds,
                    "price": snapshot.price,
                    "volume_traded": snapshot.volume,
                    "bid": snapshot.bid,
                    "ask": snapshot.ask,
                    "spread": snapshot.spread,
                    "obv": snapshot.obv,
                    "tick_rule_obv": snapshot.tick_rule_obv,
                    "price_change_since_start": snapshot.price_change_since_start,
                    "obv_change_since_start": snapshot.obv_change_since_start,
                    "tick_rule_obv_change_since_start": snapshot.tick_rule_obv_change_since_start,
                    "price_prior_z": snapshot.price_prior_z,
                    "obv_prior_z": snapshot.obv_prior_z,
                    "obv_minus_price_prior_z": snapshot.obv_minus_price_prior_z,
                    "prior_percentile": snapshot.prior_percentile,
                    "prior_p05": snapshot.prior_p05,
                    "prior_p10": snapshot.prior_p10,
                    "prior_p90": snapshot.prior_p90,
                    "prior_p95": snapshot.prior_p95,
                    "second_index": self.finalized_seconds,
                    "carried": bool(carried),
                }
            )
            self.trim_second_rows(current_epoch=epoch_second)
        if is_clock:
            self.metric_by_clock_epoch[epoch_second] = snapshot
        self.last_snapshot_payload = dict(payload)
        update_stats(self.price_stats, price_change)
        update_stats(self.obv_stats, obv_change)
        if math.isfinite(spread_z):
            if self.compute_non_clock_percentiles:
                self.ensure_spread_z_sorted()
                bisect.insort(self.sorted_spread_z, spread_z)
            else:
                self.sorted_spread_z.append(spread_z)
                self.spread_z_values_sorted = False
        self.last_finalized_second = epoch_second
        self.finalized_seconds += 1

    def quote_age_at(self, decision_epoch: int) -> float | None:
        if self.latest_quote_epoch is None:
            return None
        return float(decision_epoch) - float(self.latest_quote_epoch)

    def build_clock_row(self, clock_epoch: int, clock_label: str, point_config: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        for existing in reversed(self.clock_rows):
            try:
                if int(existing.get("epoch_second") or 0) == int(clock_epoch):
                    return dict(existing), None
            except Exception:
                continue
        self.finalize_until(clock_epoch)
        snap = self.metric_by_clock_epoch.get(int(clock_epoch))
        if snap is None:
            return None, {
                "reason": "missing_clock_metric",
                "last_finalized_second": self.last_finalized_second,
                "latest_quote_epoch": self.latest_quote_epoch,
                "latest_quote_age_seconds": self.quote_age_at(clock_epoch),
            }
        price = as_float(snap.price)
        price_change_since_start = as_float(snap.price_change_since_start)
        metric = as_float(snap.obv_minus_price_prior_z)
        prior_p90 = as_float(snap.prior_p90)
        prior_p95 = as_float(snap.prior_p95)
        prior_p10 = as_float(snap.prior_p10)
        prior_price_values = [
            value
            for raw in self.previous_clock_price_changes
            if (value := as_float(raw)) is not None and math.isfinite(value)
        ]
        if price_change_since_start is not None and prior_price_values:
            price_pct = 100.0 * sum(value <= price_change_since_start for value in prior_price_values) / len(prior_price_values)
        else:
            price_pct = math.nan
        prior_prices = [
            value
            for raw in self.previous_clock_prices[-12:]
            if (value := as_float(raw)) is not None and math.isfinite(value)
        ]
        lookback_high = max(prior_prices) if len(prior_prices) >= 12 else math.nan
        lookback_low = min(prior_prices) if len(prior_prices) >= 12 else math.nan
        prior_ranges = [
            value
            for raw in self.prev_range_history[-20:]
            if (value := as_float(raw)) is not None and math.isfinite(value)
        ]
        prior_clock_vol = float(median(prior_ranges)) if len(prior_ranges) >= 4 else math.nan
        breakout_points = dynamic_points(
            prior_clock_vol,
            price=price,
            kind="fresh_breakout",
            point_config=point_config,
            legacy_multiplier=1.4,
            legacy_floor=30.0,
            legacy_cap=120.0,
            legacy_fallback=50.0,
        )
        enough_history = len(self.clock_rows) >= MIN_CLOCK_HISTORY
        bearish_absent = metric is not None and prior_p90 is not None and metric < prior_p90
        bullish_absent = metric is not None and prior_p10 is not None and metric > prior_p10
        cfg_payload = _config_payload(point_config)
        long_strength_threshold = (
            as_float(cfg_payload.get("fresh_long_price_strength_pct"))
            or as_float(cfg_payload.get("long_price_strength_pct"))
            or 95.0
        )
        short_weakness_threshold = (
            as_float(cfg_payload.get("fresh_short_price_weakness_pct"))
            or as_float(cfg_payload.get("short_price_weakness_pct"))
            or 1.0
        )
        long_strength_pass = math.isfinite(price_pct) and price_pct >= float(long_strength_threshold)
        short_weakness_pass = math.isfinite(price_pct) and price_pct <= float(short_weakness_threshold)
        long_trigger_price = lookback_high + breakout_points if math.isfinite(lookback_high) else math.nan
        short_trigger_price = lookback_low - breakout_points if math.isfinite(lookback_low) else math.nan
        long_price_trigger_pass = price is not None and math.isfinite(long_trigger_price) and price >= long_trigger_price
        short_price_trigger_pass = price is not None and math.isfinite(short_trigger_price) and price <= short_trigger_price
        primary_short_p90_pass = metric is not None and prior_p90 is not None and metric >= prior_p90
        primary_short_abs15_pass = metric is not None and metric >= 1.5
        primary_short_abs20_pass = metric is not None and metric >= 2.0
        primary_short_warning_abs15 = enough_history and primary_short_p90_pass and primary_short_abs15_pass
        primary_short_warning_abs20 = enough_history and primary_short_p90_pass and primary_short_abs20_pass
        primary_short_price_regime_pass = math.isfinite(price_pct) and price_pct <= 50.0
        v53_short_early_weakness_pass = math.isfinite(price_pct) and price_pct <= 10.0
        v53_long_relaxed_obv_pass = metric is not None and prior_p95 is not None and metric < prior_p95
        fresh_long = enough_history and bearish_absent and long_strength_pass and long_price_trigger_pass
        fresh_short = enough_history and bullish_absent and short_weakness_pass and short_price_trigger_pass
        previous = self.clock_rows[-1] if self.clock_rows else {}
        prior_warning15 = bool(previous.get("primary_obv_short_abs15_warning"))
        long_trigger_prior = bool(previous.get("fresh_long_price_trigger_pass"))
        short_early_prior = bool(previous.get("v53_short_early_weakness_pass"))
        primary_short_sustained_failed_reclaim_pass = (
            primary_short_warning_abs15
            and prior_warning15
            and math.isfinite(price_pct)
            and price_pct <= 50.0
            and math.isfinite(lookback_high)
            and price is not None
            and price <= lookback_high
        )
        primary_short_execution_confirm_pass = (
            primary_short_price_regime_pass or fresh_short or primary_short_sustained_failed_reclaim_pass
        )
        primary_obv_short_abs15_active = primary_short_warning_abs15 and primary_short_execution_confirm_pass
        primary_obv_short_abs20_active = primary_short_warning_abs20 and primary_short_execution_confirm_pass
        primary_short_threshold = as_float(point_config.get("primary_obv_short_abs_threshold")) or 1.5
        primary_configured_active = (
            enough_history
            and primary_short_p90_pass
            and metric is not None
            and metric >= primary_short_threshold
            and primary_short_execution_confirm_pass
        )
        v53_long_hold2_price_trigger_pass = long_price_trigger_pass and long_trigger_prior
        v53_long_warning = enough_history and long_strength_pass and long_price_trigger_pass
        v53_long_execution_confirm_pass = bearish_absent or (v53_long_hold2_price_trigger_pass and v53_long_relaxed_obv_pass)
        v53_long_executable = v53_long_warning and v53_long_execution_confirm_pass
        v53_short_weak2_pass = v53_short_early_weakness_pass and short_early_prior
        v53_short_high_confidence = enough_history and bullish_absent and short_weakness_pass and short_price_trigger_pass
        v53_short_early_warning = enough_history and bullish_absent and v53_short_early_weakness_pass and short_price_trigger_pass
        v53_short_early_executable = v53_short_early_warning and v53_short_weak2_pass
        v53_short_executable = v53_short_high_confidence or v53_short_early_executable
        row = {
            "trade_date": snap.trade_date,
            "clock_label": clock_label,
            "clock_time": epoch_ist_iso(clock_epoch),
            "has_clock_row": True,
            "actual_time": epoch_ist_iso(clock_epoch),
            "received_at_ist": snap.received_at_ist,
            "exchange_timestamp": snap.exchange_timestamp or epoch_ist_iso(clock_epoch),
            "market_data_latency_seconds": snap.market_data_latency_seconds,
            "source_quote_epoch": snap.source_quote_epoch,
            "source_received_epoch": snap.source_received_epoch,
            "source_quote_age_seconds": (
                float(clock_epoch) - float(snap.source_quote_epoch)
                if snap.source_quote_epoch is not None
                else None
            ),
            "epoch_second": clock_epoch,
            "price": price,
            "price_change_since_start": price_change_since_start,
            "obv_change_since_start": snap.obv_change_since_start,
            "obv_minus_price_prior_z": metric,
            "prior_percentile": snap.prior_percentile,
            "prior_p05": snap.prior_p05,
            "prior_p10": prior_p10,
            "prior_p90": prior_p90,
            "prior_p95": prior_p95,
            "price_change_prior_pct": price_pct,
            "prior_lookback_high": lookback_high,
            "prior_lookback_low": lookback_low,
            "prev_clock_range_points": self.prev_range_by_clock_epoch.get(clock_epoch, math.nan),
            "prior_clock_vol_points": prior_clock_vol,
            "effective_fresh_breakout_points": breakout_points,
            "long_trigger_price": long_trigger_price,
            "short_trigger_price": short_trigger_price,
            "signal_enough_history": enough_history,
            "fresh_long_bearish_absent_pass": bearish_absent,
            "fresh_long_price_strength_pass": long_strength_pass,
            "fresh_long_price_strength_pct_threshold": float(long_strength_threshold),
            "fresh_long_price_trigger_pass": long_price_trigger_pass,
            "fresh_short_bullish_absent_pass": bullish_absent,
            "fresh_short_price_weakness_pass": short_weakness_pass,
            "fresh_short_price_weakness_pct_threshold": float(short_weakness_threshold),
            "fresh_short_price_trigger_pass": short_price_trigger_pass,
            "primary_short_p90_pass": primary_short_p90_pass,
            "primary_short_abs15_pass": primary_short_abs15_pass,
            "primary_short_abs20_pass": primary_short_abs20_pass,
            "primary_short_price_regime_pass": primary_short_price_regime_pass,
            "primary_short_fresh_breakdown_pass": fresh_short,
            "primary_short_sustained_failed_reclaim_pass": primary_short_sustained_failed_reclaim_pass,
            "primary_short_execution_confirm_pass": primary_short_execution_confirm_pass,
            "primary_obv_short_abs15_warning": primary_short_warning_abs15,
            "primary_obv_short_abs20_warning": primary_short_warning_abs20,
            "primary_obv_short_abs15_active": primary_obv_short_abs15_active,
            "primary_obv_short_abs20_active": primary_obv_short_abs20_active,
            "primary_obv_short_abs15_blocked_by_trend": primary_short_warning_abs15 and not primary_obv_short_abs15_active,
            "primary_obv_short_abs20_blocked_by_trend": primary_short_warning_abs20 and not primary_obv_short_abs20_active,
            "primary_obv_short_configured_active": primary_configured_active,
            "v53_long_warning": v53_long_warning,
            "v53_long_executable": v53_long_executable,
            "v53_long_execution_confirm_pass": v53_long_execution_confirm_pass,
            "v53_long_blocked_by_obv": v53_long_warning and not v53_long_executable,
            "v53_short_high_confidence": v53_short_high_confidence,
            "v53_short_early_warning": v53_short_early_warning,
            "v53_short_early_executable": v53_short_early_executable,
            "v53_short_executable": v53_short_executable,
        }
        for column in (
            "fresh_trend_long_active",
            "fresh_trend_short_active",
            "primary_obv_short_configured_active",
            "v53_long_warning",
            "v53_long_executable",
            "v53_short_early_warning",
            "v53_short_executable",
        ):
            if column == "fresh_trend_long_active":
                value = fresh_long
            elif column == "fresh_trend_short_active":
                value = fresh_short
            elif column == "primary_obv_short_configured_active":
                value = primary_configured_active
            else:
                value = bool(row[column])
            row[column] = bool(value)
            row[column + "_edge"] = bool(value) and not bool(previous.get(column))
        self.clock_rows.append(row)
        if price is not None:
            self.previous_clock_prices.append(price)
        if price_change_since_start is not None:
            self.previous_clock_price_changes.append(price_change_since_start)
        prev_range = self.prev_range_by_clock_epoch.get(clock_epoch, math.nan)
        self.prev_range_history.append(float(prev_range) if math.isfinite(prev_range) else math.nan)
        return row, None

    def to_payload(self) -> dict[str, Any]:
        self.ensure_spread_z_sorted()
        return {
            "schema": "obvfutport_v2.online_obv_state.v1",
            "key": self.key,
            "clock_epochs": sorted(int(epoch) for epoch in self.clock_epochs),
            "min_prior_seconds": self.min_prior_seconds,
            "second_row_retention_seconds": self.second_row_retention_seconds,
            "compute_non_clock_percentiles": self.compute_non_clock_percentiles,
            "spread_z_values_sorted": self.spread_z_values_sorted,
            "obv": self.obv,
            "tick_rule_obv": self.tick_rule_obv,
            "last_price": self.last_price,
            "last_volume": self.last_volume,
            "last_trade_date": self.last_trade_date,
            "last_sign": self.last_sign,
            "baseline_price": self.baseline_price,
            "current_second": self.current_second,
            "current_snapshot": self.current_snapshot,
            "last_snapshot_payload": self.last_snapshot_payload,
            "last_finalized_second": self.last_finalized_second,
            "latest_quote_epoch": self.latest_quote_epoch,
            "latest_received_epoch": self.latest_received_epoch,
            "latest_price": self.latest_price,
            "latest_bid": self.latest_bid,
            "latest_ask": self.latest_ask,
            "price_stats": self.price_stats,
            "obv_stats": self.obv_stats,
            "sorted_spread_z": encode_float_array(self.sorted_spread_z),
            "metric_by_clock_epoch": {
                str(epoch): snapshot.__dict__ for epoch, snapshot in self.metric_by_clock_epoch.items()
            },
            "previous_clock_prices": self.previous_clock_prices,
            "previous_clock_price_changes": self.previous_clock_price_changes,
            "second_rows": self.second_rows,
            "clock_rows": self.clock_rows,
            "prev_range_by_clock_epoch": {str(epoch): value for epoch, value in self.prev_range_by_clock_epoch.items()},
            "prev_range_history": self.prev_range_history,
            "interval_high": self.interval_high,
            "interval_low": self.interval_low,
            "processed_ticks": self.processed_ticks,
            "finalized_seconds": self.finalized_seconds,
            "skipped_non_append_ticks": self.skipped_non_append_ticks,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any], clock_epochs: set[int]) -> "OnlineObvState":
        state = cls(
            key=str(payload["key"]),
            clock_epochs=set(int(epoch) for epoch in payload.get("clock_epochs") or clock_epochs),
            min_prior_seconds=int(payload.get("min_prior_seconds") or MIN_PRIOR_SECONDS),
            second_row_retention_seconds=(
                int(payload["second_row_retention_seconds"])
                if payload.get("second_row_retention_seconds") is not None
                else None
            ),
        )
        for field_name in (
            "second_row_retention_seconds",
            "obv",
            "tick_rule_obv",
            "last_price",
            "last_volume",
            "last_trade_date",
            "last_sign",
            "baseline_price",
            "current_second",
            "current_snapshot",
            "last_snapshot_payload",
            "last_finalized_second",
            "latest_quote_epoch",
            "latest_received_epoch",
            "latest_price",
            "latest_bid",
            "latest_ask",
            "price_stats",
            "obv_stats",
            "sorted_spread_z",
            "compute_non_clock_percentiles",
            "spread_z_values_sorted",
            "previous_clock_prices",
            "previous_clock_price_changes",
            "second_rows",
            "clock_rows",
            "prev_range_history",
            "interval_high",
            "interval_low",
            "processed_ticks",
            "finalized_seconds",
            "skipped_non_append_ticks",
        ):
            if field_name in payload:
                setattr(state, field_name, payload[field_name])
        state.clock_epochs = set(int(epoch) for epoch in payload.get("clock_epochs") or clock_epochs)
        state.metric_by_clock_epoch = {
            int(epoch): SecondSnapshot(**snapshot)
            for epoch, snapshot in (payload.get("metric_by_clock_epoch") or {}).items()
            if isinstance(snapshot, dict)
        }
        state.prev_range_by_clock_epoch = {
            int(epoch): float(value) if value is not None else math.nan
            for epoch, value in (payload.get("prev_range_by_clock_epoch") or {}).items()
        }
        state.sorted_spread_z = array("d", sorted(decode_float_array(state.sorted_spread_z)))
        state.spread_z_values_sorted = True
        return state


def canonical_signal_id(
    *,
    strategy_id: str,
    instrument_id: str,
    side: str,
    module: str,
    signal_epoch: int,
    signal_source: str,
    signal_instrument_key: str,
    execution_instrument_key: str,
) -> str:
    parts = [
        strategy_id,
        instrument_id,
        side,
        module,
        str(signal_epoch),
        signal_source,
        signal_instrument_key,
        execution_instrument_key,
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{strategy_id}:{instrument_id}:{side}:{signal_epoch}:{digest}"


class PassiveV2Runner:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.config = read_json(config_path, {})
        self.state_dir = Path(str(self.config["state_dir"]))
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            local_state = Path(str(self.config.get("state_dir_local") or ""))
            if not local_state:
                raise
            self.state_dir = local_state
            self.state_dir.mkdir(parents=True, exist_ok=True)
        self.pointer_path = self.state_dir / "live_batch_pointer.json"
        self.target_stream_pointer_path = self.state_dir / "target_stream_consumer_pointer.json"
        self.status_path = self.state_dir / "status.json"
        self.telemetry_path = self.state_dir / "telemetry.jsonl"
        self.controlled_repair_queue_path = self.state_dir / "controlled_repair_queue.jsonl"
        self.decision_events_root = self.state_dir / "decision_events"
        self.instruments_root = self.state_dir / "instruments"
        self.producer_root = Path(str(self.config["producer_root"]))
        self.strategy_id = str(self.config.get("strategy_id") or "OBVFUTPORT_V2_PASSIVE")
        self.poll_seconds = float(self.config.get("poll_seconds") or 1.0)
        self.status_write_seconds = float(self.config.get("status_write_seconds") or 5.0)
        self.telemetry_write_seconds = float(self.config.get("telemetry_write_seconds") or 15.0)
        self.decision_delay_seconds = int(self.config.get("decision_delay_seconds") or 25)
        self.tail_max_bytes = int(self.config.get("tail_max_bytes_per_cycle") or 12_000_000)
        self.consume_target_stream = bool(self.config.get("consume_target_stream", False))
        self.target_stream_tail_max_bytes = int(self.config.get("target_stream_tail_max_bytes_per_cycle") or 4_000_000)
        self.signal_quote_max_age = float(self.config.get("signal_quote_max_age_seconds") or 45.0)
        retention_config = self.config.get("second_row_retention_seconds")
        self.second_row_retention_seconds = retention_seconds_from_config(retention_config)
        self.flat_second_row_retention_seconds = retention_seconds_from_config(
            self.config.get("flat_second_row_retention_seconds")
            if self.config.get("flat_second_row_retention_seconds") is not None
            else self.second_row_retention_seconds
        )
        self.pending_second_row_retention_seconds = retention_seconds_from_config(
            self.config.get("pending_second_row_retention_seconds")
        )
        self.active_second_row_retention_seconds = retention_seconds_from_config(
            self.config.get("active_second_row_retention_seconds")
        )
        self.shadow_lifecycle_second_row_retention_seconds = retention_seconds_from_config(
            self.config.get("shadow_lifecycle_second_row_retention_seconds")
            if self.config.get("shadow_lifecycle_second_row_retention_seconds") is not None
            else 27000
        )
        self.lifecycle_reset_second_row_retention_seconds = retention_seconds_from_config(
            self.config.get("lifecycle_reset_second_row_retention_seconds")
            if self.config.get("lifecycle_reset_second_row_retention_seconds") is not None
            else self.active_second_row_retention_seconds
        )
        self.transition_second_row_retention_seconds = retention_seconds_from_config(
            self.config.get("transition_second_row_retention_seconds")
            if self.config.get("transition_second_row_retention_seconds") is not None
            else self.active_second_row_retention_seconds
        )
        self.memory_retention_soft_limit_mb = as_float(
            self.config.get("passive_memory_retention_soft_limit_mb")
            if self.config.get("passive_memory_retention_soft_limit_mb") is not None
            else self.config.get("memory_retention_soft_limit_mb")
        )
        if self.memory_retention_soft_limit_mb is None:
            self.memory_retention_soft_limit_mb = 8192.0
        self.memory_pressure_shadow_lifecycle_second_row_retention_seconds = retention_seconds_from_config(
            self.config.get("memory_pressure_shadow_lifecycle_second_row_retention_seconds")
            if self.config.get("memory_pressure_shadow_lifecycle_second_row_retention_seconds") is not None
            else 900
        )
        self.compute_non_clock_percentiles = bool(self.config.get("compute_non_clock_percentiles", True))
        self.clock_start = str(self.config.get("clock_start_ist") or "09:20")
        self.clock_end = str(self.config.get("clock_end_ist") or "15:20")
        self.clock_step_minutes = int(self.config.get("clock_step_minutes") or 15)
        self.market_start = parse_hhmmss(str(self.config.get("market_start_ist") or "09:15:00"))
        self.market_end = parse_hhmmss(str(self.config.get("market_end_ist") or "15:30:00"))
        self.today = now_ist().date()
        self.holiday_dates = load_holiday_dates(
            resolve_config_path(self.config, "holiday_calendar_path", "holiday_calendar_path_local")
        )
        self.clock_epochs = set(
            clock_epochs_for_day(
                self.today,
                clock_start=self.clock_start,
                clock_end=self.clock_end,
                clock_step_minutes=self.clock_step_minutes,
            )
        )
        self.instruments = self.load_instruments()
        self.targets = sorted({key for item in self.instruments.values() for key in item.target_keys if key})
        self.target_set = set(self.targets)
        self.states = {
            key: OnlineObvState(
                key=key,
                clock_epochs=self.clock_epochs,
                second_row_retention_seconds=self.second_row_retention_seconds,
                compute_non_clock_percentiles=self.compute_non_clock_percentiles,
            )
            for key in self.targets
        }
        self.model_states = {symbol: self.load_model_state(symbol) for symbol in self.instruments}
        self.bootstrap_report: dict[str, Any] = {}
        self.session_id = f"{int(time.time())}-{os.getpid()}"
        self.started_at = now_ist()
        self.stop_requested = False
        self.rows_seen = 0
        self.quotes_seen = 0
        self.events_seen: set[str] = set()
        self.clock_watermark: int | None = None
        self.last_actual_evaluated_clock: int | None = None
        self.skipped_startup_due_clock: int | None = None
        self.latest_tail_report: dict[str, Any] = {}
        self.latest_decision_report: dict[str, Any] = {}
        self.latest_trade_state_report: dict[str, Any] = {}
        self.latest_transition_signal_report: dict[str, Any] = {}
        self.latest_rollover_report: dict[str, Any] = {}
        self.latest_retention_report: dict[str, Any] = {}
        self.latest_memory_pressure_report: dict[str, Any] = {}
        self.latest_due_clock_event_symbols: list[str] = []
        self.latest_feed_epoch: float | None = None
        self.partial_live_start = False
        self.partial_live_start_trade_date: str | None = None
        self.run_deadline_epoch: float | None = None
        self._last_transition_signal_eval_monotonic = 0.0
        self._last_active_trade_state_eval_monotonic = 0.0
        self._last_suppressed_decision_telemetry_monotonic = 0.0
        self._last_catchup_defer_telemetry_monotonic = 0.0
        if self.should_defer_bootstrap_for_partial_live_start():
            self.partial_live_start = True
            self.partial_live_start_trade_date = now_ist().date().isoformat()
        self.bootstrap_report = self.load_bootstrap_states()
        self.refresh_dynamic_retention()
        if bool(self.config.get("skip_past_due_clocks_on_start", True)):
            self.skipped_startup_due_clock = self.latest_due_clock_epoch(time.time())
            self.clock_watermark = self.skipped_startup_due_clock

    def load_instruments(self) -> dict[str, InstrumentMeta]:
        v1_config = read_json(
            resolve_config_path(self.config, "obvfut_v1_runtime_path", "obvfut_v1_runtime_path_local"),
            {},
        )
        manifest = read_json(
            resolve_config_path(self.config, "hurst_universe_manifest_path", "hurst_universe_manifest_path_local"),
            {},
        )
        v1_by_symbol = {str(item.get("id") or item.get("symbol")): item for item in v1_config.get("instruments", [])}
        base_point_config = dict(v1_config.get("point_thresholds") or {})
        cash_signal_defaults = dict(v1_config.get("cash_signal_point_thresholds") or {})
        cash_exec_defaults = dict(v1_config.get("cash_execution_point_thresholds") or {})
        entries = manifest.get("entries") or []
        valid_synthesized_shadow_keys = load_key_manifest(
            self.config,
            "synthesized_shadow_keys_path",
            "synthesized_shadow_keys_path_local",
        )
        contract_chain_manifest = load_contract_chain_manifest(self.config)
        require_shadow_manifest = bool(self.config.get("require_synthesized_shadow_key_in_manifest", False))
        adaptive_overrides = self.load_adaptive_calibrations()
        out: dict[str, InstrumentMeta] = {}
        index_symbols = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}

        def lifecycle_active_shadow_keys(
            chain: list[dict[str, Any]],
            shadow_index: int | None,
            shadow_execution_key: str | None,
            shadow_signal_key: str | None,
        ) -> set[str]:
            if shadow_index is None:
                return set()
            shadow_start = contract_lifecycle_start(chain, shadow_index, self.holiday_dates)
            if shadow_start and self.today.isoformat() < str(shadow_start):
                return set()
            return {key for key in {shadow_execution_key, shadow_signal_key} if key}

        def live_target_keys(
            *,
            signal_key: str | None,
            execution_key: str | None,
            cash_key: str | None,
            chain: list[dict[str, Any]],
            shadow_index: int | None,
            shadow_execution_key: str | None,
            shadow_signal_key: str | None,
        ) -> list[str]:
            keys = {key for key in {signal_key, execution_key, cash_key} if key}
            keys.update(lifecycle_active_shadow_keys(chain, shadow_index, shadow_execution_key, shadow_signal_key))
            return sorted(str(key) for key in keys if key)

        for entry in entries:
            symbol = str(entry["symbol"])
            if symbol in v1_by_symbol:
                item = dict(v1_by_symbol[symbol])
                contracts = ((item.get("contract_lifecycle") or {}).get("contracts") or [])
                chain = merge_contract_chain_with_manifest(
                    [dict(contract) for contract in contracts if isinstance(contract, dict)],
                    contract_chain_manifest.get(symbol),
                )
                roll_time = str((item.get("contract_lifecycle") or {}).get("roll_execution_time_ist") or "15:25")
                current_index = current_contract_index(chain, now_ist(), roll_time, self.holiday_dates) if chain else 0
                current_contract = dict(chain[current_index]) if chain else {}
                shadow_index = current_index + 1 if chain and current_index + 1 < len(chain) else None
                shadow_contract = dict(chain[shadow_index]) if shadow_index is not None else {}
                current_key = str(current_contract.get("instrument_key") or entry.get("fut_key") or "")
                current_label = str(current_contract.get("label") or "current")
                signal_source = str(item.get("default_signal_source") or "futures")
                signal_key = str(item.get("cash_instrument_key") or entry.get("cash_key") or current_key) if signal_source == "cash" else current_key
                execution_key = current_key
                shadow_execution_key = str(shadow_contract.get("instrument_key") or "") or None
                shadow_signal_key = signal_key if signal_source == "cash" and shadow_execution_key else shadow_execution_key
                cash_key = str(item.get("cash_instrument_key") or entry.get("cash_key") or "")
                target_keys = live_target_keys(
                    signal_key=signal_key,
                    execution_key=execution_key,
                    cash_key=cash_key,
                    chain=chain,
                    shadow_index=shadow_index,
                    shadow_execution_key=shadow_execution_key,
                    shadow_signal_key=shadow_signal_key,
                )
                signal_point_config = dict(item.get("cash_signal_point_thresholds") or item.get("point_thresholds") or cash_signal_defaults or base_point_config) if signal_source == "cash" else dict(item.get("point_thresholds") or base_point_config)
                execution_point_config = dict(item.get("cash_execution_point_thresholds") or item.get("point_thresholds") or cash_exec_defaults or base_point_config) if signal_source == "cash" else dict(item.get("point_thresholds") or base_point_config)
                signal_point_config, execution_point_config, adaptive_meta = self.apply_adaptive_calibration(
                    symbol,
                    signal_point_config,
                    execution_point_config,
                    adaptive_overrides,
                )
                point_config = signal_point_config
                out[symbol] = InstrumentMeta(
                    symbol=symbol,
                    display_name=str(item.get("display_name") or symbol),
                    signal_source=signal_source,
                    signal_key=signal_key,
                    execution_key=execution_key,
                    cash_key=cash_key,
                    lot_size=int(item.get("lot_size") or entry.get("lot_size") or 1),
                    margin_long=as_float(item.get("margin_per_lot_long_rupees") or entry.get("margin_long")),
                    margin_short=as_float(item.get("margin_per_lot_short_rupees") or entry.get("margin_short")),
                    point_config=point_config,
                    signal_point_config=signal_point_config,
                    execution_point_config=execution_point_config,
                    signal_contract_label=f"{current_label}_cash_signal" if signal_source == "cash" else current_label,
                    execution_contract_label=current_label,
                    lifecycle_start_date=contract_lifecycle_start(chain, current_index, self.holiday_dates) if chain else current_contract.get("baseline_start_date") or self.config.get("new_symbol_baseline_start_date"),
                    expiry_date=current_contract.get("expiry_date") or entry.get("expiry"),
                    round_trip_cost_points=as_float(item.get("round_trip_cost_points")) or as_float(self.config.get("fallback_round_trip_cost_points")) or 1.0,
                    contract_chain=chain,
                    current_contract_index=current_index,
                    shadow_execution_key=shadow_execution_key,
                    shadow_signal_key=shadow_signal_key,
                    roll_execution_time_ist=roll_time,
                    target_keys=[str(key) for key in target_keys],
                    source="v1_runtime",
                    synthesized=False,
                    adaptive_calibration=adaptive_meta,
                )
                continue
            cash_key = entry.get("cash_key")
            fut_key = str(entry["fut_key"])
            sep_fut_key = synthesized_september_future_key(fut_key)
            if require_shadow_manifest and sep_fut_key not in valid_synthesized_shadow_keys:
                sep_fut_key = None
            signal_source = "futures" if symbol in index_symbols or not cash_key else str(self.config.get("default_stock_signal_source") or "cash")
            if signal_source == "cash":
                signal_point_config = _deep_merge_dicts(cash_signal_defaults, {"primary_obv_short_abs_threshold": 1.5})
                execution_point_config = dict(cash_exec_defaults or base_point_config)
                signal_key = str(cash_key)
                signal_contract_label = "august_main_cash_signal"
                shadow_signal_key = signal_key if sep_fut_key else None
            else:
                signal_point_config = dict(base_point_config)
                execution_point_config = dict(base_point_config)
                signal_key = fut_key
                signal_contract_label = "august_main"
                shadow_signal_key = sep_fut_key
            base_contract_chain = [
                {
                    "label": "august_main",
                    "instrument_key": fut_key,
                    "baseline_start_date": str(self.config.get("new_symbol_baseline_start_date") or "2026-08-10"),
                    "expiry_date": str(entry.get("expiry") or "2026-08-25"),
                }
            ]
            if sep_fut_key:
                base_contract_chain.append(
                    {
                        "label": "september_shadow",
                        "instrument_key": sep_fut_key,
                        "expiry_date": str(self.config.get("synthesized_september_expiry_date") or "2026-09-29"),
                    }
                )
            contract_chain = merge_contract_chain_with_manifest(base_contract_chain, contract_chain_manifest.get(symbol))
            current_index = current_contract_index(contract_chain, now_ist(), "15:25", self.holiday_dates) if contract_chain else 0
            current_contract = dict(contract_chain[current_index]) if contract_chain else {}
            shadow_index = current_index + 1 if contract_chain and current_index + 1 < len(contract_chain) else None
            shadow_contract = dict(contract_chain[shadow_index]) if shadow_index is not None else {}
            current_key = str(current_contract.get("instrument_key") or fut_key)
            current_label = str(current_contract.get("label") or "current")
            shadow_execution_key = str(shadow_contract.get("instrument_key") or "") or None
            shadow_signal_key = str(cash_key) if signal_source == "cash" and shadow_execution_key else shadow_execution_key
            if signal_source == "futures":
                signal_key = current_key
                signal_contract_label = current_label
            else:
                signal_key = str(cash_key)
                signal_contract_label = f"{current_label}_cash_signal"
            signal_point_config, execution_point_config, adaptive_meta = self.apply_adaptive_calibration(
                symbol,
                signal_point_config,
                execution_point_config,
                adaptive_overrides,
            )
            out[symbol] = InstrumentMeta(
                symbol=symbol,
                display_name=symbol,
                signal_source=signal_source,
                signal_key=signal_key,
                execution_key=current_key,
                cash_key=str(cash_key) if cash_key else None,
                lot_size=int(entry.get("lot_size") or 1),
                margin_long=as_float(entry.get("margin_long")),
                margin_short=as_float(entry.get("margin_short")),
                point_config=signal_point_config,
                signal_point_config=signal_point_config,
                execution_point_config=execution_point_config,
                signal_contract_label=signal_contract_label,
                execution_contract_label=current_label,
                lifecycle_start_date=contract_lifecycle_start(contract_chain, current_index, self.holiday_dates) if contract_chain else str(self.config.get("new_symbol_baseline_start_date") or "2026-08-10"),
                expiry_date=str(current_contract.get("expiry_date") or entry.get("expiry") or "2026-08-25"),
                round_trip_cost_points=as_float(entry.get("round_trip_cost_points")) or as_float(self.config.get("fallback_round_trip_cost_points")) or 1.0,
                contract_chain=contract_chain,
                current_contract_index=current_index,
                shadow_execution_key=shadow_execution_key,
                shadow_signal_key=shadow_signal_key,
                roll_execution_time_ist="15:25",
                target_keys=live_target_keys(
                    signal_key=signal_key,
                    execution_key=current_key,
                    cash_key=str(cash_key) if cash_key else None,
                    chain=contract_chain,
                    shadow_index=shadow_index,
                    shadow_execution_key=shadow_execution_key,
                    shadow_signal_key=shadow_signal_key,
                ),
                source="hurst_manifest_synthesized",
                synthesized=True,
                adaptive_calibration=adaptive_meta,
            )
        return out

    def adaptive_calibration_path(self) -> Path:
        configured = str(self.config.get("adaptive_calibration_path") or "")
        local = str(self.config.get("adaptive_calibration_path_local") or "")
        if configured:
            path = Path(configured)
            if path.exists():
                return path
        if local:
            path = Path(local)
            if path.exists():
                return path
        return self.state_dir / "adaptive_calibration" / "v2_symbol_overrides_latest.json"

    def load_adaptive_calibrations(self) -> dict[str, Any]:
        if not bool(self.config.get("adaptive_calibration_enabled", True)):
            return {}
        payload = read_json(self.adaptive_calibration_path(), {})
        symbols = payload.get("symbols") if isinstance(payload, dict) else {}
        return symbols if isinstance(symbols, dict) else {}

    def apply_adaptive_calibration(
        self,
        symbol: str,
        signal_point_config: dict[str, Any],
        execution_point_config: dict[str, Any],
        adaptive_overrides: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        item = adaptive_overrides.get(symbol)
        if not isinstance(item, dict) or not item.get("adopted"):
            return dict(signal_point_config), dict(execution_point_config), {}
        overrides = item.get("overrides") if isinstance(item.get("overrides"), dict) else {}
        signal_override = overrides.get("signal_point_config") if isinstance(overrides.get("signal_point_config"), dict) else {}
        execution_override = (
            overrides.get("execution_point_config")
            if isinstance(overrides.get("execution_point_config"), dict)
            else {}
        )
        signal = _deep_merge_dicts(dict(signal_point_config), signal_override)
        execution = _deep_merge_dicts(dict(execution_point_config), execution_override)
        exit_combo = item.get("exit_combo") if isinstance(item.get("exit_combo"), dict) else {}
        hard_sl_scale = as_float(exit_combo.get("hard_sl_scale"))
        trail_activation_scale = as_float(exit_combo.get("trail_activation_scale"))
        if hard_sl_scale is not None:
            execution = _scale_dynamic_point_config(execution, kind="hard_sl", scale=float(hard_sl_scale))
        if trail_activation_scale is not None:
            execution = _scale_dynamic_point_config(
                execution,
                kind="trail_activation",
                scale=float(trail_activation_scale),
            )
        return signal, execution, dict(item)

    def calibrate_open_position_state(
        self,
        meta: InstrumentMeta,
        model_state: dict[str, Any],
        events: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if not meta.adaptive_calibration:
            return model_state, events
        v1_portfolio = load_v1_portfolio_module(self.config)
        ttsl_config = _ttsl_config_from_point_config(meta.execution_point_config)
        tranche3_config = _tranche3_config_from_adaptive(meta.adaptive_calibration)

        def _calibrate(position: Any) -> Any:
            if not isinstance(position, dict):
                return position
            out = dict(position)
            out.update(_adaptive_event_fields(meta))
            out["adaptive_calibration_applied_at_ist"] = now_ist().isoformat()
            if ttsl_config:
                out = v1_portfolio._ensure_live_two_lot_ttsl(
                    out,
                    config=ttsl_config,
                    lot_size=int(meta.lot_size or 1),
                )
            out = v1_portfolio._ensure_live_tranche3(
                out,
                config=tranche3_config,
                lot_size=int(meta.lot_size or 1),
            )
            return out

        state = dict(model_state or {})
        if isinstance(state.get("position"), dict):
            state["position"] = _calibrate(state["position"])
        updated_events: list[dict[str, Any]] = []
        for event in events:
            item = dict(event)
            if isinstance(item.get("position"), dict):
                item["position"] = _calibrate(item["position"])
            updated_events.append(item)
        return state, updated_events

    def live_batches_path(self, trade_date: str) -> Path:
        return self.producer_root / "live_batches" / trade_date / f"batches_{trade_date}.jsonl"

    def target_stream_root(self) -> Path:
        primary = Path(str(self.config.get("target_stream_root") or (self.state_dir / "target_stream")))
        local = Path(str(self.config.get("target_stream_root_local") or ""))
        configured_state = Path(str(self.config.get("state_dir") or ""))
        if local and self.state_dir != configured_state:
            return local
        return primary

    def target_stream_path(self, trade_date: str) -> Path:
        return self.target_stream_root() / trade_date / f"target_quotes_{trade_date}.jsonl"

    def bootstrap_state_root(self) -> Path:
        return self.state_dir / "bootstrap_state"

    def bootstrap_manifest_path(self, as_of_date: str | None = None) -> Path:
        if as_of_date:
            return self.bootstrap_state_root() / as_of_date / "manifest.json"
        return self.bootstrap_state_root() / "latest_manifest.json"

    def bootstrap_state_file(self, as_of_date: str, key: str) -> Path:
        return self.bootstrap_state_root() / as_of_date / f"{safe_key(key)}.json.gz"

    def instrument_dir(self, symbol: str) -> Path:
        return self.instruments_root / safe_key(symbol)

    def model_state_path(self, symbol: str) -> Path:
        return self.instrument_dir(symbol) / "model_state.json"

    def ledger_path(self, symbol: str) -> Path:
        return self.instrument_dir(symbol) / "ledger.jsonl"

    def _fallback_position_signal_id(
        self,
        *,
        meta: InstrumentMeta,
        symbol: str,
        position: dict[str, Any],
        side: str,
    ) -> str:
        signal_epoch = int(as_float(position.get("signal_epoch")) or as_float(position.get("entry_epoch")) or 0)
        return canonical_signal_id(
            strategy_id=self.strategy_id,
            instrument_id=symbol,
            side=side,
            module=str(position.get("source") or "unknown_entry"),
            signal_epoch=signal_epoch,
            signal_source=meta.signal_source,
            signal_instrument_key=meta.signal_key,
            execution_instrument_key=str(position.get("instrument_key") or meta.execution_key),
        )

    def resolve_position_identity(
        self,
        *,
        symbol: str,
        meta: InstrumentMeta,
        position: dict[str, Any],
        side: str,
    ) -> tuple[str, str]:
        position_id = str(position.get("position_id") or "").strip()
        signal_id = str(position.get("signal_id") or "").strip()
        if position_id or signal_id:
            return position_id or signal_id, signal_id or position_id

        entry_epoch = int(as_float(position.get("entry_epoch")) or 0)
        entry_time = str(position.get("entry_time") or "")
        matched: dict[str, Any] | None = None
        for event in iter_jsonl(self.ledger_path(symbol)):
            if event.get("event") != "paper_entry":
                continue
            candidate = event.get("position") if isinstance(event.get("position"), dict) else {}
            candidate_epoch = int(as_float(candidate.get("entry_epoch") or event.get("entry_epoch")) or 0)
            candidate_time = str(candidate.get("entry_time") or event.get("entry_time") or "")
            candidate_side = str(candidate.get("side") or event.get("side") or "").lower()
            if candidate_side and candidate_side != side.lower():
                continue
            if entry_epoch and candidate_epoch == entry_epoch:
                matched = {**event, "position": candidate}
            elif entry_time and candidate_time == entry_time:
                matched = {**event, "position": candidate}
        if matched:
            candidate = matched.get("position") if isinstance(matched.get("position"), dict) else {}
            position_id = str(candidate.get("position_id") or matched.get("position_id") or "").strip()
            signal_id = str(candidate.get("signal_id") or matched.get("signal_id") or "").strip()
            if position_id or signal_id:
                return position_id or signal_id, signal_id or position_id

        signal_id = self._fallback_position_signal_id(meta=meta, symbol=symbol, position=position, side=side)
        return f"{signal_id}:position", signal_id

    def rollover_position_identity(
        self,
        *,
        symbol: str,
        rollover_id: str,
        side: str,
        entry_epoch: int,
        from_position_id: str,
        to_meta: InstrumentMeta,
    ) -> tuple[str, str]:
        parts = [
            self.strategy_id,
            symbol,
            side,
            str(entry_epoch),
            rollover_id,
            from_position_id,
            to_meta.execution_key,
        ]
        digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
        signal_id = f"{self.strategy_id}:{symbol}:{side}:{entry_epoch}:roll:{digest}"
        return f"{signal_id}:position", signal_id

    def load_model_state(self, symbol: str) -> dict[str, Any]:
        payload = read_json(self.model_state_path(symbol), {})
        return dict(payload) if isinstance(payload, dict) else {}

    def save_model_state(self, symbol: str, payload: dict[str, Any]) -> None:
        atomic_write_json(self.model_state_path(symbol), payload)

    def meta_for_contract_index(self, meta: InstrumentMeta, index: int) -> InstrumentMeta:
        chain = list(meta.contract_chain or [])
        if not chain or index < 0 or index >= len(chain):
            return meta
        contract = dict(chain[index])
        label = str(contract.get("label") or meta.execution_contract_label)
        execution_key = str(contract.get("instrument_key") or meta.execution_key)
        shadow_index = index + 1 if index + 1 < len(chain) else None
        shadow_contract = dict(chain[shadow_index]) if shadow_index is not None else {}
        shadow_execution_key = str(shadow_contract.get("instrument_key") or "") or None
        if meta.signal_source == "cash" and meta.cash_key:
            signal_key = str(meta.cash_key)
            signal_label = f"{label}_cash_signal"
            shadow_signal_key = signal_key if shadow_execution_key else None
        else:
            signal_key = execution_key
            signal_label = label
            shadow_signal_key = shadow_execution_key
        target_keys = {
            *(str(item.get("instrument_key")) for item in chain if item.get("instrument_key")),
            signal_key,
            execution_key,
            meta.cash_key,
            shadow_execution_key,
            shadow_signal_key,
        }
        return replace(
            meta,
            signal_key=signal_key,
            execution_key=execution_key,
            signal_contract_label=signal_label,
            execution_contract_label=label,
            lifecycle_start_date=contract_lifecycle_start(chain, index, self.holiday_dates),
            expiry_date=str(contract.get("expiry_date")) if contract.get("expiry_date") else None,
            current_contract_index=index,
            shadow_execution_key=shadow_execution_key,
            shadow_signal_key=shadow_signal_key,
            target_keys=[str(key) for key in target_keys if key],
        )

    @staticmethod
    def _raw_rows_for_lifecycle(
        state: OnlineObvState,
        lifecycle_start_date: str | None,
        *,
        through_epoch: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = [dict(row) for row in state.second_rows if isinstance(row, dict)]
        if isinstance(state.current_snapshot, dict):
            rows.append(dict(state.current_snapshot))
        if lifecycle_start_date:
            rows = [row for row in rows if str(row.get("trade_date") or "") >= str(lifecycle_start_date)]
        if through_epoch is not None:
            rows = [row for row in rows if int(row.get("epoch_second") or 0) <= int(through_epoch)]
        rows.sort(key=lambda item: (int(item.get("epoch_second") or 0), str(item.get("trade_date") or "")))
        return rows

    def reset_online_state_to_lifecycle(
        self,
        key: str,
        lifecycle_start_date: str | None,
        *,
        retention_seconds: int | None = None,
    ) -> dict[str, Any]:
        state = self.states.get(key)
        if state is None:
            return {"key": key, "status": "state_missing"}
        rows = self._raw_rows_for_lifecycle(state, lifecycle_start_date)
        if not rows:
            return {
                "key": key,
                "status": "lifecycle_rows_missing",
                "lifecycle_start_date": lifecycle_start_date,
                "source_rows": len(state.second_rows),
            }
        next_state = OnlineObvState(
            key=key,
            clock_epochs=set(self.clock_epochs),
            second_row_retention_seconds=retention_seconds
            if retention_seconds is not None
            else state.second_row_retention_seconds,
            compute_non_clock_percentiles=state.compute_non_clock_percentiles,
        )
        for row in rows:
            next_state.process_row(row)
        next_state.flush_until_latest()
        self.states[key] = next_state
        return {
            "key": key,
            "status": "reset",
            "lifecycle_start_date": lifecycle_start_date,
            "rows_used": len(rows),
            "last_finalized_second": next_state.last_finalized_second,
        }

    @staticmethod
    def _wider_retention(current: int | None, candidate: int | None) -> int | None:
        if current is None or candidate is None:
            return None
        return max(int(current), int(candidate))

    def memory_pressure_retention_report(self) -> dict[str, Any]:
        rss_mb = current_process_rss_mb()
        soft_limit = self.memory_retention_soft_limit_mb
        active = rss_mb is not None and soft_limit is not None and float(rss_mb) >= float(soft_limit)
        return {
            "rss_mb": rss_mb,
            "soft_limit_mb": soft_limit,
            "active": bool(active),
            "shadow_lifecycle_configured_seconds": self.shadow_lifecycle_second_row_retention_seconds,
            "shadow_lifecycle_pressure_cap_seconds": self.memory_pressure_shadow_lifecycle_second_row_retention_seconds,
        }

    def effective_shadow_lifecycle_retention_seconds(self, memory_pressure: dict[str, Any]) -> int | None:
        configured = self.shadow_lifecycle_second_row_retention_seconds
        if not bool(memory_pressure.get("active")):
            return configured
        cap = self.memory_pressure_shadow_lifecycle_second_row_retention_seconds
        if cap is None:
            return configured
        if configured is None:
            return cap
        return min(int(configured), int(cap))

    def position_is_stale_rejected(self, position: dict[str, Any]) -> bool:
        if not isinstance(position, dict):
            return False
        if truthy(position.get("entry_stale")):
            return True
        reason = str(position.get("entry_stale_reason") or position.get("stale_entry_reason") or "")
        return reason in {"retained_window_late_fill", "stale_entry_rejected", "stale_live_entry"} or reason.startswith("stale_")

    def position_counts_as_active(self, position: dict[str, Any]) -> bool:
        if not isinstance(position, dict):
            return False
        if (
            bool(self.config.get("ignore_stale_open_positions_for_live_retention", True))
            and self.position_is_stale_rejected(position)
        ):
            return False
        return True

    def desired_retention_by_key(self) -> dict[str, int | None]:
        memory_pressure = self.memory_pressure_retention_report()
        shadow_lifecycle_retention = self.effective_shadow_lifecycle_retention_seconds(memory_pressure)
        retention = {key: self.flat_second_row_retention_seconds for key in self.targets}
        for symbol, meta in self.instruments.items():
            state = self.model_states.get(symbol)
            if not isinstance(state, dict):
                continue
            position = state.get("position")
            pending = state.get("pending_entry") or state.get("pending_entry_signal")
            pending_list = state.get("pending_entry_signals")
            has_pending_list = any(isinstance(item, dict) for item in pending_list) if isinstance(pending_list, list) else False
            last_closed = state.get("last_closed_trade")
            transition_watch = (
                isinstance(last_closed, dict)
                and last_closed.get("exit_reason") == "post_signal_hard_exhaustion"
            )

            if isinstance(position, dict) and self.position_counts_as_active(position):
                for key in {
                    str(position.get("instrument_key") or meta.execution_key),
                    str(position.get("signal_instrument_key") or meta.signal_key),
                    meta.execution_key,
                    meta.signal_key,
                }:
                    if key and key in retention:
                        retention[key] = self._wider_retention(retention[key], self.active_second_row_retention_seconds)
                continue

            if isinstance(pending, dict) or has_pending_list:
                for key in {meta.execution_key, meta.signal_key}:
                    if key and key in retention:
                        retention[key] = self._wider_retention(retention[key], self.pending_second_row_retention_seconds)

            if transition_watch:
                key = meta.signal_key
                if key and key in retention:
                    retention[key] = self._wider_retention(retention[key], self.transition_second_row_retention_seconds)
            chain = list(meta.contract_chain or [])
            shadow_index = meta.current_contract_index + 1 if meta.current_contract_index + 1 < len(chain) else None
            if shadow_index is not None:
                shadow_start = contract_lifecycle_start(chain, shadow_index, self.holiday_dates)
                if shadow_start and now_ist().date().isoformat() >= shadow_start:
                    for key in {meta.shadow_execution_key, meta.shadow_signal_key}:
                        if key and key in retention:
                            retention[key] = self._wider_retention(
                                retention[key],
                                shadow_lifecycle_retention,
                            )
        return retention

    def refresh_dynamic_retention(self) -> dict[str, Any]:
        memory_pressure = self.memory_pressure_retention_report()
        desired = self.desired_retention_by_key()
        changed = 0
        unlimited = 0
        trimmed = 0
        for key, state in self.states.items():
            next_retention = desired.get(key, self.flat_second_row_retention_seconds)
            if state.second_row_retention_seconds != next_retention:
                state.set_second_row_retention(next_retention)
                changed += 1
            elif next_retention is not None:
                before_rows = len(state.second_rows)
                state.trim_second_rows()
                if len(state.second_rows) < before_rows:
                    trimmed += before_rows - len(state.second_rows)
            if next_retention is None:
                unlimited += 1
        if changed or trimmed:
            release_unused_process_memory()
        self.latest_memory_pressure_report = memory_pressure
        return {
            "changed": changed,
            "trimmed_second_rows": trimmed,
            "unlimited_targets": unlimited,
            "flat_retention_seconds": self.flat_second_row_retention_seconds,
            "pending_retention_seconds": self.pending_second_row_retention_seconds,
            "active_retention_seconds": self.active_second_row_retention_seconds,
            "shadow_lifecycle_retention_seconds": self.shadow_lifecycle_second_row_retention_seconds,
            "effective_shadow_lifecycle_retention_seconds": self.effective_shadow_lifecycle_retention_seconds(memory_pressure),
            "transition_retention_seconds": self.transition_second_row_retention_seconds,
            "memory_pressure": memory_pressure,
        }

    def append_trade_state_events(self, trade_date: str, symbol: str, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        append_jsonl_many(self.ledger_path(symbol), events)
        append_jsonl_many(self.decision_events_path(trade_date), events)

    def bootstrap_load_keys(self, as_of_date: str) -> set[str]:
        load_flat_execution = bool(self.config.get("bootstrap_load_flat_execution_keys_enabled", False))
        load_shadow = bool(self.config.get("bootstrap_load_shadow_keys_enabled", False))
        load_shadow_on_lifecycle = bool(self.config.get("bootstrap_load_shadow_keys_on_lifecycle_start_enabled", True))
        today_text = now_ist().date().isoformat()
        keys: set[str] = set()
        for symbol, meta in self.instruments.items():
            if meta.signal_key:
                keys.add(meta.signal_key)
            if meta.signal_source != "cash" or load_flat_execution:
                keys.add(meta.execution_key)
            state = self.model_states.get(symbol)
            if isinstance(state, dict):
                position = state.get("position")
                pending = state.get("pending_entry") or state.get("pending_entry_signal")
                pending_list = state.get("pending_entry_signals")
                has_pending_list = any(isinstance(item, dict) for item in pending_list) if isinstance(pending_list, list) else False
                if isinstance(position, dict) or isinstance(pending, dict) or has_pending_list:
                    for key in {
                        meta.execution_key,
                        meta.signal_key,
                        str((position or {}).get("instrument_key") or "") if isinstance(position, dict) else "",
                        str((position or {}).get("signal_instrument_key") or "") if isinstance(position, dict) else "",
                    }:
                        if key:
                            keys.add(key)
            chain = list(meta.contract_chain or [])
            shadow_index = meta.current_contract_index + 1 if meta.current_contract_index + 1 < len(chain) else None
            shadow_start = contract_lifecycle_start(chain, shadow_index, self.holiday_dates) if shadow_index is not None else None
            should_load_shadow = load_shadow or (
                load_shadow_on_lifecycle and shadow_start is not None and today_text >= str(shadow_start)
            )
            if should_load_shadow:
                for key in {meta.shadow_execution_key, meta.shadow_signal_key}:
                    if key:
                        keys.add(str(key))
        return {key for key in keys if key in self.states}

    def load_bootstrap_states(self) -> dict[str, Any]:
        if not bool(self.config.get("bootstrap_load_enabled", True)):
            return {"enabled": False, "reason": "bootstrap_load_disabled"}
        if self.partial_live_start and bool(self.config.get("skip_bootstrap_on_partial_live_start", True)):
            return {
                "enabled": True,
                "loaded": False,
                "reason": "partial_live_start_bootstrap_deferred",
                "decision_grade": False,
                "deferred_at_ist": now_ist().isoformat(),
            }
        requested_as_of_date = str(self.config.get("bootstrap_load_date") or "")
        manifest_path = self.bootstrap_manifest_path(requested_as_of_date) if requested_as_of_date else self.bootstrap_manifest_path()
        manifest = read_json(manifest_path, {})
        if not manifest:
            return {
                "enabled": True,
                "loaded": False,
                "reason": "bootstrap_manifest_missing",
                "manifest_path": str(manifest_path),
            }
        as_of_date = str(requested_as_of_date or manifest.get("as_of_date") or "")
        if not as_of_date:
            return {"enabled": True, "loaded": False, "reason": "bootstrap_as_of_date_missing"}
        load_keys = self.bootstrap_load_keys(as_of_date)
        deferred_keys = sorted(set(self.targets) - set(load_keys))
        atomic_write_json(
            self.status_path,
            {
                "ok": False,
                "status": "bootstrapping",
                "passive_only": True,
                "strategy_id": self.strategy_id,
                "model_version": self.config.get("model_version"),
                "architecture_version": self.config.get("architecture_version"),
                "symbols": len(self.instruments),
                "target_keys": len(self.targets),
                "bootstrap_load_keys": len(load_keys),
                "bootstrap_deferred_keys": len(deferred_keys),
                "bootstrap_as_of_date": as_of_date,
                "bootstrap_manifest_path": str(manifest_path),
                "updated_at_ist": now_ist().isoformat(),
            },
        )
        loaded = 0
        missing = 0
        pruned_targets = 0
        pruned_second_rows = 0
        errors: list[dict[str, Any]] = []
        today_text = now_ist().date().isoformat()
        raw_load_retention = self.config.get("bootstrap_load_second_row_retention_seconds")
        if raw_load_retention is None:
            raw_load_retention = self.config.get("bootstrap_second_row_retention_seconds")
        load_retention_seconds = retention_seconds_from_config(raw_load_retention)
        prune_prior_session_second_rows = bool(self.config.get("prune_prior_session_bootstrap_second_rows", True))
        prune_bootstrap_rows = (
            prune_prior_session_second_rows
            and bool(as_of_date)
            and as_of_date < today_text
            and load_retention_seconds is not None
        )
        for key in sorted(load_keys):
            path = self.bootstrap_state_file(as_of_date, key)
            if not path.exists():
                missing += 1
                continue
            payload = read_json_gz(path, {})
            if not payload:
                errors.append({"target": key, "reason": "empty_or_invalid_state"})
                continue
            payload_for_state = payload
            if prune_bootstrap_rows:
                raw_rows = payload.get("second_rows")
                if isinstance(raw_rows, list) and raw_rows:
                    if load_retention_seconds <= 0:
                        retained_rows: list[dict[str, Any]] = []
                    else:
                        anchor = int(as_float(payload.get("last_finalized_second")) or 0)
                        if anchor <= 0:
                            anchor = max(
                                (
                                    int(as_float(row.get("epoch_second")) or 0)
                                    for row in raw_rows
                                    if isinstance(row, dict)
                                ),
                                default=0,
                            )
                        cutoff = anchor - int(load_retention_seconds)
                        retained_rows = [
                            row
                            for row in raw_rows
                            if isinstance(row, dict) and int(as_float(row.get("epoch_second")) or 0) >= cutoff
                        ]
                    if len(retained_rows) != len(raw_rows):
                        payload_for_state = dict(payload)
                        payload_for_state["second_rows"] = retained_rows
                        payload_for_state["second_row_retention_seconds"] = load_retention_seconds
                        pruned_targets += 1
                        pruned_second_rows += len(raw_rows) - len(retained_rows)
            try:
                self.states[key] = OnlineObvState.from_payload(payload_for_state, self.clock_epochs)
            except Exception as exc:
                errors.append({"target": key, "reason": f"{type(exc).__name__}: {exc}"})
                continue
            if payload_for_state is not payload:
                payload.clear()
            state = self.states[key]
            state.second_row_retention_seconds = self.second_row_retention_seconds
            state.compute_non_clock_percentiles = self.compute_non_clock_percentiles
            state.clock_epochs.update(self.clock_epochs)
            if state.latest_received_epoch is not None:
                self.latest_feed_epoch = max(self.latest_feed_epoch or state.latest_received_epoch, state.latest_received_epoch)
            loaded += 1
        if pruned_second_rows:
            release_unused_process_memory()
        return {
            "enabled": True,
            "loaded": loaded > 0,
            "as_of_date": as_of_date,
            "targets_loaded": loaded,
            "targets_missing": missing,
            "targets_deferred": len(deferred_keys),
            "deferred_samples": deferred_keys[:10],
            "pruned_prior_session_second_rows": pruned_second_rows,
            "pruned_prior_session_targets": pruned_targets,
            "prune_prior_session_second_rows": prune_bootstrap_rows,
            "bootstrap_load_second_row_retention_seconds": load_retention_seconds,
            "error_count": len(errors),
            "error_samples": errors[:10],
            "manifest": manifest,
        }

    def save_bootstrap_states(
        self,
        as_of_date: str,
        source_reports: list[dict[str, Any]],
        *,
        promote_latest: bool = True,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        saved = 0
        missing = 0
        for key, state in self.states.items():
            if state.last_finalized_second is None:
                missing += 1
                continue
            atomic_write_json_gz(self.bootstrap_state_file(as_of_date, key), state.to_payload())
            saved += 1
        manifest = {
            "schema": "obvfutport_v2.bootstrap_manifest.v1",
            "strategy_id": self.strategy_id,
            "model_version": self.config.get("model_version"),
            "architecture_version": self.config.get("architecture_version"),
            "as_of_date": as_of_date,
            "created_at_ist": now_ist().isoformat(),
            "symbols": len(self.instruments),
            "target_keys": len(self.targets),
            "targets_saved": saved,
            "targets_missing": missing,
            "source_reports": source_reports,
        }
        if extra:
            manifest.update(extra)
        atomic_write_json(self.bootstrap_manifest_path(as_of_date), manifest)
        if promote_latest:
            atomic_write_json(self.bootstrap_manifest_path(), manifest)
        return manifest

    def add_clock_epochs_for_trade_date(self, trade_date: str) -> None:
        day = date.fromisoformat(trade_date)
        epochs = set(
            clock_epochs_for_day(
                day,
                clock_start=self.clock_start,
                clock_end=self.clock_end,
                clock_step_minutes=self.clock_step_minutes,
            )
        )
        self.clock_epochs.update(epochs)
        for state in self.states.values():
            state.clock_epochs.update(epochs)

    def is_market_hours_now(self) -> bool:
        current = now_ist().time()
        start = current.replace(hour=self.market_start[0], minute=self.market_start[1], second=self.market_start[2], microsecond=0)
        end = current.replace(hour=self.market_end[0], minute=self.market_end[1], second=self.market_end[2], microsecond=0)
        return start <= current <= end

    def is_market_hours_epoch(self, epoch: int | float | None) -> bool:
        if epoch is None:
            return self.is_market_hours_now()
        current = datetime.fromtimestamp(float(epoch), tz=IST).time()
        start = current.replace(hour=self.market_start[0], minute=self.market_start[1], second=self.market_start[2], microsecond=0)
        end = current.replace(hour=self.market_end[0], minute=self.market_end[1], second=self.market_end[2], microsecond=0)
        return start <= current <= end

    def is_after_first_model_clock_now(self) -> bool:
        current = now_ist().time()
        first_hh, first_mm = parse_hhmm(self.clock_start)
        first_clock = current.replace(hour=first_hh, minute=first_mm, second=0, microsecond=0)
        return current >= first_clock

    def should_defer_bootstrap_for_partial_live_start(self) -> bool:
        if not bool(self.config.get("skip_bootstrap_on_partial_live_start", True)):
            return False
        if not self.consume_target_stream:
            return False
        if not self.is_market_hours_now() or not self.is_after_first_model_clock_now():
            return False
        trade_date = now_ist().date().isoformat()
        path = self.target_stream_path(trade_date)
        return path.exists() and path.stat().st_size > 0

    def start_at_live_eof_pointer(self, trade_date: str, reason: str) -> dict[str, Any] | None:
        if not bool(self.config.get("start_at_eof_on_market_restart", True)):
            return None
        if not self.is_market_hours_now():
            return None
        path = self.live_batches_path(trade_date)
        if not path.exists():
            return None
        size = path.stat().st_size
        backfill = max(0, int(self.config.get("market_restart_backfill_bytes") or 0))
        return {
            "trade_date": trade_date,
            "offset": max(0, size - backfill),
            "file_size": size,
            "live_batches_path": str(path),
            "updated_at_ist": now_ist().isoformat(),
            "runner_session_id": self.session_id,
            "reset_reason": reason,
            "live_safe_start": True,
            "market_restart_backfill_bytes": backfill,
        }

    def read_pointer(self, trade_date: str) -> dict[str, Any]:
        payload = read_json(self.pointer_path, {})
        if payload.get("trade_date") != trade_date:
            live_eof = self.start_at_live_eof_pointer(trade_date, "new_trade_date_market_live_eof")
            return live_eof if live_eof is not None else {"trade_date": trade_date, "offset": 0}
        if payload.get("runner_session_id") != self.session_id:
            live_eof = self.start_at_live_eof_pointer(trade_date, "new_runner_session_market_live_eof")
            if live_eof is not None:
                return live_eof
        return dict(payload)

    def write_pointer(self, payload: dict[str, Any]) -> None:
        atomic_write_json(self.pointer_path, payload)

    def read_target_stream_pointer(self, trade_date: str) -> dict[str, Any]:
        payload = read_json(self.target_stream_pointer_path, {})
        if payload.get("trade_date") != trade_date:
            if (
                bool(self.config.get("start_at_eof_on_market_restart", True))
                and self.is_market_hours_now()
                and self.is_after_first_model_clock_now()
            ):
                path = self.target_stream_path(trade_date)
                if path.exists():
                    backfill = max(0, int(self.config.get("target_stream_restart_backfill_bytes") or 0))
                    size = path.stat().st_size
                    return {
                        "trade_date": trade_date,
                        "offset": max(0, size - backfill),
                        "file_size": size,
                        "target_stream_path": str(path),
                        "updated_at_ist": now_ist().isoformat(),
                        "runner_session_id": self.session_id,
                        "reset_reason": "new_trade_date_target_stream_market_live_backfill",
                        "live_safe_start": True,
                        "partial_live_start": True,
                        "target_stream_restart_backfill_bytes": backfill,
                    }
            return {"trade_date": trade_date, "offset": 0, "reset_reason": "new_trade_date"}
        if payload.get("runner_session_id") != self.session_id:
            backfill = max(0, int(self.config.get("target_stream_restart_backfill_bytes") or 0))
            path = self.target_stream_path(trade_date)
            if path.exists() and backfill > 0 and self.is_after_first_model_clock_now():
                size = path.stat().st_size
                return {
                    "trade_date": trade_date,
                    "offset": max(0, size - backfill),
                    "file_size": size,
                    "target_stream_path": str(path),
                    "updated_at_ist": now_ist().isoformat(),
                    "runner_session_id": self.session_id,
                    "reset_reason": "new_runner_session_target_stream_backfill",
                    "live_safe_start": True,
                    "partial_live_start": True,
                    "target_stream_restart_backfill_bytes": backfill,
                }
        return dict(payload)

    def write_target_stream_pointer(self, payload: dict[str, Any]) -> None:
        atomic_write_json(self.target_stream_pointer_path, payload)

    def live_entry_lag_guard_seconds(self) -> int | None:
        if not bool(self.config.get("live_stale_entry_guard_enabled", True)):
            return None
        if "max_entry_lag_seconds" in self.config:
            value = self.config.get("max_entry_lag_seconds")
        elif "max_entry_execution_lag_seconds" in self.config:
            value = self.config.get("max_entry_execution_lag_seconds")
        else:
            value = 5
        if value is None:
            return None
        return max(0, int(value))

    def live_entry_fill_acceptance_seconds(self) -> int | None:
        if not bool(self.config.get("live_reject_retained_late_entry_fills_enabled", True)):
            return None
        value = self.config.get("max_live_entry_fill_lag_seconds")
        if value is None:
            value = self.config.get("max_retained_entry_fill_lag_seconds")
        if value is None:
            value = 120
        try:
            parsed = int(float(value))
        except (TypeError, ValueError):
            return None
        return max(0, parsed)

    def _entry_due_epoch(self, edge: dict[str, Any], entry_delay_seconds: int) -> int | None:
        due_epoch = as_float(edge.get("entry_due_epoch"))
        if due_epoch is not None:
            return int(due_epoch)
        signal_epoch = as_float(edge.get("signal_epoch"))
        if signal_epoch is None:
            return None
        return int(signal_epoch) + int(entry_delay_seconds)

    def filter_live_entry_edges_for_retained_window(
        self,
        signal_contract_state: dict[str, Any],
        *,
        evaluation_epoch: int,
        entry_delay_seconds: int,
    ) -> tuple[dict[str, Any], int]:
        max_lag = self.live_entry_fill_acceptance_seconds()
        if max_lag is None or not self.is_market_hours_epoch(evaluation_epoch):
            return signal_contract_state, 0
        edges = signal_contract_state.get("entry_edges_today")
        if not isinstance(edges, list) or not edges:
            return signal_contract_state, 0
        kept: list[dict[str, Any]] = []
        filtered = 0
        for edge in edges:
            if not isinstance(edge, dict):
                kept.append(edge)
                continue
            due_epoch = self._entry_due_epoch(edge, entry_delay_seconds)
            if due_epoch is not None and int(evaluation_epoch) - int(due_epoch) > max_lag:
                filtered += 1
                continue
            kept.append(edge)
        if filtered <= 0:
            return signal_contract_state, 0
        updated = dict(signal_contract_state)
        updated["entry_edges_today"] = kept
        return updated, filtered

    def reject_stale_pending_entries_before_fill(
        self,
        *,
        meta: InstrumentMeta,
        model_state: dict[str, Any],
        trade_date: str,
        evaluation_epoch: int,
        entry_delay_seconds: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        max_lag = self.live_entry_fill_acceptance_seconds()
        if max_lag is None or not self.is_market_hours_epoch(evaluation_epoch):
            return model_state, []
        if not any(key in model_state for key in ("pending_entry", "pending_entry_signal", "pending_entry_signals")):
            return model_state, []

        updated = dict(model_state or {})
        rejected: list[dict[str, Any]] = []

        def should_reject(entry: dict[str, Any]) -> tuple[bool, int | None, int]:
            due_epoch = self._entry_due_epoch(entry, entry_delay_seconds)
            if due_epoch is None:
                return False, None, 0
            staleness = int(evaluation_epoch) - int(due_epoch)
            return staleness > int(max_lag), int(due_epoch), staleness

        def rejection_event(entry: dict[str, Any], due_epoch: int | None, staleness: int) -> dict[str, Any]:
            signal_epoch = int(as_float(entry.get("signal_epoch")) or 0)
            return {
                "event": "entry_signal_skipped",
                "reason": "stale_pending_entry_at_evaluation",
                "decision_only": True,
                "suppress_downstream": True,
                "entry_stale": True,
                "entry_stale_reason": "stale_pending_entry_at_evaluation",
                "entry_staleness_seconds": int(staleness),
                "symbol": meta.symbol,
                "signal_id": entry.get("signal_id"),
                "position_id": entry.get("position_id"),
                "side": entry.get("side"),
                "source": entry.get("source"),
                "module": entry.get("module"),
                "signal_source": meta.signal_source,
                "signal_instrument_key": meta.signal_key,
                "execution_instrument_key": meta.execution_key,
                "signal_contract_label": meta.signal_contract_label,
                "execution_contract_label": meta.execution_contract_label,
                "lifecycle_start_date": meta.lifecycle_start_date,
                "signal_epoch": signal_epoch,
                "signal_time": entry.get("signal_time") or epoch_ist_iso(signal_epoch),
                "entry_due_epoch": due_epoch,
                "entry_due_time": epoch_ist_iso(int(due_epoch)) if due_epoch is not None else None,
                "evaluation_epoch": int(evaluation_epoch),
                "evaluation_time": epoch_ist_iso(int(evaluation_epoch)),
                "max_live_entry_fill_lag_seconds": int(max_lag),
                "debug_policy": "stale_pending_entry_rejected_before_live_materialization",
                "created_at_ist": now_ist().isoformat(),
            }

        for key in ("pending_entry", "pending_entry_signal"):
            entry = updated.get(key)
            if not isinstance(entry, dict):
                continue
            stale, due_epoch, staleness = should_reject(entry)
            if not stale:
                continue
            rejected.append(rejection_event(entry, due_epoch, staleness))
            updated.pop(key, None)

        pending_list = updated.get("pending_entry_signals")
        if isinstance(pending_list, list) and pending_list:
            kept: list[Any] = []
            for entry in pending_list:
                if not isinstance(entry, dict):
                    kept.append(entry)
                    continue
                stale, due_epoch, staleness = should_reject(entry)
                if stale:
                    rejected.append(rejection_event(entry, due_epoch, staleness))
                else:
                    kept.append(entry)
            if kept:
                updated["pending_entry_signals"] = kept
            else:
                updated.pop("pending_entry_signals", None)

        if rejected:
            latest_signal_epoch = max(
                [int(as_float(event.get("signal_epoch")) or 0) for event in rejected]
                + [int(updated.get("last_signal_epoch") or 0)]
            )
            updated["last_signal_epoch"] = latest_signal_epoch
            updated["trade_date"] = trade_date
            updated["signal_source"] = meta.signal_source
            updated["updated_at_ist"] = now_ist().isoformat()
        return updated, rejected

    def reject_retained_late_entry_fill(
        self,
        *,
        meta: InstrumentMeta,
        previous_state: dict[str, Any],
        model_state: dict[str, Any],
        events: list[dict[str, Any]],
        trade_date: str,
        evaluation_epoch: int,
        entry_delay_seconds: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        max_lag = self.live_entry_fill_acceptance_seconds()
        if max_lag is None or not self.is_market_hours_epoch(evaluation_epoch):
            return model_state, events
        for event in events:
            if not isinstance(event, dict) or event.get("event") != "paper_entry":
                continue
            position = event.get("position") if isinstance(event.get("position"), dict) else {}
            due_epoch = self._entry_due_epoch(position, entry_delay_seconds)
            entry_epoch = as_float(position.get("entry_epoch"))
            position_marked_stale = self.position_is_stale_rejected(position) or truthy(event.get("entry_stale"))
            if due_epoch is None or entry_epoch is None:
                if not position_marked_stale:
                    continue
                fill_delay_seconds = int(
                    as_float(position.get("entry_staleness_seconds"))
                    or as_float(event.get("entry_staleness_seconds"))
                    or 0
                )
            else:
                fill_delay_seconds = int(entry_epoch) - int(due_epoch)
                if position_marked_stale:
                    fill_delay_seconds = max(
                        fill_delay_seconds,
                        int(
                            as_float(position.get("entry_staleness_seconds"))
                            or as_float(event.get("entry_staleness_seconds"))
                            or 0
                        ),
                    )
            if not position_marked_stale and fill_delay_seconds <= int(max_lag):
                continue
            signal_epoch = int(as_float(position.get("signal_epoch")) or 0)
            repaired_state = dict(previous_state or {})
            repaired_state["trade_date"] = trade_date
            repaired_state["signal_source"] = meta.signal_source
            repaired_state["last_signal_epoch"] = max(int(repaired_state.get("last_signal_epoch") or 0), signal_epoch)
            repaired_state.pop("pending_entry", None)
            repaired_state.pop("pending_entry_signal", None)
            signal_id = str(position.get("signal_id") or event.get("signal_id") or "")
            pending_list = repaired_state.get("pending_entry_signals")
            if isinstance(pending_list, list):
                repaired_state["pending_entry_signals"] = [
                    item
                    for item in pending_list
                    if not (isinstance(item, dict) and str(item.get("signal_id") or "") == signal_id)
                ]
            rejected_event = {
                "event": "stale_entry_rejected",
                "reason": str(position.get("entry_stale_reason") or event.get("entry_stale_reason") or "retained_window_late_fill"),
                "entry_stale": True,
                "entry_stale_reason": str(
                    position.get("entry_stale_reason") or event.get("entry_stale_reason") or "retained_window_late_fill"
                ),
                "entry_staleness_seconds": fill_delay_seconds,
                "symbol": meta.symbol,
                "signal_id": signal_id,
                "position_id": position.get("position_id") or event.get("position_id"),
                "side": position.get("side"),
                "source": position.get("source"),
                "signal_source": meta.signal_source,
                "signal_instrument_key": meta.signal_key,
                "execution_instrument_key": meta.execution_key,
                "signal_contract_label": meta.signal_contract_label,
                "execution_contract_label": meta.execution_contract_label,
                "lifecycle_start_date": meta.lifecycle_start_date,
                "signal_epoch": signal_epoch,
                "signal_time": position.get("signal_time"),
                "entry_due_epoch": due_epoch,
                "entry_due_time": epoch_ist_iso(int(due_epoch)) if due_epoch is not None else None,
                "candidate_entry_epoch": int(entry_epoch) if entry_epoch is not None else None,
                "candidate_entry_time": epoch_ist_iso(int(entry_epoch)) if entry_epoch is not None else None,
                "fill_delay_seconds": fill_delay_seconds,
                "max_live_entry_fill_lag_seconds": int(max_lag),
                "evaluation_epoch": int(evaluation_epoch),
                "evaluation_time": epoch_ist_iso(int(evaluation_epoch)),
                "debug_policy": "rejected_for_live_positioning_preserved_for_eod_forensics",
                "created_at_ist": now_ist().isoformat(),
            }
            return repaired_state, [rejected_event]
        return model_state, events

    def source_quote_age_at_clock(
        self,
        row: dict[str, Any],
        state: OnlineObvState,
        clock_epoch: int,
    ) -> float | None:
        source_epoch = as_float(row.get("source_quote_epoch"))
        if source_epoch is None:
            received_epoch = as_float(row.get("source_received_epoch")) or as_float(row.get("received_epoch"))
            if received_epoch is not None and received_epoch <= float(clock_epoch):
                source_epoch = received_epoch
        if source_epoch is None:
            latest_epoch = as_float(state.latest_quote_epoch)
            if latest_epoch is not None and latest_epoch <= float(clock_epoch):
                source_epoch = latest_epoch
        if source_epoch is None:
            return None
        return float(clock_epoch) - float(source_epoch)

    def _queue_controlled_repair(
        self,
        *,
        symbol: str,
        trade_date: str,
        mode: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        append_jsonl(
            self.controlled_repair_queue_path,
            {
                "event": "controlled_repair_queued",
                "symbol": symbol,
                "trade_date": trade_date,
                "mode": mode,
                "reason": reason,
                "details": details or {},
                "recorded_at_ist": now_ist().isoformat(),
            },
        )

    def signal_clock_readiness_item(
        self,
        meta: InstrumentMeta,
        *,
        trade_date: str,
        clock_epoch: int,
        clock_label: str,
    ) -> dict[str, Any]:
        signal_state = self.states.get(meta.signal_key)
        if signal_state is None:
            return {
                "symbol": meta.symbol,
                "ready": False,
                "role": "signal",
                "signal_source": meta.signal_source,
                "instrument_key": meta.signal_key,
                "required_clock_label": clock_label,
                "reason": "signal_state_missing",
            }
        row, reason = signal_state.build_clock_row(clock_epoch, clock_label, meta.signal_point_config)
        if row is None:
            return {
                "symbol": meta.symbol,
                "ready": False,
                "role": "signal",
                "signal_source": meta.signal_source,
                "instrument_key": meta.signal_key,
                "required_clock_label": clock_label,
                "reason": (reason or {}).get("reason") or "missing_clock_metric",
                "details": reason or {},
            }
        quote_age = self.source_quote_age_at_clock(row, signal_state, clock_epoch)
        if quote_age is None or quote_age > self.signal_quote_max_age:
            return {
                "symbol": meta.symbol,
                "ready": False,
                "role": "signal",
                "signal_source": meta.signal_source,
                "instrument_key": meta.signal_key,
                "required_clock_label": clock_label,
                "reason": "signal_quote_stale",
                "quote_age_seconds": quote_age,
                "max_quote_age_seconds": self.signal_quote_max_age,
                "source_quote_epoch": row.get("source_quote_epoch"),
                "source_received_epoch": row.get("source_received_epoch"),
            }
        return {
            "symbol": meta.symbol,
            "ready": True,
            "role": "signal",
            "signal_source": meta.signal_source,
            "instrument_key": meta.signal_key,
            "required_clock_label": clock_label,
            "quote_age_seconds": quote_age,
            "last_finalized_second": signal_state.last_finalized_second,
        }

    def wait_for_signal_clock_readiness(
        self,
        metas: list[InstrumentMeta],
        *,
        trade_date: str,
        clock_epoch: int,
    ) -> dict[str, Any]:
        clock_label = self.clock_label(clock_epoch)
        if not bool(self.config.get("signal_clock_readiness_barrier_enabled", True)) or not metas:
            return {
                "enabled": False,
                "ready": True,
                "ready_symbols": [meta.symbol for meta in metas],
                "missing": [],
                "wait_seconds": 0.0,
            }
        started = time.perf_counter()
        cutoff_seconds = int(self.config.get("signal_clock_readiness_cutoff_seconds") or 50)
        poll_seconds = float(self.config.get("signal_clock_readiness_poll_seconds") or 1.0)
        max_wait_seconds = as_float(self.config.get("signal_clock_readiness_max_wait_seconds"))
        if max_wait_seconds is None:
            max_wait_seconds = max(0.0, float(cutoff_seconds))
        deadline_epoch = int(clock_epoch) + cutoff_seconds
        ready_first_enabled = bool(self.config.get("signal_clock_readiness_ready_first_enabled", True))
        ready_first_grace = as_float(self.config.get("signal_clock_readiness_ready_first_grace_seconds"))
        ready_first_grace = 5.0 if ready_first_grace is None else max(0.0, float(ready_first_grace))
        attempts = 0
        last_items: list[dict[str, Any]] = []
        while True:
            attempts += 1
            last_items = [
                self.signal_clock_readiness_item(
                    meta,
                    trade_date=trade_date,
                    clock_epoch=clock_epoch,
                    clock_label=clock_label,
                )
                for meta in metas
            ]
            ready_items = [item for item in last_items if item.get("ready")]
            missing = [item for item in last_items if not item.get("ready")]
            if not missing:
                return {
                    "enabled": True,
                    "ready": True,
                    "attempts": attempts,
                    "wait_seconds": round(time.perf_counter() - started, 4),
                    "clock_epoch": clock_epoch,
                    "clock_time": epoch_ist_iso(clock_epoch),
                    "clock_label": clock_label,
                    "ready_count": len(ready_items),
                    "missing_count": 0,
                    "ready_symbols": [str(item.get("symbol")) for item in ready_items if item.get("symbol")],
                    "missing": [],
                }
            elapsed = time.perf_counter() - started
            now_epoch = int(time.time())
            remaining_to_cutoff = max(0.0, float(deadline_epoch - now_epoch))
            remaining_to_max = max(0.0, float(max_wait_seconds) - float(elapsed))
            remaining = min(remaining_to_cutoff, remaining_to_max)
            if ready_first_enabled and ready_items and elapsed >= ready_first_grace:
                return {
                    "enabled": True,
                    "ready": False,
                    "ready_first_released": True,
                    "attempts": attempts,
                    "wait_seconds": round(elapsed, 4),
                    "clock_epoch": clock_epoch,
                    "clock_time": epoch_ist_iso(clock_epoch),
                    "clock_label": clock_label,
                    "cutoff_seconds": cutoff_seconds,
                    "ready_count": len(ready_items),
                    "missing_count": len(missing),
                    "ready_symbols": [str(item.get("symbol")) for item in ready_items if item.get("symbol")],
                    "missing": json_clean(missing),
                }
            if remaining <= 0:
                return {
                    "enabled": True,
                    "ready": False,
                    "cutoff_reached": now_epoch >= deadline_epoch,
                    "attempts": attempts,
                    "wait_seconds": round(elapsed, 4),
                    "clock_epoch": clock_epoch,
                    "clock_time": epoch_ist_iso(clock_epoch),
                    "clock_label": clock_label,
                    "cutoff_seconds": cutoff_seconds,
                    "ready_count": len(ready_items),
                    "missing_count": len(missing),
                    "ready_symbols": [str(item.get("symbol")) for item in ready_items if item.get("symbol")],
                    "missing": json_clean(missing),
                }
            self.latest_tail_report = self.tail_target_stream(trade_date) if self.consume_target_stream else self.tail_live_batches(trade_date)
            time.sleep(min(float(poll_seconds), remaining))

    @staticmethod
    def json_object_end(text: str, start: int) -> int:
        return json_object_end(text, start)

    def target_items_from_batch_line(self, line: str) -> list[dict[str, Any]]:
        return target_items_from_batch_line(line, self.target_set)

    def tail_live_batches(self, trade_date: str) -> dict[str, Any]:
        started = time.perf_counter()
        path = self.live_batches_path(trade_date)
        pointer = self.read_pointer(trade_date)
        offset = int(pointer.get("offset") or 0)
        if not path.exists():
            return {"exists": False, "path": str(path), "rows": 0, "quotes": 0, "duration_seconds": round(time.perf_counter() - started, 4)}
        size = path.stat().st_size
        if offset > size:
            offset = 0
        rows = 0
        quotes = 0
        new_offset = offset
        truncated = False
        trade_date_text = trade_date
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(offset)
            while True:
                pos = handle.tell()
                if self.stop_requested:
                    new_offset = pos
                    truncated = True
                    break
                if self.run_deadline_epoch is not None and time.time() >= self.run_deadline_epoch:
                    new_offset = pos
                    truncated = True
                    break
                if pos > offset and pos - offset >= self.tail_max_bytes:
                    new_offset = pos
                    truncated = True
                    break
                line = handle.readline()
                if not line:
                    new_offset = handle.tell()
                    break
                if not line.endswith("\n"):
                    new_offset = pos
                    break
                rows += 1
                for item in self.target_items_from_batch_line(line):
                    key = str(item["instrument_key"])
                    row = normalise_record(item, trade_date_text, key)
                    if row is None:
                        continue
                    self.states[key].process_row(row)
                    received_epoch = as_float(row.get("received_epoch")) or as_float(row.get("epoch"))
                    if received_epoch is not None:
                        self.latest_feed_epoch = max(self.latest_feed_epoch or received_epoch, received_epoch)
                    quotes += 1
        self.rows_seen += rows
        self.quotes_seen += quotes
        self.write_pointer(
            {
                "trade_date": trade_date,
                "offset": new_offset,
                "file_size": size,
                "live_batches_path": str(path),
                "updated_at_ist": now_ist().isoformat(),
                "runner_session_id": self.session_id,
            }
        )
        return {
            "exists": True,
            "path": str(path),
            "offset": offset,
            "new_offset": new_offset,
            "file_size": size,
            "rows": rows,
            "quotes": quotes,
            "bytes_read": max(0, new_offset - offset),
            "truncated": truncated,
            "duration_seconds": round(time.perf_counter() - started, 4),
        }

    def tail_target_stream(self, trade_date: str) -> dict[str, Any]:
        started = time.perf_counter()
        path = self.target_stream_path(trade_date)
        pointer = self.read_target_stream_pointer(trade_date)
        if pointer.get("partial_live_start"):
            self.partial_live_start = True
            self.partial_live_start_trade_date = trade_date
        offset = int(pointer.get("offset") or 0)
        if not path.exists():
            return {
                "source": "target_stream",
                "exists": False,
                "path": str(path),
                "rows": 0,
                "quotes": 0,
                "duration_seconds": round(time.perf_counter() - started, 4),
            }
        size = path.stat().st_size
        if offset > size:
            offset = 0
        rows = 0
        quotes = 0
        new_offset = offset
        truncated = False
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(offset)
            while True:
                pos = handle.tell()
                if self.stop_requested:
                    new_offset = pos
                    truncated = True
                    break
                if self.run_deadline_epoch is not None and time.time() >= self.run_deadline_epoch:
                    new_offset = pos
                    truncated = True
                    break
                if pos > offset and pos - offset >= self.target_stream_tail_max_bytes:
                    new_offset = pos
                    truncated = True
                    break
                line = handle.readline()
                if not line:
                    new_offset = handle.tell()
                    break
                if not line.endswith("\n"):
                    new_offset = pos
                    break
                rows += 1
                try:
                    compact = json.loads(line)
                except json.JSONDecodeError:
                    continue
                row = row_from_target_stream_row(compact, trade_date, self.target_set)
                if row is None:
                    continue
                self.states[str(row["target"])].process_row(row)
                received_epoch = as_float(row.get("received_epoch")) or as_float(row.get("epoch"))
                if received_epoch is not None:
                    self.latest_feed_epoch = max(self.latest_feed_epoch or received_epoch, received_epoch)
                quotes += 1
        self.rows_seen += rows
        self.quotes_seen += quotes
        self.write_target_stream_pointer(
            {
                "trade_date": trade_date,
                "offset": new_offset,
                "file_size": size,
                "target_stream_path": str(path),
                "updated_at_ist": now_ist().isoformat(),
                "runner_session_id": self.session_id,
            }
        )
        return {
            "source": "target_stream",
            "exists": True,
            "path": str(path),
            "offset": offset,
            "new_offset": new_offset,
            "file_size": size,
            "rows": rows,
            "quotes": quotes,
            "bytes_read": max(0, new_offset - offset),
            "truncated": truncated,
            "duration_seconds": round(time.perf_counter() - started, 4),
        }

    def latest_due_clock_epoch(self, now_epoch: float) -> int | None:
        due = [epoch for epoch in self.clock_epochs if epoch + self.decision_delay_seconds <= now_epoch]
        return max(due) if due else None

    def clock_label(self, epoch: int) -> str:
        return datetime.fromtimestamp(epoch, IST).strftime("%H:%M")

    def decision_events_path(self, trade_date: str) -> Path:
        return self.decision_events_root / f"decision_events_{trade_date}.jsonl"

    def ensure_clock_rows_through(self, state: OnlineObvState, point_config: dict[str, Any], through_epoch: int | None = None) -> dict[str, Any]:
        built = 0
        skipped = 0
        existing = {int(row.get("epoch_second") or 0) for row in state.clock_rows if isinstance(row, dict)}
        for epoch in sorted(state.metric_by_clock_epoch):
            if through_epoch is not None and int(epoch) > int(through_epoch):
                continue
            if int(epoch) in existing:
                continue
            row, reason = state.build_clock_row(int(epoch), self.clock_label(int(epoch)), point_config)
            if row is None:
                skipped += 1
            else:
                built += 1
                existing.add(int(epoch))
        return {"built": built, "skipped": skipped, "clock_rows": len(state.clock_rows)}

    def v1_contract_state_from_online(
        self,
        *,
        state: OnlineObvState,
        today: str,
        point_config: dict[str, Any],
        through_epoch: int | None = None,
        lifecycle_start_date: str | None = None,
        recompute_from_lifecycle_rows: bool = False,
    ) -> dict[str, Any]:
        import pandas as pd  # type: ignore

        if recompute_from_lifecycle_rows:
            v1_obv_model = load_v1_obv_model_module(self.config)
            raw_rows = self._raw_rows_for_lifecycle(
                state,
                lifecycle_start_date,
                through_epoch=through_epoch,
            )
            raw_frame = v1_obv_model.build_appended_rows(raw_rows, pd.DataFrame())
            return v1_obv_model.build_contract_state(
                raw_frame,
                today=today,
                point_config=point_config,
            )
        self.ensure_clock_rows_through(state, point_config, through_epoch=through_epoch)

        second_rows_source = state.second_rows
        if through_epoch is not None and second_rows_source:
            cutoff = int(through_epoch)
            lo = 0
            hi = len(second_rows_source)
            while lo < hi:
                mid = (lo + hi) // 2
                try:
                    mid_epoch = int(second_rows_source[mid].get("epoch_second") or 0)
                except Exception:
                    mid_epoch = 0
                if mid_epoch <= cutoff:
                    lo = mid + 1
                else:
                    hi = mid
            second_rows_source = second_rows_source[:lo]
        seconds = pd.DataFrame(second_rows_source)
        if seconds.empty:
            seconds = pd.DataFrame(columns=SECOND_STATE_COLUMNS)
        if not seconds.empty:
            seconds = seconds.sort_values("epoch_second", kind="mergesort").reset_index(drop=True)
            if through_epoch is not None and "epoch_second" in seconds.columns:
                seconds = seconds[seconds["epoch_second"].astype(int) <= int(through_epoch)].copy()
        clock_rows_source = state.clock_rows
        if through_epoch is not None and clock_rows_source:
            cutoff = int(through_epoch)
            lo = 0
            hi = len(clock_rows_source)
            while lo < hi:
                mid = (lo + hi) // 2
                try:
                    mid_epoch = int(clock_rows_source[mid].get("epoch_second") or 0)
                except Exception:
                    mid_epoch = 0
                if mid_epoch <= cutoff:
                    lo = mid + 1
                else:
                    hi = mid
            clock_rows_source = clock_rows_source[:lo]
        clock_state = pd.DataFrame(clock_rows_source)
        if clock_state.empty:
            clock_state = pd.DataFrame(columns=CLOCK_STATE_COLUMNS)
        if not clock_state.empty:
            clock_state = clock_state.sort_values("epoch_second", kind="mergesort").reset_index(drop=True)
            if through_epoch is not None and "epoch_second" in clock_state.columns:
                clock_state = clock_state[clock_state["epoch_second"].astype(int) <= int(through_epoch)].copy()
        today_seconds = (
            seconds[seconds["trade_date"].astype(str) == today].copy()
            if not seconds.empty and "trade_date" in seconds.columns
            else seconds.copy()
        )
        today_clock = (
            clock_state[clock_state["trade_date"].astype(str) == today].copy()
            if not clock_state.empty and "trade_date" in clock_state.columns
            else clock_state.copy()
        )
        latest_tick = seconds.iloc[-1].to_dict() if not seconds.empty else {}
        latest_clock = clock_state.iloc[-1].to_dict() if not clock_state.empty else {}
        v1_obv_model = load_v1_obv_model_module(self.config)
        primary_short_threshold = as_float(point_config.get("primary_obv_short_abs_threshold")) or 1.5
        modules = [
            (f"primary_obv_short_abs{primary_short_threshold:g}", "primary_obv_short_configured_active_edge", "short"),
            ("fresh_trend_long", "fresh_trend_long_active_edge", "long"),
            ("fresh_trend_short", "fresh_trend_short_active_edge", "short"),
        ]
        entry_edges_today: list[dict[str, Any]] = []
        if not today_clock.empty:
            for module, column, side in modules:
                if column not in today_clock.columns:
                    continue
                rows = today_clock[today_clock[column] == True]  # noqa: E712
                for _, row in rows.iterrows():
                    entry_edges_today.append(v1_obv_model.entry_edge_from_clock_row(row, module=module, side=side))
        diagnostic_edges_today: list[dict[str, Any]] = []
        diagnostic_modules = [
            ("v53_long_warning", "v53_long_warning_edge", "long_warning"),
            ("v53_long_executable", "v53_long_executable_edge", "long_executable"),
            ("v53_short_early_warning", "v53_short_early_warning_edge", "short_warning"),
            ("v53_short_executable", "v53_short_executable_edge", "short_executable"),
        ]
        if not today_clock.empty:
            for module, column, diagnostic_type in diagnostic_modules:
                if column not in today_clock.columns:
                    continue
                rows = today_clock[today_clock[column] == True]  # noqa: E712
                for _, row in rows.iterrows():
                    diagnostic_edges_today.append(
                        {
                            "module": module,
                            "diagnostic_type": diagnostic_type,
                            "signal_epoch": int(row["epoch_second"]),
                            "signal_time": row.get("actual_time"),
                            "clock": row.get("clock_label"),
                            "signal_price": as_float(row.get("price")),
                            "z": as_float(row.get("obv_minus_price_prior_z")),
                            "prior_p90": as_float(row.get("prior_p90")),
                            "prior_p95": as_float(row.get("prior_p95")),
                            "prior_p10": as_float(row.get("prior_p10")),
                            "price_change_prior_pct": as_float(row.get("price_change_prior_pct")),
                            "long_trigger_price": as_float(row.get("long_trigger_price")),
                            "short_trigger_price": as_float(row.get("short_trigger_price")),
                        }
                    )
        return {
            "seconds": seconds,
            "clock_state": clock_state,
            "today_seconds": today_seconds,
            "today_clock": today_clock,
            "latest_tick": latest_tick,
            "latest_clock": latest_clock,
            "entry_edges_today": entry_edges_today,
            "diagnostic_edges_today": sorted(
                diagnostic_edges_today,
                key=lambda item: int(item.get("signal_epoch") or 0),
            ),
        }

    def current_clock_entry_edges_from_contract(
        self,
        *,
        meta: InstrumentMeta,
        state: OnlineObvState,
        trade_date: str,
        clock_epoch: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        contract_state = self.v1_contract_state_from_online(
            state=state,
            today=trade_date,
            point_config=meta.signal_point_config,
            through_epoch=clock_epoch,
        )
        edges: list[dict[str, Any]] = []
        for edge in contract_state.get("entry_edges_today") or []:
            if not isinstance(edge, dict):
                continue
            try:
                signal_epoch = int(edge.get("signal_epoch") or 0)
            except (TypeError, ValueError):
                signal_epoch = 0
            if signal_epoch != int(clock_epoch):
                continue
            edges.append({**edge, "signal_loop_match": "v1_contract_current_clock_edge"})
        return edges, {
            "entry_edges_today": len(contract_state.get("entry_edges_today") or []),
            "current_clock_entry_edges": len(edges),
        }

    def enrich_v1_event(self, meta: InstrumentMeta, event: dict[str, Any]) -> dict[str, Any]:
        out = dict(event)
        out.update(
            {
                "schema": "obvfutport_v2.frozen_v1_trade_event.v1",
                "passive_only": True,
                "strategy_id": self.strategy_id,
                "model_version": self.config.get("model_version"),
                "architecture_version": self.config.get("architecture_version"),
                "symbol": meta.symbol,
                "threshold_source": meta.source,
                "threshold_synthesized": meta.synthesized,
                "source_runtime": event.get("source_runtime") or "frozen_v1_trade_state_adapter",
                "recorded_at_ist": now_ist().isoformat(),
                **_adaptive_event_fields(meta),
            }
        )
        return out

    def _active_position_rows(
        self,
        *,
        position: dict[str, Any],
        second_rows: list[dict[str, Any]],
        cutoff_epoch: int,
    ) -> list[dict[str, Any]]:
        entry_epoch = int(position.get("entry_epoch") or 0)
        latest_epoch = int(position.get("latest_epoch") or 0)
        carry_mark_epoch = 0
        if str(position.get("status") or "").lower() == "open" and str(position.get("exit_reason") or "") == "open_mark_if_closed":
            carry_mark_epoch = int(position.get("exit_epoch") or 0)
        lower_bound = max(latest_epoch, carry_mark_epoch) if max(latest_epoch, carry_mark_epoch) > 0 else max(0, entry_epoch - 1)
        if entry_epoch > 0:
            lower_bound = max(lower_bound, entry_epoch - 1)
        return self._rows_between_epochs(second_rows, lower_bound, int(cutoff_epoch))

    def _lightweight_price_exit(
        self,
        *,
        model_state: dict[str, Any],
        position: dict[str, Any],
        rows: list[dict[str, Any]],
        cost_points: float,
        lot_size: int,
        point_config: dict[str, Any] | None,
        tranche3_config: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        v1_portfolio = load_v1_portfolio_module(self.config)
        import pandas as pd  # type: ignore

        side = str(position.get("side") or "")
        entry_price = as_float(position.get("entry_price"))
        entry_fill_price = as_float(position.get("entry_fill_price")) or entry_price
        hard_sl = as_float(position.get("hard_sl_points"))
        trail_activation = as_float(position.get("trail_activation_points")) or 0.0
        entry_epoch = int(position.get("entry_epoch") or 0)
        if side not in {"long", "short"} or entry_price is None or hard_sl is None or not rows:
            return model_state, []
        point_config = _materialize_position_exit_profile(point_config, position)

        by_epoch: dict[int, dict[str, Any]] = {}
        for row in rows:
            epoch = int(row.get("epoch_second") or row.get("epoch") or 0)
            if epoch < entry_epoch or as_float(row.get("price")) is None:
                continue
            by_epoch[epoch] = row
        deduped = [by_epoch[epoch] for epoch in sorted(by_epoch)]
        if not deduped:
            return model_state, []

        settings = v1_portfolio.exit_profile_settings(point_config)
        trail_effective = v1_portfolio.effective_trail_activation_points(
            hard_sl,
            trail_activation,
            point_config=point_config,
        )
        max_favorable = max(0.0, as_float(position.get("max_favorable_points")) or 0.0)
        max_adverse = max(0.0, as_float(position.get("max_adverse_points")) or 0.0)
        exit_row: dict[str, Any] | None = None
        exit_reason: str | None = None
        exit_mfe = max_favorable
        exit_mae = max_adverse
        for row in deduped:
            price = as_float(row.get("price"))
            if price is None:
                continue
            gross = v1_portfolio._signed_position_points(side, entry_price, price)
            max_favorable = max(max_favorable, gross, 0.0)
            max_adverse = max(max_adverse, -gross, 0.0)
            hard_hit = price <= entry_price - hard_sl if side == "long" else price >= entry_price + hard_sl
            giveback = max_favorable - gross
            trail_hit = (
                max_favorable >= trail_effective
                and giveback >= float(settings["trail_giveback_fraction"]) * max_favorable
            )
            if hard_hit or trail_hit:
                exit_row = row
                exit_reason = "hard_sl" if hard_hit else "profit_trailing_sl"
                exit_mfe = max_favorable
                exit_mae = max_adverse
                break

        if exit_row is not None and int(exit_row.get("epoch_second") or exit_row.get("epoch") or 0) <= entry_epoch:
            exit_row = None
            exit_reason = None

        latest = exit_row or deduped[-1]
        latest_price = as_float(latest.get("price"))
        events: list[dict[str, Any]] = []
        if latest_price is not None:
            latest_fill = v1_portfolio.execution_fill_from_row(
                latest,
                side=side,
                phase="exit",
                point_config=point_config,
                fallback_round_trip_cost_points=cost_points,
            )
            latest_fill_price = as_float(latest_fill.get("fill_price"))
            accounting = (
                v1_portfolio.futures_trade_accounting(
                    side=side,
                    entry_fill_price=entry_fill_price,
                    exit_fill_price=latest_fill_price,
                    lot_size=lot_size,
                    point_config=point_config,
                )
                if latest_fill_price is not None
                else None
            )
            gross = (
                float(accounting["gross_points"])
                if accounting
                else v1_portfolio._signed_position_points(side, entry_price, latest_price)
            )
            position["max_favorable_points"] = max(max_favorable, gross, 0.0)
            position["max_adverse_points"] = max(max_adverse, -gross, 0.0)
            position["latest_price"] = latest_price
            position["latest_time"] = epoch_ist_iso(latest.get("epoch_second") or latest.get("epoch")) or latest.get("received_at_ist")
            position["latest_epoch"] = latest.get("epoch_second")
            position["latest_fill_price_if_closed"] = latest_fill_price
            position["latest_fill_quality_if_closed"] = latest_fill.get("fill_quality")
            position["latest_bid_price"] = latest_fill.get("bid_price")
            position["latest_ask_price"] = latest_fill.get("ask_price")
            position["gross_points"] = gross
            position["gross_rupees_if_closed"] = accounting.get("gross_rupees") if accounting else gross * lot_size
            position["charges_rupees_if_closed"] = accounting.get("charges_rupees") if accounting else cost_points * lot_size
            position["charge_breakdown_if_closed"] = accounting.get("charge_breakdown") if accounting else None
            position["net_points_if_closed"] = accounting.get("net_points") if accounting else gross - cost_points
            position["net_rupees_if_closed"] = accounting.get("net_rupees") if accounting else (gross - cost_points) * lot_size
            position["trail_activation_effective_points"] = trail_effective
            path = pd.DataFrame(deduped)
            final_epoch = (
                max(0, int(latest.get("epoch_second") or latest.get("epoch") or 0) - 1)
                if exit_row is not None
                else int(latest.get("epoch_second") or latest.get("epoch") or 0)
            ) or None
            position, tranche2_events = v1_portfolio._update_live_two_lot_ttsl(
                position=position,
                path=path,
                clock_state=None,
                latest_exit_fill_price=latest_fill_price,
                latest_exit_time=epoch_ist_iso(latest.get("epoch_second") or latest.get("epoch")) or latest.get("received_at_ist"),
                cost_points=cost_points,
                lot_size=lot_size,
                point_config=point_config,
                config=_ttsl_config_from_point_config(point_config),
                final_epoch=final_epoch,
            )
            tranche2_exit_event = next((event for event in tranche2_events if event.get("event") == "tranche2_exit"), None)
            tranche2_event_exit_epoch = (
                max(0, int(tranche2_exit_event.get("exit_epoch") or 0) - 1)
                if tranche2_exit_event and tranche2_exit_event.get("exit_epoch") is not None
                else final_epoch
            )
            tranche3_final_epoch = _tranche3_final_epoch(position, tranche2_event_exit_epoch)
            position, tranche3_events = _update_live_tranche3_v2(
                v1_portfolio=v1_portfolio,
                position=position,
                path=path,
                clock_state=None,
                latest_exit_fill_price=latest_fill_price,
                latest_exit_time=epoch_ist_iso(latest.get("epoch_second") or latest.get("epoch")) or latest.get("received_at_ist"),
                cost_points=cost_points,
                lot_size=lot_size,
                point_config=point_config,
                config=tranche3_config,
                final_epoch=tranche3_final_epoch or None,
            )
            tranche3_events = _filter_valid_tranche3_events(position, tranche3_events)
            if tranche2_exit_event and _tranche3_close_allowed(position, tranche2_exit_event.get("exit_epoch")):
                position, tranche3_exit_event = v1_portfolio._live_tranche3_close_from_event(
                    position=position,
                    exit_event=tranche2_exit_event,
                    exit_source="ttsl_exit",
                    exit_reason="tranche3_v1_ttsl_exit",
                    lot_size=lot_size,
                    point_config=point_config,
                )
                if tranche3_exit_event and _valid_tranche3_event(position, tranche3_exit_event):
                    tranche3_events.append(tranche3_exit_event)
            events.extend(
                sorted(
                    [*tranche3_events, *tranche2_events],
                    key=lambda event: int(event.get("entry_epoch") or event.get("exit_epoch") or 0),
                )
            )
            model_state["position"] = position

        if exit_row is None or exit_reason is None:
            model_state["updated_at_ist"] = now_ist().isoformat()
            return model_state, events

        exit_fill = v1_portfolio.execution_fill_from_row(
            exit_row,
            side=side,
            phase="exit",
            point_config=point_config,
            fallback_round_trip_cost_points=cost_points,
        )
        exit_price = as_float(exit_fill.get("fill_price"))
        exit_ltp_price = as_float(exit_fill.get("ltp_price"))
        if exit_price is None or exit_ltp_price is None:
            model_state["updated_at_ist"] = now_ist().isoformat()
            return model_state, events
        accounting = v1_portfolio.futures_trade_accounting(
            side=side,
            entry_fill_price=entry_fill_price,
            exit_fill_price=exit_price,
            lot_size=lot_size,
            point_config=point_config,
        )
        exit_epoch = int(exit_row.get("epoch_second") or 0)
        exit_event = {
            "event": "paper_exit",
            "signal_id": position.get("signal_id"),
            "position_id": position.get("position_id"),
            "exit_reason": exit_reason,
            "side": side,
            "instrument_key": position.get("instrument_key"),
            "contract_label": position.get("contract_label"),
            "signal_source": position.get("signal_source"),
            "signal_instrument_key": position.get("signal_instrument_key"),
            "signal_contract_label": position.get("signal_contract_label"),
            "lifecycle_start_date": position.get("lifecycle_start_date"),
            "expiry_date": position.get("expiry_date"),
            "entry_price": entry_price,
            "entry_ltp_price": position.get("entry_ltp_price"),
            "entry_fill_price": entry_fill_price,
            "entry_time": position.get("entry_time"),
            "entry_epoch": int(position.get("entry_epoch") or 0),
            "exit_price": exit_ltp_price,
            "exit_ltp_price": exit_ltp_price,
            "exit_fill_price": exit_price,
            "exit_time": epoch_ist_iso(exit_epoch),
            "exit_row_time": exit_row.get("received_at_ist"),
            "exit_epoch": exit_epoch,
            "model_gross_points": v1_portfolio._signed_position_points(side, entry_price, exit_ltp_price),
            "gross_points": accounting["gross_points"],
            "gross_rupees": accounting["gross_rupees"],
            "charges_rupees": accounting["charges_rupees"],
            "charge_breakdown": accounting["charge_breakdown"],
            "net_points": accounting["net_points"],
            "net_rupees": accounting["net_rupees"],
            "accounting_model": "bid_ask_proxy_slippage_zerodha_futures",
            **v1_portfolio.apply_fill_metadata(
                "entry",
                {key.removeprefix("entry_"): value for key, value in position.items() if key.startswith("entry_")},
            ),
            **v1_portfolio.apply_fill_metadata("exit", exit_fill),
            "source": position.get("source"),
            "signal_epoch": int(position.get("signal_epoch") or 0),
            "signal_time": position.get("signal_time"),
            "signal_price": position.get("signal_price"),
            "signal_enough_history": position.get("signal_enough_history"),
            "entry_decision": position.get("entry_decision"),
            "hard_sl_points": hard_sl,
            "trail_activation_points": trail_activation,
            "trail_activation_effective_points": trail_effective,
            "max_favorable_points": exit_mfe,
            "max_adverse_points": exit_mae,
            "created_at_ist": now_ist().isoformat(),
            "source_runtime": "v2_lightweight_price_risk",
        }
        exit_event = v1_portfolio._finalize_live_two_lot_on_base_exit(
            position=position,
            exit_event=exit_event,
            lot_size=lot_size,
            point_config=point_config,
        )
        tranche3_base_events: list[dict[str, Any]] = []
        if _tranche3_close_allowed(position, exit_event.get("exit_epoch")):
            position, exit_event, tranche3_base_events = v1_portfolio._finalize_live_tranche3_on_base_exit(
                position=position,
                exit_event=exit_event,
                lot_size=lot_size,
                point_config=point_config,
            )
            tranche3_base_events = _filter_valid_tranche3_events(position, tranche3_base_events)
        model_state["last_closed_trade"] = exit_event
        model_state["last_exit_epoch"] = exit_epoch
        model_state["position"] = None
        model_state["exhaustion_status"] = {
            "enabled": True,
            "status": "closed",
            "exit_reason": exit_reason,
            "exit_time": epoch_ist_iso(exit_epoch),
            "exit_row_time": exit_row.get("received_at_ist"),
            "source_runtime": "v2_lightweight_price_risk",
        }
        model_state["updated_at_ist"] = now_ist().isoformat()
        return model_state, events + tranche3_base_events + [exit_event]

    def _rows_through_epoch(
        self,
        rows: list[dict[str, Any]],
        cutoff_epoch: int,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        cutoff = int(cutoff_epoch)
        lo = 0
        hi = len(rows)
        while lo < hi:
            mid = (lo + hi) // 2
            try:
                mid_epoch = int(rows[mid].get("epoch_second") or rows[mid].get("epoch") or 0)
            except Exception:
                mid_epoch = 0
            if mid_epoch <= cutoff:
                lo = mid + 1
            else:
                hi = mid
        return rows[:lo]

    def _rows_between_epochs(
        self,
        rows: list[dict[str, Any]],
        lower_bound_epoch: int,
        cutoff_epoch: int,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        lower = int(lower_bound_epoch)
        cutoff = int(cutoff_epoch)
        lo = 0
        hi = len(rows)
        while lo < hi:
            mid = (lo + hi) // 2
            try:
                mid_epoch = int(rows[mid].get("epoch_second") or rows[mid].get("epoch") or 0)
            except Exception:
                mid_epoch = 0
            if mid_epoch <= lower:
                lo = mid + 1
            else:
                hi = mid
        start = lo
        lo = start
        hi = len(rows)
        while lo < hi:
            mid = (lo + hi) // 2
            try:
                mid_epoch = int(rows[mid].get("epoch_second") or rows[mid].get("epoch") or 0)
            except Exception:
                mid_epoch = 0
            if mid_epoch <= cutoff:
                lo = mid + 1
            else:
                hi = mid
        return rows[start:lo]

    def _latest_row_through_epoch(
        self,
        rows: list[dict[str, Any]],
        cutoff_epoch: int,
    ) -> dict[str, Any] | None:
        selected = self._rows_through_epoch(rows, cutoff_epoch)
        return dict(selected[-1]) if selected else None

    def _dataframe_through_epoch(
        self,
        rows: list[dict[str, Any]],
        cutoff_epoch: int,
    ):
        import pandas as pd  # type: ignore

        selected = self._rows_through_epoch(rows, cutoff_epoch)
        frame = pd.DataFrame(selected)
        if frame.empty:
            return frame
        if "epoch_second" in frame.columns:
            frame = frame.sort_values("epoch_second", kind="mergesort").reset_index(drop=True)
        return frame

    def _compact_clock_path_summary(
        self,
        *,
        position: dict[str, Any],
        second_rows: list[dict[str, Any]],
        clock_rows: list[dict[str, Any]],
        point_config: dict[str, Any] | None,
        cutoff_epoch: int,
    ):
        import pandas as pd  # type: ignore

        if not second_rows or not clock_rows:
            return pd.DataFrame()
        side = str(position.get("side") or "").lower()
        entry_epoch = int(position.get("entry_epoch") or 0)
        latest_epoch = int(position.get("latest_epoch") or 0)
        lower_bound = latest_epoch if latest_epoch > entry_epoch else max(0, entry_epoch - 1)
        entry_price = as_float(position.get("entry_price"))
        hard_sl = as_float(position.get("hard_sl_points"))
        trail_activation = (
            as_float(position.get("trail_activation_effective_points"))
            or as_float(position.get("trail_activation_points"))
        )
        if (
            side not in {"long", "short"}
            or entry_epoch <= 0
            or entry_price is None
            or hard_sl is None
            or trail_activation is None
        ):
            return pd.DataFrame()
        path_rows = [
            row
            for row in second_rows
            if lower_bound < int(row.get("epoch_second") or row.get("epoch") or 0) <= int(cutoff_epoch)
            and int(row.get("epoch_second") or row.get("epoch") or 0) >= entry_epoch
            and as_float(row.get("price")) is not None
        ]
        clock_source = [
            row
            for row in clock_rows
            if lower_bound < int(row.get("epoch_second") or row.get("epoch") or 0) <= int(cutoff_epoch)
        ]
        if not path_rows or not clock_source:
            return pd.DataFrame()
        path_rows.sort(key=lambda row: int(row.get("epoch_second") or row.get("epoch") or 0))
        clock_source.sort(key=lambda row: int(row.get("epoch_second") or row.get("epoch") or 0))
        path_epochs = [int(row.get("epoch_second") or row.get("epoch") or 0) for row in path_rows]
        settings = load_v1_portfolio_module(self.config).exit_profile_settings(point_config)
        trail_giveback = float(as_float(settings.get("trail_giveback_fraction")) or 0.80)
        hard_stop = float(entry_price) - float(hard_sl) if side == "long" else float(entry_price) + float(hard_sl)
        cursor = 0
        existing_mfe = max(0.0, as_float(position.get("max_favorable_points")) or 0.0)
        existing_mae = max(0.0, as_float(position.get("max_adverse_points")) or 0.0)
        if side == "long":
            best_favorable = float(entry_price) + existing_mfe
            worst_adverse = float(entry_price) - existing_mae
        else:
            best_favorable = float(entry_price) - existing_mfe
            worst_adverse = float(entry_price) + existing_mae
        out: list[dict[str, Any]] = []
        for clock in clock_source:
            clock_epoch = int(clock.get("epoch_second") or clock.get("epoch") or 0)
            while cursor < len(path_rows) and path_epochs[cursor] < clock_epoch:
                price_value = as_float(path_rows[cursor].get("price"))
                if price_value is not None:
                    if side == "long":
                        best_favorable = max(best_favorable, price_value)
                        worst_adverse = min(worst_adverse, price_value)
                    else:
                        best_favorable = min(best_favorable, price_value)
                        worst_adverse = max(worst_adverse, price_value)
                cursor += 1
            if cursor >= len(path_rows):
                break
            row = path_rows[cursor]
            price_value = as_float(row.get("price"))
            if price_value is None:
                cursor += 1
                continue
            if side == "long":
                best_favorable = max(best_favorable, price_value)
                worst_adverse = min(worst_adverse, price_value)
                mfe_points = max(0.0, best_favorable - float(entry_price))
                mae_points = max(0.0, float(entry_price) - worst_adverse)
                current_pnl = price_value - float(entry_price)
                if mfe_points >= float(trail_activation):
                    trail_stop = float(entry_price) + (1.0 - trail_giveback) * mfe_points
                    base_stop = max(hard_stop, trail_stop)
                    base_stop_mode = "profit_trailing_sl"
                else:
                    base_stop = hard_stop
                    base_stop_mode = "hard_sl"
            else:
                best_favorable = min(best_favorable, price_value)
                worst_adverse = max(worst_adverse, price_value)
                mfe_points = max(0.0, float(entry_price) - best_favorable)
                mae_points = max(0.0, worst_adverse - float(entry_price))
                current_pnl = float(entry_price) - price_value
                if mfe_points >= float(trail_activation):
                    trail_stop = float(entry_price) - (1.0 - trail_giveback) * mfe_points
                    base_stop = min(hard_stop, trail_stop)
                    base_stop_mode = "profit_trailing_sl"
                else:
                    base_stop = hard_stop
                    base_stop_mode = "hard_sl"
            out.append(
                {
                    "trade_date": row.get("trade_date"),
                    "epoch_second": int(row.get("epoch_second") or row.get("epoch") or 0),
                    "received_at_ist": row.get("received_at_ist"),
                    "clock_label": clock.get("clock_label"),
                    "clock_time": clock.get("actual_time") or clock.get("clock_time"),
                    "price": price_value,
                    "bid": row.get("bid"),
                    "ask": row.get("ask"),
                    "spread": row.get("spread"),
                    "current_pnl_points": current_pnl,
                    "mfe_points": mfe_points,
                    "mae_points": mae_points,
                    "base_stop": base_stop,
                    "base_stop_mode": base_stop_mode,
                }
            )
        return pd.DataFrame(out)

    def _compact_first_exhaustion_exit(
        self,
        *,
        signal_clock_state: Any,
        execution_clock_summary: Any,
        position: dict[str, Any],
        point_config: dict[str, Any] | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        v1_portfolio = load_v1_portfolio_module(self.config)
        import pandas as pd  # type: ignore

        side = str(position.get("side") or "").lower()
        entry_price = as_float(position.get("entry_price"))
        entry_epoch = int(position.get("entry_epoch") or 0)
        signal_epoch = int(position.get("signal_epoch") or 0)
        settings = v1_portfolio.exit_profile_settings(point_config)
        base = {
            "enabled": True,
            "side": side,
            "entry_epoch": entry_epoch,
            "signal_epoch": signal_epoch,
            "short_exit_threshold_pct": settings["short_exit_pct"],
            "long_exit_threshold_pct": settings["long_exit_pct"],
            "min_exit_age_sessions": settings["min_exit_age_sessions"],
            "min_profit_or_mfe_points": settings["min_profit_or_mfe_points"],
            "status": "unavailable",
            "source_runtime": "v2_compact_model_clock",
        }
        if (
            side not in {"long", "short"}
            or entry_price is None
            or entry_epoch <= 0
            or signal_epoch <= 0
            or not isinstance(signal_clock_state, pd.DataFrame)
            or signal_clock_state.empty
            or not isinstance(execution_clock_summary, pd.DataFrame)
            or execution_clock_summary.empty
        ):
            return None, base
        clocks = v1_portfolio.clock_rows_after_signal(
            signal_clock_state,
            signal_epoch=signal_epoch,
            entry_epoch=entry_epoch,
        )
        if clocks.empty:
            return None, {**base, "status": "waiting_for_post_signal_clock"}
        summary = execution_clock_summary.copy()
        if "epoch_second" not in summary.columns:
            return None, base
        summary["_epoch_for_exhaustion_v2"] = pd.to_numeric(summary["epoch_second"], errors="coerce")
        summary = summary[summary["_epoch_for_exhaustion_v2"].notna()].sort_values("_epoch_for_exhaustion_v2").reset_index(drop=True)
        if summary.empty:
            return None, base
        summary_epochs = [int(value) for value in summary["_epoch_for_exhaustion_v2"].tolist()]
        day_source = signal_clock_state if isinstance(signal_clock_state, pd.DataFrame) else summary
        days = (
            sorted(str(day) for day in day_source["trade_date"].dropna().astype(str).unique())
            if "trade_date" in day_source.columns
            else []
        )
        day_index = {day: idx for idx, day in enumerate(days)}
        entry_trade_date = str(position.get("entry_time") or "")[:10] or (days[0] if days else "")
        latest_context = {
            "latest_post_signal_pct": as_float(clocks.iloc[-1].get("post_signal_prior_pct")),
            "latest_post_signal_z": as_float(clocks.iloc[-1].get("obv_minus_price_prior_z")),
        }
        latest_status: dict[str, Any] | None = None
        for _, clock in clocks.iterrows():
            clock_epoch = int(clock["epoch_second"])
            rel_idx = next((i for i, value in enumerate(summary_epochs) if value >= clock_epoch), None)
            if rel_idx is None:
                continue
            execution_row = summary.iloc[rel_idx]
            trade_date = str(execution_row.get("trade_date") or "")
            age = day_index.get(trade_date, 0) - day_index.get(entry_trade_date, 0)
            current_pnl = as_float(execution_row.get("current_pnl_points"))
            mfe_points = as_float(execution_row.get("mfe_points"))
            post_pct = as_float(clock.get("post_signal_prior_pct"))
            post_z = as_float(clock.get("obv_minus_price_prior_z"))
            eligible = (
                age >= int(settings["min_exit_age_sessions"])
                and (
                    (mfe_points is not None and mfe_points >= float(settings["min_profit_or_mfe_points"]))
                    or (current_pnl is not None and current_pnl > 0)
                )
            )
            threshold_pass = False
            if post_pct is not None:
                threshold_pass = post_pct <= float(settings["short_exit_pct"]) if side == "short" else post_pct >= float(settings["long_exit_pct"])
            latest_status = {
                **base,
                "status": "tracking",
                "clock": clock.get("clock_label"),
                "clock_time": clock.get("actual_time"),
                "clock_epoch": clock_epoch,
                "price": as_float(execution_row.get("price")),
                "post_signal_prior_pct": post_pct,
                "post_signal_z": post_z,
                "age_sessions": age,
                "current_pnl_points": current_pnl,
                "mfe_points": mfe_points,
                "threshold_pass": bool(threshold_pass),
                "age_pass": bool(age >= int(settings["min_exit_age_sessions"])),
                "profit_or_mfe_pass": bool(
                    (mfe_points is not None and mfe_points >= float(settings["min_profit_or_mfe_points"]))
                    or (current_pnl is not None and current_pnl > 0)
                ),
                "eligible_now": bool(eligible and threshold_pass),
                **latest_context,
            }
            if eligible and threshold_pass:
                return (
                    {
                        "exit_reason": "post_signal_hard_exhaustion",
                        "exit_row": execution_row,
                        "exit_idx": rel_idx,
                        "max_favorable_points": mfe_points,
                        "post_signal_prior_pct": post_pct,
                        "post_signal_z": post_z,
                        "clock_label": clock.get("clock_label"),
                        "clock_time": clock.get("actual_time"),
                        "age_sessions": age,
                        "current_pnl_points": current_pnl,
                        "signal_exit_price": as_float(clock.get("price")),
                        **latest_context,
                    },
                    latest_status,
                )
        return None, latest_status or {**base, "status": "waiting_for_path_alignment"}

    def _compact_model_clock_position_update(
        self,
        *,
        model_state: dict[str, Any],
        position: dict[str, Any],
        signal_state: OnlineObvState,
        execution_state: OnlineObvState,
        cutoff_epoch: int,
        meta: InstrumentMeta,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        v1_portfolio = load_v1_portfolio_module(self.config)
        import pandas as pd  # type: ignore

        state = dict(model_state or {})
        position = dict(position or {})
        execution_point_config = _materialize_position_exit_profile(meta.execution_point_config, position)
        events: list[dict[str, Any]] = []
        active_rows = self._active_position_rows(
            position=position,
            second_rows=execution_state.second_rows,
            cutoff_epoch=cutoff_epoch,
        )
        state, price_events = self._lightweight_price_exit(
            model_state=state,
            position=position,
            rows=active_rows,
            cost_points=float(meta.round_trip_cost_points),
            lot_size=int(meta.lot_size or 1),
            point_config=execution_point_config,
            tranche3_config=_tranche3_config_from_adaptive(meta.adaptive_calibration),
        )
        events.extend(price_events)
        position = state.get("position") if isinstance(state.get("position"), dict) else None
        if not isinstance(position, dict):
            return state, events

        self.ensure_clock_rows_through(signal_state, meta.signal_point_config, through_epoch=cutoff_epoch)
        self.ensure_clock_rows_through(execution_state, execution_point_config, through_epoch=cutoff_epoch)
        signal_clock_state = self._dataframe_through_epoch(signal_state.clock_rows, cutoff_epoch)
        execution_clock_state = self._dataframe_through_epoch(execution_state.clock_rows, cutoff_epoch)
        latest_execution_tick = self._latest_row_through_epoch(execution_state.second_rows, cutoff_epoch)
        if not latest_execution_tick:
            return state, events

        side = str(position.get("side") or "").lower()
        entry_price = as_float(position.get("entry_price"))
        entry_fill_price = as_float(position.get("entry_fill_price")) or entry_price
        latest_price = as_float(latest_execution_tick.get("price"))
        latest_fill = v1_portfolio.execution_fill_from_row(
            latest_execution_tick,
            side=side,
            phase="exit",
            point_config=execution_point_config,
            fallback_round_trip_cost_points=float(meta.round_trip_cost_points),
        )
        latest_fill_price = as_float(latest_fill.get("fill_price"))
        if side not in {"long", "short"} or entry_price is None or entry_fill_price is None or latest_price is None:
            return state, events

        accounting = (
            v1_portfolio.futures_trade_accounting(
                side=side,
                entry_fill_price=float(entry_fill_price),
                    exit_fill_price=float(latest_fill_price),
                    lot_size=int(meta.lot_size or 1),
                    point_config=execution_point_config,
                )
            if latest_fill_price is not None
            else None
        )
        gross = (
            float(accounting["gross_points"])
            if accounting
            else v1_portfolio._signed_position_points(side, float(entry_price), float(latest_price))
        )
        clock_summary = self._compact_clock_path_summary(
            position=position,
            second_rows=execution_state.second_rows,
            clock_rows=execution_state.clock_rows,
            point_config=execution_point_config,
            cutoff_epoch=cutoff_epoch,
        )
        if isinstance(clock_summary, pd.DataFrame) and not clock_summary.empty:
            latest_summary = clock_summary.sort_values("epoch_second").iloc[-1]
            mfe = as_float(latest_summary.get("mfe_points")) or 0.0
            mae = as_float(latest_summary.get("mae_points")) or 0.0
        else:
            mfe = max(gross, 0.0)
            mae = max(-gross, 0.0)
        hard_sl = float(position.get("hard_sl_points") or 0.0)
        trail_activation = float(position.get("trail_activation_points") or 0.0)
        trail_activation_effective = v1_portfolio.effective_trail_activation_points(
            hard_sl,
            trail_activation,
            point_config=execution_point_config,
        )
        position["max_favorable_points"] = max(float(position.get("max_favorable_points") or 0.0), mfe)
        position["max_adverse_points"] = max(float(position.get("max_adverse_points") or 0.0), mae)
        position["latest_price"] = latest_price
        position["latest_time"] = epoch_ist_iso(latest_execution_tick.get("epoch_second") or latest_execution_tick.get("epoch")) or latest_execution_tick.get("received_at_ist")
        position["latest_epoch"] = int(latest_execution_tick.get("epoch_second") or latest_execution_tick.get("epoch") or 0)
        position["latest_fill_price_if_closed"] = latest_fill_price
        position["latest_fill_quality_if_closed"] = latest_fill.get("fill_quality")
        position["latest_bid_price"] = latest_fill.get("bid_price")
        position["latest_ask_price"] = latest_fill.get("ask_price")
        position["gross_points"] = gross
        position["gross_rupees_if_closed"] = accounting.get("gross_rupees") if accounting else gross * int(meta.lot_size or 1)
        position["charges_rupees_if_closed"] = accounting.get("charges_rupees") if accounting else float(meta.round_trip_cost_points) * int(meta.lot_size or 1)
        position["charge_breakdown_if_closed"] = accounting.get("charge_breakdown") if accounting else None
        position["net_points_if_closed"] = accounting.get("net_points") if accounting else gross - float(meta.round_trip_cost_points)
        position["net_rupees_if_closed"] = accounting.get("net_rupees") if accounting else (gross - float(meta.round_trip_cost_points)) * int(meta.lot_size or 1)
        position["trail_activation_effective_points"] = trail_activation_effective

        path_exit, exhaustion_status = self._compact_first_exhaustion_exit(
            signal_clock_state=signal_clock_state,
            execution_clock_summary=clock_summary,
            position=position,
            point_config=execution_point_config,
        )
        state["exhaustion_status"] = exhaustion_status
        latest_clock = signal_clock_state.iloc[-1].to_dict() if isinstance(signal_clock_state, pd.DataFrame) and not signal_clock_state.empty else {}
        warning = None
        if latest_clock:
            if side == "long" and latest_clock.get("fresh_trend_short_active"):
                warning = "fresh_trend_short_warning"
            elif side == "short" and latest_clock.get("fresh_trend_long_active"):
                warning = "fresh_trend_long_warning"
        state["latest_warning"] = warning

        final_epoch_for_clock = (
            max(0, int(path_exit["exit_row"]["epoch_second"]) - 1)
            if path_exit and path_exit.get("exit_row") is not None
            else int(latest_execution_tick.get("epoch_second") or latest_execution_tick.get("epoch") or 0)
        )
        position = v1_portfolio._update_live_two_lot_ttsl_from_clock_summary(
            position=position,
            clock_summary=clock_summary if isinstance(clock_summary, pd.DataFrame) else pd.DataFrame(),
            latest_exit_fill_price=latest_fill_price,
            latest_exit_time=epoch_ist_iso(latest_execution_tick.get("epoch_second") or latest_execution_tick.get("epoch")) or latest_execution_tick.get("received_at_ist"),
            lot_size=int(meta.lot_size or 1),
            point_config=execution_point_config,
            config=_ttsl_config_from_point_config(execution_point_config),
            final_epoch=final_epoch_for_clock or None,
        )
        final_epoch_for_tranche3 = _tranche3_final_epoch(position, final_epoch_for_clock)
        position, tranche3_events = _update_live_tranche3_v2(
            v1_portfolio=v1_portfolio,
            position=position,
            path=pd.DataFrame(active_rows),
            clock_state=execution_clock_state,
            latest_exit_fill_price=latest_fill_price,
            latest_exit_time=epoch_ist_iso(latest_execution_tick.get("epoch_second") or latest_execution_tick.get("epoch")) or latest_execution_tick.get("received_at_ist"),
            cost_points=float(meta.round_trip_cost_points),
            lot_size=int(meta.lot_size or 1),
            point_config=execution_point_config,
            config=_tranche3_config_from_adaptive(meta.adaptive_calibration),
            final_epoch=final_epoch_for_tranche3 or None,
        )
        tranche3_events = _filter_valid_tranche3_events(position, tranche3_events)
        events.extend(tranche3_events)
        state["position"] = position
        state["updated_at_ist"] = now_ist().isoformat()
        if not path_exit:
            return state, events

        exit_row = path_exit["exit_row"]
        exit_fill = v1_portfolio.execution_fill_from_row(
            exit_row,
            side=side,
            phase="exit",
            point_config=execution_point_config,
            fallback_round_trip_cost_points=float(meta.round_trip_cost_points),
        )
        exit_price = as_float(exit_fill.get("fill_price"))
        exit_ltp_price = as_float(exit_fill.get("ltp_price"))
        if exit_price is None or exit_ltp_price is None:
            return state, events
        exit_epoch = int(exit_row["epoch_second"])
        exit_time = epoch_ist_iso(exit_epoch)
        signal_exit_price = as_float(path_exit.get("signal_exit_price"))
        transition_reference_price = signal_exit_price if signal_exit_price is not None else exit_price
        exit_accounting = v1_portfolio.futures_trade_accounting(
            side=side,
            entry_fill_price=float(entry_fill_price),
            exit_fill_price=float(exit_price),
            lot_size=int(meta.lot_size or 1),
            point_config=execution_point_config,
        )
        exit_event = {
            "event": "paper_exit",
            "signal_id": position.get("signal_id"),
            "position_id": position.get("position_id"),
            "exit_reason": str(path_exit["exit_reason"]),
            "side": side,
            "instrument_key": meta.execution_key,
            "contract_label": meta.execution_contract_label,
            "signal_source": meta.signal_source,
            "signal_instrument_key": meta.signal_key,
            "signal_contract_label": meta.signal_contract_label,
            "lifecycle_start_date": meta.lifecycle_start_date,
            "expiry_date": meta.expiry_date,
            "entry_price": entry_price,
            "entry_ltp_price": position.get("entry_ltp_price"),
            "entry_fill_price": entry_fill_price,
            "entry_time": position.get("entry_time"),
            "entry_epoch": int(position.get("entry_epoch") or 0),
            "exit_price": exit_ltp_price,
            "exit_ltp_price": exit_ltp_price,
            "exit_fill_price": exit_price,
            "exit_time": exit_time,
            "exit_row_time": exit_row.get("received_at_ist"),
            "exit_epoch": exit_epoch,
            "signal_exit_price": signal_exit_price,
            "transition_reference_price": transition_reference_price,
            "transition_reference_instrument_key": meta.signal_key,
            "transition_reference_basis_method": "cash_signal_reference" if meta.signal_source == "cash" else None,
            "model_gross_points": v1_portfolio._signed_position_points(side, float(entry_price), float(exit_ltp_price)),
            "gross_points": float(exit_accounting["gross_points"]),
            "gross_rupees": exit_accounting["gross_rupees"],
            "charges_rupees": exit_accounting["charges_rupees"],
            "charge_breakdown": exit_accounting["charge_breakdown"],
            "net_points": float(exit_accounting["net_points"]),
            "net_rupees": exit_accounting["net_rupees"],
            "accounting_model": "bid_ask_proxy_slippage_zerodha_futures",
            **v1_portfolio.apply_fill_metadata("entry", {k.removeprefix("entry_"): v for k, v in position.items() if k.startswith("entry_")}),
            **v1_portfolio.apply_fill_metadata("exit", exit_fill),
            "source": position.get("source"),
            "signal_epoch": int(position.get("signal_epoch") or 0),
            "signal_time": position.get("signal_time"),
            "signal_price": position.get("signal_price"),
            "signal_enough_history": position.get("signal_enough_history"),
            "entry_decision": position.get("entry_decision"),
            "hard_sl_points": hard_sl,
            "trail_activation_points": trail_activation,
            "trail_activation_effective_points": trail_activation_effective,
            "max_favorable_points": position["max_favorable_points"],
            "max_adverse_points": position["max_adverse_points"],
            "post_signal_prior_pct": path_exit.get("post_signal_prior_pct"),
            "post_signal_z": path_exit.get("post_signal_z"),
            "clock_label": path_exit.get("clock_label"),
            "clock_time": path_exit.get("clock_time"),
            "age_sessions": path_exit.get("age_sessions"),
            "current_pnl_points": path_exit.get("current_pnl_points"),
            "latest_post_signal_pct": path_exit.get("latest_post_signal_pct"),
            "latest_post_signal_z": path_exit.get("latest_post_signal_z"),
            "created_at_ist": now_ist().isoformat(),
            "source_runtime": "v2_compact_model_clock",
        }
        exit_event = v1_portfolio._finalize_live_two_lot_on_base_exit(
            position=position,
            exit_event=exit_event,
            lot_size=int(meta.lot_size or 1),
            point_config=execution_point_config,
        )
        tranche3_base_events: list[dict[str, Any]] = []
        if _tranche3_close_allowed(position, exit_event.get("exit_epoch")):
            position, exit_event, tranche3_base_events = v1_portfolio._finalize_live_tranche3_on_base_exit(
                position=position,
                exit_event=exit_event,
                lot_size=int(meta.lot_size or 1),
                point_config=execution_point_config,
            )
            tranche3_base_events = _filter_valid_tranche3_events(position, tranche3_base_events)
        state["last_closed_trade"] = exit_event
        state["last_exit_epoch"] = exit_epoch
        state["position"] = None
        state["exhaustion_status"] = {
            "enabled": True,
            "status": "closed",
            "exit_reason": str(path_exit["exit_reason"]),
            "exit_time": exit_time,
            "post_signal_prior_pct": path_exit.get("post_signal_prior_pct"),
            "post_signal_z": path_exit.get("post_signal_z"),
            "clock_label": path_exit.get("clock_label"),
            "source_runtime": "v2_compact_model_clock",
        }
        state["updated_at_ist"] = now_ist().isoformat()
        events.extend(tranche3_base_events)
        events.append(exit_event)
        return state, events

    def evaluate_frozen_trade_state(
        self,
        trade_date: str,
        *,
        symbols: Iterable[str] | None = None,
        reason: str = "loop",
        evaluation_epoch: int | None = None,
    ) -> dict[str, Any]:
        if not bool(self.config.get("frozen_v1_trade_state_enabled", True)):
            return {"enabled": False, "reason": "disabled"}
        started = time.perf_counter()
        effective_evaluation_epoch = int(evaluation_epoch if evaluation_epoch is not None else time.time())
        v1_portfolio = load_v1_portfolio_module(self.config)
        requested = list(symbols) if symbols is not None else list(self.instruments)
        updated = 0
        event_count = 0
        skipped: list[dict[str, Any]] = []
        recovered: list[dict[str, Any]] = []
        event_samples: list[dict[str, Any]] = []
        for symbol in requested:
            meta = self.instruments.get(str(symbol))
            if meta is None:
                continue
            signal_state = self.states.get(meta.signal_key)
            execution_state = self.states.get(meta.execution_key)
            if signal_state is None or execution_state is None:
                skipped.append({"symbol": meta.symbol, "reason": "state_missing"})
                continue
            if not signal_state.second_rows or not execution_state.second_rows:
                skipped.append({"symbol": meta.symbol, "reason": "seconds_missing"})
                continue
            current_model_state = dict(self.model_states.get(meta.symbol) or {})
            position = current_model_state.get("position")
            if reason == "active_position_loop" and isinstance(position, dict):
                try:
                    cutoff = int(effective_evaluation_epoch)
                    raw_rows = self._active_position_rows(
                        position=position,
                        second_rows=execution_state.second_rows,
                        cutoff_epoch=cutoff,
                    )
                    model_state, events = self._lightweight_price_exit(
                        model_state=current_model_state,
                        position=dict(position),
                        rows=raw_rows,
                        cost_points=float(meta.round_trip_cost_points),
                        lot_size=int(meta.lot_size or 1),
                        point_config=_materialize_position_exit_profile(meta.execution_point_config, position),
                        tranche3_config=_tranche3_config_from_adaptive(meta.adaptive_calibration),
                    )
                    model_state, events = self.calibrate_open_position_state(meta, model_state, events)
                except Exception as exc:
                    skipped.append(
                        {
                            "symbol": meta.symbol,
                            "reason": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc(limit=8).splitlines()[-8:],
                            "path": "lightweight_price_exit",
                        }
                    )
                    continue
                self.model_states[meta.symbol] = dict(model_state or {})
                self.save_model_state(meta.symbol, self.model_states[meta.symbol])
                enriched = [self.enrich_v1_event(meta, event) for event in events if isinstance(event, dict)]
                if enriched:
                    self.append_trade_state_events(trade_date, meta.symbol, enriched)
                    event_count += len(enriched)
                    event_samples.extend(enriched[: max(0, 10 - len(event_samples))])
                updated += 1
                continue
            if reason in {"model_clock", "model_clock_active_symbols", "model_clock_entry_symbols"} and isinstance(position, dict):
                try:
                    model_state, events = self._compact_model_clock_position_update(
                        model_state=current_model_state,
                        position=dict(position),
                        signal_state=signal_state,
                        execution_state=execution_state,
                        cutoff_epoch=int(effective_evaluation_epoch),
                        meta=meta,
                    )
                except Exception as exc:
                    skipped.append(
                        {
                            "symbol": meta.symbol,
                            "reason": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc(limit=8).splitlines()[-8:],
                            "path": "compact_model_clock_position_update",
                        }
                    )
                    continue
                self.model_states[meta.symbol] = dict(model_state or {})
                self.save_model_state(meta.symbol, self.model_states[meta.symbol])
                enriched = [self.enrich_v1_event(meta, event) for event in events if isinstance(event, dict)]
                if enriched:
                    for event in enriched:
                        event["source_runtime"] = event.get("source_runtime") or "v2_compact_model_clock"
                    self.append_trade_state_events(trade_date, meta.symbol, enriched)
                    event_count += len(enriched)
                    event_samples.extend(enriched[: max(0, 10 - len(event_samples))])
                updated += 1
                continue
            try:
                entry_delay_seconds = int(self.config.get("entry_delay_seconds") or 60)
                execution_point_config = _materialize_position_exit_profile(
                    meta.execution_point_config,
                    position if isinstance(position, dict) else None,
                )
                current_model_state, stale_pending_events = self.reject_stale_pending_entries_before_fill(
                    meta=meta,
                    model_state=current_model_state,
                    trade_date=trade_date,
                    evaluation_epoch=effective_evaluation_epoch,
                    entry_delay_seconds=entry_delay_seconds,
                )
                signal_contract_state = self.v1_contract_state_from_online(
                    state=signal_state,
                    today=trade_date,
                    point_config=meta.signal_point_config,
                    through_epoch=effective_evaluation_epoch,
                )
                signal_contract_state, filtered_late_entry_edges = self.filter_live_entry_edges_for_retained_window(
                    signal_contract_state,
                    evaluation_epoch=effective_evaluation_epoch,
                    entry_delay_seconds=entry_delay_seconds,
                )
                execution_contract_state = self.v1_contract_state_from_online(
                    state=execution_state,
                    today=trade_date,
                    point_config=execution_point_config,
                    through_epoch=effective_evaluation_epoch,
                )
                model_state, events = v1_portfolio.update_position_state_with_execution(
                    model_state=dict(current_model_state or {}),
                    signal_contract_state=signal_contract_state,
                    execution_contract_state=execution_contract_state,
                    today=trade_date,
                    entry_delay_seconds=entry_delay_seconds,
                    cost_points=float(meta.round_trip_cost_points),
                    lot_size=int(meta.lot_size or 1),
                    signal_source=meta.signal_source,
                    signal_instrument_key=meta.signal_key,
                    signal_contract_label=meta.signal_contract_label,
                    execution_instrument_key=meta.execution_key,
                    execution_contract_label=meta.execution_contract_label,
                    instrument_id=meta.symbol,
                    strategy_id=self.strategy_id,
                    lifecycle_start_date=meta.lifecycle_start_date,
                    expiry_date=meta.expiry_date,
                    execution_point_config=execution_point_config,
                    signal_point_config=meta.signal_point_config,
                    max_entry_lag_seconds=self.live_entry_lag_guard_seconds(),
                    evaluation_epoch=effective_evaluation_epoch,
                )
                if stale_pending_events:
                    events = [*stale_pending_events, *events]
                model_state, events = self.calibrate_open_position_state(meta, model_state, events)
                model_state, events = self.reject_retained_late_entry_fill(
                    meta=meta,
                    previous_state=current_model_state,
                    model_state=model_state,
                    events=events,
                    trade_date=trade_date,
                    evaluation_epoch=effective_evaluation_epoch,
                    entry_delay_seconds=entry_delay_seconds,
                )
                if filtered_late_entry_edges:
                    for event in events:
                        if isinstance(event, dict):
                            event["late_entry_edges_filtered"] = filtered_late_entry_edges
            except Exception as exc:
                original_reason = f"{type(exc).__name__}: {exc}"
                original_traceback = traceback.format_exc(limit=8)
                if isinstance(position, dict):
                    try:
                        cutoff = int(effective_evaluation_epoch)
                        raw_rows = [
                            row
                            for row in execution_state.second_rows
                            if int(row.get("epoch_second") or 0) <= cutoff
                        ]
                        model_state, events = self._lightweight_price_exit(
                            model_state=current_model_state,
                            position=dict(position),
                            rows=raw_rows,
                            cost_points=float(meta.round_trip_cost_points),
                            lot_size=int(meta.lot_size or 1),
                            point_config=execution_point_config,
                            tranche3_config=_tranche3_config_from_adaptive(meta.adaptive_calibration),
                        )
                        model_state, events = self.calibrate_open_position_state(meta, model_state, events)
                        recovered.append(
                            {
                                "symbol": meta.symbol,
                                "reason": original_reason,
                                "fallback": "lightweight_price_exit",
                                "events": len(events),
                            }
                        )
                    except Exception as fallback_exc:
                        skipped.append(
                            {
                                "symbol": meta.symbol,
                                "reason": original_reason,
                                "traceback": original_traceback.splitlines()[-8:],
                                "fallback_reason": f"{type(fallback_exc).__name__}: {fallback_exc}",
                                "fallback_traceback": traceback.format_exc(limit=8).splitlines()[-8:],
                            }
                        )
                        continue
                else:
                    skipped.append(
                        {
                            "symbol": meta.symbol,
                            "reason": original_reason,
                            "traceback": original_traceback.splitlines()[-8:],
                        }
                    )
                    continue
            self.model_states[meta.symbol] = dict(model_state or {})
            self.save_model_state(meta.symbol, self.model_states[meta.symbol])
            enriched = [self.enrich_v1_event(meta, event) for event in events if isinstance(event, dict)]
            if enriched:
                self.append_trade_state_events(trade_date, meta.symbol, enriched)
                event_count += len(enriched)
                event_samples.extend(enriched[: max(0, 10 - len(event_samples))])
            updated += 1
        report = {
            "event": "frozen_v1_trade_state_evaluation",
            "reason": reason,
            "symbols_requested": len(requested),
            "symbols_updated": updated,
            "events": event_count,
            "event_samples": event_samples[:10],
            "skipped_count": len(skipped),
            "skipped_samples": skipped[:20],
            "recovered_count": len(recovered),
            "recovered_samples": recovered[:20],
            "evaluation_epoch": effective_evaluation_epoch,
            "evaluation_time": epoch_ist_iso(effective_evaluation_epoch),
            "duration_seconds": round(time.perf_counter() - started, 4),
            "recorded_at_ist": now_ist().isoformat(),
        }
        append_jsonl(self.telemetry_path, report)
        self.latest_trade_state_report = report
        return report

    def entry_event_from_row(self, meta: InstrumentMeta, row: dict[str, Any], *, module: str, side: str) -> dict[str, Any]:
        signal_epoch = int(row["epoch_second"])
        signal_id = canonical_signal_id(
            strategy_id=self.strategy_id,
            instrument_id=meta.symbol,
            side=side,
            module=module,
            signal_epoch=signal_epoch,
            signal_source=meta.signal_source,
            signal_instrument_key=meta.signal_key,
            execution_instrument_key=meta.execution_key,
        )
        return {
            "schema": "obvfutport_v2.passive_decision_event.v1",
            "event": "entry_signal",
            "passive_only": True,
            "strategy_id": self.strategy_id,
            "model_version": self.config.get("model_version"),
            "architecture_version": self.config.get("architecture_version"),
            "signal_id": signal_id,
            "symbol": meta.symbol,
            "side": side,
            "module": module,
            "signal_source": meta.signal_source,
            "signal_instrument_key": meta.signal_key,
            "execution_instrument_key": meta.execution_key,
            "signal_epoch": signal_epoch,
            "signal_time": row.get("actual_time"),
            "entry_due_epoch": signal_epoch + int(self.config.get("entry_delay_seconds") or 60),
            "entry_due_time": epoch_ist_iso(signal_epoch + int(self.config.get("entry_delay_seconds") or 60)),
            "signal_price": row.get("price"),
            "z": row.get("obv_minus_price_prior_z"),
            "prior_p10": row.get("prior_p10"),
            "prior_p90": row.get("prior_p90"),
            "price_change_prior_pct": row.get("price_change_prior_pct"),
            "long_trigger_price": row.get("long_trigger_price"),
            "short_trigger_price": row.get("short_trigger_price"),
            "signal_enough_history": row.get("signal_enough_history"),
            "threshold_source": meta.source,
            "threshold_synthesized": meta.synthesized,
            "recorded_at_ist": now_ist().isoformat(),
            **_adaptive_event_fields(meta),
        }

    def entry_event_from_edge(self, meta: InstrumentMeta, edge: dict[str, Any]) -> dict[str, Any]:
        signal_epoch = int(edge["signal_epoch"])
        module = str(edge.get("module") or "")
        side = str(edge.get("side") or "")
        signal_id = canonical_signal_id(
            strategy_id=self.strategy_id,
            instrument_id=meta.symbol,
            side=side,
            module=module,
            signal_epoch=signal_epoch,
            signal_source=meta.signal_source,
            signal_instrument_key=meta.signal_key,
            execution_instrument_key=meta.execution_key,
        )
        out = {
            "schema": "obvfutport_v2.passive_decision_event.v1",
            "event": "entry_signal",
            "passive_only": True,
            "strategy_id": self.strategy_id,
            "model_version": self.config.get("model_version"),
            "architecture_version": self.config.get("architecture_version"),
            "signal_id": signal_id,
            "position_id": f"{signal_id}:position",
            "symbol": meta.symbol,
            "side": side,
            "module": module,
            "signal_source": meta.signal_source,
            "signal_instrument_key": meta.signal_key,
            "execution_instrument_key": meta.execution_key,
            "signal_epoch": signal_epoch,
            "signal_time": edge.get("signal_time") or epoch_ist_iso(signal_epoch),
            "entry_due_epoch": signal_epoch + int(self.config.get("entry_delay_seconds") or 60),
            "entry_due_time": epoch_ist_iso(signal_epoch + int(self.config.get("entry_delay_seconds") or 60)),
            "signal_price": edge.get("signal_price"),
            "z": edge.get("z"),
            "prior_p10": edge.get("prior_p10"),
            "prior_p90": edge.get("prior_p90"),
            "price_change_prior_pct": edge.get("price_change_prior_pct"),
            "long_trigger_price": edge.get("long_trigger_price"),
            "short_trigger_price": edge.get("short_trigger_price"),
            "signal_enough_history": edge.get("signal_enough_history"),
            "reclaim_points": edge.get("reclaim_points"),
            "transition_trigger_price": edge.get("transition_trigger_price"),
            "source_exit_reason": edge.get("source_exit_reason"),
            "source_exit_time": edge.get("source_exit_time"),
            "source_exit_price": edge.get("source_exit_price"),
            "transition_reference_price": edge.get("transition_reference_price"),
            "transition_reference_instrument_key": edge.get("transition_reference_instrument_key"),
            "transition_reference_basis_points": edge.get("transition_reference_basis_points"),
            "transition_reference_basis_method": edge.get("transition_reference_basis_method"),
            "signal_loop_match": edge.get("signal_loop_match"),
            "threshold_source": meta.source,
            "threshold_synthesized": meta.synthesized,
            "recorded_at_ist": now_ist().isoformat(),
            **_adaptive_event_fields(meta),
        }
        return out

    def entry_signal_skip_event_from_edge(
        self,
        meta: InstrumentMeta,
        edge: dict[str, Any],
        *,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event = self.entry_event_from_edge(meta, edge)
        return {
            **event,
            "event": "entry_signal_skipped",
            "decision_only": True,
            "suppress_downstream": True,
            "selected_leg_event": False,
            "skip_reason": reason,
            "skip_details": details or {},
            "recorded_at_ist": now_ist().isoformat(),
        }

    def evaluate_clock(self, clock_epoch: int, trade_date: str) -> dict[str, Any]:
        started = time.perf_counter()
        stale: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        skipped_edges: list[dict[str, Any]] = []
        current_edge_symbols: set[str] = set()
        metas = list(self.instruments.values())
        readiness = self.wait_for_signal_clock_readiness(metas, trade_date=trade_date, clock_epoch=clock_epoch)
        missing_symbols = {
            str(item.get("symbol"))
            for item in (readiness.get("missing") or [])
            if isinstance(item, dict) and item.get("symbol")
        }
        for item in readiness.get("missing") or []:
            if not isinstance(item, dict):
                continue
            stale.append({"status": "missed_not_ready", **item})
            self._queue_controlled_repair(
                symbol=str(item.get("symbol") or ""),
                trade_date=trade_date,
                mode="signal",
                reason=str(item.get("reason") or "signal_clock_not_ready"),
                details=item,
            )
        for meta in metas:
            if meta.symbol in missing_symbols:
                continue
            signal_state = self.states.get(meta.signal_key)
            exec_state = self.states.get(meta.execution_key)
            if signal_state is None or exec_state is None:
                stale.append({"symbol": meta.symbol, "reason": "state_missing"})
                continue
            row, reason = signal_state.build_clock_row(clock_epoch, self.clock_label(clock_epoch), meta.signal_point_config)
            if row is None:
                stale.append({"symbol": meta.symbol, "role": "signal", **(reason or {})})
                self._queue_controlled_repair(
                    symbol=meta.symbol,
                    trade_date=trade_date,
                    mode="signal",
                    reason=str((reason or {}).get("reason") or "missing_clock_metric"),
                    details=reason or {},
                )
                continue
            signal_quote_age = self.source_quote_age_at_clock(row, signal_state, clock_epoch)
            if signal_quote_age is None or signal_quote_age > self.signal_quote_max_age:
                item = {
                    "symbol": meta.symbol,
                    "role": "signal",
                    "reason": "signal_quote_stale",
                    "quote_age_seconds": signal_quote_age,
                    "max_quote_age_seconds": self.signal_quote_max_age,
                    "source_quote_epoch": row.get("source_quote_epoch"),
                    "source_received_epoch": row.get("source_received_epoch"),
                }
                stale.append(item)
                self._queue_controlled_repair(
                    symbol=meta.symbol,
                    trade_date=trade_date,
                    mode="signal",
                    reason="signal_quote_stale",
                    details=item,
                )
                continue
            execution_quote_age = exec_state.quote_age_at(clock_epoch)
            if execution_quote_age is None or execution_quote_age > self.signal_quote_max_age:
                stale.append(
                    {
                        "symbol": meta.symbol,
                        "role": "execution",
                        "reason": "execution_quote_stale",
                        "quote_age_seconds": execution_quote_age,
                    }
                )
            try:
                current_edges, edge_build_report = self.current_clock_entry_edges_from_contract(
                    meta=meta,
                    state=signal_state,
                    trade_date=trade_date,
                    clock_epoch=clock_epoch,
                )
            except Exception as exc:
                item = {
                    "symbol": meta.symbol,
                    "role": "signal",
                    "reason": f"current_clock_edge_build_failed:{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(limit=8).splitlines()[-8:],
                }
                stale.append(item)
                self._queue_controlled_repair(
                    symbol=meta.symbol,
                    trade_date=trade_date,
                    mode="signal",
                    reason="current_clock_edge_build_failed",
                    details=item,
                )
                continue
            direct_modules = [
                ("fresh_trend_long", "fresh_trend_long_active_edge", "long"),
                ("fresh_trend_short", "fresh_trend_short_active_edge", "short"),
                (
                    f"primary_obv_short_abs{as_float(meta.signal_point_config.get('primary_obv_short_abs_threshold')) or 1.5:g}",
                    "primary_obv_short_configured_active_edge",
                    "short",
                ),
            ]
            direct_edge_keys = {
                (module, side)
                for module, column, side in direct_modules
                if bool(row.get(column))
            }
            contract_edge_keys = {
                (str(edge.get("module") or ""), str(edge.get("side") or ""))
                for edge in current_edges
            }
            if direct_edge_keys != contract_edge_keys:
                stale.append(
                    {
                        "symbol": meta.symbol,
                        "role": "signal",
                        "reason": "entry_edge_source_mismatch",
                        "direct_edge_keys": sorted([f"{module}:{side}" for module, side in direct_edge_keys]),
                        "contract_edge_keys": sorted([f"{module}:{side}" for module, side in contract_edge_keys]),
                        **edge_build_report,
                    }
                )
            for edge in current_edges:
                current_edge_symbols.add(meta.symbol)
                event = self.entry_event_from_edge(meta, edge)
                if event["signal_id"] in self.events_seen:
                    skip = self.entry_signal_skip_event_from_edge(
                        meta,
                        edge,
                        reason="duplicate_signal_id",
                        details={"clock_epoch": clock_epoch, "signal_id": event["signal_id"]},
                    )
                    append_jsonl(self.decision_events_path(trade_date), skip)
                    skipped_edges.append(skip)
                    continue
                self.events_seen.add(str(event["signal_id"]))
                append_jsonl(self.decision_events_path(trade_date), event)
                events.append(event)
        report = {
            "event": "clock_evaluation",
            "clock_epoch": clock_epoch,
            "clock_time": epoch_ist_iso(clock_epoch),
            "symbols": len(self.instruments),
            "target_keys": len(self.targets),
            "events": len(events),
            "event_symbols": sorted({str(event.get("symbol")) for event in events if event.get("symbol")}),
            "current_edge_symbols": sorted(current_edge_symbols),
            "current_edge_symbol_count": len(current_edge_symbols),
            "skipped_edge_count": len(skipped_edges),
            "skipped_edge_samples": skipped_edges[:20],
            "stale_count": len(stale),
            "stale_samples": stale[:20],
            "readiness_barrier": readiness,
            "missed_not_ready_count": len(missing_symbols),
            "missed_not_ready_symbols": sorted(missing_symbols),
            "duration_seconds": round(time.perf_counter() - started, 4),
            "recorded_at_ist": now_ist().isoformat(),
        }
        append_jsonl(self.telemetry_path, report)
        self.latest_decision_report = report
        self.last_actual_evaluated_clock = clock_epoch
        return report

    def evaluate_due_clocks(self, trade_date: str) -> list[int]:
        now_epoch = time.time()
        due = sorted(epoch for epoch in self.clock_epochs if epoch + self.decision_delay_seconds <= now_epoch)
        evaluated: list[int] = []
        event_symbols: set[str] = set()
        for clock_epoch in due:
            if self.clock_watermark is not None and clock_epoch <= self.clock_watermark:
                continue
            report = self.evaluate_clock(clock_epoch, trade_date)
            event_symbols.update(str(symbol) for symbol in report.get("current_edge_symbols") or [] if symbol)
            event_symbols.update(str(symbol) for symbol in report.get("event_symbols") or [] if symbol)
            self.clock_watermark = clock_epoch
            evaluated.append(clock_epoch)
        self.latest_due_clock_event_symbols = sorted(event_symbols)
        return evaluated

    def evaluate_transition_signals(self, trade_date: str) -> dict[str, Any]:
        if not bool(self.config.get("continuous_transition_signal_enabled", True)):
            return {"enabled": False, "reason": "disabled"}
        poll_seconds = float(self.config.get("transition_signal_poll_seconds") or 2.0)
        if (
            self._last_transition_signal_eval_monotonic
            and time.perf_counter() - self._last_transition_signal_eval_monotonic < poll_seconds
        ):
            return self.latest_transition_signal_report
        self._last_transition_signal_eval_monotonic = time.perf_counter()
        started = time.perf_counter()
        now_epoch = int(time.time())
        entry_delay_seconds = int(self.config.get("entry_delay_seconds") or 60)
        max_live_entry_lag_seconds = self.live_entry_fill_acceptance_seconds()
        v1_obv_model = load_v1_obv_model_module(self.config)
        watched = 0
        events: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for symbol in self.active_trade_state_symbols():
            meta = self.instruments.get(str(symbol))
            model_state = self.model_states.get(str(symbol))
            if meta is None or not isinstance(model_state, dict):
                continue
            if model_state.get("position") or model_state.get("pending_entry") or model_state.get("pending_entry_signal"):
                continue
            last_closed = model_state.get("last_closed_trade")
            if not isinstance(last_closed, dict) or last_closed.get("exit_reason") != "post_signal_hard_exhaustion":
                continue
            watched += 1
            signal_state = self.states.get(meta.signal_key)
            if signal_state is None or not signal_state.second_rows:
                skipped.append({"symbol": meta.symbol, "reason": "signal_seconds_missing"})
                continue
            try:
                signal_contract_state = self.v1_contract_state_from_online(
                    state=signal_state,
                    today=trade_date,
                    point_config=meta.signal_point_config,
                )
                transition_edges = v1_obv_model.post_exhaustion_transition_candidates(
                    today_seconds=signal_contract_state["today_seconds"],
                    clock_state=signal_contract_state["clock_state"],
                    entry_edges_today=signal_contract_state["entry_edges_today"],
                    last_exit=last_closed,
                    enable_continuation=True,
                    point_config=meta.signal_point_config,
                )
            except Exception as exc:
                skipped.append({"symbol": meta.symbol, "reason": f"{type(exc).__name__}: {exc}"})
                continue
            last_signal_epoch = int(model_state.get("last_signal_epoch") or 0)
            last_exit_epoch = int(model_state.get("last_exit_epoch") or 0)
            for edge in transition_edges:
                signal_epoch = int(edge.get("signal_epoch") or 0)
                if signal_epoch <= 0 or signal_epoch > now_epoch:
                    continue
                if signal_epoch <= last_signal_epoch or signal_epoch <= last_exit_epoch:
                    continue
                due_epoch = self._entry_due_epoch(edge, entry_delay_seconds)
                event = self.entry_event_from_edge(
                    meta,
                    {**edge, "signal_loop_match": "post_exhaustion_transition_edge"},
                )
                if event["signal_id"] in self.events_seen:
                    continue
                if (
                    max_live_entry_lag_seconds is not None
                    and due_epoch is not None
                    and self.is_market_hours_epoch(now_epoch)
                    and now_epoch - int(due_epoch) > int(max_live_entry_lag_seconds)
                ):
                    skip_event = self.entry_signal_skip_event_from_edge(
                        meta,
                        {**edge, "signal_loop_match": "post_exhaustion_transition_edge"},
                        reason="stale_transition_signal_at_detection",
                        details={
                            "signal_epoch": signal_epoch,
                            "signal_time": epoch_ist_iso(signal_epoch),
                            "entry_due_epoch": int(due_epoch),
                            "entry_due_time": epoch_ist_iso(int(due_epoch)),
                            "detected_epoch": now_epoch,
                            "detected_time": epoch_ist_iso(now_epoch),
                            "entry_staleness_seconds": now_epoch - int(due_epoch),
                            "max_live_entry_fill_lag_seconds": int(max_live_entry_lag_seconds),
                        },
                    )
                    self.events_seen.add(str(skip_event["signal_id"]))
                    append_jsonl(self.decision_events_path(trade_date), skip_event)
                    model_state["last_signal_epoch"] = max(
                        int(model_state.get("last_signal_epoch") or 0),
                        signal_epoch,
                    )
                    model_state["updated_at_ist"] = now_ist().isoformat()
                    self.model_states[meta.symbol] = dict(model_state)
                    self.save_model_state(meta.symbol, self.model_states[meta.symbol])
                    skipped.append(
                        {
                            "symbol": meta.symbol,
                            "reason": "stale_transition_signal_at_detection",
                            "signal_epoch": signal_epoch,
                            "entry_due_epoch": int(due_epoch),
                            "entry_staleness_seconds": now_epoch - int(due_epoch),
                        }
                    )
                    continue
                self.events_seen.add(str(event["signal_id"]))
                append_jsonl(self.decision_events_path(trade_date), event)
                events.append(event)
        report = {
            "event": "transition_signal_evaluation",
            "enabled": True,
            "watched_symbols": watched,
            "events": len(events),
            "event_symbols": sorted({str(event.get("symbol")) for event in events if event.get("symbol")}),
            "skipped_count": len(skipped),
            "skipped_samples": skipped[:20],
            "duration_seconds": round(time.perf_counter() - started, 4),
            "recorded_at_ist": now_ist().isoformat(),
        }
        append_jsonl(self.telemetry_path, report)
        self.latest_transition_signal_report = report
        return report

    def should_evaluate_active_trade_state(self) -> bool:
        tail_quotes = int((self.latest_tail_report or {}).get("quotes") or 0)
        if tail_quotes > 0:
            self._last_active_trade_state_eval_monotonic = time.perf_counter()
            return True
        poll_seconds = (
            float(self.config.get("active_trade_state_poll_seconds") or 1.0)
            if self.is_market_hours_now()
            else float(self.config.get("after_hours_active_trade_state_poll_seconds") or 30.0)
        )
        elapsed = time.perf_counter() - self._last_active_trade_state_eval_monotonic
        if self._last_active_trade_state_eval_monotonic <= 0.0 or elapsed >= poll_seconds:
            self._last_active_trade_state_eval_monotonic = time.perf_counter()
            return True
        return False

    def _reset_current_lifecycle_states(self, meta: InstrumentMeta) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        for key in {meta.execution_key, meta.signal_key}:
            if not key:
                continue
            reports.append(
                self.reset_online_state_to_lifecycle(
                    key,
                    meta.lifecycle_start_date,
                    retention_seconds=self.lifecycle_reset_second_row_retention_seconds,
                )
            )
        return reports

    def evaluate_rollovers(self, trade_date: str, *, when: datetime | None = None) -> dict[str, Any]:
        if not bool(self.config.get("lifecycle_rollover_enabled", True)):
            return {"enabled": False, "reason": "disabled"}
        started = time.perf_counter()
        when = when or now_ist()
        now_epoch = int(when.timestamp())
        v1_portfolio = load_v1_portfolio_module(self.config)
        import pandas as pd  # type: ignore

        checked = 0
        due = 0
        updated = 0
        event_count = 0
        statuses: list[dict[str, Any]] = []
        event_samples: list[dict[str, Any]] = []
        for symbol, meta in list(self.instruments.items()):
            context = lifecycle_status_from_meta(meta, when, self.holiday_dates)
            active = context.get("active_rollover") if isinstance(context, dict) else None
            if not isinstance(active, dict):
                continue
            checked += 1
            roll_epoch = int(datetime.fromisoformat(str(active["roll_datetime_ist"])).timestamp())
            if now_epoch < roll_epoch:
                statuses.append({**active, "instrument_id": symbol, "status": "pending", "roll_epoch": roll_epoch})
                continue
            due += 1
            rollover_id = str(active["rollover_id"])
            state = dict(self.model_states.get(symbol) or {})
            to_meta = self.meta_for_contract_index(meta, int(active["to_index"]))
            if state.get("last_rollover_id") == rollover_id:
                if meta.current_contract_index != to_meta.current_contract_index:
                    self.instruments[symbol] = to_meta
                statuses.append({**active, "instrument_id": symbol, "status": "already_applied", "roll_epoch": roll_epoch})
                continue

            from_meta = self.meta_for_contract_index(meta, int(active["from_index"]))
            from_execution_state = self.states.get(from_meta.execution_key)
            to_execution_state_obj = self.states.get(to_meta.execution_key)
            from_signal_state = self.states.get(from_meta.signal_key)
            to_signal_state_obj = self.states.get(to_meta.signal_key)
            missing = [
                name
                for name, state_obj in {
                    "from_execution": from_execution_state,
                    "to_execution": to_execution_state_obj,
                    "from_signal": from_signal_state,
                    "to_signal": to_signal_state_obj,
                }.items()
                if state_obj is None
            ]
            if missing:
                status = {
                    **active,
                    "instrument_id": symbol,
                    "status": "roll_state_missing",
                    "roll_epoch": roll_epoch,
                    "missing": missing,
                }
                state["lifecycle_rollover_status"] = status
                self.model_states[symbol] = state
                self.save_model_state(symbol, state)
                statuses.append(status)
                continue

            position = state.get("position")
            if not isinstance(position, dict):
                state["last_rollover_id"] = rollover_id
                state["last_signal_epoch"] = max(int(state.get("last_signal_epoch") or 0), roll_epoch)
                state["last_exit_epoch"] = max(int(state.get("last_exit_epoch") or 0), roll_epoch)
                reset_reports = self._reset_current_lifecycle_states(to_meta)
                self.instruments[symbol] = to_meta
                status = {
                    **active,
                    "instrument_id": symbol,
                    "status": "flat_no_position_at_roll",
                    "roll_epoch": roll_epoch,
                    "state_reset": reset_reports,
                }
                state["lifecycle_rollover_status"] = status
                self.model_states[symbol] = state
                self.save_model_state(symbol, state)
                updated += 1
                statuses.append(status)
                continue

            position_key = position.get("instrument_key")
            if position_key == to_meta.execution_key:
                state["last_rollover_id"] = rollover_id
                reset_reports = self._reset_current_lifecycle_states(to_meta)
                self.instruments[symbol] = to_meta
                status = {
                    **active,
                    "instrument_id": symbol,
                    "status": "position_already_on_next_contract",
                    "roll_epoch": roll_epoch,
                    "state_reset": reset_reports,
                }
                state["lifecycle_rollover_status"] = status
                self.model_states[symbol] = state
                self.save_model_state(symbol, state)
                updated += 1
                statuses.append(status)
                continue
            if position_key and position_key != from_meta.execution_key:
                status = {
                    **active,
                    "instrument_id": symbol,
                    "status": "position_contract_mismatch",
                    "roll_epoch": roll_epoch,
                    "position_instrument_key": position_key,
                }
                state["lifecycle_rollover_status"] = status
                self.model_states[symbol] = state
                self.save_model_state(symbol, state)
                statuses.append(status)
                continue

            roll_date = str(active["roll_date"])
            from_execution_contract_state = self.v1_contract_state_from_online(
                state=from_execution_state,  # type: ignore[arg-type]
                today=roll_date,
                point_config=from_meta.execution_point_config,
                lifecycle_start_date=from_meta.lifecycle_start_date,
                recompute_from_lifecycle_rows=True,
            )
            to_execution_contract_state = self.v1_contract_state_from_online(
                state=to_execution_state_obj,  # type: ignore[arg-type]
                today=roll_date,
                point_config=to_meta.execution_point_config,
                lifecycle_start_date=to_meta.lifecycle_start_date,
                recompute_from_lifecycle_rows=True,
            )
            from_signal_contract_state = self.v1_contract_state_from_online(
                state=from_signal_state,  # type: ignore[arg-type]
                today=roll_date,
                point_config=from_meta.signal_point_config,
                lifecycle_start_date=from_meta.lifecycle_start_date,
                recompute_from_lifecycle_rows=True,
            )
            to_signal_contract_state = self.v1_contract_state_from_online(
                state=to_signal_state_obj,  # type: ignore[arg-type]
                today=roll_date,
                point_config=to_meta.signal_point_config,
                lifecycle_start_date=to_meta.lifecycle_start_date,
                recompute_from_lifecycle_rows=True,
            )
            from_frame = from_execution_contract_state["seconds"]
            to_frame = to_execution_contract_state["seconds"]
            from_row = v1_portfolio.row_at_or_after(from_frame, epoch=roll_epoch, trade_date=roll_date)
            to_row = v1_portfolio.row_at_or_after(to_frame, epoch=roll_epoch, trade_date=roll_date)
            if from_row is None or to_row is None:
                status = {
                    **active,
                    "instrument_id": symbol,
                    "status": "waiting_for_roll_price_ticks",
                    "roll_epoch": roll_epoch,
                    "from_tick_found": from_row is not None,
                    "to_tick_found": to_row is not None,
                }
                state["lifecycle_rollover_status"] = status
                self.model_states[symbol] = state
                self.save_model_state(symbol, state)
                statuses.append(status)
                continue

            from_epoch = int(from_row["epoch_second"])
            to_epoch = int(to_row["epoch_second"])
            expiring_execution_state = self.v1_contract_state_from_online(
                state=from_execution_state,  # type: ignore[arg-type]
                today=roll_date,
                point_config=from_meta.execution_point_config,
                lifecycle_start_date=from_meta.lifecycle_start_date,
                through_epoch=from_epoch,
                recompute_from_lifecycle_rows=True,
            )
            expiring_signal_state = self.v1_contract_state_from_online(
                state=from_signal_state,  # type: ignore[arg-type]
                today=roll_date,
                point_config=from_meta.signal_point_config,
                lifecycle_start_date=from_meta.lifecycle_start_date,
                through_epoch=from_epoch,
                recompute_from_lifecycle_rows=True,
            )
            state, pre_roll_events = v1_portfolio.update_position_state_with_execution(
                model_state=state,
                signal_contract_state=expiring_signal_state,
                execution_contract_state=expiring_execution_state,
                today=roll_date,
                entry_delay_seconds=int(self.config.get("entry_delay_seconds") or 60),
                cost_points=float(from_meta.round_trip_cost_points),
                lot_size=int(from_meta.lot_size or 1),
                signal_source=from_meta.signal_source,
                signal_instrument_key=from_meta.signal_key,
                signal_contract_label=from_meta.signal_contract_label,
                execution_instrument_key=from_meta.execution_key,
                execution_contract_label=from_meta.execution_contract_label,
                instrument_id=symbol,
                strategy_id=self.strategy_id,
                lifecycle_start_date=from_meta.lifecycle_start_date,
                expiry_date=from_meta.expiry_date,
                execution_point_config=from_meta.execution_point_config,
                signal_point_config=from_meta.signal_point_config,
            )
            if state.get("position") is None:
                state["last_rollover_id"] = rollover_id
                state["last_signal_epoch"] = max(int(state.get("last_signal_epoch") or 0), roll_epoch)
                state["last_exit_epoch"] = max(int(state.get("last_exit_epoch") or 0), roll_epoch)
                reset_reports = self._reset_current_lifecycle_states(to_meta)
                self.instruments[symbol] = to_meta
                status = {
                    **active,
                    "instrument_id": symbol,
                    "status": "position_closed_by_exit_stack_before_roll_transfer",
                    "roll_epoch": roll_epoch,
                    "events_created": len(pre_roll_events),
                    "state_reset": reset_reports,
                }
                state["lifecycle_rollover_status"] = status
                self.model_states[symbol] = state
                self.save_model_state(symbol, state)
                enriched = [self.enrich_v1_event(from_meta, event) for event in pre_roll_events if isinstance(event, dict)]
                self.append_trade_state_events(roll_date, symbol, enriched)
                event_count += len(enriched)
                event_samples.extend(enriched[: max(0, 10 - len(event_samples))])
                updated += 1
                statuses.append(status)
                continue

            position = dict(state["position"])
            side = str(position["side"])
            source_position_id, source_signal_id = self.resolve_position_identity(
                symbol=symbol,
                meta=from_meta,
                position=position,
                side=side,
            )
            position["position_id"] = source_position_id
            position["signal_id"] = source_signal_id
            entry_price = float(position["entry_price"])
            entry_fill_price = as_float(position.get("entry_fill_price")) or entry_price
            roll_exit_fill = v1_portfolio.execution_fill_from_row(
                from_row,
                side=side,
                phase="exit",
                point_config=from_meta.execution_point_config,
                fallback_round_trip_cost_points=float(from_meta.round_trip_cost_points),
            )
            roll_entry_fill = v1_portfolio.execution_fill_from_row(
                to_row,
                side=side,
                phase="entry",
                point_config=to_meta.execution_point_config,
                fallback_round_trip_cost_points=float(to_meta.round_trip_cost_points),
            )
            roll_exit_fill_price = as_float(roll_exit_fill.get("fill_price"))
            roll_entry_fill_price = as_float(roll_entry_fill.get("fill_price"))
            roll_exit_ltp_price = as_float(roll_exit_fill.get("ltp_price"))
            roll_entry_ltp_price = as_float(roll_entry_fill.get("ltp_price"))
            roll_exit_price = roll_exit_ltp_price
            roll_entry_price = roll_entry_ltp_price
            if roll_exit_price is None or roll_entry_price is None or roll_exit_fill_price is None or roll_entry_fill_price is None:
                status = {
                    **active,
                    "instrument_id": symbol,
                    "status": "waiting_for_roll_executable_prices",
                    "roll_epoch": roll_epoch,
                    "from_fill_found": roll_exit_fill_price is not None,
                    "to_fill_found": roll_entry_fill_price is not None,
                    "from_ltp_found": roll_exit_price is not None,
                    "to_ltp_found": roll_entry_price is not None,
                }
                state["lifecycle_rollover_status"] = status
                self.model_states[symbol] = state
                self.save_model_state(symbol, state)
                statuses.append(status)
                continue
            accounting = v1_portfolio.futures_trade_accounting(
                side=side,
                entry_fill_price=entry_fill_price,
                exit_fill_price=roll_exit_fill_price,
                lot_size=int(from_meta.lot_size or 1),
                point_config=from_meta.execution_point_config,
            )
            gross = float(accounting["gross_points"])
            net = float(accounting["net_points"])
            expiring_path = expiring_execution_state["seconds"]
            expiring_path = expiring_path[
                pd.to_numeric(expiring_path.get("epoch_second"), errors="coerce")
                >= int(position["entry_epoch"])
            ]
            excursions = v1_portfolio._path_excursion_points(expiring_path, side, entry_price)
            mfe = float(excursions["mfe_points"])
            mae = float(excursions["mae_points"])
            exit_event = {
                "event": "paper_exit",
                "exit_reason": "lifecycle_rollover",
                "rollover_id": rollover_id,
                "position_id": source_position_id,
                "signal_id": source_signal_id,
                "side": side,
                "instrument_key": from_meta.execution_key,
                "contract_label": from_meta.execution_contract_label,
                "signal_source": from_meta.signal_source,
                "signal_instrument_key": from_meta.signal_key,
                "signal_contract_label": from_meta.signal_contract_label,
                "lifecycle_start_date": from_meta.lifecycle_start_date,
                "expiry_date": from_meta.expiry_date,
                "entry_price": entry_price,
                "entry_ltp_price": position.get("entry_ltp_price"),
                "entry_fill_price": entry_fill_price,
                "entry_time": position.get("entry_time"),
                "entry_epoch": int(position.get("entry_epoch")),
                "exit_price": roll_exit_price,
                "exit_ltp_price": roll_exit_ltp_price,
                "exit_fill_price": roll_exit_fill_price,
                "exit_time": epoch_ist_iso(from_epoch),
                "exit_row_time": from_row.get("received_at_ist"),
                "exit_epoch": from_epoch,
                "event_epoch": from_epoch,
                "model_gross_points": v1_portfolio._signed_position_points(side, entry_price, roll_exit_price),
                "gross_points": gross,
                "gross_rupees": accounting["gross_rupees"],
                "charges_rupees": accounting["charges_rupees"],
                "charge_breakdown": accounting["charge_breakdown"],
                "net_points": net,
                "net_rupees": accounting["net_rupees"],
                "accounting_model": "bid_ask_proxy_slippage_zerodha_futures",
                **v1_portfolio.apply_fill_metadata(
                    "entry",
                    {k.removeprefix("entry_"): v for k, v in position.items() if k.startswith("entry_")},
                ),
                **v1_portfolio.apply_fill_metadata("exit", roll_exit_fill),
                "source": position.get("source"),
                "signal_epoch": int(position.get("signal_epoch")),
                "signal_time": position.get("signal_time"),
                "signal_price": position.get("signal_price"),
                "hard_sl_points": position.get("hard_sl_points"),
                "trail_activation_points": position.get("trail_activation_points"),
                "trail_activation_effective_points": position.get("trail_activation_effective_points"),
                "max_favorable_points": max(float(position.get("max_favorable_points") or 0.0), mfe),
                "max_adverse_points": max(float(position.get("max_adverse_points") or 0.0), mae),
                "created_at_ist": now_ist().isoformat(),
            }
            exit_event = v1_portfolio._finalize_live_two_lot_on_base_exit(
                position=position,
                exit_event=exit_event,
                lot_size=int(from_meta.lot_size or 1),
                point_config=from_meta.execution_point_config,
            )
            tranche3_roll_exit_events: list[dict[str, Any]] = []
            if _tranche3_close_allowed(position, exit_event.get("exit_epoch")):
                position, exit_event, tranche3_roll_exit_events = v1_portfolio._finalize_live_tranche3_on_base_exit(
                    position=position,
                    exit_event=exit_event,
                    lot_size=int(from_meta.lot_size or 1),
                    point_config=from_meta.execution_point_config,
                )
                tranche3_roll_exit_events = _filter_valid_tranche3_events(position, tranche3_roll_exit_events)
            hard_sl = v1_portfolio.dynamic_risk_points(
                to_execution_contract_state["clock_state"],
                to_epoch,
                kind="hard_sl",
                point_config=to_meta.execution_point_config,
            )
            trail_activation = v1_portfolio.dynamic_risk_points(
                to_execution_contract_state["clock_state"],
                to_epoch,
                kind="trail_activation",
                point_config=to_meta.execution_point_config,
            )
            rollover_decision = {
                "module": "lifecycle_rollover",
                "side": side,
                "rollover_id": rollover_id,
                "from_position_id": source_position_id,
                "from_signal_id": source_signal_id,
                "roll_epoch": roll_epoch,
                "roll_time_ist": active.get("roll_time_ist"),
                "from_instrument_key": from_meta.execution_key,
                "to_instrument_key": to_meta.execution_key,
                "from_lifecycle_start_date": from_meta.lifecycle_start_date,
                "to_lifecycle_start_date": to_meta.lifecycle_start_date,
                "from_exit_price": roll_exit_price,
                "from_exit_ltp_price": roll_exit_ltp_price,
                "from_exit_fill_price": roll_exit_fill_price,
                "to_entry_price": roll_entry_price,
                "to_entry_ltp_price": roll_entry_ltp_price,
                "to_entry_fill_price": roll_entry_fill_price,
                "roll_spread_points": roll_entry_price - roll_exit_price,
                "roll_fill_spread_points": roll_entry_fill_price - roll_exit_fill_price,
                "roll_ltp_spread_points": (roll_entry_ltp_price - roll_exit_ltp_price)
                if roll_entry_ltp_price is not None and roll_exit_ltp_price is not None
                else None,
            }
            rolled_position_id, rolled_signal_id = self.rollover_position_identity(
                symbol=symbol,
                rollover_id=rollover_id,
                side=side,
                entry_epoch=to_epoch,
                from_position_id=source_position_id,
                to_meta=to_meta,
            )
            new_position = {
                "side": side,
                "position_id": rolled_position_id,
                "signal_id": rolled_signal_id,
                "instrument_key": to_meta.execution_key,
                "contract_label": to_meta.execution_contract_label,
                "lifecycle_start_date": to_meta.lifecycle_start_date,
                "expiry_date": to_meta.expiry_date,
                "signal_source": to_meta.signal_source,
                "signal_instrument_key": to_meta.signal_key,
                "signal_contract_label": to_meta.signal_contract_label,
                "source": "lifecycle_rollover",
                "source_rollover_id": rollover_id,
                "roll_from_position_id": source_position_id,
                "roll_from_signal_id": source_signal_id,
                "roll_from_instrument_key": from_meta.execution_key,
                "roll_from_exit_price": roll_exit_price,
                "roll_from_exit_ltp_price": roll_exit_ltp_price,
                "roll_from_exit_fill_price": roll_exit_fill_price,
                "roll_from_exit_time": epoch_ist_iso(from_epoch),
                "roll_from_exit_row_time": from_row.get("received_at_ist"),
                "roll_spread_points": roll_entry_price - roll_exit_price,
                "roll_fill_spread_points": roll_entry_fill_price - roll_exit_fill_price,
                "entry_decision": rollover_decision,
                "signal_epoch": to_epoch,
                "signal_time": epoch_ist_iso(to_epoch),
                "signal_row_time": to_row.get("received_at_ist"),
                "signal_price": roll_entry_price,
                "entry_epoch": to_epoch,
                "entry_time": epoch_ist_iso(to_epoch),
                "entry_row_time": to_row.get("received_at_ist"),
                "entry_price": roll_entry_price,
                "entry_ltp_price": roll_entry_ltp_price,
                "entry_fill_price": roll_entry_fill_price,
                "accounting_model": "bid_ask_proxy_slippage_zerodha_futures",
                **v1_portfolio.apply_fill_metadata("entry", roll_entry_fill),
                "hard_sl_points": hard_sl,
                "trail_activation_points": trail_activation,
                "trail_activation_effective_points": v1_portfolio.effective_trail_activation_points(
                    hard_sl,
                    trail_activation,
                    point_config=to_meta.execution_point_config,
                ),
                "max_favorable_points": 0.0,
                "max_adverse_points": 0.0,
                "status": "open",
                **_adaptive_event_fields(to_meta),
            }
            new_position = v1_portfolio._ensure_live_two_lot_ttsl(
                dict(new_position),
                config=_ttsl_config_from_point_config(to_meta.execution_point_config),
                lot_size=int(to_meta.lot_size or 1),
            )
            new_position = v1_portfolio._ensure_live_tranche3(
                dict(new_position),
                config=_tranche3_config_from_adaptive(to_meta.adaptive_calibration),
                lot_size=int(to_meta.lot_size or 1),
            )
            entry_event = {
                "event": "paper_entry",
                "position_id": rolled_position_id,
                "signal_id": rolled_signal_id,
                "side": side,
                "source": "lifecycle_rollover",
                "instrument_key": to_meta.execution_key,
                "contract_label": to_meta.execution_contract_label,
                "signal_source": to_meta.signal_source,
                "signal_instrument_key": to_meta.signal_key,
                "signal_contract_label": to_meta.signal_contract_label,
                "entry_epoch": to_epoch,
                "entry_time": epoch_ist_iso(to_epoch),
                "event_epoch": to_epoch,
                "position": new_position,
                "rollover_id": rollover_id,
                "created_at_ist": now_ist().isoformat(),
            }
            roll_event = {
                "event": "paper_rollover",
                "rollover_id": rollover_id,
                "position_id": rolled_position_id,
                "signal_id": rolled_signal_id,
                "from_position_id": source_position_id,
                "from_signal_id": source_signal_id,
                "to_position_id": rolled_position_id,
                "to_signal_id": rolled_signal_id,
                "event_epoch": to_epoch,
                "entry_epoch": to_epoch,
                "exit_epoch": from_epoch,
                "side": side,
                "from_instrument_key": from_meta.execution_key,
                "to_instrument_key": to_meta.execution_key,
                "from_exit_time": epoch_ist_iso(from_epoch),
                "from_exit_row_time": from_row.get("received_at_ist"),
                "to_entry_time": epoch_ist_iso(to_epoch),
                "to_entry_row_time": to_row.get("received_at_ist"),
                "from_exit_price": roll_exit_price,
                "from_exit_ltp_price": roll_exit_ltp_price,
                "from_exit_fill_price": roll_exit_fill_price,
                "to_entry_price": roll_entry_price,
                "to_entry_ltp_price": roll_entry_ltp_price,
                "to_entry_fill_price": roll_entry_fill_price,
                "roll_spread_points": roll_entry_price - roll_exit_price,
                "roll_fill_spread_points": roll_entry_fill_price - roll_exit_fill_price,
                "roll_ltp_spread_points": (roll_entry_ltp_price - roll_exit_ltp_price)
                if roll_entry_ltp_price is not None and roll_exit_ltp_price is not None
                else None,
                "rule": "previous NSE trading day before current contract expiry after 15:25 checkpoint",
                "created_at_ist": now_ist().isoformat(),
            }
            state["position"] = new_position
            state["last_closed_trade"] = exit_event
            state["last_exit_epoch"] = max(int(state.get("last_exit_epoch") or 0), from_epoch, roll_epoch)
            state["last_signal_epoch"] = max(int(state.get("last_signal_epoch") or 0), to_epoch, roll_epoch)
            state["last_rollover_id"] = rollover_id
            reset_reports = self._reset_current_lifecycle_states(to_meta)
            self.instruments[symbol] = to_meta
            status = {
                **active,
                "instrument_id": symbol,
                "status": "rolled_open_position",
                "roll_epoch": roll_epoch,
                "from_exit_epoch": from_epoch,
                "to_entry_epoch": to_epoch,
                "from_exit_price": roll_exit_price,
                "to_entry_price": roll_entry_price,
                "roll_spread_points": roll_entry_price - roll_exit_price,
                "state_reset": reset_reports,
            }
            state["lifecycle_rollover_status"] = status
            self.model_states[symbol] = state
            self.save_model_state(symbol, state)
            events = pre_roll_events + tranche3_roll_exit_events + [exit_event, entry_event, roll_event]
            enriched = [self.enrich_v1_event(to_meta, event) for event in events if isinstance(event, dict)]
            self.append_trade_state_events(roll_date, symbol, enriched)
            event_count += len(enriched)
            event_samples.extend(enriched[: max(0, 10 - len(event_samples))])
            updated += 1
            statuses.append(status)

        report = {
            "event": "lifecycle_rollover_evaluation",
            "enabled": True,
            "checked_rollover_symbols": checked,
            "due_symbols": due,
            "symbols_updated": updated,
            "events": event_count,
            "event_samples": event_samples[:10],
            "status_samples": statuses[:20],
            "duration_seconds": round(time.perf_counter() - started, 4),
            "recorded_at_ist": now_ist().isoformat(),
        }
        append_jsonl(self.telemetry_path, report)
        self.latest_rollover_report = report
        return report

    def active_trade_state_symbols(self, *, include_transition_watch: bool = True) -> list[str]:
        out: list[str] = []
        for symbol, state in self.model_states.items():
            if not isinstance(state, dict):
                continue
            position = state.get("position")
            if isinstance(position, dict) and self.position_counts_as_active(position):
                out.append(symbol)
                continue
            if state.get("pending_entry") or state.get("pending_entry_signals"):
                out.append(symbol)
                continue
            if not include_transition_watch:
                continue
            last_closed = state.get("last_closed_trade")
            if isinstance(last_closed, dict) and str(last_closed.get("exit_reason") or "").startswith("post_signal"):
                out.append(symbol)
        return out

    def pending_entry_due_symbols(self, *, lookahead_seconds: float = 2.0) -> list[str]:
        cutoff_epoch = time.time() + max(0.0, float(lookahead_seconds))
        entry_delay_seconds = int(self.config.get("entry_delay_seconds") or 60)
        out: list[str] = []
        for symbol, state in self.model_states.items():
            if not isinstance(state, dict):
                continue
            pending_items: list[dict[str, Any]] = []
            for key in ("pending_entry", "pending_entry_signal"):
                item = state.get(key)
                if isinstance(item, dict):
                    pending_items.append(item)
            pending_list = state.get("pending_entry_signals")
            if isinstance(pending_list, list):
                pending_items.extend(item for item in pending_list if isinstance(item, dict))
            for item in pending_items:
                due_epoch = as_float(item.get("entry_due_epoch"))
                if due_epoch is None:
                    signal_epoch = as_float(item.get("signal_epoch"))
                    due_epoch = signal_epoch + entry_delay_seconds if signal_epoch is not None else None
                if due_epoch is None or due_epoch <= cutoff_epoch:
                    out.append(symbol)
                    break
        return sorted(set(out))

    def active_sweep_clock_priority_block(self) -> dict[str, Any] | None:
        window_seconds = float(self.config.get("active_trade_state_clock_priority_window_seconds") or 25.0)
        now_epoch = time.time()
        for clock_epoch in sorted(self.clock_epochs):
            if self.clock_watermark is not None and clock_epoch <= self.clock_watermark:
                continue
            due_epoch = float(clock_epoch) + float(self.decision_delay_seconds)
            seconds_until_due = due_epoch - now_epoch
            if -1.0 <= seconds_until_due <= window_seconds:
                return {
                    "event": "active_position_loop_skipped",
                    "reason": "clock_priority_window",
                    "clock_epoch": int(clock_epoch),
                    "clock_time": epoch_ist_iso(int(clock_epoch)),
                    "decision_due_epoch": int(due_epoch),
                    "decision_due_time": epoch_ist_iso(int(due_epoch)),
                    "seconds_until_due": round(seconds_until_due, 3),
                    "window_seconds": window_seconds,
                    "recorded_at_ist": now_ist().isoformat(),
                }
        return None

    def load_guard_snapshot(self) -> dict[str, Any]:
        guard = self.config.get("load_guard") if isinstance(self.config.get("load_guard"), dict) else {}
        if not guard.get("enabled", True):
            return {"enabled": False, "overloaded": False}
        try:
            load1 = os.getloadavg()[0]
        except Exception:
            return {"enabled": True, "overloaded": False, "load1": None}
        threshold = float(guard.get("max_load1") or 6.5)
        return {"enabled": True, "overloaded": load1 >= threshold, "load1": load1, "threshold": threshold}

    def maybe_load_guard_sleep(self) -> None:
        guard = self.config.get("load_guard") if isinstance(self.config.get("load_guard"), dict) else {}
        if not bool(guard.get("sleep_enabled", True)):
            return
        snapshot = self.load_guard_snapshot()
        if snapshot.get("overloaded"):
            time.sleep(float(guard.get("sleep_seconds") or 2.0))

    def decision_catchup_block_reason(self) -> dict[str, Any] | None:
        if not bool(self.config.get("defer_decisions_until_stream_caught_up", False)):
            return None
        report = dict(self.latest_tail_report or {})
        if report.get("truncated"):
            return {
                "reason": "stream_tail_truncated",
                "latest_tail_report": report,
            }
        max_age = float(self.config.get("decision_catchup_max_feed_age_seconds") or self.signal_quote_max_age)
        feed_age = (time.time() - self.latest_feed_epoch) if self.latest_feed_epoch else None
        if feed_age is None:
            return {
                "reason": "no_feed_seen",
                "latest_tail_report": report,
            }
        if feed_age > max_age:
            return {
                "reason": "feed_not_caught_up",
                "feed_latest_age_seconds": feed_age,
                "max_age_seconds": max_age,
                "latest_tail_report": report,
            }
        return None

    def latest_market_clock_epoch(self, now_epoch: float | None = None) -> int | None:
        anchor = now_epoch if now_epoch is not None else time.time()
        due = [epoch for epoch in self.clock_epochs if epoch <= anchor]
        return max(due) if due else None

    def clock_metric_coverage_snapshot(self) -> dict[str, Any]:
        clock_epoch = self.latest_market_clock_epoch()
        if clock_epoch is None:
            return {"clock_epoch": None, "clock_label": None}
        started_epoch = int(self.started_at.timestamp())
        coverage_applicable = int(clock_epoch) >= started_epoch
        target_metric_ready = sum(1 for state in self.states.values() if int(clock_epoch) in state.metric_by_clock_epoch)
        signal_ready_symbols: list[str] = []
        signal_missing_symbols: list[str] = []
        execution_ready_symbols: list[str] = []
        execution_missing_symbols: list[str] = []
        for symbol, meta in self.instruments.items():
            signal_state = self.states.get(meta.signal_key)
            execution_state = self.states.get(meta.execution_key)
            if signal_state is not None and int(clock_epoch) in signal_state.metric_by_clock_epoch:
                signal_ready_symbols.append(symbol)
            else:
                signal_missing_symbols.append(symbol)
            if execution_state is not None and int(clock_epoch) in execution_state.metric_by_clock_epoch:
                execution_ready_symbols.append(symbol)
            else:
                execution_missing_symbols.append(symbol)
        return {
            "clock_epoch": clock_epoch,
            "clock_label": self.clock_label(clock_epoch),
            "clock_time_ist": epoch_ist_iso(clock_epoch),
            "coverage_applicable": coverage_applicable,
            "runner_started_after_clock": not coverage_applicable,
            "target_metric_ready": target_metric_ready,
            "target_keys": len(self.targets),
            "signal_symbols_ready": len(signal_ready_symbols),
            "execution_symbols_ready": len(execution_ready_symbols),
            "symbols": len(self.instruments),
            "signal_missing_symbols": signal_missing_symbols[:20],
            "execution_missing_symbols": execution_missing_symbols[:20],
        }

    def write_status(self) -> None:
        finalized_counts = [state.finalized_seconds for state in self.states.values()]
        retained_second_rows = [len(state.second_rows) for state in self.states.values()]
        target_ready = sum(1 for state in self.states.values() if state.last_finalized_second is not None)
        open_positions = 0
        stale_open_positions = 0
        active_counted_positions = 0
        for state_payload in self.model_states.values():
            if not isinstance(state_payload, dict):
                continue
            position = state_payload.get("position")
            if isinstance(position, dict):
                open_positions += 1
                if self.position_is_stale_rejected(position):
                    stale_open_positions += 1
                if self.position_counts_as_active(position):
                    active_counted_positions += 1
        symbol_ready = 0
        for meta in self.instruments.values():
            signal_state = self.states.get(meta.signal_key)
            execution_state = self.states.get(meta.execution_key)
            if (
                signal_state is not None
                and execution_state is not None
                and signal_state.last_finalized_second is not None
                and execution_state.last_finalized_second is not None
            ):
                symbol_ready += 1
        payload = {
            "ok": True,
            "passive_only": True,
            "strategy_id": self.strategy_id,
            "model_version": self.config.get("model_version"),
            "architecture_version": self.config.get("architecture_version"),
            "input_source": "target_stream" if self.consume_target_stream else "live_batches",
            "started_at_ist": self.started_at.isoformat(),
            "updated_at_ist": now_ist().isoformat(),
            "symbols": len(self.instruments),
            "symbols_ready": symbol_ready,
            "target_keys": len(self.targets),
            "target_keys_ready": target_ready,
            "open_positions": open_positions,
            "stale_open_positions": stale_open_positions,
            "active_counted_positions": active_counted_positions,
            "bootstrap": self.bootstrap_report,
            "rows_seen": self.rows_seen,
            "quotes_seen": self.quotes_seen,
            "feed_latest_age_seconds": (time.time() - self.latest_feed_epoch) if self.latest_feed_epoch else None,
            "partial_live_start": self.partial_live_start,
            "decisions_suppressed": self.partial_live_start
            and bool(self.config.get("suppress_decisions_on_partial_live_start", True)),
            "last_evaluated_clock": epoch_ist_iso(self.last_actual_evaluated_clock) if self.last_actual_evaluated_clock else None,
            "clock_watermark": epoch_ist_iso(self.clock_watermark) if self.clock_watermark else None,
            "skipped_startup_due_clock": epoch_ist_iso(self.skipped_startup_due_clock) if self.skipped_startup_due_clock else None,
            "latest_tail_report": self.latest_tail_report,
            "latest_decision_report": self.latest_decision_report,
            "latest_transition_signal_report": self.latest_transition_signal_report,
            "latest_rollover_report": self.latest_rollover_report,
            "latest_trade_state_report": self.latest_trade_state_report,
            "latest_retention_report": self.latest_retention_report,
            "latest_memory_pressure_report": self.latest_memory_pressure_report,
            "load_guard": self.load_guard_snapshot(),
            "clock_metric_coverage": self.clock_metric_coverage_snapshot(),
            "state": {
                "finalized_seconds_min": min(finalized_counts) if finalized_counts else 0,
                "finalized_seconds_median": sorted(finalized_counts)[len(finalized_counts) // 2] if finalized_counts else 0,
                "finalized_seconds_max": max(finalized_counts) if finalized_counts else 0,
                "retained_second_rows_total": sum(retained_second_rows),
                "retained_second_rows_max": max(retained_second_rows) if retained_second_rows else 0,
                "retained_second_rows_median": sorted(retained_second_rows)[len(retained_second_rows) // 2]
                if retained_second_rows
                else 0,
                "v1_runtime_symbols": sum(1 for item in self.instruments.values() if item.source == "v1_runtime"),
                "synthesized_symbols": sum(1 for item in self.instruments.values() if item.synthesized),
            },
            "logs": {
                "telemetry": str(self.telemetry_path),
                "decision_events_root": str(self.decision_events_root),
                "target_stream_root": str(self.target_stream_root()),
            },
        }
        atomic_write_json(self.status_path, payload)

    def request_stop(self, _signum: int, _frame: Any) -> None:
        self.stop_requested = True

    def run(self, *, max_runtime_seconds: float | None = None) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        last_status = 0.0
        deadline = time.time() + max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
        self.run_deadline_epoch = deadline
        while not self.stop_requested:
            if deadline is not None and time.time() >= deadline:
                break
            loop_start = time.perf_counter()
            trade_date = now_ist().date().isoformat()
            if self.partial_live_start and self.partial_live_start_trade_date != trade_date:
                self.partial_live_start = False
                self.partial_live_start_trade_date = None
            self.refresh_dynamic_retention()
            self.latest_tail_report = self.tail_target_stream(trade_date) if self.consume_target_stream else self.tail_live_batches(trade_date)
            decisions_suppressed = self.partial_live_start and bool(
                self.config.get("suppress_decisions_on_partial_live_start", True)
            )
            if decisions_suppressed:
                now_mono = time.monotonic()
                report = {
                    "event": "passive_decisions_suppressed",
                    "reason": "partial_live_start",
                    "trade_date": trade_date,
                    "latest_tail_report": self.latest_tail_report,
                    "recorded_at_ist": now_ist().isoformat(),
                }
                if now_mono - self._last_suppressed_decision_telemetry_monotonic >= self.telemetry_write_seconds:
                    append_jsonl(self.telemetry_path, report)
                    self._last_suppressed_decision_telemetry_monotonic = now_mono
                self.latest_decision_report = report
            else:
                catchup_block = self.decision_catchup_block_reason()
                if catchup_block is not None:
                    now_mono = time.monotonic()
                    report = {
                        "event": "decisions_deferred_stream_catchup",
                        "trade_date": trade_date,
                        **catchup_block,
                        "recorded_at_ist": now_ist().isoformat(),
                    }
                    if now_mono - self._last_catchup_defer_telemetry_monotonic >= self.telemetry_write_seconds:
                        append_jsonl(self.telemetry_path, report)
                        self._last_catchup_defer_telemetry_monotonic = now_mono
                    self.latest_decision_report = report
                else:
                    self.evaluate_rollovers(trade_date)
                    evaluated_clocks = self.evaluate_due_clocks(trade_date)
                    transition_report = self.evaluate_transition_signals(trade_date)
                    transition_entry_symbols = sorted(
                        {
                            str(symbol)
                            for symbol in transition_report.get("event_symbols") or []
                            if symbol
                        }
                    )
                    if evaluated_clocks:
                        all_symbols = bool(self.config.get("trade_state_all_symbols_on_clock", False))
                        entry_symbols = sorted(set(self.latest_due_clock_event_symbols) | set(transition_entry_symbols))
                        active_symbols = self.active_trade_state_symbols(include_transition_watch=False)
                        scoped_symbols = sorted(set(active_symbols) | set(entry_symbols))
                        if all_symbols:
                            self.evaluate_frozen_trade_state(
                                trade_date,
                                symbols=None,
                                reason="model_clock",
                            )
                        elif scoped_symbols:
                            if entry_symbols:
                                self.evaluate_frozen_trade_state(
                                    trade_date,
                                    symbols=entry_symbols,
                                    reason="model_clock_entry_symbols",
                                )
                                pending_due_symbols = self.pending_entry_due_symbols(
                                    lookahead_seconds=float(
                                        self.config.get("pending_entry_fill_priority_lookahead_seconds") or 2.0
                                    )
                                )
                                if pending_due_symbols:
                                    self.evaluate_frozen_trade_state(
                                        trade_date,
                                        symbols=pending_due_symbols,
                                        reason="entry_fill_priority",
                                    )
                            remaining_symbols = sorted(set(scoped_symbols) - set(entry_symbols))
                            if remaining_symbols:
                                self.evaluate_frozen_trade_state(
                                    trade_date,
                                    symbols=remaining_symbols,
                                    reason="model_clock_active_symbols",
                                )
                        else:
                            report = {
                                "event": "frozen_v1_trade_state_evaluation",
                                "reason": "model_clock_no_relevant_symbols",
                                "symbols_requested": 0,
                                "symbols_updated": 0,
                                "events": 0,
                                "event_samples": [],
                                "skipped_count": 0,
                                "skipped_samples": [],
                                "duration_seconds": 0.0,
                                "recorded_at_ist": now_ist().isoformat(),
                            }
                            append_jsonl(self.telemetry_path, report)
                            self.latest_trade_state_report = report
                    else:
                        if transition_entry_symbols:
                            self.evaluate_frozen_trade_state(
                                trade_date,
                                symbols=transition_entry_symbols,
                                reason="transition_entry_symbols",
                            )
                        pending_due_symbols = self.pending_entry_due_symbols(
                            lookahead_seconds=float(self.config.get("pending_entry_fill_priority_lookahead_seconds") or 2.0)
                        )
                        if pending_due_symbols:
                            self.evaluate_frozen_trade_state(
                                trade_date,
                                symbols=pending_due_symbols,
                                reason="entry_fill_priority",
                            )
                        priority_block = self.active_sweep_clock_priority_block()
                        if priority_block is not None:
                            append_jsonl(self.telemetry_path, priority_block)
                            self.latest_trade_state_report = priority_block
                        else:
                            active_symbols = self.active_trade_state_symbols(include_transition_watch=False)
                            active_symbols = sorted(set(active_symbols) - set(pending_due_symbols))
                            if active_symbols and self.should_evaluate_active_trade_state():
                                self.evaluate_frozen_trade_state(
                                    trade_date,
                                    symbols=active_symbols,
                                    reason="active_position_loop",
                                )
            retention_report = self.refresh_dynamic_retention()
            self.latest_retention_report = retention_report
            if time.time() - last_status >= self.status_write_seconds:
                self.write_status()
                last_status = time.time()
            self.maybe_load_guard_sleep()
            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0.1, self.poll_seconds - elapsed))
        self.run_deadline_epoch = None
        self.write_status()


class ObvTargetStreamExtractor:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.config = read_json(config_path, {})
        self.state_dir = Path(str(self.config["state_dir"]))
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            local_state = Path(str(self.config.get("state_dir_local") or ""))
            if not local_state:
                raise
            self.state_dir = local_state
            self.state_dir.mkdir(parents=True, exist_ok=True)
        self.producer_root = Path(str(self.config["producer_root"]))
        self.pointer_path = self.state_dir / "target_stream_extractor_pointer.json"
        self.status_path = self.state_dir / "target_stream_status.json"
        self.market_start = parse_hhmmss(str(self.config.get("market_start_ist") or "09:15:00"))
        self.market_end = parse_hhmmss(str(self.config.get("market_end_ist") or "15:30:00"))
        self.tail_max_bytes = int(
            self.config.get("target_stream_extractor_tail_max_bytes_per_cycle")
            or self.config.get("tail_max_bytes_per_cycle")
            or 12_000_000
        )
        self.poll_seconds = float(self.config.get("target_stream_extractor_poll_seconds") or self.config.get("poll_seconds") or 1.0)
        self.status_write_seconds = float(
            self.config.get("target_stream_extractor_status_write_seconds") or self.config.get("status_write_seconds") or 5.0
        )
        self.target_set = self.load_target_keys()
        self.target_symbol_count = self.load_target_symbol_count()
        self.session_id = f"{int(time.time())}-{os.getpid()}"
        self.started_at = now_ist()
        self.stop_requested = False
        self.rows_seen = 0
        self.quotes_written = 0
        self.latest_quote_received_epoch: float | None = None
        self.run_deadline_epoch: float | None = None

    def load_target_keys(self) -> set[str]:
        v1_config = read_json(
            resolve_config_path(self.config, "obvfut_v1_runtime_path", "obvfut_v1_runtime_path_local"),
            {},
        )
        manifest = read_json(
            resolve_config_path(self.config, "hurst_universe_manifest_path", "hurst_universe_manifest_path_local"),
            {},
        )
        out: set[str] = set()
        v1_by_symbol = {str(item.get("id") or item.get("symbol")): item for item in v1_config.get("instruments", [])}
        valid_synthesized_shadow_keys = load_key_manifest(
            self.config,
            "synthesized_shadow_keys_path",
            "synthesized_shadow_keys_path_local",
        )
        contract_chain_manifest = load_contract_chain_manifest(self.config)
        require_shadow_manifest = bool(self.config.get("require_synthesized_shadow_key_in_manifest", False))
        for entry in manifest.get("entries") or []:
            symbol = str(entry.get("symbol") or "")
            item = v1_by_symbol.get(symbol)
            if item:
                cash_key = item.get("cash_instrument_key") or entry.get("cash_key")
                if cash_key:
                    out.add(str(cash_key))
                chain = merge_contract_chain_with_manifest(
                    [dict(contract) for contract in ((item.get("contract_lifecycle") or {}).get("contracts", []) or []) if isinstance(contract, dict)],
                    contract_chain_manifest.get(symbol),
                )
                for contract in chain:
                    key = contract.get("instrument_key")
                    if key:
                        out.add(str(key))
                continue
            for key in (entry.get("cash_key"), entry.get("fut_key")):
                if key:
                    out.add(str(key))
            sep_fut_key = synthesized_september_future_key(str(entry.get("fut_key") or ""))
            if require_shadow_manifest and sep_fut_key not in valid_synthesized_shadow_keys:
                sep_fut_key = None
            if sep_fut_key:
                out.add(sep_fut_key)
            for contract in contract_chain_manifest.get(symbol, []):
                key = contract.get("instrument_key")
                if key:
                    out.add(str(key))
        return out

    def load_target_symbol_count(self) -> int:
        manifest = read_json(
            resolve_config_path(self.config, "hurst_universe_manifest_path", "hurst_universe_manifest_path_local"),
            {},
        )
        return len([entry for entry in manifest.get("entries") or [] if isinstance(entry, dict)])

    def live_batches_path(self, trade_date: str) -> Path:
        return self.producer_root / "live_batches" / trade_date / f"batches_{trade_date}.jsonl"

    def target_stream_root(self) -> Path:
        primary = Path(str(self.config.get("target_stream_root") or (self.state_dir / "target_stream")))
        local = Path(str(self.config.get("target_stream_root_local") or ""))
        configured_state = Path(str(self.config.get("state_dir") or ""))
        if local and self.state_dir != configured_state:
            return local
        return primary

    def target_stream_path(self, trade_date: str) -> Path:
        return self.target_stream_root() / trade_date / f"target_quotes_{trade_date}.jsonl"

    def is_market_hours_now(self) -> bool:
        current = now_ist().time()
        start = current.replace(hour=self.market_start[0], minute=self.market_start[1], second=self.market_start[2], microsecond=0)
        end = current.replace(hour=self.market_end[0], minute=self.market_end[1], second=self.market_end[2], microsecond=0)
        return start <= current <= end

    def start_at_live_eof_pointer(self, trade_date: str, reason: str) -> dict[str, Any] | None:
        if not bool(self.config.get("target_stream_extractor_start_at_eof_on_market_restart", True)):
            return None
        if not self.is_market_hours_now():
            return None
        path = self.live_batches_path(trade_date)
        if not path.exists():
            return None
        size = path.stat().st_size
        backfill = max(
            0,
            int(
                self.config.get("target_stream_extractor_restart_backfill_bytes")
                or self.config.get("market_restart_backfill_bytes")
                or 12_000_000
            ),
        )
        return {
            "trade_date": trade_date,
            "offset": max(0, size - backfill),
            "file_size": size,
            "live_batches_path": str(path),
            "updated_at_ist": now_ist().isoformat(),
            "runner_session_id": self.session_id,
            "reset_reason": reason,
            "live_safe_start": True,
            "restart_backfill_bytes": backfill,
        }

    def read_pointer(self, trade_date: str) -> dict[str, Any]:
        payload = read_json(self.pointer_path, {})
        if payload.get("trade_date") != trade_date:
            live_eof = self.start_at_live_eof_pointer(trade_date, "new_trade_date_market_live_eof")
            return live_eof if live_eof is not None else {"trade_date": trade_date, "offset": 0}
        if bool(self.config.get("reset_target_stream_extractor_pointer_on_start", False)) and payload.get("runner_session_id") != self.session_id:
            live_eof = self.start_at_live_eof_pointer(trade_date, "new_runner_session_market_live_eof")
            if live_eof is not None:
                return live_eof
            return {"trade_date": trade_date, "offset": 0, "reset_reason": "new_runner_session"}
        return dict(payload)

    def write_pointer(self, payload: dict[str, Any]) -> None:
        atomic_write_json(self.pointer_path, payload)

    @staticmethod
    def compact_row(row: dict[str, Any]) -> dict[str, Any]:
        return compact_target_stream_row_from_normalized(row)

    def tail_once(self, trade_date: str) -> dict[str, Any]:
        started = time.perf_counter()
        path = self.live_batches_path(trade_date)
        pointer = self.read_pointer(trade_date)
        offset = int(pointer.get("offset") or 0)
        if not path.exists():
            return {
                "source": "live_batches",
                "target": "target_stream",
                "exists": False,
                "path": str(path),
                "rows": 0,
                "quotes": 0,
                "duration_seconds": round(time.perf_counter() - started, 4),
            }
        size = path.stat().st_size
        if offset > size:
            offset = 0
        rows = 0
        quotes = 0
        new_offset = offset
        truncated = False
        compact_rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(offset)
            while True:
                pos = handle.tell()
                if self.run_deadline_epoch is not None and time.time() >= self.run_deadline_epoch:
                    new_offset = pos
                    truncated = True
                    break
                if pos > offset and pos - offset >= self.tail_max_bytes:
                    new_offset = pos
                    truncated = True
                    break
                line = handle.readline()
                if not line:
                    new_offset = handle.tell()
                    break
                if not line.endswith("\n"):
                    new_offset = pos
                    break
                rows += 1
                for item in target_items_from_batch_line(line, self.target_set):
                    key = str(item["instrument_key"])
                    row = normalise_record(item, trade_date, key)
                    if row is None:
                        continue
                    compact_rows.append(self.compact_row(row))
                    received_epoch = as_float(row.get("received_epoch")) or as_float(row.get("epoch"))
                    if received_epoch is not None:
                        self.latest_quote_received_epoch = max(self.latest_quote_received_epoch or received_epoch, received_epoch)
                    quotes += 1
        if compact_rows:
            append_jsonl_many(self.target_stream_path(trade_date), compact_rows)
        self.rows_seen += rows
        self.quotes_written += quotes
        self.write_pointer(
            {
                "trade_date": trade_date,
                "offset": new_offset,
                "file_size": size,
                "live_batches_path": str(path),
                "target_stream_path": str(self.target_stream_path(trade_date)),
                "updated_at_ist": now_ist().isoformat(),
                "runner_session_id": self.session_id,
            }
        )
        return {
            "source": "live_batches",
            "target": "target_stream",
            "exists": True,
            "path": str(path),
            "target_stream_path": str(self.target_stream_path(trade_date)),
            "offset": offset,
            "new_offset": new_offset,
            "file_size": size,
            "rows": rows,
            "quotes": quotes,
            "bytes_read": max(0, new_offset - offset),
            "truncated": truncated,
            "duration_seconds": round(time.perf_counter() - started, 4),
        }

    def write_status(self, tail_report: dict[str, Any] | None = None) -> None:
        payload = {
            "ok": True,
            "schema": "obvfutport_v2.target_stream_status.v1",
            "updated_at_ist": now_ist().isoformat(),
            "started_at_ist": self.started_at.isoformat(),
            "symbols": self.target_symbol_count,
            "target_keys": len(self.target_set),
            "rows_seen": self.rows_seen,
            "quotes_written": self.quotes_written,
            "latest_quote_age_seconds": (time.time() - self.latest_quote_received_epoch) if self.latest_quote_received_epoch else None,
            "tail_report": tail_report,
            "load1": os.getloadavg()[0] if hasattr(os, "getloadavg") else None,
            "target_stream_root": str(self.target_stream_root()),
        }
        atomic_write_json(self.status_path, payload)

    def request_stop(self, _signum: int, _frame: Any) -> None:
        self.stop_requested = True

    def run(self, *, max_runtime_seconds: float | None = None) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        last_status = 0.0
        deadline = time.time() + max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
        self.run_deadline_epoch = deadline
        while not self.stop_requested:
            if deadline is not None and time.time() >= deadline:
                break
            loop_start = time.perf_counter()
            trade_date = now_ist().date().isoformat()
            tail_report = self.tail_once(trade_date)
            if time.time() - last_status >= self.status_write_seconds:
                self.write_status(tail_report)
                last_status = time.time()
            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0.1, self.poll_seconds - elapsed))
        self.run_deadline_epoch = None
        self.write_status({"stopped": True})


def build_target_stream_packs(
    config_path: Path,
    start_date: str,
    end_date: str,
    *,
    output_root: Path | None = None,
    overwrite: bool = False,
    max_runtime_seconds: float | None = None,
) -> dict[str, Any]:
    cfg = read_json(config_path, {})
    if output_root is not None:
        cfg["target_stream_root"] = str(output_root)
        cfg["target_stream_root_local"] = str(output_root)
    tmp_config = Path("/tmp") / f"obvfutport_v2_target_stream_build_{os.getpid()}_{time.time_ns()}.json"
    atomic_write_json(tmp_config, cfg)
    extractor = ObvTargetStreamExtractor(tmp_config)
    target_set = set(extractor.target_set)
    target_bytes = {key.encode("utf-8") for key in target_set}
    deadline = time.time() + max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
    reports: list[dict[str, Any]] = []
    partial = False
    started = time.perf_counter()
    for trade_date in date_range(start_date, end_date):
        if date.fromisoformat(trade_date).weekday() >= 5:
            reports.append(
                {
                    "trade_date": trade_date,
                    "skipped": True,
                    "skip_reason": "weekend",
                    "target_keys": len(target_set),
                }
            )
            continue
        output_path = extractor.target_stream_path(trade_date)
        existing_size = output_path.stat().st_size if output_path.exists() else 0
        if output_path.exists() and not overwrite:
            reports.append(
                {
                    "trade_date": trade_date,
                    "skipped": True,
                    "skip_reason": "target_stream_exists",
                    "target_stream_path": str(output_path),
                    "target_stream_bytes": int(existing_size),
                    "target_keys": len(target_set),
                }
            )
            continue
        source_path: Path | None = None
        source_type = ""
        for path, candidate_type in candidate_history_sources(cfg, trade_date):
            if path.exists():
                source_path = path
                source_type = candidate_type
                break
        if source_path is None:
            reports.append(
                {
                    "trade_date": trade_date,
                    "skipped": False,
                    "source_found": False,
                    "error": "no_archive_or_history_source_found",
                    "target_keys": len(target_set),
                }
            )
            continue

        day_started = time.perf_counter()
        rows_seen = 0
        quotes_written = 0
        keys_seen: set[str] = set()
        batch: list[dict[str, Any]] = []
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_name(f".{output_path.name}.tmp-{os.getpid()}-{time.time_ns()}")

        def flush() -> None:
            nonlocal batch
            if not batch:
                return
            append_jsonl_many(tmp_path, batch)
            batch = []

        try:
            if source_type == "archive_tar":
                line_iter = iter_archive_payload_lines(source_path, trade_date)
            elif source_type == "history_gzip":
                line_iter = gzip.open(source_path, "rb")
            else:
                line_iter = source_path.open("rb")
            with line_iter if hasattr(line_iter, "__enter__") else nullcontext(line_iter) as raw_lines:
                for raw_line in raw_lines:
                    if deadline is not None and time.time() >= deadline:
                        partial = True
                        break
                    if not raw_line.strip():
                        continue
                    rows_seen += 1
                    for item in target_items_from_raw_line(raw_line, target_set, target_bytes):
                        item_key = str(item.get("instrument_key") or "")
                        if item_key not in target_set:
                            continue
                        row = normalise_record(item, trade_date, item_key)
                        if row is None:
                            continue
                        if epoch_ist_date(row.get("epoch")) != trade_date:
                            continue
                        batch.append(compact_target_stream_row_from_normalized(row))
                        keys_seen.add(item_key)
                        quotes_written += 1
                        if len(batch) >= 25_000:
                            flush()
                flush()
        finally:
            try:
                if tmp_path.exists() and (partial or quotes_written <= 0):
                    tmp_path.unlink()
            except OSError:
                pass
        if partial:
            reports.append(
                {
                    "trade_date": trade_date,
                    "partial": True,
                    "source": str(source_path),
                    "source_type": source_type,
                    "target_stream_tmp_path": str(tmp_path),
                    "rows_seen": rows_seen,
                    "quotes_written": quotes_written,
                    "keys_seen": len(keys_seen),
                    "target_keys": len(target_set),
                    "coverage_ratio": (len(keys_seen) / len(target_set)) if target_set else 0.0,
                    "duration_seconds": round(time.perf_counter() - day_started, 4),
                }
            )
            break
        if quotes_written > 0 and tmp_path.exists():
            tmp_path.replace(output_path)
        reports.append(
            {
                "trade_date": trade_date,
                "partial": False,
                "source_found": True,
                "source": str(source_path),
                "source_type": source_type,
                "target_stream_path": str(output_path),
                "target_stream_bytes": output_path.stat().st_size if output_path.exists() else 0,
                "rows_seen": rows_seen,
                "quotes_written": quotes_written,
                "keys_seen": len(keys_seen),
                "target_keys": len(target_set),
                "coverage_ratio": (len(keys_seen) / len(target_set)) if target_set else 0.0,
                "missing_key_samples": sorted(target_set - keys_seen)[:25],
                "duration_seconds": round(time.perf_counter() - day_started, 4),
            }
        )
        if partial:
            break
    status = {
        "ok": not partial and all((item.get("skipped") or item.get("source_found")) for item in reports),
        "partial": partial,
        "mode": "build_target_stream_packs",
        "start_date": start_date,
        "end_date": end_date,
        "target_stream_root": str(extractor.target_stream_root()),
        "target_keys": len(target_set),
        "reports": reports,
        "duration_seconds": round(time.perf_counter() - started, 4),
        "updated_at_ist": now_ist().isoformat(),
    }
    atomic_write_json(extractor.state_dir / "target_stream_build_status.json", status)
    print(json.dumps(json_clean(status), indent=2, sort_keys=True))
    return status


def run_smoke(config_path: Path, output_dir: Path) -> None:
    cfg = read_json(config_path, {})
    cfg["state_dir"] = str(output_dir / "state")
    cfg["target_stream_root"] = str(output_dir / "state" / "target_stream")
    cfg["producer_root"] = str(output_dir / "producer")
    cfg["skip_past_due_clocks_on_start"] = False
    cfg["start_at_eof_on_market_restart"] = False
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp_config = output_dir / "runtime.json"
    atomic_write_json(tmp_config, cfg)
    runner = PassiveV2Runner(tmp_config)
    runner.add_clock_epochs_for_trade_date("2026-08-17")
    key = next(iter(runner.targets))
    state = runner.states[key]
    base = datetime(2026, 8, 17, 9, 15, tzinfo=IST)
    volume = 1000.0
    price = 100.0
    for idx in range(5105):
        epoch = int(base.timestamp()) + idx
        if idx % 7 == 0:
            price += 0.05
        volume += 10
        row = {
            "trade_date": "2026-08-17",
            "target": key,
            "epoch": float(epoch),
            "epoch_second": epoch,
            "received_at_ist": epoch_ist_iso(epoch),
            "exchange_timestamp": epoch_ist_iso(epoch),
            "received_epoch": float(epoch) + 0.2,
            "market_data_latency_seconds": 0.2,
            "price": price,
            "volume_traded": volume,
            "bid": price - 0.01,
            "ask": price + 0.01,
            "spread": 0.02,
        }
        state.process_row(row)
    state.flush_until_latest()
    clock_epoch = int(base.timestamp()) + 4800
    row, reason = state.build_clock_row(clock_epoch, "10:35", {})
    if row is None:
        raise RuntimeError(f"smoke clock row missing: {reason}")
    runner.write_status()


def _event_position(event: dict[str, Any]) -> dict[str, Any]:
    position = event.get("position")
    return dict(position) if isinstance(position, dict) else {}


def normalized_trade_event_key(symbol: str, event: dict[str, Any]) -> str:
    position = _event_position(event)
    payload = {
        "symbol": symbol,
        "event": event.get("event"),
        "side": event.get("side") or position.get("side"),
        "source": event.get("source") or position.get("source"),
        "signal_epoch": event.get("signal_epoch") or position.get("signal_epoch"),
        "entry_epoch": event.get("entry_epoch") or position.get("entry_epoch"),
        "exit_epoch": event.get("exit_epoch"),
        "exit_reason": event.get("exit_reason"),
        "rollover_id": event.get("rollover_id") or position.get("source_rollover_id"),
        "instrument_key": event.get("instrument_key") or position.get("instrument_key"),
        "contract_label": event.get("contract_label") or position.get("contract_label"),
    }
    return json.dumps(json_clean(payload), sort_keys=True, separators=(",", ":"))


def collect_ledger_event_keys(state_root: Path, symbols: Iterable[str]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    tracked_events = {
        "paper_entry",
        "paper_exit",
        "paper_rollover",
        "tranche2_exit",
        "tranche3_entry",
        "tranche3_exit",
    }
    for symbol in symbols:
        path = state_root / "instruments" / safe_key(symbol) / "ledger.jsonl"
        keys: set[str] = set()
        for event in iter_jsonl(path):
            if event.get("event") in tracked_events:
                keys.add(normalized_trade_event_key(str(symbol), event))
        out[str(symbol)] = keys
    return out


def compare_v1_ledgers(config_path: Path, v1_state_root: Path, output_path: Path | None = None) -> dict[str, Any]:
    runner = PassiveV2Runner(config_path)
    v2_state_root = runner.state_dir
    symbols = sorted(runner.instruments)
    v1_keys = collect_ledger_event_keys(v1_state_root, symbols)
    v2_keys = collect_ledger_event_keys(v2_state_root, symbols)
    by_symbol: dict[str, Any] = {}
    missing_total = 0
    extra_total = 0
    matched_total = 0
    for symbol in symbols:
        missing = sorted(v1_keys.get(symbol, set()) - v2_keys.get(symbol, set()))
        extra = sorted(v2_keys.get(symbol, set()) - v1_keys.get(symbol, set()))
        matched = len(v1_keys.get(symbol, set()) & v2_keys.get(symbol, set()))
        missing_total += len(missing)
        extra_total += len(extra)
        matched_total += matched
        if missing or extra:
            by_symbol[symbol] = {
                "matched": matched,
                "missing_from_v2_count": len(missing),
                "extra_in_v2_count": len(extra),
                "missing_from_v2_samples": missing[:10],
                "extra_in_v2_samples": extra[:10],
            }
    report = {
        "schema": "obvfutport_v2.v1_ledger_parity_report.v1",
        "strategy_id": runner.strategy_id,
        "model_version": runner.config.get("model_version"),
        "architecture_version": runner.config.get("architecture_version"),
        "created_at_ist": now_ist().isoformat(),
        "symbols": len(symbols),
        "v1_state_root": str(v1_state_root),
        "v2_state_root": str(v2_state_root),
        "matched_events": matched_total,
        "missing_from_v2": missing_total,
        "extra_in_v2": extra_total,
        "status": "pass" if missing_total == 0 and extra_total == 0 else "fail",
        "by_symbol": by_symbol,
        "promotion_gate": "blocked" if missing_total or extra_total else "allowed_for_next_non_live_gate",
    }
    if output_path is not None:
        atomic_write_json(output_path, report)
    return report


def date_range(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    out: list[str] = []
    current = start
    while current <= end:
        out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def bootstrap_readiness_summary(runner: PassiveV2Runner) -> dict[str, Any]:
    target_ready = {
        key: state.last_finalized_second is not None
        for key, state in runner.states.items()
    }
    missing_targets = [key for key, ready in target_ready.items() if not ready]
    symbol_missing: list[dict[str, Any]] = []
    symbol_ready = 0
    for symbol, meta in runner.instruments.items():
        required = [key for key in {meta.signal_key, meta.execution_key} if key]
        missing = [key for key in required if not target_ready.get(key)]
        if missing:
            symbol_missing.append({"symbol": symbol, "missing_required_keys": missing})
        else:
            symbol_ready += 1
    finalized = [int(state.last_finalized_second) for state in runner.states.values() if state.last_finalized_second is not None]
    return {
        "symbols": len(runner.instruments),
        "symbols_ready": symbol_ready,
        "symbols_missing": len(symbol_missing),
        "symbol_missing_samples": symbol_missing[:25],
        "target_keys": len(runner.targets),
        "target_keys_ready": len(runner.targets) - len(missing_targets),
        "target_keys_missing": len(missing_targets),
        "target_missing_samples": missing_targets[:25],
        "target_coverage_ratio": ((len(runner.targets) - len(missing_targets)) / len(runner.targets)) if runner.targets else 0.0,
        "last_finalized_min": min(finalized) if finalized else None,
        "last_finalized_max": max(finalized) if finalized else None,
        "last_finalized_min_time": epoch_ist_iso(min(finalized)) if finalized else None,
        "last_finalized_max_time": epoch_ist_iso(max(finalized)) if finalized else None,
    }


def ingest_bootstrap_records(
    runner: PassiveV2Runner,
    trade_date: str,
    records: Iterable[dict[str, Any]],
    source_report: dict[str, Any],
    *,
    deadline: float | None,
) -> tuple[dict[str, Any], bool]:
    target_set = set(runner.targets)
    rows_seen = 0
    quotes_used = 0
    partial = False
    started = time.perf_counter()
    for record in records:
        if deadline is not None and time.time() >= deadline:
            partial = True
            break
        rows_seen += 1
        key = str(record.get("instrument_key") or "")
        items: list[dict[str, Any]]
        if key in target_set:
            items = [record]
        else:
            try:
                items = target_items_from_batch_line(json.dumps(record), target_set)
            except Exception:
                items = []
        for item in items:
            item_key = str(item.get("instrument_key") or "")
            if item_key not in target_set:
                continue
            row = normalise_or_pass_record(item, trade_date, item_key)
            if row is None:
                continue
            runner.states[item_key].process_row(row)
            quotes_used += 1
            received_epoch = as_float(row.get("received_epoch")) or as_float(row.get("epoch"))
            if received_epoch is not None:
                runner.latest_feed_epoch = max(runner.latest_feed_epoch or received_epoch, received_epoch)
    for state in runner.states.values():
        state.flush_until_latest()
    return (
        {
            **source_report,
            "rows_seen": rows_seen,
            "quotes_used": quotes_used,
            "duration_seconds": round(time.perf_counter() - started, 4),
            "partial": partial,
        },
        partial,
    )


def ingest_target_stream_catchup(
    runner: PassiveV2Runner,
    trade_date: str,
    *,
    deadline: float | None,
    offset: int = 0,
    max_bytes: int | None = None,
) -> tuple[dict[str, Any], bool]:
    started = time.perf_counter()
    path = runner.target_stream_path(trade_date)
    if not path.exists():
        return (
            {
                "trade_date": trade_date,
                "source": str(path),
                "source_type": "target_stream_catchup",
                "source_found": False,
                "error": "target_stream_source_missing",
                "rows_seen": 0,
                "quotes_used": 0,
                "duration_seconds": round(time.perf_counter() - started, 4),
                "partial": False,
            },
            False,
        )
    rows_seen = 0
    quotes_used = 0
    partial = False
    size = path.stat().st_size
    target_set = set(runner.targets)
    new_offset = int(offset)
    next_progress_at = 1_000_000
    progress_path = runner.state_dir / "bootstrap_progress.json"
    with path.open("rb") as handle:
        if offset > 0:
            handle.seek(offset)
        start_offset = handle.tell()
        while True:
            pos = handle.tell()
            if deadline is not None and time.time() >= deadline:
                new_offset = pos
                partial = True
                break
            if max_bytes is not None and pos - start_offset >= max_bytes:
                new_offset = pos
                partial = True
                break
            line = handle.readline()
            if not line:
                new_offset = handle.tell()
                break
            if not line.endswith(b"\n"):
                new_offset = pos
                partial = True
                break
            rows_seen += 1
            row = row_from_target_stream_line(line, trade_date, target_set)
            if row is None:
                continue
            runner.states[str(row["target"])].process_row(row)
            quotes_used += 1
            received_epoch = as_float(row.get("received_epoch")) or as_float(row.get("epoch"))
            if received_epoch is not None:
                runner.latest_feed_epoch = max(runner.latest_feed_epoch or received_epoch, received_epoch)
            if rows_seen >= next_progress_at:
                new_offset = handle.tell()
                atomic_write_json(
                    progress_path,
                    {
                        "mode": "target_stream_catchup",
                        "trade_date": trade_date,
                        "source": str(path),
                        "rows_seen": rows_seen,
                        "quotes_used": quotes_used,
                        "offset": int(offset),
                        "new_offset": int(new_offset),
                        "file_size": int(size),
                        "pct_file": round(100.0 * int(new_offset) / int(size), 4) if size else None,
                        "duration_seconds": round(time.perf_counter() - started, 4),
                        "updated_at_ist": now_ist().isoformat(),
                    },
                )
                next_progress_at += 1_000_000
    for state in runner.states.values():
        state.flush_until_latest()
    return (
        {
            "trade_date": trade_date,
            "source": str(path),
            "source_type": "target_stream_catchup",
            "source_found": True,
            "offset": int(offset),
            "new_offset": int(new_offset),
            "file_size": int(size),
            "bytes_read": max(0, int(new_offset) - int(offset)),
            "rows_seen": rows_seen,
            "quotes_used": quotes_used,
            "duration_seconds": round(time.perf_counter() - started, 4),
            "partial": partial,
        },
        partial,
    )


def run_bootstrap(config_path: Path, start_date: str, end_date: str, *, max_runtime_seconds: float | None = None) -> dict[str, Any]:
    cfg = read_json(config_path, {})
    cfg["skip_past_due_clocks_on_start"] = False
    cfg["bootstrap_load_enabled"] = False
    cfg["second_row_retention_seconds"] = int(cfg.get("bootstrap_second_row_retention_seconds") or 0)
    cfg["compute_non_clock_percentiles"] = False
    tmp_config = Path("/tmp") / f"obvfutport_v2_bootstrap_{os.getpid()}_{time.time_ns()}.json"
    atomic_write_json(tmp_config, cfg)
    runner = PassiveV2Runner(tmp_config)
    deadline = time.time() + max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
    target_set = set(runner.targets)
    reports: list[dict[str, Any]] = []
    partial = False
    checkpoint_each_day = bool(cfg.get("bootstrap_checkpoint_each_day", True))
    for trade_date in date_range(start_date, end_date):
        runner.add_clock_epochs_for_trade_date(trade_date)
        records, source_report = iter_bootstrap_records(runner.config, trade_date, runner.targets)
        rows_seen = 0
        quotes_used = 0
        started = time.perf_counter()

        for record in records:
            if deadline is not None and time.time() >= deadline:
                partial = True
                break
            rows_seen += 1
            key = str(record.get("instrument_key") or "")
            items: list[dict[str, Any]]
            if key in target_set:
                items = [record]
            else:
                try:
                    items = target_items_from_batch_line(json.dumps(record), target_set)
                except Exception:
                    items = []
            for item in items:
                item_key = str(item.get("instrument_key") or item.get("target") or "")
                if item_key not in target_set:
                    continue
                row = normalise_or_pass_record(item, trade_date, item_key)
                if row is None:
                    continue
                runner.states[item_key].process_row(row)
                quotes_used += 1
                received_epoch = as_float(row.get("received_epoch")) or as_float(row.get("epoch"))
                if received_epoch is not None:
                    runner.latest_feed_epoch = max(runner.latest_feed_epoch or received_epoch, received_epoch)
        for state in runner.states.values():
            state.flush_until_latest()
        reports.append(
            {
                **source_report,
                "rows_seen": rows_seen,
                "quotes_used": quotes_used,
                "duration_seconds": round(time.perf_counter() - started, 4),
                "partial": partial,
            }
        )
        if partial:
            break
        if checkpoint_each_day:
            manifest = runner.save_bootstrap_states(trade_date, reports)
            atomic_write_json(
                runner.state_dir / "bootstrap_status.json",
                {
                    "ok": True,
                    "partial": False,
                    "checkpoint": True,
                    "start_date": start_date,
                    "end_date": end_date,
                    "as_of_date": trade_date,
                    "manifest": manifest,
                    "updated_at_ist": now_ist().isoformat(),
                },
            )
    status_path = runner.state_dir / "bootstrap_status.json"
    if partial:
        status = {
            "ok": False,
            "partial": True,
            "start_date": start_date,
            "end_date": end_date,
            "reports": reports,
            "updated_at_ist": now_ist().isoformat(),
            "reason": "max_runtime_seconds_reached",
        }
        atomic_write_json(status_path, status)
        print(json.dumps(json_clean(status), indent=2, sort_keys=True))
        return status
    manifest = runner.save_bootstrap_states(end_date, reports)
    status = {
        "ok": True,
        "partial": False,
        "start_date": start_date,
        "end_date": end_date,
        "manifest": manifest,
        "updated_at_ist": now_ist().isoformat(),
    }
    atomic_write_json(status_path, status)
    print(json.dumps(json_clean(status), indent=2, sort_keys=True))
    return status


def run_streaming_bootstrap(
    config_path: Path,
    start_date: str,
    end_date: str,
    *,
    use_target_stream_history: bool = False,
    skip_missing_weekends: bool = True,
    catchup_date: str | None = None,
    catchup_target_stream: bool = False,
    catchup_offset: int = 0,
    catchup_max_bytes: int | None = None,
    resume_date: str | None = None,
    max_runtime_seconds: float | None = None,
    min_target_coverage_ratio: float | None = None,
    promote_latest: bool = True,
) -> dict[str, Any]:
    cfg = read_json(config_path, {})
    cfg["skip_past_due_clocks_on_start"] = False
    cfg["bootstrap_load_enabled"] = bool(resume_date)
    if resume_date:
        cfg["bootstrap_load_date"] = str(resume_date)
    cfg["second_row_retention_seconds"] = int(cfg.get("bootstrap_second_row_retention_seconds") or 0)
    cfg["compute_non_clock_percentiles"] = False
    tmp_config = Path("/tmp") / f"obvfutport_v2_streaming_bootstrap_{os.getpid()}_{time.time_ns()}.json"
    atomic_write_json(tmp_config, cfg)
    runner = PassiveV2Runner(tmp_config)
    deadline = time.time() + max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
    reports: list[dict[str, Any]] = []
    missing_source_dates: list[str] = []
    partial = False
    for trade_date in date_range(start_date, end_date):
        runner.add_clock_epochs_for_trade_date(trade_date)
        if use_target_stream_history:
            stream_path = runner.target_stream_path(trade_date)
            if not stream_path.exists() and skip_missing_weekends and date.fromisoformat(trade_date).weekday() >= 5:
                reports.append(
                    {
                        "trade_date": trade_date,
                        "source": str(stream_path),
                        "source_type": "target_stream_history",
                        "source_found": False,
                        "skipped": True,
                        "skip_reason": "weekend",
                        "rows_seen": 0,
                        "quotes_used": 0,
                        "duration_seconds": 0.0,
                        "partial": False,
                    }
                )
                continue
            report, partial = ingest_target_stream_catchup(
                runner,
                trade_date,
                deadline=deadline,
                offset=0,
                max_bytes=None,
            )
            report["source_type"] = "target_stream_history"
            reports.append(report)
            if not report.get("source_found"):
                missing_source_dates.append(trade_date)
            atomic_write_json(
                runner.state_dir / "bootstrap_status.json",
                {
                    "ok": not partial,
                    "partial": partial,
                    "mode": "streaming_compact_bootstrap",
                    "history_source": "target_stream" if use_target_stream_history else "archive_or_history",
                    "start_date": start_date,
                    "end_date": end_date,
                    "latest_completed_date": trade_date,
                    "reports": reports,
                    "readiness": bootstrap_readiness_summary(runner),
                    "updated_at_ist": now_ist().isoformat(),
                },
            )
            if partial:
                break
            continue
        records, source_report = iter_bootstrap_records(runner.config, trade_date, runner.targets)
        if not source_report.get("source_found"):
            if skip_missing_weekends and date.fromisoformat(trade_date).weekday() >= 5:
                reports.append(
                    {
                        **source_report,
                        "skipped": True,
                        "skip_reason": "weekend",
                        "rows_seen": 0,
                        "quotes_used": 0,
                        "duration_seconds": 0.0,
                        "partial": False,
                    }
                )
                continue
            missing_source_dates.append(trade_date)
            reports.append(
                {
                    **source_report,
                    "rows_seen": 0,
                    "quotes_used": 0,
                    "duration_seconds": 0.0,
                    "partial": False,
                }
            )
            continue
        report, partial = ingest_bootstrap_records(runner, trade_date, records, source_report, deadline=deadline)
        reports.append(report)
        atomic_write_json(
            runner.state_dir / "bootstrap_status.json",
            {
                "ok": not partial,
                "partial": partial,
                "mode": "streaming_compact_bootstrap",
                "history_source": "target_stream" if use_target_stream_history else "archive_or_history",
                "start_date": start_date,
                "end_date": end_date,
                "latest_completed_date": trade_date,
                "reports": reports,
                "readiness": bootstrap_readiness_summary(runner),
                "updated_at_ist": now_ist().isoformat(),
            },
        )
        if partial:
            break
    if not partial and catchup_target_stream and catchup_date:
        runner.add_clock_epochs_for_trade_date(catchup_date)
        report, partial = ingest_target_stream_catchup(
            runner,
            catchup_date,
            deadline=deadline,
            offset=int(catchup_offset),
            max_bytes=catchup_max_bytes,
        )
        reports.append(report)
        if not report.get("source_found"):
            missing_source_dates.append(catchup_date)
    readiness = bootstrap_readiness_summary(runner)
    configured_min = as_float(cfg.get("streaming_bootstrap_min_target_coverage_ratio"))
    min_coverage = (
        float(min_target_coverage_ratio)
        if min_target_coverage_ratio is not None
        else float(configured_min if configured_min is not None else 0.98)
    )
    coverage_ok = float(readiness.get("target_coverage_ratio") or 0.0) >= min_coverage
    source_ok = not missing_source_dates
    ok = (not partial) and source_ok and coverage_ok
    as_of_date = str(catchup_date if catchup_target_stream and catchup_date else end_date)
    manifest = runner.save_bootstrap_states(
        as_of_date,
        reports,
        promote_latest=bool(promote_latest and ok),
        extra={
            "mode": "streaming_compact_bootstrap",
            "history_source": "target_stream" if use_target_stream_history else "archive_or_history",
            "bootstrap_start_date": start_date,
            "bootstrap_end_date": end_date,
            "catchup_date": catchup_date if catchup_target_stream else None,
            "catchup_target_stream": bool(catchup_target_stream),
            "catchup_offset": int(catchup_offset) if catchup_target_stream else None,
            "catchup_max_bytes": int(catchup_max_bytes) if catchup_target_stream and catchup_max_bytes is not None else None,
            "resume_date": resume_date,
            "promoted_latest": bool(promote_latest and ok),
            "validation": {
                "ok": ok,
                "partial": partial,
                "source_ok": source_ok,
                "missing_source_dates": missing_source_dates,
                "coverage_ok": coverage_ok,
                "min_target_coverage_ratio": min_coverage,
                "readiness": readiness,
            },
        },
    )
    status = {
        "ok": ok,
        "partial": partial,
        "mode": "streaming_compact_bootstrap",
        "history_source": "target_stream" if use_target_stream_history else "archive_or_history",
        "start_date": start_date,
        "end_date": end_date,
        "catchup_date": catchup_date if catchup_target_stream else None,
        "catchup_target_stream": bool(catchup_target_stream),
        "catchup_offset": int(catchup_offset) if catchup_target_stream else None,
        "catchup_max_bytes": int(catchup_max_bytes) if catchup_target_stream and catchup_max_bytes is not None else None,
        "resume_date": resume_date,
        "promoted_latest": bool(promote_latest and ok),
        "min_target_coverage_ratio": min_coverage,
        "missing_source_dates": missing_source_dates,
        "readiness": readiness,
        "manifest": manifest,
        "updated_at_ist": now_ist().isoformat(),
    }
    if partial:
        status["reason"] = "max_runtime_or_stream_limit_reached"
    elif not source_ok:
        status["reason"] = "missing_archive_or_stream_sources"
    elif not coverage_ok:
        status["reason"] = "target_coverage_below_minimum"
    atomic_write_json(runner.state_dir / "bootstrap_status.json", status)
    print(json.dumps(json_clean(status), indent=2, sort_keys=True))
    return status


def run_archive_replay(
    config_path: Path,
    start_date: str,
    end_date: str,
    *,
    output_state_dir: Path,
    use_target_stream_history: bool = False,
    skip_missing_weekends: bool = True,
    max_runtime_seconds: float | None = None,
    max_trade_state_passes: int = 20,
    output_path: Path | None = None,
) -> dict[str, Any]:
    cfg = read_json(config_path, {})
    cfg["state_dir"] = str(output_state_dir)
    cfg["state_dir_local"] = str(output_state_dir)
    cfg["target_stream_root"] = str(output_state_dir / "target_stream")
    cfg["target_stream_root_local"] = str(output_state_dir / "target_stream")
    cfg["skip_past_due_clocks_on_start"] = False
    cfg["bootstrap_load_enabled"] = False
    cfg["start_at_eof_on_market_restart"] = False
    replay_retention = cfg.get("archive_replay_second_row_retention_seconds")
    replay_retention_seconds = int(replay_retention) if replay_retention is not None else None
    cfg["second_row_retention_seconds"] = replay_retention_seconds
    cfg["flat_second_row_retention_seconds"] = replay_retention_seconds
    cfg["pending_second_row_retention_seconds"] = replay_retention_seconds
    cfg["active_second_row_retention_seconds"] = replay_retention_seconds
    cfg["transition_second_row_retention_seconds"] = replay_retention_seconds
    if bool(cfg.get("archive_replay_disable_live_stale_entry_marking", True)):
        cfg["max_entry_lag_seconds"] = None
    output_state_dir.mkdir(parents=True, exist_ok=True)
    tmp_config = output_state_dir / "runtime.archive_replay.json"
    atomic_write_json(tmp_config, cfg)
    runner = PassiveV2Runner(tmp_config)
    deadline = time.time() + max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
    target_set = set(runner.targets)
    date_reports: list[dict[str, Any]] = []
    partial = False
    event_time_checkpoints = bool(cfg.get("archive_replay_event_time_checkpoints_enabled", False))
    for trade_date in date_range(start_date, end_date):
        runner.add_clock_epochs_for_trade_date(trade_date)
        checkpoint_epochs: list[int] = []
        if event_time_checkpoints:
            day_clock_epochs = clock_epochs_for_day(
                date.fromisoformat(trade_date),
                clock_start=runner.clock_start,
                clock_end=runner.clock_end,
                clock_step_minutes=runner.clock_step_minutes,
            )
            entry_delay_seconds = int(runner.config.get("entry_delay_seconds") or 60)
            checkpoint_delays = {
                int(runner.decision_delay_seconds),
                int(entry_delay_seconds),
            }
            checkpoint_epochs = sorted(
                {
                    int(clock_epoch) + int(delay)
                    for clock_epoch in day_clock_epochs
                    for delay in checkpoint_delays
                    if delay >= 0
                }
            )
        checkpoint_index = 0
        if use_target_stream_history:
            stream_path = runner.target_stream_path(trade_date)
            if not stream_path.exists() and skip_missing_weekends and date.fromisoformat(trade_date).weekday() >= 5:
                date_reports.append(
                    {
                        "trade_date": trade_date,
                        "source": str(stream_path),
                        "source_type": "target_stream_history",
                        "source_found": False,
                        "skipped": True,
                        "skip_reason": "weekend",
                        "rows_seen": 0,
                        "quotes_used": 0,
                        "trade_state_passes": 0,
                        "trade_state_events": 0,
                        "trade_state_last_report": None,
                        "duration_seconds": 0.0,
                        "partial": False,
                    }
                )
                continue
            records = iter_target_stream_normalized_rows(stream_path, trade_date, runner.targets)
            source_report = {
                "trade_date": trade_date,
                "source": str(stream_path),
                "source_type": "target_stream_history",
                "source_found": stream_path.exists(),
            }
        else:
            records, source_report = iter_bootstrap_records(runner.config, trade_date, runner.targets)
        if not source_report.get("source_found"):
            date_reports.append(
                {
                    **source_report,
                    "rows_seen": 0,
                    "quotes_used": 0,
                    "trade_state_passes": 0,
                    "trade_state_events": 0,
                    "trade_state_last_report": None,
                    "duration_seconds": 0.0,
                    "partial": False,
                }
            )
            continue
        rows_seen = 0
        quotes_used = 0
        last_replay_epoch: int | None = None
        trade_state_reports: list[dict[str, Any]] = []
        started = time.perf_counter()

        def run_trade_state_passes(checkpoint_epoch: int, reason_prefix: str) -> None:
            for state in runner.states.values():
                state.finalize_until(int(checkpoint_epoch))
            for pass_index in range(max(1, int(max_trade_state_passes))):
                report = runner.evaluate_frozen_trade_state(
                    trade_date,
                    reason=f"{reason_prefix}_pass_{pass_index + 1}",
                    evaluation_epoch=int(checkpoint_epoch),
                )
                trade_state_reports.append(report)
                if int(report.get("events") or 0) == 0:
                    break

        for record in records:
            if deadline is not None and time.time() >= deadline:
                partial = True
                break
            rows_seen += 1
            key = str(record.get("instrument_key") or record.get("target") or "")
            if key in target_set:
                items = [record]
            else:
                try:
                    items = target_items_from_batch_line(json.dumps(record), target_set)
                except Exception:
                    items = []
            max_item_epoch: int | None = None
            for item in items:
                item_key = str(item.get("instrument_key") or item.get("target") or "")
                if item_key not in target_set:
                    continue
                row = normalise_or_pass_record(item, trade_date, item_key)
                if row is None:
                    continue
                runner.states[item_key].process_row(row)
                quotes_used += 1
                row_epoch = int(row.get("epoch_second") or row.get("epoch") or 0)
                if row_epoch > 0:
                    max_item_epoch = max(max_item_epoch or row_epoch, row_epoch)
                    last_replay_epoch = max(last_replay_epoch or row_epoch, row_epoch)
                received_epoch = as_float(row.get("received_epoch")) or as_float(row.get("epoch"))
                if received_epoch is not None:
                    runner.latest_feed_epoch = max(runner.latest_feed_epoch or received_epoch, received_epoch)
            if max_item_epoch is not None:
                while event_time_checkpoints and checkpoint_index < len(checkpoint_epochs) and int(checkpoint_epochs[checkpoint_index]) <= int(max_item_epoch):
                    effective_epoch = int(checkpoint_epochs[checkpoint_index])
                    run_trade_state_passes(
                        effective_epoch,
                        f"archive_replay_{trade_date}_checkpoint_{epoch_ist_iso(checkpoint_epochs[checkpoint_index])}",
                    )
                    checkpoint_index += 1
        for state in runner.states.values():
            state.flush_until_latest()
        if not partial:
            final_epoch = int(last_replay_epoch or runner.latest_feed_epoch or time.time())
            if event_time_checkpoints:
                while checkpoint_index < len(checkpoint_epochs) and int(checkpoint_epochs[checkpoint_index]) <= final_epoch:
                    run_trade_state_passes(
                        int(checkpoint_epochs[checkpoint_index]),
                        f"archive_replay_{trade_date}_checkpoint_{epoch_ist_iso(checkpoint_epochs[checkpoint_index])}",
                    )
                    checkpoint_index += 1
                run_trade_state_passes(final_epoch, f"archive_replay_{trade_date}_final")
            else:
                for pass_index in range(max(1, int(max_trade_state_passes))):
                    report = runner.evaluate_frozen_trade_state(
                        trade_date,
                        reason=f"archive_replay_{trade_date}_pass_{pass_index + 1}",
                        evaluation_epoch=final_epoch,
                    )
                    trade_state_reports.append(report)
                    if int(report.get("events") or 0) == 0:
                        break
        date_reports.append(
            {
                **source_report,
                "rows_seen": rows_seen,
                "quotes_used": quotes_used,
                "trade_state_passes": len(trade_state_reports),
                "trade_state_events": sum(int(item.get("events") or 0) for item in trade_state_reports),
                "trade_state_last_report": trade_state_reports[-1] if trade_state_reports else None,
                "duration_seconds": round(time.perf_counter() - started, 4),
                "partial": partial,
            }
        )
        if partial:
            break
    manifest = None
    if not partial:
        manifest = runner.save_bootstrap_states(end_date, date_reports)
    runner.write_status()
    report = {
        "schema": "obvfutport_v2.archive_replay_report.v1",
        "ok": not partial,
        "partial": partial,
        "history_source": "target_stream" if use_target_stream_history else "archive_or_history",
        "start_date": start_date,
        "end_date": end_date,
        "output_state_dir": str(output_state_dir),
        "runtime_config": str(tmp_config),
        "symbols": len(runner.instruments),
        "target_keys": len(runner.targets),
        "date_reports": date_reports,
        "bootstrap_manifest": manifest,
        "updated_at_ist": now_ist().isoformat(),
    }
    if partial:
        report["reason"] = "max_runtime_seconds_reached"
    atomic_write_json(output_state_dir / "archive_replay_report.json", report)
    if output_path is not None:
        atomic_write_json(output_path, report)
    print(json.dumps(json_clean(report), indent=2, sort_keys=True))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="OBVFUTPORT v2 passive compact runner")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run_p = sub.add_parser("run")
    run_p.add_argument("--config", required=True)
    run_p.add_argument("--max-runtime-seconds", type=float, default=None)
    target_p = sub.add_parser("target-stream")
    target_p.add_argument("--config", required=True)
    target_p.add_argument("--max-runtime-seconds", type=float, default=None)
    build_stream_p = sub.add_parser("build-target-stream-packs")
    build_stream_p.add_argument("--config", required=True)
    build_stream_p.add_argument("--start-date", required=True)
    build_stream_p.add_argument("--end-date", required=True)
    build_stream_p.add_argument("--output-root", default=None)
    build_stream_p.add_argument("--overwrite", action="store_true")
    build_stream_p.add_argument("--max-runtime-seconds", type=float, default=None)
    bootstrap_p = sub.add_parser("bootstrap")
    bootstrap_p.add_argument("--config", required=True)
    bootstrap_p.add_argument("--start-date", required=True)
    bootstrap_p.add_argument("--end-date", default=now_ist().date().isoformat())
    bootstrap_p.add_argument("--max-runtime-seconds", type=float, default=None)
    streaming_bootstrap_p = sub.add_parser("streaming-bootstrap")
    streaming_bootstrap_p.add_argument("--config", required=True)
    streaming_bootstrap_p.add_argument("--start-date", required=True)
    streaming_bootstrap_p.add_argument("--end-date", default=now_ist().date().isoformat())
    streaming_bootstrap_p.add_argument("--use-target-stream-history", action="store_true")
    streaming_bootstrap_p.add_argument("--no-skip-missing-weekends", action="store_true")
    streaming_bootstrap_p.add_argument("--catchup-date", default=None)
    streaming_bootstrap_p.add_argument("--catchup-target-stream", action="store_true")
    streaming_bootstrap_p.add_argument("--catchup-offset", type=int, default=0)
    streaming_bootstrap_p.add_argument("--catchup-max-bytes", type=int, default=None)
    streaming_bootstrap_p.add_argument("--resume-date", default=None)
    streaming_bootstrap_p.add_argument("--max-runtime-seconds", type=float, default=None)
    streaming_bootstrap_p.add_argument("--min-target-coverage-ratio", type=float, default=None)
    streaming_bootstrap_p.add_argument("--no-promote-latest", action="store_true")
    replay_p = sub.add_parser("archive-replay")
    replay_p.add_argument("--config", required=True)
    replay_p.add_argument("--start-date", required=True)
    replay_p.add_argument("--end-date", default=now_ist().date().isoformat())
    replay_p.add_argument("--output-state-dir", required=True)
    replay_p.add_argument("--use-target-stream-history", action="store_true")
    replay_p.add_argument("--no-skip-missing-weekends", action="store_true")
    replay_p.add_argument("--max-runtime-seconds", type=float, default=None)
    replay_p.add_argument("--max-trade-state-passes", type=int, default=20)
    replay_p.add_argument("--output", default=None)
    smoke_p = sub.add_parser("smoke")
    smoke_p.add_argument("--config", required=True)
    smoke_p.add_argument("--output-dir", required=True)
    parity_p = sub.add_parser("parity-v1-ledgers")
    parity_p.add_argument("--config", required=True)
    parity_p.add_argument("--v1-state-root", required=True)
    parity_p.add_argument("--output", default=None)
    args = parser.parse_args()
    if args.cmd == "run":
        PassiveV2Runner(Path(args.config)).run(max_runtime_seconds=args.max_runtime_seconds)
    elif args.cmd == "target-stream":
        ObvTargetStreamExtractor(Path(args.config)).run(max_runtime_seconds=args.max_runtime_seconds)
    elif args.cmd == "build-target-stream-packs":
        build_target_stream_packs(
            Path(args.config),
            args.start_date,
            args.end_date,
            output_root=Path(args.output_root) if args.output_root else None,
            overwrite=bool(args.overwrite),
            max_runtime_seconds=args.max_runtime_seconds,
        )
    elif args.cmd == "bootstrap":
        run_bootstrap(Path(args.config), args.start_date, args.end_date, max_runtime_seconds=args.max_runtime_seconds)
    elif args.cmd == "streaming-bootstrap":
        run_streaming_bootstrap(
            Path(args.config),
            args.start_date,
            args.end_date,
            use_target_stream_history=bool(args.use_target_stream_history),
            skip_missing_weekends=not bool(args.no_skip_missing_weekends),
            catchup_date=args.catchup_date,
            catchup_target_stream=bool(args.catchup_target_stream),
            catchup_offset=int(args.catchup_offset or 0),
            catchup_max_bytes=args.catchup_max_bytes,
            resume_date=args.resume_date,
            max_runtime_seconds=args.max_runtime_seconds,
            min_target_coverage_ratio=args.min_target_coverage_ratio,
            promote_latest=not bool(args.no_promote_latest),
        )
    elif args.cmd == "archive-replay":
        run_archive_replay(
            Path(args.config),
            args.start_date,
            args.end_date,
            output_state_dir=Path(args.output_state_dir),
            use_target_stream_history=bool(args.use_target_stream_history),
            skip_missing_weekends=not bool(args.no_skip_missing_weekends),
            max_runtime_seconds=args.max_runtime_seconds,
            max_trade_state_passes=args.max_trade_state_passes,
            output_path=Path(args.output) if args.output else None,
        )
    elif args.cmd == "smoke":
        run_smoke(Path(args.config), Path(args.output_dir))
    elif args.cmd == "parity-v1-ledgers":
        report = compare_v1_ledgers(
            Path(args.config),
            Path(args.v1_state_root),
            Path(args.output) if args.output else None,
        )
        print(json.dumps(json_clean(report), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
