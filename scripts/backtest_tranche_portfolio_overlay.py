from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import os
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def add_paths(root: Path) -> None:
    for path in (root / "src", Path("/opt/cloud-deploy-candidates/obv-futures-portable-v1/src")):
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


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            handle.write("\n")
    tmp.replace(path)


def as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def epoch_ist_iso(epoch: int | float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), tz=IST).isoformat()


def parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def day_bounds(day: date) -> tuple[int, int]:
    start = datetime.combine(day, dt_time(0, 0), tzinfo=IST)
    end = datetime.combine(day, dt_time(23, 59, 59), tzinfo=IST)
    return int(start.timestamp()), int(end.timestamp())


def minute_floor(epoch: int | float) -> int:
    return int(float(epoch) // 60) * 60


def session_clock_epochs(day: date) -> list[int]:
    out: list[int] = []
    current = datetime.combine(day, dt_time(9, 16), tzinfo=IST)
    end = datetime.combine(day, dt_time(15, 30), tzinfo=IST)
    while current <= end:
        out.append(int(current.timestamp()))
        current = datetime.fromtimestamp(int(current.timestamp()) + 60, tz=IST)
    return out


@dataclass
class Quote:
    minute_epoch: int
    event_epoch: float
    price: float
    bid: float | None
    ask: float | None


class QuoteIndex:
    def __init__(self) -> None:
        self._raw: dict[str, dict[int, Quote]] = {}
        self._keys: dict[str, list[int]] = {}
        self._values: dict[str, list[Quote]] = {}

    def add(self, key: str, epoch: float, price: float, bid: float | None, ask: float | None) -> None:
        minute = minute_floor(epoch)
        by_minute = self._raw.setdefault(key, {})
        old = by_minute.get(minute)
        if old is None or epoch >= old.event_epoch:
            by_minute[minute] = Quote(minute, epoch, price, bid, ask)

    def finalize(self) -> None:
        for key, rows in self._raw.items():
            minutes = sorted(rows)
            self._keys[key] = minutes
            self._values[key] = [rows[minute] for minute in minutes]
        self._raw.clear()

    def key_count(self) -> int:
        return len(self._keys)

    def row_count(self) -> int:
        return sum(len(v) for v in self._values.values())

    def quote_at_or_before(self, key: str, epoch: int | float, *, max_age_seconds: int | None = None) -> Quote | None:
        minutes = self._keys.get(key)
        if not minutes:
            return None
        target = minute_floor(epoch)
        idx = bisect.bisect_right(minutes, target) - 1
        if idx < 0:
            return None
        quote = self._values[key][idx]
        if max_age_seconds is not None and target - quote.minute_epoch > max_age_seconds:
            return None
        return quote

    def quote_by_trading_offset(self, key: str, epoch: int | float, offset_minutes: int) -> Quote | None:
        minutes = self._keys.get(key)
        if not minutes:
            return None
        target = minute_floor(epoch)
        idx = bisect.bisect_right(minutes, target) - 1
        idx -= int(offset_minutes)
        if idx < 0:
            return None
        return self._values[key][idx]

    def price_window(self, key: str, epoch: int | float, lookback_minutes: int) -> list[float]:
        minutes = self._keys.get(key)
        values = self._values.get(key)
        if not minutes or not values:
            return []
        target = minute_floor(epoch)
        idx = bisect.bisect_right(minutes, target) - 1
        start = idx - int(lookback_minutes)
        if idx < 0 or start < 0:
            return []
        return [quote.price for quote in values[start : idx + 1]]

    def key_earliest_epoch(self, key: str) -> int | None:
        minutes = self._keys.get(key)
        return minutes[0] if minutes else None

    def key_latest_epoch(self, key: str) -> int | None:
        minutes = self._keys.get(key)
        return minutes[-1] if minutes else None

    def ram_available_from_epoch(self, key: str, lookback_minutes: int) -> int | None:
        values = self._values.get(key)
        if not values or len(values) <= int(lookback_minutes):
            return None
        return int(values[int(lookback_minutes)].minute_epoch + 60)

    def lookback_window_bounds(self, key: str, epoch: int | float, lookback_minutes: int) -> tuple[int, int] | None:
        minutes = self._keys.get(key)
        values = self._values.get(key)
        if not minutes or not values:
            return None
        target = minute_floor(epoch)
        idx = bisect.bisect_right(minutes, target) - 1
        start = idx - int(lookback_minutes)
        if idx < 0 or start < 0:
            return None
        return int(values[start].minute_epoch), int(values[idx].minute_epoch)

    def last_epoch(self) -> int | None:
        latest: int | None = None
        for minutes in self._keys.values():
            if minutes:
                latest = max(latest or minutes[-1], minutes[-1])
        return latest


@dataclass
class TrancheLeg:
    row_id: str
    symbol: str
    tranche: str
    side: str
    entry_epoch: int
    exit_epoch: int | None
    position_id: str
    signal_source: str
    signal_key: str
    execution_key: str
    entry_fill_price: float | None
    exit_fill_price: float | None
    margin_per_lot: float
    lot_size: int
    source_row: dict[str, Any]


@dataclass
class Holding:
    leg: TrancheLeg
    lots: int
    margin_locked: float
    entry_epoch: int
    entry_fill_price: float
    entry_ltp_price: float
    entry_score: float
    entry_ram_10: float | None
    entry_ram_60: float | None


def row_id(row: dict[str, Any]) -> str:
    return "|".join(
        str(row.get(key) or "")
        for key in ("symbol", "tranche", "position_id", "entry_epoch", "exit_epoch", "status")
    )


def load_rows(root: Path) -> dict[str, Any]:
    os.environ["OBVFUTPORT_V2_ROOT"] = str(root)
    os.environ["OBVFUTPORT_V2_STATE_DIR"] = str(root / "state")
    from obvfut_portable_v2 import dashboard  # type: ignore

    return dashboard.load_rows()


def load_margin_lookup(root: Path) -> dict[str, dict[str, float]]:
    from obvfut_portable_v2 import dashboard  # type: ignore

    return dashboard.load_margin_lookup()


def load_contract_manifest(root: Path) -> dict[str, Any]:
    return read_json(root / "config" / "obvfutport_v2_contract_chain_manifest.json", {})


def manifest_symbol(manifest: dict[str, Any], symbol: str) -> dict[str, Any]:
    payload = (manifest.get("symbols") or {}).get(symbol)
    return payload if isinstance(payload, dict) else {}


def lot_size_for(manifest: dict[str, Any], symbol: str, execution_key: str) -> int:
    payload = manifest_symbol(manifest, symbol)
    for contract in payload.get("contracts") or []:
        if isinstance(contract, dict) and str(contract.get("instrument_key") or "") == execution_key:
            return max(1, int(contract.get("lot_size") or 1))
    for contract in payload.get("contracts") or []:
        if isinstance(contract, dict) and contract.get("lot_size"):
            return max(1, int(contract.get("lot_size") or 1))
    return 1


def fallback_execution_key(manifest: dict[str, Any], symbol: str) -> str:
    payload = manifest_symbol(manifest, symbol)
    return str(payload.get("base_fut_key") or "")


def fallback_signal_key(manifest: dict[str, Any], row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "")
    source = str(row.get("signal_source") or "").lower()
    payload = manifest_symbol(manifest, symbol)
    if source == "cash":
        return str(row.get("signal_instrument_key") or payload.get("cash_key") or row.get("instrument_key") or "")
    return str(row.get("signal_instrument_key") or row.get("instrument_key") or payload.get("base_fut_key") or "")


def margin_for(row: dict[str, Any], margins: dict[str, dict[str, float]]) -> float | None:
    symbol = str(row.get("symbol") or "")
    side = str(row.get("side") or "").lower()
    value = as_float(row.get("margin_rupees"))
    if value:
        return value
    item = margins.get(symbol) or {}
    return item.get(side) or item.get("long") or item.get("short")


def build_legs(rows_by_tranche: dict[str, list[dict[str, Any]]], manifest: dict[str, Any], margins: dict[str, dict[str, float]]) -> dict[str, list[TrancheLeg]]:
    result: dict[str, list[TrancheLeg]] = {"T2": [], "T3": []}
    for tranche in ("T2", "T3"):
        for row in rows_by_tranche.get(tranche) or []:
            symbol = str(row.get("symbol") or "")
            side = str(row.get("side") or "").lower()
            entry_epoch = as_int(row.get("entry_epoch"))
            if not symbol or side not in {"long", "short"} or entry_epoch is None:
                continue
            execution_key = str(row.get("instrument_key") or fallback_execution_key(manifest, symbol))
            signal_key = fallback_signal_key(manifest, row)
            margin = margin_for(row, margins)
            if not execution_key or not signal_key or margin is None or margin <= 0:
                continue
            leg = TrancheLeg(
                row_id=row_id(row),
                symbol=symbol,
                tranche=tranche,
                side=side,
                entry_epoch=entry_epoch,
                exit_epoch=as_int(row.get("exit_epoch")) if row.get("status") == "closed" else None,
                position_id=str(row.get("position_id") or row.get("signal_id") or ""),
                signal_source=str(row.get("signal_source") or ""),
                signal_key=signal_key,
                execution_key=execution_key,
                entry_fill_price=as_float(row.get("entry_fill_price")),
                exit_fill_price=as_float(row.get("exit_fill_price")),
                margin_per_lot=float(margin),
                lot_size=lot_size_for(manifest, symbol, execution_key),
                source_row=row,
            )
            result[tranche].append(leg)
    for legs in result.values():
        legs.sort(key=lambda item: (item.entry_epoch, item.exit_epoch or 2**31, item.symbol, item.row_id))
    return result


def discover_stream_paths(root: Path, start_date: date, end_date: date) -> list[tuple[str, Path]]:
    paths: list[tuple[str, Path]] = []
    base = root / "state" / "target_stream"
    for day_dir in sorted(base.glob("20??-??-??")):
        try:
            day = parse_date(day_dir.name)
        except ValueError:
            continue
        if day < start_date or day > end_date:
            continue
        path = day_dir / f"target_quotes_{day_dir.name}.jsonl"
        if path.exists():
            paths.append((day_dir.name, path))
    return paths


def load_quote_index(root: Path, stream_paths: list[tuple[str, Path]], required_keys: set[str]) -> tuple[QuoteIndex, dict[str, Any]]:
    from obvfut_portable_v2.passive_runner import row_from_target_stream_line  # type: ignore

    index = QuoteIndex()
    per_day: dict[str, dict[str, Any]] = {}
    total_lines = 0
    kept_rows = 0
    started = time.monotonic()
    for trade_date, path in stream_paths:
        day_lines = 0
        day_kept = 0
        with path.open("rb") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                day_lines += 1
                row = row_from_target_stream_line(raw_line, trade_date, required_keys)
                if row is None:
                    continue
                price = as_float(row.get("price"))
                epoch = as_float(row.get("epoch"))
                if price is None or epoch is None:
                    continue
                key = str(row.get("target") or "")
                index.add(key, epoch, price, as_float(row.get("bid")), as_float(row.get("ask")))
                day_kept += 1
        total_lines += day_lines
        kept_rows += day_kept
        per_day[trade_date] = {
            "path": str(path),
            "line_count": day_lines,
            "kept_target_rows": day_kept,
            "size_bytes": path.stat().st_size if path.exists() else None,
            "is_symlink": path.is_symlink(),
            "resolved_path": str(path.resolve()) if path.exists() else None,
        }
    index.finalize()
    return index, {
        "stream_days": per_day,
        "total_lines_scanned": total_lines,
        "kept_target_rows": kept_rows,
        "quote_keys_loaded": index.key_count(),
        "minute_quote_rows": index.row_count(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def signed_direction(side: str) -> float:
    return 1.0 if side == "long" else -1.0


def sample_std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.pstdev(values)


def directional_return(index: QuoteIndex, leg: TrancheLeg, clock_epoch: int, lookback_minutes: int) -> float | None:
    ref_epoch = clock_epoch - 60
    end = index.quote_by_trading_offset(leg.signal_key, ref_epoch, 0)
    start = index.quote_by_trading_offset(leg.signal_key, ref_epoch, lookback_minutes)
    if end is None or start is None or start.price <= 0:
        return None
    return signed_direction(leg.side) * ((end.price / start.price) - 1.0)


def risk_adjusted_momentum(index: QuoteIndex, leg: TrancheLeg, clock_epoch: int, lookback_minutes: int, *, risk_floor: float) -> float | None:
    ref_epoch = clock_epoch - 60
    window = index.price_window(leg.signal_key, ref_epoch, lookback_minutes)
    if len(window) < lookback_minutes:
        return None
    returns = []
    for prev, cur in zip(window, window[1:]):
        if prev > 0:
            returns.append((cur / prev) - 1.0)
    if len(returns) < max(2, lookback_minutes // 2):
        return None
    dir_ret = directional_return(index, leg, clock_epoch, lookback_minutes)
    if dir_ret is None:
        return None
    realized_risk = sample_std(returns) * math.sqrt(max(1, len(returns)))
    return dir_ret / max(risk_floor, realized_risk)


def percentile_ranks(items: list[tuple[str, float | None]]) -> dict[str, float | None]:
    valid = [(row_id_, value) for row_id_, value in items if value is not None and math.isfinite(float(value))]
    if not valid:
        return {row_id_: None for row_id_, _ in items}
    valid_sorted = sorted(valid, key=lambda pair: (pair[1], pair[0]))
    ranks: dict[str, float] = {}
    n = len(valid_sorted)
    if n == 1:
        ranks[valid_sorted[0][0]] = 1.0
    else:
        for idx, (row_id_, _value) in enumerate(valid_sorted):
            ranks[row_id_] = idx / (n - 1)
    return {row_id_: ranks.get(row_id_) for row_id_, _ in items}


def score_eligible(index: QuoteIndex, legs: list[TrancheLeg], clock_epoch: int, *, weight_10: float, risk_floor: float) -> dict[str, dict[str, Any]]:
    raw: dict[str, dict[str, Any]] = {}
    for leg in legs:
        ram10 = risk_adjusted_momentum(index, leg, clock_epoch, 10, risk_floor=risk_floor)
        ram60 = risk_adjusted_momentum(index, leg, clock_epoch, 60, risk_floor=risk_floor)
        ret10 = directional_return(index, leg, clock_epoch, 10)
        ret60 = directional_return(index, leg, clock_epoch, 60)
        raw[leg.row_id] = {"ram_10": ram10, "ram_60": ram60, "ret_10": ret10, "ret_60": ret60}
    rank10 = percentile_ranks([(leg.row_id, raw[leg.row_id]["ram_10"]) for leg in legs])
    rank60 = percentile_ranks([(leg.row_id, raw[leg.row_id]["ram_60"]) for leg in legs])
    out: dict[str, dict[str, Any]] = {}
    weight_60 = 1.0 - weight_10
    for leg in legs:
        r10 = rank10.get(leg.row_id)
        r60 = rank60.get(leg.row_id)
        if r10 is None or r60 is None:
            score = None
        else:
            score = weight_10 * r10 + weight_60 * r60
        out[leg.row_id] = {
            "score": score,
            "ram_10": raw[leg.row_id]["ram_10"],
            "ram_60": raw[leg.row_id]["ram_60"],
            "ret_10": raw[leg.row_id]["ret_10"],
            "ret_60": raw[leg.row_id]["ret_60"],
            "rank_10": r10,
            "rank_60": r60,
        }
    return out


def execution_fill(index: QuoteIndex, v1_portfolio: Any, leg: TrancheLeg, epoch: int, *, phase: str) -> dict[str, Any] | None:
    quote = index.quote_at_or_before(leg.execution_key, epoch, max_age_seconds=300)
    if quote is None:
        return None
    row = {
        "price": quote.price,
        "bid": quote.bid,
        "ask": quote.ask,
        "epoch_second": quote.minute_epoch,
    }
    fill = v1_portfolio.execution_fill_from_row(row, side=leg.side, phase=phase, point_config=None)
    if as_float(fill.get("fill_price")) is None:
        return None
    return fill


def accounting(v1_portfolio: Any, leg: TrancheLeg, entry_fill: float, exit_fill: float, lots: int) -> dict[str, Any]:
    return v1_portfolio.futures_trade_accounting(
        side=leg.side,
        entry_fill_price=float(entry_fill),
        exit_fill_price=float(exit_fill),
        lot_size=int(leg.lot_size or 1),
        lots=int(lots or 1),
        point_config=None,
    )


def estimated_cost_adjusted_edge(
    index: QuoteIndex,
    v1_portfolio: Any,
    leg: TrancheLeg,
    clock_epoch: int,
    score: dict[str, Any],
    *,
    weight_10: float,
) -> dict[str, Any]:
    ret10 = as_float(score.get("ret_10"))
    ret60 = as_float(score.get("ret_60"))
    if ret10 is None or ret60 is None:
        return {"ok": False, "reason": "missing_directional_return"}
    edge_return = max(0.0, weight_10 * ret10 + (1.0 - weight_10) * ret60)
    if edge_return <= 0:
        return {"ok": False, "reason": "non_positive_edge_return", "edge_return": edge_return}
    fill = execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")
    if fill is None:
        return {"ok": False, "reason": "missing_entry_fill_for_edge"}
    entry_fill = float(fill["fill_price"])
    if leg.side == "long":
        projected_exit = entry_fill * (1.0 + edge_return)
    else:
        projected_exit = entry_fill * (1.0 - edge_return)
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


def holding_mtm(index: QuoteIndex, v1_portfolio: Any, holding: Holding, epoch: int) -> float:
    fill = execution_fill(index, v1_portfolio, holding.leg, epoch, phase="exit")
    if fill is None:
        return 0.0
    acct = accounting(v1_portfolio, holding.leg, holding.entry_fill_price, float(fill["fill_price"]), holding.lots)
    return float(acct.get("net_rupees") or 0.0)


def portfolio_equity(cash: float, holdings: dict[str, Holding], index: QuoteIndex, v1_portfolio: Any, epoch: int) -> float:
    locked = sum(item.margin_locked for item in holdings.values())
    unrealized = sum(holding_mtm(index, v1_portfolio, item, epoch) for item in holdings.values())
    return cash + locked + unrealized


def close_holding(
    *,
    holding: Holding,
    exit_epoch: int,
    reason: str,
    index: QuoteIndex,
    v1_portfolio: Any,
    use_leg_exit_fill: bool,
    scores: dict[str, dict[str, Any]] | None = None,
) -> tuple[float, dict[str, Any]]:
    leg = holding.leg
    exit_fill_price: float | None = None
    exit_ltp_price: float | None = None
    fill_quality: str | None = None
    if use_leg_exit_fill and leg.exit_fill_price is not None:
        exit_fill_price = leg.exit_fill_price
        exit_ltp_price = as_float(leg.source_row.get("exit_price")) or exit_fill_price
        fill_quality = "underlying_tranche_exit_fill"
    else:
        fill = execution_fill(index, v1_portfolio, leg, exit_epoch, phase="exit")
        if fill is None:
            raise RuntimeError(f"missing exit fill for {leg.tranche} {leg.symbol} {exit_epoch}")
        exit_fill_price = float(fill["fill_price"])
        exit_ltp_price = as_float(fill.get("ltp_price")) or exit_fill_price
        fill_quality = str(fill.get("fill_quality") or "")
    acct = accounting(v1_portfolio, leg, holding.entry_fill_price, exit_fill_price, holding.lots)
    score = (scores or {}).get(leg.row_id, {})
    row = {
        "event": "portfolio_exit",
        "portfolio": leg.tranche,
        "symbol": leg.symbol,
        "side": leg.side,
        "position_id": leg.position_id,
        "row_id": leg.row_id,
        "lots": holding.lots,
        "lot_size": leg.lot_size,
        "margin_locked": holding.margin_locked,
        "entry_epoch": holding.entry_epoch,
        "entry_time": epoch_ist_iso(holding.entry_epoch),
        "exit_epoch": int(exit_epoch),
        "exit_time": epoch_ist_iso(exit_epoch),
        "entry_fill_price": holding.entry_fill_price,
        "entry_ltp_price": holding.entry_ltp_price,
        "exit_fill_price": exit_fill_price,
        "exit_ltp_price": exit_ltp_price,
        "exit_reason": reason,
        "exit_fill_quality": fill_quality,
        "entry_score": holding.entry_score,
        "entry_ram_10": holding.entry_ram_10,
        "entry_ram_60": holding.entry_ram_60,
        "exit_score": score.get("score"),
        "exit_ram_10": score.get("ram_10"),
        "exit_ram_60": score.get("ram_60"),
        "gross_rupees": acct.get("gross_rupees"),
        "charges_rupees": acct.get("charges_rupees"),
        "net_rupees": acct.get("net_rupees"),
        "net_pct_margin": (float(acct.get("net_rupees") or 0.0) / holding.margin_locked * 100.0) if holding.margin_locked else None,
        "charge_breakdown": acct.get("charge_breakdown"),
    }
    return float(acct.get("net_rupees") or 0.0), row


def enter_holding(
    *,
    leg: TrancheLeg,
    lots: int,
    clock_epoch: int,
    score: dict[str, Any],
    index: QuoteIndex,
    v1_portfolio: Any,
) -> Holding:
    fill = execution_fill(index, v1_portfolio, leg, clock_epoch, phase="entry")
    if fill is None:
        raise RuntimeError(f"missing entry fill for {leg.tranche} {leg.symbol} {clock_epoch}")
    return Holding(
        leg=leg,
        lots=int(lots),
        margin_locked=float(leg.margin_per_lot) * int(lots),
        entry_epoch=int(clock_epoch),
        entry_fill_price=float(fill["fill_price"]),
        entry_ltp_price=as_float(fill.get("ltp_price")) or float(fill["fill_price"]),
        entry_score=float(score["score"]),
        entry_ram_10=as_float(score.get("ram_10")),
        entry_ram_60=as_float(score.get("ram_60")),
    )


def choose_lots(cash: float, equity: float, margin_per_lot: float) -> int:
    budget = min(max(0.0, cash), max(0.0, equity) * 0.20)
    if margin_per_lot <= 0 or budget < margin_per_lot:
        return 0
    return max(0, int(math.floor(budget / margin_per_lot)))


def positive_ram_count(score: dict[str, Any]) -> int:
    return sum(
        1
        for key in ("ram_10", "ram_60")
        if as_float(score.get(key)) is not None and float(score[key]) > 0
    )


def parse_tranche_float_map(text: str, *, default: float | None = None) -> dict[str, float | None]:
    out = {"T2": default, "T3": default}
    raw = (text or "").strip()
    if not raw:
        return out
    if ":" not in raw:
        value = None if raw.lower() in {"none", "null", "na"} else float(raw)
        return {"T2": value, "T3": value}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        tranche, value_text = item.split(":", 1)
        value = None if value_text.strip().lower() in {"none", "null", "na"} else float(value_text)
        out[tranche.strip().upper()] = value
    return out


def summarize_transactions(
    transactions: list[dict[str, Any]],
    open_holdings: dict[str, Holding],
    cash: float,
    equity: float,
    peak_margin: float,
    peak_equity: float,
    *,
    initial_capital: float,
) -> dict[str, Any]:
    exits = [row for row in transactions if row.get("event") == "portfolio_exit"]
    net = [float(row.get("net_rupees") or 0.0) for row in exits]
    wins = [x for x in net if x > 0]
    losses = [x for x in net if x <= 0]
    return {
        "closed_trades": len(exits),
        "wins": len(wins),
        "losses": len(losses),
        "success_rate_pct": (len(wins) / len(exits) * 100.0) if exits else None,
        "realized_net_rupees": sum(net),
        "cash_rupees": cash,
        "ending_equity_rupees": equity,
        "open_positions": len(open_holdings),
        "current_margin_rupees": sum(item.margin_locked for item in open_holdings.values()),
        "peak_margin_rupees": peak_margin,
        "peak_equity_rupees": peak_equity,
        "return_on_initial_pct": ((equity / initial_capital) - 1.0) * 100.0 if initial_capital else None,
        "return_on_peak_margin_pct": (sum(net) / peak_margin * 100.0) if peak_margin else None,
        "avg_net_rupees": statistics.mean(net) if net else None,
        "median_net_rupees": statistics.median(net) if net else None,
        "worst_trade_rupees": min(net) if net else None,
        "best_trade_rupees": max(net) if net else None,
    }


def run_portfolio(
    *,
    tranche: str,
    legs: list[TrancheLeg],
    dates: list[date],
    index: QuoteIndex,
    v1_portfolio: Any,
    initial_capital: float,
    max_positions: int,
    replacement_delta: float,
    replacement_mode: str,
    min_hold_minutes: int,
    replacement_persist_clocks: int,
    weakest_score_ceiling: float | None,
    max_leg_age_minutes: float | None,
    min_positive_ram_count: int,
    min_edge_cost_multiple: float | None,
    age_score_penalty: float | None,
    min_minutes_to_session_end: float | None,
    min_score: float,
    weight_10: float,
    risk_floor: float,
) -> dict[str, Any]:
    cash = float(initial_capital)
    holdings: dict[str, Holding] = {}
    transactions: list[dict[str, Any]] = []
    diagnostics = {
        "missing_entry_fill": 0,
        "missing_exit_fill": 0,
        "cash_blocked_entries": 0,
        "score_missing_candidates": 0,
        "forced_exits": 0,
        "replacement_exits": 0,
        "replacement_blocked_min_hold": 0,
        "replacement_blocked_persistence": 0,
        "replacement_blocked_weakest_score_ceiling": 0,
        "entry_blocked_max_age": 0,
        "entry_blocked_positive_ram": 0,
        "entry_blocked_cost_edge": 0,
        "entry_blocked_session_runway": 0,
        "entries": 0,
        "evaluated_clocks": 0,
    }
    peak_margin = 0.0
    peak_equity = cash
    legs_by_id = {leg.row_id: leg for leg in legs}
    replacement_streak: dict[tuple[str, str], int] = {}
    for day in dates:
        day_clocks = session_clock_epochs(day)
        session_end_epoch = day_clocks[-1] if day_clocks else None
        for clock_epoch in day_clocks:
            diagnostics["evaluated_clocks"] += 1
            # Underlying T2/T3 exits are immediate; process them at the next portfolio clock.
            for holding_key, holding in list(holdings.items()):
                leg = holding.leg
                if leg.exit_epoch is not None and leg.exit_epoch <= clock_epoch:
                    try:
                        pnl, exit_row = close_holding(
                            holding=holding,
                            exit_epoch=leg.exit_epoch,
                            reason="underlying_tranche_exit",
                            index=index,
                            v1_portfolio=v1_portfolio,
                            use_leg_exit_fill=True,
                        )
                    except RuntimeError:
                        diagnostics["missing_exit_fill"] += 1
                        continue
                    cash += holding.margin_locked + pnl
                    transactions.append(exit_row)
                    diagnostics["forced_exits"] += 1
                    holdings.pop(holding_key, None)

            active = [
                leg
                for leg in legs
                if leg.entry_epoch <= clock_epoch and (leg.exit_epoch is None or leg.exit_epoch > clock_epoch)
            ]
            if not active:
                continue
            scores = score_eligible(index, active, clock_epoch, weight_10=weight_10, risk_floor=risk_floor)
            eligible: list[TrancheLeg] = []
            for leg in active:
                if leg.row_id in holdings:
                    continue
                score = scores.get(leg.row_id, {})
                raw_score = as_float(score.get("score"))
                if raw_score is None:
                    continue
                leg_age_minutes = max(0.0, (clock_epoch - leg.entry_epoch) / 60.0)
                minutes_to_session_end = ((session_end_epoch - clock_epoch) / 60.0) if session_end_epoch else None
                if (
                    min_minutes_to_session_end is not None
                    and minutes_to_session_end is not None
                    and minutes_to_session_end < min_minutes_to_session_end
                ):
                    diagnostics["entry_blocked_session_runway"] += 1
                    continue
                if max_leg_age_minutes is not None and leg_age_minutes > max_leg_age_minutes:
                    diagnostics["entry_blocked_max_age"] += 1
                    continue
                if min_positive_ram_count > 0 and positive_ram_count(score) < min_positive_ram_count:
                    diagnostics["entry_blocked_positive_ram"] += 1
                    continue
                if age_score_penalty is not None and max_leg_age_minutes and max_leg_age_minutes > 0:
                    adjusted_score = max(0.0, raw_score - float(age_score_penalty) * min(1.0, leg_age_minutes / max_leg_age_minutes))
                    score["raw_score"] = raw_score
                    score["age_adjusted_score"] = adjusted_score
                    score["score"] = adjusted_score
                    raw_score = adjusted_score
                if raw_score < min_score:
                    continue
                edge = estimated_cost_adjusted_edge(index, v1_portfolio, leg, clock_epoch, score, weight_10=weight_10)
                score["edge_diagnostics"] = edge
                if min_edge_cost_multiple is not None:
                    multiple = as_float(edge.get("edge_to_cost_multiple"))
                    if not edge.get("ok") or multiple is None or multiple < min_edge_cost_multiple:
                        diagnostics["entry_blocked_cost_edge"] += 1
                        continue
                eligible.append(leg)
            diagnostics["score_missing_candidates"] += sum(
                1 for leg in active if scores.get(leg.row_id, {}).get("score") is None
            )
            eligible.sort(key=lambda leg: (float(scores[leg.row_id]["score"]), leg.symbol, leg.row_id), reverse=True)

            blocked_this_clock: set[str] = set()
            # Fill empty slots first.
            for leg in list(eligible):
                if len(holdings) >= max_positions:
                    break
                if leg.row_id in blocked_this_clock:
                    continue
                equity = portfolio_equity(cash, holdings, index, v1_portfolio, clock_epoch)
                lots = choose_lots(cash, equity, leg.margin_per_lot)
                if lots <= 0:
                    diagnostics["cash_blocked_entries"] += 1
                    continue
                try:
                    holding = enter_holding(
                        leg=leg,
                        lots=lots,
                        clock_epoch=clock_epoch,
                        score=scores[leg.row_id],
                        index=index,
                        v1_portfolio=v1_portfolio,
                    )
                except RuntimeError:
                    diagnostics["missing_entry_fill"] += 1
                    continue
                cash -= holding.margin_locked
                holdings[leg.row_id] = holding
                transactions.append(
                    {
                        "event": "portfolio_entry",
                        "portfolio": tranche,
                        "symbol": leg.symbol,
                        "side": leg.side,
                        "position_id": leg.position_id,
                        "row_id": leg.row_id,
                        "lots": holding.lots,
                        "lot_size": leg.lot_size,
                        "margin_locked": holding.margin_locked,
                        "entry_epoch": clock_epoch,
                        "entry_time": epoch_ist_iso(clock_epoch),
                        "entry_fill_price": holding.entry_fill_price,
                        "entry_ltp_price": holding.entry_ltp_price,
                        "entry_score": holding.entry_score,
                        "entry_ram_10": holding.entry_ram_10,
                        "entry_ram_60": holding.entry_ram_60,
                        "entry_raw_score": scores[leg.row_id].get("raw_score"),
                        "entry_age_adjusted_score": scores[leg.row_id].get("age_adjusted_score"),
                        "entry_edge_diagnostics": scores[leg.row_id].get("edge_diagnostics"),
                        "leg_age_minutes": max(0.0, (clock_epoch - leg.entry_epoch) / 60.0),
                        "minutes_to_session_end": ((session_end_epoch - clock_epoch) / 60.0) if session_end_epoch else None,
                        "signal_key": leg.signal_key,
                        "execution_key": leg.execution_key,
                        "underlying_tranche_entry_epoch": leg.entry_epoch,
                        "underlying_tranche_exit_epoch": leg.exit_epoch,
                    }
                )
                diagnostics["entries"] += 1

            # Replacement pass.
            seen_replacement_pairs: set[tuple[str, str]] = set()
            for leg in eligible:
                if len(holdings) < max_positions:
                    break
                if leg.row_id in holdings or leg.row_id in blocked_this_clock:
                    continue
                candidate_score = float(scores[leg.row_id]["score"])
                held_scores = []
                for holding_key, holding in holdings.items():
                    score = scores.get(holding_key, {}).get("score")
                    held_scores.append((float(score) if score is not None else -1.0, holding_key, holding))
                if not held_scores:
                    break
                weakest_score, weakest_key, weakest = min(held_scores, key=lambda item: (item[0], item[1]))
                pair_key = (leg.row_id, weakest_key)
                if replacement_mode == "pct":
                    replacement_bar = weakest_score * (1.0 + replacement_delta)
                else:
                    replacement_bar = weakest_score + replacement_delta
                if candidate_score <= replacement_bar:
                    replacement_streak.pop(pair_key, None)
                    continue
                if weakest_score_ceiling is not None and weakest_score > weakest_score_ceiling:
                    diagnostics["replacement_blocked_weakest_score_ceiling"] += 1
                    replacement_streak.pop(pair_key, None)
                    continue
                if min_hold_minutes > 0 and clock_epoch - weakest.entry_epoch < min_hold_minutes * 60:
                    diagnostics["replacement_blocked_min_hold"] += 1
                    replacement_streak.pop(pair_key, None)
                    continue
                seen_replacement_pairs.add(pair_key)
                replacement_streak[pair_key] = replacement_streak.get(pair_key, 0) + 1
                if replacement_streak[pair_key] < max(1, replacement_persist_clocks):
                    diagnostics["replacement_blocked_persistence"] += 1
                    continue
                equity = portfolio_equity(cash, holdings, index, v1_portfolio, clock_epoch)
                available_after_release = cash + weakest.margin_locked
                lots = choose_lots(available_after_release, equity, leg.margin_per_lot)
                if lots <= 0:
                    diagnostics["cash_blocked_entries"] += 1
                    continue
                try:
                    pnl, exit_row = close_holding(
                        holding=weakest,
                        exit_epoch=clock_epoch,
                        reason=f"replacement_{replacement_mode}_{replacement_delta:.2f}",
                        index=index,
                        v1_portfolio=v1_portfolio,
                        use_leg_exit_fill=False,
                        scores=scores,
                    )
                    new_holding = enter_holding(
                        leg=leg,
                        lots=lots,
                        clock_epoch=clock_epoch,
                        score=scores[leg.row_id],
                        index=index,
                        v1_portfolio=v1_portfolio,
                    )
                except RuntimeError:
                    diagnostics["missing_entry_fill"] += 1
                    continue
                cash += weakest.margin_locked + pnl
                holdings.pop(weakest_key, None)
                transactions.append(exit_row)
                diagnostics["replacement_exits"] += 1
                blocked_this_clock.add(weakest_key)
                replacement_streak.pop(pair_key, None)

                cash -= new_holding.margin_locked
                holdings[leg.row_id] = new_holding
                transactions.append(
                    {
                        "event": "portfolio_entry",
                        "portfolio": tranche,
                        "symbol": leg.symbol,
                        "side": leg.side,
                        "position_id": leg.position_id,
                        "row_id": leg.row_id,
                        "lots": new_holding.lots,
                        "lot_size": leg.lot_size,
                        "margin_locked": new_holding.margin_locked,
                        "entry_epoch": clock_epoch,
                        "entry_time": epoch_ist_iso(clock_epoch),
                        "entry_fill_price": new_holding.entry_fill_price,
                        "entry_ltp_price": new_holding.entry_ltp_price,
                        "entry_score": new_holding.entry_score,
                        "entry_ram_10": new_holding.entry_ram_10,
                        "entry_ram_60": new_holding.entry_ram_60,
                        "entry_raw_score": scores[leg.row_id].get("raw_score"),
                        "entry_age_adjusted_score": scores[leg.row_id].get("age_adjusted_score"),
                        "entry_edge_diagnostics": scores[leg.row_id].get("edge_diagnostics"),
                        "leg_age_minutes": max(0.0, (clock_epoch - leg.entry_epoch) / 60.0),
                        "minutes_to_session_end": ((session_end_epoch - clock_epoch) / 60.0) if session_end_epoch else None,
                        "signal_key": leg.signal_key,
                        "execution_key": leg.execution_key,
                        "underlying_tranche_entry_epoch": leg.entry_epoch,
                        "underlying_tranche_exit_epoch": leg.exit_epoch,
                    }
                )
                diagnostics["entries"] += 1
            if replacement_streak:
                replacement_streak = {key: value for key, value in replacement_streak.items() if key in seen_replacement_pairs}

            equity = portfolio_equity(cash, holdings, index, v1_portfolio, clock_epoch)
            peak_equity = max(peak_equity, equity)
            peak_margin = max(peak_margin, sum(item.margin_locked for item in holdings.values()))

    final_epoch = index.last_epoch() or (session_clock_epochs(dates[-1])[-1] if dates else int(time.time()))
    final_equity = portfolio_equity(cash, holdings, index, v1_portfolio, final_epoch)
    open_rows = []
    for holding in holdings.values():
        open_rows.append(
            {
                "portfolio": tranche,
                "symbol": holding.leg.symbol,
                "side": holding.leg.side,
                "position_id": holding.leg.position_id,
                "row_id": holding.leg.row_id,
                "lots": holding.lots,
                "margin_locked": holding.margin_locked,
                "entry_epoch": holding.entry_epoch,
                "entry_time": epoch_ist_iso(holding.entry_epoch),
                "entry_fill_price": holding.entry_fill_price,
                "entry_score": holding.entry_score,
                "unrealized_net_rupees": holding_mtm(index, v1_portfolio, holding, final_epoch),
            }
        )
    summary = summarize_transactions(
        transactions,
        holdings,
        cash,
        final_equity,
        peak_margin,
        peak_equity,
        initial_capital=initial_capital,
    )
    summary.update(
        {
            "portfolio": tranche,
            "initial_capital": initial_capital,
            "max_positions": max_positions,
            "replacement_delta": replacement_delta,
            "replacement_mode": replacement_mode,
            "min_hold_minutes": min_hold_minutes,
            "replacement_persist_clocks": replacement_persist_clocks,
            "weakest_score_ceiling": weakest_score_ceiling,
            "max_leg_age_minutes": max_leg_age_minutes,
            "min_positive_ram_count": min_positive_ram_count,
            "min_edge_cost_multiple": min_edge_cost_multiple,
            "age_score_penalty": age_score_penalty,
            "min_minutes_to_session_end": min_minutes_to_session_end,
            "min_score": min_score,
            "weight_10": weight_10,
            "weight_60": 1.0 - weight_10,
            "risk_floor": risk_floor,
            "diagnostics": diagnostics,
        }
    )
    return {"summary": summary, "transactions": transactions, "open_positions": open_rows}


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "portfolio",
        "replacement_delta",
        "replacement_mode",
        "min_hold_minutes",
        "replacement_persist_clocks",
        "weakest_score_ceiling",
        "max_leg_age_minutes",
        "min_positive_ram_count",
        "min_edge_cost_multiple",
        "age_score_penalty",
        "min_minutes_to_session_end",
        "min_score",
        "weight_10",
        "closed_trades",
        "success_rate_pct",
        "realized_net_rupees",
        "ending_equity_rupees",
        "return_on_initial_pct",
        "open_positions",
        "current_margin_rupees",
        "peak_margin_rupees",
        "worst_trade_rupees",
        "best_trade_rupees",
        "cash_rupees",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
    parser.add_argument("--start-date", default="2026-08-10")
    parser.add_argument("--end-date", default="2026-08-24")
    parser.add_argument("--initial-capital", type=float, default=10_000_000.0)
    parser.add_argument("--max-positions", type=int, default=5)
    parser.add_argument("--replacement-deltas", default="0,0.05,0.10,0.15,0.20")
    parser.add_argument("--replacement-mode", choices=["pct", "gap"], default="pct")
    parser.add_argument("--min-hold-minutes", default="0")
    parser.add_argument("--replacement-persist-clocks", default="1")
    parser.add_argument("--weakest-score-ceilings", default="none")
    parser.add_argument("--max-leg-age-minutes", default="none")
    parser.add_argument("--min-positive-ram-count", type=int, default=0)
    parser.add_argument("--min-edge-cost-multiple", default="none")
    parser.add_argument("--age-score-penalty", default="none")
    parser.add_argument("--min-minutes-to-session-end", default="none")
    parser.add_argument("--min-scores", default="0,0.50,0.60,0.70")
    parser.add_argument("--weight-10", type=float, default=0.5)
    parser.add_argument("--risk-floor", type=float, default=0.001)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    root = args.root
    add_paths(root)
    from obvfut_portable_v1 import obv_model as v1_portfolio  # type: ignore

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    output_dir = args.output_dir or (root / "state" / "reports" / "portfolio_overlay" / f"{args.start_date}_to_{args.end_date}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")
    loaded = load_rows(root)
    manifest = load_contract_manifest(root)
    margins = load_margin_lookup(root)
    legs_by_tranche = build_legs(loaded.get("rows_by_tranche") or {}, manifest, margins)
    required_keys: set[str] = set()
    for legs in legs_by_tranche.values():
        for leg in legs:
            required_keys.add(leg.signal_key)
            required_keys.add(leg.execution_key)
    stream_paths = discover_stream_paths(root, start_date, end_date)
    index, input_report = load_quote_index(root, stream_paths, required_keys)
    dates = [parse_date(day) for day, _path in stream_paths]
    replacement_deltas = [float(item) for item in args.replacement_deltas.split(",") if item.strip()]
    min_scores = [float(item) for item in args.min_scores.split(",") if item.strip()]
    min_hold_values = [int(float(item)) for item in args.min_hold_minutes.split(",") if item.strip()]
    persist_values = [max(1, int(float(item))) for item in args.replacement_persist_clocks.split(",") if item.strip()]
    weakest_score_ceilings: list[float | None] = []
    for item in args.weakest_score_ceilings.split(","):
        text = item.strip().lower()
        if not text:
            continue
        weakest_score_ceilings.append(None if text in {"none", "null", "na"} else float(text))
    max_age_by_tranche = parse_tranche_float_map(args.max_leg_age_minutes, default=None)
    min_edge_cost_by_tranche = parse_tranche_float_map(args.min_edge_cost_multiple, default=None)
    age_penalty_by_tranche = parse_tranche_float_map(args.age_score_penalty, default=None)
    min_session_runway_by_tranche = parse_tranche_float_map(args.min_minutes_to_session_end, default=None)
    all_summaries: list[dict[str, Any]] = []
    best_by_portfolio: dict[str, dict[str, Any]] = {}
    manifest_report = {
        "schema": "obvfutport_v2.tranche_portfolio_overlay_backtest.v1",
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "root": str(root),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "input_rule": {
            "eligible_universe": "open T2 or T3 v2 dashboard-normalized tranche rows at each 1-minute clock",
            "score_cutoff": "uses target-stream data up to one minute before the evaluation clock",
            "score_direction": "direction-independent; long profitable momentum and short profitable momentum both score positive",
            "score_formula": "final_score = weight10*percentile(ram10) + weight60*percentile(ram60), where ramN = directional_return_N / max(realized_vol_N, risk_floor)",
            "replacement_rule": "pct mode: new_score > weakest_score * (1 + threshold); gap mode: new_score > weakest_score + threshold",
            "churn_controls": {
                "min_hold_minutes": "holding must age at least this many minutes before replacement; underlying tranche exits still happen",
                "replacement_persist_clocks": "same candidate must beat the same weakest holding for this many consecutive clocks",
                "weakest_score_ceiling": "replacement allowed only if weakest held score is <= this ceiling; none disables this gate",
            },
            "forward_edge_controls": {
                "max_leg_age_minutes": max_age_by_tranche,
                "min_positive_ram_count": args.min_positive_ram_count,
                "min_edge_cost_multiple": min_edge_cost_by_tranche,
                "age_score_penalty": age_penalty_by_tranche,
                "min_minutes_to_session_end": min_session_runway_by_tranche,
            },
            "lookback_basis": "last N available trading-minute target-stream observations",
            "execution": "futures execution key; v1 bid/ask proxy slippage and futures accounting",
            "sizing": "margin based, max 20 percent current portfolio equity per entry, integer futures lots only",
        },
        "tranche_leg_counts": {key: len(value) for key, value in legs_by_tranche.items()},
        "required_key_count": len(required_keys),
        "input_stream": input_report,
        "output_dir": str(output_dir),
    }
    write_json(output_dir / "input_manifest.json", manifest_report)

    for tranche in ("T2", "T3"):
        for replacement_delta in replacement_deltas:
            for min_score in min_scores:
                for min_hold in min_hold_values:
                    for persist in persist_values:
                        for weakest_ceiling in weakest_score_ceilings:
                            result = run_portfolio(
                                tranche=tranche,
                                legs=legs_by_tranche[tranche],
                                dates=dates,
                                index=index,
                                v1_portfolio=v1_portfolio,
                                initial_capital=float(args.initial_capital),
                                max_positions=int(args.max_positions),
                                replacement_delta=float(replacement_delta),
                                replacement_mode=args.replacement_mode,
                                min_hold_minutes=int(min_hold),
                                replacement_persist_clocks=int(persist),
                                weakest_score_ceiling=weakest_ceiling,
                                max_leg_age_minutes=max_age_by_tranche.get(tranche),
                                min_positive_ram_count=int(args.min_positive_ram_count),
                                min_edge_cost_multiple=min_edge_cost_by_tranche.get(tranche),
                                age_score_penalty=age_penalty_by_tranche.get(tranche),
                                min_minutes_to_session_end=min_session_runway_by_tranche.get(tranche),
                                min_score=float(min_score),
                                weight_10=float(args.weight_10),
                                risk_floor=float(args.risk_floor),
                            )
                            summary = result["summary"]
                            all_summaries.append(summary)
                            ceiling_slug = "none" if weakest_ceiling is None else f"{weakest_ceiling:.2f}"
                            slug = (
                                f"{tranche}_delta{replacement_delta:.2f}_min{min_score:.2f}"
                                f"_hold{int(min_hold)}_persist{int(persist)}_weak{ceiling_slug}"
                            ).replace(".", "p")
                            write_json(output_dir / f"{slug}_summary.json", summary)
                            write_jsonl(output_dir / f"{slug}_transactions.jsonl", result["transactions"])
                            write_json(output_dir / f"{slug}_open_positions.json", result["open_positions"])
                            current_best = best_by_portfolio.get(tranche)
                            if current_best is None or float(summary.get("ending_equity_rupees") or 0.0) > float(current_best.get("ending_equity_rupees") or 0.0):
                                best_by_portfolio[tranche] = summary
    write_summary_csv(output_dir / "portfolio_overlay_summary.csv", all_summaries)
    write_json(output_dir / "best_by_portfolio.json", best_by_portfolio)
    latest = root / "state" / "reports" / "portfolio_overlay" / "latest.json"
    write_json(latest, {"output_dir": str(output_dir), "best_by_portfolio": best_by_portfolio, "input_manifest": manifest_report})
    print(json.dumps({"output_dir": str(output_dir), "best_by_portfolio": best_by_portfolio}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
