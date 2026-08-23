#!/usr/bin/env python3
"""Risk-first scorer for OBVFUTPORT-v2 T1 threshold candidates.

The fast T1 recalibration precompute emits candidate entry edges. This scorer
uses the frozen v1 fill/accounting/exit helpers to score those edges through
the unchanged T1/T2/T3 exit stack against the v2 quote-valid compact stream.
It is evidence-only: it writes reports and never mutates live ledgers/state.
"""

from __future__ import annotations

import argparse
import bisect
import itertools
import json
import math
import statistics
import sys
import time
from array import array
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PACKAGE_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from obvfut_portable_v2.passive_runner import (  # noqa: E402
    OnlineObvState,
    PassiveV2Runner,
    as_float,
    atomic_write_json,
    clock_epochs_for_day,
    epoch_ist_iso,
    iter_target_stream_normalized_rows,
    json_clean,
    load_v1_portfolio_module,
    read_json,
)


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def date_range(start: str, end: str, *, skip_weekends: bool = True) -> list[str]:
    current = date.fromisoformat(start)
    final = date.fromisoformat(end)
    out: list[str] = []
    while current <= final:
        if not skip_weekends or current.weekday() < 5:
            out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def stage_timer(report: dict[str, Any], stage: str):
    class _Timer:
        def __enter__(self) -> "_Timer":
            self.started = time.perf_counter()
            return self

        def __exit__(self, *_exc: object) -> None:
            report.setdefault("stage_timings", {})[stage] = round(time.perf_counter() - self.started, 4)

    return _Timer()


def target_stream_candidates(config: dict[str, Any], trade_date: str) -> list[Path]:
    filename = f"target_quotes_{trade_date}.jsonl"
    candidates: list[Path] = []
    state_dir = Path(str(config.get("state_dir") or ""))
    if state_dir:
        candidates.append(state_dir / "target_stream" / trade_date / filename)
    configured_root = Path(str(config.get("target_stream_root") or ""))
    if configured_root:
        candidates.append(configured_root / trade_date / filename)
    local_root = Path(str(config.get("target_stream_root_local") or ""))
    if local_root:
        candidates.append(local_root / trade_date / filename)
    out: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def target_stream_path(config: dict[str, Any], trade_date: str) -> Path:
    candidates = target_stream_candidates(config, trade_date)
    existing = [path for path in candidates if path.exists()]
    if not existing:
        return candidates[0]
    return max(existing, key=lambda path: path.stat().st_size)


def prepare_runner(config_path: Path, output_dir: Path) -> PassiveV2Runner:
    cfg = read_json(config_path, {})
    tmp_state = output_dir / "_runner_state"
    tmp_state.mkdir(parents=True, exist_ok=True)
    cfg["state_dir"] = str(tmp_state)
    cfg["state_dir_local"] = str(tmp_state)
    cfg["bootstrap_load_enabled"] = False
    cfg["skip_past_due_clocks_on_start"] = False
    cfg["second_row_retention_seconds"] = 0
    cfg["flat_second_row_retention_seconds"] = 0
    cfg["pending_second_row_retention_seconds"] = 0
    cfg["active_second_row_retention_seconds"] = 0
    tmp_config = output_dir / "_runtime_recalibration_score_tmp.json"
    atomic_write_json(tmp_config, cfg)
    return PassiveV2Runner(tmp_config)


@dataclass
class PathArrays:
    epochs: array = field(default_factory=lambda: array("d"))
    prices: array = field(default_factory=lambda: array("d"))
    bids: array = field(default_factory=lambda: array("d"))
    asks: array = field(default_factory=lambda: array("d"))
    received_epochs: array = field(default_factory=lambda: array("d"))

    def append(self, row: dict[str, Any]) -> None:
        self.epochs.append(float(row["epoch_second"]))
        self.prices.append(float(row["price"]))
        bid = as_float(row.get("bid"))
        ask = as_float(row.get("ask"))
        received = as_float(row.get("received_epoch"))
        self.bids.append(float(bid) if bid is not None else math.nan)
        self.asks.append(float(ask) if ask is not None else math.nan)
        self.received_epochs.append(float(received) if received is not None else math.nan)

    def __len__(self) -> int:
        return len(self.epochs)

    def row(self, idx: int) -> dict[str, Any]:
        epoch = int(self.epochs[idx])
        bid = float(self.bids[idx])
        ask = float(self.asks[idx])
        received = float(self.received_epochs[idx])
        return {
            "trade_date": epoch_ist_iso(epoch)[:10] if epoch_ist_iso(epoch) else None,
            "epoch_second": epoch,
            "epoch": float(epoch),
            "received_at_ist": epoch_ist_iso(received) if math.isfinite(received) else "",
            "exchange_timestamp": epoch_ist_iso(epoch),
            "received_epoch": received if math.isfinite(received) else None,
            "price": float(self.prices[idx]),
            "bid": bid if math.isfinite(bid) else None,
            "ask": ask if math.isfinite(ask) else None,
            "spread": (ask - bid) if math.isfinite(bid) and math.isfinite(ask) else None,
        }

    def rows_between(self, start_idx: int, end_idx_exclusive: int) -> list[dict[str, Any]]:
        return [self.row(idx) for idx in range(max(0, start_idx), min(len(self), end_idx_exclusive))]


def selected_metas(runner: PassiveV2Runner, symbols: list[str], max_symbols: int | None) -> list[Any]:
    if symbols:
        missing = [symbol for symbol in symbols if symbol not in runner.instruments]
        if missing:
            raise SystemExit(f"Unknown symbols in v2 universe: {', '.join(missing)}")
        metas = [runner.instruments[symbol] for symbol in symbols]
    else:
        metas = list(runner.instruments.values())
    if max_symbols is not None:
        metas = metas[: max(0, int(max_symbols))]
    return metas


def date_start_epoch(trade_date: str) -> int:
    from datetime import datetime

    return int(datetime.fromisoformat(f"{trade_date}T00:00:00+05:30").timestamp())


def load_candidates(
    path: Path,
    selected_symbols: set[str],
    *,
    start_epoch: int,
    end_epoch: int,
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            if not raw.strip():
                continue
            item = json.loads(raw)
            symbol = str(item.get("symbol") or "")
            if symbol not in selected_symbols:
                continue
            signal_epoch = int(item.get("signal_epoch") or 0)
            if signal_epoch < int(start_epoch) or signal_epoch > int(end_epoch):
                continue
            item["candidate_id"] = f"{symbol}|{item.get('variant')}|{item.get('module')}|{item.get('side')}|{int(item.get('signal_epoch') or 0)}"
            out[symbol].append(item)
    for rows in out.values():
        rows.sort(key=lambda row: (int(row.get("signal_epoch") or 0), str(row.get("variant") or "")))
    return out


def parse_variant(item: dict[str, Any]) -> dict[str, Any]:
    variant = str(item.get("variant") or "")
    if variant.startswith("primary_abs_"):
        return {"primary_abs": float(variant.removeprefix("primary_abs_"))}
    if variant.startswith("fresh_m") and "_longp" in variant:
        left, right = variant.removeprefix("fresh_m").split("_longp", 1)
        return {"fresh_multiplier": float(left), "long_pct": float(right)}
    if variant.startswith("fresh_m") and "_shortp" in variant:
        left, right = variant.removeprefix("fresh_m").split("_shortp", 1)
        return {"fresh_multiplier": float(left), "short_pct": float(right)}
    return {}


def configured_fresh_multiplier(point_config: dict[str, Any] | None) -> float:
    cfg = point_config or {}
    payload = cfg.get("point_thresholds") if isinstance(cfg.get("point_thresholds"), dict) else cfg
    fresh = payload.get("fresh_breakout") if isinstance(payload.get("fresh_breakout"), dict) else {}
    value = as_float(fresh.get("multiplier"))
    return float(value if value is not None else 1.4)


def configured_primary_short(point_config: dict[str, Any] | None) -> float:
    cfg = point_config or {}
    value = as_float(cfg.get("primary_obv_short_abs_threshold"))
    return float(value if value is not None else 1.5)


def combo_variants(combo: dict[str, Any]) -> set[str]:
    return {
        f"primary_abs_{combo['primary_abs']:g}",
        f"fresh_m{combo['fresh_multiplier']:g}_longp{combo['long_pct']:g}",
        f"fresh_m{combo['fresh_multiplier']:g}_shortp{combo['short_pct']:g}",
    }


def combo_label(combo: dict[str, Any]) -> str:
    return (
        f"primary_abs={combo['primary_abs']:g}|"
        f"fresh_m={combo['fresh_multiplier']:g}|"
        f"long_pct={combo['long_pct']:g}|"
        f"short_pct={combo['short_pct']:g}"
    )


def build_combos(meta: Any, args: argparse.Namespace) -> list[dict[str, Any]]:
    primary_values = [float(x) for x in parse_csv(args.primary_short_thresholds)]
    multiplier_values = [float(x) for x in parse_csv(args.fresh_breakout_multipliers)]
    long_values = [float(x) for x in parse_csv(args.long_strength_pcts)]
    short_values = [float(x) for x in parse_csv(args.short_weakness_pcts)]
    combos = [
        {
            "primary_abs": primary,
            "fresh_multiplier": multiplier,
            "long_pct": long_pct,
            "short_pct": short_pct,
        }
        for primary, multiplier, long_pct, short_pct in itertools.product(
            primary_values,
            multiplier_values,
            long_values,
            short_values,
        )
    ]
    current = {
        "primary_abs": configured_primary_short(meta.signal_point_config),
        "fresh_multiplier": configured_fresh_multiplier(meta.signal_point_config),
        "long_pct": float(args.current_long_strength_pct),
        "short_pct": float(args.current_short_weakness_pct),
    }
    if combo_label(current) not in {combo_label(combo) for combo in combos}:
        combos.append(current)
    return combos


def margin_for(meta: Any, side: str, entry_price: float | None = None) -> float | None:
    margin = meta.margin_long if str(side).lower() == "long" else meta.margin_short
    parsed = as_float(margin)
    if parsed is not None and parsed > 0:
        return float(parsed)
    if entry_price is None:
        return None
    return float(entry_price) * int(meta.lot_size or 1) * 0.15


def first_price_exit(
    *,
    rows: PathArrays,
    start_idx: int,
    side: str,
    entry_price: float,
    hard_sl: float,
    trail_activation: float,
    trail_giveback_fraction: float,
) -> dict[str, Any] | None:
    max_favorable = 0.0
    max_adverse = 0.0
    for idx in range(start_idx, len(rows)):
        price = float(rows.prices[idx])
        gross = price - entry_price if side == "long" else entry_price - price
        max_favorable = max(max_favorable, gross, 0.0)
        max_adverse = max(max_adverse, -gross, 0.0)
        hard_hit = price <= entry_price - hard_sl if side == "long" else price >= entry_price + hard_sl
        giveback = max_favorable - gross
        trail_hit = max_favorable >= trail_activation and giveback >= trail_giveback_fraction * max_favorable
        if hard_hit or trail_hit:
            return {
                "idx": idx,
                "row": rows.row(idx),
                "reason": "hard_sl" if hard_hit else "profit_trailing_sl",
                "mfe_points": max_favorable,
                "mae_points": max_adverse,
            }
    return None


def signed_points(side: str, entry_price: float, exit_price: float) -> float:
    return exit_price - entry_price if str(side).lower() == "long" else entry_price - exit_price


def candidate_position(
    *,
    runner: PassiveV2Runner,
    v1: Any,
    meta: Any,
    candidate: dict[str, Any],
    entry_row: dict[str, Any],
    entry_fill: dict[str, Any],
    hard_sl: float,
    trail_activation: float,
) -> dict[str, Any]:
    signal_epoch = int(candidate["signal_epoch"])
    side = str(candidate["side"])
    signal_id = runner.entry_event_from_edge(meta, candidate).get("signal_id")
    position = {
        "side": side,
        "signal_id": signal_id,
        "position_id": f"{signal_id}:position",
        "instrument_key": meta.execution_key,
        "contract_label": meta.execution_contract_label,
        "lifecycle_start_date": meta.lifecycle_start_date,
        "expiry_date": meta.expiry_date,
        "signal_source": meta.signal_source,
        "signal_instrument_key": meta.signal_key,
        "signal_contract_label": meta.signal_contract_label,
        "source": candidate.get("module"),
        "variant": candidate.get("variant"),
        "signal_epoch": signal_epoch,
        "signal_time": candidate.get("signal_time") or epoch_ist_iso(signal_epoch),
        "signal_price": candidate.get("signal_price"),
        "entry_epoch": int(entry_row["epoch_second"]),
        "entry_time": epoch_ist_iso(entry_row["epoch_second"]),
        "entry_row_time": entry_row.get("received_at_ist"),
        "entry_due_epoch": signal_epoch + int(runner.config.get("entry_delay_seconds") or 60),
        "entry_due_time": epoch_ist_iso(signal_epoch + int(runner.config.get("entry_delay_seconds") or 60)),
        "entry_price": entry_fill.get("ltp_price"),
        "entry_ltp_price": entry_fill.get("ltp_price"),
        "entry_fill_price": entry_fill.get("fill_price"),
        "execution_entry_price": entry_fill.get("ltp_price"),
        "execution_entry_ltp_price": entry_fill.get("ltp_price"),
        "execution_entry_fill_price": entry_fill.get("fill_price"),
        **v1.apply_fill_metadata("entry", entry_fill),
        "hard_sl_points": hard_sl,
        "trail_activation_points": trail_activation,
        "trail_activation_effective_points": v1.effective_trail_activation_points(
            hard_sl,
            trail_activation,
            point_config=meta.execution_point_config,
        ),
        "max_favorable_points": 0.0,
        "max_adverse_points": 0.0,
        "status": "open",
        "entry_margin_used_rupees": margin_for(meta, side, as_float(entry_fill.get("ltp_price"))),
        "lot_size": int(meta.lot_size or 1),
        "accounting_model": "bid_ask_proxy_slippage_zerodha_futures",
    }
    position = v1._ensure_live_two_lot_ttsl(dict(position), config=None, lot_size=int(meta.lot_size or 1))
    position = v1._ensure_live_tranche3(dict(position), config=None, lot_size=int(meta.lot_size or 1))
    return position


def outcome_nets(position: dict[str, Any], exit_event: dict[str, Any] | None) -> dict[str, Any]:
    t1_net = as_float(exit_event.get("net_rupees")) if isinstance(exit_event, dict) else as_float(position.get("net_rupees_if_closed"))
    t2 = (position.get("two_lot_ttsl") or {}).get("tranche2") if isinstance(position.get("two_lot_ttsl"), dict) else {}
    t3 = position.get("tranche3") if isinstance(position.get("tranche3"), dict) else {}
    t2_net = as_float(t2.get("net_rupees")) if isinstance(t2, dict) else None
    t3_net = as_float(t3.get("net_rupees")) if isinstance(t3, dict) else None
    return {
        "t1_net_rupees": t1_net,
        "t2_net_rupees": t2_net,
        "t3_net_rupees": t3_net,
        "two_lot_net_rupees": (t1_net or 0.0) + (t2_net or 0.0),
        "three_lot_net_rupees": (t1_net or 0.0) + (t2_net or 0.0) + (t3_net or 0.0),
        "t2_status": t2.get("status") if isinstance(t2, dict) else None,
        "t2_exit_source": t2.get("exit_source") if isinstance(t2, dict) else None,
        "t3_status": t3.get("status") if isinstance(t3, dict) else None,
        "t3_entered": bool(isinstance(t3, dict) and t3.get("entry_epoch")),
    }


def simulate_candidate(
    *,
    runner: PassiveV2Runner,
    v1: Any,
    meta: Any,
    candidate: dict[str, Any],
    exec_rows: PathArrays,
    signal_clock_rows: list[dict[str, Any]],
    execution_clock_rows: list[dict[str, Any]],
    end_epoch: int,
) -> dict[str, Any]:
    import pandas as pd  # type: ignore

    signal_epoch = int(candidate.get("signal_epoch") or 0)
    side = str(candidate.get("side") or "")
    due_epoch = signal_epoch + int(runner.config.get("entry_delay_seconds") or 60)
    entry_idx = bisect.bisect_left(exec_rows.epochs, float(due_epoch))
    if entry_idx >= len(exec_rows):
        return {**candidate, "status": "entry_unavailable", "entry_due_epoch": due_epoch}
    entry_row = exec_rows.row(entry_idx)
    entry_fill = v1.execution_fill_from_row(
        entry_row,
        side=side,
        phase="entry",
        point_config=meta.execution_point_config,
        fallback_round_trip_cost_points=float(meta.round_trip_cost_points),
    )
    entry_fill_price = as_float(entry_fill.get("fill_price"))
    entry_ltp_price = as_float(entry_fill.get("ltp_price"))
    if entry_fill_price is None or entry_ltp_price is None:
        return {**candidate, "status": "entry_fill_unavailable", "entry_due_epoch": due_epoch}
    execution_clock_state = pd.DataFrame(execution_clock_rows)
    signal_clock_state = pd.DataFrame(signal_clock_rows)
    hard_sl = v1.dynamic_risk_points(
        execution_clock_state,
        signal_epoch,
        kind="hard_sl",
        point_config=meta.execution_point_config,
    )
    trail_activation = v1.dynamic_risk_points(
        execution_clock_state,
        signal_epoch,
        kind="trail_activation",
        point_config=meta.execution_point_config,
    )
    if not math.isfinite(float(hard_sl)) or not math.isfinite(float(trail_activation)):
        return {**candidate, "status": "risk_points_unavailable", "entry_due_epoch": due_epoch}
    position = candidate_position(
        runner=runner,
        v1=v1,
        meta=meta,
        candidate=candidate,
        entry_row=entry_row,
        entry_fill=entry_fill,
        hard_sl=float(hard_sl),
        trail_activation=float(trail_activation),
    )
    settings = v1.exit_profile_settings(meta.execution_point_config)
    price_exit = first_price_exit(
        rows=exec_rows,
        start_idx=entry_idx,
        side=side,
        entry_price=float(entry_ltp_price),
        hard_sl=float(hard_sl),
        trail_activation=float(position["trail_activation_effective_points"]),
        trail_giveback_fraction=float(settings["trail_giveback_fraction"]),
    )
    price_exit_epoch = int(price_exit["row"]["epoch_second"]) if price_exit else None
    cutoff_for_exhaustion = (price_exit_epoch - 1) if price_exit_epoch else end_epoch
    end_idx_exclusive = bisect.bisect_right(exec_rows.epochs, float(cutoff_for_exhaustion))
    path_rows = exec_rows.rows_between(entry_idx, end_idx_exclusive)
    if not path_rows:
        path_rows = [entry_row]
    clock_summary = runner._compact_clock_path_summary(
        position=position,
        second_rows=path_rows,
        clock_rows=execution_clock_rows,
        point_config=meta.execution_point_config,
        cutoff_epoch=cutoff_for_exhaustion,
    )
    exhaustion_exit, exhaustion_status = runner._compact_first_exhaustion_exit(
        signal_clock_state=signal_clock_state,
        execution_clock_summary=clock_summary,
        position=position,
        point_config=meta.execution_point_config,
    )
    if exhaustion_exit and exhaustion_exit.get("exit_row") is not None:
        base_exit_row = dict(exhaustion_exit["exit_row"])
        base_exit_reason = str(exhaustion_exit["exit_reason"])
        base_exit_epoch = int(base_exit_row["epoch_second"])
    elif price_exit:
        base_exit_row = dict(price_exit["row"])
        base_exit_reason = str(price_exit["reason"])
        base_exit_epoch = int(base_exit_row["epoch_second"])
    else:
        latest_idx = min(len(exec_rows) - 1, bisect.bisect_right(exec_rows.epochs, float(end_epoch)) - 1)
        base_exit_row = exec_rows.row(latest_idx) if latest_idx >= entry_idx else entry_row
        base_exit_reason = "open_mark_if_closed"
        base_exit_epoch = int(base_exit_row["epoch_second"])

    final_epoch_for_tranches = max(int(position["entry_epoch"]), int(base_exit_epoch) - 1)
    tranche_end_idx = bisect.bisect_right(exec_rows.epochs, float(final_epoch_for_tranches))
    tranche_path_rows = exec_rows.rows_between(entry_idx, tranche_end_idx)
    if not tranche_path_rows:
        tranche_path_rows = [entry_row]
    latest_fill_for_mark = v1.execution_fill_from_row(
        base_exit_row,
        side=side,
        phase="exit",
        point_config=meta.execution_point_config,
        fallback_round_trip_cost_points=float(meta.round_trip_cost_points),
    )
    latest_exit_fill_price = as_float(latest_fill_for_mark.get("fill_price"))
    position, tranche2_events = v1._update_live_two_lot_ttsl(
        position=position,
        path=pd.DataFrame(tranche_path_rows),
        clock_state=execution_clock_state,
        latest_exit_fill_price=latest_exit_fill_price,
        latest_exit_time=epoch_ist_iso(base_exit_epoch),
        cost_points=float(meta.round_trip_cost_points),
        lot_size=int(meta.lot_size or 1),
        point_config=meta.execution_point_config,
        config=None,
        final_epoch=final_epoch_for_tranches,
    )
    tranche2_exit_event = next(
        (event for event in tranche2_events if isinstance(event, dict) and event.get("event") == "tranche2_exit"),
        None,
    )
    tranche3_final_epoch = (
        max(0, int(tranche2_exit_event.get("exit_epoch") or 0) - 1)
        if tranche2_exit_event and tranche2_exit_event.get("exit_epoch") is not None
        else final_epoch_for_tranches
    )
    position, tranche3_events = v1._update_live_tranche3(
        position=position,
        path=pd.DataFrame(tranche_path_rows),
        clock_state=execution_clock_state,
        latest_exit_fill_price=latest_exit_fill_price,
        latest_exit_time=epoch_ist_iso(base_exit_epoch),
        cost_points=float(meta.round_trip_cost_points),
        lot_size=int(meta.lot_size or 1),
        point_config=meta.execution_point_config,
        config=None,
        final_epoch=tranche3_final_epoch or None,
    )
    if tranche2_exit_event:
        position, t3_from_t2 = v1._live_tranche3_close_from_event(
            position=position,
            exit_event=tranche2_exit_event,
            exit_source="ttsl_exit",
            exit_reason="tranche3_v1_ttsl_exit",
            lot_size=int(meta.lot_size or 1),
            point_config=meta.execution_point_config,
        )
        if t3_from_t2:
            tranche3_events.append(t3_from_t2)

    path_prices = [float(row["price"]) for row in exec_rows.rows_between(entry_idx, bisect.bisect_right(exec_rows.epochs, float(base_exit_epoch)))]
    if path_prices:
        if side == "long":
            position["max_favorable_points"] = max(0.0, max(path_prices) - float(entry_ltp_price))
            position["max_adverse_points"] = max(0.0, float(entry_ltp_price) - min(path_prices))
        else:
            position["max_favorable_points"] = max(0.0, float(entry_ltp_price) - min(path_prices))
            position["max_adverse_points"] = max(0.0, max(path_prices) - float(entry_ltp_price))

    exit_event: dict[str, Any] | None = None
    exit_fill = v1.execution_fill_from_row(
        base_exit_row,
        side=side,
        phase="exit",
        point_config=meta.execution_point_config,
        fallback_round_trip_cost_points=float(meta.round_trip_cost_points),
    )
    exit_fill_price = as_float(exit_fill.get("fill_price"))
    exit_ltp_price = as_float(exit_fill.get("ltp_price"))
    if exit_fill_price is not None and exit_ltp_price is not None:
        accounting = v1.futures_trade_accounting(
            side=side,
            entry_fill_price=float(entry_fill_price),
            exit_fill_price=float(exit_fill_price),
            lot_size=int(meta.lot_size or 1),
            point_config=meta.execution_point_config,
        )
        position.update(
            {
                "latest_price": exit_ltp_price,
                "latest_time": epoch_ist_iso(base_exit_epoch),
                "latest_epoch": base_exit_epoch,
                "latest_fill_price_if_closed": exit_fill_price,
                "gross_points": float(accounting["gross_points"]),
                "gross_rupees_if_closed": accounting["gross_rupees"],
                "charges_rupees_if_closed": accounting["charges_rupees"],
                "net_points_if_closed": float(accounting["net_points"]),
                "net_rupees_if_closed": accounting["net_rupees"],
            }
        )
        exit_event = {
            "event": "paper_exit",
            "signal_id": position.get("signal_id"),
            "position_id": position.get("position_id"),
            "exit_reason": base_exit_reason,
            "side": side,
            "instrument_key": meta.execution_key,
            "contract_label": meta.execution_contract_label,
            "signal_source": meta.signal_source,
            "signal_instrument_key": meta.signal_key,
            "signal_contract_label": meta.signal_contract_label,
            "lifecycle_start_date": meta.lifecycle_start_date,
            "expiry_date": meta.expiry_date,
            "entry_price": entry_ltp_price,
            "entry_ltp_price": entry_ltp_price,
            "entry_fill_price": entry_fill_price,
            "entry_time": position.get("entry_time"),
            "entry_epoch": int(position.get("entry_epoch") or 0),
            "exit_price": exit_ltp_price,
            "exit_ltp_price": exit_ltp_price,
            "exit_fill_price": exit_fill_price,
            "exit_time": epoch_ist_iso(base_exit_epoch),
            "exit_epoch": base_exit_epoch,
            "model_gross_points": signed_points(side, float(entry_ltp_price), float(exit_ltp_price)),
            "gross_points": float(accounting["gross_points"]),
            "gross_rupees": accounting["gross_rupees"],
            "charges_rupees": accounting["charges_rupees"],
            "charge_breakdown": accounting["charge_breakdown"],
            "net_points": float(accounting["net_points"]),
            "net_rupees": accounting["net_rupees"],
            **v1.apply_fill_metadata("entry", entry_fill),
            **v1.apply_fill_metadata("exit", exit_fill),
            "source": candidate.get("module"),
            "variant": candidate.get("variant"),
            "signal_epoch": signal_epoch,
            "signal_time": candidate.get("signal_time"),
            "signal_price": candidate.get("signal_price"),
            "hard_sl_points": float(hard_sl),
            "trail_activation_points": float(trail_activation),
            "trail_activation_effective_points": float(position["trail_activation_effective_points"]),
            "max_favorable_points": position.get("max_favorable_points"),
            "max_adverse_points": position.get("max_adverse_points"),
        }
        if base_exit_reason != "open_mark_if_closed":
            exit_event = v1._finalize_live_two_lot_on_base_exit(
                position=position,
                exit_event=exit_event,
                lot_size=int(meta.lot_size or 1),
                point_config=meta.execution_point_config,
            )
            position, exit_event, tranche3_base_events = v1._finalize_live_tranche3_on_base_exit(
                position=position,
                exit_event=exit_event,
                lot_size=int(meta.lot_size or 1),
                point_config=meta.execution_point_config,
            )
            for event in tranche3_base_events:
                if isinstance(event, dict):
                    tranche3_events.append(event)
        else:
            position = v1._refresh_live_two_lot_marks(
                position,
                latest_exit_fill_price=exit_fill_price,
                latest_exit_time=epoch_ist_iso(base_exit_epoch),
                lot_size=int(meta.lot_size or 1),
                point_config=meta.execution_point_config,
            )
            position = v1._refresh_live_tranche3_marks(
                position,
                latest_exit_fill_price=exit_fill_price,
                latest_exit_time=epoch_ist_iso(base_exit_epoch),
                lot_size=int(meta.lot_size or 1),
                point_config=meta.execution_point_config,
            )

    nets = outcome_nets(position, exit_event)
    one_margin = margin_for(meta, side, float(entry_ltp_price))
    t3_entered = bool(nets.get("t3_entered"))
    return {
        **candidate,
        "status": "closed" if base_exit_reason != "open_mark_if_closed" else "open_mark",
        "entry_due_epoch": due_epoch,
        "entry_epoch": int(position.get("entry_epoch") or 0),
        "entry_time": position.get("entry_time"),
        "entry_ltp_price": entry_ltp_price,
        "entry_fill_price": entry_fill_price,
        "exit_epoch": base_exit_epoch,
        "exit_time": epoch_ist_iso(base_exit_epoch),
        "exit_reason": base_exit_reason,
        "exit_ltp_price": exit_ltp_price,
        "exit_fill_price": exit_fill_price,
        "one_lot_margin_rupees": one_margin,
        "two_lot_margin_rupees": 2.0 * one_margin if one_margin else None,
        "three_lot_peak_margin_rupees": (3.0 if t3_entered else 2.0) * one_margin if one_margin else None,
        "max_favorable_points": position.get("max_favorable_points"),
        "max_adverse_points": position.get("max_adverse_points"),
        **nets,
        "one_lot_net_pct_margin": (100.0 * nets["t1_net_rupees"] / one_margin) if one_margin and nets.get("t1_net_rupees") is not None else None,
        "two_lot_net_pct_margin": (100.0 * nets["two_lot_net_rupees"] / (2.0 * one_margin)) if one_margin else None,
        "three_lot_net_pct_margin": (
            100.0 * nets["three_lot_net_rupees"] / ((3.0 if t3_entered else 2.0) * one_margin)
        )
        if one_margin
        else None,
        "exhaustion_status": exhaustion_status,
    }


def max_drawdown(values: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    worst = 0.0
    for value in values:
        equity += float(value)
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * q)))
    return float(ordered[idx])


def summarize_sequence(rows: list[dict[str, Any]], *, metric_prefix: str = "three_lot") -> dict[str, Any]:
    if not rows:
        return {
            "trade_count": 0,
            "closed_count": 0,
            "open_count": 0,
            "success_rate_pct": None,
            "total_net_rupees": 0.0,
        }
    net_key = f"{metric_prefix}_net_rupees"
    pct_key = f"{metric_prefix}_net_pct_margin"
    ordered = sorted(rows, key=lambda row: int(row.get("exit_epoch") or row.get("entry_epoch") or 0))
    nets = [float(row.get(net_key) or 0.0) for row in ordered]
    pcts = [float(row[pct_key]) for row in ordered if row.get(pct_key) is not None and math.isfinite(float(row[pct_key]))]
    closed = [row for row in ordered if row.get("status") == "closed"]
    wins = [row for row in closed if float(row.get(net_key) or 0.0) > 0]
    losses = [row for row in closed if float(row.get(net_key) or 0.0) <= 0]
    loss_nets = [float(row.get(net_key) or 0.0) for row in losses]
    return {
        "trade_count": len(ordered),
        "closed_count": len(closed),
        "open_count": len(ordered) - len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "success_rate_pct": 100.0 * len(wins) / len(closed) if closed else None,
        "total_net_rupees": sum(nets),
        "closed_net_rupees": sum(float(row.get(net_key) or 0.0) for row in closed),
        "open_mark_net_rupees": sum(float(row.get(net_key) or 0.0) for row in ordered if row.get("status") != "closed"),
        "worst_loss_rupees": min(loss_nets) if loss_nets else 0.0,
        "max_drawdown_rupees": max_drawdown(nets),
        "avg_net_pct_margin": statistics.mean(pcts) if pcts else None,
        "median_net_pct_margin": statistics.median(pcts) if pcts else None,
        "min_net_pct_margin": min(pcts) if pcts else None,
        "max_net_pct_margin": max(pcts) if pcts else None,
        "p10_net_pct_margin": percentile(pcts, 0.10),
        "p25_net_pct_margin": percentile(pcts, 0.25),
    }


def score_combo(
    *,
    combo: dict[str, Any],
    candidates: list[dict[str, Any]],
    outcomes: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    active_variants = combo_variants(combo)
    selected = [item for item in candidates if str(item.get("variant") or "") in active_variants]
    selected.sort(key=lambda row: int(row.get("signal_epoch") or 0))
    rows: list[dict[str, Any]] = []
    last_exit_epoch = 0
    for item in selected:
        outcome = outcomes.get(str(item.get("candidate_id")))
        if not outcome or outcome.get("status") not in {"closed", "open_mark"}:
            continue
        entry_epoch = int(outcome.get("entry_epoch") or 0)
        if entry_epoch <= last_exit_epoch:
            continue
        rows.append(outcome)
        exit_epoch = int(outcome.get("exit_epoch") or entry_epoch)
        if outcome.get("status") == "open_mark":
            last_exit_epoch = 10**18
        else:
            last_exit_epoch = max(last_exit_epoch, exit_epoch)
    return {
        "combo": combo,
        "combo_label": combo_label(combo),
        "rows": rows,
        "summary_three_lot": summarize_sequence(rows, metric_prefix="three_lot"),
        "summary_two_lot": summarize_sequence(rows, metric_prefix="two_lot"),
        "summary_one_lot": summarize_sequence(rows, metric_prefix="one_lot"),
    }


def reject_reason(candidate: dict[str, Any], current: dict[str, Any] | None, args: argparse.Namespace) -> str | None:
    summary = candidate["summary_three_lot"]
    trades = int(summary.get("trade_count") or 0)
    closed = int(summary.get("closed_count") or 0)
    success = as_float(summary.get("success_rate_pct"))
    worst_pct = as_float(summary.get("min_net_pct_margin"))
    current_worst_pct = as_float((current or {}).get("summary_three_lot", {}).get("min_net_pct_margin"))
    if trades < int(args.min_trades):
        return "too_few_trades"
    if closed < int(args.min_closed_trades):
        return "too_few_closed_trades"
    if success is not None and success < float(args.min_success_rate_pct):
        return "low_success_rate"
    if current_worst_pct is not None and worst_pct is not None:
        if worst_pct < current_worst_pct - float(args.max_worst_loss_pct_deterioration):
            return "worse_worst_loss_than_current"
    nets = [float(row.get("three_lot_net_rupees") or 0.0) for row in candidate.get("rows") or []]
    positive = [value for value in nets if value > 0]
    if len(nets) <= 2 and sum(positive) > 0:
        return "one_trade_overfit"
    if positive and max(positive) > float(args.max_single_win_share) * max(1.0, sum(positive)):
        return "single_win_dominates"
    return None


def risk_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    s = item["summary_three_lot"]
    return (
        as_float(s.get("min_net_pct_margin")) if as_float(s.get("min_net_pct_margin")) is not None else -9999.0,
        as_float(s.get("max_drawdown_rupees")) if as_float(s.get("max_drawdown_rupees")) is not None else -10**18,
        as_float(s.get("success_rate_pct")) if as_float(s.get("success_rate_pct")) is not None else -1.0,
        as_float(s.get("median_net_pct_margin")) if as_float(s.get("median_net_pct_margin")) is not None else -9999.0,
        as_float(s.get("total_net_rupees")) if as_float(s.get("total_net_rupees")) is not None else -10**18,
        int(s.get("trade_count") or 0),
    )


def return_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    s = item["summary_three_lot"]
    return (
        as_float(s.get("total_net_rupees")) if as_float(s.get("total_net_rupees")) is not None else -10**18,
        as_float(s.get("success_rate_pct")) if as_float(s.get("success_rate_pct")) is not None else -1.0,
        as_float(s.get("min_net_pct_margin")) if as_float(s.get("min_net_pct_margin")) is not None else -9999.0,
        int(s.get("trade_count") or 0),
    )


def promotion_decision(current: dict[str, Any] | None, risk_best: dict[str, Any] | None, args: argparse.Namespace) -> dict[str, Any]:
    if not current or not risk_best:
        return {"decision": "do_not_promote", "reason": "missing_current_or_candidate"}
    if risk_best.get("rejected_reason"):
        return {"decision": "do_not_promote", "reason": risk_best.get("rejected_reason")}
    cur = current["summary_three_lot"]
    best = risk_best["summary_three_lot"]
    cur_worst = as_float(cur.get("min_net_pct_margin"))
    best_worst = as_float(best.get("min_net_pct_margin"))
    cur_success = as_float(cur.get("success_rate_pct"))
    best_success = as_float(best.get("success_rate_pct"))
    cur_net = as_float(cur.get("total_net_rupees")) or 0.0
    best_net = as_float(best.get("total_net_rupees")) or 0.0
    if best_worst is None or cur_worst is None or best_success is None or cur_success is None:
        return {"decision": "do_not_promote", "reason": "insufficient_metrics"}
    worst_improved = best_worst >= cur_worst + float(args.min_worst_loss_pct_improvement)
    success_ok = best_success >= cur_success - float(args.max_success_rate_deterioration_pct)
    net_ok = best_net >= cur_net * float(args.min_net_preservation_ratio)
    if worst_improved and success_ok and net_ok:
        return {
            "decision": "promote_candidate_for_review",
            "reason": "risk_first_improvement",
            "worst_loss_pct_improvement": best_worst - cur_worst,
            "success_rate_delta": best_success - cur_success,
            "net_delta_rupees": best_net - cur_net,
        }
    return {
        "decision": "do_not_promote",
        "reason": "risk_first_gate_not_met",
        "worst_loss_pct_delta": None if best_worst is None or cur_worst is None else best_worst - cur_worst,
        "success_rate_delta": None if best_success is None or cur_success is None else best_success - cur_success,
        "net_delta_rupees": best_net - cur_net,
    }


def serializable_score(item: dict[str, Any], *, include_rows: bool = False) -> dict[str, Any]:
    out = {
        "combo": item.get("combo"),
        "combo_label": item.get("combo_label"),
        "rejected_reason": item.get("rejected_reason"),
        "summary_three_lot": item.get("summary_three_lot"),
        "summary_two_lot": item.get("summary_two_lot"),
        "summary_one_lot": item.get("summary_one_lot"),
    }
    if include_rows:
        out["rows"] = item.get("rows")
    return out


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "schema": "obvfutport_v2.t1_threshold_risk_first_scoring.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "config": str(args.config),
        "candidate_file": str(args.candidate_file),
        "output_dir": str(output_dir),
        "scoring_policy": {
            "primary": "risk_first",
            "reject_candidates_with": {
                "min_trades": args.min_trades,
                "min_closed_trades": args.min_closed_trades,
                "min_success_rate_pct": args.min_success_rate_pct,
                "max_single_win_share": args.max_single_win_share,
            },
            "ranking_order": [
                "best/wleast bad worst loss pct",
                "lowest drawdown",
                "higher success rate",
                "median return per margin",
                "net return",
                "trade count",
            ],
        },
    }
    config = read_json(Path(args.config), {})
    dates = date_range(args.start_date, args.end_date, skip_weekends=not args.no_skip_weekends)
    report["dates"] = dates
    start_epoch = date_start_epoch(args.start_date)
    end_epoch = date_start_epoch(args.end_date) + 24 * 3600 - 1
    with stage_timer(report, "instrument_load"):
        runner = prepare_runner(Path(args.config), output_dir)
        metas = selected_metas(runner, parse_csv(args.symbols), args.max_symbols)
    meta_by_symbol = {meta.symbol: meta for meta in metas}
    candidates_by_symbol = load_candidates(
        Path(args.candidate_file),
        set(meta_by_symbol),
        start_epoch=start_epoch,
        end_epoch=end_epoch,
    )
    report["symbols"] = [meta.symbol for meta in metas]
    report["symbol_count"] = len(metas)
    report["candidate_count_loaded"] = sum(len(items) for items in candidates_by_symbol.values())

    signal_keys = {meta.signal_key for meta in metas}
    execution_keys = {meta.execution_key for meta in metas}
    target_keys = sorted(signal_keys | execution_keys)
    clock_epochs_by_date = {
        trade_date: clock_epochs_for_day(
            date.fromisoformat(trade_date),
            clock_start=str(config.get("clock_start_ist") or "09:20"),
            clock_end=str(config.get("clock_end_ist") or "15:20"),
            clock_step_minutes=int(config.get("clock_step_minutes") or 15),
        )
        for trade_date in dates
    }
    all_clock_epochs = {epoch for epochs in clock_epochs_by_date.values() for epoch in epochs}
    states = {
        key: OnlineObvState(
            key=key,
            clock_epochs=set(all_clock_epochs),
            second_row_retention_seconds=0,
            compute_non_clock_percentiles=False,
        )
        for key in target_keys
    }
    exec_paths = {key: PathArrays() for key in execution_keys}
    scan_stats: dict[str, Any] = {}
    with stage_timer(report, "single_pass_stream_scan"):
        for trade_date in dates:
            path = target_stream_path(config, trade_date)
            started = time.perf_counter()
            rows_used = 0
            exec_rows_used = 0
            if not path.exists():
                scan_stats[trade_date] = {"source_found": False, "path": str(path)}
                continue
            size = path.stat().st_size
            for row in iter_target_stream_normalized_rows(path, trade_date, target_keys):
                key = str(row.get("target") or "")
                state = states.get(key)
                if state is not None:
                    state.process_row(row)
                    rows_used += 1
                path_store = exec_paths.get(key)
                if path_store is not None:
                    path_store.append(row)
                    exec_rows_used += 1
            for state in states.values():
                state.flush_until_latest()
            scan_stats[trade_date] = {
                "source_found": True,
                "path": str(path),
                "size_bytes": size,
                "target_rows_used": rows_used,
                "execution_rows_stored": exec_rows_used,
                "duration_seconds": round(time.perf_counter() - started, 4),
            }
    report["scan_stats"] = scan_stats
    report["target_key_count"] = len(target_keys)
    report["execution_path_rows"] = {key: len(path) for key, path in exec_paths.items()}

    with stage_timer(report, "clock_row_build"):
        for meta in metas:
            signal_state = states.get(meta.signal_key)
            execution_state = states.get(meta.execution_key)
            if signal_state is not None:
                runner.ensure_clock_rows_through(signal_state, meta.signal_point_config)
            if execution_state is not None:
                runner.ensure_clock_rows_through(execution_state, meta.execution_point_config)

    v1 = load_v1_portfolio_module(runner.config)
    replay_end_epoch = max(all_clock_epochs) + 10 * 3600 if all_clock_epochs else end_epoch
    outcomes_by_symbol: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    outcome_rows_written = 0
    outcome_path = output_dir / "candidate_outcomes.jsonl"
    with stage_timer(report, "candidate_outcome_simulation"):
        with outcome_path.open("w", encoding="utf-8") as handle:
            for meta in metas:
                symbol_candidates = candidates_by_symbol.get(meta.symbol) or []
                if not symbol_candidates:
                    continue
                exec_rows = exec_paths.get(meta.execution_key)
                signal_state = states.get(meta.signal_key)
                execution_state = states.get(meta.execution_key)
                if not exec_rows or len(exec_rows) == 0 or signal_state is None or execution_state is None:
                    continue
                for candidate in symbol_candidates:
                    outcome = simulate_candidate(
                        runner=runner,
                        v1=v1,
                        meta=meta,
                        candidate=candidate,
                        exec_rows=exec_rows,
                        signal_clock_rows=signal_state.clock_rows,
                        execution_clock_rows=execution_state.clock_rows,
                        end_epoch=replay_end_epoch,
                    )
                    outcomes_by_symbol[meta.symbol][str(candidate.get("candidate_id"))] = outcome
                    handle.write(json.dumps(json_clean(outcome), sort_keys=True) + "\n")
                    outcome_rows_written += 1
                    if args.max_outcomes and outcome_rows_written >= int(args.max_outcomes):
                        break
                if args.max_outcomes and outcome_rows_written >= int(args.max_outcomes):
                    break
    report["candidate_outcomes_written"] = outcome_rows_written

    symbol_reports: dict[str, Any] = {}
    all_combo_rows: list[dict[str, Any]] = []
    with stage_timer(report, "combo_scoring"):
        for meta in metas:
            symbol_candidates = candidates_by_symbol.get(meta.symbol) or []
            outcomes = outcomes_by_symbol.get(meta.symbol) or {}
            if not symbol_candidates or not outcomes:
                symbol_reports[meta.symbol] = {
                    "symbol": meta.symbol,
                    "status": "no_candidates",
                    "candidate_entries": len(symbol_candidates),
                    "threshold_source": meta.source,
                    "threshold_synthesized": meta.synthesized,
                }
                continue
            combos = build_combos(meta, args)
            current_label = combo_label(
                {
                    "primary_abs": configured_primary_short(meta.signal_point_config),
                    "fresh_multiplier": configured_fresh_multiplier(meta.signal_point_config),
                    "long_pct": float(args.current_long_strength_pct),
                    "short_pct": float(args.current_short_weakness_pct),
                }
            )
            scored = [
                score_combo(combo=combo, candidates=symbol_candidates, outcomes=outcomes)
                for combo in combos
            ]
            current = next((item for item in scored if item["combo_label"] == current_label), None)
            for item in scored:
                item["rejected_reason"] = reject_reason(item, current, args)
            accepted = [item for item in scored if not item.get("rejected_reason")]
            risk_pool = accepted or scored
            risk_best = max(risk_pool, key=risk_sort_key) if risk_pool else None
            return_best = max(scored, key=return_sort_key) if scored else None
            decision = promotion_decision(current, risk_best, args)
            symbol_report = {
                "symbol": meta.symbol,
                "status": "scored",
                "signal_source": meta.signal_source,
                "signal_key": meta.signal_key,
                "execution_key": meta.execution_key,
                "threshold_source": meta.source,
                "threshold_synthesized": meta.synthesized,
                "candidate_entries": len(symbol_candidates),
                "candidate_outcomes": len(outcomes),
                "combo_count": len(scored),
                "current_deployed": serializable_score(current) if current else None,
                "best_risk_first_candidate": serializable_score(risk_best) if risk_best else None,
                "best_return_candidate": serializable_score(return_best) if return_best else None,
                "promotion": decision,
                "top_risk_first_candidates": [
                    serializable_score(item)
                    for item in sorted(scored, key=risk_sort_key, reverse=True)[:5]
                ],
            }
            symbol_reports[meta.symbol] = symbol_report
            if current:
                all_combo_rows.append({"symbol": meta.symbol, "kind": "current", **serializable_score(current)})
            if risk_best:
                all_combo_rows.append({"symbol": meta.symbol, "kind": "risk_best", **serializable_score(risk_best)})
            if return_best:
                all_combo_rows.append({"symbol": meta.symbol, "kind": "return_best", **serializable_score(return_best)})

    promoted = [
        item
        for item in symbol_reports.values()
        if item.get("promotion", {}).get("decision") == "promote_candidate_for_review"
    ]
    scored_symbols = [item for item in symbol_reports.values() if item.get("status") == "scored"]
    report["summary"] = {
        "scored_symbols": len(scored_symbols),
        "no_candidate_symbols": sum(1 for item in symbol_reports.values() if item.get("status") == "no_candidates"),
        "promotion_for_review_count": len(promoted),
        "promotion_for_review_symbols": [item.get("symbol") for item in promoted],
    }
    report["symbol_reports_path"] = str(output_dir / "symbol_t1_risk_first_report.json")
    report["combo_extract_path"] = str(output_dir / "symbol_combo_score_extract.jsonl")
    report["candidate_outcomes_path"] = str(outcome_path)
    report["completed_at_ist"] = epoch_ist_iso(time.time())
    atomic_write_json(output_dir / "symbol_t1_risk_first_report.json", {"report": report, "symbols": symbol_reports})
    with (output_dir / "symbol_combo_score_extract.jsonl").open("w", encoding="utf-8") as handle:
        for row in all_combo_rows:
            handle.write(json.dumps(json_clean(row), sort_keys=True) + "\n")
    atomic_write_json(output_dir / "score_t1_candidates_risk_first_summary.json", report)
    print(json.dumps(json_clean(report), indent=2, sort_keys=True))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--candidate-file", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--primary-short-thresholds", default="1.5,1.75,2.0")
    parser.add_argument("--fresh-breakout-multipliers", default="1.2,1.4,1.6")
    parser.add_argument("--long-strength-pcts", default="90,95")
    parser.add_argument("--short-weakness-pcts", default="1,5,10")
    parser.add_argument("--current-long-strength-pct", type=float, default=95.0)
    parser.add_argument("--current-short-weakness-pct", type=float, default=1.0)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--min-closed-trades", type=int, default=3)
    parser.add_argument("--min-success-rate-pct", type=float, default=45.0)
    parser.add_argument("--max-worst-loss-pct-deterioration", type=float, default=0.25)
    parser.add_argument("--min-worst-loss-pct-improvement", type=float, default=0.25)
    parser.add_argument("--max-success-rate-deterioration-pct", type=float, default=5.0)
    parser.add_argument("--min-net-preservation-ratio", type=float, default=0.80)
    parser.add_argument("--max-single-win-share", type=float, default=0.60)
    parser.add_argument("--max-outcomes", type=int, default=0)
    parser.add_argument("--no-skip-weekends", action="store_true")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
