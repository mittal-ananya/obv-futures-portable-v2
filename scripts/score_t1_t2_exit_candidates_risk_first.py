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
import copy
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
    _update_live_tranche3_v2,
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
            duration = round(time.perf_counter() - self.started, 4)
            report.setdefault("stage_timings", {})[stage] = duration
            print(json.dumps({"stage": stage, "duration_seconds": duration}), flush=True)

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

    def carried_row(self, idx: int, *, epoch_second: int) -> dict[str, Any]:
        row = self.row(idx)
        source_epoch = int(row.get("epoch_second") or 0)
        carried = dict(row)
        carried["source_epoch_second"] = source_epoch
        carried["source_exchange_timestamp"] = row.get("exchange_timestamp")
        carried["epoch_second"] = int(epoch_second)
        carried["epoch"] = float(epoch_second)
        carried["exchange_timestamp"] = epoch_ist_iso(int(epoch_second))
        carried["carried_quote"] = int(epoch_second) != source_epoch
        carried["carried_quote_age_seconds"] = int(epoch_second) - source_epoch
        return carried

    def rows_between(self, start_idx: int, end_idx_exclusive: int) -> list[dict[str, Any]]:
        return [self.row(idx) for idx in range(max(0, start_idx), min(len(self), end_idx_exclusive))]


class PathFrameCache:
    """Prebuilt pandas frames for repeated chronological checkpoint slicing."""

    def __init__(self, path: PathArrays, clock_rows: list[dict[str, Any]]) -> None:
        import pandas as pd  # type: ignore

        epochs = [int(value) for value in path.epochs]
        bids = [float(value) for value in path.bids]
        asks = [float(value) for value in path.asks]
        received_epochs = [float(value) for value in path.received_epochs]
        self.epochs = epochs
        self.seconds = pd.DataFrame(
            {
                "trade_date": [
                    epoch_ist_iso(epoch)[:10] if epoch_ist_iso(epoch) else None
                    for epoch in epochs
                ],
                "epoch_second": epochs,
                "epoch": [float(epoch) for epoch in epochs],
                "received_at_ist": [
                    epoch_ist_iso(received) if math.isfinite(received) else ""
                    for received in received_epochs
                ],
                "exchange_timestamp": [epoch_ist_iso(epoch) for epoch in epochs],
                "received_epoch": [
                    received if math.isfinite(received) else None
                    for received in received_epochs
                ],
                "price": [float(value) for value in path.prices],
                "bid": [bid if math.isfinite(bid) else None for bid in bids],
                "ask": [ask if math.isfinite(ask) else None for ask in asks],
                "spread": [
                    (ask - bid) if math.isfinite(bid) and math.isfinite(ask) else None
                    for bid, ask in zip(bids, asks)
                ],
            }
        )
        if not self.seconds.empty:
            self.seconds = self.seconds.sort_values("epoch_second", kind="mergesort").reset_index(drop=True)
            self.epochs = [int(value) for value in self.seconds["epoch_second"].tolist()]
        self.clock = pd.DataFrame(clock_rows)
        if not self.clock.empty and "epoch_second" in self.clock.columns:
            self.clock = self.clock.sort_values("epoch_second", kind="mergesort").reset_index(drop=True)
            self.clock_epochs = [int(value) for value in self.clock["epoch_second"].tolist()]
        else:
            self.clock_epochs = []

    def state_at(self, *, checkpoint_epoch: int, trade_date: str) -> dict[str, Any]:
        import pandas as pd  # type: ignore

        end_idx = bisect.bisect_right(self.epochs, int(checkpoint_epoch))
        if end_idx <= 0:
            seconds = self.seconds.iloc[:0].copy()
            today_seconds = seconds
            latest_tick: dict[str, Any] | None = None
        else:
            seconds = self.seconds.iloc[:end_idx].copy()
            today_seconds = seconds[seconds["trade_date"].astype(str) == str(trade_date)].copy()
            latest_tick = self.seconds.iloc[end_idx - 1].to_dict()
        clock_end_idx = bisect.bisect_right(self.clock_epochs, int(checkpoint_epoch))
        clock_state = (
            self.clock.iloc[:clock_end_idx].copy()
            if clock_end_idx > 0
            else pd.DataFrame(columns=self.clock.columns)
        )
        latest_clock = (
            self.clock.iloc[clock_end_idx - 1].to_dict()
            if clock_end_idx > 0 and not self.clock.empty
            else {}
        )
        return {
            "seconds": seconds,
            "today_seconds": today_seconds,
            "clock_state": clock_state,
            "latest_tick": latest_tick,
            "latest_clock": latest_clock,
            "entry_edges_today": [],
        }

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
    allowed_variants_by_symbol: dict[str, set[str]] | None = None,
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
            allowed_variants = allowed_variants_by_symbol.get(symbol) if allowed_variants_by_symbol else None
            if allowed_variants is not None and str(item.get("variant") or "") not in allowed_variants:
                continue
            signal_epoch = int(item.get("signal_epoch") or 0)
            if signal_epoch < int(start_epoch) or signal_epoch > int(end_epoch):
                continue
            item["candidate_id"] = f"{symbol}|{item.get('variant')}|{item.get('module')}|{item.get('side')}|{int(item.get('signal_epoch') or 0)}"
            out[symbol].append(item)
    for rows in out.values():
        rows.sort(key=lambda row: (int(row.get("signal_epoch") or 0), str(row.get("variant") or "")))
    return out


def load_supported_variants_by_symbol(
    candidate_file: Path,
    selected_symbols: set[str],
) -> dict[str, set[str]] | None:
    support_path = candidate_file.parent / "entry_variant_support.json"
    payload: dict[str, Any] | None = None
    if support_path.exists():
        payload = json.loads(support_path.read_text(encoding="utf-8"))
    else:
        report_path = candidate_file.parent / "recalibration_smoke_report.json"
        if report_path.exists():
            report = json.loads(report_path.read_text(encoding="utf-8"))
            payload = {"symbols": report.get("entry_variant_grid_by_symbol")}
    if not isinstance(payload, dict):
        return None
    raw_symbols = payload.get("symbols")
    if not isinstance(raw_symbols, dict):
        return None
    out: dict[str, set[str]] = {}
    for symbol in selected_symbols:
        values = raw_symbols.get(symbol)
        if isinstance(values, list):
            out[symbol] = {str(value) for value in values if str(value)}
    return out or None


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


def configured_price_strength_pct(
    point_config: dict[str, Any] | None,
    key: str,
    fallback: float,
) -> float:
    cfg = point_config or {}
    payload = cfg.get("point_thresholds") if isinstance(cfg.get("point_thresholds"), dict) else cfg
    value = as_float(payload.get(key))
    return float(value if value is not None else fallback)


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


def dedupe_entry_combos(combos: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for combo in combos:
        if not combo:
            continue
        label = combo_label(combo)
        if label in seen:
            continue
        seen.add(label)
        out.append(dict(combo))
    return out


def current_entry_combo(meta: Any, args: argparse.Namespace) -> dict[str, Any]:
    return {
        "primary_abs": configured_primary_short(meta.signal_point_config),
        "fresh_multiplier": configured_fresh_multiplier(meta.signal_point_config),
        "long_pct": configured_price_strength_pct(
            meta.signal_point_config,
            "fresh_long_price_strength_pct",
            float(args.current_long_strength_pct),
        ),
        "short_pct": configured_price_strength_pct(
            meta.signal_point_config,
            "fresh_short_price_weakness_pct",
            float(args.current_short_weakness_pct),
        ),
    }


def load_entry_combo_report(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    raw_path = Path(path)
    if not raw_path.exists():
        raise SystemExit(f"Entry combo report not found: {raw_path}")
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    return payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}


def entry_combos_for_symbol(
    *,
    meta: Any,
    args: argparse.Namespace,
    entry_report_item: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not entry_report_item:
        return build_combos(meta, args)
    combos = [current_entry_combo(meta, args)]
    for key in ("best_risk_first_candidate", "best_return_candidate"):
        item = entry_report_item.get(key) if isinstance(entry_report_item, dict) else None
        combo = item.get("combo") if isinstance(item, dict) else None
        if isinstance(combo, dict) and combo:
            combos.append(
                {
                    "primary_abs": float(combo["primary_abs"]),
                    "fresh_multiplier": float(combo["fresh_multiplier"]),
                    "long_pct": float(combo["long_pct"]),
                    "short_pct": float(combo["short_pct"]),
                }
            )
    return dedupe_entry_combos(combos)


def exit_combo_label(combo: dict[str, Any]) -> str:
    return str(combo.get("label") or "").strip() or (
        "exit_h{hard_sl_scale:g}_ta{trail_activation_scale:g}_r{trail_activation_r_multiple:g}"
        "_gb{trail_giveback_fraction:g}_sx{short_exit_pct:g}_lx{long_exit_pct:g}"
        "_age{min_exit_age_sessions:g}_mfe{min_profit_or_mfe_r:g}_t2{t2_activation_clocks:g}c"
        "_{t2_tighten_pct:g}pct"
    ).format(**combo)


def exit_combo_identity(combo: dict[str, Any]) -> str:
    keys = (
        "hard_sl_scale",
        "trail_activation_scale",
        "trail_activation_r_multiple",
        "trail_giveback_fraction",
        "short_exit_pct",
        "long_exit_pct",
        "min_exit_age_sessions",
        "min_profit_or_mfe_r",
        "t2_activation_clocks",
        "t2_tighten_pct",
    )
    payload = {key: combo.get(key) for key in keys}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def default_current_exit_combo() -> dict[str, Any]:
    return {
        "label": "exit_current",
        "hard_sl_scale": 1.00,
        "trail_activation_scale": 1.00,
        "trail_activation_r_multiple": 2.50,
        "trail_giveback_fraction": 0.80,
        "short_exit_pct": 5.0,
        "long_exit_pct": 95.0,
        "min_exit_age_sessions": 2,
        "min_profit_or_mfe_r": None,
        "t2_activation_clocks": 16,
        "t2_tighten_pct": 8.0,
    }


def normalize_exit_combo(combo: dict[str, Any] | None, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    base = dict(fallback or default_current_exit_combo())
    raw = combo if isinstance(combo, dict) else {}
    out = dict(base)
    if raw.get("label"):
        out["label"] = str(raw.get("label"))
    for key in (
        "hard_sl_scale",
        "trail_activation_scale",
        "trail_activation_r_multiple",
        "trail_giveback_fraction",
        "short_exit_pct",
        "long_exit_pct",
        "min_profit_or_mfe_r",
        "t2_tighten_pct",
    ):
        if key in raw:
            value = as_float(raw.get(key))
            out[key] = float(value) if value is not None else None
    if "min_exit_age_sessions" in raw:
        value = as_float(raw.get("min_exit_age_sessions"))
        if value is not None:
            out["min_exit_age_sessions"] = int(value)
    if "t2_activation_clocks" in raw:
        value = as_float(raw.get("t2_activation_clocks"))
        if value is not None:
            out["t2_activation_clocks"] = int(value)
    return out


def current_exit_combo(meta: Any, args: argparse.Namespace) -> dict[str, Any]:
    adaptive = getattr(meta, "adaptive_calibration", None)
    if isinstance(adaptive, dict) and isinstance(adaptive.get("exit_combo"), dict):
        combo = normalize_exit_combo(adaptive.get("exit_combo"))
        # meta.execution_point_config is already materialized with adaptive hard
        # SL / trail scaling. The current-runtime scorer must replay that config
        # as installed, not apply the historical combo scale a second time.
        combo["hard_sl_scale"] = 1.0
        combo["trail_activation_scale"] = 1.0
        return combo
    return normalize_exit_combo(default_current_exit_combo())


def dedupe_exit_combos(combos: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for combo in combos:
        identity = exit_combo_identity(combo)
        if identity in seen:
            continue
        seen.add(identity)
        out.append(dict(combo))
    return out


def build_exit_combos(args: argparse.Namespace) -> list[dict[str, Any]]:
    # Compact, risk-first grid. It deliberately avoids a full Cartesian explosion.
    raw = [
        ("exit_current", 1.00, 1.00, 2.50, 0.80, 5.0, 95.0, 2, None, 16, 8.0),
        ("t2_12c_8pct", 1.00, 1.00, 2.50, 0.80, 5.0, 95.0, 2, None, 12, 8.0),
        ("t2_16c_10pct", 1.00, 1.00, 2.50, 0.80, 5.0, 95.0, 2, None, 16, 10.0),
        ("t2_24c_10pct", 1.00, 1.00, 2.50, 0.80, 5.0, 95.0, 2, None, 24, 10.0),
        ("tight_sl", 0.80, 1.00, 2.50, 0.80, 5.0, 95.0, 2, None, 16, 8.0),
        ("tight_sl_fast_trail", 0.80, 0.75, 2.00, 0.65, 5.0, 95.0, 2, None, 16, 8.0),
        ("fast_trail", 1.00, 0.75, 2.00, 0.65, 5.0, 95.0, 2, None, 16, 8.0),
        ("wide_sl_fast_trail", 1.20, 0.75, 2.00, 0.65, 5.0, 95.0, 2, None, 16, 8.0),
        ("early_exhaustion", 1.00, 1.00, 2.50, 0.80, 10.0, 90.0, 1, 0.50, 16, 8.0),
        ("early_exhaustion_t2_12c_10pct", 1.00, 1.00, 2.50, 0.80, 10.0, 90.0, 1, 0.50, 12, 10.0),
        ("tight_sl_early_exhaustion_t2_12c_10pct", 0.80, 0.75, 2.00, 0.65, 10.0, 90.0, 1, 0.50, 12, 10.0),
        ("late_exhaustion_t2_24c_10pct", 1.00, 1.00, 2.50, 0.80, 5.0, 95.0, 2, 1.00, 24, 10.0),
    ]
    if args.exit_combo_labels:
        wanted = set(parse_csv(args.exit_combo_labels))
        raw = [item for item in raw if item[0] in wanted]
    return [
        {
            "label": label,
            "hard_sl_scale": hard_sl_scale,
            "trail_activation_scale": trail_activation_scale,
            "trail_activation_r_multiple": trail_activation_r_multiple,
            "trail_giveback_fraction": trail_giveback_fraction,
            "short_exit_pct": short_exit_pct,
            "long_exit_pct": long_exit_pct,
            "min_exit_age_sessions": min_exit_age_sessions,
            "min_profit_or_mfe_r": min_profit_or_mfe_r,
            "t2_activation_clocks": t2_activation_clocks,
            "t2_tighten_pct": t2_tighten_pct,
        }
        for (
            label,
            hard_sl_scale,
            trail_activation_scale,
            trail_activation_r_multiple,
            trail_giveback_fraction,
            short_exit_pct,
            long_exit_pct,
            min_exit_age_sessions,
            min_profit_or_mfe_r,
            t2_activation_clocks,
            t2_tighten_pct,
        ) in raw
    ]


def exit_combos_for_symbol(
    *,
    meta: Any,
    args: argparse.Namespace,
    base_combos: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    current = current_exit_combo(meta, args)
    if args.current_runtime_combos_only:
        return [current]
    return dedupe_exit_combos([*base_combos, current])


def point_config_for_exit_combo(
    base: dict[str, Any] | None,
    exit_combo: dict[str, Any],
    *,
    hard_sl_points: float | None,
) -> dict[str, Any]:
    point_config = copy.deepcopy(base or {})
    profile = point_config.get("exit_profile") if isinstance(point_config.get("exit_profile"), dict) else {}
    profile = dict(profile)
    profile.update(
        {
            "short_exit_pct": float(exit_combo["short_exit_pct"]),
            "long_exit_pct": float(exit_combo["long_exit_pct"]),
            "min_exit_age_sessions": int(exit_combo["min_exit_age_sessions"]),
            "trail_activation_r_multiple": float(exit_combo["trail_activation_r_multiple"]),
            "trail_giveback_fraction": float(exit_combo["trail_giveback_fraction"]),
        }
    )
    mfe_r = exit_combo.get("min_profit_or_mfe_r")
    if mfe_r is not None and hard_sl_points is not None:
        profile["min_profit_or_mfe_points"] = float(hard_sl_points) * float(mfe_r)
    point_config["exit_profile"] = profile
    point_config.update(ttsl_config_for_exit_combo(exit_combo))
    return point_config


def ttsl_config_for_exit_combo(exit_combo: dict[str, Any]) -> dict[str, Any]:
    return {
        "two_lot_ttsl_enabled": True,
        "two_lot_ttsl_activation_clocks": int(exit_combo["t2_activation_clocks"]),
        "two_lot_ttsl_tighten_pct": float(exit_combo["t2_tighten_pct"]),
        "two_lot_ttsl_sync_with_base_stop": True,
    }


def tranche3_combo_label(combo: dict[str, Any]) -> str:
    combo = combo or {"activation_clocks": 16, "entry_r_multiple": 0.75}
    if str(combo.get("entry_mode") or "").strip().lower() == "pullback":
        return str(combo.get("label") or "").strip() or (
            "t3_pullback_{activation_clocks:g}c_{pullback_r_multiple:g}R"
        ).format(**combo)
    return str(combo.get("label") or "").strip() or ("t3_{activation_clocks:g}c_{entry_r_multiple:g}R").format(**combo)


def tranche3_combo_identity(combo: dict[str, Any]) -> str:
    combo = combo or {}
    entry_mode = str(combo.get("entry_mode") or "momentum").strip().lower()
    payload = {
        "enabled": bool(combo.get("enabled", True)),
        "entry_mode": entry_mode if entry_mode in {"momentum", "pullback"} else "momentum",
        "activation_clocks": int(combo.get("activation_clocks") or 16),
        "entry_r_multiple": as_float(combo.get("entry_r_multiple")),
        "pullback_r_multiple": as_float(combo.get("pullback_r_multiple")),
        "exit_rule": combo.get("exit_rule"),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def tranche3_config_for_combo(combo: dict[str, Any] | None) -> dict[str, Any]:
    combo = combo or {}
    entry_mode = str(combo.get("entry_mode") or "momentum").strip().lower()
    if entry_mode not in {"momentum", "pullback"}:
        entry_mode = "momentum"
    return {
        "tranche3_enabled": bool(combo.get("enabled", True)),
        "tranche3_activation_clocks": int(combo.get("activation_clocks") or 16),
        "tranche3_entry_r_multiple": float(combo.get("entry_r_multiple") if combo.get("entry_r_multiple") is not None else 0.75),
        "tranche3_entry_mode": entry_mode,
        "tranche3_pullback_r_multiple": float(
            combo.get("pullback_r_multiple") if combo.get("pullback_r_multiple") is not None else combo.get("entry_r_multiple", 0.50)
        ),
    }


def default_current_tranche3_combo() -> dict[str, Any]:
    return {
        "label": "t3_current_16c_0.75R",
        "enabled": True,
        "entry_mode": "momentum",
        "activation_clocks": 16,
        "entry_r_multiple": 0.75,
        "exit_rule": "same_as_v21_tranche2_selected_exit_else_base_exit",
    }


def parse_tranche3_label(label: str) -> tuple[int | None, float | None]:
    cleaned = str(label or "").strip()
    if cleaned.startswith("t3_current_"):
        cleaned = "t3_" + cleaned.removeprefix("t3_current_")
    if cleaned.startswith("t3_pullback_"):
        cleaned = "t3_" + cleaned.removeprefix("t3_pullback_")
    if not cleaned.startswith("t3_") or "c_" not in cleaned or not cleaned.endswith("R"):
        return None, None
    body = cleaned.removeprefix("t3_").removesuffix("R")
    left, right = body.split("c_", 1)
    try:
        return int(float(left)), float(right)
    except ValueError:
        return None, None


def normalize_tranche3_combo(combo: dict[str, Any] | None, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    base = dict(fallback or default_current_tranche3_combo())
    raw = combo if isinstance(combo, dict) else {}
    out = dict(base)
    if raw.get("label"):
        out["label"] = str(raw.get("label"))
    activation = as_float(raw.get("activation_clocks"))
    if activation is None:
        activation = as_float(raw.get("tranche3_activation_clocks"))
    r_multiple = as_float(raw.get("entry_r_multiple"))
    if r_multiple is None:
        r_multiple = as_float(raw.get("tranche3_entry_r_multiple"))
    if (activation is None or r_multiple is None) and out.get("label"):
        parsed_activation, parsed_r = parse_tranche3_label(str(out.get("label")))
        if activation is None:
            activation = parsed_activation
        if r_multiple is None:
            r_multiple = parsed_r
    if activation is not None:
        out["activation_clocks"] = int(activation)
    if r_multiple is not None:
        out["entry_r_multiple"] = float(r_multiple)
    if "enabled" in raw:
        out["enabled"] = bool(raw.get("enabled"))
    entry_mode = raw.get("entry_mode") or raw.get("tranche3_entry_mode") or out.get("entry_mode")
    if entry_mode:
        parsed_mode = str(entry_mode).strip().lower()
        out["entry_mode"] = parsed_mode if parsed_mode in {"momentum", "pullback"} else "momentum"
    elif str(out.get("label") or "").startswith("t3_pullback_"):
        out["entry_mode"] = "pullback"
    pullback_r = as_float(raw.get("pullback_r_multiple"))
    if pullback_r is None:
        pullback_r = as_float(raw.get("tranche3_pullback_r_multiple"))
    if pullback_r is not None:
        out["pullback_r_multiple"] = float(pullback_r)
    return out


def current_tranche3_combo(meta: Any, args: argparse.Namespace) -> dict[str, Any]:
    adaptive = getattr(meta, "adaptive_calibration", None)
    if isinstance(adaptive, dict):
        raw = adaptive.get("tranche3_combo") if isinstance(adaptive.get("tranche3_combo"), dict) else {}
        label = adaptive.get("tranche3_combo_label")
        if label and "label" not in raw:
            raw = {**raw, "label": label}
        if raw:
            return normalize_tranche3_combo(raw)
    return normalize_tranche3_combo(default_current_tranche3_combo())


def dedupe_tranche3_combos(combos: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for combo in combos:
        identity = tranche3_combo_identity(combo)
        if identity in seen:
            continue
        seen.add(identity)
        out.append(dict(combo))
    return out


def tranche3_entry_epoch(position: dict[str, Any] | None) -> int | None:
    if not isinstance(position, dict):
        return None
    tranche3 = position.get("tranche3")
    if not isinstance(tranche3, dict):
        return None
    entry_epoch = as_float(tranche3.get("entry_epoch"))
    if entry_epoch is None or entry_epoch <= 0:
        return None
    return int(entry_epoch)


def tranche3_close_allowed(position: dict[str, Any] | None, exit_epoch: Any) -> bool:
    entry_epoch = tranche3_entry_epoch(position)
    parsed_exit = as_float(exit_epoch)
    if entry_epoch is None or parsed_exit is None:
        return False
    return int(parsed_exit) >= int(entry_epoch)


def tranche2_selected_exit_epoch(position: dict[str, Any] | None) -> int | None:
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


def resolve_tranche3_final_epoch(position: dict[str, Any] | None, proposed_final_epoch: Any) -> int | None:
    proposed = as_float(proposed_final_epoch)
    cap = int(proposed) if proposed is not None and proposed > 0 else None
    tranche2_exit_epoch = tranche2_selected_exit_epoch(position)
    if tranche2_exit_epoch is not None:
        tranche2_cap = max(0, int(tranche2_exit_epoch) - 1)
        cap = tranche2_cap if cap is None else min(cap, tranche2_cap)
    return cap


def valid_tranche3_event(position: dict[str, Any] | None, event: dict[str, Any]) -> bool:
    if not isinstance(event, dict):
        return False
    if event.get("event") != "tranche3_exit":
        return True
    return tranche3_close_allowed(position, event.get("exit_epoch"))


def filter_valid_tranche3_events(
    position: dict[str, Any] | None,
    events: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [event for event in events if valid_tranche3_event(position, event)]


def _tranche3_param_value(config: dict[str, Any] | None, *keys: str, default: float) -> float:
    payload = config if isinstance(config, dict) else {}
    for key in keys:
        value = as_float(payload.get(key))
        if value is not None:
            return float(value)
    return float(default)


def _compact_update_tranche3_for_candidate(
    *,
    v1: Any,
    position: dict[str, Any],
    exec_rows: PathArrays,
    execution_clock_epochs: list[int],
    entry_idx: int,
    tranche_end_idx: int,
    latest_exit_fill_price: float | None,
    latest_exit_time: Any,
    cost_points: float,
    lot_size: int,
    point_config: dict[str, Any] | None,
    config: dict[str, Any] | None,
    final_epoch: int | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Array-based T3 candidate scorer; selected rows are still replay-proven."""
    events: list[dict[str, Any]] = []
    position = v1._ensure_live_tranche3(position, config=config, lot_size=lot_size)
    t3 = dict(position.get("tranche3") or {})
    if not t3.get("enabled", True):
        t3["status"] = "disabled"
        position["tranche3"] = t3
        return position, events
    if t3.get("status") in {"open", "closed"}:
        position = v1._refresh_live_tranche3_marks(
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

    side = str(position.get("side") or "").lower()
    entry_epoch = int(position.get("entry_epoch") or 0)
    entry_price = as_float(position.get("entry_price"))
    hard_sl = as_float(position.get("hard_sl_points"))
    if side not in {"long", "short"} or entry_epoch <= 0 or entry_price is None or hard_sl is None or hard_sl <= 0:
        t3["status"] = "missing_base_entry_inputs"
        position["tranche3"] = t3
        return position, events

    payload = config if isinstance(config, dict) else {}
    entry_mode = str(payload.get("tranche3_entry_mode") or payload.get("entry_mode") or "momentum").strip().lower()
    if entry_mode not in {"momentum", "pullback"}:
        entry_mode = "momentum"
    activation_clocks = max(
        1,
        int(_tranche3_param_value(payload, "tranche3_activation_clocks", "activation_clocks", default=16.0)),
    )
    entry_r = _tranche3_param_value(payload, "tranche3_entry_r_multiple", "entry_r_multiple", default=0.75)
    pullback_r = _tranche3_param_value(payload, "tranche3_pullback_r_multiple", "pullback_r_multiple", default=0.50)
    t3.update(
        {
            "entry_mode": entry_mode,
            "activation_clocks": activation_clocks,
            "entry_r_multiple": pullback_r if entry_mode == "pullback" else entry_r,
        }
    )

    latest_epoch = int(final_epoch) if final_epoch is not None else None
    if latest_epoch is None and tranche_end_idx > entry_idx:
        latest_epoch = int(exec_rows.epochs[min(len(exec_rows), tranche_end_idx) - 1])
    activation_candidates = [
        epoch
        for epoch in execution_clock_epochs
        if int(epoch) > entry_epoch and (latest_epoch is None or int(epoch) <= int(latest_epoch))
    ]
    if len(activation_candidates) < activation_clocks:
        t3["status"] = "waiting_activation_clocks"
        t3["clocks_since_entry"] = len(activation_candidates)
        position["tranche3"] = t3
        return position, events
    activation_epoch = int(activation_candidates[activation_clocks - 1])
    t3.update(
        {
            "activation_epoch": activation_epoch,
            "activation_time": epoch_ist_iso(activation_epoch),
            "clocks_since_entry": len(activation_candidates),
        }
    )

    scan_start = activation_epoch
    scan_end = int(latest_epoch) if latest_epoch is not None else scan_start
    if scan_start >= scan_end:
        t3["status"] = "armed_waiting_trigger"
        position["tranche3"] = t3
        return position, events

    trigger_idx: int | None = None
    trigger_pnl: float | None = None
    trigger_points = float(hard_sl) * (float(pullback_r) if entry_mode == "pullback" else float(entry_r))
    hard_sl_price = float(entry_price) - float(hard_sl) if side == "long" else float(entry_price) + float(hard_sl)
    if entry_mode == "pullback":
        trigger_price = float(entry_price) - trigger_points if side == "long" else float(entry_price) + trigger_points
        t3.update(
            {
                "entry_trigger_points": trigger_points,
                "entry_trigger_price": trigger_price,
                "hard_sl_price": hard_sl_price,
                "pullback_r_multiple": float(pullback_r),
            }
        )
    else:
        t3["entry_trigger_points"] = trigger_points

    start_idx = max(entry_idx, bisect.bisect_right(exec_rows.epochs, float(scan_start)))
    end_idx = min(tranche_end_idx, bisect.bisect_right(exec_rows.epochs, float(scan_end)))
    for idx in range(start_idx, end_idx):
        price = float(exec_rows.prices[idx])
        pnl_points = signed_points(side, float(entry_price), price)
        if entry_mode == "pullback":
            if side == "long":
                if price <= hard_sl_price:
                    t3["status"] = "pullback_reached_hard_sl_zone"
                    t3["last_checked_epoch"] = int(exec_rows.epochs[idx])
                    position["tranche3"] = t3
                    return position, events
                trigger_hit = hard_sl_price < price <= trigger_price
            else:
                if price >= hard_sl_price:
                    t3["status"] = "pullback_reached_hard_sl_zone"
                    t3["last_checked_epoch"] = int(exec_rows.epochs[idx])
                    position["tranche3"] = t3
                    return position, events
                trigger_hit = hard_sl_price > price >= trigger_price
        else:
            trigger_hit = pnl_points >= trigger_points
        if trigger_hit:
            trigger_idx = idx
            trigger_pnl = pnl_points
            break

    if trigger_idx is None:
        t3["status"] = "armed_waiting_pullback" if entry_mode == "pullback" else "armed_waiting_trigger"
        if end_idx > start_idx:
            t3["last_checked_epoch"] = int(exec_rows.epochs[end_idx - 1])
        position["tranche3"] = t3
        return position, events

    trigger_row = exec_rows.row(trigger_idx)
    entry_fill = v1.execution_fill_from_row(
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
        t3["last_checked_epoch"] = int(trigger_row["epoch_second"])
        position["tranche3"] = t3
        return position, events
    entry_epoch = int(entry_fill.get("epoch_second") or trigger_row.get("epoch_second") or 0)
    entry_reason = (
        f"tranche3_pullback_{activation_clocks}c_{float(pullback_r):g}R"
        if entry_mode == "pullback"
        else "tranche3_v1_16c_0_75R"
    )
    t3.update(
        {
            "status": "open",
            "entry_reason": entry_reason,
            "entry_time": entry_fill.get("time") or epoch_ist_iso(entry_epoch),
            "entry_epoch": entry_epoch,
            "entry_price": entry_ltp_price,
            "entry_ltp_price": entry_ltp_price,
            "entry_fill_price": entry_fill_price,
            "entry_fill_quality": entry_fill.get("fill_quality"),
            "entry_pnl_from_base_points": trigger_pnl,
            "entry_R_from_base": trigger_pnl / float(hard_sl) if hard_sl and trigger_pnl is not None else None,
            "last_checked_epoch": entry_epoch,
        }
    )
    position["tranche3"] = t3
    position = v1._refresh_live_tranche3_marks(
        position,
        latest_exit_fill_price=latest_exit_fill_price,
        latest_exit_time=latest_exit_time,
        lot_size=lot_size,
        point_config=point_config,
    )
    event = v1._live_tranche3_entry_event(position, dict(position.get("tranche3") or {}))
    if entry_mode == "pullback":
        event["entry_reason"] = entry_reason
        event["entry_mode"] = "pullback"
        event["pullback_r_multiple"] = float(pullback_r)
        event["tranche3"] = dict(position.get("tranche3") or {})
    events.append(event)
    return position, events


def build_tranche3_combos(args: argparse.Namespace) -> list[dict[str, Any]]:
    raw = [
        ("t3_current_16c_0.75R", "momentum", 16, 0.75),
        ("t3_8c_0.15R", "momentum", 8, 0.15),
        ("t3_8c_0.25R", "momentum", 8, 0.25),
        ("t3_8c_0.50R", "momentum", 8, 0.50),
        ("t3_12c_0.50R", "momentum", 12, 0.50),
        ("t3_16c_0.50R", "momentum", 16, 0.50),
        ("t3_16c_1.00R", "momentum", 16, 1.00),
        ("t3_24c_0.75R", "momentum", 24, 0.75),
        ("t3_pullback_4c_0.25R", "pullback", 4, 0.25),
        ("t3_pullback_4c_0.50R", "pullback", 4, 0.50),
        ("t3_pullback_8c_0.25R", "pullback", 8, 0.25),
        ("t3_pullback_8c_0.50R", "pullback", 8, 0.50),
        ("t3_pullback_8c_0.75R", "pullback", 8, 0.75),
        ("t3_pullback_12c_0.25R", "pullback", 12, 0.25),
        ("t3_pullback_12c_0.50R", "pullback", 12, 0.50),
    ]
    if args.tranche3_combo_labels:
        wanted = set(parse_csv(args.tranche3_combo_labels))
        raw = [item for item in raw if item[0] in wanted]
    return [
        {
            "label": label,
            "enabled": True,
            "entry_mode": entry_mode,
            "activation_clocks": int(activation_clocks),
            "entry_r_multiple": float(entry_r_multiple),
            "pullback_r_multiple": float(entry_r_multiple) if entry_mode == "pullback" else None,
            "exit_rule": "same_as_v21_tranche2_selected_exit_else_base_exit",
        }
        for label, entry_mode, activation_clocks, entry_r_multiple in raw
    ]


def tranche3_combos_for_symbol(
    *,
    meta: Any,
    args: argparse.Namespace,
    base_combos: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    current = current_tranche3_combo(meta, args)
    if args.current_runtime_combos_only:
        return [current]
    return dedupe_tranche3_combos([*base_combos, current])


def outcome_family_label(exit_combo: dict[str, Any], tranche3_combo: dict[str, Any]) -> str:
    return f"{exit_combo_identity(exit_combo)}::{tranche3_combo_identity(tranche3_combo)}"


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
    current = current_entry_combo(meta, args)
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
    point_config: dict[str, Any] | None,
    ttsl_config: dict[str, Any] | None,
    tranche3_config: dict[str, Any] | None = None,
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
            point_config=point_config,
        ),
        "max_favorable_points": 0.0,
        "max_adverse_points": 0.0,
        "status": "open",
        "entry_margin_used_rupees": margin_for(meta, side, as_float(entry_fill.get("ltp_price"))),
        "lot_size": int(meta.lot_size or 1),
        "accounting_model": "bid_ask_proxy_slippage_zerodha_futures",
    }
    position = v1._ensure_live_two_lot_ttsl(dict(position), config=ttsl_config, lot_size=int(meta.lot_size or 1))
    position = v1._ensure_live_tranche3(dict(position), config=tranche3_config, lot_size=int(meta.lot_size or 1))
    return position


def entry_row_for_due(
    rows: PathArrays,
    *,
    due_epoch: int,
    max_carry_age_seconds: float,
) -> tuple[int, dict[str, Any] | None]:
    next_idx = bisect.bisect_left(rows.epochs, float(due_epoch))
    if next_idx < len(rows) and int(rows.epochs[next_idx]) == int(due_epoch):
        row = rows.row(next_idx)
        return next_idx, canonical_same_second_fill_row(
            rows,
            epoch_second=int(due_epoch),
            fallback=row,
            preferred_price=as_float(row.get("price")),
        )
    prev_idx = next_idx - 1
    if prev_idx >= 0:
        age = int(due_epoch) - int(rows.epochs[prev_idx])
        if 0 <= age <= float(max_carry_age_seconds):
            return next_idx, rows.carried_row(prev_idx, epoch_second=int(due_epoch))
    if next_idx < len(rows):
        row = rows.row(next_idx)
        return next_idx, canonical_same_second_fill_row(
            rows,
            epoch_second=int(row.get("epoch_second") or row.get("epoch") or due_epoch),
            fallback=row,
            preferred_price=as_float(row.get("price")),
        )
    return next_idx, None


def _has_valid_non_crossed_quote(row: dict[str, Any]) -> bool:
    bid = as_float(row.get("bid"))
    ask = as_float(row.get("ask"))
    return bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid


def _ltp_inside_valid_quote(row: dict[str, Any]) -> bool:
    price = as_float(row.get("price"))
    bid = as_float(row.get("bid"))
    ask = as_float(row.get("ask"))
    return (
        price is not None
        and bid is not None
        and ask is not None
        and bid > 0
        and ask > 0
        and ask >= bid
        and bid <= price <= ask
    )


def _without_stale_quote(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    if not _ltp_inside_valid_quote(out):
        out["bid"] = None
        out["ask"] = None
        out["spread"] = None
        out["quote_warning"] = "ltp_outside_bid_ask_sanitized"
    return out


def canonical_same_second_fill_row(
    rows: PathArrays,
    *,
    epoch_second: int,
    fallback: dict[str, Any],
    preferred_price: float | None,
) -> dict[str, Any]:
    """Prefer a valid same-second quote without changing the selected LTP path."""
    lo = bisect.bisect_left(rows.epochs, float(epoch_second))
    hi = bisect.bisect_right(rows.epochs, float(epoch_second))
    if lo >= hi:
        return dict(fallback)
    same_price_rows: list[dict[str, Any]] = []
    inside_quote_rows: list[dict[str, Any]] = []
    for idx in range(lo, hi):
        row = rows.row(idx)
        price = as_float(row.get("price"))
        if _ltp_inside_valid_quote(row):
            inside_quote_rows.append(row)
        if preferred_price is None or (price is not None and abs(float(price) - float(preferred_price)) <= 1e-9):
            same_price_rows.append(row)
    same_price_inside = [row for row in same_price_rows if _ltp_inside_valid_quote(row)]
    if same_price_inside:
        return min(
            same_price_inside,
            key=lambda row: as_float(row.get("received_epoch")) or float(row.get("epoch_second") or epoch_second),
        )
    if inside_quote_rows:
        return min(
            inside_quote_rows,
            key=lambda row: (
                abs(float(as_float(row.get("price")) or 0.0) - float(preferred_price))
                if preferred_price is not None
                else 0.0,
                as_float(row.get("received_epoch")) or float(row.get("epoch_second") or epoch_second),
            ),
        )
    if not same_price_rows:
        return _without_stale_quote(fallback)
    valid_rows = [row for row in same_price_rows if _has_valid_non_crossed_quote(row)]
    if not valid_rows:
        return _without_stale_quote(fallback)
    return _without_stale_quote(
        min(
            valid_rows,
            key=lambda row: as_float(row.get("received_epoch")) or float(row.get("epoch_second") or epoch_second),
        )
    )


def checkpoint_epochs_for_candidate(
    *,
    runner: PassiveV2Runner,
    signal_clock_rows: list[dict[str, Any]],
    due_epoch: int,
    end_epoch: int,
) -> list[int]:
    entry_delay_seconds = int(runner.config.get("entry_delay_seconds") or 60)
    decision_delay_seconds = int(getattr(runner, "decision_delay_seconds", runner.config.get("decision_delay_seconds") or 5))
    delays = {entry_delay_seconds, decision_delay_seconds}
    out = {
        int(row.get("epoch_second") or row.get("epoch") or 0) + int(delay)
        for row in signal_clock_rows
        for delay in delays
        if int(row.get("epoch_second") or row.get("epoch") or 0) > 0
    }
    out = {epoch for epoch in out if int(due_epoch) <= int(epoch) <= int(end_epoch)}
    out.add(int(end_epoch))
    return sorted(out)


def frame_from_path_until(path: PathArrays, checkpoint_epoch: int):
    import pandas as pd  # type: ignore

    end_idx = bisect.bisect_right(path.epochs, float(checkpoint_epoch))
    rows = path.rows_between(0, end_idx)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("epoch_second", kind="mergesort").reset_index(drop=True)


def contract_state_at(
    *,
    path: PathArrays,
    clock_rows: list[dict[str, Any]],
    checkpoint_epoch: int,
    trade_date: str,
) -> dict[str, Any]:
    import pandas as pd  # type: ignore

    seconds = frame_from_path_until(path, checkpoint_epoch)
    if seconds.empty:
        today_seconds = seconds
        latest_tick: dict[str, Any] | None = None
    else:
        today_seconds = seconds[seconds["trade_date"].astype(str) == str(trade_date)].copy()
        latest_tick = seconds.iloc[-1].to_dict()
    filtered_clocks = [
        row
        for row in clock_rows
        if int(row.get("epoch_second") or row.get("epoch") or 0) <= int(checkpoint_epoch)
    ]
    clock_state = pd.DataFrame(filtered_clocks)
    latest_clock = clock_state.iloc[-1].to_dict() if not clock_state.empty else {}
    return {
        "seconds": seconds,
        "today_seconds": today_seconds,
        "clock_state": clock_state,
        "latest_tick": latest_tick,
        "latest_clock": latest_clock,
        "entry_edges_today": [],
    }


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


def score_from_rows(
    *,
    combo: dict[str, Any],
    exit_combo: dict[str, Any],
    tranche3_combo: dict[str, Any],
    rows: list[dict[str, Any]],
    invalid_reason: str | None = None,
    missing_variants: list[str] | None = None,
    available_variant_count: int | None = None,
) -> dict[str, Any]:
    return {
        "combo": combo,
        "combo_label": combo_label(combo),
        "exit_combo": exit_combo,
        "exit_combo_label": exit_combo_label(exit_combo),
        "tranche3_combo": tranche3_combo,
        "tranche3_combo_label": tranche3_combo_label(tranche3_combo),
        "joint_label": f"{combo_label(combo)}|{exit_combo_label(exit_combo)}|{tranche3_combo_label(tranche3_combo)}",
        "rows": rows,
        "summary_three_lot": summarize_sequence(rows, metric_prefix="three_lot"),
        "summary_two_lot": summarize_sequence(rows, metric_prefix="two_lot"),
        "summary_one_lot": summarize_sequence(rows, metric_prefix="one_lot"),
        "invalid_reason": invalid_reason,
        "missing_variants": missing_variants or [],
        "available_variant_count": available_variant_count,
    }


def combo_meta(meta: Any, point_config: dict[str, Any], tranche3_combo: dict[str, Any]) -> Any:
    out = copy.copy(meta)
    out.execution_point_config = point_config
    out.adaptive_calibration = {
        "adopted": True,
        "tranche3_combo": dict(tranche3_combo or {}),
        "tranche3_combo_label": tranche3_combo_label(tranche3_combo or {}),
        "overrides": {
            "tranche3_config": tranche3_config_for_combo(tranche3_combo),
        },
    }
    return out


def rows_from_events_and_state(
    *,
    meta: Any,
    events: list[dict[str, Any]],
    model_state: dict[str, Any],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        merged = event.get("position") if isinstance(event.get("position"), dict) else event
        signal_id = str(merged.get("signal_id") or event.get("signal_id") or "")
        if not signal_id:
            continue
        group = groups.setdefault(signal_id, {"events": []})
        group["events"].append(event)
        kind = event.get("event")
        if kind == "paper_entry":
            group["entry"] = dict(merged)
        elif kind == "paper_exit":
            group["paper_exit"] = dict(merged)
        elif kind == "tranche2_exit":
            group["tranche2_exit"] = dict(merged)
        elif kind == "tranche3_entry":
            group["tranche3_entry"] = dict(merged)
        elif kind == "tranche3_exit":
            group["tranche3_exit"] = dict(merged)
    open_position = model_state.get("position") if isinstance(model_state.get("position"), dict) else None
    if open_position and open_position.get("signal_id"):
        signal_id = str(open_position.get("signal_id"))
        group = groups.setdefault(signal_id, {"events": []})
        group.setdefault("entry", dict(open_position))
        group["open_position"] = dict(open_position)
    rows: list[dict[str, Any]] = []
    for signal_id, group in groups.items():
        entry = group.get("entry")
        if not isinstance(entry, dict):
            continue
        paper_exit = group.get("paper_exit") if isinstance(group.get("paper_exit"), dict) else None
        open_pos = group.get("open_position") if isinstance(group.get("open_position"), dict) else None
        position_for_state = open_pos or paper_exit or entry
        t2: dict[str, Any] = {}
        if isinstance(position_for_state.get("two_lot_ttsl"), dict):
            t2 = dict(position_for_state.get("two_lot_ttsl", {}).get("tranche2") or {})
        if group.get("tranche2_exit"):
            t2 = {**t2, **group["tranche2_exit"]}
        t3 = position_for_state.get("tranche3") if isinstance(position_for_state.get("tranche3"), dict) else {}
        if (
            isinstance(t3, dict)
            and t3.get("status") not in {"open", "closed"}
            and not t3.get("entry_epoch")
        ):
            t3 = {}
        if group.get("tranche3_entry"):
            t3 = {**t3, **group["tranche3_entry"]}
        if group.get("tranche3_exit"):
            t3 = {**t3, **group["tranche3_exit"]}
        status = "closed" if paper_exit else "open_mark"
        side = str(entry.get("side") or "")
        entry_ltp = as_float(entry.get("entry_ltp_price") or entry.get("entry_price"))
        one_margin = margin_for(meta, side, entry_ltp)
        t1_net = as_float(paper_exit.get("net_rupees")) if paper_exit else as_float((open_pos or {}).get("net_rupees_if_closed"))
        t2_net = as_float(t2.get("net_rupees")) if isinstance(t2, dict) else None
        t3_net = as_float(t3.get("net_rupees")) if isinstance(t3, dict) else None
        t3_entered = bool(isinstance(t3, dict) and t3.get("entry_epoch"))
        two_net = (t1_net or 0.0) + (t2_net or 0.0)
        three_net = two_net + (t3_net or 0.0)
        rows.append(
            {
                "symbol": meta.symbol,
                "signal_id": signal_id,
                "signal_epoch": entry.get("signal_epoch"),
                "source": entry.get("source"),
                "side": side,
                "status": status,
                "entry_epoch": entry.get("entry_epoch"),
                "entry_ltp_price": entry_ltp,
                "entry_fill_price": entry.get("entry_fill_price"),
                "exit_epoch": paper_exit.get("exit_epoch") if paper_exit else (open_pos or {}).get("latest_epoch"),
                "exit_reason": paper_exit.get("exit_reason") if paper_exit else "open_mark_if_closed",
                "exit_ltp_price": paper_exit.get("exit_ltp_price") if paper_exit else (open_pos or {}).get("latest_price"),
                "exit_fill_price": paper_exit.get("exit_fill_price") if paper_exit else (open_pos or {}).get("latest_fill_price_if_closed"),
                "t1_net_rupees": t1_net,
                "t2_exit_epoch": t2.get("exit_epoch"),
                "t2_exit_ltp_price": t2.get("exit_price") or t2.get("partial_exit_ltp"),
                "t2_exit_fill_price": t2.get("exit_fill_price") or t2.get("partial_exit_fill_price"),
                "t2_net_rupees": t2_net,
                "t3_entry_epoch": t3.get("entry_epoch") if isinstance(t3, dict) else None,
                "t3_entry_fill_price": t3.get("entry_fill_price") if isinstance(t3, dict) else None,
                "t3_entry_mode": t3.get("entry_mode") if isinstance(t3, dict) else None,
                "t3_entry_reason": t3.get("entry_reason") if isinstance(t3, dict) else None,
                "t3_entry_trigger_price": t3.get("entry_trigger_price") if isinstance(t3, dict) else None,
                "t3_hard_sl_price": t3.get("hard_sl_price") if isinstance(t3, dict) else None,
                "t3_exit_epoch": t3.get("exit_epoch") if isinstance(t3, dict) else None,
                "t3_exit_ltp_price": t3.get("exit_price") if isinstance(t3, dict) else None,
                "t3_exit_fill_price": t3.get("exit_fill_price") if isinstance(t3, dict) else None,
                "t3_net_rupees": t3_net,
                "one_lot_margin_rupees": one_margin,
                "two_lot_margin_rupees": 2.0 * one_margin if one_margin else None,
                "three_lot_peak_margin_rupees": (3.0 if t3_entered else 2.0) * one_margin if one_margin else None,
                "one_lot_net_rupees": t1_net,
                "one_lot_net_pct_margin": (100.0 * t1_net / one_margin) if one_margin and t1_net is not None else None,
                "two_lot_net_rupees": two_net,
                "two_lot_net_pct_margin": (100.0 * two_net / (2.0 * one_margin)) if one_margin else None,
                "three_lot_net_rupees": three_net,
                "three_lot_net_pct_margin": (
                    100.0 * three_net / ((3.0 if t3_entered else 2.0) * one_margin)
                )
                if one_margin
                else None,
            }
        )
    return sorted(rows, key=lambda row: int(row.get("signal_epoch") or row.get("entry_epoch") or 0))


def simulate_combo_chronological(
    *,
    runner: PassiveV2Runner,
    v1: Any,
    meta: Any,
    combo: dict[str, Any],
    exit_combo: dict[str, Any],
    tranche3_combo: dict[str, Any],
    candidates: list[dict[str, Any]],
    signal_rows: PathArrays,
    exec_rows: PathArrays,
    signal_clock_rows: list[dict[str, Any]],
    execution_clock_rows: list[dict[str, Any]],
    signal_online_state: OnlineObvState | None = None,
    execution_online_state: OnlineObvState | None = None,
    end_epoch: int,
    available_variants: set[str] | None = None,
) -> dict[str, Any]:
    active_variants = combo_variants(combo)
    missing_variants = sorted(active_variants - set(available_variants or set()))
    if missing_variants:
        return score_from_rows(
            combo=combo,
            exit_combo=exit_combo,
            tranche3_combo=tranche3_combo,
            rows=[],
            invalid_reason="missing_required_entry_variants",
            missing_variants=missing_variants,
            available_variant_count=len(available_variants or set()),
        )
    selected = [dict(item) for item in candidates if str(item.get("variant") or "") in active_variants]
    selected.sort(key=lambda row: int(row.get("signal_epoch") or 0))
    if not selected:
        return score_from_rows(combo=combo, exit_combo=exit_combo, tranche3_combo=tranche3_combo, rows=[])
    rows: list[dict[str, Any]] = []
    last_exit_epoch = 0
    for edge in selected:
        signal_epoch = int(edge.get("signal_epoch") or 0)
        if signal_epoch <= last_exit_epoch:
            continue
        outcome = simulate_candidate(
            runner=runner,
            v1=v1,
            meta=meta,
            candidate=edge,
            signal_rows=signal_rows,
            exec_rows=exec_rows,
            signal_clock_rows=signal_clock_rows,
            execution_clock_rows=execution_clock_rows,
            end_epoch=end_epoch,
            exit_combo=exit_combo,
            tranche3_combo=tranche3_combo,
        )
        if outcome.get("status") not in {"closed", "open_mark"}:
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
    return score_from_rows(combo=combo, exit_combo=exit_combo, tranche3_combo=tranche3_combo, rows=rows)


def simulate_candidate(
    *,
    runner: PassiveV2Runner,
    v1: Any,
    meta: Any,
    candidate: dict[str, Any],
    signal_rows: PathArrays,
    exec_rows: PathArrays,
    signal_clock_rows: list[dict[str, Any]],
    execution_clock_rows: list[dict[str, Any]],
    end_epoch: int,
    exit_combo: dict[str, Any],
    tranche3_combo: dict[str, Any] | None = None,
    signal_clock_state_frame: Any | None = None,
    execution_clock_state_frame: Any | None = None,
    execution_clock_epochs: list[int] | None = None,
    execution_seconds_frame: Any | None = None,
    base_exit_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    import pandas as pd  # type: ignore

    signal_epoch = int(candidate.get("signal_epoch") or 0)
    side = str(candidate.get("side") or "")
    due_epoch = signal_epoch + int(runner.config.get("entry_delay_seconds") or 60)
    max_carry_age = as_float(runner.config.get("entry_quote_carry_max_age_seconds"))
    if max_carry_age is None:
        max_carry_age = as_float(runner.config.get("signal_quote_max_age_seconds")) or 45.0
    entry_idx, entry_row = entry_row_for_due(
        exec_rows,
        due_epoch=due_epoch,
        max_carry_age_seconds=float(max_carry_age),
    )
    if entry_row is None:
        return {**candidate, "status": "entry_unavailable", "entry_due_epoch": due_epoch}
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
    execution_clock_state = (
        execution_clock_state_frame
        if execution_clock_state_frame is not None
        else pd.DataFrame(execution_clock_rows)
    )
    signal_clock_state = (
        signal_clock_state_frame
        if signal_clock_state_frame is not None
        else pd.DataFrame(signal_clock_rows)
    )
    hard_sl = v1.dynamic_risk_points(
        execution_clock_state,
        signal_epoch,
        kind="hard_sl",
        point_config=meta.execution_point_config,
    )
    hard_sl = float(hard_sl) * float(exit_combo.get("hard_sl_scale") or 1.0)
    trail_activation = v1.dynamic_risk_points(
        execution_clock_state,
        signal_epoch,
        kind="trail_activation",
        point_config=meta.execution_point_config,
    )
    trail_activation = float(trail_activation) * float(exit_combo.get("trail_activation_scale") or 1.0)
    if not math.isfinite(float(hard_sl)) or not math.isfinite(float(trail_activation)):
        return {**candidate, "status": "risk_points_unavailable", "entry_due_epoch": due_epoch}
    point_config = point_config_for_exit_combo(
        meta.execution_point_config,
        exit_combo,
        hard_sl_points=float(hard_sl),
    )
    ttsl_config = ttsl_config_for_exit_combo(exit_combo)
    tranche3_config = tranche3_config_for_combo(tranche3_combo)
    position = candidate_position(
        runner=runner,
        v1=v1,
        meta=meta,
        candidate=candidate,
        entry_row=entry_row,
        entry_fill=entry_fill,
        hard_sl=float(hard_sl),
        trail_activation=float(trail_activation),
        point_config=point_config,
        ttsl_config=ttsl_config,
        tranche3_config=tranche3_config,
    )
    # Base/T1/T2 lifecycle is independent of T3. Do not let cached base
    # outcomes carry a T3 state from whichever T3 combo was scored first.
    position.pop("tranche3", None)
    base_cache_key = (
        f"{candidate.get('candidate_id')}::{exit_combo_identity(exit_combo)}"
        if base_exit_cache is not None
        else None
    )
    cached_base = base_exit_cache.get(base_cache_key) if base_cache_key and base_exit_cache is not None else None
    if cached_base:
        position = copy.deepcopy(cached_base["position_after_t2"])
        base_exit_row = dict(cached_base["base_exit_row"])
        base_exit_reason = str(cached_base["base_exit_reason"])
        base_exit_epoch = int(cached_base["base_exit_epoch"])
        final_epoch_for_tranches = int(cached_base["final_epoch_for_tranches"])
        tranche_end_idx = int(cached_base["tranche_end_idx"])
        latest_exit_fill_price = as_float(cached_base.get("latest_exit_fill_price"))
        tranche2_exit_event = copy.deepcopy(cached_base.get("tranche2_exit_event"))
        tranche2_event_exit_epoch = int(cached_base["tranche2_event_exit_epoch"])
    else:
        replay_end_idx_exclusive = bisect.bisect_right(exec_rows.epochs, float(end_epoch))
        if execution_seconds_frame is not None:
            path_frame = execution_seconds_frame.iloc[
                max(0, entry_idx) : min(len(exec_rows), replay_end_idx_exclusive)
            ]
            if path_frame.empty:
                path_frame = pd.DataFrame([entry_row])
        else:
            path_rows = exec_rows.rows_between(entry_idx, replay_end_idx_exclusive)
            if not path_rows:
                path_rows = [entry_row]
            path_frame = pd.DataFrame(path_rows)
        path_exit = v1.first_exit_on_path(
            path_frame,
            side=side,
            entry_price=float(entry_ltp_price),
            hard_sl_points=float(hard_sl),
            trail_activation_points=float(trail_activation),
            clock_state=signal_clock_state,
            entry_epoch=int(position["entry_epoch"]),
            signal_epoch=signal_epoch,
            exit_config=point_config,
        )
        if path_exit and path_exit.get("exit_row") is not None:
            base_exit_row = dict(path_exit["exit_row"])
            base_exit_reason = str(path_exit["exit_reason"])
            base_exit_epoch = int(base_exit_row["epoch_second"])
            base_exit_row = canonical_same_second_fill_row(
                exec_rows,
                epoch_second=base_exit_epoch,
                fallback=base_exit_row,
                preferred_price=as_float(base_exit_row.get("price")),
            )
        else:
            latest_idx = min(len(exec_rows) - 1, bisect.bisect_right(exec_rows.epochs, float(end_epoch)) - 1)
            base_exit_row = exec_rows.row(latest_idx) if latest_idx >= entry_idx else entry_row
            base_exit_reason = "open_mark_if_closed"
            base_exit_epoch = int(base_exit_row["epoch_second"])
            base_exit_row = canonical_same_second_fill_row(
                exec_rows,
                epoch_second=base_exit_epoch,
                fallback=base_exit_row,
                preferred_price=as_float(base_exit_row.get("price")),
            )

        final_epoch_for_tranches = max(int(position["entry_epoch"]), int(base_exit_epoch) - 1)
        tranche_end_idx = bisect.bisect_right(exec_rows.epochs, float(final_epoch_for_tranches))
        if execution_seconds_frame is not None:
            tranche_path_frame = execution_seconds_frame.iloc[
                max(0, entry_idx) : min(len(exec_rows), tranche_end_idx)
            ]
            if tranche_path_frame.empty:
                tranche_path_frame = pd.DataFrame([entry_row])
        else:
            tranche_path_rows = exec_rows.rows_between(entry_idx, tranche_end_idx)
            if not tranche_path_rows:
                tranche_path_rows = [entry_row]
            tranche_path_frame = pd.DataFrame(tranche_path_rows)
        latest_fill_for_mark = v1.execution_fill_from_row(
            base_exit_row,
            side=side,
            phase="exit",
            point_config=point_config,
            fallback_round_trip_cost_points=float(meta.round_trip_cost_points),
        )
        latest_exit_fill_price = as_float(latest_fill_for_mark.get("fill_price"))
        position, tranche2_events = v1._update_live_two_lot_ttsl(
            position=position,
            path=tranche_path_frame,
            clock_state=execution_clock_state,
            latest_exit_fill_price=latest_exit_fill_price,
            latest_exit_time=epoch_ist_iso(base_exit_epoch),
            cost_points=float(meta.round_trip_cost_points),
            lot_size=int(meta.lot_size or 1),
            point_config=point_config,
            config=ttsl_config,
            final_epoch=final_epoch_for_tranches,
        )
        tranche2_exit_event = next(
            (event for event in tranche2_events if isinstance(event, dict) and event.get("event") == "tranche2_exit"),
            None,
        )
        tranche2_event_exit_epoch = (
            max(0, int(tranche2_exit_event.get("exit_epoch") or 0) - 1)
            if tranche2_exit_event and tranche2_exit_event.get("exit_epoch") is not None
            else final_epoch_for_tranches
        )
        path_price_end_idx = bisect.bisect_right(exec_rows.epochs, float(base_exit_epoch))
        if execution_seconds_frame is not None:
            path_prices = [
                float(value)
                for value in execution_seconds_frame.iloc[
                    max(0, entry_idx) : min(len(exec_rows), path_price_end_idx)
                ]["price"].tolist()
            ]
        else:
            path_prices = [
                float(row["price"])
                for row in exec_rows.rows_between(entry_idx, path_price_end_idx)
            ]
        if path_prices:
            if side == "long":
                position["max_favorable_points"] = max(0.0, max(path_prices) - float(entry_ltp_price))
                position["max_adverse_points"] = max(0.0, float(entry_ltp_price) - min(path_prices))
            else:
                position["max_favorable_points"] = max(0.0, float(entry_ltp_price) - min(path_prices))
                position["max_adverse_points"] = max(0.0, max(path_prices) - float(entry_ltp_price))
        if base_cache_key and base_exit_cache is not None:
            cached_position = copy.deepcopy(position)
            cached_position.pop("tranche3", None)
            base_exit_cache[base_cache_key] = {
                "position_after_t2": cached_position,
                "base_exit_row": dict(base_exit_row),
                "base_exit_reason": base_exit_reason,
                "base_exit_epoch": int(base_exit_epoch),
                "final_epoch_for_tranches": int(final_epoch_for_tranches),
                "tranche_end_idx": int(tranche_end_idx),
                "latest_exit_fill_price": latest_exit_fill_price,
                "tranche2_exit_event": copy.deepcopy(tranche2_exit_event),
                "tranche2_event_exit_epoch": int(tranche2_event_exit_epoch),
            }
    if execution_seconds_frame is not None:
        tranche_path_frame = execution_seconds_frame.iloc[
            max(0, entry_idx) : min(len(exec_rows), tranche_end_idx)
        ]
        if tranche_path_frame.empty:
            tranche_path_frame = pd.DataFrame([entry_row])
    else:
        tranche_path_rows = exec_rows.rows_between(entry_idx, tranche_end_idx)
        if not tranche_path_rows:
            tranche_path_rows = [entry_row]
        tranche_path_frame = pd.DataFrame(tranche_path_rows)
    tranche3_final_epoch = resolve_tranche3_final_epoch(position, tranche2_event_exit_epoch)
    position, tranche3_events = _compact_update_tranche3_for_candidate(
        v1=v1,
        position=position,
        exec_rows=exec_rows,
        execution_clock_epochs=execution_clock_epochs
        if execution_clock_epochs is not None
        else [int(value) for value in getattr(execution_clock_state, "epoch_second", [])],
        entry_idx=entry_idx,
        tranche_end_idx=tranche_end_idx,
        latest_exit_fill_price=latest_exit_fill_price,
        latest_exit_time=epoch_ist_iso(base_exit_epoch),
        cost_points=float(meta.round_trip_cost_points),
        lot_size=int(meta.lot_size or 1),
        point_config=point_config,
        config=tranche3_config_for_combo(tranche3_combo),
        final_epoch=tranche3_final_epoch or None,
    )
    tranche3_events = filter_valid_tranche3_events(position, tranche3_events)
    if tranche2_exit_event and tranche3_close_allowed(position, tranche2_exit_event.get("exit_epoch")):
        position, t3_from_t2 = v1._live_tranche3_close_from_event(
            position=position,
            exit_event=tranche2_exit_event,
            exit_source="ttsl_exit",
            exit_reason="tranche3_v1_ttsl_exit",
            lot_size=int(meta.lot_size or 1),
            point_config=point_config,
        )
        if t3_from_t2 and valid_tranche3_event(position, t3_from_t2):
            tranche3_events.append(t3_from_t2)

    exit_event: dict[str, Any] | None = None
    exit_fill = v1.execution_fill_from_row(
        base_exit_row,
        side=side,
        phase="exit",
        point_config=point_config,
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
            point_config=point_config,
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
                point_config=point_config,
            )
            tranche3_base_events: list[dict[str, Any]] = []
            if tranche3_close_allowed(position, exit_event.get("exit_epoch")):
                position, exit_event, tranche3_base_events = v1._finalize_live_tranche3_on_base_exit(
                    position=position,
                    exit_event=exit_event,
                    lot_size=int(meta.lot_size or 1),
                    point_config=point_config,
                )
            for event in filter_valid_tranche3_events(position, tranche3_base_events):
                if isinstance(event, dict):
                    tranche3_events.append(event)
        else:
            position = v1._refresh_live_two_lot_marks(
                position,
                latest_exit_fill_price=exit_fill_price,
                latest_exit_time=epoch_ist_iso(base_exit_epoch),
                lot_size=int(meta.lot_size or 1),
                point_config=point_config,
            )
            position = v1._refresh_live_tranche3_marks(
                position,
                latest_exit_fill_price=exit_fill_price,
                latest_exit_time=epoch_ist_iso(base_exit_epoch),
                lot_size=int(meta.lot_size or 1),
                point_config=point_config,
            )

    nets = outcome_nets(position, exit_event)
    t2_state = ((position.get("two_lot_ttsl") or {}).get("tranche2") or {}) if isinstance(position.get("two_lot_ttsl"), dict) else {}
    t3_state = position.get("tranche3") if isinstance(position.get("tranche3"), dict) else {}
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
        "hard_sl_points": float(hard_sl),
        "trail_activation_points": float(trail_activation),
        "trail_activation_effective_points": float(position["trail_activation_effective_points"]),
        "t2_exit_epoch": t2_state.get("exit_epoch"),
        "t2_exit_ltp_price": t2_state.get("exit_price"),
        "t2_exit_fill_price": t2_state.get("exit_fill_price"),
        "t2_exit_stop": t2_state.get("exit_stop"),
        "t2_sl_price": t2_state.get("sl_price"),
        "t2_sl_mode": t2_state.get("sl_mode"),
        "t3_entry_epoch": t3_state.get("entry_epoch"),
        "t3_entry_fill_price": t3_state.get("entry_fill_price"),
        "t3_entry_mode": t3_state.get("entry_mode"),
        "t3_entry_reason": t3_state.get("entry_reason"),
        "t3_entry_trigger_price": t3_state.get("entry_trigger_price"),
        "t3_hard_sl_price": t3_state.get("hard_sl_price"),
        "t3_exit_epoch": t3_state.get("exit_epoch"),
        "t3_exit_ltp_price": t3_state.get("exit_price"),
        "t3_exit_fill_price": t3_state.get("exit_fill_price"),
        "t3_exit_reason": t3_state.get("exit_reason"),
        "one_lot_margin_rupees": one_margin,
        "two_lot_margin_rupees": 2.0 * one_margin if one_margin else None,
        "three_lot_peak_margin_rupees": (3.0 if t3_entered else 2.0) * one_margin if one_margin else None,
        "max_favorable_points": position.get("max_favorable_points"),
        "max_adverse_points": position.get("max_adverse_points"),
        **nets,
        "one_lot_net_rupees": nets.get("t1_net_rupees"),
        "one_lot_net_pct_margin": (100.0 * nets["t1_net_rupees"] / one_margin) if one_margin and nets.get("t1_net_rupees") is not None else None,
        "two_lot_net_pct_margin": (100.0 * nets["two_lot_net_rupees"] / (2.0 * one_margin)) if one_margin else None,
        "three_lot_net_pct_margin": (
            100.0 * nets["three_lot_net_rupees"] / ((3.0 if t3_entered else 2.0) * one_margin)
        )
        if one_margin
        else None,
        "exhaustion_status": {},
        "exit_combo": exit_combo,
        "exit_combo_label": exit_combo_label(exit_combo),
        "tranche3_combo": tranche3_combo,
        "tranche3_combo_label": tranche3_combo_label(tranche3_combo or {}),
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


PRIMARY_SUMMARY_KEY = "summary_three_lot"


def score_combo(
    *,
    combo: dict[str, Any],
    exit_combo: dict[str, Any],
    tranche3_combo: dict[str, Any],
    candidates: list[dict[str, Any]],
    outcomes: dict[str, dict[str, Any]],
    available_variants: set[str] | None = None,
) -> dict[str, Any]:
    active_variants = combo_variants(combo)
    missing_variants = sorted(active_variants - set(available_variants or set()))
    selected = [item for item in candidates if str(item.get("variant") or "") in active_variants]
    selected.sort(key=lambda row: int(row.get("signal_epoch") or 0))
    rows: list[dict[str, Any]] = []
    invalid_reason = "missing_required_entry_variants" if missing_variants else None
    if invalid_reason:
        return {
            "combo": combo,
            "combo_label": combo_label(combo),
            "exit_combo": exit_combo,
            "exit_combo_label": exit_combo_label(exit_combo),
            "tranche3_combo": tranche3_combo,
            "tranche3_combo_label": tranche3_combo_label(tranche3_combo),
            "joint_label": f"{combo_label(combo)}|{exit_combo_label(exit_combo)}|{tranche3_combo_label(tranche3_combo)}",
            "rows": rows,
            "summary_three_lot": summarize_sequence(rows, metric_prefix="three_lot"),
            "summary_two_lot": summarize_sequence(rows, metric_prefix="two_lot"),
            "summary_one_lot": summarize_sequence(rows, metric_prefix="one_lot"),
            "invalid_reason": invalid_reason,
            "missing_variants": missing_variants,
            "available_variant_count": len(available_variants or set()),
        }
    last_exit_epoch = 0
    for item in selected:
        outcome = outcomes.get(str(item.get("candidate_id")))
        if not outcome or outcome.get("status") not in {"closed", "open_mark"}:
            continue
        signal_epoch = int(outcome.get("signal_epoch") or item.get("signal_epoch") or 0)
        if signal_epoch <= last_exit_epoch:
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
        "exit_combo": exit_combo,
        "exit_combo_label": exit_combo_label(exit_combo),
        "tranche3_combo": tranche3_combo,
        "tranche3_combo_label": tranche3_combo_label(tranche3_combo),
        "joint_label": f"{combo_label(combo)}|{exit_combo_label(exit_combo)}|{tranche3_combo_label(tranche3_combo)}",
        "rows": rows,
        "summary_three_lot": summarize_sequence(rows, metric_prefix="three_lot"),
        "summary_two_lot": summarize_sequence(rows, metric_prefix="two_lot"),
        "summary_one_lot": summarize_sequence(rows, metric_prefix="one_lot"),
    }


def reject_reason(candidate: dict[str, Any], current: dict[str, Any] | None, args: argparse.Namespace) -> str | None:
    if candidate.get("invalid_reason"):
        return str(candidate.get("invalid_reason"))
    summary = candidate[PRIMARY_SUMMARY_KEY]
    trades = int(summary.get("trade_count") or 0)
    closed = int(summary.get("closed_count") or 0)
    success = as_float(summary.get("success_rate_pct"))
    worst_pct = as_float(summary.get("min_net_pct_margin"))
    current_worst_pct = as_float((current or {}).get(PRIMARY_SUMMARY_KEY, {}).get("min_net_pct_margin"))
    if trades < int(args.min_trades):
        return "too_few_trades"
    if closed < int(args.min_closed_trades):
        return "too_few_closed_trades"
    if success is not None and success < float(args.min_success_rate_pct):
        return "low_success_rate"
    if current_worst_pct is not None and worst_pct is not None:
        if worst_pct < current_worst_pct - float(args.max_worst_loss_pct_deterioration):
            return "worse_worst_loss_than_current"
    metric_prefix = PRIMARY_SUMMARY_KEY.removeprefix("summary_")
    nets = [float(row.get(f"{metric_prefix}_net_rupees") or 0.0) for row in candidate.get("rows") or []]
    positive = [value for value in nets if value > 0]
    if len(nets) <= 2 and sum(positive) > 0:
        return "one_trade_overfit"
    if positive and max(positive) > float(args.max_single_win_share) * max(1.0, sum(positive)):
        return "single_win_dominates"
    return None


def risk_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    s = item[PRIMARY_SUMMARY_KEY]
    return (
        as_float(s.get("min_net_pct_margin")) if as_float(s.get("min_net_pct_margin")) is not None else -9999.0,
        as_float(s.get("max_drawdown_rupees")) if as_float(s.get("max_drawdown_rupees")) is not None else -10**18,
        as_float(s.get("success_rate_pct")) if as_float(s.get("success_rate_pct")) is not None else -1.0,
        as_float(s.get("median_net_pct_margin")) if as_float(s.get("median_net_pct_margin")) is not None else -9999.0,
        as_float(s.get("total_net_rupees")) if as_float(s.get("total_net_rupees")) is not None else -10**18,
        int(s.get("trade_count") or 0),
    )


def return_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    s = item[PRIMARY_SUMMARY_KEY]
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
    cur = current[PRIMARY_SUMMARY_KEY]
    best = risk_best[PRIMARY_SUMMARY_KEY]
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
        "exit_combo": item.get("exit_combo"),
        "exit_combo_label": item.get("exit_combo_label"),
        "tranche3_combo": item.get("tranche3_combo"),
        "tranche3_combo_label": item.get("tranche3_combo_label"),
        "joint_label": item.get("joint_label"),
        "invalid_reason": item.get("invalid_reason"),
        "missing_variants": item.get("missing_variants"),
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
        "schema": "obvfutport_v2.t1_t2_t3_joint_risk_first_scoring.v1",
        "started_at_ist": epoch_ist_iso(time.time()),
        "config": str(args.config),
        "candidate_file": str(args.candidate_file),
        "output_dir": str(output_dir),
        "scoring_policy": {
            "primary": "risk_first",
            "primary_metric": PRIMARY_SUMMARY_KEY,
            "entry_combo_scope": "current_deployed + best_risk_first + best_return from prior T1 report when supplied",
            "exit_combo_scope": "compact risk-first T1/T2 grid",
            "tranche3_combo_scope": "compact T3 activation-clock/R-multiple grid; T3 exits via selected T2/T1 path",
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
    prior_entry_report = load_entry_combo_report(args.entry_combo_report)
    entry_combos_by_symbol = {
        meta.symbol: entry_combos_for_symbol(
            meta=meta,
            args=args,
            entry_report_item=prior_entry_report.get(meta.symbol) if prior_entry_report else None,
        )
        for meta in metas
    }
    allowed_variants_by_symbol = {
        symbol: set().union(*(combo_variants(combo) for combo in combos)) if combos else set()
        for symbol, combos in entry_combos_by_symbol.items()
    }
    base_exit_combos = build_exit_combos(args)
    base_tranche3_combos = build_tranche3_combos(args)
    exit_combos_by_symbol = {
        meta.symbol: exit_combos_for_symbol(meta=meta, args=args, base_combos=base_exit_combos)
        for meta in metas
    }
    tranche3_combos_by_symbol = {
        meta.symbol: tranche3_combos_for_symbol(meta=meta, args=args, base_combos=base_tranche3_combos)
        for meta in metas
    }
    unique_exit_combos = dedupe_exit_combos(
        combo for combos in exit_combos_by_symbol.values() for combo in combos
    )
    unique_tranche3_combos = dedupe_tranche3_combos(
        combo for combos in tranche3_combos_by_symbol.values() for combo in combos
    )
    report["current_runtime_combos_only"] = bool(args.current_runtime_combos_only)
    report["exit_combo_count"] = len(unique_exit_combos)
    report["exit_combos"] = unique_exit_combos
    report["tranche3_combo_count"] = len(unique_tranche3_combos)
    report["tranche3_combos"] = unique_tranche3_combos
    report["exit_combo_count_by_symbol"] = {
        symbol: len(combos) for symbol, combos in exit_combos_by_symbol.items()
    }
    report["tranche3_combo_count_by_symbol"] = {
        symbol: len(combos) for symbol, combos in tranche3_combos_by_symbol.items()
    }
    report["entry_combo_count_by_symbol"] = {
        symbol: len(combos) for symbol, combos in entry_combos_by_symbol.items()
    }
    candidate_file = Path(args.candidate_file)
    supported_variants_by_symbol = load_supported_variants_by_symbol(candidate_file, set(meta_by_symbol))
    candidates_by_symbol = load_candidates(
        candidate_file,
        set(meta_by_symbol),
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        allowed_variants_by_symbol=allowed_variants_by_symbol,
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
            second_row_retention_seconds=None,
            compute_non_clock_percentiles=False,
        )
        for key in target_keys
    }
    path_stores = {key: PathArrays() for key in target_keys}
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
                path_store = path_stores.get(key)
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
    report["target_path_rows"] = {key: len(path) for key, path in path_stores.items()}

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
    chronological_combo_mode = bool(args.chronological_combo_simulation)
    outcomes_by_symbol: dict[str, dict[str, dict[str, dict[str, Any]]]] = defaultdict(lambda: defaultdict(dict))
    outcome_rows_written = 0
    outcome_path = output_dir / "candidate_outcomes.jsonl"
    with stage_timer(report, "candidate_outcome_simulation"):
        with outcome_path.open("w", encoding="utf-8") as handle:
            if chronological_combo_mode:
                report["candidate_outcome_simulation_mode"] = "skipped_chronological_combo_scoring"
            else:
                for meta in metas:
                    symbol_candidates = candidates_by_symbol.get(meta.symbol) or []
                    if not symbol_candidates:
                        continue
                    exec_rows = path_stores.get(meta.execution_key)
                    signal_rows = path_stores.get(meta.signal_key)
                    signal_state = states.get(meta.signal_key)
                    execution_state = states.get(meta.execution_key)
                    if (
                        not exec_rows
                        or len(exec_rows) == 0
                        or not signal_rows
                        or len(signal_rows) == 0
                        or signal_state is None
                        or execution_state is None
                    ):
                        continue
                    for exit_combo in exit_combos_by_symbol.get(meta.symbol, []):
                        for tranche3_combo in tranche3_combos_by_symbol.get(meta.symbol, []):
                            family_label = outcome_family_label(exit_combo, tranche3_combo)
                            for candidate in symbol_candidates:
                                outcome = simulate_candidate(
                                    runner=runner,
                                    v1=v1,
                                    meta=meta,
                                    candidate=candidate,
                                    signal_rows=signal_rows,
                                    exec_rows=exec_rows,
                                    signal_clock_rows=signal_state.clock_rows,
                                    execution_clock_rows=execution_state.clock_rows,
                                    end_epoch=replay_end_epoch,
                                    exit_combo=exit_combo,
                                    tranche3_combo=tranche3_combo,
                                )
                                outcomes_by_symbol[meta.symbol][family_label][str(candidate.get("candidate_id"))] = outcome
                                handle.write(json.dumps(json_clean(outcome), sort_keys=True) + "\n")
                                outcome_rows_written += 1
                                if args.max_outcomes and outcome_rows_written >= int(args.max_outcomes):
                                    break
                            if args.max_outcomes and outcome_rows_written >= int(args.max_outcomes):
                                break
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
            outcomes_by_exit = outcomes_by_symbol.get(meta.symbol) or {}
            if not symbol_candidates:
                symbol_reports[meta.symbol] = {
                    "symbol": meta.symbol,
                    "status": "no_candidates",
                    "candidate_entries": len(symbol_candidates),
                    "threshold_source": meta.source,
                    "threshold_synthesized": meta.synthesized,
                }
                continue
            combos = entry_combos_by_symbol.get(meta.symbol) or build_combos(meta, args)
            supported_variants = (
                supported_variants_by_symbol.get(meta.symbol)
                if supported_variants_by_symbol is not None
                else {str(item.get("variant") or "") for item in symbol_candidates}
            )
            observed_variants = {str(item.get("variant") or "") for item in symbol_candidates}
            current_entry_label = combo_label(current_entry_combo(meta, args))
            current_exit_label = exit_combo_label(current_exit_combo(meta, args))
            current_tranche3_label = tranche3_combo_label(current_tranche3_combo(meta, args))
            scored = []
            if chronological_combo_mode:
                exec_rows = path_stores.get(meta.execution_key)
                signal_rows = path_stores.get(meta.signal_key)
                signal_state = states.get(meta.signal_key)
                execution_state = states.get(meta.execution_key)
                if (
                    not exec_rows
                    or len(exec_rows) == 0
                    or not signal_rows
                    or len(signal_rows) == 0
                    or signal_state is None
                    or execution_state is None
                ):
                    symbol_reports[meta.symbol] = {
                        "symbol": meta.symbol,
                        "status": "missing_target_state",
                        "candidate_entries": len(symbol_candidates),
                        "threshold_source": meta.source,
                        "threshold_synthesized": meta.synthesized,
                    }
                    continue
                for combo in combos:
                    for exit_combo in exit_combos_by_symbol.get(meta.symbol, []):
                        for tranche3_combo in tranche3_combos_by_symbol.get(meta.symbol, []):
                            scored.append(
                                simulate_combo_chronological(
                                    runner=runner,
                                    v1=v1,
                                    meta=meta,
                                    combo=combo,
                                    exit_combo=exit_combo,
                                    tranche3_combo=tranche3_combo,
                                    candidates=symbol_candidates,
                                    signal_rows=signal_rows,
                                    exec_rows=exec_rows,
                                    signal_clock_rows=signal_state.clock_rows,
                                    execution_clock_rows=execution_state.clock_rows,
                                    signal_online_state=signal_state,
                                    execution_online_state=execution_state,
                                    end_epoch=replay_end_epoch,
                                    available_variants=supported_variants,
                                )
                            )
            else:
                if not outcomes_by_exit:
                    symbol_reports[meta.symbol] = {
                        "symbol": meta.symbol,
                        "status": "no_candidate_outcomes",
                        "candidate_entries": len(symbol_candidates),
                        "threshold_source": meta.source,
                        "threshold_synthesized": meta.synthesized,
                    }
                    continue
                for combo in combos:
                    for exit_combo in exit_combos_by_symbol.get(meta.symbol, []):
                        for tranche3_combo in tranche3_combos_by_symbol.get(meta.symbol, []):
                            family_label = outcome_family_label(exit_combo, tranche3_combo)
                            scored.append(
                                score_combo(
                                    combo=combo,
                                    exit_combo=exit_combo,
                                    tranche3_combo=tranche3_combo,
                                    candidates=symbol_candidates,
                                    outcomes=outcomes_by_exit.get(family_label) or {},
                                    available_variants=supported_variants,
                                )
                            )
            current = next(
                (
                    item
                    for item in scored
                    if item["combo_label"] == current_entry_label
                    and item["exit_combo_label"] == current_exit_label
                    and item["tranche3_combo_label"] == current_tranche3_label
                ),
                None,
            )
            for item in scored:
                item["rejected_reason"] = reject_reason(item, current, args)
            valid_scored = [item for item in scored if not item.get("invalid_reason")]
            accepted = [item for item in valid_scored if not item.get("rejected_reason")]
            risk_pool = accepted or valid_scored
            risk_best = max(risk_pool, key=risk_sort_key) if risk_pool else None
            return_best = max(valid_scored, key=return_sort_key) if valid_scored else None
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
                "candidate_outcomes": (
                    sum(len(item.get("rows") or []) for item in scored)
                    if chronological_combo_mode
                    else sum(len(items) for items in outcomes_by_exit.values())
                ),
                "supported_variants": sorted(supported_variants),
                "observed_variants": sorted(observed_variants),
                "combo_count": len(scored),
                "current_exit_combo_label": current_exit_label,
                "current_tranche3_combo_label": current_tranche3_label,
                "valid_combo_count": len(valid_scored),
                "invalid_combo_count": len(scored) - len(valid_scored),
                "current_deployed": serializable_score(
                    current,
                    include_rows=bool(args.include_score_rows),
                )
                if current
                else None,
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
    parser.add_argument("--entry-combo-report", default="")
    parser.add_argument("--exit-combo-labels", default="")
    parser.add_argument("--tranche3-combo-labels", default="")
    parser.add_argument(
        "--current-runtime-combos-only",
        action="store_true",
        help="Score only the currently installed per-symbol runtime entry/exit/T3 combo.",
    )
    parser.add_argument(
        "--chronological-combo-simulation",
        action="store_true",
        help=(
            "Score each entry/exit/T3 combo through the checkpointed strategy state "
            "machine instead of independent per-candidate path simulation."
        ),
    )
    parser.add_argument(
        "--include-score-rows",
        action="store_true",
        help="Include selected combo rows in the JSON report for regression-gate comparison.",
    )
    parser.add_argument("--primary-short-thresholds", default="1.5,1.75,2.0")
    parser.add_argument("--fresh-breakout-multipliers", default="1,1.2,1.4,1.6")
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
