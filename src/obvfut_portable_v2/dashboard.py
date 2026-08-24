from __future__ import annotations

import html
import json
import os
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse


IST_OFFSET_SECONDS = 5 * 3600 + 30 * 60
STATE_DIR = Path(os.environ.get("OBVFUTPORT_V2_STATE_DIR", "/opt/cloud-deploy-candidates/obv-futures-portable-v2/state"))
ROOT_DIR = Path(os.environ.get("OBVFUTPORT_V2_ROOT", "/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
START_DATE = os.environ.get("OBVFUTPORT_V2_DASHBOARD_START_DATE", "2026-08-10")
CACHE_TTL_SECONDS = float(os.environ.get("OBVFUTPORT_V2_DASHBOARD_CACHE_TTL_SECONDS", "5"))

app = FastAPI(title="OBVFUTPORT V2 Dashboard")

_SNAPSHOT_CACHE: dict[str, Any] = {"built_at": 0.0, "data": None}


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


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


def read_last_jsonl(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            end = handle.tell()
            if end <= 0:
                return {}
            size = min(end, 65536)
            handle.seek(end - size)
            chunk = handle.read(size)
        lines = [line for line in chunk.splitlines() if line.strip()]
        if not lines:
            return {}
        return json.loads(lines[-1].decode("utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}


def load_adaptive_lookup() -> dict[str, Any]:
    payload = read_json(STATE_DIR / "adaptive_calibration" / "v2_symbol_overrides_latest.json", {})
    symbols = payload.get("symbols") if isinstance(payload, dict) else {}
    return symbols if isinstance(symbols, dict) else {}


def adaptive_fields(symbol: str, source: dict[str, Any], parent: dict[str, Any]) -> dict[str, Any]:
    adaptive = source.get("adaptive_calibration") or parent.get("adaptive_calibration")
    if not isinstance(adaptive, dict):
        adaptive = {}
    metrics = adaptive.get("metrics") if isinstance(adaptive.get("metrics"), dict) else {}
    deltas = metrics.get("deltas") if isinstance(metrics.get("deltas"), dict) else {}
    tags = adaptive.get("tags") if isinstance(adaptive.get("tags"), list) else []
    return {
        "adaptive_adopted": source.get("adaptive_adopted") if source.get("adaptive_adopted") is not None else parent.get("adaptive_adopted") if parent.get("adaptive_adopted") is not None else adaptive.get("adopted"),
        "adaptive_tier": source.get("adaptive_tier") or parent.get("adaptive_tier") or adaptive.get("tier"),
        "adaptive_tags": source.get("adaptive_tags") or parent.get("adaptive_tags") or tags,
        "adaptive_candidate_kind": source.get("adaptive_candidate_kind") or parent.get("adaptive_candidate_kind") or adaptive.get("candidate_kind"),
        "adaptive_combo_label": source.get("adaptive_combo_label") or parent.get("adaptive_combo_label") or adaptive.get("combo_label"),
        "adaptive_exit_combo_label": source.get("adaptive_exit_combo_label") or parent.get("adaptive_exit_combo_label") or adaptive.get("exit_combo_label"),
        "adaptive_tranche3_combo_label": source.get("adaptive_tranche3_combo_label") or parent.get("adaptive_tranche3_combo_label") or adaptive.get("tranche3_combo_label"),
        "adaptive_net_delta_rupees": safe_float(source.get("adaptive_net_delta_rupees") or parent.get("adaptive_net_delta_rupees") or deltas.get("net_delta_rupees")),
        "adaptive_success_rate_delta_pct": safe_float(source.get("adaptive_success_rate_delta_pct") or parent.get("adaptive_success_rate_delta_pct") or deltas.get("success_rate_delta_pct")),
        "adaptive_worst_loss_pct_delta": safe_float(source.get("adaptive_worst_loss_pct_delta") or parent.get("adaptive_worst_loss_pct_delta") or deltas.get("worst_loss_pct_delta")),
        "adaptive_drawdown_delta_rupees": safe_float(source.get("adaptive_drawdown_delta_rupees") or parent.get("adaptive_drawdown_delta_rupees") or deltas.get("drawdown_delta_rupees")),
        "adaptive_source_run": adaptive.get("source_run"),
    }


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def entry_stale_reason(source: dict[str, Any], parent: dict[str, Any] | None = None) -> str:
    parent = parent or {}
    value = (
        source.get("entry_stale_reason")
        or source.get("stale_entry_reason")
        or source.get("stale_reason")
        or parent.get("entry_stale_reason")
        or parent.get("stale_entry_reason")
        or parent.get("stale_reason")
    )
    return str(value or "")


def is_stale_entry(source: dict[str, Any], parent: dict[str, Any] | None = None) -> bool:
    parent = parent or {}
    reason = entry_stale_reason(source, parent)
    return (
        truthy(source.get("entry_stale"))
        or truthy(source.get("stale_entry"))
        or truthy(parent.get("entry_stale"))
        or truthy(parent.get("stale_entry"))
        or reason in {"stale_live_entry", "stale_entry_rejected"}
        or reason.startswith("stale_")
    )


def parse_epoch(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def start_epoch_from_date(trade_date: str) -> int:
    return int(datetime.fromisoformat(f"{trade_date}T00:00:00+05:30").timestamp())


def pct(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return 100.0 * numerator / denominator


def fmt_epoch(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(ist).isoformat()


def compact_decision_report(report: Any) -> dict[str, Any]:
    if not isinstance(report, dict):
        return {}
    keys = [
        "clock_epoch",
        "clock_label",
        "clock_time_ist",
        "duration_seconds",
        "evaluated_count",
        "events",
        "event_count",
        "missed_not_ready",
        "stale_count",
        "symbols",
        "target_keys",
    ]
    return {key: report.get(key) for key in keys if key in report}


def compact_runner_status(status: Any) -> dict[str, Any]:
    if not isinstance(status, dict):
        return {}
    bootstrap = status.get("bootstrap") if isinstance(status.get("bootstrap"), dict) else {}
    manifest = bootstrap.get("manifest") if isinstance(bootstrap.get("manifest"), dict) else {}
    validation = manifest.get("validation") if isinstance(manifest.get("validation"), dict) else {}
    readiness = validation.get("readiness") if isinstance(validation.get("readiness"), dict) else {}
    return {
        "architecture_version": status.get("architecture_version"),
        "input_source": status.get("input_source"),
        "decisions_suppressed": status.get("decisions_suppressed"),
        "partial_live_start": status.get("partial_live_start"),
        "feed_latest_age_seconds": status.get("feed_latest_age_seconds"),
        "last_evaluated_clock": status.get("last_evaluated_clock"),
        "clock_watermark": status.get("clock_watermark"),
        "latest_decision_report": compact_decision_report(status.get("latest_decision_report")),
        "clock_metric_coverage": status.get("clock_metric_coverage"),
        "bootstrap": {
            "loaded": bootstrap.get("loaded"),
            "as_of_date": bootstrap.get("as_of_date"),
            "targets_loaded": bootstrap.get("targets_loaded"),
            "targets_deferred": bootstrap.get("targets_deferred"),
            "targets_missing": bootstrap.get("targets_missing"),
            "validation_ok": validation.get("ok"),
            "symbols_ready": readiness.get("symbols_ready"),
            "symbols": readiness.get("symbols"),
            "target_keys_ready": readiness.get("target_keys_ready"),
            "target_keys": readiness.get("target_keys"),
        },
    }


def load_margin_lookup() -> dict[str, dict[str, float]]:
    lookup: dict[str, dict[str, float]] = {}
    paths = [
        ROOT_DIR / "config" / "universe_v1_overlap50_aug10_flat_parity.json",
        Path("/opt/cloud-deploy-candidates/stock-ws-pullback-reclaim-v0-1/config/universe_broad212.json"),
    ]
    for path in paths:
        data = read_json(path, {})
        for entry in data.get("entries") or []:
            if not isinstance(entry, dict):
                continue
            symbol = str(entry.get("symbol") or "")
            if not symbol:
                continue
            item = lookup.setdefault(symbol, {})
            for source, target in (("margin_long", "long"), ("margin_short", "short")):
                value = safe_float(entry.get(source))
                if value is not None:
                    item[target] = value
    return lookup


def lookup_margin(margins: dict[str, dict[str, float]], symbol: str, side: Any) -> float | None:
    item = margins.get(symbol) or {}
    side_key = str(side or "").lower()
    return item.get(side_key) or item.get("long") or item.get("short")


def quantity_from_charge_breakdown(source: dict[str, Any]) -> float | None:
    charges = source.get("charge_breakdown")
    if isinstance(charges, dict):
        return safe_float(charges.get("quantity"))
    charges = source.get("charge_breakdown_if_closed")
    if isinstance(charges, dict):
        return safe_float(charges.get("quantity"))
    return safe_float(source.get("lot_size"))


def row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("symbol"),
        row.get("tranche"),
        row.get("position_id"),
        row.get("entry_epoch"),
        row.get("exit_epoch"),
        row.get("status"),
    )


def margin_for_row(
    source: dict[str, Any],
    symbol: str,
    side: Any,
    margins: dict[str, dict[str, float]],
    parent: dict[str, Any] | None = None,
) -> float | None:
    parent = parent or {}
    return (
        safe_float(source.get("current_one_lot_margin_rupees"))
        or safe_float(source.get("entry_margin_used_rupees"))
        or safe_float(source.get("margin_rupees"))
        or safe_float(parent.get("entry_margin_used_rupees"))
        or safe_float(parent.get("margin_rupees"))
        or lookup_margin(margins, symbol, side)
    )


def normalize_row(
    *,
    symbol: str,
    tranche: str,
    source: dict[str, Any],
    status: str,
    margins: dict[str, dict[str, float]],
    parent: dict[str, Any] | None = None,
) -> dict[str, Any]:
    parent = parent or {}
    side = source.get("side") or parent.get("side")
    entry_epoch = parse_epoch(source.get("entry_epoch") or parent.get("entry_epoch"))
    exit_epoch = parse_epoch(
        source.get("exit_epoch")
        or source.get("latest_epoch")
        or parent.get("latest_epoch")
        or parent.get("exit_epoch")
    )
    net = safe_float(
        source.get("net_rupees")
        if source.get("net_rupees") is not None
        else source.get("net_rupees_if_closed")
    )
    gross = safe_float(
        source.get("gross_rupees")
        if source.get("gross_rupees") is not None
        else source.get("gross_rupees_if_closed")
    )
    charges = safe_float(
        source.get("charges_rupees")
        if source.get("charges_rupees") is not None
        else source.get("charges_rupees_if_closed")
    )
    margin = margin_for_row(source, symbol, side, margins, parent)
    return {
        "symbol": symbol,
        "tranche": tranche,
        "status": status,
        "side": side,
        "signal_source": source.get("signal_source") or parent.get("signal_source"),
        "source": source.get("source") or parent.get("source"),
        "position_id": source.get("position_id") or parent.get("position_id"),
        "signal_id": source.get("signal_id") or parent.get("signal_id"),
        "contract_label": source.get("contract_label") or parent.get("contract_label"),
        "instrument_key": source.get("instrument_key") or parent.get("instrument_key"),
        "entry_time": source.get("entry_time") or parent.get("entry_time"),
        "exit_time": source.get("exit_time") or source.get("mark_time") or source.get("latest_time") or parent.get("latest_time"),
        "entry_epoch": entry_epoch,
        "exit_epoch": exit_epoch,
        "entry_price": safe_float(source.get("entry_price") or parent.get("entry_price")),
        "entry_fill_price": safe_float(source.get("entry_fill_price") or parent.get("entry_fill_price")),
        "exit_price": safe_float(source.get("exit_price") or source.get("mark_price") or source.get("latest_price") or parent.get("latest_price")),
        "exit_fill_price": safe_float(source.get("exit_fill_price") or source.get("mark_fill_price") or source.get("latest_fill_price_if_closed") or parent.get("latest_fill_price_if_closed")),
        "signal_price": safe_float(source.get("signal_price") or parent.get("signal_price")),
        "signal_time": source.get("signal_time") or parent.get("signal_time"),
        "exit_reason": source.get("exit_reason") or source.get("exit_source") or source.get("source_exit_reason") or parent.get("source_exit_reason"),
        "gross_points": safe_float(source.get("gross_points") or source.get("gross_points_if_closed")),
        "net_points": safe_float(source.get("net_points") or source.get("net_points_if_closed")),
        "gross_rupees": gross,
        "charges_rupees": charges,
        "net_rupees": net,
        "margin_rupees": margin,
        "net_pct_margin": pct(net, margin),
        "lot_size": quantity_from_charge_breakdown(source) or quantity_from_charge_breakdown(parent),
        "performance_variant": source.get("performance_variant") or parent.get("performance_variant"),
        "threshold_source": source.get("threshold_source") or parent.get("threshold_source"),
        "threshold_synthesized": bool(source.get("threshold_synthesized") or parent.get("threshold_synthesized")),
        "entry_stale": is_stale_entry(source, parent),
        "entry_stale_reason": entry_stale_reason(source, parent) or None,
        "entry_staleness_seconds": safe_float(source.get("entry_staleness_seconds") or parent.get("entry_staleness_seconds")),
        **adaptive_fields(symbol, source, parent),
    }


def load_rows() -> dict[str, Any]:
    start_epoch = start_epoch_from_date(START_DATE)
    margins = load_margin_lookup()
    adaptive_lookup = load_adaptive_lookup()
    rows_by_tranche: dict[str, list[dict[str, Any]]] = {"T1": [], "T2": [], "T3": []}
    stale_suppressed_open_positions: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    instrument_dirs = [p for p in sorted((STATE_DIR / "instruments").glob("*")) if p.is_dir()]

    def add(row: dict[str, Any]) -> None:
        entry_epoch = parse_epoch(row.get("entry_epoch"))
        if entry_epoch is None or entry_epoch < start_epoch:
            return
        symbol = str(row.get("symbol") or "")
        if symbol and not row.get("adaptive_tier"):
            adaptive = adaptive_lookup.get(symbol)
            if isinstance(adaptive, dict):
                metrics = adaptive.get("metrics") if isinstance(adaptive.get("metrics"), dict) else {}
                deltas = metrics.get("deltas") if isinstance(metrics.get("deltas"), dict) else {}
                row["adaptive_tier"] = adaptive.get("tier")
                row["adaptive_tags"] = adaptive.get("tags")
                row["adaptive_adopted"] = adaptive.get("adopted")
                row["adaptive_candidate_kind"] = adaptive.get("candidate_kind")
                row["adaptive_combo_label"] = adaptive.get("combo_label")
                row["adaptive_exit_combo_label"] = adaptive.get("exit_combo_label")
                row["adaptive_net_delta_rupees"] = safe_float(deltas.get("net_delta_rupees"))
                row["adaptive_success_rate_delta_pct"] = safe_float(deltas.get("success_rate_delta_pct"))
                row["adaptive_worst_loss_pct_delta"] = safe_float(deltas.get("worst_loss_pct_delta"))
                row["adaptive_drawdown_delta_rupees"] = safe_float(deltas.get("drawdown_delta_rupees"))
                row["adaptive_source_run"] = adaptive.get("source_run")
        key = row_key(row)
        if key in seen:
            return
        seen.add(key)
        rows_by_tranche.setdefault(str(row.get("tranche")), []).append(row)

    def record_stale_open_position(symbol: str, position: dict[str, Any]) -> None:
        ttsl = position.get("two_lot_ttsl") if isinstance(position.get("two_lot_ttsl"), dict) else {}
        tranche2 = ttsl.get("tranche2") if isinstance(ttsl.get("tranche2"), dict) else {}
        tranche3 = position.get("tranche3") if isinstance(position.get("tranche3"), dict) else {}
        has_t2 = bool(tranche2 and tranche2.get("status") != "closed")
        has_t3 = bool(tranche3 and parse_epoch(tranche3.get("entry_epoch")) is not None and tranche3.get("status") != "closed")
        tranches = ["T1"]
        if has_t2:
            tranches.append("T2")
        if has_t3:
            tranches.append("T3")
        stale_suppressed_open_positions.append(
            {
                "symbol": symbol,
                "position_id": position.get("position_id"),
                "signal_id": position.get("signal_id"),
                "side": position.get("side"),
                "entry_time": position.get("entry_time"),
                "entry_epoch": parse_epoch(position.get("entry_epoch")),
                "entry_price": safe_float(position.get("entry_price")),
                "entry_fill_price": safe_float(position.get("entry_fill_price")),
                "entry_stale": True,
                "entry_stale_reason": entry_stale_reason(position) or "stale_live_entry",
                "entry_staleness_seconds": safe_float(position.get("entry_staleness_seconds")),
                "has_t1": True,
                "has_t2": has_t2,
                "has_t3": has_t3,
                "tranches": tranches,
            }
        )

    for instrument_dir in instrument_dirs:
        symbol = instrument_dir.name
        model = read_json(instrument_dir / "model_state.json", {})
        if isinstance(model, dict):
            position = model.get("position")
            if isinstance(position, dict) and position:
                if is_stale_entry(position):
                    record_stale_open_position(symbol, position)
                else:
                    add(normalize_row(symbol=symbol, tranche="T1", source=position, status="open", margins=margins))
                    ttsl = position.get("two_lot_ttsl") if isinstance(position.get("two_lot_ttsl"), dict) else {}
                    tranche2 = ttsl.get("tranche2") if isinstance(ttsl.get("tranche2"), dict) else {}
                    if tranche2 and tranche2.get("status") != "closed":
                        add(
                            normalize_row(
                                symbol=symbol,
                                tranche="T2",
                                source=tranche2,
                                status="open",
                                margins=margins,
                                parent=position,
                            )
                        )
                    tranche3 = position.get("tranche3") if isinstance(position.get("tranche3"), dict) else {}
                    if tranche3 and parse_epoch(tranche3.get("entry_epoch")) is not None and tranche3.get("status") != "closed":
                        add(
                            normalize_row(
                                symbol=symbol,
                                tranche="T3",
                                source=tranche3,
                                status="open",
                                margins=margins,
                                parent=position,
                            )
                        )

        for event in iter_jsonl(instrument_dir / "ledger.jsonl"):
            event_type = event.get("event")
            if event_type == "paper_exit":
                add(normalize_row(symbol=symbol, tranche="T1", source=event, status="closed", margins=margins))
                ttsl = event.get("two_lot_ttsl") if isinstance(event.get("two_lot_ttsl"), dict) else {}
                tranche2 = ttsl.get("tranche2") if isinstance(ttsl.get("tranche2"), dict) else {}
                if tranche2:
                    add(
                        normalize_row(
                            symbol=symbol,
                            tranche="T2",
                            source=tranche2,
                            status="closed",
                            margins=margins,
                            parent=event,
                        )
                    )
                tranche3 = event.get("tranche3") if isinstance(event.get("tranche3"), dict) else {}
                if tranche3 and parse_epoch(tranche3.get("entry_epoch")) is not None and tranche3.get("status") == "closed":
                    add(
                        normalize_row(
                            symbol=symbol,
                            tranche="T3",
                            source=tranche3,
                            status="closed",
                            margins=margins,
                            parent=event,
                        )
                    )
            elif event_type == "tranche2_exit":
                add(normalize_row(symbol=symbol, tranche="T2", source=event, status="closed", margins=margins))
            elif event_type == "tranche3_exit":
                add(normalize_row(symbol=symbol, tranche="T3", source=event, status="closed", margins=margins))

    all_rows = rows_by_tranche["T1"] + rows_by_tranche["T2"] + rows_by_tranche["T3"]
    return {
        "instrument_dirs": len(instrument_dirs),
        "instrument_symbols": [path.name for path in instrument_dirs],
        "adaptive_lookup": adaptive_lookup,
        "stale_suppressed_open_positions": sorted(
            stale_suppressed_open_positions,
            key=lambda r: (str(r.get("symbol")), parse_epoch(r.get("entry_epoch")) or 0),
        ),
        "rows_by_tranche": {k: sorted(v, key=lambda r: (r.get("entry_epoch") or 0, r.get("exit_epoch") or 0)) for k, v in rows_by_tranche.items()},
        "rows": sorted(all_rows, key=lambda r: (r.get("entry_epoch") or 0, r.get("tranche") or "", r.get("exit_epoch") or 0)),
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [r for r in rows if r.get("status") == "closed"]
    open_rows = [r for r in rows if r.get("status") == "open"]
    wins = [r for r in closed if (safe_float(r.get("net_rupees")) or 0.0) > 0]
    net_values = [safe_float(r.get("net_rupees")) for r in rows]
    net_values = [v for v in net_values if v is not None]
    pct_values = [safe_float(r.get("net_pct_margin")) for r in rows]
    pct_values = [v for v in pct_values if v is not None]
    closed_pct_values = [safe_float(r.get("net_pct_margin")) for r in closed]
    closed_pct_values = [v for v in closed_pct_values if v is not None]
    return {
        "rows": len(rows),
        "closed": len(closed),
        "open": len(open_rows),
        "closed_wins": len(wins),
        "closed_losses": len(closed) - len(wins),
        "success_rate_pct": 100.0 * len(wins) / len(closed) if closed else None,
        "closed_net_rupees": sum(safe_float(r.get("net_rupees")) or 0.0 for r in closed),
        "open_net_rupees": sum(safe_float(r.get("net_rupees")) or 0.0 for r in open_rows),
        "total_net_rupees": sum(net_values),
        "gross_rupees": sum(safe_float(r.get("gross_rupees")) or 0.0 for r in rows),
        "charges_rupees": sum(safe_float(r.get("charges_rupees")) or 0.0 for r in rows),
        "avg_net_pct_margin": statistics.mean(pct_values) if pct_values else None,
        "median_net_pct_margin": statistics.median(pct_values) if pct_values else None,
        "avg_closed_net_pct_margin": statistics.mean(closed_pct_values) if closed_pct_values else None,
        "median_closed_net_pct_margin": statistics.median(closed_pct_values) if closed_pct_values else None,
    }


def margin_timeline(rows: list[dict[str, Any]]) -> dict[str, Any]:
    events: list[tuple[int, int, float, str]] = []
    current_margin = 0.0
    for row in rows:
        margin = safe_float(row.get("margin_rupees")) or 0.0
        entry_epoch = parse_epoch(row.get("entry_epoch"))
        exit_epoch = parse_epoch(row.get("exit_epoch"))
        if margin <= 0 or entry_epoch is None:
            continue
        events.append((entry_epoch, 1, margin, "entry"))
        if row.get("status") == "closed" and exit_epoch is not None:
            events.append((exit_epoch, 0, -margin, "exit"))
        else:
            current_margin += margin
    running = 0.0
    peak = 0.0
    peak_epoch: int | None = None
    for epoch, _, delta, _ in sorted(events, key=lambda item: (item[0], item[1])):
        running += delta
        if running > peak:
            peak = running
            peak_epoch = epoch
    return {
        "current_margin_rupees": current_margin,
        "peak_margin_rupees": peak,
        "peak_margin_epoch": peak_epoch,
        "peak_margin_time": fmt_epoch(peak_epoch),
    }


def build_snapshot() -> dict[str, Any]:
    loaded = load_rows()
    rows = loaded["rows"]
    by_tranche = loaded["rows_by_tranche"]
    summary = summarize(rows)
    tranche_summary = {tranche: summarize(items) for tranche, items in by_tranche.items()}
    stale_suppressed_open_positions = loaded.get("stale_suppressed_open_positions", [])
    stale_suppressed_by_tranche = {
        "T1": sum(1 for item in stale_suppressed_open_positions if item.get("has_t1")),
        "T2": sum(1 for item in stale_suppressed_open_positions if item.get("has_t2")),
        "T3": sum(1 for item in stale_suppressed_open_positions if item.get("has_t3")),
    }
    summary["stale_suppressed_open_count"] = len(stale_suppressed_open_positions)
    for tranche, count in stale_suppressed_by_tranche.items():
        tranche_summary.setdefault(tranche, {})["stale_suppressed_open_count"] = count
    margins = margin_timeline(rows)
    by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in loaded.get("instrument_symbols", []):
        by_symbol.setdefault(str(symbol), {"symbol": str(symbol), "rows": [], "open_rows": [], "closed_rows": []})
    for row in rows:
        symbol = str(row.get("symbol") or "")
        item = by_symbol.setdefault(symbol, {"symbol": symbol, "rows": [], "open_rows": [], "closed_rows": []})
        item["rows"].append(row)
        if row.get("status") == "open":
            item["open_rows"].append(row)
        else:
            item["closed_rows"].append(row)
    for item in by_symbol.values():
        adaptive = loaded.get("adaptive_lookup", {}).get(item["symbol"]) if isinstance(loaded.get("adaptive_lookup"), dict) else None
        item["adaptive_calibration"] = adaptive if isinstance(adaptive, dict) else {}
        item.update(adaptive_fields(item["symbol"], {"adaptive_calibration": item["adaptive_calibration"]}, {}))
        item["summary"] = summarize(item["rows"])
        item["open_rows"] = sorted(item["open_rows"], key=lambda r: (r.get("entry_epoch") or 0, r.get("tranche") or ""))
        item["closed_rows"] = sorted(item["closed_rows"], key=lambda r: (r.get("exit_epoch") or 0, r.get("entry_epoch") or 0), reverse=True)
    status = compact_runner_status(read_json(STATE_DIR / "status.json", {}))
    latest_telemetry = read_last_jsonl(STATE_DIR / "telemetry.jsonl")
    adaptive_lookup = loaded.get("adaptive_lookup") if isinstance(loaded.get("adaptive_lookup"), dict) else {}
    adaptive_counts: dict[str, int] = {"adopted": 0}
    for adaptive in adaptive_lookup.values():
        if not isinstance(adaptive, dict) or not adaptive.get("adopted"):
            continue
        adaptive_counts["adopted"] += 1
        tier = str(adaptive.get("tier") or "unknown")
        adaptive_counts[tier] = adaptive_counts.get(tier, 0) + 1
    snapshot = {
        "schema": "obvfutport_v2.dashboard_snapshot.v1",
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "state_dir": str(STATE_DIR),
        "start_date": START_DATE,
        "instrument_count": loaded["instrument_dirs"],
        "summary": summary,
        "tranche_summary": tranche_summary,
        "margin": margins,
        "rows": rows,
        "open_positions": sorted([r for r in rows if r.get("status") == "open"], key=lambda r: (str(r.get("symbol")), str(r.get("tranche")))),
        "stale_suppressed_open_positions": stale_suppressed_open_positions,
        "stale_suppressed_by_tranche": stale_suppressed_by_tranche,
        "stale_suppressed_open_count": len(stale_suppressed_open_positions),
        "transactions": sorted([r for r in rows if r.get("status") == "closed"], key=lambda r: (r.get("exit_epoch") or 0, r.get("entry_epoch") or 0), reverse=True),
        "symbols": sorted(by_symbol.values(), key=lambda item: item["symbol"]),
        "runner_status": status,
        "latest_telemetry": compact_decision_report(latest_telemetry) or latest_telemetry,
        "adaptive_calibration": {
            "counts": adaptive_counts,
            "source_path": str(STATE_DIR / "adaptive_calibration" / "v2_symbol_overrides_latest.json"),
        },
    }
    peak = safe_float(margins.get("peak_margin_rupees"))
    total_net = safe_float(summary.get("total_net_rupees"))
    snapshot["summary"]["total_net_pct_peak_margin"] = pct(total_net, peak)
    return snapshot


def get_snapshot(force: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    if not force and _SNAPSHOT_CACHE.get("data") is not None and now - float(_SNAPSHOT_CACHE.get("built_at") or 0) < CACHE_TTL_SECONDS:
        return _SNAPSHOT_CACHE["data"]
    data = build_snapshot()
    _SNAPSHOT_CACHE["built_at"] = now
    _SNAPSHOT_CACHE["data"] = data
    return data


def json_response(payload: Any) -> JSONResponse:
    return JSONResponse(payload)


@app.get("/api/obvfutport/v2/state")
def api_state() -> JSONResponse:
    return json_response(get_snapshot())


@app.get("/api/obvfutport/v2/summary")
def api_summary() -> JSONResponse:
    data = get_snapshot()
    return json_response({k: data[k] for k in ["schema", "created_at_utc", "start_date", "instrument_count", "summary", "tranche_summary", "margin", "runner_status", "latest_telemetry"]})


@app.get("/api/obvfutport/v2/instruments/{symbol}")
def api_instrument(symbol: str) -> JSONResponse:
    wanted = symbol.upper()
    for item in get_snapshot().get("symbols", []):
        if str(item.get("symbol", "")).upper() == wanted:
            return json_response(item)
    raise HTTPException(status_code=404, detail=f"symbol not found: {symbol}")


def dashboard_html() -> str:
    return r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OBVFUTPORT V2</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #07090d;
      --panel: #111722;
      --panel2: #151d2a;
      --line: rgba(255,255,255,.08);
      --text: #eef3f8;
      --muted: #8d9aaa;
      --good: #20c997;
      --bad: #ff6b6b;
      --warn: #f7c948;
      --blue: #6ea8fe;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    header { position: sticky; top: 0; z-index: 5; background: rgba(7,9,13,.92); border-bottom: 1px solid var(--line); backdrop-filter: blur(14px); }
    .wrap { max-width: 1480px; margin: 0 auto; padding: 18px 22px; }
    .top { display: flex; align-items: center; justify-content: space-between; gap: 18px; }
    h1 { margin: 0; font-size: 20px; font-weight: 720; letter-spacing: 0; }
    .sub { color: var(--muted); font-size: 12px; margin-top: 4px; }
    .pill { display: inline-flex; align-items: center; gap: 8px; border: 1px solid var(--line); background: var(--panel2); border-radius: 999px; padding: 8px 10px; color: var(--muted); font-size: 12px; }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--good); box-shadow: 0 0 12px var(--good); }
    .grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; margin-top: 16px; }
    .card { border: 1px solid var(--line); background: linear-gradient(180deg, var(--panel2), var(--panel)); border-radius: 8px; padding: 12px; min-height: 86px; }
    .label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size: 20px; font-weight: 760; margin-top: 7px; white-space: nowrap; }
    .value.small { font-size: 16px; }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin: 16px 0 10px; }
    input, select, button { background: var(--panel2); color: var(--text); border: 1px solid var(--line); border-radius: 8px; padding: 9px 10px; font: inherit; font-size: 13px; }
    button { cursor: pointer; }
    button.active { border-color: rgba(110,168,254,.65); color: var(--blue); }
    section { margin-top: 18px; }
    h2 { font-size: 14px; margin: 0 0 10px; color: #dce6f2; }
    .table-wrap { border: 1px solid var(--line); border-radius: 8px; overflow: auto; background: var(--panel); }
    table { width: 100%; border-collapse: collapse; min-width: 1480px; }
    th, td { border-bottom: 1px solid var(--line); padding: 9px 10px; font-size: 12px; text-align: right; vertical-align: top; }
    th { color: var(--muted); font-weight: 620; background: rgba(255,255,255,.03); position: sticky; top: 0; z-index: 1; }
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) { text-align: left; }
    tr:hover td { background: rgba(255,255,255,.025); }
    .mono { font-variant-numeric: tabular-nums; }
    .muted { color: var(--muted); }
    .tag { display: inline-block; border: 1px solid var(--line); border-radius: 999px; padding: 2px 7px; font-size: 11px; color: var(--muted); }
    .tabs { display: flex; gap: 8px; margin-top: 4px; }
    .empty { color: var(--muted); padding: 18px; text-align: center; }
    footer { color: var(--muted); font-size: 12px; padding: 20px 0 30px; }
    @media (max-width: 980px) {
      .wrap { padding: 14px; }
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .top { align-items: flex-start; flex-direction: column; }
      .value { font-size: 17px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="wrap top">
      <div>
        <h1>OBVFUTPORT V2</h1>
        <div class="sub">Compact-stream shadow dashboard since Aug 10 · read-only</div>
      </div>
      <div class="pill"><span class="dot"></span><span id="health">Loading</span><span id="stamp" class="muted"></span></div>
    </div>
  </header>
  <main class="wrap">
    <div class="grid" id="cards"></div>
    <div class="toolbar">
      <input id="search" placeholder="Search symbol" />
      <select id="symbol"></select>
      <button data-filter="open" class="active">Open Positions</button>
      <button data-filter="closed">Closed Transactions</button>
      <button data-filter="all">All Rows</button>
      <button id="refresh">Refresh</button>
    </div>
    <section>
      <h2 id="sectionTitle">Open Positions</h2>
      <div class="table-wrap"><table id="mainTable"></table></div>
    </section>
    <section>
      <h2>Tranche Performance</h2>
      <div class="table-wrap"><table id="trancheTable"></table></div>
    </section>
    <section>
      <h2>Suppressed Stale Entries</h2>
      <div class="table-wrap"><table id="staleTable"></table></div>
    </section>
    <footer>V2 dashboard reads only OBVFUTPORT-v2 state and does not write ledgers, orders, Compass, or v1 state.</footer>
  </main>
  <script>
    let snapshot = null;
    let filter = 'open';
    const rupee = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0 });
    const pctFmt = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2, minimumFractionDigits: 2 });
    function n(v){ return Number.isFinite(Number(v)) ? Number(v) : null; }
    function money(v){ const x=n(v); if(x===null) return '--'; return (x<0?'-':'') + '₹' + rupee.format(Math.abs(x)); }
    function pct(v){ const x=n(v); if(x===null) return '--'; return pctFmt.format(x) + '%'; }
    function cls(v){ const x=n(v); return x===null ? '' : (x>=0 ? 'good' : 'bad'); }
    function t(s){ if(!s) return '--'; return String(s).replace('T',' ').replace('+05:30',' IST'); }
    function price(v){ const x=n(v); if(x===null) return '--'; return x.toFixed(Math.abs(x) >= 100 ? 2 : 4); }
    function shortTier(v){
      if(!v) return '--';
      return String(v).replace('tier1_formal_promotion_candidate','T1 formal')
        .replace('tier2_provisional_all_metrics_improved','T2 provisional')
        .replace('tier3_watchlist_improved_non_worsening_risk','T3 watch');
    }
    function tags(v){
      if(!Array.isArray(v) || !v.length) return '--';
      return v.slice(0,2).map(x => `<span class="tag">${x}</span>`).join(' ');
    }
    function shortCombo(v){
      if(!v) return '--';
      return String(v).replaceAll('|',' · ').replaceAll('primary_abs=','abs ').replaceAll('fresh_m=','m ').replaceAll('long_pct=','L ').replaceAll('short_pct=','S ');
    }
    function card(label, value, klass=''){
      return `<div class="card"><div class="label">${label}</div><div class="value ${klass}">${value}</div></div>`;
    }
    function renderCards(){
      const s=snapshot.summary, m=snapshot.margin;
      const adaptiveSymbols = ((snapshot.adaptive_calibration || {}).counts || {}).adopted || 0;
      document.getElementById('cards').innerHTML = [
        card('Total Net / Peak Margin', pct(s.total_net_pct_peak_margin), cls(s.total_net_pct_peak_margin)),
        card('Total Net', money(s.total_net_rupees), cls(s.total_net_rupees)),
        card('Closed Net', money(s.closed_net_rupees), cls(s.closed_net_rupees)),
        card('Open MTM Net', money(s.open_net_rupees), cls(s.open_net_rupees)),
        card('Current Margin', money(m.current_margin_rupees)),
        card('Peak Margin', money(m.peak_margin_rupees)),
        card('Closed Success', pct(s.success_rate_pct)),
        card('Open Legs', String(s.open)),
        card('Suppressed Stale Opens', String(snapshot.stale_suppressed_open_count || 0), 'small'),
        card('Closed Legs', String(s.closed)),
        card('Symbols', String(snapshot.instrument_count)),
        card('Adaptive Symbols', String(adaptiveSymbols)),
        card('Charges', money(s.charges_rupees), 'small'),
        card('Updated', new Date(snapshot.created_at_utc).toLocaleTimeString('en-IN', {hour12:false}), 'small')
      ].join('');
    }
    function renderTranches(){
      const rows = Object.entries(snapshot.tranche_summary || {}).map(([k,s]) => `
        <tr><td>${k}</td><td>${s.closed}</td><td>${s.open}</td><td>${pct(s.success_rate_pct)}</td>
        <td class="${cls(s.closed_net_rupees)}">${money(s.closed_net_rupees)}</td>
        <td class="${cls(s.open_net_rupees)}">${money(s.open_net_rupees)}</td>
        <td class="${cls(s.total_net_rupees)}">${money(s.total_net_rupees)}</td>
        <td>${pct(s.avg_net_pct_margin)}</td><td>${pct(s.median_net_pct_margin)}</td></tr>`).join('');
      document.getElementById('trancheTable').innerHTML =
        `<thead><tr><th>Tranche</th><th>Closed</th><th>Open</th><th>Success</th><th>Closed Net</th><th>Open MTM</th><th>Total Net</th><th>Avg % Margin</th><th>Median % Margin</th></tr></thead><tbody>${rows}</tbody>`;
    }
    function renderStaleSuppressed(){
      const rows = snapshot.stale_suppressed_open_positions || [];
      if(!rows.length){
        document.getElementById('staleTable').innerHTML = '<tbody><tr><td class="empty">No suppressed stale entries</td></tr></tbody>';
        return;
      }
      const body = rows.map(r => `<tr>
        <td><strong>${r.symbol}</strong></td>
        <td>${String(r.side || '').toUpperCase()}</td>
        <td>${Array.isArray(r.tranches) ? r.tranches.map(x => `<span class="tag">${x}</span>`).join(' ') : '--'}</td>
        <td class="mono">${t(r.entry_time)}</td>
        <td>${price(r.entry_fill_price || r.entry_price)}</td>
        <td>${r.entry_staleness_seconds == null ? '--' : pctFmt.format(Number(r.entry_staleness_seconds)) + 's'}</td>
        <td>${r.entry_stale_reason || '--'}</td>
        <td class="mono">${r.position_id || '--'}</td>
      </tr>`).join('');
      document.getElementById('staleTable').innerHTML =
        `<thead><tr><th>Symbol</th><th>Side</th><th>Suppressed Legs</th><th>Entry Time</th><th>Entry Fill</th><th>Stale By</th><th>Reason</th><th>Position Id</th></tr></thead><tbody>${body}</tbody>`;
    }
    function populateSymbols(){
      const select=document.getElementById('symbol');
      const current=select.value;
      select.innerHTML = '<option value="">All symbols</option>' + (snapshot.symbols || []).map(s => `<option value="${s.symbol}">${s.symbol}</option>`).join('');
      select.value = current;
    }
    function filteredRows(){
      const q=document.getElementById('search').value.trim().toUpperCase();
      const sym=document.getElementById('symbol').value;
      let rows = filter === 'open' ? snapshot.open_positions : filter === 'closed' ? snapshot.transactions : snapshot.rows || [];
      return rows.filter(r => (!q || String(r.symbol).includes(q)) && (!sym || r.symbol === sym));
    }
    function renderRows(){
      const rows=filteredRows();
      document.getElementById('sectionTitle').textContent = filter === 'open' ? 'Open Positions' : filter === 'closed' ? 'Closed Transactions' : 'All Rows';
      if(!rows.length){ document.getElementById('mainTable').innerHTML = '<tbody><tr><td class="empty">No rows for this filter</td></tr></tbody>'; return; }
      const body = rows.map(r => `<tr>
        <td><strong>${r.symbol}</strong><div class="muted">${r.instrument_key || ''}</div></td>
        <td><span class="tag">${r.tranche}</span> <span class="tag">${r.status}</span></td>
        <td>${shortTier(r.adaptive_tier)}</td>
        <td>${tags(r.adaptive_tags)}</td>
        <td>${shortCombo(r.adaptive_combo_label)}<div class="muted">${r.adaptive_exit_combo_label || '--'}</div><div class="muted">${r.adaptive_tranche3_combo_label || '--'}</div></td>
        <td class="${cls(r.adaptive_net_delta_rupees)}">${money(r.adaptive_net_delta_rupees)}</td>
        <td class="${cls(r.adaptive_success_rate_delta_pct)}">${pct(r.adaptive_success_rate_delta_pct)}</td>
        <td class="${cls(r.adaptive_worst_loss_pct_delta)}">${pct(r.adaptive_worst_loss_pct_delta)}</td>
        <td class="${cls(r.adaptive_drawdown_delta_rupees)}">${money(r.adaptive_drawdown_delta_rupees)}</td>
        <td>${String(r.side || '').toUpperCase()}</td>
        <td>${r.signal_source || '--'}</td>
        <td class="mono">${t(r.entry_time)}</td>
        <td class="mono">${t(r.exit_time)}</td>
        <td>${price(r.entry_fill_price || r.entry_price)}</td>
        <td>${price(r.exit_fill_price || r.exit_price)}</td>
        <td class="${cls(r.gross_rupees)}">${money(r.gross_rupees)}</td>
        <td>${money(r.charges_rupees)}</td>
        <td class="${cls(r.net_rupees)}">${money(r.net_rupees)}</td>
        <td>${money(r.margin_rupees)}</td>
        <td class="${cls(r.net_pct_margin)}">${pct(r.net_pct_margin)}</td>
        <td>${r.exit_reason || r.source || '--'}</td>
      </tr>`).join('');
      document.getElementById('mainTable').innerHTML =
        `<thead><tr><th>Symbol</th><th>Leg</th><th>Tier</th><th>Tags</th><th>Candidate</th><th>ΔNet</th><th>ΔSuccess</th><th>ΔWorst</th><th>ΔDrawdown</th><th>Side</th><th>Signal</th><th>Entry</th><th>Exit / Mark</th><th>Entry Fill</th><th>Exit / Mark Fill</th><th>Gross</th><th>Charges</th><th>Net</th><th>Margin</th><th>Net %</th><th>Reason</th></tr></thead><tbody>${body}</tbody>`;
    }
    async function load(){
      const res = await fetch('/api/obvfutport/v2/state', { cache: 'no-store' });
      snapshot = await res.json();
      document.getElementById('health').textContent = 'Dashboard Live';
      document.getElementById('stamp').textContent = new Date(snapshot.created_at_utc).toLocaleString('en-IN', {hour12:false});
      renderCards(); populateSymbols(); renderRows(); renderTranches(); renderStaleSuppressed();
    }
    document.querySelectorAll('button[data-filter]').forEach(btn => btn.addEventListener('click', () => {
      filter = btn.dataset.filter;
      document.querySelectorAll('button[data-filter]').forEach(b => b.classList.toggle('active', b === btn));
      renderRows();
    }));
    document.getElementById('search').addEventListener('input', renderRows);
    document.getElementById('symbol').addEventListener('change', renderRows);
    document.getElementById('refresh').addEventListener('click', load);
    load().catch(err => { document.getElementById('health').textContent = 'Load failed'; console.error(err); });
    setInterval(load, 30000);
  </script>
</body>
</html>"""


@app.get("/dashboard/OBVFUTPORT/v2", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    return HTMLResponse(dashboard_html())


@app.head("/dashboard/OBVFUTPORT/v2")
def dashboard_head() -> HTMLResponse:
    return HTMLResponse("")
