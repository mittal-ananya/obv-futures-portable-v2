from __future__ import annotations

import json
import tarfile
from io import BytesIO
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

import obvfut_portable_v2.passive_runner as passive_runner_module
from obvfut_portable_v2.passive_runner import (
    IST,
    OnlineObvState,
    PassiveV2Runner,
    clock_epochs_for_day,
    canonical_signal_id,
    synthesized_september_future_key,
    normalise_record,
    row_from_target_stream_row,
    iter_live_batch_target_items,
    build_target_stream_packs,
    target_items_from_batch_line,
    load_v1_obv_model_module,
    read_json_gz,
    run_streaming_bootstrap,
)


def _runner(tmp_path: Path) -> PassiveV2Runner:
    passive_runner_module.now_ist = lambda: datetime(2026, 8, 17, 9, 0, tzinfo=IST)
    config = json.loads((Path(__file__).resolve().parents[1] / "config" / "runtime.json").read_text())
    config["state_dir"] = str(tmp_path)
    config["state_dir_local"] = str(tmp_path)
    path = tmp_path / "runtime.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return PassiveV2Runner(path)


def _execution_second(epoch: int, price: float, signal_epoch: int) -> dict[str, object]:
    return {
        "trade_date": "2026-08-17",
        "epoch_second": epoch,
        "epoch": float(epoch),
        "received_at_ist": datetime.fromtimestamp(epoch, IST).isoformat(),
        "exchange_timestamp": datetime.fromtimestamp(epoch, IST).isoformat(),
        "received_epoch": float(epoch),
        "market_data_latency_seconds": 0.0,
        "price": price,
        "volume_traded": 100000.0 + (epoch - signal_epoch),
        "bid": price - 0.5,
        "ask": price + 0.5,
        "spread": 1.0,
        "obv": 0.0,
        "tick_rule_obv": 0.0,
        "price_change_since_start": price - 57000.0,
        "obv_change_since_start": 0.0,
        "tick_rule_obv_change_since_start": 0.0,
        "price_prior_z": 0.0,
        "obv_prior_z": 0.0,
        "obv_minus_price_prior_z": 0.0,
        "prior_percentile": 50.0,
        "prior_p05": -1.0,
        "prior_p10": -0.5,
        "prior_p90": 1.0,
        "prior_p95": 1.5,
        "second_index": 0,
        "carried": False,
    }


def _forced_long_clock(signal_epoch: int) -> dict[str, object]:
    return {
        "trade_date": "2026-08-17",
        "clock_label": "09:20",
        "clock_time": datetime.fromtimestamp(signal_epoch, IST).isoformat(),
        "has_clock_row": True,
        "actual_time": datetime.fromtimestamp(signal_epoch, IST).isoformat(),
        "epoch_second": signal_epoch,
        "price": 57000.0,
        "price_change_since_start": 0.0,
        "obv_change_since_start": 0.0,
        "obv_minus_price_prior_z": 0.0,
        "prior_percentile": 50.0,
        "prior_p05": -1.0,
        "prior_p10": -0.5,
        "prior_p90": 1.0,
        "prior_p95": 1.5,
        "price_change_prior_pct": 99.0,
        "prior_lookback_high": 56900.0,
        "prior_lookback_low": 56800.0,
        "prev_clock_range_points": 100.0,
        "prior_clock_vol_points": 100.0,
        "long_trigger_price": 56950.0,
        "short_trigger_price": 56750.0,
        "effective_fresh_breakout_points": 50.0,
        "signal_enough_history": True,
        "fresh_long_bearish_absent_pass": True,
        "fresh_long_price_strength_pass": True,
        "fresh_long_price_trigger_pass": True,
        "fresh_short_bullish_absent_pass": False,
        "fresh_short_price_weakness_pass": False,
        "fresh_short_price_trigger_pass": False,
        "fresh_trend_long_active": True,
        "fresh_trend_long_active_edge": True,
        "fresh_trend_short_active": False,
        "fresh_trend_short_active_edge": False,
        "primary_short_p90_pass": False,
        "primary_short_abs15_pass": False,
        "primary_short_abs20_pass": False,
        "primary_short_price_regime_pass": False,
        "primary_short_fresh_breakdown_pass": False,
        "primary_short_sustained_failed_reclaim_pass": False,
        "primary_short_execution_confirm_pass": False,
        "primary_obv_short_abs15_active": False,
        "primary_obv_short_abs20_active": False,
        "primary_obv_short_abs15_blocked_by_trend": False,
        "primary_obv_short_abs20_blocked_by_trend": False,
        "primary_obv_short_configured_active": False,
        "primary_obv_short_configured_active_edge": False,
        "v53_long_warning": False,
        "v53_long_executable": False,
        "v53_long_execution_confirm_pass": False,
        "v53_long_blocked_by_obv": False,
        "v53_short_high_confidence": False,
        "v53_short_early_warning": False,
        "v53_short_early_executable": False,
        "v53_short_executable": False,
    }


def _seed_forced_banknifty_long(runner: PassiveV2Runner, *, final_price: float) -> None:
    meta = runner.instruments["BANKNIFTY"]
    state = runner.states[meta.execution_key]
    signal_epoch = int(datetime(2026, 8, 17, 9, 20, tzinfo=IST).timestamp())
    due_epoch = signal_epoch + 60
    state.second_rows = [
        _execution_second(signal_epoch, 57000.0, signal_epoch),
        _execution_second(due_epoch, 57020.0, signal_epoch),
        _execution_second(due_epoch + 1, final_price, signal_epoch),
    ]
    state.clock_rows = [_forced_long_clock(signal_epoch)]
    state.last_finalized_second = due_epoch + 1
    state.latest_quote_epoch = float(due_epoch + 1)
    state.latest_received_epoch = float(due_epoch + 1)
    state.latest_price = final_price
    state.latest_bid = final_price - 0.5
    state.latest_ask = final_price + 0.5


def test_canonical_signal_id_is_stable() -> None:
    first = canonical_signal_id(
        strategy_id="OBVFUTPORT_V2_PASSIVE",
        instrument_id="RELIANCE",
        side="long",
        module="fresh_trend_long",
        signal_epoch=1786948200,
        signal_source="cash",
        signal_instrument_key="NSE:RELIANCE",
        execution_instrument_key="NFO:RELIANCE26AUGFUT",
    )
    second = canonical_signal_id(
        strategy_id="OBVFUTPORT_V2_PASSIVE",
        instrument_id="RELIANCE",
        side="long",
        module="fresh_trend_long",
        signal_epoch=1786948200,
        signal_source="cash",
        signal_instrument_key="NSE:RELIANCE",
        execution_instrument_key="NFO:RELIANCE26AUGFUT",
    )
    assert first == second
    assert first.startswith("OBVFUTPORT_V2_PASSIVE:RELIANCE:long:1786948200:")


def test_synthesized_symbols_include_available_september_shadow_keys(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    assert synthesized_september_future_key("NFO:RELIANCE26AUGFUT") == "NFO:RELIANCE26SEPFUT"
    assert len(runner.instruments) == 212
    assert len(runner.targets) == 631
    assert sum(1 for key in runner.targets if key.startswith("NFO:") and key.endswith("26AUGFUT")) == 212
    assert sum(1 for key in runner.targets if key.startswith("NFO:") and key.endswith("26SEPFUT")) == 211
    assert runner.instruments["360ONE"].shadow_execution_key == "NFO:360ONE26SEPFUT"
    assert "NFO:360ONE26SEPFUT" in runner.instruments["360ONE"].target_keys
    assert runner.instruments["DALBHARAT"].shadow_execution_key is None
    assert "NFO:DALBHARAT26SEPFUT" not in runner.targets


def test_normalise_record_uses_exchange_time_and_depth() -> None:
    row = normalise_record(
        {
            "instrument_key": "NSE:ABC",
            "received_at_epoch": 1786938301.5,
            "received_at_ist": "2026-08-17T09:15:01.500000+05:30",
            "tick": {
                "exchange_timestamp": "2026-08-17T09:15:01+05:30",
                "last_price": 100.5,
                "volume_traded": 1200,
                "depth": {"buy": [{"price": 100.45}], "sell": [{"price": 100.55}]},
            },
        },
        "2026-08-17",
        "NSE:ABC",
    )
    assert row is not None
    assert row["epoch_second"] == int(datetime(2026, 8, 17, 9, 15, 1, tzinfo=IST).timestamp())
    assert row["price"] == 100.5
    assert row["bid"] == 100.45
    assert row["ask"] == 100.55


def test_target_stream_row_requires_obv_volume() -> None:
    row = row_from_target_stream_row(
        {
            "schema": "obvfutport_v2.target_quote.v1",
            "key": "NSE:ABC",
            "exchange_epoch": datetime(2026, 8, 17, 9, 15, 1, tzinfo=IST).timestamp(),
            "received_epoch": datetime(2026, 8, 17, 9, 15, 2, tzinfo=IST).timestamp(),
            "price": 100.5,
            "volume_traded": 1200,
            "bid": 100.45,
            "ask": 100.55,
        },
        "2026-08-17",
        {"NSE:ABC"},
    )
    assert row is not None
    assert row["target"] == "NSE:ABC"
    assert row["price"] == 100.5
    assert row["volume_traded"] == 1200


def test_target_stream_row_rejects_missing_volume() -> None:
    row = row_from_target_stream_row(
        {
            "schema": "stock_ws_pullback_reclaim.target_quote.v1",
            "key": "NSE:ABC",
            "exchange_epoch": datetime(2026, 8, 17, 9, 15, 1, tzinfo=IST).timestamp(),
            "price": 100.5,
            "bid": 100.45,
            "ask": 100.55,
        },
        "2026-08-17",
        {"NSE:ABC"},
    )
    assert row is None


def test_streaming_bootstrap_can_catch_up_from_target_stream(tmp_path: Path) -> None:
    config = json.loads((Path(__file__).resolve().parents[1] / "config" / "runtime.json").read_text())
    state_dir = tmp_path / "state"
    stream_root = tmp_path / "target_stream"
    config["state_dir"] = str(state_dir)
    config["state_dir_local"] = str(state_dir)
    config["target_stream_root"] = str(stream_root)
    config["target_stream_root_local"] = str(stream_root)
    config_path = tmp_path / "runtime.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    probe_runner = PassiveV2Runner(config_path)
    key = "NSE:RELIANCE" if "NSE:RELIANCE" in probe_runner.targets else probe_runner.targets[0]
    stream_path = stream_root / "2026-08-17" / "target_quotes_2026-08-17.jsonl"
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    stream_path.write_text(
        json.dumps(
            {
                "key": key,
                "exchange_epoch": datetime(2026, 8, 17, 9, 15, 1, tzinfo=IST).timestamp(),
                "received_epoch": datetime(2026, 8, 17, 9, 15, 2, tzinfo=IST).timestamp(),
                "price": 100.5,
                "volume_traded": 1200,
                "bid": 100.45,
                "ask": 100.55,
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    status = run_streaming_bootstrap(
        config_path,
        "2026-08-18",
        "2026-08-17",
        catchup_date="2026-08-17",
        catchup_target_stream=True,
        min_target_coverage_ratio=0.0,
    )

    assert status["ok"] is True
    assert status["promoted_latest"] is True
    assert status["readiness"]["target_keys_ready"] == 1
    manifest = json.loads((state_dir / "bootstrap_state" / "latest_manifest.json").read_text(encoding="utf-8"))
    assert manifest["mode"] == "streaming_compact_bootstrap"
    payload = read_json_gz(state_dir / "bootstrap_state" / "2026-08-17" / f"{key.replace(':', '_')}.json.gz", {})
    assert payload["latest_price"] == 100.5


def test_streaming_bootstrap_can_use_target_stream_history(tmp_path: Path) -> None:
    config = json.loads((Path(__file__).resolve().parents[1] / "config" / "runtime.json").read_text())
    state_dir = tmp_path / "state"
    stream_root = tmp_path / "target_stream"
    config["state_dir"] = str(state_dir)
    config["state_dir_local"] = str(state_dir)
    config["target_stream_root"] = str(stream_root)
    config["target_stream_root_local"] = str(stream_root)
    config_path = tmp_path / "runtime.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    probe_runner = PassiveV2Runner(config_path)
    key = "NSE:RELIANCE" if "NSE:RELIANCE" in probe_runner.targets else probe_runner.targets[0]
    stream_path = stream_root / "2026-08-17" / "target_quotes_2026-08-17.jsonl"
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    stream_path.write_text(
        json.dumps(
            {
                "key": key,
                "exchange_epoch": datetime(2026, 8, 17, 9, 15, 1, tzinfo=IST).timestamp(),
                "received_epoch": datetime(2026, 8, 17, 9, 15, 2, tzinfo=IST).timestamp(),
                "price": 101.5,
                "volume_traded": 1500,
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    status = run_streaming_bootstrap(
        config_path,
        "2026-08-17",
        "2026-08-17",
        use_target_stream_history=True,
        min_target_coverage_ratio=0.0,
    )

    assert status["ok"] is True
    assert status["history_source"] == "target_stream"
    assert status["promoted_latest"] is True
    payload = read_json_gz(state_dir / "bootstrap_state" / "2026-08-17" / f"{key.replace(':', '_')}.json.gz", {})
    assert payload["latest_price"] == 101.5


def test_build_target_stream_packs_reads_live_batch_archive_member(tmp_path: Path) -> None:
    config = json.loads((Path(__file__).resolve().parents[1] / "config" / "runtime.json").read_text())
    state_dir = tmp_path / "state"
    producer_root = tmp_path / "producer"
    stream_root = tmp_path / "target_stream"
    config["state_dir"] = str(state_dir)
    config["state_dir_local"] = str(state_dir)
    config["producer_root"] = str(producer_root)
    config["target_stream_root"] = str(stream_root)
    config["target_stream_root_local"] = str(stream_root)
    config["prefer_archive_tar_for_replay"] = True
    config_path = tmp_path / "runtime.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    probe_runner = PassiveV2Runner(config_path)
    key = "NSE:RELIANCE" if "NSE:RELIANCE" in probe_runner.targets else probe_runner.targets[0]
    archive_path = producer_root / "nse_nfo" / "2026-08-17" / "market_data_live_batches_nse_nfo_2026-08-17.tar.gz"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(
            {
                "received_at_epoch": datetime(2026, 8, 17, 9, 15, 2, tzinfo=IST).timestamp(),
                "received_at_ist": "2026-08-17T09:15:02+05:30",
                "ticks": [
                    {
                        "instrument_key": key,
                        "exchange_timestamp": "2026-08-17T09:15:01+05:30",
                        "last_price": 102.5,
                        "volume_traded": 1800,
                    }
                ],
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    with tarfile.open(archive_path, "w:gz") as archive:
        member = tarfile.TarInfo("nse_nfo/2026-08-17/market_data_live_batches_nse_nfo_2026-08-17.jsonl")
        member.size = len(payload)
        archive.addfile(member, BytesIO(payload))

    status = build_target_stream_packs(config_path, "2026-08-17", "2026-08-17")

    assert status["ok"] is True
    report = status["reports"][0]
    assert report["quotes_written"] == 1
    stream_path = stream_root / "2026-08-17" / "target_quotes_2026-08-17.jsonl"
    row = json.loads(stream_path.read_text(encoding="utf-8"))
    assert row["key"] == key
    assert row["price"] == 102.5
    assert row["volume_traded"] == 1800


def test_target_items_from_batch_line_extracts_only_targets_without_full_batch_parse() -> None:
    line = json.dumps(
        {
            "received_at_epoch": 1786938302.0,
            "received_at_ist": "2026-08-17T09:15:02+05:30",
            "ticks": [
                {
                    "instrument_key": "NSE:KEEP",
                    "exchange_timestamp": "2026-08-17T09:15:01+05:30",
                    "last_price": 101.0,
                    "volume_traded": 1500,
                    "depth": {"buy": [{"price": 100.95}], "sell": [{"price": 101.05}]},
                },
                {
                    "instrument_key": "NSE:SKIP",
                    "exchange_timestamp": "2026-08-17T09:15:01+05:30",
                    "last_price": 201.0,
                    "volume_traded": 2500,
                    "depth": {"buy": [{"price": 200.95}], "sell": [{"price": 201.05}]},
                },
            ],
        },
        separators=(",", ":"),
    )

    items = target_items_from_batch_line(line, {"NSE:KEEP"})

    assert len(items) == 1
    assert items[0]["instrument_key"] == "NSE:KEEP"
    assert items[0]["received_at_epoch"] == 1786938302.0
    assert items[0]["tick"]["last_price"] == 101.0


def test_iter_live_batch_target_items_reads_plain_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "batches.jsonl"
    path.write_text(
        json.dumps(
            {
                "received_at_epoch": 1786938302.0,
                "ticks": [
                    {
                        "instrument_key": "NSE:KEEP",
                        "exchange_timestamp": "2026-08-17T09:15:01+05:30",
                        "last_price": 101.0,
                        "volume_traded": 1500,
                    }
                ],
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    items = list(iter_live_batch_target_items(path, target_hints={"NSE:KEEP"}))

    assert len(items) == 1
    assert items[0]["instrument_key"] == "NSE:KEEP"
    assert items[0]["tick"]["volume_traded"] == 1500


def test_online_obv_state_can_drop_second_rows_but_keep_clock_metrics() -> None:
    clock_epoch = int(datetime(2026, 8, 17, 9, 20, tzinfo=IST).timestamp())
    state = OnlineObvState(
        key="NSE:KEEP",
        clock_epochs={clock_epoch},
        second_row_retention_seconds=0,
    )
    start = int(datetime(2026, 8, 17, 9, 19, 58, tzinfo=IST).timestamp())
    volume = 1000.0
    for offset in range(4):
        volume += 10
        state.process_row(_execution_second(start + offset, 100.0 + offset, start))
        state.current_snapshot["trade_date"] = "2026-08-17"
        state.current_snapshot["volume_traded"] = volume
    state.finalize_until(clock_epoch)

    assert state.second_rows == []
    assert clock_epoch in state.metric_by_clock_epoch


def test_online_state_builds_model_clock_row() -> None:
    day = date(2026, 8, 17)
    clocks = set(clock_epochs_for_day(day, clock_start="09:20", clock_end="15:20", clock_step_minutes=15))
    state = OnlineObvState(key="NSE:ABC", clock_epochs=clocks)
    start = int(datetime(2026, 8, 17, 9, 15, tzinfo=IST).timestamp())
    price = 100.0
    volume = 1000.0
    for offset in range(0, 1250):
        epoch = start + offset
        if offset % 10 == 0:
            price += 0.01
        volume += 1
        state.process_row(
            {
                "trade_date": "2026-08-17",
                "target": "NSE:ABC",
                "epoch": float(epoch),
                "epoch_second": epoch,
                "received_at_ist": None,
                "exchange_timestamp": None,
                "received_epoch": float(epoch) + 0.1,
                "market_data_latency_seconds": 0.1,
                "price": price,
                "volume_traded": volume,
                "bid": price - 0.01,
                "ask": price + 0.01,
                "spread": 0.02,
            }
        )
    state.flush_until_latest()
    clock_epoch = int(datetime(2026, 8, 17, 9, 35, tzinfo=IST).timestamp())
    row, reason = state.build_clock_row(clock_epoch, "09:35", {})
    assert reason is None
    assert row is not None
    assert row["clock_label"] == "09:35"
    assert row["has_clock_row"] is True


def test_online_state_carries_last_quote_to_clock_row() -> None:
    day = date(2026, 8, 17)
    clocks = set(clock_epochs_for_day(day, clock_start="09:20", clock_end="15:20", clock_step_minutes=15))
    state = OnlineObvState(key="NSE:ABC", clock_epochs=clocks)
    start = int(datetime(2026, 8, 17, 9, 15, tzinfo=IST).timestamp())
    price = 100.0
    volume = 1000.0
    for offset in range(0, 1199):
        epoch = start + offset
        if offset % 10 == 0:
            price += 0.01
        volume += 1
        state.process_row(
            {
                "trade_date": "2026-08-17",
                "target": "NSE:ABC",
                "epoch": float(epoch),
                "epoch_second": epoch,
                "received_at_ist": None,
                "exchange_timestamp": None,
                "received_epoch": float(epoch) + 0.1,
                "market_data_latency_seconds": 0.1,
                "price": price,
                "volume_traded": volume,
                "bid": price - 0.01,
                "ask": price + 0.01,
                "spread": 0.02,
            }
        )
    clock_epoch = int(datetime(2026, 8, 17, 9, 35, tzinfo=IST).timestamp())
    row, reason = state.build_clock_row(clock_epoch, "09:35", {})
    assert reason is None
    assert row is not None
    assert row["clock_label"] == "09:35"
    assert any(item["epoch_second"] == clock_epoch and item["carried"] is True for item in state.second_rows)


def test_online_state_gap_fill_uses_last_finalized_snapshot() -> None:
    day = date(2026, 8, 17)
    clocks = set(clock_epochs_for_day(day, clock_start="09:20", clock_end="15:20", clock_step_minutes=15))
    state = OnlineObvState(key="NSE:ABC", clock_epochs=clocks)
    start = int(datetime(2026, 8, 17, 9, 19, 58, tzinfo=IST).timestamp())
    for offset, price in [(0, 100.0), (5, 100.2)]:
        epoch = start + offset
        state.process_row(
            {
                "trade_date": "2026-08-17",
                "target": "NSE:ABC",
                "epoch": float(epoch),
                "epoch_second": epoch,
                "received_at_ist": None,
                "exchange_timestamp": None,
                "received_epoch": float(epoch) + 0.1,
                "market_data_latency_seconds": 0.1,
                "price": price,
                "volume_traded": 1000 + offset,
                "bid": price - 0.01,
                "ask": price + 0.01,
                "spread": 0.02,
            }
        )
    state.flush_until_latest()
    clock_epoch = int(datetime(2026, 8, 17, 9, 20, tzinfo=IST).timestamp())
    assert clock_epoch in state.metric_by_clock_epoch


def test_online_state_payload_restore_preserves_clock_metrics() -> None:
    day = date(2026, 8, 17)
    clocks = set(clock_epochs_for_day(day, clock_start="09:20", clock_end="09:20", clock_step_minutes=15))
    state = OnlineObvState(key="NSE:ABC", clock_epochs=clocks)
    start = int(datetime(2026, 8, 17, 9, 19, 58, tzinfo=IST).timestamp())
    for offset, price in [(0, 100.0), (2, 100.2)]:
        epoch = start + offset
        state.process_row(
            {
                "trade_date": "2026-08-17",
                "target": "NSE:ABC",
                "epoch": float(epoch),
                "epoch_second": epoch,
                "received_at_ist": None,
                "exchange_timestamp": None,
                "received_epoch": float(epoch) + 0.1,
                "market_data_latency_seconds": 0.1,
                "price": price,
                "volume_traded": 1000 + offset,
                "bid": price - 0.01,
                "ask": price + 0.01,
                "spread": 0.02,
            }
        )
    state.flush_until_latest()
    restored = OnlineObvState.from_payload(state.to_payload(), clocks)
    clock_epoch = int(datetime(2026, 8, 17, 9, 20, tzinfo=IST).timestamp())
    assert restored.key == "NSE:ABC"
    assert clock_epoch in restored.metric_by_clock_epoch
    assert restored.latest_price == state.latest_price


def test_online_clock_math_matches_v1_replay_clock_math(tmp_path: Path) -> None:
    import pandas as pd

    runner = _runner(tmp_path)
    day = date(2026, 8, 17)
    clocks = set(clock_epochs_for_day(day, clock_start="09:20", clock_end="14:20", clock_step_minutes=15))
    state = OnlineObvState(key="NSE:ABC", clock_epochs=clocks)
    start = int(datetime(2026, 8, 17, 9, 15, tzinfo=IST).timestamp())
    end = int(datetime(2026, 8, 17, 14, 20, tzinfo=IST).timestamp())
    price = 100.0
    volume = 1000.0
    rows: list[dict[str, object]] = []
    for idx, epoch in enumerate(range(start, end + 1)):
        price += 0.01 if idx % 17 < 9 else -0.004
        volume += 5 + (idx % 3)
        row = {
            "trade_date": "2026-08-17",
            "target": "NSE:ABC",
            "epoch": float(epoch),
            "epoch_second": epoch,
            "received_at_ist": datetime.fromtimestamp(epoch, IST).isoformat(),
            "exchange_timestamp": datetime.fromtimestamp(epoch, IST).isoformat(),
            "received_epoch": float(epoch) + 0.1,
            "market_data_latency_seconds": 0.1,
            "price": price,
            "volume_traded": volume,
            "bid": price - 0.01,
            "ask": price + 0.01,
            "spread": 0.02,
        }
        rows.append(row)
        state.process_row(row)
    state.flush_until_latest()
    for clock_epoch in sorted(clocks):
        state.build_clock_row(clock_epoch, datetime.fromtimestamp(clock_epoch, IST).strftime("%H:%M"), {})

    v1_obv_model = load_v1_obv_model_module(runner.config)
    v1_ticks = v1_obv_model.build_appended_rows(rows, pd.DataFrame())
    v1_state = v1_obv_model.build_contract_state(v1_ticks, today="2026-08-17", point_config={})
    v1_clock = v1_state["clock_state"].sort_values("epoch_second").iloc[-1]
    v2_clock = state.clock_rows[-1]

    for column in [
        "price",
        "price_change_since_start",
        "obv_change_since_start",
        "obv_minus_price_prior_z",
        "price_change_prior_pct",
        "prior_lookback_high",
        "prior_lookback_low",
        "prior_clock_vol_points",
        "effective_fresh_breakout_points",
    ]:
        assert abs(float(v1_clock[column]) - float(v2_clock[column])) < 1e-9
    for column in [
        "signal_enough_history",
        "fresh_trend_long_active",
        "fresh_trend_short_active",
        "primary_obv_short_configured_active",
        "v53_long_warning",
        "v53_long_executable",
        "v53_short_early_warning",
        "v53_short_executable",
    ]:
        assert bool(v1_clock[column]) is bool(v2_clock[column])


def test_frozen_v1_adapter_books_paper_entry_and_initializes_tranches(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    _seed_forced_banknifty_long(runner, final_price=57025.0)

    report = runner.evaluate_frozen_trade_state("2026-08-17", symbols=["BANKNIFTY"], reason="unit_forced_entry")

    assert report["events"] == 1
    ledger_events = [
        json.loads(line)["event"]
        for line in runner.ledger_path("BANKNIFTY").read_text(encoding="utf-8").splitlines()
    ]
    assert ledger_events == ["paper_entry"]
    position = runner.model_states["BANKNIFTY"]["position"]
    assert position["side"] == "long"
    assert position["two_lot_ttsl"]["performance_variant"] == "delayed_ttsl_v21_16c_8pct_pos_or_trail_gate"
    assert position["tranche3"]["performance_variant"] == "tranche3_v1_16c_0_75R_exit_v21_16c_8pct_t2_open_required"


def test_frozen_v1_adapter_books_hard_sl_exit(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    _seed_forced_banknifty_long(runner, final_price=56500.0)

    report = runner.evaluate_frozen_trade_state("2026-08-17", symbols=["BANKNIFTY"], reason="unit_forced_exit")

    assert report["events"] == 2
    ledger_events = [
        json.loads(line)["event"]
        for line in runner.ledger_path("BANKNIFTY").read_text(encoding="utf-8").splitlines()
    ]
    assert ledger_events == ["paper_entry", "paper_exit"]
    assert runner.model_states["BANKNIFTY"]["position"] is None
    assert runner.model_states["BANKNIFTY"]["last_closed_trade"]["exit_reason"] == "hard_sl"


def test_dynamic_retention_protects_open_and_transition_symbols(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    for state in runner.states.values():
        state.second_row_retention_seconds = 300
    bank = runner.instruments["BANKNIFTY"]
    reliance = runner.instruments["RELIANCE"]
    runner.model_states["BANKNIFTY"] = {
        "position": {
            "instrument_key": bank.execution_key,
            "signal_instrument_key": bank.signal_key,
        }
    }
    runner.model_states["RELIANCE"] = {
        "last_closed_trade": {
            "exit_reason": "post_signal_hard_exhaustion",
            "exit_time": "2026-08-16T15:20:00+05:30",
        }
    }

    report = runner.refresh_dynamic_retention()

    assert report["changed"] >= 2
    assert report["unlimited_targets"] == 0
    expected_retention = runner.active_second_row_retention_seconds
    expected_transition_retention = runner.desired_retention_by_key()[reliance.signal_key]
    assert runner.states[bank.execution_key].second_row_retention_seconds == expected_retention
    assert runner.states[bank.signal_key].second_row_retention_seconds == expected_retention
    assert runner.states[reliance.signal_key].second_row_retention_seconds == expected_transition_retention


def test_v1_contract_state_exposes_v53_diagnostic_edges(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    meta = runner.instruments["BANKNIFTY"]
    state = runner.states[meta.signal_key]
    signal_epoch = int(datetime(2026, 8, 17, 9, 20, tzinfo=IST).timestamp())
    state.second_rows = [_execution_second(signal_epoch, 57000.0, signal_epoch)]
    row = _forced_long_clock(signal_epoch)
    row["v53_long_warning"] = True
    row["v53_long_warning_edge"] = True
    state.clock_rows = [row]
    state.last_finalized_second = signal_epoch

    contract_state = runner.v1_contract_state_from_online(
        state=state,
        today="2026-08-17",
        point_config=meta.signal_point_config,
    )

    diagnostics = contract_state["diagnostic_edges_today"]
    assert len(diagnostics) == 1
    assert diagnostics[0]["module"] == "v53_long_warning"
    assert diagnostics[0]["diagnostic_type"] == "long_warning"


def test_transition_signal_watcher_emits_post_exhaustion_edge(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    meta = runner.instruments["BANKNIFTY"]
    signal_state = runner.states[meta.signal_key]
    previous_clock_epoch = int(datetime(2026, 8, 16, 15, 20, tzinfo=IST).timestamp())
    transition_epoch = int(datetime(2026, 8, 17, 9, 16, tzinfo=IST).timestamp())
    signal_state.clock_rows = [
        {
            "trade_date": "2026-08-16",
            "clock_label": "15:20",
            "actual_time": datetime.fromtimestamp(previous_clock_epoch, IST).isoformat(),
            "epoch_second": previous_clock_epoch,
            "price": 57000.0,
            "prior_clock_vol_points": 10.0,
        }
    ]
    signal_state.second_rows = [_execution_second(transition_epoch, 57200.0, transition_epoch)]
    signal_state.last_finalized_second = transition_epoch
    runner.model_states["BANKNIFTY"] = {
        "position": None,
        "last_signal_epoch": 0,
        "last_exit_epoch": previous_clock_epoch,
        "last_closed_trade": {
            "exit_reason": "post_signal_hard_exhaustion",
            "exit_time": datetime.fromtimestamp(previous_clock_epoch, IST).isoformat(),
            "exit_epoch": previous_clock_epoch,
            "exit_price": 57000.0,
            "transition_reference_price": 57000.0,
            "side": "short",
        },
    }

    report = runner.evaluate_transition_signals("2026-08-17")

    assert report["watched_symbols"] == 1
    assert report["events"] == 1
    events = [
        json.loads(line)
        for line in runner.decision_events_path("2026-08-17").read_text(encoding="utf-8").splitlines()
    ]
    assert events[0]["module"] == "long_after_short_obv_exhaustion"
    assert events[0]["signal_loop_match"] == "post_exhaustion_transition_edge"


def test_lifecycle_reset_recomputes_baseline_from_lifecycle_start(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    state = OnlineObvState(key="NSE:ABC", clock_epochs=set())
    first_epoch = int(datetime(2026, 8, 10, 9, 15, tzinfo=IST).timestamp())
    reset_epoch = int(datetime(2026, 8, 17, 9, 15, tzinfo=IST).timestamp())
    for epoch, trade_date, price in [
        (first_epoch, "2026-08-10", 100.0),
        (first_epoch + 1, "2026-08-10", 101.0),
        (reset_epoch, "2026-08-17", 200.0),
        (reset_epoch + 1, "2026-08-17", 201.0),
    ]:
        state.process_row(
            {
                "trade_date": trade_date,
                "target": "NSE:ABC",
                "epoch": float(epoch),
                "epoch_second": epoch,
                "received_at_ist": datetime.fromtimestamp(epoch, IST).isoformat(),
                "exchange_timestamp": datetime.fromtimestamp(epoch, IST).isoformat(),
                "received_epoch": float(epoch),
                "market_data_latency_seconds": 0.0,
                "price": price,
                "volume_traded": 1000.0 + (epoch - first_epoch),
                "bid": price - 0.01,
                "ask": price + 0.01,
                "spread": 0.02,
            }
        )
    state.flush_until_latest()
    runner.states["NSE:ABC"] = state

    report = runner.reset_online_state_to_lifecycle("NSE:ABC", "2026-08-17", retention_seconds=None)

    assert report["status"] == "reset"
    assert runner.states["NSE:ABC"].baseline_price == 200.0
    assert runner.states["NSE:ABC"].last_price == 201.0


def test_rollover_evaluator_books_v1_style_rollover_events(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    meta = runner.instruments["BANKNIFTY"]
    assert meta.shadow_execution_key is not None
    signal_epoch = int(datetime(2026, 8, 17, 9, 20, tzinfo=IST).timestamp())
    entry_epoch = int(datetime(2026, 8, 17, 9, 21, tzinfo=IST).timestamp())
    roll_epoch = int(datetime(2026, 8, 17, 9, 22, tzinfo=IST).timestamp())
    chain = [
        {
            "label": meta.execution_contract_label,
            "instrument_key": meta.execution_key,
            "baseline_start_date": "2026-08-10",
            "expiry_date": "2026-08-18",
            "roll_date": "2026-08-17",
        },
        {
            "label": "september_shadow",
            "instrument_key": meta.shadow_execution_key,
            "expiry_date": "2026-09-29",
        },
    ]
    meta = replace(
        meta,
        contract_chain=chain,
        current_contract_index=0,
        roll_execution_time_ist="09:22",
        lifecycle_start_date="2026-08-10",
    )
    meta = runner.meta_for_contract_index(meta, 0)
    runner.instruments["BANKNIFTY"] = meta
    from_state = runner.states[meta.execution_key]
    from_state.second_rows = [
        _execution_second(entry_epoch, 57020.0, signal_epoch),
        _execution_second(roll_epoch, 57040.0, signal_epoch),
    ]
    from_state.last_finalized_second = roll_epoch
    from_state.latest_quote_epoch = float(roll_epoch)
    to_key = meta.shadow_execution_key
    assert to_key is not None
    to_state = runner.states[to_key]
    to_state.second_rows = [_execution_second(roll_epoch, 57200.0, signal_epoch)]
    to_state.last_finalized_second = roll_epoch
    to_state.latest_quote_epoch = float(roll_epoch)
    runner.model_states["BANKNIFTY"] = {
        "position": {
            "side": "long",
            "instrument_key": meta.execution_key,
            "contract_label": meta.execution_contract_label,
            "signal_source": "futures",
            "signal_instrument_key": meta.signal_key,
            "signal_contract_label": meta.signal_contract_label,
            "source": "fresh_trend_long",
            "signal_epoch": signal_epoch,
            "signal_time": datetime.fromtimestamp(signal_epoch, IST).isoformat(),
            "signal_price": 57000.0,
            "entry_epoch": entry_epoch,
            "entry_time": datetime.fromtimestamp(entry_epoch, IST).isoformat(),
            "entry_price": 57020.0,
            "entry_ltp_price": 57020.0,
            "entry_fill_price": 57020.0,
            "hard_sl_points": 1000.0,
            "trail_activation_points": 2000.0,
            "trail_activation_effective_points": 2000.0,
            "max_favorable_points": 20.0,
            "max_adverse_points": 0.0,
            "status": "open",
        },
        "last_signal_epoch": signal_epoch,
        "last_exit_epoch": 0,
    }

    report = runner.evaluate_rollovers(
        "2026-08-17",
        when=datetime(2026, 8, 17, 9, 22, 30, tzinfo=IST),
    )

    assert report["events"] == 3
    events = [
        json.loads(line)["event"]
        for line in runner.ledger_path("BANKNIFTY").read_text(encoding="utf-8").splitlines()
    ]
    assert events == ["paper_exit", "paper_entry", "paper_rollover"]
    position = runner.model_states["BANKNIFTY"]["position"]
    assert position["instrument_key"] == to_key
    assert position["source"] == "lifecycle_rollover"
    assert runner.instruments["BANKNIFTY"].current_contract_index == 1


def test_clock_only_percentile_mode_matches_full_mode_at_model_clocks() -> None:
    trade_date = date(2026, 8, 17)
    clock_epochs = set(
        clock_epochs_for_day(
            trade_date,
            clock_start="09:20",
            clock_end="15:20",
            clock_step_minutes=15,
        )
    )
    full = OnlineObvState(
        key="NSE:ABC",
        clock_epochs=clock_epochs,
        min_prior_seconds=20,
        second_row_retention_seconds=0,
        compute_non_clock_percentiles=True,
    )
    clock_only = OnlineObvState(
        key="NSE:ABC",
        clock_epochs=clock_epochs,
        min_prior_seconds=20,
        second_row_retention_seconds=0,
        compute_non_clock_percentiles=False,
    )
    start = int(datetime(2026, 8, 17, 9, 15, tzinfo=IST).timestamp())
    first_clock = int(datetime(2026, 8, 17, 9, 20, tzinfo=IST).timestamp())
    last_clock = int(datetime(2026, 8, 17, 9, 35, tzinfo=IST).timestamp())
    for idx, epoch in enumerate(range(start, last_clock + 1)):
        price = 100.0 + (idx % 11) * 0.07 + idx * 0.001
        row = {
            "trade_date": trade_date.isoformat(),
            "target": "NSE:ABC",
            "epoch": float(epoch),
            "epoch_second": epoch,
            "received_at_ist": datetime.fromtimestamp(epoch, IST).isoformat(),
            "exchange_timestamp": datetime.fromtimestamp(epoch, IST).isoformat(),
            "received_epoch": float(epoch),
            "market_data_latency_seconds": 0.0,
            "price": price,
            "volume_traded": 1000.0 + idx * 3.0,
            "bid": price - 0.01,
            "ask": price + 0.01,
            "spread": 0.02,
        }
        full.process_row(dict(row))
        clock_only.process_row(dict(row))
    full.flush_until_latest()
    clock_only.flush_until_latest()

    for clock_epoch in (first_clock, last_clock):
        left = full.metric_by_clock_epoch[clock_epoch]
        right = clock_only.metric_by_clock_epoch[clock_epoch]
        for attr in (
            "price_prior_z",
            "obv_prior_z",
            "obv_minus_price_prior_z",
            "prior_percentile",
            "prior_p05",
            "prior_p10",
            "prior_p90",
            "prior_p95",
        ):
            assert getattr(right, attr) == getattr(left, attr)
