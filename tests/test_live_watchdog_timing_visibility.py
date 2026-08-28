from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "live_watchdog.py"
IST = timezone(timedelta(hours=5, minutes=30))


def load_watchdog():
    spec = importlib.util.spec_from_file_location("live_watchdog_under_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def configure_watchdog(monkeypatch, tmp_path: Path, *, now: datetime):
    watchdog = load_watchdog()
    monkeypatch.setattr(watchdog, "V2_STATUS_PATH", tmp_path / "status.json")
    monkeypatch.setattr(
        watchdog,
        "STREAM_STATUS_PATH",
        tmp_path / "target_stream_extractor" / "target_stream_status.json",
    )
    monkeypatch.setattr(watchdog, "WATCHED_SERVICES", ["cloud-obvfutport-v2-passive.service"])
    monkeypatch.setattr(
        watchdog,
        "systemctl_show",
        lambda service: {
            "ActiveState": "active",
            "SubState": "running",
            "NRestarts": 0,
            "MemoryCurrent": 128 * 1024 * 1024,
            "MemoryMax": 10 * 1024 * 1024 * 1024,
            "Result": "success",
        },
    )
    monkeypatch.setattr(watchdog, "recent_oom_lines", lambda: [])
    monkeypatch.setattr(watchdog, "now_ist", lambda: now)
    monkeypatch.setattr(watchdog.os, "getloadavg", lambda: (0.5, 0.5, 0.5))
    return watchdog


def test_watchdog_alerts_when_decisions_are_deferred_for_stream_catchup(tmp_path: Path, monkeypatch) -> None:
    watchdog = configure_watchdog(
        monkeypatch,
        tmp_path,
        now=datetime(2026, 8, 27, 15, 32, 5, tzinfo=IST),
    )
    write_json(
        watchdog.V2_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "feed_latest_age_seconds": 121.2,
            "clock_watermark": "2026-08-27T15:20:00+05:30",
            "latest_decision_report": {
                "event": "decisions_deferred_stream_catchup",
                "reason": "feed_not_caught_up",
                "feed_latest_age_seconds": 121.2,
                "max_age_seconds": 120,
                "latest_tail_report": {
                    "path": "/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/target_stream.jsonl",
                    "file_size": 2087591218,
                    "offset": 2087591218,
                    "new_offset": 2087591218,
                    "bytes_read": 0,
                    "rows": 0,
                    "quotes": 0,
                    "truncated": False,
                },
            },
        },
    )
    write_json(
        watchdog.STREAM_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "latest_quote_age_seconds": 2.0,
            "quotes_written": 100,
        },
    )

    status = watchdog.evaluate({})

    criticals = {row["code"]: row for row in status["criticals"]}
    assert "decision_catchup_deferred" in criticals
    report = criticals["decision_catchup_deferred"]["latest_decision_report"]
    assert report["reason"] == "feed_not_caught_up"
    assert report["latest_tail_report"]["bytes_read"] == 0
    assert status["metrics"]["v2_latest_decision_report"]["event"] == "decisions_deferred_stream_catchup"


def test_watchdog_ignores_stale_catchup_report_after_feed_recovers(tmp_path: Path, monkeypatch) -> None:
    watchdog = configure_watchdog(
        monkeypatch,
        tmp_path,
        now=datetime(2026, 8, 28, 9, 17, 0, tzinfo=IST),
    )
    write_json(
        watchdog.V2_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "target_keys_ready": 418,
            "feed_latest_age_seconds": 1.0,
            "latest_tail_report": {
                "exists": True,
                "rows": 189,
                "quotes": 189,
            },
            "latest_decision_report": {
                "event": "decisions_deferred_stream_catchup",
                "reason": "feed_not_caught_up",
                "feed_latest_age_seconds": 63897.4,
                "max_age_seconds": 120,
                "latest_tail_report": {
                    "path": "/opt/cloud-deploy-candidates/obv-futures-portable-v2/state/target_stream.jsonl",
                    "rows": 0,
                    "quotes": 0,
                },
            },
        },
    )
    write_json(
        watchdog.STREAM_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "latest_quote_age_seconds": 1.0,
            "quotes_written": 30000,
        },
    )

    status = watchdog.evaluate({})

    criticals = {row["code"]: row for row in status["criticals"]}
    assert "decision_catchup_deferred" not in criticals
    assert status["ok"] is True


def test_watchdog_alerts_on_missed_not_ready_clock_report(tmp_path: Path, monkeypatch) -> None:
    watchdog = configure_watchdog(
        monkeypatch,
        tmp_path,
        now=datetime(2026, 8, 27, 10, 6, 0, tzinfo=IST),
    )
    write_json(
        watchdog.V2_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "feed_latest_age_seconds": 1.0,
            "clock_watermark": "2026-08-27T10:05:00+05:30",
            "latest_decision_report": {
                "event": "clock_evaluation",
                "clock_time": "2026-08-27T10:05:00+05:30",
                "missed_not_ready_count": 2,
                "missed_not_ready_symbols": ["ABC", "XYZ"],
            },
        },
    )
    write_json(
        watchdog.STREAM_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "latest_quote_age_seconds": 1.0,
            "quotes_written": 100,
        },
    )

    status = watchdog.evaluate({})

    criticals = {row["code"]: row for row in status["criticals"]}
    assert "missed_not_ready" in criticals
    assert criticals["missed_not_ready"]["symbols"] == ["ABC", "XYZ"]
    assert status["metrics"]["v2_latest_decision_report"]["missed_not_ready_count"] == 2


def test_watchdog_alerts_on_passive_memory_growth_slope(tmp_path: Path, monkeypatch) -> None:
    watchdog = configure_watchdog(
        monkeypatch,
        tmp_path,
        now=datetime(2026, 8, 27, 11, 0, 0, tzinfo=IST),
    )
    write_json(
        watchdog.V2_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "feed_latest_age_seconds": 1.0,
            "clock_watermark": "2026-08-27T10:50:00+05:30",
            "latest_decision_report": {"event": "clock_evaluation", "missed_not_ready_count": 0},
        },
    )
    write_json(
        watchdog.STREAM_STATUS_PATH,
        {
            "ok": True,
            "target_keys": 418,
            "latest_quote_age_seconds": 1.0,
            "quotes_written": 100,
        },
    )
    memory_current = {"bytes": 1024 * 1024 * 1024}
    now_epoch = {"value": 1_787_827_800.0}

    monkeypatch.setattr(
        watchdog,
        "systemctl_show",
        lambda service: {
            "ActiveState": "active",
            "SubState": "running",
            "NRestarts": 0,
            "MemoryCurrent": memory_current["bytes"],
            "MemoryMax": 12 * 1024 * 1024 * 1024,
            "Result": "success",
        },
    )
    monkeypatch.setattr(watchdog.time, "time", lambda: now_epoch["value"])

    loop_state: dict[str, object] = {}
    first = watchdog.evaluate(loop_state)
    assert "service_memory_growth_critical" not in {row["code"] for row in first["criticals"]}

    memory_current["bytes"] = 2 * 1024 * 1024 * 1024
    now_epoch["value"] += 300
    second = watchdog.evaluate(loop_state)

    criticals = {row["code"]: row for row in second["criticals"]}
    assert "service_memory_growth_critical" in criticals
    assert criticals["service_memory_growth_critical"]["slope_mb_per_min"] > 128
