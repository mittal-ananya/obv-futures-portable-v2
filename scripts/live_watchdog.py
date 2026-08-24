#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any

IST = timezone(timedelta(hours=5, minutes=30))

ROOT = Path(os.environ.get("OBVFUTPORT_V2_ROOT", "/opt/cloud-deploy-candidates/obv-futures-portable-v2"))
STATE_DIR = ROOT / "state"
STATUS_PATH = STATE_DIR / "live_watchdog_status.json"
ALERTS_PATH = STATE_DIR / "live_watchdog_alerts.jsonl"
V2_STATUS_PATH = STATE_DIR / "status.json"
STREAM_STATUS_PATH = STATE_DIR / "target_stream_extractor" / "target_stream_status.json"

PASSIVE_SERVICE = "cloud-obvfutport-v2-passive.service"
STREAM_SERVICE = "cloud-obvfutport-v2-target-stream.service"
DASHBOARD_SERVICE = "cloud-obvfutport-v2-dashboard.service"
MATRIX_SERVICE = "cloud-matrix-v1.service"
MATRIX_BRIDGE_SERVICE = "cloud-matrix-v1-bridge.service"
WATCHED_SERVICES = [PASSIVE_SERVICE, STREAM_SERVICE, DASHBOARD_SERVICE, MATRIX_SERVICE, MATRIX_BRIDGE_SERVICE]

INTERVAL_SECONDS = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_INTERVAL_SECONDS", "30"))
QUOTE_AGE_WARN_SECONDS = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_QUOTE_AGE_WARN_SECONDS", "5"))
QUOTE_AGE_CRIT_SECONDS = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_QUOTE_AGE_CRIT_SECONDS", "15"))
FEED_AGE_WARN_SECONDS = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_FEED_AGE_WARN_SECONDS", "10"))
FEED_AGE_CRIT_SECONDS = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_FEED_AGE_CRIT_SECONDS", "30"))
LOAD1_WARN = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_LOAD1_WARN", "5"))
LOAD1_CRIT = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_LOAD1_CRIT", "7.5"))
SERVICE_MEMORY_WARN_FRACTION = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_SERVICE_MEMORY_WARN_FRACTION", "0.85"))
SERVICE_MEMORY_CRIT_FRACTION = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_SERVICE_MEMORY_CRIT_FRACTION", "0.95"))
CLOCK_EVAL_GRACE_SECONDS = int(os.environ.get("OBVFUTPORT_V2_WATCHDOG_CLOCK_GRACE_SECONDS", "90"))
ALERT_REPEAT_SECONDS = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_ALERT_REPEAT_SECONDS", "300"))
OOM_CHECK_SECONDS = float(os.environ.get("OBVFUTPORT_V2_WATCHDOG_OOM_CHECK_SECONDS", "180"))


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(IST)
    except Exception:
        return None


def as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def systemctl_show(service: str) -> dict[str, Any]:
    props = [
        "ActiveState",
        "SubState",
        "NRestarts",
        "MemoryCurrent",
        "MemoryMax",
        "CPUUsageNSec",
        "Result",
    ]
    cmd = ["systemctl", "show", service, "--property", ",".join(props)]
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=3)
    except Exception as exc:
        return {"error": str(exc)}
    out: dict[str, Any] = {}
    for line in proc.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key] = value
    if proc.returncode != 0:
        out["error"] = proc.stderr.strip() or f"systemctl_return_{proc.returncode}"
    for key in ("NRestarts", "MemoryCurrent", "MemoryMax", "CPUUsageNSec"):
        if key in out:
            try:
                out[key] = int(out[key])
            except Exception:
                pass
    return out


def recent_oom_lines() -> list[str]:
    lines: list[str] = []
    for service in (PASSIVE_SERVICE, STREAM_SERVICE):
        cmd = [
            "journalctl",
            "-u",
            service,
            "--since",
            "5 minutes ago",
            "--no-pager",
            "-n",
            "80",
        ]
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=4)
        except Exception:
            continue
        for line in proc.stdout.splitlines():
            lower = line.lower()
            if "oom" in lower or "out of memory" in lower or "memory cgroup" in lower or "killed process" in lower:
                lines.append(line[-500:])
    return lines[-10:]


def latest_expected_clock(current: datetime) -> datetime | None:
    market_start = dtime(9, 20)
    market_end = dtime(15, 20)
    if current.time() < market_start or current.time() > dtime(15, 35):
        return None
    base = current.replace(hour=9, minute=20, second=0, microsecond=0)
    if current < base:
        return None
    elapsed = int((current - base).total_seconds())
    clock_index = elapsed // 900
    expected = base + timedelta(seconds=clock_index * 900)
    if expected.time() > market_end:
        expected = current.replace(hour=15, minute=20, second=0, microsecond=0)
    if current < expected + timedelta(seconds=CLOCK_EVAL_GRACE_SECONDS):
        previous = expected - timedelta(seconds=900)
        if previous < base:
            return None
        return previous
    return expected


def market_data_expected(current: datetime) -> bool:
    return dtime(9, 15) <= current.time() <= dtime(15, 35)


def evaluate(loop_state: dict[str, Any]) -> dict[str, Any]:
    current = now_ist()
    v2 = load_json(V2_STATUS_PATH)
    stream = load_json(STREAM_STATUS_PATH)
    services = {name: systemctl_show(name) for name in WATCHED_SERVICES}
    load1 = os.getloadavg()[0] if hasattr(os, "getloadavg") else None

    warnings: list[dict[str, Any]] = []
    criticals: list[dict[str, Any]] = []

    def add(severity: str, code: str, message: str, **fields: Any) -> None:
        row = {"severity": severity, "code": code, "message": message, **fields}
        if severity == "critical":
            criticals.append(row)
        else:
            warnings.append(row)

    for service, info in services.items():
        if info.get("ActiveState") != "active":
            add("critical", "service_inactive", f"{service} is not active", service=service, state=info)
        prev_restarts = loop_state.setdefault("restart_counts", {}).get(service)
        restarts = info.get("NRestarts")
        if isinstance(restarts, int):
            if prev_restarts is not None and restarts > prev_restarts:
                add("critical", "service_restart", f"{service} restart count increased", service=service, previous=prev_restarts, current=restarts)
            loop_state["restart_counts"][service] = restarts

    if v2.get("ok") is not True:
        add("critical", "v2_not_ok", "v2 status is not ok", status=v2.get("status"))
    if v2.get("partial_live_start") is True:
        add("critical", "partial_live_start", "v2 partial_live_start is true")
    if v2.get("decisions_suppressed") is True:
        add("critical", "decisions_suppressed", "v2 decisions are suppressed")
    v2_target_keys = int(v2.get("target_keys") or 0)
    stream_target_keys = int(stream.get("target_keys") or 0)
    if v2_target_keys <= 0:
        add("critical", "v2_target_key_count", "v2 target key count is missing", target_keys=v2.get("target_keys"))

    if stream.get("ok") is not True:
        add("critical", "stream_not_ok", "v2 owned stream status is not ok", status=stream.get("status"))
    if stream_target_keys <= 0:
        add("critical", "stream_target_key_count", "v2 owned stream key count is missing", target_keys=stream.get("target_keys"))
    elif v2_target_keys > 0 and stream_target_keys != v2_target_keys:
        add(
            "critical",
            "stream_target_key_count_mismatch",
            "v2 owned stream key count does not match v2 runner target count",
            stream_target_keys=stream_target_keys,
            v2_target_keys=v2_target_keys,
        )

    quote_age = as_float(stream.get("latest_quote_age_seconds"))
    feed_age = as_float(v2.get("feed_latest_age_seconds"))
    if market_data_expected(current):
        if quote_age is None:
            add("warning", "quote_age_missing", "v2 owned stream quote age is missing")
        elif quote_age > QUOTE_AGE_CRIT_SECONDS:
            add("critical", "quote_age_critical", "v2 owned stream quote age is too high", quote_age_seconds=quote_age)
        elif quote_age > QUOTE_AGE_WARN_SECONDS:
            add("warning", "quote_age_warning", "v2 owned stream quote age is elevated", quote_age_seconds=quote_age)

        if feed_age is None:
            add("warning", "feed_age_missing", "v2 feed age is missing")
        elif feed_age > FEED_AGE_CRIT_SECONDS:
            add("critical", "feed_age_critical", "v2 feed age is too high", feed_age_seconds=feed_age)
        elif feed_age > FEED_AGE_WARN_SECONDS:
            add("warning", "feed_age_warning", "v2 feed age is elevated", feed_age_seconds=feed_age)

    if load1 is not None:
        if load1 > LOAD1_CRIT:
            add("critical", "load1_critical", "host load1 is too high", load1=load1)
        elif load1 > LOAD1_WARN:
            add("warning", "load1_warning", "host load1 is elevated", load1=load1)

    for service, info in services.items():
        mem_current = info.get("MemoryCurrent")
        mem_max = info.get("MemoryMax")
        if isinstance(mem_current, int) and isinstance(mem_max, int) and mem_max > 0:
            fraction = mem_current / mem_max
            if fraction >= SERVICE_MEMORY_CRIT_FRACTION:
                add(
                    "critical",
                    "service_memory_near_cap",
                    f"{service} memory is near cap",
                    service=service,
                    memory_current=mem_current,
                    memory_max=mem_max,
                    fraction=fraction,
                )
            elif fraction >= SERVICE_MEMORY_WARN_FRACTION:
                add(
                    "warning",
                    "service_memory_elevated",
                    f"{service} memory is elevated",
                    service=service,
                    memory_current=mem_current,
                    memory_max=mem_max,
                    fraction=fraction,
                )

    expected = latest_expected_clock(current)
    last_clock = parse_dt(v2.get("last_evaluated_clock") or v2.get("clock_watermark"))
    if expected is not None and (last_clock is None or last_clock < expected):
        add(
            "critical",
            "clock_eval_stale",
            "v2 latest evaluated clock is behind expected clock",
            expected_clock=expected.isoformat(),
            last_evaluated_clock=last_clock.isoformat() if last_clock else None,
        )

    last_oom_check = float(loop_state.get("last_oom_check_epoch") or 0)
    if time.time() - last_oom_check >= OOM_CHECK_SECONDS:
        loop_state["last_oom_check_epoch"] = time.time()
        oom_lines = recent_oom_lines()
        if oom_lines:
            add("critical", "recent_oom_log", "recent OOM-related journal lines found", samples=oom_lines)

    status = {
        "ok": not criticals,
        "updated_at_ist": current.isoformat(),
        "warnings": warnings,
        "criticals": criticals,
        "metrics": {
            "load1": load1,
            "v2_ok": v2.get("ok"),
            "v2_target_keys": v2.get("target_keys"),
            "v2_feed_latest_age_seconds": feed_age,
            "v2_last_evaluated_clock": v2.get("last_evaluated_clock"),
            "v2_clock_watermark": v2.get("clock_watermark"),
            "v2_partial_live_start": v2.get("partial_live_start"),
            "v2_decisions_suppressed": v2.get("decisions_suppressed"),
            "stream_ok": stream.get("ok"),
            "stream_target_keys": stream.get("target_keys"),
            "stream_latest_quote_age_seconds": quote_age,
            "stream_quotes_written": stream.get("quotes_written"),
            "services": services,
        },
    }
    return status


def alert_key(row: dict[str, Any]) -> str:
    return f"{row.get('severity')}:{row.get('code')}"


def maybe_alert(status: dict[str, Any], loop_state: dict[str, Any]) -> None:
    current_epoch = time.time()
    last_alerts = loop_state.setdefault("last_alerts", {})
    for row in [*status.get("criticals", []), *status.get("warnings", [])]:
        key = alert_key(row)
        last = float(last_alerts.get(key) or 0)
        if current_epoch - last < ALERT_REPEAT_SECONDS:
            continue
        last_alerts[key] = current_epoch
        payload = {
            "alerted_at_ist": status["updated_at_ist"],
            **row,
        }
        append_jsonl(ALERTS_PATH, payload)
        print("WATCHDOG_ALERT " + json.dumps(payload, sort_keys=True), flush=True)


def main() -> int:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    loop_state: dict[str, Any] = {}
    once = "--once" in sys.argv
    while True:
        try:
            status = evaluate(loop_state)
            atomic_write_json(STATUS_PATH, status)
            maybe_alert(status, loop_state)
        except Exception as exc:
            payload = {
                "ok": False,
                "updated_at_ist": now_ist().isoformat(),
                "criticals": [{"severity": "critical", "code": "watchdog_exception", "message": str(exc)}],
                "warnings": [],
            }
            atomic_write_json(STATUS_PATH, payload)
            append_jsonl(ALERTS_PATH, {"alerted_at_ist": payload["updated_at_ist"], **payload["criticals"][0]})
            print("WATCHDOG_EXCEPTION " + repr(exc), flush=True)
        if once:
            break
        time.sleep(max(5.0, INTERVAL_SECONDS))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
