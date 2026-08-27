from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse


IST = ZoneInfo("Asia/Kolkata")
V2MATRIX_ROOT = Path(os.environ.get("V2MATRIX_ROOT", "/opt/cloud-deploy-candidates/v2matrix"))
STATE_PATH = V2MATRIX_ROOT / "state" / "portfolio_state.json"
STATUS_PATH = V2MATRIX_ROOT / "state" / "overlay_status.json"

app = FastAPI(title="v2Matrix Portfolios")


def now_ist() -> str:
    return datetime.now(tz=IST).isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def money(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    sign = "-" if number < 0 else ""
    number = abs(number)
    if number >= 10_000_000:
        return f"{sign}Rs {number / 10_000_000:.2f}Cr"
    if number >= 100_000:
        return f"{sign}Rs {number / 100_000:.2f}L"
    return f"{sign}Rs {number:,.0f}"


def pct(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "-"


def payload() -> dict[str, Any]:
    state = read_json(STATE_PATH, {})
    status = read_json(STATUS_PATH, {})
    if not isinstance(state, dict):
        state = {}
    if not isinstance(status, dict):
        status = {}
    return {
        "ok": bool(state),
        "service": "v2Matrix Portfolios",
        "updated_at_ist": now_ist(),
        "state_path": str(STATE_PATH),
        "overlay_status": status,
        "portfolio_state": state,
    }


@app.get("/api/v2matrix-portfolios/v1/status")
def api_status() -> JSONResponse:
    return JSONResponse(payload())


@app.get("/api/v2matrix-portfolios/v1/stream")
async def api_stream() -> StreamingResponse:
    async def events():
        while True:
            yield "event: status\n"
            yield "data: " + json.dumps(payload(), ensure_ascii=True, sort_keys=True) + "\n\n"
            await asyncio.sleep(5)

    return StreamingResponse(events(), media_type="text/event-stream")


def render_summary_cards(summaries: list[dict[str, Any]]) -> str:
    cards = []
    for summary in summaries:
        cards.append(
            f"""
            <section class="summary">
              <div class="kicker">{summary.get('variant', '-')}</div>
              <h2>{summary.get('open_positions', 0)} Open / {summary.get('closed_trades', 0)} Closed</h2>
              <div class="metrics">
                <span>Net <strong>{money(summary.get('total_net_rupees'))}</strong></span>
                <span>Realized <strong>{money(summary.get('realized_net_rupees'))}</strong></span>
                <span>Peak Margin <strong>{money(summary.get('peak_margin_rupees'))}</strong></span>
                <span>Return / Peak Margin <strong>{pct(summary.get('return_on_peak_margin_pct'))}</strong></span>
                <span>Success <strong>{pct(summary.get('success_rate_pct'))}</strong></span>
              </div>
            </section>
            """
        )
    return "\n".join(cards) or "<section class='summary'><h2>No portfolio state yet</h2></section>"


def render_holdings(portfolios: dict[str, Any]) -> str:
    rows = []
    for portfolio_id, portfolio in sorted(portfolios.items()):
        if not isinstance(portfolio, dict):
            continue
        for holding in (portfolio.get("holdings") or {}).values():
            if not isinstance(holding, dict):
                continue
            rows.append(
                f"""
                <tr>
                  <td>{portfolio_id}</td>
                  <td>{holding.get('symbol', '-')}</td>
                  <td>{holding.get('side', '-')}</td>
                  <td>{holding.get('lots', '-')}</td>
                  <td>{money(holding.get('margin_locked'))}</td>
                  <td>{holding.get('entry_time', '-')}</td>
                  <td>{holding.get('entry_score', '-')}</td>
                </tr>
                """
            )
    return "\n".join(rows) or "<tr><td colspan='7'>No open portfolio positions</td></tr>"


def render_transactions(portfolios: dict[str, Any]) -> str:
    rows = []
    for portfolio_id, portfolio in sorted(portfolios.items()):
        if not isinstance(portfolio, dict):
            continue
        txs = [row for row in (portfolio.get("transactions") or []) if isinstance(row, dict)]
        for row in reversed(txs[-120:]):
            rows.append(
                f"""
                <tr>
                  <td>{portfolio_id}</td>
                  <td>{row.get('event', '-')}</td>
                  <td>{row.get('symbol', '-')}</td>
                  <td>{row.get('side', '-')}</td>
                  <td>{row.get('lots', '-')}</td>
                  <td>{row.get('entry_time', '-')}</td>
                  <td>{row.get('exit_time', '-')}</td>
                  <td>{row.get('exit_reason', '-')}</td>
                  <td>{money(row.get('net_rupees'))}</td>
                </tr>
                """
            )
    return "\n".join(rows) or "<tr><td colspan='9'>No portfolio transactions yet</td></tr>"


@app.get("/v2Matrix_portfolios", response_class=HTMLResponse)
@app.get("/v2Matrix_portfolios/", response_class=HTMLResponse)
def page() -> HTMLResponse:
    data = payload()
    state = data.get("portfolio_state") if isinstance(data.get("portfolio_state"), dict) else {}
    summaries = [row for row in (state.get("summaries") or []) if isinstance(row, dict)]
    portfolios = state.get("portfolios") if isinstance(state.get("portfolios"), dict) else {}
    overlay_status = data.get("overlay_status") if isinstance(data.get("overlay_status"), dict) else {}
    clock = overlay_status.get("clock") if isinstance(overlay_status.get("clock"), dict) else {}
    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>v2Matrix Portfolios</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #07090d;
      --panel: #111722;
      --panel-2: #151d2b;
      --text: #f4f7fb;
      --muted: #9aa6b5;
      --line: #263247;
      --good: #48d597;
      --bad: #ff6b7a;
      --accent: #7cc4ff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{
      width: min(1520px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 24px 0 48px;
    }}
    header {{
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 18px;
      margin-bottom: 18px;
    }}
    h1, h2 {{ margin: 0; letter-spacing: 0; }}
    h1 {{ font-size: 28px; }}
    h2 {{ font-size: 19px; }}
    .sub {{ color: var(--muted); margin-top: 4px; }}
    .status {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
      color: var(--muted);
    }}
    .pill {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 10px;
      background: #0c111a;
      white-space: nowrap;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }}
    .summary, .table-block {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 14px;
    }}
    .kicker {{
      color: var(--accent);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .08em;
      margin-bottom: 6px;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 8px;
      margin-top: 12px;
    }}
    .metrics span {{
      background: var(--panel-2);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      color: var(--muted);
      min-height: 58px;
    }}
    strong {{ display: block; color: var(--text); font-size: 16px; margin-top: 4px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 9px 8px;
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .06em;
    }}
    .table-block {{ margin-top: 14px; overflow-x: auto; }}
    @media (max-width: 900px) {{
      header {{ display: block; }}
      .status {{ justify-content: flex-start; margin-top: 12px; }}
      .grid, .metrics {{ grid-template-columns: 1fr; }}
      main {{ width: min(100vw - 20px, 1520px); padding-top: 16px; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>v2Matrix Portfolios</h1>
        <div class="sub">Fixed Rs 5L per entry, no replacement, max 3 positions per paper portfolio.</div>
      </div>
      <div class="status">
        <span class="pill">Overlay ok: {overlay_status.get('ok', '-')}</span>
        <span class="pill">Clock: {clock.get('clock_time_ist', '-')}</span>
        <span class="pill">Eligible: {clock.get('eligible_count', '-')}</span>
        <span class="pill">Updated: {state.get('updated_at_ist', '-')}</span>
      </div>
    </header>
    <div class="grid">{render_summary_cards(summaries)}</div>
    <section class="table-block">
      <h2>Open Positions</h2>
      <table>
        <thead><tr><th>Portfolio</th><th>Symbol</th><th>Side</th><th>Lots</th><th>Margin</th><th>Entry Time</th><th>Score</th></tr></thead>
        <tbody>{render_holdings(portfolios)}</tbody>
      </table>
    </section>
    <section class="table-block">
      <h2>Recent Transactions</h2>
      <table>
        <thead><tr><th>Portfolio</th><th>Event</th><th>Symbol</th><th>Side</th><th>Lots</th><th>Entry Time</th><th>Exit Time</th><th>Reason</th><th>Net</th></tr></thead>
        <tbody>{render_transactions(portfolios)}</tbody>
      </table>
    </section>
  </main>
  <script>
    const source = new EventSource("/api/v2matrix-portfolios/v1/stream");
    source.addEventListener("status", () => {{
      window.__v2matrixPortfolioLastEvent = Date.now();
    }});
  </script>
</body>
</html>
"""
    return HTMLResponse(html)


@app.head("/v2Matrix_portfolios")
@app.head("/v2Matrix_portfolios/")
def page_head() -> HTMLResponse:
    return HTMLResponse("")
