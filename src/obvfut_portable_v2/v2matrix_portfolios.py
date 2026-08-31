from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from html import escape
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


def text(value: Any) -> str:
    if value in {None, ""}:
        return "-"
    return escape(str(value))


def sort_attr(value: Any) -> str:
    if value in {None, ""}:
        return ""
    return escape(str(value), quote=True)


def cell(display: Any, sort_value: Any = None) -> str:
    display_text = str(display) if display not in {None, ""} else "-"
    value = display if sort_value is None else sort_value
    return f'<td data-sort-value="{sort_attr(value)}">{escape(display_text)}</td>'


def head(label: str) -> str:
    return f'<th scope="col"><button type="button" class="sort-button">{escape(label)}<span class="sort-mark" aria-hidden="true"></span></button></th>'


def portfolio_source(portfolio: dict[str, Any] | None = None, summary: dict[str, Any] | None = None) -> dict[str, Any]:
    source: dict[str, Any] = {}
    if isinstance(portfolio, dict):
        source.update(portfolio)
    if isinstance(summary, dict):
        source.update(summary)
    return source


def portfolio_display_name(portfolio_id: str, portfolio: dict[str, Any] | None = None, summary: dict[str, Any] | None = None) -> str:
    source = portfolio_source(portfolio, summary)
    label = source.get("label") or source.get("rule") or source.get("variant")
    return str(label) if label else portfolio_id


def portfolio_max_positions(portfolio_id: str, source: dict[str, Any]) -> str:
    value = source.get("max_positions")
    if value not in {None, ""}:
        return str(value)
    haystack = " ".join(str(source.get(key) or "") for key in ("label", "rule", "variant"))
    haystack = f"{haystack} {portfolio_id}".lower()
    for size in (3, 4, 5, 6, 7, 8, 10):
        if f"max{size}" in haystack or f"max {size}" in haystack:
            return str(size)
    return ""


def portfolio_badges(portfolio_id: str, source: dict[str, Any]) -> list[str]:
    labelish = " ".join(str(source.get(key) or "") for key in ("label", "rule", "variant", "portfolio_id"))
    labelish = f"{labelish} {portfolio_id}".lower()
    badges = []
    fixed_margin = source.get("fixed_entry_margin_rupees")
    if fixed_margin not in {None, ""}:
        badges.append(f"{money(fixed_margin)}/entry")
    max_positions = portfolio_max_positions(portfolio_id, source)
    if max_positions:
        badges.append(f"Max {max_positions}")
    if "age0" in labelish or "age 0" in labelish:
        badges.append("Age 0")
    elif "age60" in labelish or "age 60" in labelish:
        badges.append("Age 60")
    if "stop100" in labelish or "stop 100" in labelish:
        badges.append("Stop 100 bps")
    elif "no stop" in labelish:
        badges.append("No stop")
    if source.get("requalify") is True:
        cooldown = source.get("cooldown_minutes")
        max_entries = source.get("max_entries_per_t2_leg")
        bits = ["Requalify"]
        if cooldown not in {None, ""}:
            bits.append(f"cd{cooldown}m")
        if max_entries not in {None, ""}:
            bits.append(f"max{max_entries}/leg")
        badges.append(" ".join(bits))
    elif source.get("requalify") is False:
        badges.append("First only")
    return badges


def render_badges(badges: list[str]) -> str:
    return "".join(f'<span class="badge">{escape(str(badge))}</span>' for badge in badges)


def render_portfolio_filter(portfolios: dict[str, Any], summaries: list[dict[str, Any]]) -> str:
    summary_by_id = {str(row.get("portfolio_id") or ""): row for row in summaries if isinstance(row, dict)}
    portfolio_ids = sorted(
        {str(key) for key in portfolios} | {key for key in summary_by_id if key},
        key=lambda item: (
            portfolio_max_positions(item, portfolio_source(portfolios.get(item) if isinstance(portfolios.get(item), dict) else {}, summary_by_id.get(item))),
            portfolio_display_name(item, portfolios.get(item) if isinstance(portfolios.get(item), dict) else {}, summary_by_id.get(item)).lower(),
            item,
        ),
    )
    max_values = sorted(
        {
            portfolio_max_positions(portfolio_id, portfolio_source(portfolios.get(portfolio_id) if isinstance(portfolios.get(portfolio_id), dict) else {}, summary_by_id.get(portfolio_id)))
            for portfolio_id in portfolio_ids
        }
        - {""},
        key=lambda value: int(value),
    )
    options = ['<option value="">All Portfolios</option>']
    for portfolio_id in portfolio_ids:
        portfolio = portfolios.get(portfolio_id) if isinstance(portfolios.get(portfolio_id), dict) else {}
        label = portfolio_display_name(portfolio_id, portfolio, summary_by_id.get(portfolio_id))
        options.append(f'<option value="{sort_attr(portfolio_id)}">{escape(label)}</option>')
    max_options = ['<option value="">All Max Sizes</option>']
    max_options.extend(f'<option value="{sort_attr(value)}">Max {escape(value)}</option>' for value in max_values)
    return f"""
      <div class="filters">
        <div class="filter-field">
          <label for="portfolioFilter">Portfolio</label>
          <select id="portfolioFilter">
            {''.join(options)}
          </select>
        </div>
        <div class="filter-field compact">
          <label for="maxFilter">Max Positions</label>
          <select id="maxFilter">
            {''.join(max_options)}
          </select>
        </div>
      </div>
    """


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
        portfolio_id = str(summary.get("portfolio_id") or "")
        label = portfolio_display_name(portfolio_id, summary=summary)
        badges = render_badges(portfolio_badges(portfolio_id, summary))
        cards.append(
            f"""
            <section class="summary">
              <div class="kicker">{text(summary.get('variant'))}</div>
              <div class="summary-title">
                <h2>{escape(label)}</h2>
                <span class="id-chip">{escape(portfolio_id)}</span>
              </div>
              <div class="badge-row">{badges}</div>
              <h2>{text(summary.get('open_positions', 0))} Open / {text(summary.get('closed_trades', 0))} Closed</h2>
              <div class="metrics">
                <span>Net <strong>{money(summary.get('total_net_rupees'))}</strong></span>
                <span>Realized <strong>{money(summary.get('realized_net_rupees'))}</strong></span>
                <span>Peak Margin <strong>{money(summary.get('peak_margin_rupees'))}</strong></span>
                <span>Return / Peak Margin <strong>{pct(summary.get('return_on_peak_margin_pct'))}</strong></span>
                <span>Portfolio Closed Success <strong>{pct(summary.get('portfolio_closed_success_rate_pct') or summary.get('success_rate_pct'))}</strong></span>
                <span>All Qualified Signal Success <strong>{pct(summary.get('all_qualified_signal_success_rate_pct'))}</strong></span>
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
        label = portfolio_display_name(str(portfolio_id), portfolio)
        max_positions = portfolio_max_positions(str(portfolio_id), portfolio)
        for holding in (portfolio.get("holdings") or {}).values():
            if not isinstance(holding, dict):
                continue
            rows.append(
                f"""
                <tr data-portfolio-id="{sort_attr(portfolio_id)}" data-portfolio-variant="{sort_attr(portfolio.get('variant'))}" data-portfolio-max="{sort_attr(max_positions)}">
                  {cell(label, label)}
                  {cell(holding.get('symbol'))}
                  {cell(holding.get('side'))}
                  {cell(holding.get('lots'), holding.get('lots'))}
                  {cell(money(holding.get('margin_locked')), holding.get('margin_locked'))}
                  {cell(holding.get('entry_time'), holding.get('entry_epoch') or holding.get('entry_time'))}
                  {cell(holding.get('entry_score'), holding.get('entry_score'))}
                </tr>
                """
            )
    return "\n".join(rows) or "<tr><td colspan='7'>No open portfolio positions</td></tr>"


def render_transactions(portfolios: dict[str, Any]) -> str:
    rows = []
    for portfolio_id, portfolio in sorted(portfolios.items()):
        if not isinstance(portfolio, dict):
            continue
        label = portfolio_display_name(str(portfolio_id), portfolio)
        max_positions = portfolio_max_positions(str(portfolio_id), portfolio)
        txs = [row for row in (portfolio.get("transactions") or []) if isinstance(row, dict)]
        for row in reversed(txs[-120:]):
            rows.append(
                f"""
                <tr data-portfolio-id="{sort_attr(portfolio_id)}" data-portfolio-variant="{sort_attr(portfolio.get('variant'))}" data-portfolio-max="{sort_attr(max_positions)}">
                  {cell(label, label)}
                  {cell(row.get('event'))}
                  {cell(row.get('symbol'))}
                  {cell(row.get('side'))}
                  {cell(row.get('lots'), row.get('lots'))}
                  {cell(row.get('entry_time'), row.get('entry_epoch') or row.get('entry_time'))}
                  {cell(row.get('exit_time'), row.get('exit_epoch') or row.get('exit_time'))}
                  {cell(row.get('exit_reason'))}
                  {cell(money(row.get('net_rupees')), row.get('net_rupees'))}
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
    filter_html = render_portfolio_filter(portfolios, summaries)
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
    .summary-title {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      align-items: start;
    }}
    .summary-title h2 {{
      font-size: 17px;
      line-height: 1.25;
    }}
    .id-chip {{
      max-width: 320px;
      color: var(--muted);
      font-size: 11px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .badge-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin: 8px 0 10px;
    }}
    .badge {{
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #0c111a;
      color: var(--muted);
      font-size: 12px;
      padding: 4px 8px;
      white-space: nowrap;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
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
    .sort-button {{
      appearance: none;
      border: 0;
      background: transparent;
      color: inherit;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font: inherit;
      letter-spacing: inherit;
      padding: 0;
      text-align: left;
      text-transform: inherit;
      width: 100%;
    }}
    .sort-button:hover, .sort-button:focus-visible {{
      color: var(--text);
      outline: none;
    }}
    .sort-mark::after {{
      content: "";
      color: var(--accent);
      font-size: 11px;
    }}
    th[aria-sort="ascending"] .sort-mark::after {{ content: "ASC"; }}
    th[aria-sort="descending"] .sort-mark::after {{ content: "DESC"; }}
    .table-block {{ margin-top: 14px; overflow-x: auto; }}
    .filters {{
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 10px 12px;
      margin: 8px 0 14px;
    }}
    .filter-field {{
      display: grid;
      gap: 6px;
      min-width: min(760px, 100%);
    }}
    .filter-field.compact {{
      min-width: 180px;
    }}
    .filter-field label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .06em;
    }}
    .filter-field select {{
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #0c111a;
      color: var(--text);
      padding: 8px 10px;
      font: inherit;
    }}
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
        <div class="sub">Fixed Rs 5L per entry, no replacement, with max 3 or max 5 positions by selected variant.</div>
      </div>
      <div class="status">
        <span class="pill">Overlay ok: {text(overlay_status.get('ok'))}</span>
        <span class="pill">Clock: {text(clock.get('clock_time_ist'))}</span>
        <span class="pill">Eligible: {text(clock.get('eligible_count'))}</span>
        <span class="pill">Updated: {text(state.get('updated_at_ist'))}</span>
      </div>
    </header>
    <div class="grid">{render_summary_cards(summaries)}</div>
    {filter_html}
    <section class="table-block">
      <h2>Open Positions</h2>
      <table class="sortable-table">
        <thead><tr>{head('Portfolio')}{head('Symbol')}{head('Side')}{head('Lots')}{head('Margin')}{head('Entry Time')}{head('Score')}</tr></thead>
        <tbody>{render_holdings(portfolios)}</tbody>
      </table>
    </section>
    <section class="table-block">
      <h2>Recent Transactions</h2>
      <table class="sortable-table">
        <thead><tr>{head('Portfolio')}{head('Event')}{head('Symbol')}{head('Side')}{head('Lots')}{head('Entry Time')}{head('Exit Time')}{head('Reason')}{head('Net')}</tr></thead>
        <tbody>{render_transactions(portfolios)}</tbody>
      </table>
    </section>
  </main>
  <script>
    const source = new EventSource("/api/v2matrix-portfolios/v1/stream");
    source.addEventListener("status", () => {{
      window.__v2matrixPortfolioLastEvent = Date.now();
    }});
    const parseSortValue = (raw) => {{
      const value = (raw || "").trim();
      if (!value || value === "-") return {{ empty: true, type: "text", value: "" }};
      const numeric = Number(value);
      if (Number.isFinite(numeric)) return {{ empty: false, type: "number", value: numeric }};
      const timestamp = Date.parse(value);
      if (Number.isFinite(timestamp)) return {{ empty: false, type: "number", value: timestamp }};
      return {{ empty: false, type: "text", value: value.toLowerCase() }};
    }};
    const compareValues = (left, right, direction) => {{
      if (left.empty && right.empty) return 0;
      if (left.empty) return 1;
      if (right.empty) return -1;
      let result = 0;
      if (left.type === "number" && right.type === "number") {{
        result = left.value === right.value ? 0 : left.value > right.value ? 1 : -1;
      }} else {{
        result = String(left.value).localeCompare(String(right.value), undefined, {{ numeric: true, sensitivity: "base" }});
      }}
      return direction === "ascending" ? result : -result;
    }};
    document.querySelectorAll("table.sortable-table").forEach((table) => {{
      const headers = Array.from(table.querySelectorAll("thead th"));
      const body = table.querySelector("tbody");
      if (!body) return;
      headers.forEach((header, index) => {{
        const button = header.querySelector("button");
        if (!button) return;
        button.addEventListener("click", () => {{
          const current = header.getAttribute("aria-sort");
          const direction = current === "ascending" ? "descending" : "ascending";
          headers.forEach((item) => item.removeAttribute("aria-sort"));
          header.setAttribute("aria-sort", direction);
          const rows = Array.from(body.querySelectorAll("tr"))
            .filter((row) => !row.querySelector("td[colspan]"))
            .map((row, originalIndex) => ({{ row, originalIndex }}));
          rows.sort((a, b) => {{
            const left = parseSortValue(a.row.children[index]?.dataset.sortValue || a.row.children[index]?.textContent || "");
            const right = parseSortValue(b.row.children[index]?.dataset.sortValue || b.row.children[index]?.textContent || "");
            const result = compareValues(left, right, direction);
            return result || a.originalIndex - b.originalIndex;
          }});
          rows.forEach((item) => body.appendChild(item.row));
        }});
      }});
    }});
    const portfolioFilter = document.getElementById("portfolioFilter");
    const maxFilter = document.getElementById("maxFilter");
    const applyPortfolioFilter = () => {{
      const selected = portfolioFilter ? portfolioFilter.value : "";
      const selectedMax = maxFilter ? maxFilter.value : "";
      document.querySelectorAll("tbody tr[data-portfolio-id]").forEach((row) => {{
        const portfolioMatches = !selected || row.dataset.portfolioId === selected;
        const maxMatches = !selectedMax || row.dataset.portfolioMax === selectedMax;
        const visible = portfolioMatches && maxMatches;
        row.style.display = visible ? "" : "none";
      }});
    }};
    if (portfolioFilter) {{
      portfolioFilter.addEventListener("change", applyPortfolioFilter);
    }}
    if (maxFilter) maxFilter.addEventListener("change", applyPortfolioFilter);
    applyPortfolioFilter();
  </script>
</body>
</html>
"""
    return HTMLResponse(html)


@app.head("/v2Matrix_portfolios")
@app.head("/v2Matrix_portfolios/")
def page_head() -> HTMLResponse:
    return HTMLResponse("")
