from __future__ import annotations

import json
import sys
import types
import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "obvfut_portable_v2" / "v2matrix_portfolios.py"


def load_portfolios_module(monkeypatch):
    fastapi = types.ModuleType("fastapi")
    responses = types.ModuleType("fastapi.responses")

    class FastAPI:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            return lambda fn: fn

        def head(self, *args, **kwargs):
            return lambda fn: fn

    class HTMLResponse:
        def __init__(self, content="", *args, **kwargs):
            self.body = str(content).encode("utf-8")

    class JSONResponse:
        def __init__(self, content=None, *args, **kwargs):
            self.content = content

    class StreamingResponse:
        def __init__(self, content=None, *args, **kwargs):
            self.content = content

    fastapi.FastAPI = FastAPI
    responses.HTMLResponse = HTMLResponse
    responses.JSONResponse = JSONResponse
    responses.StreamingResponse = StreamingResponse
    monkeypatch.setitem(sys.modules, "fastapi", fastapi)
    monkeypatch.setitem(sys.modules, "fastapi.responses", responses)
    spec = importlib.util.spec_from_file_location("v2matrix_portfolios_under_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_portfolio_tables_escape_cells_and_keep_sort_values(monkeypatch) -> None:
    v2matrix_portfolios = load_portfolios_module(monkeypatch)
    html = v2matrix_portfolios.render_holdings(
        {
            'p<1>': {
                "holdings": {
                    "row": {
                        "symbol": "<script>alert(1)</script>",
                        "side": "long",
                        "lots": 2,
                        "margin_locked": 500000,
                        "entry_time": "2026-08-27T10:00:00+05:30",
                        "entry_epoch": 1787795400,
                        "entry_score": "9>8",
                    }
                }
            }
        }
    )

    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert 'data-portfolio-id="p&lt;1&gt;"' in html
    assert "<script>" not in html
    assert 'data-sort-value="1787795400"' in html
    assert "Rs 5.00L" in html
    assert "9&gt;8" in html


def test_portfolio_page_renders_sortable_tables(tmp_path: Path, monkeypatch) -> None:
    v2matrix_portfolios = load_portfolios_module(monkeypatch)
    state_path = tmp_path / "portfolio_state.json"
    status_path = tmp_path / "overlay_status.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_at_ist": "2026-08-27T15:45:00+05:30",
                "summaries": [
                    {
                        "variant": "fixed<capital>",
                        "open_positions": 1,
                        "closed_trades": 2,
                        "total_net_rupees": 1000,
                        "portfolio_closed_success_rate_pct": 50,
                        "all_qualified_signal_success_rate_pct": 75,
                    }
                ],
                "portfolios": {
                    "fixed": {
                        "holdings": {
                            "ABC": {
                                "symbol": "ABC",
                                "side": "long",
                                "lots": 1,
                                "margin_locked": 100000,
                                "entry_time": "2026-08-27T10:00:00+05:30",
                                "entry_epoch": 1787795400,
                            }
                        },
                        "transactions": [
                            {
                                "event": "exit",
                                "symbol": "XYZ",
                                "side": "short",
                                "lots": 1,
                                "entry_epoch": 1787795400,
                                "exit_epoch": 1787799000,
                                "net_rupees": -250.5,
                            }
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    status_path.write_text(json.dumps({"ok": True, "clock": {"clock_time_ist": "15:20", "eligible_count": 3}}), encoding="utf-8")
    monkeypatch.setattr(v2matrix_portfolios, "STATE_PATH", state_path)
    monkeypatch.setattr(v2matrix_portfolios, "STATUS_PATH", status_path)

    html = v2matrix_portfolios.page().body.decode("utf-8")

    assert 'table class="sortable-table"' in html
    assert 'id="portfolioFilter"' in html
    assert "applyPortfolioFilter" in html
    assert 'data-portfolio-id="fixed"' in html
    assert "parseSortValue" in html
    assert "aria-sort" in html
    assert 'data-sort-value="1787795400"' in html
    assert "fixed&lt;capital&gt;" in html
    assert "Portfolio Closed Success" in html
    assert "All Qualified Signal Success" in html
    assert "75.00%" in html
    assert "Portfolio Comparison" in html
    assert "window.setInterval(() =&gt; window.location.reload(), 60000)" not in html
    assert "window.setInterval(() => window.location.reload(), 60000)" in html


def test_portfolio_page_uses_readable_variant_labels_and_max_filters(tmp_path: Path, monkeypatch) -> None:
    v2matrix_portfolios = load_portfolios_module(monkeypatch)
    state_path = tmp_path / "portfolio_state.json"
    status_path = tmp_path / "overlay_status.json"
    max3_id = "fixed5L_no_replacement_max3_smooth_survivor_profit25"
    max5_id = "fixed5L_no_replacement_max5_smooth_survivor_profit25_age0_max5_stop100_requal_cd15_max2"
    state_path.write_text(
        json.dumps(
            {
                "updated_at_ist": "2026-08-31T12:00:00+05:30",
                "summaries": [
                    {
                        "portfolio_id": max3_id,
                        "variant": "smooth_survivor_profit25",
                        "label": "Profit25 / age60 / max3 / no stop / first only",
                        "max_positions": 3,
                        "fixed_entry_margin_rupees": 500000,
                        "requalify": False,
                        "open_positions": 1,
                        "closed_trades": 10,
                    },
                    {
                        "portfolio_id": max5_id,
                        "variant": "smooth_survivor_profit25_age0_max5_stop100_requal_cd15_max2",
                        "label": "Profit25 / age0 / max5 / stop100 / requalify cd15 max2",
                        "max_positions": 5,
                        "fixed_entry_margin_rupees": 500000,
                        "requalify": True,
                        "cooldown_minutes": 15,
                        "max_entries_per_t2_leg": 2,
                        "open_positions": 2,
                        "closed_trades": 20,
                    },
                ],
                "portfolios": {
                    max3_id: {
                        "label": "Profit25 / age60 / max3 / no stop / first only",
                        "variant": "smooth_survivor_profit25",
                        "max_positions": 3,
                        "holdings": {
                            "ABC": {"symbol": "ABC", "side": "long", "lots": 1, "entry_epoch": 1}
                        },
                        "transactions": [],
                    },
                    max5_id: {
                        "label": "Profit25 / age0 / max5 / stop100 / requalify cd15 max2",
                        "variant": "smooth_survivor_profit25_age0_max5_stop100_requal_cd15_max2",
                        "max_positions": 5,
                        "holdings": {
                            "XYZ": {"symbol": "XYZ", "side": "short", "lots": 1, "entry_epoch": 2}
                        },
                        "transactions": [],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    status_path.write_text(json.dumps({"ok": True, "clock": {}}), encoding="utf-8")
    monkeypatch.setattr(v2matrix_portfolios, "STATE_PATH", state_path)
    monkeypatch.setattr(v2matrix_portfolios, "STATUS_PATH", status_path)

    html = v2matrix_portfolios.page().body.decode("utf-8")

    assert "max 3 or max 5 positions" in html
    assert 'id="maxFilter"' in html
    assert ">Max 5<" in html
    assert "Profit25 / age0 / max5 / stop100 / requalify cd15 max2" in html
    assert "Portfolio Comparison" in html
    assert "Qualified Signals" in html
    assert 'data-portfolio-max="5"' in html
    assert f'<option value="{max5_id}">Profit25 / age0 / max5 / stop100 / requalify cd15 max2</option>' in html


def test_portfolio_page_uses_requested_portfolio_order(tmp_path: Path, monkeypatch) -> None:
    v2matrix_portfolios = load_portfolios_module(monkeypatch)
    state_path = tmp_path / "portfolio_state.json"
    status_path = tmp_path / "overlay_status.json"
    labels = [
        "Profit25 / age0 / max5 / stop100 / requalify cd15 max2",
        "Armed20 Floor80 / age60 / max3 / no stop / first only",
        "Profit25 / age60 / max3 / no stop / requalify cd0 max3",
        "Armed20 Floor80 / age0 / max5 / stop100 / requalify cd0 max3",
    ]
    state_path.write_text(
        json.dumps(
            {
                "updated_at_ist": "2026-08-31T12:00:00+05:30",
                "summaries": [
                    {"portfolio_id": f"p{i}", "label": label, "max_positions": 5 if "max5" in label else 3}
                    for i, label in enumerate(labels)
                ],
                "portfolios": {
                    f"p{i}": {"label": label, "max_positions": 5 if "max5" in label else 3, "holdings": {}, "transactions": []}
                    for i, label in enumerate(labels)
                },
            }
        ),
        encoding="utf-8",
    )
    status_path.write_text(json.dumps({"ok": True, "clock": {}}), encoding="utf-8")
    monkeypatch.setattr(v2matrix_portfolios, "STATE_PATH", state_path)
    monkeypatch.setattr(v2matrix_portfolios, "STATUS_PATH", status_path)

    html = v2matrix_portfolios.page().body.decode("utf-8")

    ordered_labels = [
        "Armed20 Floor80 / age60 / max3 / no stop / first only",
        "Profit25 / age60 / max3 / no stop / requalify cd0 max3",
        "Armed20 Floor80 / age0 / max5 / stop100 / requalify cd0 max3",
        "Profit25 / age0 / max5 / stop100 / requalify cd15 max2",
    ]
    positions = [html.index(label) for label in ordered_labels]
    assert positions == sorted(positions)
