"""Regression: Beeswax CSV reports return Title Case headers.

Without column normalization, `df.get("impression", 0).fillna(0)` blows up
with `AttributeError: 'int' object has no attribute 'fillna'` because
`df.get(missing_col, default)` returns the *default* (an int) instead of
a Series. Every report-fetcher in the pipeline now normalizes column
names — this file exercises that.

Reported by Casey on April 28 in `_fetch_last_3_days_cpm`. Same bug
pattern existed in 4 other places; this test pins all of them.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from sbo.bid_optimizer import _fetch_term_floor_prices
from sbo.full_report import (
    _fetch_deal_floor_prices,
    _fetch_last_1_day_imps,
    _fetch_last_3_days_cpm,
    _normalize_atr,
)
from sbo.pacing import _fetch_imps_by_li
from sbo.utils import REPORT_ALIASES, normalize_columns


@pytest.fixture
def fake_run():
    run = MagicMock()
    run.log.side_effect = lambda msg: None
    return run


def _bw_with_csv_rows(rows):
    """Stand-in BeeswaxClient.fetch_report → returns the supplied rows."""
    bw = MagicMock()
    bw.fetch_report.return_value = rows
    return bw


# ── normalize_columns helper ─────────────────────────────────────────────


def test_normalize_columns_handles_title_case():
    df = pd.DataFrame([{"Line Item ID": "9001", "Impression": 100}])
    out = normalize_columns(df, REPORT_ALIASES["performance_agg"])
    assert "line_item_id" in out.columns
    assert "impression" in out.columns
    assert out.iloc[0]["line_item_id"] == "9001"


def test_normalize_columns_handles_snake_case_already():
    df = pd.DataFrame([{"line_item_id": "9001", "impression": 100}])
    out = normalize_columns(df, REPORT_ALIASES["performance_agg"])
    assert "line_item_id" in out.columns
    assert out.iloc[0]["line_item_id"] == "9001"


def test_normalize_columns_handles_dotted_headers():
    df = pd.DataFrame([{"line.item.id": "9001", "impression": 100}])
    out = normalize_columns(df, REPORT_ALIASES["performance_agg"])
    assert "line_item_id" in out.columns


def test_normalize_columns_empty_df_safe():
    df = pd.DataFrame()
    out = normalize_columns(df, REPORT_ALIASES["performance_agg"])
    assert out.empty


# ── _fetch_last_3_days_cpm — the function Casey hit ──────────────────────


def test_last_3_days_cpm_with_title_case_csv(fake_run):
    """Beeswax returns 'Line Item ID' / 'Impression' / 'Media Spend USD'."""
    rows = [
        {"Line Item ID": "9001", "Impression": "1000", "Media Spend USD": "5.00"},
        {"Line Item ID": "9001", "Impression": "2000", "Media Spend USD": "10.00"},
        {"Line Item ID": "9002", "Impression": "500",  "Media Spend USD": "2.50"},
    ]
    bw = _bw_with_csv_rows(rows)
    df = _fetch_last_3_days_cpm(bw, ["9001", "9002"], fake_run)
    assert len(df) == 2
    assert set(df.columns) == {"line_item_id", "last_3_days_cpm"}
    # 9001: $15 spend / 3000 imps × 1000 = $5.00 CPM
    cpm_9001 = df[df["line_item_id"] == "9001"]["last_3_days_cpm"].iloc[0]
    assert cpm_9001 == 5.0


def test_last_3_days_cpm_with_snake_case_csv(fake_run):
    """Backwards compat — original snake_case still works."""
    rows = [{"line_item_id": "9001", "impression": "1000", "media_spend_usd": "5.00"}]
    bw = _bw_with_csv_rows(rows)
    df = _fetch_last_3_days_cpm(bw, ["9001"], fake_run)
    assert len(df) == 1
    assert df.iloc[0]["last_3_days_cpm"] == 5.0


def test_last_3_days_cpm_with_empty_response(fake_run):
    """Empty report → empty result, no crash."""
    bw = _bw_with_csv_rows([])
    df = _fetch_last_3_days_cpm(bw, ["9001"], fake_run)
    assert df.empty
    assert list(df.columns) == ["line_item_id", "last_3_days_cpm"]


def test_last_3_days_cpm_with_missing_columns_logs_and_returns_empty(fake_run):
    """If the CSV is structurally broken, log + return empty (never crash)."""
    rows = [{"WeirdCol": "9001"}]  # no recognizable impression/spend
    bw = _bw_with_csv_rows(rows)
    df = _fetch_last_3_days_cpm(bw, ["9001"], fake_run)
    assert df.empty


# ── _fetch_last_1_day_imps ───────────────────────────────────────────────


def test_last_1_day_imps_with_title_case(fake_run):
    rows = [
        {"Line Item ID": "9001", "Impression": "100"},
        {"Line Item ID": "9002", "Impression": "0"},
    ]
    bw = _bw_with_csv_rows(rows)
    df = _fetch_last_1_day_imps(bw, ["9001", "9002", "9003"], fake_run)
    # All input LIs present
    assert set(df["BW_Line_Item_ID"]) == {"9001", "9002", "9003"}
    flag = dict(zip(df["BW_Line_Item_ID"], df["Had_Impressions_Yesterday"]))
    assert flag["9001"] == "Y"
    assert flag["9002"] == "N"
    assert flag["9003"] == "N"  # not in report = no delivery


def test_last_1_day_imps_with_empty_response(fake_run):
    bw = _bw_with_csv_rows([])
    df = _fetch_last_1_day_imps(bw, ["9001"], fake_run)
    assert df.iloc[0]["Had_Impressions_Yesterday"] == "N"


# ── _fetch_deal_floor_prices ─────────────────────────────────────────────


def test_deal_floor_prices_with_title_case(fake_run):
    rows = [
        {"Deal ID": "tri/abc", "Floor Price": "4.00"},
        {"Deal ID": "tri/xyz", "Floor Price": "8.00"},
    ]
    bw = _bw_with_csv_rows(rows)
    df = _fetch_deal_floor_prices(bw, ["tri/abc", "tri/xyz"], fake_run)
    assert len(df) == 2
    floors = dict(zip(df["deal_id"], df["floor_price"]))
    assert floors["tri/abc"] == 4.00
    assert floors["tri/xyz"] == 8.00


# ── _fetch_term_floor_prices (bid_optimizer.py) ──────────────────────────


def test_term_floor_prices_with_title_case(fake_run):
    rows = [
        {"Deal ID": "tri/abc", "Floor Price": "4.00"},
        {"Deal ID": "tri/xyz", "Floor Price": "8.00"},
    ]
    bw = _bw_with_csv_rows(rows)
    out = _fetch_term_floor_prices(bw, ["tri/abc", "tri/xyz"], fake_run)
    assert out == {"tri/abc": 4.0, "tri/xyz": 8.0}


def test_term_floor_prices_handles_empty(fake_run):
    bw = _bw_with_csv_rows([])
    out = _fetch_term_floor_prices(bw, ["tri/abc"], fake_run)
    assert out == {}


# ── _fetch_imps_by_li (pacing.py) ────────────────────────────────────────


def test_pacing_imps_with_title_case(fake_run):
    rows = [
        {"Line Item ID": "9001", "Impression": "500"},
        {"Line Item ID": "9001", "Impression": "1500"},  # same LI, multi-row
        {"Line Item ID": "9002", "Impression": "300"},
    ]
    bw = _bw_with_csv_rows(rows)
    out = _fetch_imps_by_li(bw, ["9001", "9002"], "yesterday", "Test")
    assert out["9001"] == 2000  # summed across rows
    assert out["9002"] == 300


def test_pacing_imps_with_empty_report(fake_run):
    bw = _bw_with_csv_rows([])
    out = _fetch_imps_by_li(bw, ["9001"], "yesterday", "Test")
    assert out == {}


# ── _normalize_atr (already tested at integration level, but lock it down) ──


def test_normalize_atr_with_title_case():
    df = pd.DataFrame([{
        "Line Item ID": "9001", "Deal ID": "tri/abc",
        "Deal Alternative ID": "RON-Pub-SSP-Fixed$4-Podcast-ALL-Cat",
        "Deal Name": "Test", "Impression": "1000",
        "Media Spend USD": "5.00", "CPM USD": "5.00",
        "Bid Shading Fee USD": "0.00",
    }])
    out = _normalize_atr(df)
    expected_snake = {
        "line_item_id", "deal_id", "alternative_id", "name",
        "impression", "media_spend_usd", "cpm_usd", "bid_shading_fee_usd",
    }
    assert expected_snake.issubset(set(out.columns))
    assert out.iloc[0]["impression"] == 1000.0
    assert out.iloc[0]["media_spend_usd"] == 5.00
