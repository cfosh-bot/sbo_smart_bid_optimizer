"""Tests for state_import — Apps Script tab → Parquet state migration.

Uses fake worksheets (no Google Sheets / OAuth) to verify the parsers work
end-to-end. Critical tests:
    - first/second run logs preserve End_Date so prune still works
    - paused_log preserves both Paused_Date and Resumed_Date
    - pacing_history is parsed in wide format (date columns)
    - clean_id strips '.0' from numeric IDs
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from sbo.state import StateStore
from sbo.state_import import _read_pacing_history, _read_tab_to_df


def _ws(values: list[list[str]]):
    """Stand-in gspread Worksheet — only the get_all_values method is used."""
    ws = MagicMock()
    ws.get_all_values.return_value = values
    return ws


# ── _read_tab_to_df ──────────────────────────────────────────────────────


def test_read_first_run_log():
    ws = _ws([
        ["BW_Line_Item_ID", "End_Date", "First_Run_Date"],
        ["9001", "2026-12-31", "2026-04-15"],
        ["9002", "2026-06-30", "2026-04-20"],
    ])
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    assert len(df) == 2
    assert list(df["BW_Line_Item_ID"]) == ["9001", "9002"]
    assert df.iloc[0]["End_Date"] == "2026-12-31"


def test_read_handles_float_ids():
    """Apps Script sometimes serializes IDs as 9001.0 — clean_id fixes."""
    ws = _ws([
        ["BW_Line_Item_ID", "End_Date", "First_Run_Date"],
        ["9001.0", "2026-12-31", "2026-04-15"],
    ])
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    assert df.iloc[0]["BW_Line_Item_ID"] == "9001"


def test_read_handles_lower_case_headers():
    ws = _ws([
        ["bw_line_item_id", "end_date", "first_run_date"],
        ["9001", "2026-12-31", "2026-04-15"],
    ])
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    assert df.iloc[0]["BW_Line_Item_ID"] == "9001"


def test_read_skips_empty_rows():
    ws = _ws([
        ["BW_Line_Item_ID", "End_Date", "First_Run_Date"],
        ["9001", "2026-12-31", "2026-04-15"],
        ["", "", ""],
        ["9002", "", "2026-04-20"],
    ])
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    assert len(df) == 2


def test_read_paused_log_preserves_resume_status():
    ws = _ws([
        ["BW_Line_Item_ID", "End_Date", "Paused_Date", "Resumed_Date"],
        ["9001", "2026-12-31", "2026-04-15", "2026-04-18"],   # resumed
        ["9002", "2026-12-31", "2026-04-20", ""],             # still paused
    ])
    df = _read_tab_to_df(
        ws, ["BW_Line_Item_ID", "End_Date", "Paused_Date", "Resumed_Date"],
    )
    assert df.iloc[0]["Resumed_Date"] == "2026-04-18"
    assert df.iloc[1]["Resumed_Date"] == ""


def test_read_handles_extra_source_columns():
    """If source has extra columns we don't care about, ignore them."""
    ws = _ws([
        ["BW_Line_Item_ID", "End_Date", "First_Run_Date", "ExtraDebugCol"],
        ["9001", "2026-12-31", "2026-04-15", "ignore me"],
    ])
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    assert "ExtraDebugCol" not in df.columns
    assert df.iloc[0]["BW_Line_Item_ID"] == "9001"


def test_read_handles_missing_source_column():
    """If source is missing a column we expected, fill with empty string."""
    ws = _ws([
        ["BW_Line_Item_ID", "End_Date"],  # no First_Run_Date
        ["9001", "2026-12-31"],
    ])
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    assert df.iloc[0]["First_Run_Date"] == ""


def test_read_empty_sheet():
    ws = _ws([["BW_Line_Item_ID", "End_Date", "First_Run_Date"]])  # headers only
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    assert df.empty


# ── _read_pacing_history (wide format) ───────────────────────────────────


def test_read_pacing_history_basic():
    ws = _ws([
        ["BW_Line_Item_ID", "2026-04-24", "2026-04-25", "2026-04-26", "2026-04-27"],
        ["9001", "OVER", "OVER", "OVER", "OVER"],
        ["9002", "UNDER", "UNDER", "OVER", ""],
    ])
    df = _read_pacing_history(ws)
    assert len(df) == 2
    assert list(df.columns) == [
        "BW_Line_Item_ID", "2026-04-24", "2026-04-25", "2026-04-26", "2026-04-27",
    ]
    row_9001 = df[df["BW_Line_Item_ID"] == "9001"].iloc[0]
    assert row_9001["2026-04-27"] == "OVER"
    row_9002 = df[df["BW_Line_Item_ID"] == "9002"].iloc[0]
    assert row_9002["2026-04-27"] == ""  # empty signal preserved as ""


def test_read_pacing_history_filters_invalid_signals():
    """Only OVER/UNDER are kept — other junk becomes ''."""
    ws = _ws([
        ["BW_Line_Item_ID", "2026-04-27"],
        ["9001", "OVER"],
        ["9002", "weird-signal"],
        ["9003", "PACE_DOWN_MOD"],   # legacy code, should be normalized to ""
    ])
    df = _read_pacing_history(ws)
    assert df[df["BW_Line_Item_ID"] == "9001"].iloc[0]["2026-04-27"] == "OVER"
    assert df[df["BW_Line_Item_ID"] == "9002"].iloc[0]["2026-04-27"] == ""
    assert df[df["BW_Line_Item_ID"] == "9003"].iloc[0]["2026-04-27"] == ""


def test_read_pacing_history_unfamiliar_layout():
    """If the first column isn't BW_Line_Item_ID, refuse rather than guess."""
    ws = _ws([
        ["SomethingElse", "2026-04-27"],
        ["9001", "OVER"],
    ])
    df = _read_pacing_history(ws)
    assert df.empty


# ── Round-trip through StateStore ────────────────────────────────────────


def test_imported_first_run_log_loads_back_through_state(tmp_path):
    """Critical: imported data must round-trip through StateStore.load()."""
    state = StateStore(tmp_path / "state")
    ws = _ws([
        ["BW_Line_Item_ID", "End_Date", "First_Run_Date"],
        ["9001", "2026-12-31", "2026-04-15"],
        ["9002", "2026-06-30", "2026-04-20"],
    ])
    df = _read_tab_to_df(ws, ["BW_Line_Item_ID", "End_Date", "First_Run_Date"])
    state.save("first_run_log", df)
    loaded = state.load("first_run_log")
    # Engine reads BW_Line_Item_ID into a set — verify that's possible
    seen = set(loaded["BW_Line_Item_ID"].astype(str))
    assert seen == {"9001", "9002"}


def test_imported_pacing_history_round_trips(tmp_path):
    state = StateStore(tmp_path / "state")
    ws = _ws([
        ["BW_Line_Item_ID", "2026-04-26", "2026-04-27"],
        ["9001", "OVER", "OVER"],
    ])
    df = _read_pacing_history(ws)
    state.save_pacing_history(df)
    loaded = state.load_pacing_history(max_runs=4)
    assert "2026-04-27" in loaded.columns
    assert loaded.iloc[0]["2026-04-27"] == "OVER"
