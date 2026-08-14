"""Tests for state_apply — the critical glue between engine and persistent state.

Without these working correctly, the engine produces correct decisions for
one run but loses all state on the next run (every line Day-1-baselines
forever, pacing trends never accumulate, paused lines stay paused).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from sbo.multiplier_engine import EngineResult
from sbo.state import StateStore
from sbo.state_apply import (
    apply_engine_state,
    pacing_history_update,
    prune_state_at_run_start,
)


@pytest.fixture
def state(tmp_path):
    return StateStore(tmp_path / "state")


@pytest.fixture
def now():
    return datetime(2026, 4, 28, 3, 0, 0)


def test_first_run_log_appended(state, now):
    """new_first_run entries get persisted across runs."""
    result = EngineResult(new_first_run=[("9001", "2026-12-31"), ("9002", "")])
    counts = apply_engine_state(state, result, now)
    assert counts["first_run_added"] == 2
    df = state.load("first_run_log")
    assert set(df["BW_Line_Item_ID"].astype(str)) == {"9001", "9002"}


def test_first_run_log_appends_not_overwrites(state, now):
    """Multiple runs accumulate entries, not replace them."""
    apply_engine_state(state, EngineResult(new_first_run=[("9001", "")]), now)
    apply_engine_state(state, EngineResult(new_first_run=[("9002", "")]), now + timedelta(days=1))
    df = state.load("first_run_log")
    assert set(df["BW_Line_Item_ID"].astype(str)) == {"9001", "9002"}


def test_pre_flight_resets_remove_run_log_entries(state, now):
    """Pre-flight reset clears prior Day-1/Day-2 entries so line re-baselines."""
    apply_engine_state(
        state,
        EngineResult(new_first_run=[("9001", ""), ("9002", "")],
                     new_second_run=[("9001", "")]),
        now,
    )
    counts = apply_engine_state(
        state, EngineResult(pre_flight_resets=["9001"]), now,
    )
    assert counts["pre_flight_resets"] == 1
    first_df = state.load("first_run_log")
    second_df = state.load("second_run_log")
    assert "9001" not in set(first_df["BW_Line_Item_ID"].astype(str))
    assert "9002" in set(first_df["BW_Line_Item_ID"].astype(str))  # untouched
    assert "9001" not in set(second_df["BW_Line_Item_ID"].astype(str))


def test_new_pauses_appended_with_empty_resumed_date(state, now):
    """New pauses leave Resumed_Date empty so they're 'actively paused'."""
    result = EngineResult(new_pauses=[("9001", "2026-12-31")])
    apply_engine_state(state, result, now)
    df = state.load("paused_log")
    assert len(df) == 1
    assert df.iloc[0]["BW_Line_Item_ID"] == "9001"
    # Resumed_Date should be empty so paused_active picks it up next run
    assert df.iloc[0]["Resumed_Date"] == ""


def test_resumed_lines_get_resume_date(state, now):
    """resumed_row_indices writes Resumed_Date back into paused_log."""
    # First run: pause two lines
    apply_engine_state(
        state,
        EngineResult(new_pauses=[("9001", ""), ("9002", "")]),
        now,
    )
    paused = state.load("paused_log")
    # Second run: line at index 0 resumes
    target_idx = paused.index[0]
    apply_engine_state(
        state,
        EngineResult(resumed_row_indices=[target_idx]),
        now + timedelta(days=2),
    )
    df = state.load("paused_log")
    # That row should now have a Resumed_Date
    assert df.loc[target_idx, "Resumed_Date"] != ""
    # Other row still active
    other_idx = [i for i in df.index if i != target_idx][0]
    assert df.loc[other_idx, "Resumed_Date"] == ""


def test_kill_log_appended(state, now):
    """Kill log entries persist across runs."""
    result = EngineResult(kill_log_entries=[
        ["9001", "DEAL_KILL", now, "tri/abc [Other - ≥105%]", "", "2026-12-31"],
    ])
    counts = apply_engine_state(state, result, now)
    assert counts["kill_log_rows"] == 1
    df = state.load("kill_log")
    assert df.iloc[0]["Action"] == "DEAL_KILL"


def test_pacing_history_first_run_creates_wide_format(state, now):
    """First call seeds the wide-format pacing history."""
    pacing_history_update(state, {"9001": "OVER", "9002": "UNDER"}, now)
    df = state.load_pacing_history()
    assert "BW_Line_Item_ID" in df.columns
    today_col = now.strftime("%Y-%m-%d")
    assert today_col in df.columns
    assert df.set_index("BW_Line_Item_ID").at["9001", today_col] == "OVER"


def test_pacing_history_keeps_max_runs_columns(state, now):
    """Wide format trims to last N date columns."""
    for i in range(6):
        pacing_history_update(
            state, {"9001": "OVER"}, now + timedelta(days=i), max_runs=4,
        )
    df = state.load_pacing_history()
    date_cols = [c for c in df.columns if c != "BW_Line_Item_ID"]
    assert len(date_cols) <= 4


def test_pacing_history_handles_new_li_in_later_run(state, now):
    """Adding a new LI on a later run doesn't break alignment."""
    pacing_history_update(state, {"9001": "OVER"}, now)
    pacing_history_update(
        state, {"9001": "UNDER", "9002": "OVER"}, now + timedelta(days=1),
    )
    df = state.load_pacing_history()
    df = df.set_index("BW_Line_Item_ID")
    today_col = now.strftime("%Y-%m-%d")
    tmrw_col = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    assert df.at["9001", today_col] == "OVER"
    assert df.at["9001", tmrw_col] == "UNDER"
    assert df.at["9002", today_col] == ""  # didn't exist on day 1
    assert df.at["9002", tmrw_col] == "OVER"


def test_prune_drops_expired_run_log_entries(state, now):
    """prune_expired_run_logs removes entries past their End_Date."""
    apply_engine_state(
        state,
        EngineResult(new_first_run=[
            ("9001", "2025-01-01"),  # in the past — should be pruned
            ("9002", "2027-01-01"),  # future — kept
            ("9003", ""),            # no end date — kept
        ]),
        now,
    )
    pruned = prune_state_at_run_start(state, now)
    assert pruned["first_pruned"] == 1
    df = state.load("first_run_log")
    surviving = set(df["BW_Line_Item_ID"].astype(str))
    assert "9001" not in surviving
    assert {"9002", "9003"}.issubset(surviving)


def test_empty_engine_result_is_safe_noop(state, now):
    """Calling with an empty EngineResult should not raise or write anything."""
    counts = apply_engine_state(state, EngineResult(), now)
    assert all(v == 0 for v in counts.values())
    # All state files should be empty
    for name in ("first_run_log", "second_run_log", "paused_log", "kill_log"):
        assert state.load(name).empty
