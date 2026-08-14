"""Select CTV multiplier engine tests — one synthetic case per priority
branch, mirroring the style of test_multiplier_engine.py.

Source of truth: the Select CTV Apps Script, 2026-08-14 revision. Formulas
are verified at branch boundaries, including the on-target margin-health
trim added 2026-08-14 (PACE_HOLD_MARGIN_TRIM), which has no prior test
coverage anywhere else.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from sbo.config_models import load_config
from sbo.multiplier_engine_select_ctv import decide_multipliers_select_ctv


@pytest.fixture
def cfg():
    return load_config("sbo/config/select_ctv.yaml")


def _opt_row(**overrides) -> dict:
    """Minimal Select CTV Bid Optimizer row with sensible defaults.

    Floor=$5, CPM bid=$10 => base_mult = min(5*1.20/10, 1.20) = 0.60,
    norm_min = max(5*1.05/10, 0.01) = 0.525, throttle = 5*1.00/10 = 0.50,
    kill = 5*0.90/10 = 0.45.
    """
    base = {
        "SF_Line_Item_ID": "100", "BW_Line_Item_ID": "9001",
        "Line_Item_Name": "Test", "Bid_Modifier_ID": "555",
        "Publisher": "TestPub", "Deal_ID": "12345",
        "CPM_Bid": "10.00", "Floor_Price": "5.00",
        "Deal_Clearing_CPM_On_LI": "0", "Last_3_Days_Clearing_CPM": "0",
        "Pub_Impression_Share_Pct": "10", "Pub_Clearing_CPM_On_LI": "0",
        "Pub_Global_Clearing_CPM": "0",
        "End_Date": "2026-12-31", "Days_Remaining": 30,
        "Pacing_Pct": 1.00, "Daily_Imps_Target": "10000",
        "Pacing_Last_Updated": "2026-08-14 03:00",
        "Current_Multiplier": "1.20",
        "Calculated_New_Multiplier": "", "Effective_Bid_Current": "",
        "Effective_Bid_New": "", "Decision_Reason": "", "Update_Status": "",
    }
    base.update(overrides)
    return base


def _empty_state():
    return {
        "pacing_history": pd.DataFrame(columns=["BW_Line_Item_ID"]),
        "first_run_seen": set(),
        "second_run_seen": set(),
        "paused_active": {},
        "zero_delivery": set(),
        "run_date": date(2026, 8, 14),
    }


def _decide(cfg, row, **state_overrides):
    df = pd.DataFrame([row])
    s = _empty_state()
    s.update(state_overrides)
    result = decide_multipliers_select_ctv(df, cfg=cfg, **s)
    return result, result.decisions[0]


# ── Day 1 / Day 2 baselines ─────────────────────────────────────────────


def test_first_run_normal(cfg):
    """Day 1, >3 days remaining, never seen before -> floor x 1.20 / cpm_bid."""
    result, d = _decide(cfg, _opt_row())
    assert d.reason_code == "FIRST_RUN"
    assert d.new_multiplier == pytest.approx(0.60, abs=0.001)
    assert ("9001", "2026-12-31") in result.new_first_run


def test_first_run_short(cfg):
    """Day 1, <=3 days remaining -> flat 1.0x, both Day1+Day2 marked done."""
    result, d = _decide(cfg, _opt_row(Days_Remaining=2))
    assert d.reason_code == "FIRST_RUN_SHORT"
    assert d.new_multiplier == 1.0
    assert ("9001", "2026-12-31") in result.new_first_run
    assert ("9001", "2026-12-31") in result.new_second_run


def test_day2_baseline_uses_pub_cpm(cfg):
    """Day 2, Pub_Clearing_CPM_On_LI known -> pubCpm x 1.20 / cpm_bid."""
    row = _opt_row(Pub_Clearing_CPM_On_LI="5.50")
    result, d = _decide(cfg, row, first_run_seen={"9001"})
    assert d.reason_code == "DAY2_BASELINE"
    assert d.new_multiplier == pytest.approx((5.50 * 1.20) / 10.0, abs=0.001)


def test_day2_baseline_fallback(cfg):
    """Day 2, no pub clearing CPM -> falls back to floor baseline."""
    result, d = _decide(cfg, _opt_row(), first_run_seen={"9001"})
    assert d.reason_code == "DAY2_BASELINE_FALLBACK"
    assert d.new_multiplier == pytest.approx(0.60, abs=0.001)


# ── Last 3 days ──────────────────────────────────────────────────────────


def test_last_3_days_hold_on_pace(cfg):
    row = _opt_row(Days_Remaining=2, Pacing_Pct=1.00)
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "LAST_3_DAYS_HOLD"
    assert d.new_multiplier == pytest.approx(1.20, abs=0.001)


def test_last_3_days_under_raises(cfg):
    row = _opt_row(Days_Remaining=2, Pacing_Pct=0.80, Current_Multiplier="1.20")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "LAST_3_DAYS_UNDER"
    # base_up 0.20 * eoc_boost 1.20 * trend 1.0 * tier(0.5,'up_normal')=0.85 = 0.204
    assert d.new_multiplier == pytest.approx(1.20 + 0.204, abs=0.002)


# ── Publisher cap ────────────────────────────────────────────────────────


def test_cap_kill_over_40_pct(cfg):
    row = _opt_row(Pub_Impression_Share_Pct="45", Pacing_Pct=1.00, Days_Remaining=30)
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "CAP_KILL"
    assert d.new_multiplier == pytest.approx(0.45, abs=0.001)  # floor*0.90/cpm


def test_cap_kill_suppressed_when_underpacing_near_eoc(cfg):
    """Publisher over cap but underpacing within 7 days -> cap doesn't fire."""
    row = _opt_row(
        Pub_Impression_Share_Pct="45", Pacing_Pct=0.80, Days_Remaining=5,
        Current_Multiplier="1.00",
    )
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code != "CAP_KILL"


def test_cap_throttle_approaching_cap(cfg):
    row = _opt_row(Pub_Impression_Share_Pct="35", Pacing_Pct=1.00, Days_Remaining=30,
                    Current_Multiplier="1.20")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "CAP_THROTTLE"
    # cap_prog = (0.35-0.32)/(0.40-0.32) = 0.375; throttle=0.50
    expected = max(0.50, 1.20 - (1.20 - 0.50) * 0.375)
    assert d.new_multiplier == pytest.approx(expected, abs=0.002)


# ── Normal pacing: down ──────────────────────────────────────────────────


def test_pace_down_dampened_within_7_days(cfg):
    row = _opt_row(Pacing_Pct=1.20, Days_Remaining=5, Current_Multiplier="1.20")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_DOWN_AGG"
    # base_down 0.15 * tier(0.5,'down')=1.0 * 0.5 dampener = 0.075
    assert d.new_multiplier == pytest.approx(1.20 - 0.075, abs=0.002)


def test_pace_down_no_dampener_beyond_7_days(cfg):
    row = _opt_row(Pacing_Pct=1.30, Days_Remaining=30, Current_Multiplier="1.20")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_DOWN_AGG"
    # base_down 0.20 * tier 1.0, no dampener = 0.20
    assert d.new_multiplier == pytest.approx(1.00, abs=0.002)


# ── On-target margin-health trim (added 2026-08-14) ──────────────────────


def test_pace_hold_ontarget_no_cpm_data(cfg):
    """>7d, 100-105% pacing, no Deal/L3 CPM data -> plain hold, no trim."""
    row = _opt_row(Pacing_Pct=1.02, Days_Remaining=30, Current_Multiplier="0.80")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_HOLD_ONTARGET"
    assert d.new_multiplier == pytest.approx(0.80, abs=0.001)


def test_pace_hold_margin_trim_thin_margin(cfg):
    """Margin < 6% -> faster 0.05 trim step."""
    # margin = (10 - 9.7) / 10 = 3% < 6%
    row = _opt_row(
        Pacing_Pct=1.02, Days_Remaining=30, Current_Multiplier="0.80",
        Deal_Clearing_CPM_On_LI="9.70",
    )
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_HOLD_MARGIN_TRIM"
    assert d.new_multiplier == pytest.approx(0.80 - 0.05, abs=0.001)


def test_pace_hold_margin_trim_healthy_margin(cfg):
    """Margin >= 6% -> slower 0.025 trim step."""
    # margin = (10 - 9.0) / 10 = 10% >= 6%
    row = _opt_row(
        Pacing_Pct=1.02, Days_Remaining=30, Current_Multiplier="0.80",
        Deal_Clearing_CPM_On_LI="9.00",
    )
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_HOLD_MARGIN_TRIM"
    assert d.new_multiplier == pytest.approx(0.80 - 0.025, abs=0.001)


def test_pace_hold_margin_trim_falls_back_to_l3_cpm(cfg):
    """Deal_Clearing_CPM_On_LI unavailable -> falls back to Last_3_Days_Clearing_CPM."""
    row = _opt_row(
        Pacing_Pct=1.02, Days_Remaining=30, Current_Multiplier="0.80",
        Deal_Clearing_CPM_On_LI="0", Last_3_Days_Clearing_CPM="9.00",
    )
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_HOLD_MARGIN_TRIM"
    assert d.new_multiplier == pytest.approx(0.80 - 0.025, abs=0.001)


def test_pace_hold_ontarget_already_at_floor(cfg):
    """Margin trim would go below norm_min -> hold instead, no trim."""
    # norm_min = 0.525; curr_mult 0.50 is already below it
    row = _opt_row(
        Pacing_Pct=1.02, Days_Remaining=30, Current_Multiplier="0.50",
        Deal_Clearing_CPM_On_LI="9.70",
    )
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_HOLD_ONTARGET"
    assert d.new_multiplier == pytest.approx(0.50, abs=0.001)


# ── Normal pacing: up ────────────────────────────────────────────────────


def test_pace_up_normal_beyond_14_days(cfg):
    row = _opt_row(Pacing_Pct=0.85, Days_Remaining=30, Current_Multiplier="0.60")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_UP_AGG"
    # base_up 0.20 * tier(0.5,'up_normal')=0.85 = 0.17
    assert d.new_multiplier == pytest.approx(0.60 + 0.17, abs=0.002)


def test_pace_up_severe_kill_override_within_7_days(cfg):
    """Severe underpacing within 7 days -> effective floor drops to ~0, hard max = severe (1.50)."""
    row = _opt_row(Pacing_Pct=0.50, Days_Remaining=5, Current_Multiplier="0.10")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "PACE_UP_CRITICAL"
    # base_up 0.30 * eoc_boost 1.30 * tier(0.5,'up_severe')=1.0 = 0.39
    assert d.new_multiplier == pytest.approx(0.10 + 0.39, abs=0.002)


def test_no_pacing_holds_current(cfg):
    row = _opt_row(Pacing_Pct="", Current_Multiplier="0.77")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "NO_PACING"
    assert d.new_multiplier == pytest.approx(0.77, abs=0.001)


# ── Pause / resume / pre-flight (Step 0) ─────────────────────────────────


def test_pre_flight_hold(cfg):
    row = _opt_row(Pacing_Pct=0.0, Days_Remaining="", Pub_Global_Clearing_CPM="5.50")
    result, d = _decide(cfg, row, zero_delivery={"9001"})
    assert d.reason_code == "PRE_FLIGHT_HOLD"
    assert d.new_multiplier == pytest.approx((5.50 * 1.20) / 10.0, abs=0.001)


def test_line_paused_holding_is_frozen(cfg):
    """Still paused, still zero -> frozen at curr_mult, not recomputed."""
    row = _opt_row(Current_Multiplier="0.77")
    result, d = _decide(cfg, row, paused_active={"9001": 3}, zero_delivery={"9001"})
    assert d.reason_code == "LINE_PAUSED_HOLDING"
    assert d.new_multiplier == pytest.approx(0.77, abs=0.001)


def test_line_paused_newly_detected_holds_unchanged(cfg):
    """Newly zero -> hold at curr_mult unchanged, no floor-protection recompute."""
    row = _opt_row(Current_Multiplier="0.77")
    result, d = _decide(cfg, row, zero_delivery={"9001"})
    assert d.reason_code == "LINE_PAUSED"
    assert d.new_multiplier == pytest.approx(0.77, abs=0.001)
    assert ("9001", "2026-12-31") in result.new_pauses
    assert "9001" in result.pre_flight_resets  # removed from first/second run logs
    assert len(result.pause_snapshots) == 1


def test_resumed_falls_through_to_normal_cascade(cfg):
    """Resumed (paused_active but not zero_delivery) -> no dedicated reason
    code; marks the paused_log row resumed and re-runs the normal cascade
    (Day 1 naturally re-fires since it isn't in first_run_seen)."""
    row = _opt_row()
    result, d = _decide(cfg, row, paused_active={"9001": 7})
    assert result.resumed_row_indices == [7]
    assert d.reason_code == "FIRST_RUN"  # fell through, re-baselined
    assert d.new_multiplier == pytest.approx(0.60, abs=0.001)


# ── Guardrails / final clamp ─────────────────────────────────────────────


def test_no_floor_or_cpm_data(cfg):
    row = _opt_row(Floor_Price="0", CPM_Bid="0")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.reason_code == "NO_FLOOR_CPM_DATA"
    assert d.new_multiplier == 0.0


def test_final_clamp_never_exceeds_hard_max_last3(cfg):
    """Even a huge underpacing raise is capped at 2.00x (OPT_HARD_MAX_LAST3)."""
    row = _opt_row(Days_Remaining=2, Pacing_Pct=0.01, Current_Multiplier="1.90")
    result, d = _decide(cfg, row, first_run_seen={"9001"}, second_run_seen={"9001"})
    assert d.new_multiplier <= 2.00
