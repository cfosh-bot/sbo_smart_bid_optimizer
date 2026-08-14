"""Multiplier engine tests — one synthetic case per priority branch.

Each test builds a minimal Bid Optimizer DataFrame + state inputs and
asserts the engine produces the expected reason_code and a multiplier
in the expected range. The math is verified at branch boundaries.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from sbo.config_models import load_config
from sbo.multiplier_engine import (
    Decision,
    EngineResult,
    _category_max_bid,
    _price_tier_mod,
    _RowContext,
    decide_multipliers,
)


@pytest.fixture
def cfg():
    return load_config("sbo/config/podcast.yaml")


def _opt_row(**overrides) -> dict:
    """Minimal Bid Optimizer row with sensible defaults."""
    base = {
        "SF_Line_Item_ID": "100", "BW_Line_Item_ID": "9001",
        "Line_Item_Name": "Test", "Bid_Modifier_ID": "555",
        "Deal_ID": "tri/abc", "CPM_Bid": "10.00", "Floor_Price": "5.00",
        "Deal_Clearing_CPM_On_LI": "6.00", "Deal_Global_Clearing_CPM": "5.50",
        "Last_3_Days_Clearing_CPM": "6.00",
        "Pub_Impression_Share_Pct": "50", "Pub_Clearing_CPM_On_LI": "5.50",
        "Pub_Global_Clearing_CPM": "5.50", "SSP_Impression_Share_Pct": "100",
        "SSP_Clearing_CPM_On_LI": "5.50", "SSP_Global_Clearing_CPM": "5.50",
        "Modifier_Deal_List": "Spreaker",
        "Modifier_Impression_Share_Pct": "100",
        "Modifier_Clearing_CPM_On_LI": "5.50", "Modifier_Global_Clearing_CPM": "5.50",
        "End_Date": "2026-12-31", "Days_Remaining": 30,
        "Pacing_Pct": 1.00, "Daily_Imps_Target": "10000",
        "Pacing_Last_Updated": "2026-04-27 03:00",
        "Current_Multiplier": "1.20",
        "Calculated_New_Multiplier": "", "Effective_Bid_Current": "",
        "Effective_Bid_New": "", "Decision_Reason": "",
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
        "run_date": date(2026, 4, 28),  # Tuesday — avoid weekend window
    }


# ── helper functions ──────────────────────────────────────────────────────


def test_price_tier_mod_endpoints(cfg):
    """0.0 = cheapest, 1.0 = most expensive."""
    # Down direction: cheapest gets softest decrease (smallest mod)
    assert _price_tier_mod(0.0, "down", cfg) == cfg.price_tier.down_min  # 0.5
    assert _price_tier_mod(1.0, "down", cfg) == cfg.price_tier.down_max  # 1.5
    # Up normal: cheapest gets biggest increase
    assert _price_tier_mod(0.0, "up_normal", cfg) == cfg.price_tier.up_norm_max  # 1.3
    assert _price_tier_mod(1.0, "up_normal", cfg) == cfg.price_tier.up_norm_min  # 0.4


def test_category_max_bid_interpolation(cfg):
    """Spreaker: $11 at 100%, $14 at 0%, linear between."""
    df = pd.DataFrame([_opt_row(**{"Modifier_Deal_List": "Spreaker", "Pacing_Pct": 0.50})])
    ctx = _RowContext.from_row(
        df.iloc[0], cfg, price_ranks={}, history_map={},
        zero_delivery=set(), paused_active={}, is_weekend=False,
    )
    # 50% pacing → 14 + 0.5*(11-14) = 14 - 1.5 = 12.50
    assert _category_max_bid(ctx, is_eoc_under=False) == 12.50


def test_category_max_bid_eoc_bonus(cfg):
    """EOC underpacing adds +$1 bonus interpolated by remaining pacing gap."""
    df = pd.DataFrame([_opt_row(**{"Modifier_Deal_List": "Spreaker", "Pacing_Pct": 0.50, "Days_Remaining": 2})])
    ctx = _RowContext.from_row(
        df.iloc[0], cfg, price_ranks={}, history_map={},
        zero_delivery=set(), paused_active={}, is_weekend=False,
    )
    # 50% pacing, EOC: 14 + (1-0.5)*1.00 = 14.50
    assert _category_max_bid(ctx, is_eoc_under=True) == 14.50


# ── priority branches ────────────────────────────────────────────────────


def test_first_run_normal(cfg):
    """Day 1, never seen before → floor × 1.2 / cpm_bid."""
    df = pd.DataFrame([_opt_row()])
    s = _empty_state()
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "FIRST_RUN"
    # 5.00 × 1.2 / 10.00 = 0.6 → clamped by category cap (Spreaker @100% = $11 → mult 1.1)
    assert d.new_multiplier == 0.6
    assert ("9001", "2026-12-31") in result.new_first_run


def test_first_run_eoc_underpacing_skips_day2(cfg):
    """≤3 days remaining + underpacing on Day 1 → both Day 1 and Day 2 logged."""
    df = pd.DataFrame([_opt_row(**{"Days_Remaining": 2, "Pacing_Pct": 0.85})])
    s = _empty_state()
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "FIRST_RUN_EOC_UNDER"
    assert ("9001", "2026-12-31") in result.new_first_run
    assert ("9001", "2026-12-31") in result.new_second_run  # Day 2 also marked


def test_day2_baseline_uses_deal_cpm(cfg):
    """Day 2 (firstRunSeen, not secondRunSeen) → dealCpmLi × 1.2 / cpm_bid."""
    df = pd.DataFrame([_opt_row()])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "DAY2_BASELINE"
    # 6.00 × 1.2 / 10.00 = 0.72
    assert d.new_multiplier == 0.72


def test_last_3_days_hold_when_on_pace(cfg):
    """≤3 days + ≥100% pacing → hold current multiplier (when under category cap)."""
    df = pd.DataFrame([_opt_row(**{
        "Days_Remaining": 2, "Pacing_Pct": 1.05, "Current_Multiplier": "1.00",
    })])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "LAST_3_DAYS_HOLD"
    assert d.new_multiplier == 1.0  # held at current


def test_last_3_days_under_uses_sliding_scale(cfg):
    """≤3 days + underpacing → EOC sliding scale jump."""
    df = pd.DataFrame([_opt_row(**{"Days_Remaining": 2, "Pacing_Pct": 0.50})])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "LAST_3_DAYS_UNDER"
    # EOC ceiling: 14 + 0.5*1.00 = 14.50; mult = 14.50/10 = 1.45
    assert d.new_multiplier == 1.45


def test_priority_mode_throttles_other(cfg):
    """4 consecutive OVER history + Other category → throttle to 1.10× floor."""
    df = pd.DataFrame([_opt_row(**{"Modifier_Deal_List": "Other", "Pacing_Pct": 1.10})])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    s["pacing_history"] = pd.DataFrame([{
        "BW_Line_Item_ID": "9001",
        "2026-04-24": "OVER", "2026-04-25": "OVER",
        "2026-04-26": "OVER", "2026-04-27": "OVER",
    }])
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "PRIORITY_MODE_THROTTLE_OTHER"
    # 5.00 × 1.10 / 10.00 = 0.55
    assert d.new_multiplier == 0.55


def test_other_category_pace_down_at_high_pace(cfg):
    """Other + ≥105% (no priority mode) → normal PACE_DOWN like any other category."""
    df = pd.DataFrame([_opt_row(**{"Modifier_Deal_List": "Other", "Pacing_Pct": 1.10})])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "PACE_DOWN_MOD"
    assert d.new_multiplier < 1.20  # decreased, not killed


def test_pace_hold_on_target_band(cfg):
    """100–105% pacing → hold (when under category cap)."""
    df = pd.DataFrame([_opt_row(**{"Pacing_Pct": 1.03, "Current_Multiplier": "1.00"})])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "PACE_HOLD_ONTARGET"
    assert d.new_multiplier == 1.0


def test_pace_down_aggressive_above_115(cfg):
    """>115% pacing → aggressive down step."""
    df = pd.DataFrame([_opt_row(**{"Pacing_Pct": 1.20, "Days_Remaining": 30})])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "PACE_DOWN_AGG"
    assert d.new_multiplier < 1.20  # decreased


def test_pace_up_critical_below_75(cfg):
    """<75% pacing → critical up step (severe tier mod)."""
    df = pd.DataFrame([_opt_row(**{"Pacing_Pct": 0.50, "Days_Remaining": 30})])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "PACE_UP_CRITICAL"
    assert d.new_multiplier > 1.20  # increased


def test_weekend_guard_blocks_underpacing_increase(cfg):
    """Sun/Mon EST + underpacing + >3 days → hold (no increase from current)."""
    df = pd.DataFrame([_opt_row(**{
        "Pacing_Pct": 0.85, "Days_Remaining": 30, "Current_Multiplier": "1.00",
    })])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}
    s["second_run_seen"] = {"9001"}
    s["run_date"] = date(2026, 4, 26)  # Sunday
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "PACE_HOLD_WEEKEND"
    # Held at current — should not have increased despite underpacing
    assert d.new_multiplier == 1.0


def test_zero_delivery_creates_pause(cfg):
    """zero_delivery set membership → LINE_PAUSED + pause_snapshots entry."""
    df = pd.DataFrame([_opt_row()])
    s = _empty_state()
    s["zero_delivery"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "LINE_PAUSED"
    assert len(result.pause_snapshots) == 1
    assert ("9001", "2026-12-31") in result.new_pauses


def test_paused_holding_when_still_zero(cfg):
    """already paused + still zero delivery → LINE_PAUSED_HOLDING."""
    df = pd.DataFrame([_opt_row()])
    s = _empty_state()
    s["zero_delivery"] = {"9001"}
    s["paused_active"] = {"9001": 5}  # row 5 in paused log
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "LINE_PAUSED_HOLDING"
    assert d.new_multiplier == 1.20  # held


def test_resumed_when_delivery_returns(cfg):
    """already paused but delivery returned → LINE_RESUMED."""
    df = pd.DataFrame([_opt_row()])
    s = _empty_state()
    s["zero_delivery"] = set()  # NOT zero
    s["paused_active"] = {"9001": 5}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "LINE_RESUMED"


def test_pre_flight_hold_for_never_delivered_line(cfg):
    """Day-1-seen + zero delivery + ~0% pacing + >3d → PRE_FLIGHT_HOLD."""
    df = pd.DataFrame([_opt_row(**{"Pacing_Pct": 0.0001, "Days_Remaining": 30})])
    s = _empty_state()
    s["first_run_seen"] = {"9001"}  # has been seen
    s["zero_delivery"] = {"9001"}
    result = decide_multipliers(df, cfg=cfg, **s)
    d = result.decisions[0]
    assert d.reason_code == "PRE_FLIGHT_HOLD"
    # First run log entry should be reset (so line re-baselines on real delivery)
    assert "9001" in result.pre_flight_resets
