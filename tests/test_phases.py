"""Phase 1/2/3 tests using a fake Beeswax client + temp state dir."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from sbo.config_models import load_config
from sbo.phases import (
    DealPricingLookup,
    build_pricing_lookup,
    create_publisher_bid_modifiers,
    patch_line_item_bid_modifiers,
    smart_starting_mult,
    update_bid_modifier_terms,
)
from sbo.state import StateStore


@pytest.fixture
def cfg():
    return load_config("sbo/config/podcast.yaml")


@pytest.fixture
def fake_run(tmp_path):
    run = MagicMock()
    run.path = tmp_path
    (tmp_path / "beeswax_raw").mkdir(exist_ok=True)
    (tmp_path / "01_inputs").mkdir(exist_ok=True)

    def _save_df(name, df):
        out = tmp_path / (name if name.endswith(".parquet") else f"{name}.parquet")
        df.to_parquet(out, index=False)
        return out

    run.save_dataframe.side_effect = _save_df
    run.log.side_effect = lambda msg: None
    return run


@pytest.fixture
def state_store(tmp_path):
    return StateStore(tmp_path / "state")


@pytest.fixture
def fake_bw():
    bw = MagicMock()
    bw.authenticate.return_value = None
    return bw


def _input_df(**rows):
    """Build an input DataFrame with the AM-facing column names."""
    base_cols = [
        "SF LI ID", "BW LI ID", "Advertiser", "Bid Modifier ID",
        "Bid Modifier Created Date", "Bid Modifier Added Date",
        "End Date", "New Line Indicator - Add Yes",
    ]
    return pd.DataFrame([rows], columns=base_cols)


# ── smart_starting_mult ───────────────────────────────────────────────────


def test_smart_mult_o_o_fixed_price():
    """Floor ≤ $0.01 → always 1.0×."""
    mult, src = smart_starting_mult(cpm_bid=10.0, deal_glob_cpm=8.0, deal_floor=0.01, fallback_dollar=11)
    assert mult == 1.0
    assert "0.01" in src


def test_smart_mult_global_cpm_priority():
    """globCpm > 0 → (globCpm × 1.2) / cpm_bid."""
    mult, src = smart_starting_mult(cpm_bid=10.0, deal_glob_cpm=5.0, deal_floor=4.0, fallback_dollar=11)
    # 5.0 × 1.2 / 10.0 = 0.60
    assert mult == 0.60
    assert "globCPM" in src


def test_smart_mult_floor_fallback():
    """No globCpm → floor × 1.3 / cpm_bid."""
    mult, src = smart_starting_mult(cpm_bid=10.0, deal_glob_cpm=0, deal_floor=4.0, fallback_dollar=11)
    # 4.0 × 1.3 / 10.0 = 0.52
    assert mult == 0.52
    assert "floor" in src


def test_smart_mult_dollar_fallback():
    """No pricing data → $11 / cpm_bid."""
    mult, src = smart_starting_mult(cpm_bid=10.0, deal_glob_cpm=0, deal_floor=0, fallback_dollar=11)
    assert mult == 1.10
    assert "fallback" in src


def test_smart_mult_clamped_to_min():
    """Even with weird inputs, multiplier never goes below 0.01."""
    mult, _ = smart_starting_mult(cpm_bid=1000.0, deal_glob_cpm=0.001, deal_floor=0, fallback_dollar=11)
    assert mult >= 0.01


# ── build_pricing_lookup ──────────────────────────────────────────────────


def test_pricing_lookup_extracts_first_nonzero():
    ps = pd.DataFrame([
        {"Line_Item_ID": "9001", "Deal_ID": "tri/abc", "Deal_Global_Clearing_CPM": 5.0,
         "Floor_Price": 4.0, "CPM_Bid": 10.0},
        {"Line_Item_ID": "9001", "Deal_ID": "tri/abc", "Deal_Global_Clearing_CPM": 6.0,
         "Floor_Price": 5.0, "CPM_Bid": 10.0},
    ])
    lookup = build_pricing_lookup(ps)
    assert lookup.glob_cpm["tri/abc"] == 5.0  # first nonzero wins
    assert lookup.floor["tri/abc"] == 4.0
    assert lookup.cpm_bid["9001"] == 10.0


# ── Phase 1: create_publisher_bid_modifiers ───────────────────────────────


def test_phase1_creates_modifier_for_unmapped_line(cfg, fake_run, state_store, fake_bw):
    """Row with no BM ID → fetch LI → expand TE → POST modifier."""
    fake_bw.fetch_line_items.return_value = [{
        "id": 9001, "advertiser_id": 500,
        "targeting_expression_id": 9999,
        "bid_modifier_id": None,
        "bidding": {"values": {"cpm_bid": 10.0}},
    }]
    fake_bw.fetch_targeting_expressions.return_value = [{
        "id": 9999,
        "modules": {"app_site": {"all": {"deal_id_list": {"any": [{"value": 100}]}}}},
    }]
    fake_bw.fetch_all_list_items_by_list_id.return_value = {
        "100": {"tri/abc": True, "tri/xyz": True},
    }
    fake_bw.create_bid_modifier.return_value = {
        "results": [{"id": 8888, "name": "100-9001-Test-Podcast IAN - API Test"}]
    }

    df = _input_df(**{
        "SF LI ID": "100", "BW LI ID": "9001", "Advertiser": "Test Adv",
        "Bid Modifier ID": "", "Bid Modifier Created Date": "",
        "Bid Modifier Added Date": "", "End Date": "2026-12-31",
        "New Line Indicator - Add Yes": "",
    })
    updated, results, summary = create_publisher_bid_modifiers(
        bw=fake_bw, cfg=cfg, run=fake_run, state=state_store,
        input_snapshot=df, publisher_stats=None,
    )
    assert summary.created == 1
    assert summary.errors == 0
    assert updated.iloc[0]["Bid Modifier ID"] == "8888"

    # Inspect the POST payload
    fake_bw.create_bid_modifier.assert_called_once()
    payload = fake_bw.create_bid_modifier.call_args[0][0]
    assert payload["account_id"] == cfg.beeswax.account_id
    assert payload["active"] is True
    assert "Podcast IAN - API Test" in payload["name"]
    assert len(payload["terms"]) == 2
    assert {t["targeting_key"] for t in payload["terms"]} == {"deal_id"}


def test_phase1_skips_when_li_already_has_bm(cfg, fake_run, state_store, fake_bw):
    """If live LI already has bid_modifier_id, just record it and skip."""
    fake_bw.fetch_line_items.return_value = [{
        "id": 9001, "bid_modifier_id": 7777,
        "targeting_expression_id": 9999,
        "bidding": {"values": {"cpm_bid": 10.0}},
    }]
    df = _input_df(**{
        "SF LI ID": "100", "BW LI ID": "9001", "Advertiser": "Test",
        "Bid Modifier ID": "", "Bid Modifier Created Date": "",
        "Bid Modifier Added Date": "", "End Date": "2026-12-31",
        "New Line Indicator - Add Yes": "",
    })
    updated, results, summary = create_publisher_bid_modifiers(
        bw=fake_bw, cfg=cfg, run=fake_run, state=state_store,
        input_snapshot=df, publisher_stats=None,
    )
    assert summary.created == 0
    assert summary.skipped == 1
    assert updated.iloc[0]["Bid Modifier ID"] == "7777"
    fake_bw.create_bid_modifier.assert_not_called()


# ── Phase 2: patch_line_item_bid_modifiers ────────────────────────────────


def test_phase2_patches_li_with_max_bid_2x_cpm(cfg, fake_run, fake_bw):
    """max_bid = min(cpm_bid × 2, 100). min_bid = 0.01."""
    fake_bw.fetch_line_items.return_value = [{
        "id": 9001, "bidding": {"values": {"cpm_bid": 10.0}},
    }]
    fake_bw.patch_line_item.return_value = {"id": 9001}
    df = _input_df(**{
        "SF LI ID": "100", "BW LI ID": "9001", "Advertiser": "Test",
        "Bid Modifier ID": "8888",
        "Bid Modifier Created Date": "", "Bid Modifier Added Date": "",
        "End Date": "2026-12-31", "New Line Indicator - Add Yes": "",
    })
    updated, results, summary = patch_line_item_bid_modifiers(
        bw=fake_bw, cfg=cfg, run=fake_run, input_snapshot=df,
    )
    assert summary.patched == 1
    fake_bw.patch_line_item.assert_called_once()
    args = fake_bw.patch_line_item.call_args
    assert args[0][0] == "9001"
    payload = args[0][1]
    assert payload == {"bid_modifier_id": 8888, "min_bid": 0.01, "max_bid": 20.0}


def test_phase2_caps_max_bid_at_100(cfg, fake_run, fake_bw):
    """Even with very high cpm_bid, max_bid never exceeds $100."""
    fake_bw.fetch_line_items.return_value = [{
        "id": 9001, "bidding": {"values": {"cpm_bid": 75.0}},
    }]
    fake_bw.patch_line_item.return_value = {"id": 9001}
    df = _input_df(**{
        "SF LI ID": "100", "BW LI ID": "9001", "Advertiser": "Test",
        "Bid Modifier ID": "8888", "Bid Modifier Created Date": "",
        "Bid Modifier Added Date": "", "End Date": "", "New Line Indicator - Add Yes": "",
    })
    patch_line_item_bid_modifiers(
        bw=fake_bw, cfg=cfg, run=fake_run, input_snapshot=df,
    )
    payload = fake_bw.patch_line_item.call_args[0][1]
    assert payload["max_bid"] == 100.0  # capped, not 150


# ── Phase 3: update_bid_modifier_terms ────────────────────────────────────


def test_phase3_appends_missing_deals(cfg, fake_run, state_store, fake_bw):
    """Targeted has 3 deals, modifier has 1 → append the 2 missing."""
    fake_bw.fetch_line_items.return_value = [{
        "id": 9001, "bid_modifier_id": 8888,
        "targeting_expression_id": 9999,
        "bidding": {"values": {"cpm_bid": 10.0}},
    }]
    fake_bw.fetch_targeting_expressions.return_value = [{
        "id": 9999,
        "modules": {"app_site": {"all": {"deal_id_list": {"any": [{"value": 100}]}}}},
    }]
    fake_bw.fetch_all_list_items_by_list_id.return_value = {
        "100": {"tri/abc": True, "tri/xyz": True, "tri/new": True},
    }
    fake_bw.get_bid_modifier.return_value = {
        "id": 8888,
        "terms": [
            {"value": "tri/abc", "multiplier": "1.00", "targeting_key": "deal_id"},
        ],
    }
    fake_bw.update_bid_modifier.return_value = {"id": 8888}

    df = _input_df(**{
        "SF LI ID": "100", "BW LI ID": "9001", "Advertiser": "Test",
        "Bid Modifier ID": "8888", "Bid Modifier Created Date": "",
        "Bid Modifier Added Date": "", "End Date": "", "New Line Indicator - Add Yes": "",
    })
    updated, results, summary = update_bid_modifier_terms(
        bw=fake_bw, cfg=cfg, run=fake_run, state=state_store,
        input_snapshot=df, publisher_stats=None,
    )
    assert summary.updated == 1
    fake_bw.update_bid_modifier.assert_called_once()
    payload = fake_bw.update_bid_modifier.call_args[0][1]
    deal_values = {t["value"] for t in payload["terms"]}
    assert deal_values == {"tri/abc", "tri/xyz", "tri/new"}  # 1 existing + 2 added


def test_phase3_migrates_old_list_terms(cfg, fake_run, state_store, fake_bw):
    """Old `deal_id_list` terms present → wipe all + rebuild with deal_id terms."""
    fake_bw.fetch_line_items.return_value = [{
        "id": 9001, "bid_modifier_id": 8888,
        "targeting_expression_id": 9999,
        "bidding": {"values": {"cpm_bid": 10.0}},
    }]
    fake_bw.fetch_targeting_expressions.return_value = [{
        "id": 9999,
        "modules": {"app_site": {"all": {"deal_id_list": {"any": [{"value": 100}]}}}},
    }]
    fake_bw.fetch_all_list_items_by_list_id.return_value = {
        "100": {"tri/abc": True, "tri/xyz": True},
    }
    fake_bw.get_bid_modifier.return_value = {
        "id": 8888,
        "terms": [
            {"value": "100", "multiplier": "1.00", "targeting_key": "deal_id_list"},
        ],
    }
    fake_bw.update_bid_modifier.return_value = {"id": 8888}

    df = _input_df(**{
        "SF LI ID": "100", "BW LI ID": "9001", "Advertiser": "Test",
        "Bid Modifier ID": "8888", "Bid Modifier Created Date": "",
        "Bid Modifier Added Date": "", "End Date": "", "New Line Indicator - Add Yes": "",
    })
    updated, results, summary = update_bid_modifier_terms(
        bw=fake_bw, cfg=cfg, run=fake_run, state=state_store,
        input_snapshot=df, publisher_stats=None,
    )
    assert summary.updated == 1
    payload = fake_bw.update_bid_modifier.call_args[0][1]
    # All terms should be deal_id (no list term left)
    assert all(t["targeting_key"] == "deal_id" for t in payload["terms"])
    deal_values = {t["value"] for t in payload["terms"]}
    assert deal_values == {"tri/abc", "tri/xyz"}
    assert "Migrated" in results.iloc[0]["Status"]


def test_phase3_no_op_when_terms_already_match(cfg, fake_run, state_store, fake_bw):
    """All targeted deals already in modifier → skip PUT."""
    fake_bw.fetch_line_items.return_value = [{
        "id": 9001, "bid_modifier_id": 8888,
        "targeting_expression_id": 9999,
        "bidding": {"values": {"cpm_bid": 10.0}},
    }]
    fake_bw.fetch_targeting_expressions.return_value = [{
        "id": 9999,
        "modules": {"app_site": {"all": {"deal_id_list": {"any": [{"value": 100}]}}}},
    }]
    fake_bw.fetch_all_list_items_by_list_id.return_value = {
        "100": {"tri/abc": True, "tri/xyz": True},
    }
    fake_bw.get_bid_modifier.return_value = {
        "id": 8888,
        "terms": [
            {"value": "tri/abc", "multiplier": "1.00", "targeting_key": "deal_id"},
            {"value": "tri/xyz", "multiplier": "1.00", "targeting_key": "deal_id"},
        ],
    }
    df = _input_df(**{
        "SF LI ID": "100", "BW LI ID": "9001", "Advertiser": "Test",
        "Bid Modifier ID": "8888", "Bid Modifier Created Date": "",
        "Bid Modifier Added Date": "", "End Date": "", "New Line Indicator - Add Yes": "",
    })
    updated, results, summary = update_bid_modifier_terms(
        bw=fake_bw, cfg=cfg, run=fake_run, state=state_store,
        input_snapshot=df, publisher_stats=None,
    )
    assert summary.updated == 0
    assert summary.skipped == 1
    fake_bw.update_bid_modifier.assert_not_called()
