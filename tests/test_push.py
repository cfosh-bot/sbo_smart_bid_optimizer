"""Push tests — fake Beeswax client lets us verify GET/PUT logic without HTTP."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from sbo.config_models import load_config
from sbo.push import push_multipliers_to_beeswax


@pytest.fixture
def cfg():
    return load_config("sbo/config/podcast.yaml")


@pytest.fixture
def fake_run(tmp_path):
    """Minimal RunFolder stand-in — only the methods push.py uses."""
    run = MagicMock()
    run.path = tmp_path
    (tmp_path / "beeswax_raw").mkdir()
    (tmp_path / "01_inputs").mkdir()

    def _save_df(name, df):
        out = tmp_path / (name if name.endswith(".parquet") else f"{name}.parquet")
        df.to_parquet(out, index=False)
        return out

    def _save_bw(label, body):
        out = tmp_path / "beeswax_raw" / f"{label}.json"
        out.write_text(body)
        return out

    run.save_dataframe.side_effect = _save_df
    run.save_beeswax_response.side_effect = _save_bw
    run.log.side_effect = lambda msg: None
    return run


@pytest.fixture
def fake_bw():
    """Mock BeeswaxClient with controllable get/put."""
    bw = MagicMock()
    bw.authenticate.return_value = None
    bw._modifiers = {
        "555": {
            "id": 555,
            "name": "Test Modifier",
            "terms": [
                {"value": "tri/abc", "multiplier": "1.00", "targeting_key": "deal_id"},
                {"value": "tri/xyz", "multiplier": "0.80", "targeting_key": "deal_id"},
            ],
        }
    }

    def _get(mod_id):
        return bw._modifiers[str(mod_id)]

    def _put(mod_id, payload):
        bw._modifiers[str(mod_id)] = payload
        return payload

    bw.get_bid_modifier.side_effect = _get
    bw.update_bid_modifier.side_effect = _put
    return bw


def _opt_row(**overrides):
    base = {
        "BW_Line_Item_ID": "9001", "Deal_ID": "tri/abc", "Bid_Modifier_ID": "555",
        "Current_Multiplier": "1.00", "Calculated_New_Multiplier": "1.30",
    }
    base.update(overrides)
    return base


# ── tests ─────────────────────────────────────────────────────────────────


def test_push_skips_when_no_change(cfg, fake_run, fake_bw):
    df = pd.DataFrame([_opt_row(**{
        "Current_Multiplier": "1.00", "Calculated_New_Multiplier": "1.00",
    })])
    results, summary = push_multipliers_to_beeswax(
        bw=fake_bw, cfg=cfg, run=fake_run, bid_optimizer=df,
    )
    assert summary.updated == 0
    assert summary.skipped == 1
    fake_bw.update_bid_modifier.assert_not_called()


def test_push_writes_changed_term(cfg, fake_run, fake_bw):
    df = pd.DataFrame([_opt_row()])
    results, summary = push_multipliers_to_beeswax(
        bw=fake_bw, cfg=cfg, run=fake_run, bid_optimizer=df,
    )
    assert summary.updated == 1
    assert summary.errors == 0
    fake_bw.update_bid_modifier.assert_called_once()
    # Confirm the term was actually mutated
    updated_mod = fake_bw._modifiers["555"]
    abc_term = next(t for t in updated_mod["terms"] if t["value"] == "tri/abc")
    assert abc_term["multiplier"] == "1.30"
    # Other term should be untouched
    xyz_term = next(t for t in updated_mod["terms"] if t["value"] == "tri/xyz")
    assert xyz_term["multiplier"] == "0.80"


def test_dry_run_does_not_put(cfg, fake_run, fake_bw):
    df = pd.DataFrame([_opt_row()])
    results, summary = push_multipliers_to_beeswax(
        bw=fake_bw, cfg=cfg, run=fake_run, bid_optimizer=df, dry_run=True,
    )
    assert summary.updated == 1  # would-have-updated counted
    fake_bw.update_bid_modifier.assert_not_called()
    fake_bw.authenticate.assert_not_called()


def test_push_groups_multiple_terms_into_one_put(cfg, fake_run, fake_bw):
    """Two changing terms on same modifier → one GET + one PUT, not two."""
    df = pd.DataFrame([
        _opt_row(**{"Deal_ID": "tri/abc", "Calculated_New_Multiplier": "1.30"}),
        _opt_row(**{"Deal_ID": "tri/xyz", "Calculated_New_Multiplier": "1.50"}),
    ])
    results, summary = push_multipliers_to_beeswax(
        bw=fake_bw, cfg=cfg, run=fake_run, bid_optimizer=df,
    )
    assert summary.updated == 2
    assert fake_bw.get_bid_modifier.call_count == 1
    assert fake_bw.update_bid_modifier.call_count == 1
    updated_mod = fake_bw._modifiers["555"]
    multipliers = {t["value"]: t["multiplier"] for t in updated_mod["terms"]}
    assert multipliers["tri/abc"] == "1.30"
    assert multipliers["tri/xyz"] == "1.50"


def test_push_results_schema(cfg, fake_run, fake_bw):
    df = pd.DataFrame([_opt_row()])
    results, summary = push_multipliers_to_beeswax(
        bw=fake_bw, cfg=cfg, run=fake_run, bid_optimizer=df,
    )
    expected = {
        "BW_Line_Item_ID", "Deal_ID", "Bid_Modifier_ID",
        "Prev_Multiplier", "New_Multiplier",
        "Status", "Error", "Pushed_At",
    }
    assert set(results.columns) == expected
    assert len(results) == 1
    assert "1.000 → 1.300" in results.iloc[0]["Status"]


def test_per_modifier_failure_does_not_break_run(cfg, fake_run, fake_bw):
    """One bad modifier → that group errors, other modifiers continue."""
    from sbo.beeswax_client import BeeswaxError

    fake_bw._modifiers["666"] = {
        "id": 666, "terms": [{"value": "tri/qqq", "multiplier": "1.00"}],
    }

    def get_with_failure(mod_id):
        if str(mod_id) == "555":
            raise BeeswaxError("simulated GET failure")
        return fake_bw._modifiers[str(mod_id)]
    fake_bw.get_bid_modifier.side_effect = get_with_failure

    df = pd.DataFrame([
        _opt_row(**{"Bid_Modifier_ID": "555"}),
        _opt_row(**{
            "Bid_Modifier_ID": "666", "Deal_ID": "tri/qqq",
            "Calculated_New_Multiplier": "1.20",
        }),
    ])
    results, summary = push_multipliers_to_beeswax(
        bw=fake_bw, cfg=cfg, run=fake_run, bid_optimizer=df,
    )
    assert summary.errors == 1            # row from 555
    assert summary.updated == 1           # row from 666
    assert summary.modifiers_failed == 1
    assert summary.modifiers_touched == 1
    assert len(summary.error_log) == 1


def test_no_matching_terms_logged_not_pushed(cfg, fake_run, fake_bw):
    """If the live modifier doesn't have the deal_id we expected → no PUT."""
    df = pd.DataFrame([_opt_row(**{"Deal_ID": "tri/missing"})])
    results, summary = push_multipliers_to_beeswax(
        bw=fake_bw, cfg=cfg, run=fake_run, bid_optimizer=df,
    )
    fake_bw.update_bid_modifier.assert_not_called()
    assert "No matching terms" in results.iloc[0]["Status"]
