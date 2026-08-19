"""Build the Bid Optimizer DataFrame (schema depends on tactic).

Port of `pullBidModifiers` (Apps Script Section 9). Joins:
    - publisher_stats (from full_report.py — pub/SSP/modifier/deal CPMs)
    - live bid modifier terms (one row per LI × deal_id_term)
    - LI cpm_bid + bid_modifier_id (already fetched by full_report)
    - per-deal floor prices (deal_agg report, ALL term deal IDs)
    - advertiser names (for state.li_modifier_map)
    - SF↔BW LI ID mapping (from the snapshotted input)

Podcast / Streaming / Total Audio output columns (A–AF, exactly the schema
the multiplier engine consumes):

    A SF_Line_Item_ID         B  BW_Line_Item_ID         C  Line_Item_Name
    D Bid_Modifier_ID         E  Deal_ID                 F  CPM_Bid
    G Floor_Price             H  Deal_Clearing_CPM_On_LI I  Deal_Global_Clearing_CPM
    J Last_3_Days_Clearing_CPM
    K Pub_Impression_Share_Pct  L Pub_Clearing_CPM_On_LI  M Pub_Global_Clearing_CPM
    N SSP_Impression_Share_Pct  O SSP_Clearing_CPM_On_LI  P SSP_Global_Clearing_CPM
    Q Modifier_Deal_List      R  Modifier_Impression_Share_Pct
    S Modifier_Clearing_CPM_On_LI                          T Modifier_Global_Clearing_CPM
    U End_Date                V  Days_Remaining           W Pacing_Pct
    X Daily_Imps_Target       Y  Pacing_Last_Updated
    Z Current_Multiplier
    AA Alternative_ID   AB Calculated_New_Multiplier  AC Effective_Bid_Current  AD Effective_Bid_New
    AE Decision_Reason   AF Sub_Tactic

Marketplace CTV uses its own 30-col schema (see BID_OPTIMIZER_COLUMNS_MP_CTV)
matching the AppScript Bid Optimizer tab for that product exactly.

Cols U-Y (pacing) are written empty here — `calculate_pacing_from_bw`
fills them. Engine cols are written empty — `multiplier_engine` fills them.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from sbo.beeswax_client import BeeswaxClient
from sbo.config_models import EngineConfig
from sbo.run_storage import RunFolder
from sbo.state import StateStore
from sbo.utils import REPORT_ALIASES, clean_id, normalize_columns


BID_OPTIMIZER_COLUMNS: List[str] = [
    "SF_Line_Item_ID",
    "BW_Line_Item_ID",
    "Line_Item_Name",
    "Bid_Modifier_ID",
    "Deal_ID",
    "CPM_Bid",
    "Floor_Price",
    "Deal_Clearing_CPM_On_LI",
    "Deal_Global_Clearing_CPM",
    "Last_3_Days_Clearing_CPM",
    "Pub_Impression_Share_Pct",
    "Pub_Clearing_CPM_On_LI",
    "Pub_Global_Clearing_CPM",
    "SSP_Impression_Share_Pct",
    "SSP_Clearing_CPM_On_LI",
    "SSP_Global_Clearing_CPM",
    "Modifier_Deal_List",
    "Modifier_Impression_Share_Pct",
    "Modifier_Clearing_CPM_On_LI",
    "Modifier_Global_Clearing_CPM",
    "End_Date",
    "Days_Remaining",
    "Pacing_Pct",
    "Daily_Imps_Target",
    "Pacing_Last_Updated",
    "Current_Multiplier",
    "Calculated_New_Multiplier",
    "Effective_Bid_Current",
    "Effective_Bid_New",
    "Decision_Reason",
    "Alternative_ID",
    "Sub_Tactic",
]

# MP CTV Bid Optimizer column schema — matches AppScript Bid Optimizer tab exactly
BID_OPTIMIZER_COLUMNS_MP_CTV: List[str] = [
    "SF_Line_Item_ID",
    "BW_Line_Item_ID",
    "Line_Item_Name",
    "Bid_Modifier_ID",
    "Publisher",
    "Deal_ID",
    "CPM_Bid",
    "Floor_Price",
    "Last_3_Days_Clearing_CPM",
    "Pub_Impression_Share_Pct",
    "Pub_Clearing_CPM_On_LI",
    "Pub_Global_Clearing_CPM",
    "Deal_Impression_Share_Pct",
    "Deal_Clearing_CPM_On_LI",
    "Deal_Global_Clearing_CPM",
    "Category",
    "Category_Share_Pct",
    "Targets_537",
    "End_Date",
    "Days_Remaining",
    "Pacing_Pct",
    "Daily_Imps_Target",
    "Pacing_Last_Updated",
    "Current_Multiplier",
    "Calculated_New_Multiplier",
    "Effective_Bid_Current",
    "Effective_Bid_New",
    "Decision_Reason",
    "Update_Status",     # col 29 — written by push phase
    "_Included_Deal_Lists",  # internal — stripped before sheet write
    "_LI_Targets_537",       # internal — LI-level classification for engine
]

# Select CTV Bid Optimizer column schema — no category/537 concept; single
# fee-adjusted Floor_Price (Max_Floor == Min_Floor by construction in the
# source Apps Script); adds Deal_Clearing_CPM_On_LI (2026-08-14 metric, used
# by the on-target margin-health trim).
BID_OPTIMIZER_COLUMNS_SELECT_CTV: List[str] = [
    "SF_Line_Item_ID",
    "BW_Line_Item_ID",
    "Line_Item_Name",
    "Bid_Modifier_ID",
    "Publisher",
    "Deal_ID",
    "CPM_Bid",
    "Floor_Price",
    "Deal_Clearing_CPM_On_LI",
    "Last_3_Days_Clearing_CPM",
    "Pub_Impression_Share_Pct",
    "Pub_Clearing_CPM_On_LI",
    "Pub_Global_Clearing_CPM",
    "End_Date",
    "Days_Remaining",
    "Pacing_Pct",
    "Daily_Imps_Target",
    "Pacing_Last_Updated",
    "Current_Multiplier",
    "Calculated_New_Multiplier",
    "Effective_Bid_Current",
    "Effective_Bid_New",
    "Decision_Reason",
    "Update_Status",     # written by push phase
]


def _parse_sub_tactic(alt_id: str) -> str:
    """Extract 'streaming' or 'podcast' from alternative_id.

    Fallback for when Sub_Tactic can't be derived from the deal list name.
    Used by Total Audio, which blends Streaming + Podcast deals in one sheet.
    """
    if not alt_id:
        return ""
    parts = alt_id.split("-")
    if len(parts) > 4:
        candidate = parts[4].strip().lower()
        if candidate in ("streaming", "podcast"):
            return candidate
    lower = alt_id.lower()
    if "podcast" in lower:
        return "podcast"
    if "streaming" in lower:
        return "streaming"
    return ""


def pull_bid_modifiers(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
    publisher_stats: pd.DataFrame,
    li_settings: pd.DataFrame,
    last3_cpm: pd.DataFrame,
    input_snapshot: pd.DataFrame,
    deal_to_mod_type: Dict[str, str] | None = None,
    deal_to_sub_tactic: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Build the Bid Optimizer DataFrame.

    Args:
        bw: authenticated Beeswax client (auth happens upstream in pipeline)
        cfg: engine config
        run: per-run folder for snapshots + logs
        state: persistent state store (we update LI Modifier Map here)
        publisher_stats: from full_report.build_full_report (LI × deal aggs)
        li_settings: from full_report._fetch_li_settings (cpm_bid, bm_id, te_id)
        last3_cpm: from full_report._fetch_last_3_days_cpm
        input_snapshot: the Beeswax Line Item Settings tab as read from sheet
                        (cols: 'SF LI ID', 'BW LI ID', 'Advertiser', 'Bid Modifier ID')
        deal_to_sub_tactic: (Total Audio only) deal_id → 'streaming'/'podcast'

    Returns:
        Bid Optimizer DataFrame, one row per LI × deal term.
        Pacing cols (End_Date..Pacing_Last_Updated) are present but empty.
    """
    run.log("=== pull_bid_modifiers start ===")
    bw.authenticate()

    # 1. Resolve modifier IDs from LI settings (skip blanks)
    modifier_ids = (
        li_settings["bid_modifier_id"]
        .replace({"": pd.NA})
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )
    if not modifier_ids:
        run.log("WARNING: no LI has a bid_modifier_id assigned — run Phase 1/2 first.")
        return _empty_bid_optimizer(mp_ctv=cfg.is_mp_ctv, select_ctv=cfg.is_select_ctv)

    # 2. Fetch bid modifiers (each modifier has its terms inline)
    modifiers = bw.fetch_bid_modifiers(modifier_ids)
    run.log(f"Fetched {len(modifiers):,} bid modifiers")

    # 3. Backfill advertiser names (for state.li_modifier_map)
    adv_id_to_name = _fetch_advertiser_names(bw, li_settings, run)

    # 4. All term deal IDs across every modifier — needed for Bid-Opt floor pull
    all_term_deal_ids = sorted({
        str(t.get("value")).strip()
        for m in modifiers
        for t in (m.get("terms") or [])
        if t.get("value") is not None
    })
    run.log(f"Modifier terms point to {len(all_term_deal_ids):,} unique deal IDs")

    # 5. Pull floor prices for those term deals (separate from full_report's pull
    #    which only covers deals in modifier deal-LISTS — not all term deals)
    bid_opt_floor_prices = _fetch_term_floor_prices(bw, all_term_deal_ids, run)

    # 6. Build SF↔BW map from the snapshot
    sf_to_bw, bw_to_sf, bw_to_name = _build_id_maps(input_snapshot)

    # 7. Build LI name + cpm_bid lookups
    li_name_map = dict(zip(li_settings["line_item_id"], li_settings["name"]))
    li_cpm_map = dict(zip(li_settings["line_item_id"], li_settings["cpm_bid"]))

    # 8. Group publisher_stats by (LI, deal) for quick lookup
    ps_lookup = _publisher_stats_lookup(publisher_stats)
    last3_map = dict(zip(last3_cpm["line_item_id"], last3_cpm["last_3_days_cpm"]))

    # 8b. MP CTV only — fallback map for deals with no ATR delivery history on
    # this specific LI (so ps_lookup misses them): pull Publisher/Category/
    # Floor from the persistent deal_cpm_history log instead of leaving them
    # blank. When falling back, also inherit that publisher's/category's
    # already-computed share % from any OTHER deal on this LI that's in the
    # same group, so cap rules treat this deal as already belonging to that
    # group rather than as invisible/uncapped.
    fallback_map: Dict[str, Dict[str, Any]] = {}
    pub_share_lookup: Dict[tuple[str, str], Dict[str, Any]] = {}
    cat_share_lookup: Dict[tuple[str, str], Dict[str, Any]] = {}
    if cfg.is_mp_ctv:
        fallback_map = _deal_fallback_map(state)
        pub_share_lookup = _group_share_lookup(
            publisher_stats, "Publisher",
            ["Pub_Impression_Share_Pct", "Pub_Clearing_CPM_On_LI", "Pub_Global_Clearing_CPM"],
        )
        cat_share_lookup = _group_share_lookup(
            publisher_stats, "Deal_Category",
            ["Category_Impression_Share_Pct", "Category_Clearing_CPM_On_LI", "Category_Global_Clearing_CPM"],
        )
    deal_to_sub_tactic = deal_to_sub_tactic or {}
    _deal_to_mod = deal_to_mod_type or {}

    # 9. Modifier ID → list of LI IDs (one modifier can serve multiple LIs)
    mod_to_lis: Dict[str, List[str]] = {}
    for _, row in li_settings.iterrows():
        bm_id = str(row.get("bid_modifier_id", "")).strip()
        li_id = str(row.get("line_item_id", "")).strip()
        if bm_id and li_id:
            mod_to_lis.setdefault(bm_id, []).append(li_id)

    # 10. Build rows: cross-product of (modifier × term × LI)
    # Pre-compute LI-level Targets_537 classification from publisher_stats
    # Included_Deal_Lists column so all deals on the same LI share the same
    # 537 / MARKETPLACE / NONE value. MP CTV only.
    li_targets_537_map: Dict[str, str] = {}
    if cfg.is_mp_ctv and not publisher_stats.empty:
        marketplace_set_mp = set(cfg.marketplace_list_ids)
        li_col = "Line_Item_ID" if "Line_Item_ID" in publisher_stats.columns else "line_item_id"
        inc_col = "Included_Deal_Lists"
        if li_col in publisher_stats.columns and inc_col in publisher_stats.columns:
            # Take first non-empty Included_Deal_Lists value per LI
            for _, _ps_row in publisher_stats.drop_duplicates(subset=[li_col]).iterrows():
                _li_id = str(_ps_row[li_col]).strip()
                _included_raw = str(_ps_row.get(inc_col, "") or "")
                _included_ids = {s.strip() for s in _included_raw.split(",") if s.strip()}
                if cfg.deal_537_id in _included_ids:
                    li_targets_537_map[_li_id] = "537"
                elif bool(_included_ids & marketplace_set_mp):
                    li_targets_537_map[_li_id] = "MARKETPLACE"
                else:
                    li_targets_537_map[_li_id] = "NONE"

    rows: List[Dict[str, Any]] = []
    for mod in modifiers:
        mod_id = str(mod.get("id", ""))
        terms = mod.get("terms") or []
        target_li_ids = mod_to_lis.get(mod_id, [])
        for term in terms:
            deal_id = (
                str(term["value"]).strip() if term.get("value") is not None else ""
            )
            multiplier = term.get("multiplier", "")
            for li_id in target_li_ids:
                ps_row = ps_lookup.get((li_id, deal_id), {})
                raw_floor = bid_opt_floor_prices.get(deal_id, "")

                if cfg.is_mp_ctv:
                    fb = fallback_map.get(deal_id, {})
                    # No ATR row for this (LI, deal) at all -- backfill
                    # Publisher/Category from the log, and inherit that
                    # group's already-computed share % from any other deal
                    # on this LI in the same group (falls back to 0% if
                    # this is the only deal in that group on this LI, which
                    # is correct -- it genuinely isn't over any cap yet).
                    if not ps_row and fb:
                        ps_row = dict(ps_row)
                        fb_pub = fb.get("Publisher")
                        fb_cat = fb.get("Deal_Category")
                        if fb_pub:
                            ps_row["Publisher"] = fb_pub
                            ps_row.update(pub_share_lookup.get((li_id, fb_pub), {}))
                        if fb_cat:
                            ps_row["Deal_Category"] = fb_cat
                            ps_row.update(cat_share_lookup.get((li_id, fb_cat), {}))
                    # Today's deal_agg pull came back blank for this deal --
                    # reuse the last known real floor instead of leaving it
                    # blank (which would otherwise fall through to the
                    # multiplier engine's rougher same-day floor estimate).
                    if raw_floor in ("", None) and fb.get("Floor_Price") is not None:
                        raw_floor = fb["Floor_Price"]

                # Apply 1.07 fee multiplier exactly once here (matches Apps Script)
                floor_str = ""
                if raw_floor not in ("", None):
                    try:
                        floor_str = f"{float(raw_floor) * cfg.floor_fee_mult:.2f}"
                    except (TypeError, ValueError):
                        floor_str = ""

                if cfg.is_select_ctv:
                    rows.append({
                        "SF_Line_Item_ID":          bw_to_sf.get(li_id, ""),
                        "BW_Line_Item_ID":          li_id,
                        "Line_Item_Name":           li_name_map.get(li_id, "") or bw_to_name.get(li_id, ""),
                        "Bid_Modifier_ID":          mod_id,
                        "Publisher":                ps_row.get("Publisher", ""),
                        "Deal_ID":                  deal_id,
                        "CPM_Bid":                  li_cpm_map.get(li_id, ""),
                        "Floor_Price":              floor_str,
                        "Deal_Clearing_CPM_On_LI":  float(ps_row["Deal_Clearing_CPM_On_LI"]) if ps_row.get("Deal_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "Last_3_Days_Clearing_CPM": float(last3_map[li_id]) if last3_map.get(li_id) not in ("", None) else 0.0,
                        "Pub_Impression_Share_Pct": float(ps_row["Pub_Impression_Share_Pct"]) if ps_row.get("Pub_Impression_Share_Pct") not in ("", None) else 0.0,
                        "Pub_Clearing_CPM_On_LI":   float(ps_row["Pub_Clearing_CPM_On_LI"]) if ps_row.get("Pub_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "Pub_Global_Clearing_CPM":  float(ps_row["Pub_Global_Clearing_CPM"]) if ps_row.get("Pub_Global_Clearing_CPM") not in ("", None) else 0.0,
                        "End_Date":                 "",
                        "Days_Remaining":           "",
                        "Pacing_Pct":               "",
                        "Daily_Imps_Target":        "",
                        "Pacing_Last_Updated":      "",
                        "Current_Multiplier":       multiplier,
                        "Calculated_New_Multiplier": "",
                        "Effective_Bid_Current":    "",
                        "Effective_Bid_New":        "",
                        "Decision_Reason":          "",
                        "Update_Status":            "",
                    })
                elif cfg.is_mp_ctv:
                    included_raw = str(ps_row.get("Included_Deal_Lists", "") or "")
                    # Use LI-level classification (pre-computed from TE map)
                    # so all deals on the same LI share the same Targets_537 value
                    targets_537 = li_targets_537_map.get(str(li_id), "NONE")
                    rows.append({
                        "SF_Line_Item_ID":           bw_to_sf.get(li_id, ""),
                        "BW_Line_Item_ID":           li_id,
                        "Line_Item_Name":            li_name_map.get(li_id, "") or bw_to_name.get(li_id, ""),
                        "Bid_Modifier_ID":           mod_id,
                        "Publisher":                 ps_row.get("Publisher", ""),
                        "Deal_ID":                   deal_id,
                        "CPM_Bid":                   li_cpm_map.get(li_id, ""),
                        "Floor_Price":               floor_str,
                        "Last_3_Days_Clearing_CPM":  float(last3_map[li_id]) if last3_map.get(li_id) not in ("", None) else 0.0,
                        "Pub_Impression_Share_Pct":  float(ps_row["Pub_Impression_Share_Pct"]) if ps_row.get("Pub_Impression_Share_Pct") not in ("", None) else 0.0,
                        "Pub_Clearing_CPM_On_LI":    float(ps_row["Pub_Clearing_CPM_On_LI"]) if ps_row.get("Pub_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "Pub_Global_Clearing_CPM":   float(ps_row["Pub_Global_Clearing_CPM"]) if ps_row.get("Pub_Global_Clearing_CPM") not in ("", None) else 0.0,
                        "Deal_Impression_Share_Pct": float(ps_row["Deal_Impression_Share_Pct"]) if ps_row.get("Deal_Impression_Share_Pct") not in ("", None) else 0.0,
                        "Deal_Clearing_CPM_On_LI":   float(ps_row["Deal_Clearing_CPM_On_LI"]) if ps_row.get("Deal_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "Deal_Global_Clearing_CPM":  float(ps_row["Deal_Global_Clearing_CPM"]) if ps_row.get("Deal_Global_Clearing_CPM") not in ("", None) else 0.0,
                        "Category":                  ps_row.get("Deal_Category", ""),
                        "Category_Share_Pct":        float(ps_row["Category_Impression_Share_Pct"]) if ps_row.get("Category_Impression_Share_Pct") not in ("", None) else 0.0,
                        "Targets_537":               targets_537,
                        "End_Date":                  "",
                        "Days_Remaining":            "",
                        "Pacing_Pct":                "",
                        "Daily_Imps_Target":         "",
                        "Pacing_Last_Updated":       "",
                        "Current_Multiplier":        multiplier,
                        "Calculated_New_Multiplier": "",
                        "Effective_Bid_Current":     "",
                        "Effective_Bid_New":         "",
                        "Decision_Reason":           "",
                        "Update_Status":             "",
                        "_Included_Deal_Lists":      included_raw,
                        "_LI_Targets_537":           targets_537,
                    })
                else:
                    rows.append({
                        "SF_Line_Item_ID":               bw_to_sf.get(li_id, ""),
                        "BW_Line_Item_ID":               li_id,
                        "Line_Item_Name":                li_name_map.get(li_id, "") or bw_to_name.get(li_id, ""),
                        "Bid_Modifier_ID":               mod_id,
                        "Deal_ID":                        deal_id,
                        "CPM_Bid":                        li_cpm_map.get(li_id, ""),
                        "Floor_Price":                    floor_str,
                        "Deal_Clearing_CPM_On_LI":        float(ps_row["Deal_Clearing_CPM_On_LI"]) if ps_row.get("Deal_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "Deal_Global_Clearing_CPM":       float(ps_row["Deal_Global_Clearing_CPM"]) if ps_row.get("Deal_Global_Clearing_CPM") not in ("", None) else 0.0,
                        "Last_3_Days_Clearing_CPM":       float(last3_map[li_id]) if last3_map.get(li_id) not in ("", None) else 0.0,
                        "Pub_Impression_Share_Pct":       float(ps_row["Pub_Impression_Share_Pct"]) if ps_row.get("Pub_Impression_Share_Pct") not in ("", None) else 0.0,
                        "Pub_Clearing_CPM_On_LI":         float(ps_row["Pub_Clearing_CPM_On_LI"]) if ps_row.get("Pub_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "Pub_Global_Clearing_CPM":        float(ps_row["Pub_Global_Clearing_CPM"]) if ps_row.get("Pub_Global_Clearing_CPM") not in ("", None) else 0.0,
                        "SSP_Impression_Share_Pct":       float(ps_row["SSP_Impression_Share_Pct"]) if ps_row.get("SSP_Impression_Share_Pct") not in ("", None) else 0.0,
                        "SSP_Clearing_CPM_On_LI":         float(ps_row["SSP_Clearing_CPM_On_LI"]) if ps_row.get("SSP_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "SSP_Global_Clearing_CPM":        float(ps_row["SSP_Global_Clearing_CPM"]) if ps_row.get("SSP_Global_Clearing_CPM") not in ("", None) else 0.0,
                        "Modifier_Deal_List":             _deal_to_mod.get(deal_id) or ps_row.get("Modifier_Deal_List", ""),
                        "Modifier_Impression_Share_Pct":  float(ps_row["Modifier_Impression_Share_Pct"]) if ps_row.get("Modifier_Impression_Share_Pct") not in ("", None) else 0.0,
                        "Modifier_Clearing_CPM_On_LI":    float(ps_row["Modifier_Clearing_CPM_On_LI"]) if ps_row.get("Modifier_Clearing_CPM_On_LI") not in ("", None) else 0.0,
                        "Modifier_Global_Clearing_CPM":   float(ps_row["Modifier_Global_Clearing_CPM"]) if ps_row.get("Modifier_Global_Clearing_CPM") not in ("", None) else 0.0,
                        # Pacing cols filled later by calculate_pacing_from_bw
                        "End_Date":                       "",
                        "Days_Remaining":                 "",
                        "Pacing_Pct":                     "",
                        "Daily_Imps_Target":              "",
                        "Pacing_Last_Updated":            "",
                        "Current_Multiplier":             multiplier,
                        # Engine cols filled later by multiplier_engine
                        "Calculated_New_Multiplier":      "",
                        "Effective_Bid_Current":          "",
                        "Effective_Bid_New":              "",
                        "Decision_Reason":                "",
                        "Alternative_ID":                 ps_row.get("Deal_Alternative_ID", ""),
                        "Sub_Tactic":                     deal_to_sub_tactic.get(deal_id, "") or _parse_sub_tactic(str(ps_row.get("Deal_Alternative_ID", "") or "")),
                    })

    if cfg.is_mp_ctv:
        col_schema = BID_OPTIMIZER_COLUMNS_MP_CTV
    elif cfg.is_select_ctv:
        col_schema = BID_OPTIMIZER_COLUMNS_SELECT_CTV
    else:
        col_schema = BID_OPTIMIZER_COLUMNS
    df = pd.DataFrame(rows, columns=col_schema)
    run.log(f"Bid Optimizer built: {len(df):,} rows × {len(df.columns)} cols")

    # 11. Update LI Modifier Map state (BW LI ID → advertiser_name + modifier_id)
    _update_li_modifier_map(state, li_settings, adv_id_to_name)

    run.log("=== pull_bid_modifiers complete ===")
    return df


# ── helpers ───────────────────────────────────────────────────────────────


def _empty_bid_optimizer(mp_ctv: bool = False, select_ctv: bool = False) -> pd.DataFrame:
    if mp_ctv:
        cols = BID_OPTIMIZER_COLUMNS_MP_CTV
    elif select_ctv:
        cols = BID_OPTIMIZER_COLUMNS_SELECT_CTV
    else:
        cols = BID_OPTIMIZER_COLUMNS
    return pd.DataFrame(columns=cols)


def _fetch_advertiser_names(
    bw: BeeswaxClient, li_settings: pd.DataFrame, run: RunFolder
) -> Dict[str, str]:
    """advertiser_id → name. Used to backfill state.li_modifier_map."""
    adv_ids = (
        li_settings["advertiser_id"]
        .replace({"": pd.NA})
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )
    if not adv_ids:
        return {}
    out = {
        str(adv["id"]): adv.get("name", "")
        for adv in bw.fetch_advertisers(adv_ids)
        if adv.get("id") is not None
    }
    run.log(f"Fetched {len(out):,} advertiser names")
    return out


def _fetch_term_floor_prices(
    bw: BeeswaxClient, deal_ids: List[str], run: RunFolder
) -> Dict[str, float]:
    """deal_id → raw floor price (NOT fee-adjusted yet)."""
    if not deal_ids:
        return {}
    rows = bw.fetch_report(
        {
            "view": "deal_agg",
            "fields": ["deal_id", "floor_price"],
            "filters": {"deal_id": ",".join(deal_ids), "bid_hour": "NOT NULL"},
            "result_format": "csv",
        },
        label="Bid Opt Deal Floor Prices",
    )
    df = pd.DataFrame(rows)
    out: Dict[str, float] = {}
    if df.empty:
        run.log("Bid Opt floors: no rows returned.")
        return out
    df = normalize_columns(df, REPORT_ALIASES["deal_agg"])
    if "deal_id" not in df.columns or "floor_price" not in df.columns:
        run.log(
            f"WARNING: Bid Opt floors report missing deal_id/floor_price "
            f"(got {list(df.columns)}) — returning empty"
        )
        return out
    df["deal_id"] = df["deal_id"].astype(str).str.strip()
    df["floor_price"] = pd.to_numeric(df["floor_price"], errors="coerce")
    df = df.dropna(subset=["floor_price"]).drop_duplicates(subset=["deal_id"])
    out = dict(zip(df["deal_id"], df["floor_price"].astype(float)))
    run.log(f"Bid Opt floors mapped: {len(out):,} unique deals")
    return out


def _build_id_maps(
    input_snapshot: pd.DataFrame,
) -> tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    """SF↔BW lookups + BW LI → advertiser name from the input tab."""
    sf_to_bw: Dict[str, str] = {}
    bw_to_sf: Dict[str, str] = {}
    bw_to_name: Dict[str, str] = {}
    if input_snapshot.empty:
        return sf_to_bw, bw_to_sf, bw_to_name

    # Resolve column names (sheet snapshot may use Title Case + has None headers)
    cols = {
        str(c).lower().strip(): c
        for c in input_snapshot.columns
        if c is not None and str(c).strip()
    }
    sf_col = cols.get("sf li id") or cols.get("sf_li_id")
    bw_col = cols.get("bw li id") or cols.get("bw_li_id")
    adv_col = cols.get("advertiser")

    if not sf_col or not bw_col:
        return sf_to_bw, bw_to_sf, bw_to_name

    for _, row in input_snapshot.iterrows():
        sf = clean_id(row[sf_col]) if pd.notna(row[sf_col]) else ""
        bw = str(row[bw_col]).strip() if pd.notna(row[bw_col]) else ""
        if not sf or not bw or bw in ("0", "nan"):
            continue
        sf_to_bw[sf] = bw
        bw_to_sf[bw] = sf
        if adv_col:
            bw_to_name[bw] = str(row[adv_col]).strip() if pd.notna(row[adv_col]) else ""
    return sf_to_bw, bw_to_sf, bw_to_name


def _publisher_stats_lookup(
    publisher_stats: pd.DataFrame,
) -> Dict[tuple[str, str], Dict[str, Any]]:
    """(line_item_id, deal_id) → first matching publisher_stats row as dict."""
    if publisher_stats.empty:
        return {}
    df = publisher_stats.drop_duplicates(subset=["Line_Item_ID", "Deal_ID"], keep="first")
    return {
        (str(r["Line_Item_ID"]), str(r["Deal_ID"])): r.to_dict()
        for _, r in df.iterrows()
    }


def _deal_fallback_map(state: StateStore) -> Dict[str, Dict[str, Any]]:
    """Deal_ID -> {Publisher, Deal_Category, Floor_Price} from the persistent
    deal_cpm_history log, for deals this run's ATR has no delivery history
    for. Blank/NaN fields are omitted so callers naturally fall through to
    their own "" default rather than overwriting it with an empty string."""
    log = state.load("deal_cpm_history")
    if log.empty or "Deal_ID" not in log.columns:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for _, r in log.iterrows():
        deal_id = str(r["Deal_ID"]).strip()
        if not deal_id:
            continue
        entry = {}
        pub = r.get("Publisher")
        if pd.notna(pub) and str(pub).strip():
            entry["Publisher"] = str(pub).strip()
        cat = r.get("Deal_Category")
        if pd.notna(cat) and str(cat).strip():
            entry["Deal_Category"] = str(cat).strip()
        floor = r.get("Floor_Price")
        if pd.notna(floor):
            entry["Floor_Price"] = float(floor)
        if entry:
            out[deal_id] = entry
    return out


def _group_share_lookup(
    publisher_stats: pd.DataFrame, group_col: str, share_cols: List[str],
) -> Dict[tuple[str, str], Dict[str, Any]]:
    """(line_item_id, group value) -> first matching row's share/CPM columns.

    Lets a fallback-mapped deal (no publisher_stats row of its own) inherit
    the already-computed share % of whatever publisher/category it's mapped
    to, from any OTHER deal on the same LI that's actually in that group.
    """
    if publisher_stats.empty or group_col not in publisher_stats.columns:
        return {}
    df = publisher_stats[publisher_stats[group_col].astype(str).str.strip() != ""]
    df = df.drop_duplicates(subset=["Line_Item_ID", group_col], keep="first")
    return {
        (str(r["Line_Item_ID"]), str(r[group_col])): {c: r.get(c) for c in share_cols}
        for _, r in df.iterrows()
    }


def _update_li_modifier_map(
    state: StateStore,
    li_settings: pd.DataFrame,
    adv_id_to_name: Dict[str, str],
) -> None:
    """Upsert BW LI ID → advertiser name → bid modifier ID into state.

    Mirrors `sboUpsertLiModifierMap_`. Persistent across runs so we don't
    have to re-fetch advertisers on every Phase 1/3 run.
    """
    if li_settings.empty:
        return
    new_rows = []
    for _, row in li_settings.iterrows():
        bw_id = str(row.get("line_item_id", "")).strip()
        if not bw_id or bw_id == "0":
            continue
        adv_id = str(row.get("advertiser_id", "")).strip()
        bm_id = str(row.get("bid_modifier_id", "")).strip()
        adv_name = adv_id_to_name.get(adv_id, "")
        new_rows.append({
            "BW_Line_Item_ID": bw_id,
            "Advertiser_Name": adv_name,
            "Bid_Modifier_ID": bm_id,
        })
    if not new_rows:
        return
    new_df = pd.DataFrame(new_rows)

    existing = state.load("li_modifier_map")
    if existing.empty:
        merged = new_df
    else:
        # Upsert: drop existing rows for these BW IDs, then append
        existing = existing[~existing["BW_Line_Item_ID"].isin(new_df["BW_Line_Item_ID"])]
        merged = pd.concat([existing, new_df], ignore_index=True)
    state.save("li_modifier_map", merged)
