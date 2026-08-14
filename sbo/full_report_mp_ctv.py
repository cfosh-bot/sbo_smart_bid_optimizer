"""Build the Publisher Stats DataFrame for Marketplace CTV.

Parallel to full_report.py but with the MP CTV-specific aggregation:
- Publisher share / CPM (from alternative_id parsed pub name)
- Category share / CPM (from MP CTV Audience deal lists)
- Deal share / CPM (per LI × deal)
- Pub_Deal_List_ID / Name (from Marketplace CTV - Pub: lists)
- Targets_537 column (whether any targeted list is deal list 537)

The column set matches the MP CTV Apps Script Publisher Stats schema
(Section 8 / buildFullReport).

Pipeline:
    1. Fetch ATR                              → 02_atr.parquet
    2. Fetch LI settings + targeting exprs
    3. Fetch Last 3 Days CPM + Last 1 Day Imps
    4. Build pub list map  (listId → pubName)
    5. Build category map  (dealId → categoryName)
    6. Fetch deal floor prices
    7. Two-pass pandas aggregation            → 03_publisher_stats.parquet
    8. Upsert Deal + Publisher CPM history

Returns FullReportArtifacts (same dataclass as full_report.py so pipeline.py
can call either interchangeably).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd

from sbo.beeswax_client import BeeswaxClient
from sbo.config_models import EngineConfig
from sbo.full_report import (
    FullReportArtifacts,
    _fetch_atr,
    _fetch_deal_floor_prices,
    _fetch_last_1_day_imps,
    _fetch_last_3_days_cpm,
    _fetch_li_settings,
    _fetch_targeting_expressions,
    _read_input_bw_ids,
)
from sbo.run_storage import RunFolder
from sbo.state import StateStore
from sbo.utils import REPORT_ALIASES, normalize_columns


# ── public entry point ────────────────────────────────────────────────────


def build_full_report_mp_ctv(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
) -> FullReportArtifacts:
    """Run the MP CTV full report build. Returns FullReportArtifacts."""
    run.log("=== build_full_report_mp_ctv start ===")
    bw.authenticate()

    # 1. ATR
    bw_ids = _read_input_bw_ids(run)
    atr = _fetch_atr(bw, bw_ids, run)
    run.save_dataframe("02_atr", atr)
    run.log(f"ATR fetched: {len(atr):,} rows, {atr['line_item_id'].nunique():,} unique LIs")

    all_li_ids = bw_ids

    # 2. LI settings + targeting expressions
    li_settings = _fetch_li_settings(bw, all_li_ids, run)
    te_map = _fetch_targeting_expressions(bw, li_settings, run)

    # 3. Last 3 days CPM + last 1 day imps
    last3_cpm  = _fetch_last_3_days_cpm(bw, all_li_ids, run)
    last1_imps = _fetch_last_1_day_imps(bw, all_li_ids, run)
    run.save_dataframe("last_day_imps", last1_imps)

    # 4. Fetch all Beeswax lists + their items (needed for pub map + category map)
    run.log("Fetching all deal lists and items...")
    all_lists        = bw.fetch_all_lists()
    items_by_list_id = bw.fetch_all_list_items_by_list_id()

    # 5a. Pub list map: {listId: str → pubName: str}
    pub_list_map = _build_pub_list_map(all_lists, cfg)
    run.log(f"Pub list map: {len(pub_list_map):,} publisher lists")

    # 5b. Category map: {dealId → categoryName}
    deal_to_category = _build_category_map(all_lists, items_by_list_id, cfg)
    run.log(f"Category map: {len(deal_to_category):,} deals mapped to categories")

    # 5c. Pub name → {listId, listName} (for Publisher Stats Pub_Deal_List columns)
    pub_name_to_list = _build_pub_name_to_list(pub_list_map)

    # 5d. For MP CTV the deal_to_mod_type returned to the pipeline is the
    # category map (mirrors how Podcast uses modifier categories).
    # We also need all unique deal IDs for floor price fetch — use category
    # map keys PLUS any deal IDs that appear in pub lists.
    all_deal_ids_for_floors = set(deal_to_category.keys())
    for list_id, items in items_by_list_id.items():
        if str(list_id) in pub_list_map:
            all_deal_ids_for_floors.update(items.keys())

    # 6. Deal floor prices
    floor_prices = _fetch_deal_floor_prices(bw, list(all_deal_ids_for_floors), run)

    # 7. Build Publisher Stats
    pub_stats = _build_publisher_stats_mp_ctv(
        atr=atr,
        li_settings=li_settings,
        te_map=te_map,
        last3_cpm=last3_cpm,
        deal_to_category=deal_to_category,
        pub_list_map=pub_list_map,
        pub_name_to_list=pub_name_to_list,
        floor_prices=floor_prices,
        items_by_list_id=items_by_list_id,
        cfg=cfg,
    )
    run.save_dataframe("03_publisher_stats", pub_stats)
    run.log(f"Publisher stats built: {len(pub_stats):,} rows")

    # 8. Upsert Deal + Publisher CPM history
    _upsert_deal_cpm_history(state, atr, deal_to_category, floor_prices)
    _upsert_publisher_cpm_history(state, atr, pub_name_to_list)

    run.log("=== build_full_report_mp_ctv complete ===")
    return FullReportArtifacts(
        publisher_stats=pub_stats,
        li_settings=li_settings,
        last3_cpm=last3_cpm,
        last1_imps=last1_imps,
        te_map=te_map,
        deal_to_mod_type=deal_to_category,  # used downstream as the "category" dimension
    )


# ── list helpers ──────────────────────────────────────────────────────────


def _build_pub_list_map(all_lists: List[Dict], cfg: EngineConfig) -> Dict[str, str]:
    """listId (str) → publisher name, for lists named 'Marketplace CTV - Pub: <name>'."""
    pub_prefix = cfg.beeswax.pub_prefix  # "Marketplace CTV - Pub:"
    out: Dict[str, str] = {}
    for lst in all_lists:
        name = lst.get("name") or ""
        if pub_prefix in name:
            list_id  = str(lst.get("id", ""))
            pub_name = name.split(pub_prefix, 1)[1].strip()
            out[list_id] = pub_name
    return out


def _build_category_map(
    all_lists: List[Dict],
    items_by_list_id: Dict[str, Dict],
    cfg: EngineConfig,
) -> Dict[str, str]:
    """deal_id → category name, from lists named 'MP CTV Audience: <Category> Pubs'."""
    cat_prefix = cfg.beeswax.cat_prefix  # "MP CTV Audience:"
    out: Dict[str, str] = {}
    for lst in all_lists:
        name    = lst.get("name") or ""
        list_id = str(lst.get("id", ""))
        if cat_prefix not in name:
            continue
        # Strip trailing ' Pubs' if present
        raw_cat  = name.split(cat_prefix, 1)[1].strip()
        cat_name = raw_cat.rstrip(" Pubs").strip() if raw_cat.endswith(" Pubs") else raw_cat
        deals    = items_by_list_id.get(list_id, {})
        for deal_id in deals:
            out[str(deal_id)] = cat_name
    return out


def _build_pub_name_to_list(pub_list_map: Dict[str, str]) -> Dict[str, Dict]:
    """pubName.lower() → {list_id, list_name} for Publisher Stats join."""
    out: Dict[str, Dict] = {}
    for list_id, pub_name in pub_list_map.items():
        out[pub_name.lower()] = {"list_id": list_id, "list_name": f"Marketplace CTV - Pub: {pub_name}"}
    return out


# ── alt_id parser ─────────────────────────────────────────────────────────


def _parse_alt_id(alt_id: str) -> Dict[str, str]:
    """Parse MP CTV alternative_id format: Genre-Publisher-SSP-DeviceType-DealSource.

    Mirrors sboParseAltId_() from the MP CTV Apps Script.
    Returns a dict with keys: genre, publisher, ssp, device_type, deal_source.
    All values default to '' if the segment is missing.
    """
    parts = str(alt_id or "").split("-")
    keys  = ["genre", "publisher", "ssp", "device_type", "deal_source"]
    return {k: parts[i].strip() if i < len(parts) else "" for i, k in enumerate(keys)}


# ── Publisher Stats aggregation ────────────────────────────────────────────


def _build_publisher_stats_mp_ctv(
    atr: pd.DataFrame,
    li_settings: pd.DataFrame,
    te_map: Dict[str, Dict[str, str]],
    last3_cpm: pd.DataFrame,
    deal_to_category: Dict[str, str],
    pub_list_map: Dict[str, str],
    pub_name_to_list: Dict[str, Dict],
    floor_prices: pd.DataFrame,
    items_by_list_id: Dict[str, Dict],
    cfg: EngineConfig,
) -> pd.DataFrame:
    """Produce the MP CTV Publisher Stats DataFrame (one row per LI × deal).

    pandas equivalent of the two-pass aggregation in MP CTV buildFullReport.
    """
    if atr.empty:
        return pd.DataFrame()

    # Parse alternative_id → pub, genre, ssp, device_type, deal_source
    parsed = atr["alternative_id"].fillna("").astype(str).apply(_parse_alt_id)
    df = atr.copy()
    df["genre"]        = parsed.map(lambda x: x["genre"])
    df["publisher"]    = parsed.map(lambda x: x["publisher"])
    df["ssp"]          = parsed.map(lambda x: x["ssp"])
    df["device_type"]  = parsed.map(lambda x: x["device_type"])
    df["deal_source"]  = parsed.map(lambda x: x["deal_source"])
    df["category"]     = df["deal_id"].map(deal_to_category).fillna("")

    def _cpm(spend: pd.Series, imps: pd.Series) -> pd.Series:
        return ((spend / imps).where(imps > 0, 0) * 1000).round(2)

    def _share(num: pd.Series, denom: pd.Series) -> pd.Series:
        return ((num / denom).where(denom > 0, 0) * 100).round(2)

    # ── Pass 1 aggregations ───────────────────────────────────────────
    li_tot = df.groupby("line_item_id", as_index=False).agg(
        li_imps=("impression", "sum"), li_spend=("media_spend_usd", "sum")
    )
    pub_li = df.groupby(["line_item_id", "publisher"], as_index=False).agg(
        pub_li_imps=("impression", "sum"), pub_li_spend=("media_spend_usd", "sum")
    )
    pub_global = df.groupby("publisher", as_index=False).agg(
        pub_glob_imps=("impression", "sum"), pub_glob_spend=("media_spend_usd", "sum")
    )
    cat_li = df.groupby(["line_item_id", "category"], as_index=False).agg(
        cat_li_imps=("impression", "sum"), cat_li_spend=("media_spend_usd", "sum")
    )
    cat_global = df.groupby("category", as_index=False).agg(
        cat_glob_imps=("impression", "sum"), cat_glob_spend=("media_spend_usd", "sum")
    )
    deal_li = df.groupby(["line_item_id", "deal_id"], as_index=False).agg(
        deal_li_imps=("impression", "sum"), deal_li_spend=("media_spend_usd", "sum")
    )
    deal_global = df.groupby("deal_id", as_index=False).agg(
        deal_glob_imps=("impression", "sum"), deal_glob_spend=("media_spend_usd", "sum")
    )

    # ── Merge all aggregations back to ATR rows ───────────────────────
    out = (
        df.merge(li_tot,     on="line_item_id",              how="left")
          .merge(pub_li,     on=["line_item_id", "publisher"], how="left")
          .merge(pub_global, on="publisher",                  how="left")
          .merge(cat_li,     on=["line_item_id", "category"], how="left")
          .merge(cat_global, on="category",                   how="left")
          .merge(deal_li,    on=["line_item_id", "deal_id"],  how="left")
          .merge(deal_global, on="deal_id",                   how="left")
    )

    # ── Computed share / CPM columns ─────────────────────────────────
    out["Pub_Impression_Share_Pct"]      = _share(out["pub_li_imps"],   out["li_imps"])
    out["Pub_Clearing_CPM_On_LI"]        = _cpm(out["pub_li_spend"],    out["pub_li_imps"])
    out["Pub_Global_Clearing_CPM"]       = _cpm(out["pub_glob_spend"],  out["pub_glob_imps"])
    out["Category_Impression_Share_Pct"] = _share(out["cat_li_imps"],   out["li_imps"])
    out["Category_Clearing_CPM_On_LI"]   = _cpm(out["cat_li_spend"],    out["cat_li_imps"])
    out["Category_Global_Clearing_CPM"]  = _cpm(out["cat_glob_spend"],  out["cat_glob_imps"])
    out["Deal_Impression_Share_Pct"]     = _share(out["deal_li_imps"],  out["li_imps"])
    out["Deal_Clearing_CPM_On_LI"]       = _cpm(out["deal_li_spend"],   out["deal_li_imps"])
    out["Deal_Global_Clearing_CPM"]      = _cpm(out["deal_glob_spend"], out["deal_glob_imps"])

    # ── Pub deal list ID / name lookup ────────────────────────────────
    def _pub_list_id(pub: str) -> str:
        info = pub_name_to_list.get(pub.lower(), {})
        return info.get("list_id", "")

    def _pub_list_name(pub: str) -> str:
        info = pub_name_to_list.get(pub.lower(), {})
        return info.get("list_name", "")

    out["Pub_Deal_List_ID"]   = out["publisher"].apply(_pub_list_id)
    out["Pub_Deal_List_Name"] = out["publisher"].apply(_pub_list_name)

    # ── Floor price (raw — fee applied downstream in bid_optimizer) ───
    out = out.merge(floor_prices, on="deal_id", how="left")

    # ── LI settings join ─────────────────────────────────────────────
    li_set = li_settings.set_index("line_item_id")
    out["Bid_Modifier_ID"]        = out["line_item_id"].map(li_set["bid_modifier_id"]).fillna("")
    out["CPM_Bid"]                = pd.to_numeric(
        out["line_item_id"].map(li_set["cpm_bid"]), errors="coerce"
    ).fillna(0.0)
    out["Targeting_Expression_ID"] = out["line_item_id"].map(
        li_set["targeting_expression_id"]
    ).fillna("")

    # ── TE-derived deal lists ─────────────────────────────────────────
    out["Included_Deal_Lists"] = out["Targeting_Expression_ID"].map(
        lambda te_id: te_map.get(te_id, {}).get("included", "")
    )
    out["Excluded_Deal_Lists"] = out["Targeting_Expression_ID"].map(
        lambda te_id: te_map.get(te_id, {}).get("excluded", "")
    )

    # ── Last 3 Days CPM ───────────────────────────────────────────────
    out = out.merge(last3_cpm, on="line_item_id", how="left")
    out["Last_3_Days_CPM"] = pd.to_numeric(out["last_3_days_cpm"], errors="coerce").fillna(0.0)

    # ── Rename to final schema ────────────────────────────────────────
    rename = {
        "line_item_id":        "Line_Item_ID",
        "deal_id":             "Deal_ID",
        "alternative_id":      "Deal_Alternative_ID",
        "name":                "Deal_Name",
        "genre":               "Genre",
        "publisher":           "Publisher",
        "ssp":                 "SSP",
        "device_type":         "Device_Type",
        "deal_source":         "Deal_Source",
        "impression":          "Impressions",
        "media_spend_usd":     "Media_Spend_USD",
        "cpm_usd":             "CPM_USD",
        "bid_shading_fee_usd": "Bid_Shading_Fee_USD",
        "floor_price":         "Floor_Price",
        "category":            "Deal_Category",
    }
    out = out.rename(columns=rename)

    final_cols = [
        "Line_Item_ID", "Deal_ID", "Deal_Alternative_ID", "Deal_Name",
        "Genre", "Publisher", "SSP", "Device_Type", "Deal_Source",
        "Impressions", "Media_Spend_USD", "CPM_USD", "Bid_Shading_Fee_USD",
        "Floor_Price",
        "Pub_Impression_Share_Pct", "Pub_Clearing_CPM_On_LI", "Pub_Global_Clearing_CPM",
        "Pub_Deal_List_ID", "Pub_Deal_List_Name",
        "Deal_Category",
        "Category_Impression_Share_Pct", "Category_Clearing_CPM_On_LI", "Category_Global_Clearing_CPM",
        "Deal_Impression_Share_Pct", "Deal_Clearing_CPM_On_LI", "Deal_Global_Clearing_CPM",
        "Targeting_Expression_ID", "Included_Deal_Lists", "Excluded_Deal_Lists",
        "Bid_Modifier_ID", "CPM_Bid", "Last_3_Days_CPM",
    ]
    # Only keep columns that actually exist (guard against empty ATR edge cases)
    final_cols = [c for c in final_cols if c in out.columns]
    return out[final_cols]


# ── state: CPM history upserts ────────────────────────────────────────────


def _upsert_deal_cpm_history(
    state: StateStore,
    atr: pd.DataFrame,
    deal_to_category: Dict[str, str],
    floor_prices: pd.DataFrame,
) -> None:
    """Daily upsert of deal-level global clearing CPM.

    Mirrors sboUpsertDealCpmLog_ from MP CTV Apps Script.
    Columns: Deal_ID | Deal_Category | Floor_Price | Global_Clearing_CPM | Last_Updated
    """
    if atr.empty:
        return
    deal_agg = atr.groupby("deal_id", as_index=False).agg(
        imps=("impression", "sum"), spend=("media_spend_usd", "sum")
    )
    deal_agg["Global_Clearing_CPM"] = (
        (deal_agg["spend"] / deal_agg["imps"]).where(deal_agg["imps"] > 0, 0) * 1000
    ).round(2)
    fp_map = (
        floor_prices.set_index("deal_id")["floor_price"].to_dict()
        if not floor_prices.empty else {}
    )
    today = datetime.now().isoformat(timespec="seconds")
    new_rows = pd.DataFrame({
        "Deal_ID":              deal_agg["deal_id"].astype(str),
        "Deal_Category":        deal_agg["deal_id"].map(deal_to_category).fillna(""),
        "Floor_Price":          pd.to_numeric(deal_agg["deal_id"].map(fp_map), errors="coerce"),
        "Global_Clearing_CPM":  deal_agg["Global_Clearing_CPM"],
        "Last_Updated":         today,
    })
    existing = state.load("deal_cpm_history") if hasattr(state, "load") else pd.DataFrame()
    if not existing.empty and "Deal_ID" in existing.columns:
        existing = existing[~existing["Deal_ID"].isin(new_rows["Deal_ID"])]
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = new_rows
    state.save("deal_cpm_history", merged)


def _upsert_publisher_cpm_history(
    state: StateStore,
    atr: pd.DataFrame,
    pub_name_to_list: Dict[str, Dict],
) -> None:
    """Daily upsert of publisher-level global clearing CPM.

    Mirrors sboUpsertPubCpmLog_ from MP CTV Apps Script.
    Columns: Publisher | Global_Clearing_CPM | Last_Updated
    """
    if atr.empty:
        return
    # Parse publisher from alternative_id
    atr2 = atr.copy()
    atr2["publisher"] = atr2["alternative_id"].fillna("").astype(str).apply(
        lambda x: _parse_alt_id(x)["publisher"]
    )
    atr2 = atr2[atr2["publisher"] != ""]
    if atr2.empty:
        return
    pub_agg = atr2.groupby("publisher", as_index=False).agg(
        imps=("impression", "sum"), spend=("media_spend_usd", "sum")
    )
    pub_agg["Global_Clearing_CPM"] = (
        (pub_agg["spend"] / pub_agg["imps"]).where(pub_agg["imps"] > 0, 0) * 1000
    ).round(2)
    today = datetime.now().isoformat(timespec="seconds")
    new_rows = pd.DataFrame({
        "Publisher":            pub_agg["publisher"],
        "Global_Clearing_CPM":  pub_agg["Global_Clearing_CPM"],
        "Last_Updated":         today,
    })
    existing = state.load("publisher_cpm_history") if hasattr(state, "load") else pd.DataFrame()
    if not existing.empty and "Publisher" in existing.columns:
        existing = existing[~existing["Publisher"].isin(new_rows["Publisher"])]
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = new_rows
    state.save("publisher_cpm_history", merged)
