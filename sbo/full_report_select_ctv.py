"""Build the Publisher Stats DataFrame for Select CTV.

Parallel to full_report.py / full_report_mp_ctv.py but with the Select CTV
structural differences (source: Select CTV Apps Script, 2026-08-14 revision,
Sections 7-9):
    - No content-category deal lists — no category aggregation at all.
    - One floor per deal (not per publisher-list) — Max_Floor == Min_Floor
      by construction downstream in bid_optimizer.py.
    - New Deal_Clearing_CPM_On_LI metric (added 2026-08-14): deal-level,
      line-specific, all-time clearing CPM from ATR. Used by the multiplier
      engine's on-target margin-health trim.
    - Publisher name is parsed from alternative_id the same way MP CTV does,
      but using the exact Select CTV parser (sboParseAltId_): publisher is
      everything between the first segment and the last 3 segments, joined
      by '-', so publisher names containing hyphens parse correctly.

Pipeline:
    1. Fetch ATR                              → 02_atr.parquet
    2. Fetch LI settings + targeting exprs
    3. Fetch Last 3 Days CPM + Last 1 Day Imps
    4. Build pub list map  (listId → pubName), prefix "Select CTV - "
    5. Fetch deal floor prices (from every deal_id seen in the ATR)
    6. Two-pass pandas aggregation            → 03_publisher_stats.parquet

Returns FullReportArtifacts (same dataclass as full_report.py) so
pipeline.py can call any of the three full-report builders interchangeably.
"""

from __future__ import annotations

from typing import Dict, List

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


# ── public entry point ────────────────────────────────────────────────────


def build_full_report_select_ctv(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
) -> FullReportArtifacts:
    """Run the Select CTV full report build. Returns FullReportArtifacts."""
    run.log("=== build_full_report_select_ctv start ===")
    bw.authenticate()

    # 1. ATR
    bw_ids = _read_input_bw_ids(run)
    atr = _fetch_atr(bw, bw_ids, run)
    run.save_dataframe("02_atr", atr)
    run.log(f"ATR fetched: {len(atr):,} rows, {atr['line_item_id'].nunique():,} unique LIs" if not atr.empty else "ATR fetched: 0 rows")

    all_li_ids = bw_ids

    # 2. LI settings + targeting expressions
    li_settings = _fetch_li_settings(bw, all_li_ids, run)
    te_map = _fetch_targeting_expressions(bw, li_settings, run)

    # 3. Last 3 days CPM + last 1 day imps (pause detection)
    last3_cpm = _fetch_last_3_days_cpm(bw, all_li_ids, run)
    last1_imps = _fetch_last_1_day_imps(bw, all_li_ids, run)
    run.save_dataframe("last_day_imps", last1_imps)

    # 4. Publisher list map — Select CTV has no category deal lists at all
    all_lists = bw.fetch_all_lists()
    pub_list_map = _build_pub_list_map(all_lists, cfg)
    pub_name_to_list = _build_pub_name_to_list(pub_list_map)
    run.log(f"Pub list map: {len(pub_list_map):,} publisher lists")

    # 5. Deal floor prices — Select CTV has no category/pub-list-derived deal
    # universe to scope from, so use every deal_id that actually delivered.
    all_deal_ids = (
        atr["deal_id"].dropna().astype(str).str.strip().unique().tolist()
        if not atr.empty and "deal_id" in atr.columns else []
    )
    floor_prices = _fetch_deal_floor_prices(bw, all_deal_ids, run)

    # 6. Build Publisher Stats
    pub_stats = _build_publisher_stats_select_ctv(
        atr=atr,
        li_settings=li_settings,
        te_map=te_map,
        last3_cpm=last3_cpm,
        pub_name_to_list=pub_name_to_list,
        floor_prices=floor_prices,
    )
    run.save_dataframe("03_publisher_stats", pub_stats)
    run.log(f"Publisher stats built: {len(pub_stats):,} rows")

    run.log("=== build_full_report_select_ctv complete ===")
    return FullReportArtifacts(
        publisher_stats=pub_stats,
        li_settings=li_settings,
        last3_cpm=last3_cpm,
        last1_imps=last1_imps,
        te_map=te_map,
        deal_to_mod_type={},  # Select CTV has no category/modifier-type concept
    )


# ── list helpers ──────────────────────────────────────────────────────────


def _build_pub_list_map(all_lists: List[Dict], cfg: EngineConfig) -> Dict[str, str]:
    """listId (str) → publisher name, for lists named 'Select CTV - <name>'."""
    pub_prefix = cfg.beeswax.pub_prefix  # "Select CTV - "
    out: Dict[str, str] = {}
    for lst in all_lists:
        name = lst.get("name") or ""
        if pub_prefix in name:
            list_id = str(lst.get("id", ""))
            pub_name = name.split(pub_prefix, 1)[1].strip()
            out[list_id] = pub_name
    return out


def _build_pub_name_to_list(pub_list_map: Dict[str, str]) -> Dict[str, Dict]:
    """pubName.lower() → {list_id, list_name} for Publisher Stats join."""
    out: Dict[str, Dict] = {}
    for list_id, pub_name in pub_list_map.items():
        out[pub_name.lower()] = {"list_id": list_id, "list_name": f"Select CTV - {pub_name}"}
    return out


# ── alt_id parser ─────────────────────────────────────────────────────────


def _parse_alt_id_select_ctv(alt_id: str) -> Dict[str, str]:
    """Parse Select CTV's alternative_id format.

    Mirrors sboParseAltId_() exactly: genre-publisher(-may contain hyphens-)
    -ssp-deviceType-dealSource. Publisher is everything between the first
    segment and the last 3 segments, joined back with '-' so publisher names
    that themselves contain hyphens still parse correctly. Returns a
    mostly-empty dict if fewer than 5 hyphen-segments are present.
    """
    parts = str(alt_id or "").split("-")
    if len(parts) < 5:
        return {"genre": "", "publisher": "", "ssp": "", "device_type": "", "deal_source": ""}
    genre = parts[0].strip()
    ssp = parts[-3].strip()
    device_type = parts[-2].strip()
    deal_source = parts[-1].strip()
    publisher = "-".join(parts[1:-3]).strip()
    return {
        "genre": genre, "publisher": publisher, "ssp": ssp,
        "device_type": device_type, "deal_source": deal_source,
    }


# ── Publisher Stats aggregation ────────────────────────────────────────────


def _build_publisher_stats_select_ctv(
    atr: pd.DataFrame,
    li_settings: pd.DataFrame,
    te_map: Dict[str, Dict[str, str]],
    last3_cpm: pd.DataFrame,
    pub_name_to_list: Dict[str, Dict],
    floor_prices: pd.DataFrame,
) -> pd.DataFrame:
    """Produce the Select CTV Publisher Stats DataFrame (one row per LI × deal).

    No category dimension — publisher + deal-level aggregation only.
    """
    if atr.empty:
        return pd.DataFrame()

    parsed = atr["alternative_id"].fillna("").astype(str).apply(_parse_alt_id_select_ctv)
    df = atr.copy()
    df["genre"] = parsed.map(lambda x: x["genre"])
    df["publisher"] = parsed.map(lambda x: x["publisher"])
    df["ssp"] = parsed.map(lambda x: x["ssp"])
    df["device_type"] = parsed.map(lambda x: x["device_type"])
    df["deal_source"] = parsed.map(lambda x: x["deal_source"])

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
    # Deal-level aggregation — clearing CPM per deal per LI (new 2026-08-14 metric)
    deal_li = df.groupby(["line_item_id", "deal_id"], as_index=False).agg(
        deal_li_imps=("impression", "sum"), deal_li_spend=("media_spend_usd", "sum")
    )

    out = (
        df.merge(li_tot, on="line_item_id", how="left")
          .merge(pub_li, on=["line_item_id", "publisher"], how="left")
          .merge(pub_global, on="publisher", how="left")
          .merge(deal_li, on=["line_item_id", "deal_id"], how="left")
    )

    out["Pub_Impression_Share_Pct"] = _share(out["pub_li_imps"], out["li_imps"])
    out["Pub_Clearing_CPM_On_LI"] = _cpm(out["pub_li_spend"], out["pub_li_imps"])
    out["Pub_Global_Clearing_CPM"] = _cpm(out["pub_glob_spend"], out["pub_glob_imps"])
    out["Deal_Clearing_CPM_On_LI"] = _cpm(out["deal_li_spend"], out["deal_li_imps"])

    def _pub_list_id(pub: str) -> str:
        return pub_name_to_list.get(pub.lower(), {}).get("list_id", "")

    def _pub_list_name(pub: str) -> str:
        return pub_name_to_list.get(pub.lower(), {}).get("list_name", "")

    out["Pub_Deal_List_ID"] = out["publisher"].apply(_pub_list_id)
    out["Pub_Deal_List_Name"] = out["publisher"].apply(_pub_list_name)

    # Floor price (raw — 1.07 fee applied downstream in bid_optimizer.py)
    out = out.merge(floor_prices, on="deal_id", how="left")

    li_set = li_settings.set_index("line_item_id")
    out["Bid_Modifier_ID"] = out["line_item_id"].map(li_set["bid_modifier_id"]).fillna("")
    out["CPM_Bid"] = pd.to_numeric(
        out["line_item_id"].map(li_set["cpm_bid"]), errors="coerce"
    ).fillna(0.0)
    out["Targeting_Expression_ID"] = out["line_item_id"].map(
        li_set["targeting_expression_id"]
    ).fillna("")

    out["Included_Deal_Lists"] = out["Targeting_Expression_ID"].map(
        lambda te_id: te_map.get(te_id, {}).get("included", "")
    )
    out["Excluded_Deal_Lists"] = out["Targeting_Expression_ID"].map(
        lambda te_id: te_map.get(te_id, {}).get("excluded", "")
    )

    out = out.merge(last3_cpm, on="line_item_id", how="left")
    out["Last_3_Days_CPM"] = pd.to_numeric(out["last_3_days_cpm"], errors="coerce").fillna(0.0)

    rename = {
        "line_item_id": "Line_Item_ID",
        "deal_id": "Deal_ID",
        "alternative_id": "Deal_Alternative_ID",
        "name": "Deal_Name",
        "genre": "Genre",
        "publisher": "Publisher",
        "ssp": "SSP",
        "device_type": "Device_Type",
        "deal_source": "Deal_Source",
        "impression": "Impressions",
        "media_spend_usd": "Media_Spend_USD",
        "cpm_usd": "CPM_USD",
        "bid_shading_fee_usd": "Bid_Shading_Fee_USD",
        "floor_price": "Floor_Price",
    }
    out = out.rename(columns=rename)

    final_cols = [
        "Line_Item_ID", "Deal_ID", "Deal_Alternative_ID", "Deal_Name",
        "Genre", "Publisher", "SSP", "Device_Type", "Deal_Source",
        "Impressions", "Media_Spend_USD", "CPM_USD", "Bid_Shading_Fee_USD",
        "Floor_Price",
        "Pub_Impression_Share_Pct", "Pub_Clearing_CPM_On_LI", "Pub_Global_Clearing_CPM",
        "Pub_Deal_List_ID", "Pub_Deal_List_Name",
        "Deal_Clearing_CPM_On_LI",
        "Targeting_Expression_ID", "Included_Deal_Lists", "Excluded_Deal_Lists",
        "Bid_Modifier_ID", "CPM_Bid", "Last_3_Days_CPM",
    ]
    final_cols = [c for c in final_cols if c in out.columns]
    return out[final_cols]
