"""Build the Publisher Stats DataFrame.

Port of `buildFullReport` (Apps Script Section 8). Replaces the Apps Script
two-pass forEach over the All Time Report with a single pandas groupby
cascade, and writes intermediates to the run folder as Parquet instead of
to extra sheet tabs.

Pipeline:
    1. Fetch ATR (performance_agg, all-time, all LIs)            → 02_atr.parquet
    2. Fetch line item settings + targeting expressions
    3. Fetch Last 3 Days CPM + Last 1 Day Imps                   → last_day_imps
    4. Build Podcast modifier deal-list map (deal_id → category)
    5. Fetch deal_agg floor prices for all relevant deals
    6. Aggregate via pandas groupby (LI, pub, SSP, modifier, deal)
    7. Emit one row per (LI × deal)                              → 03_publisher_stats.parquet
    8. Upsert state/category_cpm_history.parquet

Returns the publisher_stats DataFrame for the next phase.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from sbo.beeswax_client import BeeswaxClient
from sbo.config_models import EngineConfig
from sbo.run_storage import RunFolder
from sbo.state import StateStore


@dataclass
class FullReportArtifacts:
    """Everything build_full_report produces — what downstream phases need.

    pull_bid_modifiers consumes this; calculate_pacing_from_bw uses last1_imps.
    """

    publisher_stats: pd.DataFrame
    li_settings: pd.DataFrame
    last3_cpm: pd.DataFrame
    last1_imps: pd.DataFrame
    te_map: Dict[str, Dict[str, str]]
    deal_to_mod_type: Dict[str, str]
    deal_to_sub_tactic: Dict[str, str] = field(default_factory=dict)


# Column normalization moved to sbo/utils.py — see REPORT_ALIASES dict.
# Backwards-compat aliases for any older imports / tests:
from sbo.utils import REPORT_ALIASES, normalize_columns

_normalize_columns = normalize_columns
_ATR_ALIASES = REPORT_ALIASES["performance_agg"]


# ── public entry point ────────────────────────────────────────────────────


def build_full_report(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
) -> FullReportArtifacts:
    """Run the full report build. Returns all intermediate artifacts."""
    run.log("=== build_full_report start ===")
    bw.authenticate()

    # 1. Pull ATR
    bw_ids = _read_input_bw_ids(run)
    atr = _fetch_atr(bw, bw_ids, run)
    run.save_dataframe("02_atr", atr)
    run.log(f"ATR fetched: {len(atr):,} rows, {atr['line_item_id'].nunique():,} unique LIs")

    # IMPORTANT: Use ALL input BW IDs — not just those that appeared in the ATR.
    # LIs with zero all-time impressions are absent from the ATR but still need
    # their settings fetched so their bid modifier terms appear in the Bid Optimizer.
    # This matches Apps Script pullBidModifiers which calls sboGetLineItemIds_()
    # (reads the full input sheet) rather than deriving IDs from the ATR.
    # Previously using atr-derived IDs caused ~246 zero-delivery LIs to be silently
    # dropped from every downstream step.
    all_li_ids = bw_ids  # full input list — superset of what ATR returns

    # 2. LI settings + targeting expressions
    li_settings = _fetch_li_settings(bw, all_li_ids, run)
    te_map = _fetch_targeting_expressions(bw, li_settings, run)

    # 3. Last 3 days + Last 1 day reports
    last3_cpm = _fetch_last_3_days_cpm(bw, all_li_ids, run)
    last1_imps = _fetch_last_1_day_imps(bw, all_li_ids, run)
    run.save_dataframe("last_day_imps", last1_imps)

    # 4. Modifier deal-list map (deal_id → category like "Spreaker", "iHM O&O", …)
    deal_to_mod_type, deal_to_sub_tactic = _build_modifier_deal_map(bw, cfg, run)

    # 5. Deal floor prices (deal_agg)
    floor_prices = _fetch_deal_floor_prices(bw, list(deal_to_mod_type.keys()), run)

    # 6 + 7. Aggregate + emit publisher stats
    pub_stats = _build_publisher_stats(
        atr=atr,
        li_settings=li_settings,
        te_map=te_map,
        last3_cpm=last3_cpm,
        deal_to_mod_type=deal_to_mod_type,
        floor_prices=floor_prices,
        cfg=cfg,
    )
    run.save_dataframe("03_publisher_stats", pub_stats)
    run.log(f"Publisher stats built: {len(pub_stats):,} rows")

    # 8. Category CPM history (state)
    _upsert_category_cpm_history(state, atr, deal_to_mod_type)

    run.log("=== build_full_report complete ===")
    return FullReportArtifacts(
        publisher_stats=pub_stats,
        li_settings=li_settings,
        last3_cpm=last3_cpm,
        last1_imps=last1_imps,
        te_map=te_map,
        deal_to_mod_type=deal_to_mod_type,
        deal_to_sub_tactic=deal_to_sub_tactic,
    )


# ── inputs ────────────────────────────────────────────────────────────────


def _read_input_bw_ids(run: RunFolder) -> List[str]:
    """Read BW LI IDs from the input snapshot we wrote earlier in the run.

    `Beeswax Line Item Settings` is snapshotted by sheets_io into 01_inputs/
    before we get here. The orchestrator (pipeline.run_full) is responsible
    for that snapshot — we just consume it here.
    """
    inputs = run.path / "01_inputs" / "beeswax_line_item_settings.parquet"
    if not inputs.exists():
        raise FileNotFoundError(
            f"Expected snapshot at {inputs}. The orchestrator should call "
            "sheets.read_tab('beeswax_line_item_settings') and "
            "run.save_input(...) before build_full_report."
        )
    df = pd.read_parquet(inputs)
    bw_ids = (
        df.get("BW LI ID", pd.Series(dtype=str))
        .astype(str)
        .str.strip()
        .replace({"": pd.NA})
        .dropna()
        .unique()
        .tolist()
    )
    # Filter out non-numeric junk.
    # Use int(float(x)) so both "12345" and "12345.0" (gspread float strings) are accepted.
    # Mirrors AppScript: !isNaN(Number(bwId)) which handles both formats.
    def _is_valid_bw_id(x: str) -> bool:
        try:
            return int(float(x)) > 0
        except (ValueError, TypeError):
            return False
    return [str(int(float(x))) for x in bw_ids if _is_valid_bw_id(x)]


# ── ATR ───────────────────────────────────────────────────────────────────


def _fetch_atr(
    bw: BeeswaxClient, bw_ids: List[str], run: RunFolder
) -> pd.DataFrame:
    """All-time performance_agg report keyed on line_item_id + deal_id.

    Mirrors the Apps Script ATR rebuild + binary-split-on-row-cap, but
    BeeswaxClient.fetch_report handles the split internally.
    """
    payload = {
        "view": "performance_agg",
        "fields": [
            "line_item_id",
            "deal_id",
            "alternative_id",
            "name",
            "impression",
            "media_spend_usd",
            "cpm_usd",
            "bid_shading_fee_usd",
        ],
        "filters": {"line_item_id": ",".join(bw_ids), "bid_day": "NOT NULL"},
        "result_format": "csv",
    }
    rows = bw.fetch_report(payload, label="ATR", row_cap=30000)
    df = pd.DataFrame(rows)
    return _normalize_atr(df)


def _fetch_deal_performance_1day(
    bw: BeeswaxClient, bw_ids: List[str], run: RunFolder
) -> pd.DataFrame:
    """Yesterday's performance_agg report, keyed on line_item_id + deal_id.

    Same shape as `_fetch_atr` (deal-level, not just the LI-level yes/no flag
    `_fetch_last_1_day_imps` produces) but scoped to one settled day instead
    of all-time — gives real per-day impressions/spend for the dashboard's
    actual-clearing-CPM view, without diffing cumulative snapshots.

    2026-08-21: bid_day was "1 day" (a rolling relative window from call
    time), which massively undercounted delivery when this runs early the
    next morning -- confirmed empirically: pulling with "1 day" at 6am
    caught ~2% of a line item's true daily volume, producing a wildly
    skewed blended CPM. sbo/pacing.py's own real "yesterday impressions"
    fetch already uses the literal "yesterday" keyword on this same view
    and is correct in production every day -- switched to match it.
    Confirmed via a live pull that "yesterday" returns the exact settled
    total (to the penny) that the platform's own report shows for that
    calendar day.

    Note the resulting day-labeling: the row this feeds is stamped with
    the RUN's own date (that morning's bid decision, correctly same-day),
    while these impressions/spend are for the PRECEDING calendar day --
    by design, since a day's delivery isn't settled until the next
    morning. The dashboard excludes "today" for the same reason.

    Non-critical: callers should catch and log, not fail the run, on error —
    this feeds dashboard history, not the bid push.
    """
    payload = {
        "view": "performance_agg",
        "fields": [
            "line_item_id",
            "deal_id",
            "alternative_id",
            "name",
            "impression",
            "media_spend_usd",
        ],
        "filters": {"line_item_id": ",".join(bw_ids), "bid_day": "yesterday"},
        "result_format": "csv",
    }
    rows = bw.fetch_report(payload, label="Deal Performance 1-Day", row_cap=30000)
    df = pd.DataFrame(rows)
    df = _normalize_atr(df)
    if df.empty:
        return pd.DataFrame(
            columns=["line_item_id", "deal_id", "alternative_id", "name",
                     "impression", "media_spend_usd"]
        )
    return df


def _normalize_atr(df: pd.DataFrame) -> pd.DataFrame:
    """Canonical lowercase column names + numeric coercion.

    Used by both `_fetch_atr` (live API) and the smoke tests / replays
    (which load from xlsx snapshots with Title Case headers).
    """
    if df.empty:
        return df
    df = normalize_columns(df, REPORT_ALIASES["performance_agg"])
    for col in ("impression", "media_spend_usd", "cpm_usd", "bid_shading_fee_usd"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "line_item_id" in df.columns:
        df["line_item_id"] = df["line_item_id"].astype(str).str.strip()
    if "deal_id" in df.columns:
        df["deal_id"] = df["deal_id"].astype(str).str.strip()
    return df


# ── LI settings + targeting expressions ───────────────────────────────────


def _fetch_li_settings(
    bw: BeeswaxClient, li_ids: List[str], run: RunFolder
) -> pd.DataFrame:
    """One row per LI: bid_modifier_id, targeting_expression_id, cpm_bid."""
    out: List[Dict[str, Any]] = []
    for li in bw.fetch_line_items(li_ids):
        bidding = li.get("bidding") or {}
        values = bidding.get("values") or {}
        out.append(
            {
                "line_item_id": str(li.get("id", "")),
                "bid_modifier_id": str(li["bid_modifier_id"])
                if li.get("bid_modifier_id") is not None
                else "",
                "targeting_expression_id": str(li["targeting_expression_id"])
                if li.get("targeting_expression_id") is not None
                else "",
                "cpm_bid": float(values["cpm_bid"]) if values.get("cpm_bid") not in (None, "") else 0.0,
                "advertiser_id": str(li["advertiser_id"])
                if li.get("advertiser_id") is not None
                else "",
                "name": li.get("name", ""),
            }
        )
    df = pd.DataFrame(out)
    run.log(f"LI settings: {len(df):,} fetched")
    return df


def _fetch_targeting_expressions(
    bw: BeeswaxClient, li_settings: pd.DataFrame, run: RunFolder
) -> Dict[str, Dict[str, str]]:
    """te_id → {included: 'list_id,list_id', excluded: '...'} (CSV strings)."""
    if li_settings.empty:
        return {}
    te_ids = (
        li_settings["targeting_expression_id"]
        .replace({"": pd.NA})
        .dropna()
        .unique()
        .tolist()
    )
    out: Dict[str, Dict[str, str]] = {}
    for te in bw.fetch_targeting_expressions(te_ids):
        included = _extract_deal_list_ids(te, bucket="included")
        excluded = _extract_deal_list_ids(te, bucket="excluded")
        out[str(te.get("id", ""))] = {
            "included": ",".join(included),
            "excluded": ",".join(excluded),
        }
    run.log(f"Targeting expressions: {len(out):,} fetched")
    return out


def _extract_deal_list_ids(te: Dict[str, Any], bucket: str) -> List[str]:
    """Mirrors sboGetTargetedDealListIds_ — checks both .all and .any buckets.

    bucket='included' → all + any deal_id_list.any
    bucket='excluded' → none.deal_id_list.any
    """
    modules = (te or {}).get("modules", {}) or {}
    app_site = modules.get("app_site", {}) or {}
    if bucket == "included":
        all_b = (app_site.get("all") or {}).get("deal_id_list", {}) or {}
        any_b = (app_site.get("any") or {}).get("deal_id_list", {}) or {}
        items = (all_b.get("any") or []) + (any_b.get("any") or [])
    else:
        none_b = (app_site.get("none") or {}).get("deal_id_list", {}) or {}
        items = none_b.get("any") or []
    return [str(item["value"]) for item in items if item.get("value") is not None]


# ── small reports ─────────────────────────────────────────────────────────


def _fetch_last_3_days_cpm(
    bw: BeeswaxClient, li_ids: List[str], run: RunFolder
) -> pd.DataFrame:
    """Per-LI CPM over last 3 days. cols: line_item_id, last_3_days_cpm."""
    rows = bw.fetch_report(
        {
            "view": "performance_agg",
            "fields": ["line_item_id", "impression", "media_spend_usd"],
            "filters": {"line_item_id": ",".join(li_ids), "bid_day": "3 days"},
            "result_format": "csv",
        },
        label="Last 3 Days CPM",
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["line_item_id", "last_3_days_cpm"])
    # Beeswax CSV may use Title Case headers — normalize before column access.
    df = normalize_columns(df, REPORT_ALIASES["performance_agg"])
    if "impression" not in df.columns or "media_spend_usd" not in df.columns:
        run.log(
            f"WARNING: Last 3 Days report missing impression/media_spend columns "
            f"(got {list(df.columns)}) — returning empty"
        )
        return pd.DataFrame(columns=["line_item_id", "last_3_days_cpm"])
    df["impression"] = pd.to_numeric(df["impression"], errors="coerce").fillna(0)
    df["media_spend_usd"] = pd.to_numeric(df["media_spend_usd"], errors="coerce").fillna(0)
    agg = df.groupby("line_item_id", as_index=False).agg(
        imps=("impression", "sum"), spend=("media_spend_usd", "sum")
    )
    agg["last_3_days_cpm"] = (
        (agg["spend"] / agg["imps"]).where(agg["imps"] > 0, 0) * 1000
    ).round(2)
    return agg[["line_item_id", "last_3_days_cpm"]]


def _fetch_last_1_day_imps(
    bw: BeeswaxClient, li_ids: List[str], run: RunFolder
) -> pd.DataFrame:
    """Per-LI yesterday-impression flag. cols: BW_Line_Item_ID, Had_Impressions_Yesterday, Last_Updated.

    Produces every input LI ID (so consumers can detect zero-delivery as 'N').

    2026-08-21: was bid_day: "1 day" -- same rolling-window undercounting bug
    fixed in _fetch_deal_performance_1day (confirmed there to catch as little
    as ~2% of true volume when this runs at 6am). Here the consequence is
    worse than a display bug: pipeline.py's paused-state guard trusts an 'N'
    here to mean genuinely zero delivery before confirming a line as
    actively paused. A low-volume but real-delivering line could round down
    to 'N' under the old filter and get wrongly held in a paused state.
    Switched to "yesterday" to match the already-correct pattern in
    sbo/pacing.py.
    """
    rows = bw.fetch_report(
        {
            "view": "performance_agg",
            "fields": ["line_item_id", "impression"],
            "filters": {"line_item_id": ",".join(li_ids), "bid_day": "yesterday"},
            "result_format": "csv",
        },
        label="Last 1 Day Imps",
    )
    df = pd.DataFrame(rows)
    had_map: Dict[str, bool] = {}
    if not df.empty:
        df = normalize_columns(df, REPORT_ALIASES["performance_agg"])
        if "line_item_id" in df.columns and "impression" in df.columns:
            df["impression"] = pd.to_numeric(df["impression"], errors="coerce").fillna(0)
            had = df.groupby("line_item_id", as_index=False)["impression"].sum()
            had_map = {row["line_item_id"]: row["impression"] > 0 for _, row in had.iterrows()}
        else:
            run.log(
                f"WARNING: Last 1 Day report missing line_item_id/impression "
                f"(got {list(df.columns)}) — every LI will be flagged 'N'"
            )
    now = datetime.now()
    return pd.DataFrame(
        {
            "BW_Line_Item_ID": li_ids,
            "Had_Impressions_Yesterday": ["Y" if had_map.get(x, False) else "N" for x in li_ids],
            "Last_Updated": [now] * len(li_ids),
        }
    )


# ── modifier deal-list map ────────────────────────────────────────────────


def _build_modifier_deal_map(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
) -> tuple[Dict[str, str], Dict[str, str]]:
    """Returns (deal_id → category, deal_id → sub_tactic)."""
    all_lists = bw.fetch_all_lists()
    items_by_list = bw.fetch_all_list_items_by_list_id()
    prefix = cfg.beeswax.podcast_mod_prefix
    prefixes = [prefix] if isinstance(prefix, str) else prefix
    out: Dict[str, str] = {}
    sub_tactic_map: Dict[str, str] = {}
    for lst in all_lists:
        name = lst.get("name") or ""
        list_id = str(lst.get("id"))
        matched_prefix = next((p for p in prefixes if p in name), None)
        if not matched_prefix:
            continue
        if "TEMP" in name.upper():
            continue
        category = name.split(matched_prefix, 1)[1].strip()
        # Parse sub-tactic from prefix e.g. "Streaming - Modifier ALL - " → "streaming"
        sub_tactic = matched_prefix.split("-")[0].strip().lower()
        deals = items_by_list.get(list_id, {})
        for deal_id in deals:
            out[deal_id] = category
            if deal_id not in sub_tactic_map and sub_tactic in ("streaming", "podcast"):
                sub_tactic_map[deal_id] = sub_tactic
    run.log(f"Modifier deal map: {len(out):,} deals across categories")
    return out, sub_tactic_map


# ── deal floor prices ─────────────────────────────────────────────────────


def _fetch_deal_floor_prices(
    bw: BeeswaxClient, deal_ids: List[str], run: RunFolder
) -> pd.DataFrame:
    """deal_agg report → cols: deal_id, floor_price (raw — 1.07 fee not applied)."""
    if not deal_ids:
        return pd.DataFrame(columns=["deal_id", "floor_price"])
    rows = bw.fetch_report(
        {
            "view": "deal_agg",
            "fields": ["deal_id", "floor_price"],
            "filters": {"deal_id": ",".join(deal_ids), "bid_hour": "NOT NULL"},
            "result_format": "csv",
        },
        label="Deal Floor Prices",
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["deal_id", "floor_price"])
    df = normalize_columns(df, REPORT_ALIASES["deal_agg"])
    if "deal_id" not in df.columns or "floor_price" not in df.columns:
        run.log(
            f"WARNING: Deal floor report missing deal_id/floor_price "
            f"(got {list(df.columns)}) — returning empty"
        )
        return pd.DataFrame(columns=["deal_id", "floor_price"])
    df["deal_id"] = df["deal_id"].astype(str).str.strip()
    df["floor_price"] = pd.to_numeric(df["floor_price"], errors="coerce")
    # First non-null per deal
    df = df.dropna(subset=["floor_price"]).drop_duplicates(subset=["deal_id"])
    run.log(f"Deal floor prices: {len(df):,} unique deals")
    return df[["deal_id", "floor_price"]]


# ── publisher stats aggregation ───────────────────────────────────────────


def _build_publisher_stats(
    atr: pd.DataFrame,
    li_settings: pd.DataFrame,
    te_map: Dict[str, Dict[str, str]],
    last3_cpm: pd.DataFrame,
    deal_to_mod_type: Dict[str, str],
    floor_prices: pd.DataFrame,
    cfg: EngineConfig,
) -> pd.DataFrame:
    """Produce the 34-col Publisher Stats DataFrame (one row per LI × deal).

    pandas equivalent of the Apps Script two-pass aggregation. All groupbys
    are vectorized — typically <2 seconds for 65k ATR rows.
    """
    if atr.empty:
        return pd.DataFrame()

    # Parse alternative_id: Genre-Publisher-SSP-Price-Format-Position-ModifierGroup
    parts = atr["alternative_id"].fillna("").astype(str).str.split("-", expand=True)
    for i in range(7):
        if i not in parts.columns:
            parts[i] = ""
    df = atr.copy()
    df["genre"] = parts[0].str.strip()
    df["publisher"] = parts[1].str.strip()
    df["ssp"] = parts[2].str.strip()
    df["price"] = parts[3].str.strip()
    df["format"] = parts[4].str.strip()
    df["position"] = parts[5].str.strip()
    df["modifier_group"] = parts[6].str.strip()

    # Modifier category from deal-list map (NOT the parsed col 7 from alt_id)
    df["modifier_deal_list"] = df["deal_id"].map(deal_to_mod_type).fillna("")

    # ── aggregations ─────────────────────────────────────────────────

    def _cpm(spend: pd.Series, imps: pd.Series) -> pd.Series:
        return ((spend / imps).where(imps > 0, 0) * 1000).round(2)

    def _share_pct(num: pd.Series, denom: pd.Series) -> pd.Series:
        return ((num / denom).where(denom > 0, 0) * 100).round(2)

    li_tot = df.groupby("line_item_id", as_index=False).agg(
        li_imps=("impression", "sum"), li_spend=("media_spend_usd", "sum")
    )

    pub_li = df.groupby(["line_item_id", "publisher"], as_index=False).agg(
        pub_li_imps=("impression", "sum"), pub_li_spend=("media_spend_usd", "sum")
    )
    pub_global = df.groupby("publisher", as_index=False).agg(
        pub_glob_imps=("impression", "sum"), pub_glob_spend=("media_spend_usd", "sum")
    )

    ssp_li = df.groupby(["line_item_id", "ssp"], as_index=False).agg(
        ssp_li_imps=("impression", "sum"), ssp_li_spend=("media_spend_usd", "sum")
    )
    ssp_global = df.groupby("ssp", as_index=False).agg(
        ssp_glob_imps=("impression", "sum"), ssp_glob_spend=("media_spend_usd", "sum")
    )

    mod_li = df.groupby(["line_item_id", "modifier_deal_list"], as_index=False).agg(
        mod_li_imps=("impression", "sum"), mod_li_spend=("media_spend_usd", "sum")
    )
    mod_global = df.groupby("modifier_deal_list", as_index=False).agg(
        mod_glob_imps=("impression", "sum"), mod_glob_spend=("media_spend_usd", "sum")
    )

    deal_li = df.groupby(["line_item_id", "deal_id"], as_index=False).agg(
        deal_li_imps=("impression", "sum"), deal_li_spend=("media_spend_usd", "sum")
    )
    deal_global = df.groupby("deal_id", as_index=False).agg(
        deal_glob_imps=("impression", "sum"), deal_glob_spend=("media_spend_usd", "sum")
    )

    # ── merge everything back to ATR rows ────────────────────────────

    out = (
        df.merge(li_tot, on="line_item_id", how="left")
        .merge(pub_li, on=["line_item_id", "publisher"], how="left")
        .merge(pub_global, on="publisher", how="left")
        .merge(ssp_li, on=["line_item_id", "ssp"], how="left")
        .merge(ssp_global, on="ssp", how="left")
        .merge(mod_li, on=["line_item_id", "modifier_deal_list"], how="left")
        .merge(mod_global, on="modifier_deal_list", how="left")
        .merge(deal_li, on=["line_item_id", "deal_id"], how="left")
        .merge(deal_global, on="deal_id", how="left")
    )

    # Compute the CPM + share columns
    out["Pub_Impression_Share_Pct"] = _share_pct(out["pub_li_imps"], out["li_imps"])
    out["Pub_Clearing_CPM_On_LI"] = _cpm(out["pub_li_spend"], out["pub_li_imps"])
    out["Pub_Global_Clearing_CPM"] = _cpm(out["pub_glob_spend"], out["pub_glob_imps"])
    out["SSP_Impression_Share_Pct"] = _share_pct(out["ssp_li_imps"], out["li_imps"])
    out["SSP_Clearing_CPM_On_LI"] = _cpm(out["ssp_li_spend"], out["ssp_li_imps"])
    out["SSP_Global_Clearing_CPM"] = _cpm(out["ssp_glob_spend"], out["ssp_glob_imps"])
    out["Modifier_Impression_Share_Pct"] = _share_pct(out["mod_li_imps"], out["li_imps"])
    out["Modifier_Clearing_CPM_On_LI"] = _cpm(out["mod_li_spend"], out["mod_li_imps"])
    out["Modifier_Global_Clearing_CPM"] = _cpm(out["mod_glob_spend"], out["mod_glob_imps"])
    out["Deal_Clearing_CPM_On_LI"] = _cpm(out["deal_li_spend"], out["deal_li_imps"])
    out["Deal_Global_Clearing_CPM"] = _cpm(out["deal_glob_spend"], out["deal_glob_imps"])

    # Floor price (raw, NOT fee-adjusted yet — that happens in pull_bid_modifiers)
    out = out.merge(floor_prices, on="deal_id", how="left")

    # LI settings join (bid_modifier_id, targeting_expression_id, cpm_bid)
    li_set = li_settings.set_index("line_item_id")
    out["Bid_Modifier_ID"] = out["line_item_id"].map(li_set["bid_modifier_id"]).fillna("")
    out["CPM_Bid"] = pd.to_numeric(out["line_item_id"].map(li_set["cpm_bid"]), errors="coerce").fillna(0.0)
    out["Targeting_Expression_ID"] = out["line_item_id"].map(
        li_set["targeting_expression_id"]
    ).fillna("")

    # TE-derived deal lists (CSV strings)
    out["Included_Deal_Lists"] = out["Targeting_Expression_ID"].map(
        lambda te_id: te_map.get(te_id, {}).get("included", "")
    )
    out["Excluded_Deal_Lists"] = out["Targeting_Expression_ID"].map(
        lambda te_id: te_map.get(te_id, {}).get("excluded", "")
    )

    # Last 3 Days CPM (per LI)
    out = out.merge(last3_cpm, on="line_item_id", how="left")
    out["Last_3_Days_CPM"] = pd.to_numeric(out["last_3_days_cpm"], errors="coerce").fillna(0.0)

    # ── final column shape — match Apps Script Publisher Stats schema ────

    rename = {
        "line_item_id": "Line_Item_ID",
        "deal_id": "Deal_ID",
        "alternative_id": "Deal_Alternative_ID",
        "name": "Deal_Name",
        "genre": "Genre",
        "publisher": "Publisher",
        "ssp": "SSP",
        "price": "Price",
        "format": "Format",
        "position": "Position",
        "modifier_group": "Modifier_Group",
        "impression": "Impressions",
        "media_spend_usd": "Media_Spend_USD",
        "cpm_usd": "CPM_USD",
        "bid_shading_fee_usd": "Bid_Shading_Fee_USD",
        "floor_price": "Floor_Price",
        "modifier_deal_list": "Modifier_Deal_List",
    }
    out = out.rename(columns=rename)

    final_cols = [
        "Line_Item_ID", "Deal_ID", "Deal_Alternative_ID", "Deal_Name",
        "Genre", "Publisher", "SSP", "Price", "Format", "Position", "Modifier_Group",
        "Impressions", "Media_Spend_USD", "CPM_USD", "Bid_Shading_Fee_USD",
        "Floor_Price",
        "Deal_Clearing_CPM_On_LI", "Deal_Global_Clearing_CPM",
        "Pub_Impression_Share_Pct", "Pub_Clearing_CPM_On_LI", "Pub_Global_Clearing_CPM",
        "SSP_Impression_Share_Pct", "SSP_Clearing_CPM_On_LI", "SSP_Global_Clearing_CPM",
        "Modifier_Deal_List",
        "Modifier_Impression_Share_Pct", "Modifier_Clearing_CPM_On_LI", "Modifier_Global_Clearing_CPM",
        "Targeting_Expression_ID", "Included_Deal_Lists", "Excluded_Deal_Lists",
        "Bid_Modifier_ID", "CPM_Bid", "Last_3_Days_CPM",
    ]
    return out[final_cols]


# ── state: category CPM history ───────────────────────────────────────────


def _upsert_category_cpm_history(
    state: StateStore, atr: pd.DataFrame, deal_to_mod_type: Dict[str, str]
) -> None:
    """Append today's modifier-category global clearing CPM to the history.

    Mirrors `sboUpsertCatCpmLog_` — one row per category, today's value
    replaces any prior entry for the same category.
    """
    if atr.empty or not deal_to_mod_type:
        return
    df = atr.copy()
    df["modifier_category"] = df["deal_id"].map(deal_to_mod_type).fillna("")
    df = df[df["modifier_category"] != ""]
    if df.empty:
        return
    cat_agg = df.groupby("modifier_category", as_index=False).agg(
        imps=("impression", "sum"), spend=("media_spend_usd", "sum")
    )
    cat_agg["Global_Clearing_CPM"] = (
        (cat_agg["spend"] / cat_agg["imps"]).where(cat_agg["imps"] > 0, 0) * 1000
    ).round(2)
    today_iso = datetime.now().isoformat(timespec="seconds")
    new_rows = pd.DataFrame(
        {
            "Modifier_Category": cat_agg["modifier_category"],
            "Global_Clearing_CPM": cat_agg["Global_Clearing_CPM"],
            "Last_Updated": today_iso,
        }
    )
    existing = state.load("category_cpm_history")
    # Replace any same-category rows from today; keep older rows intact
    if not existing.empty:
        existing = existing[~existing["Modifier_Category"].isin(new_rows["Modifier_Category"])]
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = new_rows
    state.save("category_cpm_history", merged)
