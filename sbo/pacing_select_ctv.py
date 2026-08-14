"""Calculate pacing for Select CTV from Beeswax delivery + goals.

Port of `calculatePacingFromBW_` (Select CTV Apps Script Section 16B). Fills
the pacing columns of the Bid Optimizer DataFrame: End_Date, Days_Remaining,
Pacing_Pct, Daily_Imps_Target, Pacing_Last_Updated.

Pacing formula — deliberately WITHOUT the "lifetime minus today" subtraction
that the generic `calculate_pacing_from_bw` (Podcast/Streaming/MP CTV) does.
The Select CTV Apps Script comment is explicit: "no today-subtraction needed
for Select CTV scale."

    pacing = ((imps_yesterday × days_left) + imps_lifetime) / goal

Inputs:
    - 2 Beeswax reports: lifetime imps (bid_day NOT NULL), yesterday imps
      (no "today" report needed here, unlike the generic version)
    - "Beeswax Select CTV" tab: end date (col N) + Target Impressions (col R),
      read with header_row=2 (title row 1, columns row 2, data row 3+)
    - input snapshot: BW↔SF LI ID mapping

Multiple BW LIs can map to the same SF LI — their impressions are summed
before calculating pacing, same as the generic version.

Also mirrors the Apps Script's `UP Pacing` sheet overwrite — writes a
summary table (SF_Line_Item_ID, Pacing_Pct, Imps_Yesterday, Imps_Lifetime,
Impression_Goal, Days_Remaining) for AM review/comparison against the
manual process. Callers write this back to the live `up_pacing` tab.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from sbo.beeswax_client import BeeswaxClient
from sbo.config_models import EngineConfig
from sbo.run_storage import RunFolder
from sbo.utils import REPORT_ALIASES, clean_id, normalize_columns


def calculate_pacing_select_ctv(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    bid_optimizer: pd.DataFrame,
    input_snapshot: pd.DataFrame,
    beeswax_select_ctv: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fill Bid Optimizer pacing columns. Returns (bid_optimizer, up_pacing_summary).

    `up_pacing_summary` mirrors the Apps Script's overwrite of the live
    "UP Pacing" sheet tab — the caller is responsible for writing it there.
    """
    run.log("=== calculate_pacing_select_ctv start ===")
    bw.authenticate()

    bw_ids = (
        bid_optimizer["BW_Line_Item_ID"]
        .astype(str).str.strip()
        .replace({"": pd.NA}).dropna().unique().tolist()
    )
    if not bw_ids:
        run.log("WARNING: bid_optimizer empty, skipping pacing")
        return bid_optimizer, pd.DataFrame()

    # 1. Pull 2 Beeswax reports — lifetime + yesterday (no "today" report
    # needed; Select CTV does not subtract partial-day-in-progress delivery)
    lifetime_imps = _fetch_imps_by_li(bw, bw_ids, bid_day="NOT NULL", label="Lifetime Imps (Select CTV)")
    yesterday_imps = _fetch_imps_by_li(bw, bw_ids, bid_day="yesterday", label="Yesterday Imps (Select CTV)")
    run.log(f"BW imps: lifetime={len(lifetime_imps):,} | yesterday={len(yesterday_imps):,}")

    # 2. BW → SF mapping
    bw_to_sf = _bw_to_sf_map(input_snapshot)
    run.log(f"BW→SF mapping: {len(bw_to_sf):,} entries")

    # 3. Aggregate at SF level (sum across BW LIs sharing an SF ID)
    sf_lifetime: Dict[str, float] = {}
    sf_yesterday: Dict[str, float] = {}
    for bw_id in bw_ids:
        sf_id = bw_to_sf.get(bw_id)
        if not sf_id:
            continue
        sf_lifetime[sf_id] = sf_lifetime.get(sf_id, 0) + lifetime_imps.get(bw_id, 0)
        sf_yesterday[sf_id] = sf_yesterday.get(sf_id, 0) + yesterday_imps.get(bw_id, 0)

    # 4. SF goals from "Beeswax Select CTV": end_date (col N) + goal (col R)
    sf_goals = _read_select_ctv_goals(beeswax_select_ctv)
    run.log(
        f"Select CTV goals: {len(sf_goals):,} loaded | "
        f"with goal>0: {sum(1 for g in sf_goals.values() if g['goal'] > 0)}"
    )

    # 5. Calculate pacing per SF LI — NO today-subtraction
    pacing_by_sf, end_date_by_sf = _calculate_pacing_per_sf(
        sf_goals=sf_goals, sf_lifetime=sf_lifetime, sf_yesterday=sf_yesterday, run=run,
    )
    run.log(f"Pacing calculated for {len(pacing_by_sf):,} SF LIs")

    up_pacing_summary = _build_up_pacing_summary(pacing_by_sf)

    # 6. Map back to bid_optimizer rows (each row keyed on BW_Line_Item_ID)
    df = bid_optimizer.copy()
    today = pd.Timestamp(datetime.now().date())
    now_str = datetime.now().strftime("%m/%d/%Y %H:%M") + " [BW]"

    end_dates: List[Any] = []
    days_rems: List[Any] = []
    pacings: List[Any] = []
    daily_targets: List[Any] = []
    timestamps: List[Any] = []
    matched = 0
    unmatched = 0

    for _, row in df.iterrows():
        bw_id = str(row["BW_Line_Item_ID"]).strip()
        sf_id = str(row.get("SF_Line_Item_ID", "")).strip() or bw_to_sf.get(bw_id, "")
        end_date_val = end_date_by_sf.get(sf_id, pd.NaT)
        end_dates.append(end_date_val)

        days_rem = 0
        if pd.notna(end_date_val) and end_date_val != "":
            try:
                end_d = pd.Timestamp(end_date_val).normalize()
                days_rem = max(0, (end_d - today).days)
            except (TypeError, ValueError):
                days_rem = 0
        days_rems.append(days_rem)

        if sf_id and sf_id in pacing_by_sf:
            p = pacing_by_sf[sf_id]
            pacings.append(p["pacing_pct"])
            daily_targets.append(p["imps_yesterday"])
            timestamps.append(now_str)
            matched += 1
        else:
            pacings.append(0.0)
            daily_targets.append(0.0)
            timestamps.append(pd.NaT)
            unmatched += 1

    df["End_Date"] = end_dates
    df["Days_Remaining"] = days_rems
    df["Pacing_Pct"] = pacings
    df["Daily_Imps_Target"] = daily_targets
    df["Pacing_Last_Updated"] = timestamps

    run.log(f"Pacing applied to Bid Optimizer: matched={matched:,} unmatched={unmatched:,}")
    run.log("=== calculate_pacing_select_ctv complete ===")
    return df, up_pacing_summary


# ── Beeswax fetch helpers ─────────────────────────────────────────────────


def _fetch_imps_by_li(
    bw: BeeswaxClient, li_ids: List[str], bid_day: str, label: str
) -> Dict[str, float]:
    rows = bw.fetch_report(
        {
            "view": "performance_agg",
            "fields": ["line_item_id", "impression"],
            "filters": {"line_item_id": ",".join(li_ids), "bid_day": bid_day},
            "result_format": "csv",
        },
        label=label,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return {}
    df = normalize_columns(df, REPORT_ALIASES["performance_agg"])
    if "line_item_id" not in df.columns or "impression" not in df.columns:
        return {}
    df["impression"] = pd.to_numeric(df["impression"], errors="coerce").fillna(0)
    df["line_item_id"] = df["line_item_id"].astype(str).str.strip()
    agg = df.groupby("line_item_id", as_index=False)["impression"].sum()
    return dict(zip(agg["line_item_id"], agg["impression"].astype(float)))


# ── input parsing helpers ─────────────────────────────────────────────────


def _bw_to_sf_map(input_snapshot: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if input_snapshot.empty:
        return out
    cols = {
        str(c).lower().strip(): c
        for c in input_snapshot.columns
        if c is not None and str(c).strip()
    }
    sf_col = cols.get("sf li id") or cols.get("sf_li_id")
    bw_col = cols.get("bw li id") or cols.get("bw_li_id")
    if not sf_col or not bw_col:
        return out
    for _, row in input_snapshot.iterrows():
        sf = "" if pd.isna(row[sf_col]) else clean_id(row[sf_col])
        bw = "" if pd.isna(row[bw_col]) else str(row[bw_col]).strip()
        if sf and bw and bw not in ("0", "nan"):
            out[bw] = sf
    return out


def _read_select_ctv_goals(beeswax_select_ctv: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """SF LI ID → {end_date, goal}. Reads col A (SF ID), col N (End Date),
    col R (Target Impressions / "Goal") of the "Beeswax Select CTV" tab.

    Caller must pass a DataFrame already read with header_row=2 (title row 1,
    real column headers row 2, data starts row 3).
    """
    out: Dict[str, Dict[str, Any]] = {}
    if beeswax_select_ctv.empty:
        return out

    cols = {
        str(c).strip().lower(): c
        for c in beeswax_select_ctv.columns
        if c is not None and str(c).strip()
    }
    id_col = (
        cols.get("sf li id") or cols.get("sf_line_item_id")
        or cols.get("operative sales order line item id") or cols.get("salesforce id")
    )
    end_col = cols.get("end date") or cols.get("end_date")
    goal_col = (
        cols.get("target impressions") or cols.get("up impressions") or cols.get("impression goal")
    )
    if not (id_col and end_col and goal_col):
        return out

    for _, row in beeswax_select_ctv.iterrows():
        sf_id = "" if pd.isna(row[id_col]) else clean_id(row[id_col])
        if not sf_id:
            continue
        end_val = row[end_col] if not pd.isna(row[end_col]) else ""
        try:
            goal_val = float(row[goal_col]) if not pd.isna(row[goal_col]) else 0
        except (TypeError, ValueError):
            goal_val = 0
        out[sf_id] = {"end_date": end_val, "goal": goal_val}
    return out


# ── pacing math ───────────────────────────────────────────────────────────


def _calculate_pacing_per_sf(
    sf_goals: Dict[str, Dict[str, Any]],
    sf_lifetime: Dict[str, float],
    sf_yesterday: Dict[str, float],
    run: RunFolder,
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """pacing = ((imps_yesterday × days_left) + imps_lifetime) / goal.

    No today-subtraction (unlike the generic Podcast/Streaming/MP CTV path).
    """
    pacing_by_sf: Dict[str, Dict[str, Any]] = {}
    end_date_by_sf: Dict[str, Any] = {}
    today = pd.Timestamp(datetime.now().date())
    skipped_no_goal = 0
    skipped_no_date = 0

    for sf_id, g in sf_goals.items():
        end_date_by_sf[sf_id] = g.get("end_date", "")
        goal = g.get("goal") or 0
        if not goal or goal <= 0:
            skipped_no_goal += 1
            continue
        try:
            end_d = pd.Timestamp(g["end_date"]).normalize()
        except (TypeError, ValueError):
            skipped_no_date += 1
            continue
        days_left = max(0, (end_d - today).days)
        imps_yesterday = sf_yesterday.get(sf_id, 0)
        imps_lifetime = sf_lifetime.get(sf_id, 0)
        projected_total = (imps_yesterday * days_left) + imps_lifetime
        pacing_pct = projected_total / goal
        pacing_by_sf[sf_id] = {
            "pacing_pct": pacing_pct,
            "days_left": days_left,
            "end_date": g["end_date"],
            "imps_yesterday": imps_yesterday,
            "imps_lifetime": imps_lifetime,
            "goal": goal,
        }

    if skipped_no_goal or skipped_no_date:
        run.log(f"Pacing skipped: no_goal={skipped_no_goal} no_date={skipped_no_date}")
    return pacing_by_sf, end_date_by_sf


def _build_up_pacing_summary(pacing_by_sf: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    """Mirrors the Apps Script overwriting the live 'UP Pacing' tab with a
    summary table for AM review/comparison against the manual process.
    """
    if not pacing_by_sf:
        return pd.DataFrame()
    rows = [
        {
            "SF_Line_Item_ID": sf_id,
            "Pacing_Pct": p["pacing_pct"],
            "Imps_Yesterday": p["imps_yesterday"],
            "Imps_Lifetime": p["imps_lifetime"],
            "Impression_Goal": p["goal"],
            "Days_Remaining": p["days_left"],
        }
        for sf_id, p in pacing_by_sf.items()
    ]
    return pd.DataFrame(rows)
