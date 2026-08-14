"""Calculate pacing from Beeswax delivery + Salesforce goals.

Port of `calculatePacingFromBW_` (Apps Script Section 16B). Fills cols U–Y
of the Bid Optimizer DataFrame:

    U End_Date            V Days_Remaining       W Pacing_Pct
    X Daily_Imps_Target   Y Pacing_Last_Updated

Pacing formula (matches what AMs see in UP):

    pacing = ((imps_yesterday × days_left) + imps_through_yesterday) / goal

Inputs:
    - 3 small Beeswax reports: lifetime imps, yesterday imps, today imps
      (today is subtracted from lifetime to get "through yesterday")
    - SF Data Import: end date (col N) + Target Impressions (col R)
    - input snapshot: BW↔SF LI ID mapping

Multiple BW LIs can map to the same SF LI — we sum their impressions before
calculating pacing.

NOTE: this can be swapped for a Redshift query later — pacing data lives
there too. The contract (returns DataFrame with U-Y filled) stays the same.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from sbo.beeswax_client import BeeswaxClient
from sbo.config_models import EngineConfig
from sbo.run_storage import RunFolder
from sbo.utils import REPORT_ALIASES, clean_id, normalize_columns


def calculate_pacing_from_bw(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    bid_optimizer: pd.DataFrame,
    input_snapshot: pd.DataFrame,
    sf_data_import: pd.DataFrame,
) -> pd.DataFrame:
    """Fill Bid Optimizer cols U–Y. Returns the modified DataFrame.

    Also writes a UP-Pacing-style snapshot to the run folder (for AM review,
    drop-in replacement for the UP Pacing sheet tab).
    """
    run.log("=== calculate_pacing_from_bw start ===")
    bw.authenticate()

    # 1. Unique BW LI IDs (from bid_optimizer — these are the LIs we care about)
    bw_ids = (
        bid_optimizer["BW_Line_Item_ID"]
        .astype(str)
        .str.strip()
        .replace({"": pd.NA})
        .dropna()
        .unique()
        .tolist()
    )
    if not bw_ids:
        run.log("WARNING: bid_optimizer empty, skipping pacing")
        return bid_optimizer

    # 2. Pull 3 Beeswax reports
    lifetime_imps = _fetch_imps_by_li(bw, bw_ids, bid_day="NOT NULL", label="Lifetime Imps")
    yesterday_imps = _fetch_imps_by_li(bw, bw_ids, bid_day="yesterday", label="Yesterday Imps")
    today_imps = _fetch_imps_by_li(bw, bw_ids, bid_day="today", label="Today Imps")
    run.log(
        f"BW imps: lifetime={len(lifetime_imps):,} | "
        f"yesterday={len(yesterday_imps):,} | today={len(today_imps):,}"
    )

    # 3. BW → SF mapping from the input snapshot
    bw_to_sf = _bw_to_sf_map(input_snapshot)
    run.log(f"BW→SF mapping: {len(bw_to_sf):,} entries")

    # 4. Aggregate at SF level (sum impressions across BW LIs that share an SF ID)
    sf_lifetime: Dict[str, float] = {}
    sf_yesterday: Dict[str, float] = {}
    for bw_id in bw_ids:
        sf_id = bw_to_sf.get(bw_id)
        if not sf_id:
            continue
        through_yesterday = max(
            0, lifetime_imps.get(bw_id, 0) - today_imps.get(bw_id, 0)
        )
        sf_lifetime[sf_id] = sf_lifetime.get(sf_id, 0) + through_yesterday
        sf_yesterday[sf_id] = sf_yesterday.get(sf_id, 0) + yesterday_imps.get(bw_id, 0)

    # 5. SF goals: end_date (col N) + Target Impressions (col R)
    sf_goals = _read_sf_goals(sf_data_import)
    run.log(
        f"SF goals: {len(sf_goals):,} loaded | "
        f"with goal>0: {sum(1 for g in sf_goals.values() if g['goal'] > 0)}"
    )

    # 6. Calculate pacing per SF LI
    pacing_by_sf, end_date_by_sf = _calculate_pacing_per_sf(
        sf_goals=sf_goals,
        sf_lifetime=sf_lifetime,
        sf_yesterday=sf_yesterday,
        run=run,
    )
    run.log(f"Pacing calculated for {len(pacing_by_sf):,} SF LIs")

    # 7. Write UP Pacing snapshot to run folder (AM review artifact)
    _write_up_pacing_snapshot(run, pacing_by_sf)

    # 8. Map back to bid_optimizer rows (each row keyed on BW_Line_Item_ID)
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
    run.log("=== calculate_pacing_from_bw complete ===")
    return df


# ── Beeswax fetch helpers ─────────────────────────────────────────────────


def _fetch_imps_by_li(
    bw: BeeswaxClient, li_ids: List[str], bid_day: str, label: str
) -> Dict[str, float]:
    """Returns {bw_li_id: total_impressions} for the given bid_day filter."""
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
        # Quiet failure mode — return empty so pacing can fall back gracefully.
        # Live runs log this via the run folder's logs.txt elsewhere.
        return {}
    df["impression"] = pd.to_numeric(df["impression"], errors="coerce").fillna(0)
    df["line_item_id"] = df["line_item_id"].astype(str).str.strip()
    agg = df.groupby("line_item_id", as_index=False)["impression"].sum()
    return dict(zip(agg["line_item_id"], agg["impression"].astype(float)))


# ── input parsing helpers ─────────────────────────────────────────────────


def _bw_to_sf_map(input_snapshot: pd.DataFrame) -> Dict[str, str]:
    """Resolve BW LI ID → SF LI ID from the Beeswax Line Item Settings snapshot."""
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


def _read_sf_goals(sf_data_import: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """SF LI ID → {end_date, goal}. Reads cols A (id), N (end date), R (target imps).

    The SF Data Import sheet has its title at row 1 and headers at row 2;
    callers should pass a DataFrame already keyed on the row-2 headers
    (i.e. read with `header_row=2`).
    """
    out: Dict[str, Dict[str, Any]] = {}
    if sf_data_import.empty:
        return out

    # Resolve cols by name (more robust than positional)
    cols = {
        str(c).strip().lower(): c
        for c in sf_data_import.columns
        if c is not None and str(c).strip()
    }
    id_col = (
        cols.get("operative sales order line item id")
        or cols.get("salesforce id")
        or cols.get("sf li id")
        or cols.get("sf_line_item_id")
    )
    end_col = cols.get("end date") or cols.get("end_date")
    goal_col = (
        cols.get("target impressions")
        or cols.get("up impressions")
        or cols.get("impression goal")
    )
    if not (id_col and end_col and goal_col):
        return out

    for _, row in sf_data_import.iterrows():
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
    """Apply the UP pacing formula. Returns (pacing_by_sf, end_date_by_sf).

    pacing = ((imps_yesterday × days_left) + imps_through_yesterday) / goal
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
        run.log(
            f"Pacing skipped: no_goal={skipped_no_goal} no_date={skipped_no_date}"
        )
    return pacing_by_sf, end_date_by_sf


def _write_up_pacing_snapshot(
    run: RunFolder, pacing_by_sf: Dict[str, Dict[str, Any]]
) -> None:
    """Write a UP-Pacing-style table to the run folder for AM review.

    Same shape as the live UP Pacing tab (cols A-F) so AMs can diff
    against their existing reports.
    """
    if not pacing_by_sf:
        return
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
    df = pd.DataFrame(rows)
    run.save_dataframe("up_pacing", df)
