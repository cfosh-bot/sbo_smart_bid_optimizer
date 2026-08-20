"""One-time backfill for 2026-08-19 -- the day git_guard blocked the entire
pipeline run (no bid decision was made and no per-day delivery was ever
captured for that date -- see the incident write-up).

Reconstructs the missing dashboard row from:
  - A Beeswax performance_agg export scoped to exactly 2026-08-19 (confirmed
    via that report's own "Performance Report Day" column) -- used directly
    as that day's impressions/spend, no cumulative math needed.
  - The bid decision that was ACTUALLY in effect all through 08-19 -- since
    the pipeline never ran that day, whatever 08-20's run started with
    (Effective_Bid_Current) is exactly what 08-19 delivered under, carried
    straight through from 08-18's real decision.
  - Publisher/Category/Floor from 08-20's run, falling back to the
    deal_cpm_history log for anything that run didn't resolve.

Decision_Reason is tagged BACKFILL_ESTIMATED_KILL / BACKFILL_ESTIMATED_HOLD
rather than a real engine code -- no actual decision was made that day.
Kill status (for Avg_Bid exclusion purposes only) is estimated as
Effective_Bid_Current < Floor_Price * 0.95, per instruction.

Usage:
    Dry run (default) -- computes the slice, writes a preview CSV, prints a
    summary, touches nothing else:
        python backfill_2026_08_19.py <performance_agg.csv> <bid_optimizer_run_dir>

    Commit -- actually merges into the live dashboard file (run this on the
    droplet, where the real file paths exist) and re-uploads to the HF
    Dataset repo the public dashboard reads from:
        python backfill_2026_08_19.py <performance_agg.csv> <bid_optimizer_run_dir> --commit
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

BACKFILL_DATE = "2026-08-19"
KILL_FLOOR_RATIO = 0.95


def _clean_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


def load_performance_agg(path: Path, expected_date: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    day_col = "Performance Report Day"
    if day_col in df.columns:
        bad_dates = sorted(set(df[day_col].astype(str)) - {expected_date})
        if bad_dates:
            raise ValueError(
                f"Report contains rows outside {expected_date}: {bad_dates}. "
                f"Expected every row scoped to exactly one day."
            )
    df = df.rename(columns={
        "Line Items Line Item ID": "BW_Line_Item_ID",
        "Performance Report Deal ID": "Deal_ID",
        "Deals Deal Alternative ID": "Deal_Alternative_ID",
        "Performance Report Impressions": "Deal_Impressions_1Day",
        "Performance Report Media Spend": "Deal_Spend_1Day_USD",
    })
    df["BW_Line_Item_ID"] = df["BW_Line_Item_ID"].astype(str).str.strip()
    df["Deal_ID"] = df["Deal_ID"].astype(str).str.strip()
    df["Deal_Impressions_1Day"] = _clean_int(df["Deal_Impressions_1Day"])
    df["Deal_Spend_1Day_USD"] = pd.to_numeric(df["Deal_Spend_1Day_USD"], errors="coerce")
    # A (LI, deal) pair should be unique in this export; guard anyway.
    dupes = df.duplicated(subset=["BW_Line_Item_ID", "Deal_ID"]).sum()
    if dupes:
        print(f"WARNING: {dupes} duplicate (LI, deal) rows in performance_agg export -- summing them.")
        df = df.groupby(["BW_Line_Item_ID", "Deal_ID"], as_index=False).agg(
            Deal_Impressions_1Day=("Deal_Impressions_1Day", "sum"),
            Deal_Spend_1Day_USD=("Deal_Spend_1Day_USD", "sum"),
        )
    return df[["BW_Line_Item_ID", "Deal_ID", "Deal_Impressions_1Day", "Deal_Spend_1Day_USD"]]


def load_bid_snapshot(run_dir: Path) -> pd.DataFrame:
    """Today's bid_optimizer row per (LI, deal) -- Effective_Bid_Current here
    is exactly what was in effect all through the backfill date, since no
    decision changed it that day."""
    df = pd.read_parquet(run_dir / "04_bid_optimizer.parquet")
    cols = [
        "SF_Line_Item_ID", "BW_Line_Item_ID", "Line_Item_Name", "Publisher",
        "Deal_ID", "CPM_Bid", "Floor_Price", "Category", "Pacing_Pct",
        "Effective_Bid_Current", "Targets_537", "_Included_Deal_Lists",
    ]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{run_dir} missing expected columns: {missing}")
    bid = df[cols].rename(columns={"_Included_Deal_Lists": "Included_Deal_Lists"}).copy()
    bid["BW_Line_Item_ID"] = bid["BW_Line_Item_ID"].astype(str).str.strip()
    bid["Deal_ID"] = bid["Deal_ID"].astype(str).str.strip()
    # A (LI, deal) can appear more than once if more than one bid modifier
    # targets the same deal on the same line -- keep the first, matching
    # _publisher_stats_lookup's existing convention.
    return bid.drop_duplicates(subset=["BW_Line_Item_ID", "Deal_ID"], keep="first")


def load_fallback_map(deal_cpm_history_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(deal_cpm_history_path)
    df["Deal_ID"] = df["Deal_ID"].astype(str).str.strip()
    keep = ["Deal_ID"]
    for c in ("Publisher", "Deal_Category", "Floor_Price"):
        if c in df.columns:
            keep.append(c)
    return df[keep].rename(columns={"Deal_Category": "Category"})


def build_backfill_slice(performance_agg_path: Path, bid_run_dir: Path, fallback_path: Path):
    out = load_performance_agg(performance_agg_path, BACKFILL_DATE)
    bid = load_bid_snapshot(bid_run_dir)
    fallback = load_fallback_map(fallback_path)

    out = out.merge(bid, on=["BW_Line_Item_ID", "Deal_ID"], how="left")
    no_bid_match_count = int(out["CPM_Bid"].isna().sum())

    # Fill Publisher/Category/Floor from the fallback log wherever the bid
    # snapshot itself didn't resolve them.
    out = out.merge(fallback, on="Deal_ID", how="left", suffixes=("", "_fb"))
    for c in ("Publisher", "Category", "Floor_Price"):
        fb_col = f"{c}_fb"
        if fb_col in out.columns:
            out[c] = out[c].where(out[c].notna() & (out[c].astype(str).str.strip() != ""), out[fb_col])
            out = out.drop(columns=[fb_col])

    out["Effective_Bid_Current"] = pd.to_numeric(out["Effective_Bid_Current"], errors="coerce")
    out["Floor_Price"] = pd.to_numeric(out["Floor_Price"], errors="coerce")
    killed = out["Effective_Bid_Current"].notna() & out["Floor_Price"].notna() & (
        out["Effective_Bid_Current"] < out["Floor_Price"] * KILL_FLOOR_RATIO
    )
    out["Decision_Reason"] = None
    out.loc[out["Effective_Bid_Current"].notna(), "Decision_Reason"] = "BACKFILL_ESTIMATED_HOLD"
    out.loc[killed, "Decision_Reason"] = "BACKFILL_ESTIMATED_KILL"

    out["Run_Date"] = BACKFILL_DATE
    out["Effective_Bid_New"] = out["Effective_Bid_Current"]

    final_cols = [
        "Run_Date", "SF_Line_Item_ID", "BW_Line_Item_ID", "Line_Item_Name", "Publisher",
        "Deal_ID", "CPM_Bid", "Floor_Price", "Category", "Pacing_Pct",
        "Effective_Bid_Current", "Effective_Bid_New", "Decision_Reason",
        "Deal_Impressions_1Day", "Deal_Spend_1Day_USD", "Targets_537", "Included_Deal_Lists",
    ]
    for c in final_cols:
        if c not in out.columns:
            out[c] = None
    out = out[final_cols]

    summary = {
        "rows": len(out),
        "total_impressions": int(out["Deal_Impressions_1Day"].sum()),
        "total_spend": round(float(out["Deal_Spend_1Day_USD"].sum()), 2),
        "no_bid_match_rows": no_bid_match_count,
        "killed_rows": int(killed.sum()),
    }
    return out, summary


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("performance_agg_csv")
    ap.add_argument("bid_run_dir")
    ap.add_argument("--fallback-path", default="state/marketplace_ctv/deal_cpm_history.parquet")
    ap.add_argument(
        "--commit", action="store_true",
        help="Actually merge into the live file and re-upload to HF. Default is dry-run/preview only.",
    )
    args = ap.parse_args()

    out, summary = build_backfill_slice(
        Path(args.performance_agg_csv), Path(args.bid_run_dir), Path(args.fallback_path),
    )

    print(f"=== Backfill summary for {BACKFILL_DATE} ===")
    for k, v in summary.items():
        print(f"  {k}: {v:,}" if isinstance(v, (int, float)) else f"  {k}: {v}")

    if summary["no_bid_match_rows"]:
        no_bid_path = Path("/tmp/backfill_no_bid_match.csv")
        out[out["CPM_Bid"].isna()].to_csv(no_bid_path, index=False)
        print(
            f"\n  NOTE: {summary['no_bid_match_rows']} rows delivered on {BACKFILL_DATE} but aren't in "
            f"today's bid_optimizer snapshot (no longer targeted, or targeting changed) -- kept with blank "
            f"bid fields rather than dropped. Written to {no_bid_path} for review."
        )

    preview_path = Path(f"/tmp/backfill_{BACKFILL_DATE}_preview.csv")
    out.to_csv(preview_path, index=False)
    print(f"\nPreview written to {preview_path} ({len(out):,} rows).")

    if not args.commit:
        print("\nDry run only -- nothing written to the live file. Re-run with --commit to actually merge this in.")
        return

    print("\n--commit passed -- merging into the live dashboard file...")
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from sbo import pacing_history
    total = pacing_history._merge_into_live_file(out, drop_run_date=BACKFILL_DATE)
    print(f"Live file now has {total:,} total rows.")


if __name__ == "__main__":
    main()
