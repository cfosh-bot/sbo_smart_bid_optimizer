"""One-time correction for the "1 day"-vs-"yesterday" bug (see full_report.py's
_fetch_deal_performance_1day, fixed 2026-08-21): every day's Deal_Impressions_1Day
/Deal_Spend_1Day_USD in the live dashboard file was captured with a Beeswax
filter that massively undercounted delivery (confirmed empirically -- as low
as ~2% of true volume on some line items), which skewed Actual_Clearing_CPM.

This corrects EVERY day in the given date range using a comprehensive
performance_agg export (Day, Line Item ID, Deal ID, Impressions, Spend),
one calendar day at a time:
  - For each (BW_Line_Item_ID, Deal_ID) that already has a bid-decision row
    that day, replaces just its Deal_Impressions_1Day/Deal_Spend_1Day_USD --
    every other column (CPM_Bid, Floor_Price, Publisher, Category,
    Decision_Reason, etc.) is untouched, since those were never wrong.
  - A (LI, deal) with real delivery that day but no bid-decision row is kept
    with blank bid fields (Option A -- surface the anomaly, don't hide it),
    with Publisher/Category/Floor backfilled from deal_cpm_history where
    the bid snapshot didn't resolve them.
  - A (LI, deal) that had a bid-decision row but the corrected export shows
    NO delivery that day gets its impressions/spend cleared to NULL rather
    than left at the old (wrong) value.

Reuses pacing_history._merge_into_live_file() -- the same bounded-memory,
already-tested function the daily cron uses -- once per date, so this never
holds more than one day's slice in memory at a time.

Usage:
    Dry run (default) -- prints a before/after summary per date, writes
    nothing:
        python backfill_full_history_delivery.py <performance_agg.csv>

    Commit -- actually corrects the live file, one date at a time:
        python backfill_full_history_delivery.py <performance_agg.csv> --commit
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

LIVE_FILE_COLUMNS = [
    "Run_Date", "SF_Line_Item_ID", "BW_Line_Item_ID", "Line_Item_Name", "Publisher",
    "Deal_ID", "CPM_Bid", "Floor_Price", "Category", "Pacing_Pct",
    "Effective_Bid_Current", "Effective_Bid_New", "Decision_Reason",
    "Deal_Impressions_1Day", "Deal_Spend_1Day_USD", "Targets_537", "Included_Deal_Lists",
]
BID_COLUMNS = [c for c in LIVE_FILE_COLUMNS if c not in ("Deal_Impressions_1Day", "Deal_Spend_1Day_USD")]


def _clean_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


def load_corrected_report(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.rename(columns={
        "Performance Report Day": "Run_Date",
        "Line Items Line Item ID": "BW_Line_Item_ID",
        "Performance Report Deal ID": "Deal_ID",
        "Performance Report Impressions": "Deal_Impressions_1Day",
        "Performance Report Media Spend": "Deal_Spend_1Day_USD",
    })
    df["BW_Line_Item_ID"] = df["BW_Line_Item_ID"].astype(str).str.strip()
    df["Deal_ID"] = df["Deal_ID"].astype(str).str.strip()
    df["Deal_Impressions_1Day"] = _clean_int(df["Deal_Impressions_1Day"])
    df["Deal_Spend_1Day_USD"] = pd.to_numeric(df["Deal_Spend_1Day_USD"], errors="coerce")
    df = df.groupby(["Run_Date", "BW_Line_Item_ID", "Deal_ID"], as_index=False).agg(
        Deal_Impressions_1Day=("Deal_Impressions_1Day", "sum"),
        Deal_Spend_1Day_USD=("Deal_Spend_1Day_USD", "sum"),
    )
    return df


def load_fallback_map(deal_cpm_history_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(deal_cpm_history_path)
    df["Deal_ID"] = df["Deal_ID"].astype(str).str.strip()
    keep = ["Deal_ID"]
    for c in ("Publisher", "Deal_Category", "Floor_Price"):
        if c in df.columns:
            keep.append(c)
    return df[keep].rename(columns={"Deal_Category": "Category"})


def existing_bid_slice_for_date(history_path: Path, run_date: str) -> pd.DataFrame:
    """Every column except the two delivery columns, for one Run_Date,
    deduped to one row per (BW_Line_Item_ID, Deal_ID) -- straight from the
    gzip CSV via DuckDB, never loading the whole file into pandas."""
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='400MB'")
    con.execute("PRAGMA threads=2")
    cols_sql = ", ".join(BID_COLUMNS)
    df = con.execute(f"""
        SELECT {cols_sql}
        FROM read_csv(
            '{history_path}', compression='gzip', header=true, all_varchar=true
        )
        WHERE Run_Date = ?
    """, [run_date]).fetchdf()
    con.close()
    if df.empty:
        return df
    return df.drop_duplicates(subset=["BW_Line_Item_ID", "Deal_ID"], keep="first")


def build_corrected_day(
    run_date: str, day_correct: pd.DataFrame, history_path: Path, fallback: pd.DataFrame,
):
    existing = existing_bid_slice_for_date(history_path, run_date)
    old_total_imps = 0.0
    if not existing.empty:
        # Old (wrong) totals for this date, pulled straight from the live
        # file's own delivery columns, for the before/after comparison --
        # separate query since BID_COLUMNS above excludes them on purpose.
        con = duckdb.connect()
        con.execute("PRAGMA memory_limit='400MB'")
        con.execute("PRAGMA threads=2")
        old_total_imps = con.execute(f"""
            SELECT SUM(CAST(Deal_Impressions_1Day AS DOUBLE))
            FROM read_csv('{history_path}', compression='gzip', header=true, all_varchar=true)
            WHERE Run_Date = ?
        """, [run_date]).fetchone()[0] or 0.0
        con.close()

    merged = existing.merge(
        day_correct[["BW_Line_Item_ID", "Deal_ID", "Deal_Impressions_1Day", "Deal_Spend_1Day_USD"]],
        on=["BW_Line_Item_ID", "Deal_ID"], how="outer",
    )
    merged = merged.merge(fallback, on="Deal_ID", how="left", suffixes=("", "_fb"))
    for c in ("Publisher", "Category", "Floor_Price"):
        fb_col = f"{c}_fb"
        if fb_col in merged.columns:
            merged[c] = merged[c].where(
                merged[c].notna() & (merged[c].astype(str).str.strip() != ""), merged[fb_col]
            )
            merged = merged.drop(columns=[fb_col])

    merged["Run_Date"] = run_date
    for c in LIVE_FILE_COLUMNS:
        if c not in merged.columns:
            merged[c] = None
    merged = merged[LIVE_FILE_COLUMNS]

    new_total_imps = merged["Deal_Impressions_1Day"].sum()
    summary = {
        "run_date": run_date,
        "rows": len(merged),
        "old_total_impressions": int(old_total_imps),
        "new_total_impressions": int(new_total_imps),
    }
    return merged, summary


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("performance_agg_csv")
    ap.add_argument("--history-path", default="dashboards/mp_ctv_pacing_history.csv.gz")
    ap.add_argument("--fallback-path", default="state/marketplace_ctv/deal_cpm_history.parquet")
    ap.add_argument(
        "--commit", action="store_true",
        help="Actually correct the live file, one date at a time. Default is dry-run/preview only.",
    )
    args = ap.parse_args()

    corrected = load_corrected_report(Path(args.performance_agg_csv))
    fallback = load_fallback_map(Path(args.fallback_path))
    dates = sorted(corrected["Run_Date"].unique())
    print(f"Correcting {len(dates)} dates: {dates[0]} .. {dates[-1]}")

    summaries = []
    for run_date in dates:
        day_correct = corrected[corrected["Run_Date"] == run_date]
        day_df, summary = build_corrected_day(
            run_date, day_correct, Path(args.history_path), fallback,
        )
        summaries.append(summary)
        delta = summary["new_total_impressions"] - summary["old_total_impressions"]
        print(
            f"  {run_date}: rows={summary['rows']:,}  "
            f"old_imps={summary['old_total_impressions']:,}  "
            f"new_imps={summary['new_total_impressions']:,}  "
            f"delta={delta:+,}"
        )

        if args.commit:
            from sbo import pacing_history
            pacing_history.LIVE_FILE = Path(args.history_path)
            total = pacing_history._merge_into_live_file(day_df, drop_run_date=run_date)
            print(f"    -> committed, live file now {total:,} total rows")
        else:
            preview_path = Path(f"/tmp/backfill_delivery_preview_{run_date}.csv")
            day_df.to_csv(preview_path, index=False)

    total_old = sum(s["old_total_impressions"] for s in summaries)
    total_new = sum(s["new_total_impressions"] for s in summaries)
    print(f"\n=== TOTAL across {len(dates)} dates ===")
    print(f"  old: {total_old:,} impressions")
    print(f"  new: {total_new:,} impressions")
    print(f"  delta: {total_new - total_old:+,}")

    if not args.commit:
        print("\nDry run only -- nothing written. Per-date previews in /tmp/backfill_delivery_preview_*.csv. Re-run with --commit to apply.")


if __name__ == "__main__":
    main()
