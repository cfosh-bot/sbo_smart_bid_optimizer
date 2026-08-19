"""
sbo/pacing_history.py

Builds and maintains a rolling dashboard CSV for Marketplace CTV pacing history.

Source: each day's `*_marketplace_ctv_pushonly` run folder's 04_bid_optimizer.parquet
        (the snapshot that fed the actual Beeswax push).

Output:
    /root/sbo/dashboards/mp_ctv_pacing_history.csv.gz   - rolling 90-day live file
    /root/sbo/dashboards/archive/mp_ctv_pacing_history_YYYY-MM.csv.gz - monthly archives, never deleted

CLI:
    python -m sbo.pacing_history backfill --days 14
    python -m sbo.pacing_history append --run-dir runs/2026-08-11_0623_marketplace_ctv_pushonly
    python -m sbo.pacing_history cleanup --keep-days 14
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

RUNS_DIR = Path("/root/sbo/runs")
HISTORY_DIR = Path("/root/sbo/dashboards")
LIVE_FILE = HISTORY_DIR / "mp_ctv_pacing_history.csv.gz"
ARCHIVE_DIR = HISTORY_DIR / "archive"

LIVE_RETENTION_DAYS = 90
DEFAULT_BACKFILL_DAYS = 14
DEFAULT_RAW_KEEP_DAYS = 14

DASHBOARD_COLUMNS = [
    "SF_Line_Item_ID",
    "BW_Line_Item_ID",
    "Line_Item_Name",
    "Publisher",
    "Deal_ID",
    "CPM_Bid",
    "Floor_Price",
    "Category",
    "Pacing_Pct",
    "Effective_Bid_Current",
    "Effective_Bid_New",
    "Decision_Reason",
    "Targets_537",
    "_Included_Deal_Lists",
]

# 04_bid_optimizer.parquet's leading-underscore columns are its own "internal"
# naming convention -- stripped here since this file is a public dashboard
# schema, not an internal pipeline artifact.
DASHBOARD_COLUMN_RENAMES = {"_Included_Deal_Lists": "Included_Deal_Lists"}

# Columns sourced from the paired `_full` run's 03b_deal_performance_1day.parquet
# (see _load_deal_performance_1day) rather than 04_bid_optimizer.parquet.
DEAL_PERF_COLUMNS = ["Deal_Impressions_1Day", "Deal_Spend_1Day_USD"]

# Full live-file column order -- the single source of truth for both the
# pandas empty-frame fallback and the DuckDB merge path below.
# NOT derived by concatenating DASHBOARD_COLUMNS + DEAL_PERF_COLUMNS --
# dashboard_app.py's DuckDB read_csv(..., columns={...}) matches columns by
# POSITION, not name (confirmed directly: a reordered-but-same-named CSV
# raises "Header mismatch at position", it doesn't just remap). This order
# must stay in lockstep with the `columns` dict in dashboard_app.py's
# get_connection() -- if that dict's order ever changes, this must too.
LIVE_FILE_COLUMNS = [
    "Run_Date", "SF_Line_Item_ID", "BW_Line_Item_ID", "Line_Item_Name", "Publisher",
    "Deal_ID", "CPM_Bid", "Floor_Price", "Category", "Pacing_Pct",
    "Effective_Bid_Current", "Effective_Bid_New", "Decision_Reason",
    "Deal_Impressions_1Day", "Deal_Spend_1Day_USD", "Targets_537", "Included_Deal_Lists",
]

DEDUPE_KEYS = ["Run_Date", "BW_Line_Item_ID", "Deal_ID"]

RUN_FOLDER_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{4})_marketplace_ctv_(?P<phase>\w+)$"
)


# ######################################################################
# Run discovery
# ######################################################################


def _parse_run_folder(path: Path) -> Optional[dict]:
    m = RUN_FOLDER_RE.match(path.name)
    if not m:
        return None
    return {
        "path": path,
        "date": m.group("date"),
        "time": m.group("time"),
        "phase": m.group("phase"),
    }


def _run_status_ok(run_dir: Path) -> bool:
    meta_path = run_dir / "00_run_metadata.json"
    if not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text())
    except Exception:
        return False
    return meta.get("status") == "success"


def find_pushonly_runs(min_date: Optional[str] = None) -> list[Path]:
    """One folder per calendar day - the latest successful `pushonly` run for that day.

    min_date: 'YYYY-MM-DD' string; if given, only runs on/after this date are returned.
    """
    if not RUNS_DIR.exists():
        return []
    candidates: dict[str, dict] = {}
    for entry in RUNS_DIR.iterdir():
        if not entry.is_dir():
            continue
        parsed = _parse_run_folder(entry)
        if not parsed or parsed["phase"] != "pushonly":
            continue
        if min_date and parsed["date"] < min_date:
            continue
        if not _run_status_ok(entry):
            continue
        existing = candidates.get(parsed["date"])
        if existing is None or parsed["time"] > existing["time"]:
            candidates[parsed["date"]] = parsed
    return [c["path"] for c in sorted(candidates.values(), key=lambda c: (c["date"], c["time"]))]


def find_all_marketplace_ctv_runs(older_than_date: Optional[str] = None) -> list[Path]:
    """All marketplace_ctv run folders (any phase), optionally filtered to strictly
    before a given 'YYYY-MM-DD' date - used by cleanup(), not the history builder.
    """
    if not RUNS_DIR.exists():
        return []
    out = []
    for entry in RUNS_DIR.iterdir():
        if not entry.is_dir():
            continue
        parsed = _parse_run_folder(entry)
        if not parsed:
            continue
        if older_than_date and parsed["date"] >= older_than_date:
            continue
        out.append(entry)
    return out


# ######################################################################
# Slice extraction
# ######################################################################


def _run_date_str(run_dir: Path) -> str:
    parsed = _parse_run_folder(run_dir)
    if parsed:
        return parsed["date"]
    # fallback: try run_metadata started_at
    meta_path = run_dir / "00_run_metadata.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            return meta["started_at"][:10]
        except Exception:
            pass
    raise ValueError(f"Could not determine run date for {run_dir}")


def _find_full_run_for_date(date_str: str) -> Optional[Path]:
    """Latest successful `_full` run folder for a given date.

    03b_deal_performance_1day.parquet only ever lands in a `_full` run folder —
    `run_pushonly` (sbo/pipeline.py) only ever copies 04_bid_optimizer.parquet
    forward into its own folder, so the pushonly folder `load_run_slice` is
    normally handed can't supply it. We look up that date's `_full` run
    separately instead.
    """
    if not RUNS_DIR.exists():
        return None
    candidates = []
    for entry in RUNS_DIR.iterdir():
        if not entry.is_dir():
            continue
        parsed = _parse_run_folder(entry)
        if not parsed or parsed["phase"] != "full" or parsed["date"] != date_str:
            continue
        if not _run_status_ok(entry):
            continue
        candidates.append(parsed)
    if not candidates:
        return None
    candidates.sort(key=lambda c: c["time"])
    return candidates[-1]["path"]


def _load_deal_performance_1day(full_run_dir: Path) -> pd.DataFrame:
    """Read that day's actual deal-level impressions/spend, if present.

    The 1-day Beeswax fetch is itself non-critical in the pipeline (see
    full_report_mp_ctv.py) — it can be legitimately absent if it errored that
    day. Returns an empty frame with the right columns rather than raising, so
    a missing fetch degrades to blank stats instead of breaking the whole
    day's history append.
    """
    cols = ["BW_Line_Item_ID", "Deal_ID"] + DEAL_PERF_COLUMNS
    path = full_run_dir / "03b_deal_performance_1day.parquet" if full_run_dir else None
    if path is None or not path.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_parquet(path)
    if df.empty or "line_item_id" not in df.columns or "deal_id" not in df.columns:
        return pd.DataFrame(columns=cols)
    out = pd.DataFrame({
        "BW_Line_Item_ID": df["line_item_id"].astype(str).str.strip(),
        "Deal_ID": df["deal_id"].astype(str).str.strip(),
        "Deal_Impressions_1Day": pd.to_numeric(df.get("impression"), errors="coerce"),
        "Deal_Spend_1Day_USD": pd.to_numeric(df.get("media_spend_usd"), errors="coerce"),
    })
    # Defensive: collapse to one row per (LI, deal) in case the report ever
    # returns split rows (e.g. by alternative_id) for the same pair.
    return out.groupby(["BW_Line_Item_ID", "Deal_ID"], as_index=False).sum()


def load_run_slice(run_dir: Path) -> pd.DataFrame:
    """Read 04_bid_optimizer.parquet, select dashboard columns, stamp Run_Date.

    Full-outer-joins in that day's actual deal-level impressions/spend (from
    the paired `_full` run's 03b_deal_performance_1day.parquet). Deals with
    real spend that day but no bid-decision row are kept, with the bid-side
    columns left NULL, rather than dropped — a deal delivering without an
    active decision is a real anomaly worth surfacing, not noise to hide.
    """
    parquet_path = run_dir / "04_bid_optimizer.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"No 04_bid_optimizer.parquet in {run_dir}")
    df = pd.read_parquet(parquet_path)
    missing = [c for c in DASHBOARD_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{run_dir} missing expected columns: {missing}")
    bid = df[DASHBOARD_COLUMNS].copy().rename(columns=DASHBOARD_COLUMN_RENAMES)
    bid["BW_Line_Item_ID"] = bid["BW_Line_Item_ID"].astype(str).str.strip()
    bid["Deal_ID"] = bid["Deal_ID"].astype(str).str.strip()

    run_date = _run_date_str(run_dir)
    full_run_dir = _find_full_run_for_date(run_date)
    deal_perf = _load_deal_performance_1day(full_run_dir)

    out = bid.merge(deal_perf, on=["BW_Line_Item_ID", "Deal_ID"], how="outer")
    out.insert(0, "Run_Date", run_date)
    return out


# ######################################################################
# Live file read/write helpers
# ######################################################################


def _write_live_file(df: pd.DataFrame) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    df = df.sort_values("Run_Date")
    tmp_path = LIVE_FILE.with_suffix(".tmp.gz")
    df.to_csv(tmp_path, compression="gzip", index=False)
    tmp_path.replace(LIVE_FILE)


def _archive_path_for_month(year_month: str) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    return ARCHIVE_DIR / f"mp_ctv_pacing_history_{year_month}.csv.gz"


def _roll_off_old_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Split rows older than LIVE_RETENTION_DAYS into monthly archive files.

    Returns the trimmed live dataframe (rows within the retention window).
    """
    if df.empty:
        return df
    cutoff = (date.today() - timedelta(days=LIVE_RETENTION_DAYS)).isoformat()
    old_mask = df["Run_Date"] < cutoff
    if not old_mask.any():
        return df
    old_rows = df[old_mask].copy()
    keep_rows = df[~old_mask].copy()

    old_rows["_year_month"] = old_rows["Run_Date"].str.slice(0, 7)
    for year_month, group in old_rows.groupby("_year_month"):
        group = group.drop(columns=["_year_month"])
        archive_path = _archive_path_for_month(year_month)
        if archive_path.exists():
            existing = pd.read_csv(archive_path, compression="gzip", dtype=str)
            combined = pd.concat([existing, group], ignore_index=True)
            combined = combined.drop_duplicates(subset=DEDUPE_KEYS, keep="last")
        else:
            combined = group
        tmp_path = archive_path.with_suffix(".tmp.gz")
        combined.to_csv(tmp_path, compression="gzip", index=False)
        tmp_path.replace(archive_path)
        print(f"  Archived {len(group):,} rows -> {archive_path.name}")

    return keep_rows


# ######################################################################
# DuckDB-backed merge (the daily hot path -- append_run, cleanup_old_runs)
# ######################################################################
#
# The daily cron's `append` step used to load the WHOLE live file into a
# pandas DataFrame with dtype=str before touching a single new row. That was
# fine when the file was small, but at 2.5M+ rows it materializes ~776MB of
# Python string objects -- confirmed via journalctl as exactly what got that
# step OOM-killed on the droplet (961MB RAM total). The functions below
# replace that path for the two things that run every single day: appending
# the new slice, and looking up which dates are already captured for
# cleanup. Both stream the live file through DuckDB instead of materializing
# it in Python -- the same fix already applied to the dashboard itself for
# the same reason. build_history() (a bounded, manually-triggered backfill,
# not part of the daily cron) still uses the plain pandas path above; it
# never reads the accumulated live file, so it isn't the OOM risk.


# DuckDB auto-tunes its own buffer/memory budget from the *detected* system
# RAM, which is exactly the wrong thing to rely on here -- tested locally on
# a dev machine, an unbounded connection still peaked over 2GB processing
# this file, because DuckDB happily used what a full-size machine offered.
# The droplet has 961MB total, so the limit is pinned explicitly rather than
# trusted to auto-detection: DuckDB spills to temp disk instead of growing
# past this, which is exactly the tradeoff wanted (slower, not OOM-killed).
DUCKDB_MEMORY_LIMIT = "400MB"
DUCKDB_THREADS = 2


def _bounded_duckdb_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    # The explicit ORDER BY Run_Date in the COPY query below is what actually
    # keeps the output file sorted -- this setting is a separate, internal
    # DuckDB execution-order guarantee for streaming results that isn't needed
    # here, and disabling it is what DuckDB's own OOM error suggested: without
    # it, a 2.5M-row ORDER BY needed more than the 400MB budget to buffer.
    con.execute("PRAGMA preserve_insertion_order=false")
    return con


def _select_cast_varchar(cols: list[str]) -> str:
    return ", ".join(f"CAST({c} AS VARCHAR) AS {c}" for c in cols)


def _combined_relation_sql(drop_run_date: Optional[str]) -> str:
    """SQL for 'new_df UNION ALL the existing live file (minus drop_run_date)'.
    Assumes a `new_df` relation is already registered on the connection."""
    cols_sql = _select_cast_varchar(LIVE_FILE_COLUMNS)
    new_sql = f"SELECT {cols_sql} FROM new_df"
    if not LIVE_FILE.exists():
        return new_sql
    where = f"WHERE Run_Date != '{drop_run_date}'" if drop_run_date else ""
    existing_sql = f"""
        SELECT {cols_sql} FROM read_csv('{LIVE_FILE}', compression='gzip', header=true, all_varchar=true)
        {where}
    """
    return f"{new_sql} UNION ALL BY NAME {existing_sql}"


def _merge_into_live_file(new_df: pd.DataFrame, drop_run_date: Optional[str]) -> int:
    """Combine new_df into the live file, roll old rows off into monthly
    archives, and write the result -- all via DuckDB streaming. Returns the
    resulting live-file row count. The only pandas DataFrame ever fully
    materialized here is `old_rows`, which is small by construction (just
    the tail crossing the 90-day boundary on any given day, not the whole
    history) -- safe for the existing pandas archival logic to handle as-is.
    """
    con = _bounded_duckdb_connection()
    con.register("new_df", new_df)
    combined_sql = _combined_relation_sql(drop_run_date)
    cutoff = (date.today() - timedelta(days=LIVE_RETENTION_DAYS)).isoformat()

    old_rows = con.sql(f"SELECT * FROM ({combined_sql}) WHERE Run_Date < '{cutoff}'").fetchdf()
    if not old_rows.empty:
        _archive_old_rows(old_rows)

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = LIVE_FILE.with_suffix(".tmp.gz")
    # No ORDER BY: sorting 2.5M+ rows needed more than the 400MB budget to
    # buffer (confirmed -- it OOM'd even with preserve_insertion_order off).
    # Nothing downstream depends on physical row order -- the dashboard
    # always queries with an explicit Run_Date predicate, never relies on
    # file order -- so it's dropped rather than raising the memory ceiling.
    con.sql(f"""
        COPY (
            SELECT * FROM ({combined_sql}) WHERE Run_Date >= '{cutoff}'
        ) TO '{tmp_path}' (HEADER, COMPRESSION gzip)
    """)
    tmp_path.replace(LIVE_FILE)

    total = con.sql(f"""
        SELECT COUNT(*) FROM ({combined_sql}) WHERE Run_Date >= '{cutoff}'
    """).fetchone()[0]
    return total


def _archive_old_rows(old_rows: pd.DataFrame) -> None:
    """Archive rows older than the retention window into monthly files.
    old_rows is always small (the tail crossing the 90-day boundary on any
    given day), so pandas here is fine -- this is not the OOM path. Same
    dedup semantics (keep last) as the original _roll_off_old_rows."""
    old_rows = old_rows.copy()
    old_rows["_year_month"] = old_rows["Run_Date"].str.slice(0, 7)
    for year_month, group in old_rows.groupby("_year_month"):
        group = group.drop(columns=["_year_month"])
        archive_path = _archive_path_for_month(year_month)
        if archive_path.exists():
            existing = pd.read_csv(archive_path, compression="gzip", dtype=str)
            combined = pd.concat([existing, group], ignore_index=True)
            combined = combined.drop_duplicates(subset=DEDUPE_KEYS, keep="last")
        else:
            combined = group
        tmp_path = archive_path.with_suffix(".tmp.gz")
        combined.to_csv(tmp_path, compression="gzip", index=False)
        tmp_path.replace(archive_path)
        print(f"  Archived {len(group):,} rows -> {archive_path.name}")


def _live_file_distinct_dates() -> set[str]:
    """Just the distinct Run_Date values from the live file -- used by
    cleanup_old_runs, which only ever needed this one column. Streamed via
    DuckDB instead of pandas-loading all 17 columns of the whole file."""
    if not LIVE_FILE.exists():
        return set()
    con = _bounded_duckdb_connection()
    rows = con.sql(f"""
        SELECT DISTINCT Run_Date FROM read_csv('{LIVE_FILE}', compression='gzip', header=true, all_varchar=true)
    """).fetchall()
    return {r[0] for r in rows}


# ######################################################################
# Public entry points
# ######################################################################


def build_history(days: int = DEFAULT_BACKFILL_DAYS) -> None:
    """One-time backfill: stack the last `days` calendar days of pushonly runs."""
    min_date = (date.today() - timedelta(days=days)).isoformat()
    run_dirs = find_pushonly_runs(min_date=min_date)
    if not run_dirs:
        print(f"No successful pushonly runs found on/after {min_date}.")
        return
    slices = []
    for run_dir in run_dirs:
        try:
            slices.append(load_run_slice(run_dir))
            print(f"  Loaded {run_dir.name}")
        except Exception as e:
            print(f"  SKIPPED {run_dir.name}: {e}")
    if not slices:
        print("Nothing loaded - aborting.")
        return
    combined = pd.concat(slices, ignore_index=True)
    combined = combined.drop_duplicates(subset=DEDUPE_KEYS, keep="last")
    combined = _roll_off_old_rows(combined)
    _write_live_file(combined)
    print(f"Backfill complete: {len(combined):,} rows -> {LIVE_FILE}")


def append_run(run_dir: Path) -> None:
    """Daily append: add one run's slice, replacing any existing rows for that date."""
    run_dir = Path(run_dir)
    new_slice = load_run_slice(run_dir)
    run_date = new_slice["Run_Date"].iloc[0]

    total = _merge_into_live_file(new_slice, drop_run_date=run_date)
    print(f"Appended {len(new_slice):,} rows for {run_date} -> {LIVE_FILE} "
          f"({total:,} total rows in live file)")


def cleanup_old_runs(keep_days: int = DEFAULT_RAW_KEEP_DAYS, dry_run: bool = False) -> None:
    """Delete run folders older than `keep_days`, but only if their date is already
    present in the live history file (safety check - never delete unbacked-up data).
    """
    cutoff_date = (date.today() - timedelta(days=keep_days)).isoformat()
    old_run_dirs = find_all_marketplace_ctv_runs(older_than_date=cutoff_date)
    if not old_run_dirs:
        print(f"No marketplace_ctv run folders older than {cutoff_date}.")
        return

    captured_dates = _live_file_distinct_dates()

    # Also check archives for dates that may have already rolled off the live file
    if ARCHIVE_DIR.exists():
        for archive_file in ARCHIVE_DIR.glob("mp_ctv_pacing_history_*.csv.gz"):
            arch_df = pd.read_csv(archive_file, compression="gzip", dtype=str, usecols=["Run_Date"])
            captured_dates.update(arch_df["Run_Date"].unique())

    deleted, skipped = 0, 0
    for run_dir in old_run_dirs:
        run_date = _run_date_str(run_dir)
        if run_date not in captured_dates:
            print(f"  SKIP (not yet captured in history): {run_dir.name}")
            skipped += 1
            continue
        if dry_run:
            print(f"  [DRY RUN] would delete {run_dir.name}")
        else:
            shutil.rmtree(run_dir)
            print(f"  Deleted {run_dir.name}")
        deleted += 1

    print(f"Cleanup complete: {deleted} deleted, {skipped} skipped (not yet in history).")


# ######################################################################
# CLI
# ######################################################################


def main():
    parser = argparse.ArgumentParser(description="MP CTV pacing dashboard history")
    sub = parser.add_subparsers(dest="command", required=True)

    p_backfill = sub.add_parser("backfill", help="One-time backfill of recent runs")
    p_backfill.add_argument("--days", type=int, default=DEFAULT_BACKFILL_DAYS)

    p_append = sub.add_parser("append", help="Append one run's slice (called daily)")
    p_append.add_argument("--run-dir", required=True)

    p_cleanup = sub.add_parser("cleanup", help="Delete old raw run folders")
    p_cleanup.add_argument("--keep-days", type=int, default=DEFAULT_RAW_KEEP_DAYS)
    p_cleanup.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.command == "backfill":
        build_history(days=args.days)
    elif args.command == "append":
        append_run(Path(args.run_dir))
    elif args.command == "cleanup":
        cleanup_old_runs(keep_days=args.keep_days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
