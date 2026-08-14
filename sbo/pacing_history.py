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
    "Publisher",
    "Deal_ID",
    "CPM_Bid",
    "Floor_Price",
    "Category",
    "Pacing_Pct",
    "Effective_Bid_Current",
    "Effective_Bid_New",
    "Decision_Reason",
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


def load_run_slice(run_dir: Path) -> pd.DataFrame:
    """Read 04_bid_optimizer.parquet, select dashboard columns, stamp Run_Date."""
    parquet_path = run_dir / "04_bid_optimizer.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"No 04_bid_optimizer.parquet in {run_dir}")
    df = pd.read_parquet(parquet_path)
    missing = [c for c in DASHBOARD_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{run_dir} missing expected columns: {missing}")
    out = df[DASHBOARD_COLUMNS].copy()
    out.insert(0, "Run_Date", _run_date_str(run_dir))
    return out


# ######################################################################
# Live file read/write helpers
# ######################################################################


def _read_live_file() -> pd.DataFrame:
    if not LIVE_FILE.exists():
        return pd.DataFrame(columns=["Run_Date"] + DASHBOARD_COLUMNS)
    return pd.read_csv(LIVE_FILE, compression="gzip", dtype=str)


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

    live = _read_live_file()
    live = live[live["Run_Date"] != run_date]  # drop same-day rows in case of rerun
    combined = pd.concat([live, new_slice], ignore_index=True)
    combined = _roll_off_old_rows(combined)
    _write_live_file(combined)
    print(f"Appended {len(new_slice):,} rows for {run_date} -> {LIVE_FILE} "
          f"({len(combined):,} total rows in live file)")


def cleanup_old_runs(keep_days: int = DEFAULT_RAW_KEEP_DAYS, dry_run: bool = False) -> None:
    """Delete run folders older than `keep_days`, but only if their date is already
    present in the live history file (safety check - never delete unbacked-up data).
    """
    cutoff_date = (date.today() - timedelta(days=keep_days)).isoformat()
    old_run_dirs = find_all_marketplace_ctv_runs(older_than_date=cutoff_date)
    if not old_run_dirs:
        print(f"No marketplace_ctv run folders older than {cutoff_date}.")
        return

    live = _read_live_file()
    captured_dates = set(live["Run_Date"].unique()) if not live.empty else set()

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
