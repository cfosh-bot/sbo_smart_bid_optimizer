"""One-time script to seed the deal Publisher/Category/Floor fallback log.

The daily MP CTV run only knows a deal's Publisher/Category/Floor once that
deal has real delivery history to derive them from (see bid_optimizer.py's
fallback-log logic, added 2026-08-19). For deals that predate that history
-- or simply haven't delivered on a given line item yet -- this backfills
the log from a manually-compiled CSV so the fallback has something to serve
immediately, instead of waiting for delivery to happen naturally.

This is a GAP-FILL, not an overwrite: for a Deal_ID already in the log, only
its currently-blank fields get filled from the CSV. A field the log already
has a real value for is left alone -- the daily run's own data is treated as
more current than a one-time manual backfill.

Run this from the SBO Python Engine root directory, on whichever machine
actually holds the live state/ directory (the droplet, not a local dev
checkout) -- unless you're deliberately seeding a local copy for testing:

    python seed_deal_backlog_mapping.py "/path/to/SBO backlog mapping starter.csv"

Expected CSV columns (case/spacing-insensitive): Deal ID, Publisher Name,
Media Category, Floor Price. Blank cells are skipped -- they never overwrite
a real value, and they're never used to fill a gap either (there's nothing
to fill with).
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from sbo.state import StateStore

STATE_DIR = Path("state/marketplace_ctv")


def _clean_money(val) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace("$", "").replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python seed_deal_backlog_mapping.py <path-to-csv>")
    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    cols = {str(c).strip().lower(): c for c in df.columns}
    deal_col = cols.get("deal id") or cols.get("deal_id")
    pub_col = cols.get("publisher name") or cols.get("publisher")
    cat_col = cols.get("media category") or cols.get("category")
    floor_col = cols.get("floor price") or cols.get("floor_price")
    if not deal_col:
        raise SystemExit(f"Couldn't find a Deal ID column in {list(df.columns)}")

    state = StateStore(STATE_DIR)
    existing = state.load("deal_cpm_history")
    # Publisher was added to this file's schema 2026-08-19 -- a parquet
    # written by an older pipeline run won't have the column yet.
    if "Publisher" not in existing.columns:
        existing["Publisher"] = ""
    existing_by_id = (
        {str(r["Deal_ID"]): i for i, r in existing.iterrows()}
        if not existing.empty else {}
    )

    now = datetime.now().isoformat(timespec="seconds")
    added, filled_pub, filled_cat, filled_floor, untouched, blank_rows = 0, 0, 0, 0, 0, 0

    for _, row in df.iterrows():
        deal_id = str(row.get(deal_col, "")).strip()
        if not deal_id:
            continue
        pub = str(row.get(pub_col, "")).strip() if pub_col else ""
        cat = str(row.get(cat_col, "")).strip() if cat_col else ""
        floor = _clean_money(row.get(floor_col)) if floor_col else None
        if not pub and not cat and floor is None:
            blank_rows += 1
            continue

        if deal_id not in existing_by_id:
            new_row = {
                "Deal_ID": deal_id,
                "Deal_Category": cat,
                "Floor_Price": floor,
                "Global_Clearing_CPM": None,
                "Last_Updated": now,
                "Publisher": pub,
            }
            existing = pd.concat([existing, pd.DataFrame([new_row])], ignore_index=True)
            existing_by_id[deal_id] = len(existing) - 1
            added += 1
            continue

        idx = existing_by_id[deal_id]
        touched = False
        if pub and not str(existing.at[idx, "Publisher"]).strip():
            existing.at[idx, "Publisher"] = pub
            filled_pub += 1
            touched = True
        if cat and not str(existing.at[idx, "Deal_Category"]).strip():
            existing.at[idx, "Deal_Category"] = cat
            filled_cat += 1
            touched = True
        if floor is not None and pd.isna(existing.at[idx, "Floor_Price"]):
            existing.at[idx, "Floor_Price"] = floor
            filled_floor += 1
            touched = True
        if touched:
            existing.at[idx, "Last_Updated"] = now
        else:
            untouched += 1

    state.save("deal_cpm_history", existing)

    print(f"Seeded from: {csv_path}")
    print(f"  New deals added:              {added:,}")
    print(f"  Existing deals gap-filled:")
    print(f"    Publisher filled:           {filled_pub:,}")
    print(f"    Category filled:            {filled_cat:,}")
    print(f"    Floor filled:               {filled_floor:,}")
    print(f"  Existing deals already full (untouched): {untouched:,}")
    print(f"  Fully blank CSV rows skipped: {blank_rows:,}")
    print(f"  Log now has {len(existing):,} total deals -> {STATE_DIR / 'deal_cpm_history.parquet'}")


if __name__ == "__main__":
    main()
