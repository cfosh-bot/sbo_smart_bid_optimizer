"""One-time migration: import Apps Script state into Python Parquet state.

The Apps Script tracks Day-1/Day-2 baseline runs, paused lines, kill-log
events, pacing history, etc. in dedicated tabs of the original (multi-tab)
Google Sheet. The Python port stores the same data in `state/*.parquet`
files on the local machine.

On a fresh Python install, those Parquet files are empty — so every line
gets treated as Day 1 even when Apps Script has been tracking it for weeks.
This module reads the Apps Script tabs and seeds the Python state files.

Run this ONCE per machine, before the first real Full Run, to inherit the
Apps Script's run history. After that, Python maintains its own state.

CLI:
    python -m sbo.state_import --sheet-id <google-sheet-id>

Streamlit:
    State tab → "Import state from Apps Script sheet" button.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from sbo.state import StateStore
from sbo.utils import clean_id


# Apps Script tab name → (state file key, expected column list)
# Column lists must match `sbo/state.py` schemas exactly.
APPS_SCRIPT_TABS: Dict[str, Tuple[str, List[str]]] = {
    "Optimizer First Run Log":  ("first_run_log",       ["BW_Line_Item_ID", "End_Date", "First_Run_Date"]),
    "Optimizer Second Run Log": ("second_run_log",      ["BW_Line_Item_ID", "End_Date", "Second_Run_Date"]),
    "SBO Paused Log":           ("paused_log",          ["BW_Line_Item_ID", "End_Date", "Paused_Date", "Resumed_Date"]),
    "Publisher Kill Log":       ("kill_log",            ["BW_Line_Item_ID", "Action", "Action_Date", "Deal_ID_and_Modifier_Type", "Undo_Date", "End_Date"]),
    "LI Modifier Map":          ("li_modifier_map",     ["BW_Line_Item_ID", "Advertiser_Name", "Bid_Modifier_ID"]),
    "Category CPM History":     ("category_cpm_history", ["Modifier_Category", "Global_Clearing_CPM", "Last_Updated"]),
}


def import_apps_script_state(
    sheet_id: str,
    state_dir: Path | str = "state",
    overwrite: bool = False,
    log=print,
) -> Dict[str, int]:
    """Read Apps Script tabs and seed Python state files.

    Args:
        sheet_id: the Google Sheet ID containing the Apps Script tabs
        state_dir: where to write the Parquet files
        overwrite: if False, skip any state file that already has rows
        log: callable for progress messages (defaults to print)

    Returns:
        {state_key: rows_imported} per state file.
    """
    # Deferred import so the file-parsing helpers below stay testable without
    # gspread/OAuth setup. Live invocations (CLI / Streamlit) need this module.
    from sbo.sheets_io import get_authorized_client

    state_dir = Path(state_dir)
    state = StateStore(state_dir)
    client = get_authorized_client()
    book = client.open_by_key(sheet_id)
    counts: Dict[str, int] = {}

    for tab_name, (state_key, cols) in APPS_SCRIPT_TABS.items():
        log(f"\n→ {tab_name}")
        existing = state.load(state_key)
        if not overwrite and not existing.empty:
            log(f"  ⏭️  Skipping — {state_key}.parquet already has {len(existing):,} rows. "
                f"Pass --overwrite to replace.")
            counts[state_key] = 0
            continue
        try:
            ws = book.worksheet(tab_name)
        except Exception:
            log(f"  ⚠️  Tab not found in sheet — skipping.")
            counts[state_key] = 0
            continue
        df = _read_tab_to_df(ws, cols)
        if df.empty:
            log(f"  (empty)")
            counts[state_key] = 0
            continue
        state.save(state_key, df)
        log(f"  ✅  Imported {len(df):,} rows → state/{state_key}.parquet")
        counts[state_key] = len(df)

    # Pacing history is wide-format — different shape, handled separately
    log(f"\n→ SBO Pacing History (wide format)")
    try:
        ws = book.worksheet("SBO Pacing History")
    except Exception:
        log(f"  ⚠️  Tab not found in sheet — skipping.")
        counts["pacing_history"] = 0
    else:
        existing = state.load_pacing_history(max_runs=1000)
        if not overwrite and not existing.empty:
            log(f"  ⏭️  Skipping — pacing_history.parquet already has {len(existing):,} rows. "
                f"Pass --overwrite to replace.")
            counts["pacing_history"] = 0
        else:
            df = _read_pacing_history(ws)
            if df.empty:
                log(f"  (empty)")
                counts["pacing_history"] = 0
            else:
                state.save_pacing_history(df)
                log(f"  ✅  Imported {len(df):,} LI rows × {len(df.columns) - 1} dates "
                    f"→ state/pacing_history.parquet")
                counts["pacing_history"] = len(df)

    return counts


# ── helpers ───────────────────────────────────────────────────────────────


def _read_tab_to_df(ws, expected_cols: List[str]) -> pd.DataFrame:
    """Read a worksheet into a DataFrame with the expected column schema.

    Source-sheet headers may differ slightly from our canonical names; we
    match by normalized header (case + space-insensitive).
    """
    rows = ws.get_all_values()
    if len(rows) < 2:
        return pd.DataFrame(columns=expected_cols)
    headers = rows[0]
    data = rows[1:]

    def _norm(s: str) -> str:
        return str(s).strip().lower().replace(" ", "_")

    # Map each expected col to the source col index (if any)
    norm_to_idx = {_norm(h): i for i, h in enumerate(headers)}
    col_to_idx: Dict[str, int] = {}
    for c in expected_cols:
        idx = norm_to_idx.get(_norm(c))
        if idx is not None:
            col_to_idx[c] = idx

    out_rows = []
    for row in data:
        rec = {}
        for c in expected_cols:
            idx = col_to_idx.get(c)
            val = row[idx] if (idx is not None and idx < len(row)) else ""
            # IDs need clean_id treatment so '1234.0' becomes '1234'
            if c in ("BW_Line_Item_ID", "Bid_Modifier_ID"):
                rec[c] = clean_id(val)
            else:
                rec[c] = val
        out_rows.append(rec)

    df = pd.DataFrame(out_rows, columns=expected_cols)
    # Drop completely empty rows
    df = df[df.ne("").any(axis=1)].reset_index(drop=True)
    return df


def _read_pacing_history(ws) -> pd.DataFrame:
    """Pacing history is wide-format: BW_Line_Item_ID + N date columns.

    Source format from Apps Script: row 1 is headers (BW_Line_Item_ID, then
    date strings); rows 2+ each contain OVER/UNDER/empty signal cells.
    """
    rows = ws.get_all_values()
    if len(rows) < 2:
        return pd.DataFrame(columns=["BW_Line_Item_ID"])
    headers = rows[0]
    if not headers or headers[0].strip().lower() not in ("bw_line_item_id", "bw line item id"):
        # Header layout looks unfamiliar — skip rather than guess
        return pd.DataFrame(columns=["BW_Line_Item_ID"])

    canonical_headers = ["BW_Line_Item_ID"] + [h.strip() for h in headers[1:]]
    data = []
    for row in rows[1:]:
        # Skip empty rows
        if not row or all(c.strip() == "" for c in row):
            continue
        rec = {"BW_Line_Item_ID": clean_id(row[0])}
        for i, date_col in enumerate(canonical_headers[1:], start=1):
            val = row[i].strip() if i < len(row) else ""
            # Only keep recognized signals; everything else becomes ""
            rec[date_col] = val if val in ("OVER", "UNDER") else ""
        if rec["BW_Line_Item_ID"]:
            data.append(rec)
    return pd.DataFrame(data, columns=canonical_headers)


# ── CLI ───────────────────────────────────────────────────────────────────


def main():
    import argparse
    import os
    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(description="Import Apps Script state into Python Parquet state.")
    parser.add_argument("--sheet-id", help="Google Sheet ID (or set SHEET_ID_PODCAST in .env)")
    parser.add_argument(
        "--tactic", default="podcast",
        help="Which tactic env var to use for default sheet ID (podcast/streaming/marketplace_ctv/select_ctv)",
    )
    parser.add_argument("--state-dir", default=os.environ.get("STATE_DIR", "state"))
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Replace existing state files (default: skip non-empty files)",
    )
    args = parser.parse_args()

    sheet_id = args.sheet_id or os.environ.get(f"SHEET_ID_{args.tactic.upper()}")
    if not sheet_id:
        raise SystemExit(
            f"No sheet ID — pass --sheet-id or set SHEET_ID_{args.tactic.upper()} in .env"
        )

    print(f"Importing Apps Script state from sheet {sheet_id} → {args.state_dir}/")
    counts = import_apps_script_state(
        sheet_id=sheet_id, state_dir=args.state_dir, overwrite=args.overwrite,
    )
    print("\n--- Summary ---")
    for k, n in counts.items():
        print(f"  {k}: {n:,} rows")
    total = sum(counts.values())
    print(f"\n✅  Total: {total:,} rows imported.")
    if total == 0 and not args.overwrite:
        print(
            "\nNothing was imported — state files already exist. "
            "Use --overwrite to replace them."
        )


if __name__ == "__main__":
    main()
