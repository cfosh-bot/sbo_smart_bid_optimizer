"""Persistent state files.

These survive across runs. **Do not delete** — losing `first_run_log` would
cause every line to re-baseline as Day 1 on the next run.

Stored as Parquet under `state/`. All operations are upsert-friendly and
keyed on a clear primary key per file.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


# ── schemas (keep these in sync with podcast.yaml + Apps Script) ─────────

LI_MODIFIER_MAP_COLS = ["BW_Line_Item_ID", "Advertiser_Name", "Bid_Modifier_ID"]
FIRST_RUN_LOG_COLS = ["BW_Line_Item_ID", "End_Date", "First_Run_Date"]
SECOND_RUN_LOG_COLS = ["BW_Line_Item_ID", "End_Date", "Second_Run_Date"]
PAUSED_LOG_COLS = ["BW_Line_Item_ID", "End_Date", "Paused_Date", "Resumed_Date"]
KILL_LOG_COLS = [
    "BW_Line_Item_ID",
    "Action",
    "Action_Date",
    "Deal_ID_and_Modifier_Type",
    "Undo_Date",
    "End_Date",
]
CATEGORY_CPM_COLS = ["Modifier_Category", "Global_Clearing_CPM", "Last_Updated"]


class StateStore:
    """Reads/writes the seven persistent state files."""

    FILES = {
        "li_modifier_map": ("li_modifier_map.parquet", LI_MODIFIER_MAP_COLS),
        "first_run_log": ("first_run_log.parquet", FIRST_RUN_LOG_COLS),
        "second_run_log": ("second_run_log.parquet", SECOND_RUN_LOG_COLS),
        "paused_log": ("paused_log.parquet", PAUSED_LOG_COLS),
        "paused_snapshot": ("paused_snapshot.parquet", ["BW_Line_Item_ID", "Deal_ID", "Paused_Date", "Held_Multiplier", "Basis"]),
        "kill_log": ("kill_log.parquet", KILL_LOG_COLS),
        "category_cpm_history": ("category_cpm_history.parquet", CATEGORY_CPM_COLS),
        # MP CTV-specific CPM history logs
        "deal_cpm_history": ("deal_cpm_history.parquet", ["Deal_ID", "Deal_Category", "Floor_Price", "Global_Clearing_CPM", "Last_Updated"]),
        "publisher_cpm_history": ("publisher_cpm_history.parquet", ["Publisher", "Global_Clearing_CPM", "Last_Updated"]),
        # pacing_history is wide-format (cols = recent dates) — handled separately
    }

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def load(self, name: str) -> pd.DataFrame:
        if name not in self.FILES:
            raise KeyError(f"Unknown state file: {name}")
        filename, cols = self.FILES[name]
        path = self.state_dir / filename
        if not path.exists():
            return pd.DataFrame(columns=cols)
        return pd.read_parquet(path)

    def save(self, name: str, df: pd.DataFrame) -> None:
        if name not in self.FILES:
            raise KeyError(f"Unknown state file: {name}")
        filename, _ = self.FILES[name]
        df.to_parquet(self.state_dir / filename, index=False)

    # ── pacing history (wide-format, special-cased) ───────────

    def load_pacing_history(self, max_runs: int = 4) -> pd.DataFrame:
        """Wide-format: cols = BW_Line_Item_ID + last N date columns.

        Cell values: 'OVER' (≥100% pacing), 'UNDER' (<100%), '' (paused/skip).
        """
        path = self.state_dir / "pacing_history.parquet"
        if not path.exists():
            return pd.DataFrame(columns=["BW_Line_Item_ID"])
        df = pd.read_parquet(path)
        # Trim to most-recent max_runs date columns
        date_cols = [c for c in df.columns if c != "BW_Line_Item_ID"]
        keep = ["BW_Line_Item_ID"] + date_cols[-max_runs:]
        return df[keep]

    def save_pacing_history(self, df: pd.DataFrame) -> None:
        df.to_parquet(self.state_dir / "pacing_history.parquet", index=False)

    # ── pruning helpers ───────────────────────────────────────

    def prune_expired_run_logs(self, today: pd.Timestamp) -> tuple[int, int]:
        """Drop entries where End_Date < today. Returns (first_pruned, second_pruned).

        Also prunes paused_log and paused_snapshot of:
        - Lines with End_Date < today (campaign ended)
        - Lines that have a non-empty Resumed_Date (already back delivering)
        And prunes paused_snapshot of deals whose LI no longer appears in paused_log.
        """
        pruned = []
        for name in ("first_run_log", "second_run_log"):
            df = self.load(name)
            if df.empty:
                pruned.append(0)
                continue
            df["End_Date"] = pd.to_datetime(df["End_Date"], errors="coerce")
            kept = df[(df["End_Date"].isna()) | (df["End_Date"] >= today)]
            pruned.append(len(df) - len(kept))
            self.save(name, kept.reset_index(drop=True))

        # Prune paused_log: remove resumed lines and expired campaigns
        paused = self.load("paused_log")
        if not paused.empty and "End_Date" in paused.columns:
            paused["End_Date"] = pd.to_datetime(paused["End_Date"], errors="coerce")
            resumed_mask = paused["Resumed_Date"].notna() & (paused["Resumed_Date"] != "")
            expired_mask = paused["End_Date"].notna() & (paused["End_Date"] < today)
            kept_paused = paused[~resumed_mask & ~expired_mask].reset_index(drop=True)
            if len(kept_paused) < len(paused):
                self.save("paused_log", kept_paused)
                # Prune paused_snapshot to only LIs still in paused_log
                active_ids = set(kept_paused["BW_Line_Item_ID"].astype(str).str.strip())
                snap = self.load("paused_snapshot")
                if not snap.empty and "BW_Line_Item_ID" in snap.columns:
                    kept_snap = snap[
                        snap["BW_Line_Item_ID"].astype(str).str.strip().isin(active_ids)
                    ].reset_index(drop=True)
                    self.save("paused_snapshot", kept_snap)

        return pruned[0], pruned[1]
