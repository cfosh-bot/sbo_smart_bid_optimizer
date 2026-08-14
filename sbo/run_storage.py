"""Per-run folder helpers.

Each pipeline invocation creates a folder under `runs/`:

    runs/2026-04-27_0300_podcast_full/
        00_run_metadata.json   {start, end, user, config, status}
        01_inputs/             snapshots of sheet tabs we read
        02_atr.parquet         raw all-time report
        03_publisher_stats.parquet
        04_bid_optimizer.parquet
        05_decisions.parquet   engine reason codes per term
        06_push_results.parquet
        beeswax_raw/           raw API responses (auth-stripped)
        logs.txt               structured run log

This is QA gold. When a push goes wrong, you open the folder named in the
Sheet's Run Log and replay step-by-step.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

Phase = Literal["full", "phase1", "phase2", "phase3", "pushonly", "pacing_only"]


class RunFolder:
    """A single run's working directory."""

    def __init__(self, root: Path, tactic: str, phase: Phase):
        self.root = root
        self.tactic = tactic
        self.phase = phase
        self.started_at = datetime.now()
        self.folder_name = self._build_folder_name()
        self.path = self.root / self.folder_name
        self.path.mkdir(parents=True, exist_ok=False)
        (self.path / "01_inputs").mkdir()
        (self.path / "beeswax_raw").mkdir()
        self._metadata: dict[str, Any] = {
            "tactic": tactic,
            "phase": phase,
            "started_at": self.started_at.isoformat(),
            "user": os.environ.get("USER", "unknown"),
            "status": "running",
        }
        self._write_metadata()

    def _build_folder_name(self) -> str:
        ts = self.started_at.strftime("%Y-%m-%d_%H%M")
        return f"{ts}_{self.tactic}_{self.phase}"

    def _write_metadata(self) -> None:
        (self.path / "00_run_metadata.json").write_text(
            json.dumps(self._metadata, indent=2)
        )

    # ── public API ────────────────────────────────────────────

    @staticmethod
    def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
        """Drop blank/unnamed columns and deduplicate column names.

        Sheets tabs often have trailing empty columns that cause pyarrow
        to raise ValueError('Duplicate column names found') on to_parquet.
        """
        # Drop columns whose name is empty, whitespace-only, or NaN
        df = df.loc[:, [c for c in df.columns
                        if c is not None and str(c).strip() not in ("", "nan")]]
        # Deduplicate any remaining duplicate names by appending _1, _2 …
        seen: dict = {}
        new_cols = []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)
        df.columns = new_cols
        return df

    def save_input(self, name: str, df: pd.DataFrame) -> Path:
        """Snapshot a sheet tab we read in. Stored under `01_inputs/`."""
        out = self.path / "01_inputs" / f"{name}.parquet"
        self._clean_df(df).to_parquet(out, index=False)
        return out

    def save_dataframe(self, filename: str, df: pd.DataFrame) -> Path:
        """Save a working DataFrame as Parquet (e.g. 02_atr.parquet)."""
        if not filename.endswith(".parquet"):
            filename = f"{filename}.parquet"
        out = self.path / filename
        self._clean_df(df).to_parquet(out, index=False)
        return out

    def save_csv(self, filename: str, df: pd.DataFrame) -> Path:
        """Save a DataFrame as a human-readable CSV (e.g. 06_push_results.csv).

        Uses utf-8-sig so Excel opens it correctly instead of mangling
        special characters (the ✅/❌ status glyphs, en-dashes, etc.).
        This is separate from save_dataframe/Parquet — Parquet stays the
        machine-readable source of truth for downstream phases; this CSV
        is purely for a human to open and read.
        """
        if not filename.endswith(".csv"):
            filename = f"{filename}.csv"
        out = self.path / filename
        self._clean_df(df).to_csv(out, index=False, encoding="utf-8-sig")
        return out

    def save_beeswax_response(self, label: str, body: str) -> Path:
        """Dump a raw Beeswax response (CSV or JSON text) for replay."""
        ext = "csv" if body.lstrip().startswith(("li,", "line_item_id,")) else "json"
        out = self.path / "beeswax_raw" / f"{label}.{ext}"
        out.write_text(body)
        return out

    def log(self, msg: str) -> None:
        """Append a timestamped log line."""
        line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}\n"
        with (self.path / "logs.txt").open("a") as f:
            f.write(line)

    def update_metadata(self, **kwargs: Any) -> None:
        self._metadata.update(kwargs)
        self._write_metadata()

    def mark_complete(
        self,
        status: Literal["success", "partial", "failed"],
        summary: dict[str, Any] | None = None,
    ) -> None:
        self._metadata["ended_at"] = datetime.now().isoformat()
        self._metadata["status"] = status
        if summary:
            self._metadata["summary"] = summary
        self._write_metadata()


def list_recent_runs(root: Path, limit: int = 20) -> list[Path]:
    """Return the most recent `limit` run folders, newest first."""
    if not root.exists():
        return []
    folders = [p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")]
    return sorted(folders, key=lambda p: p.name, reverse=True)[:limit]
