"""Google Sheets I/O with per-user OAuth.

First launch: opens the system browser to Google's consent screen, the user
logs in, and the resulting refresh token is cached at
`credentials/token.json`. Subsequent runs use that cache silently.

The 8 AM-facing tabs we read/write (per podcast.yaml):
    - Beeswax Line Item Settings  (read)
    - SF Data Import              (read)
    - UP Pacing                   (read+write)
    - Bid Optimizer               (write — pre-push review)
    - Pipeline State              (read+write — run lock)
    - Run Log                     (write — append per run)
    - Pause Log                   (read+write)
    - Reason Key                  (write once, read for UI)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import gspread
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    # Full Drive scope (not the narrower drive.file) — the Podcast/Streaming/
    # Total Audio Drive-snapshot feature (pipeline._save_drive_snapshot)
    # creates a new file and moves it into a specific shared folder outside
    # the app's own file set, which drive.file cannot do.
    "https://www.googleapis.com/auth/drive",
]


def get_authorized_client(
    client_secrets_path: str | Path | None = None,
    token_cache_path: str | Path | None = None,
) -> gspread.Client:
    """Returns an authorized gspread client. Triggers OAuth flow on first run."""
    client_secrets_path = Path(
        client_secrets_path
        or os.environ.get("GOOGLE_OAUTH_CLIENT_SECRETS", "credentials/oauth_client.json")
    )
    token_cache_path = Path(
        token_cache_path
        or os.environ.get("GOOGLE_OAUTH_TOKEN_CACHE", "credentials/token.json")
    )

    creds: Credentials | None = None
    if token_cache_path.exists():
        creds = Credentials.from_authorized_user_file(
            str(token_cache_path), scopes=SCOPES
        )

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not client_secrets_path.exists():
                raise FileNotFoundError(
                    f"OAuth client secrets not found at {client_secrets_path}. "
                    "Download from GCP Console (OAuth 2.0 client → Desktop app) "
                    "and place there, then re-run."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secrets_path), SCOPES
            )
            creds = flow.run_local_server(port=0)
        token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        token_cache_path.write_text(creds.to_json())

    return gspread.authorize(creds)


class SheetsIO:
    """Thin wrapper that maps tab names → DataFrames and back."""

    def __init__(self, sheet_id: str, tab_names: dict[str, str]):
        self.sheet_id = sheet_id
        self.tab_names = tab_names  # logical key → actual tab name
        self._client = get_authorized_client()
        self._book = self._client.open_by_key(sheet_id)

    # ── reads ─────────────────────────────────────────────────

    def read_tab(
        self, logical_name: str, header_row: int = 1, skip_rows: int = 0
    ) -> pd.DataFrame:
        """Read a tab into a DataFrame.

        Args:
            logical_name: key from `tab_names` (e.g. 'beeswax_line_item_settings')
            header_row:   1-indexed row to use as headers (SF Data Import = 2)
            skip_rows:    rows above the header to skip
        """
        tab_name = self.tab_names[logical_name]
        ws = self._book.worksheet(tab_name)
        all_values = ws.get_all_values()
        if not all_values:
            return pd.DataFrame()

        header_idx = header_row - 1 + skip_rows
        if header_idx >= len(all_values):
            return pd.DataFrame()
        headers = all_values[header_idx]
        rows = all_values[header_idx + 1 :]
        # Pad short rows
        max_len = len(headers)
        rows = [r + [""] * (max_len - len(r)) for r in rows]
        return pd.DataFrame(rows, columns=headers)

    # ── writes ────────────────────────────────────────────────

    @staticmethod
    def _df_to_sheet_values(df: pd.DataFrame) -> list:
        """Convert a DataFrame to a gspread-safe 2D list.

        Handles float NaN, NaT, None, inf and any other non-JSON-serializable
        value by converting to empty string. Applied to every sheet write so
        no individual caller needs to pre-sanitize.
        """
        import math

        def _safe(v):
            if v is None:
                return ""
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return ""
            s = str(v)
            if s in ("nan", "NaN", "NaT", "<NA>", "None", "inf", "-inf", ""):
                return ""
            return s

        headers = df.columns.tolist()
        rows = [[_safe(cell) for cell in row] for row in df.values.tolist()]
        return [headers] + rows

    def clear_tab(self, logical_name: str) -> None:
        """Clear all content from a tab without deleting it.

        Used at run start to free cell budget before writing new data.
        If the tab doesn't exist yet, does nothing.
        """
        tab_name = self.tab_names.get(logical_name, logical_name)
        try:
            ws = self._book.worksheet(tab_name)
            ws.clear()
            ws.resize(rows=1000, cols=30)
        except gspread.WorksheetNotFound:
            pass

    def write_tab(
        self,
        logical_name: str,
        df: pd.DataFrame,
        clear_first: bool = True,
        chunk_size: int = 50000,
    ) -> None:
        """Overwrite a tab with a DataFrame (headers + values).

        Writes in chunks to avoid Google Sheets API 500 errors on large
        payloads.  Also pre-expands the worksheet row count so append_rows
        never hits a grid-limit error.

        chunk_size defaults to 50,000 rows — safe for up to ~26 cols.
        For very wide sheets (50+ cols) consider lowering to 20,000.
        """
        import time
        tab_name = self.tab_names[logical_name]
        n_rows = len(df) + 1  # +1 for header
        n_cols = len(df.columns)

        try:
            ws = self._book.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            ws = self._book.add_worksheet(
                title=tab_name, rows=max(n_rows, 1000), cols=max(n_cols, 30)
            )

        # Only expand the sheet if needed — never shrink.
        # Shrinking via resize() triggers a cell-count check against the
        # workbook limit even when reducing, which causes 400 errors when
        # the workbook is near the 10M cell limit.
        # The Bid Optimizer tab is cleared at run start which reclaims budget.
        if ws.row_count < n_rows:
            ws.add_rows(n_rows - ws.row_count)

        if clear_first:
            ws.clear()

        if df.empty:
            ws.update([df.columns.tolist()], "A1", value_input_option="RAW")
            return

        values = self._df_to_sheet_values(df)  # header + all data rows

        # Chunk 1: header + first batch written with update() to A1
        first_chunk = values[:chunk_size + 1]
        try:
            ws.update(first_chunk, "A1", value_input_option="RAW")
        except gspread.exceptions.APIError as e:
            raise RuntimeError(
                f"Sheets write_tab failed on first chunk for '{tab_name}': "
                f"{e.response.status_code} — {e.response.text[:300]}"
            ) from e

        # Remaining chunks appended in batches
        for start in range(chunk_size + 1, len(values), chunk_size):
            chunk = values[start : start + chunk_size]
            try:
                ws.append_rows(chunk, value_input_option="RAW")
            except gspread.exceptions.APIError as e:
                raise RuntimeError(
                    f"Sheets write_tab failed appending rows {start}–"
                    f"{start + len(chunk)} for '{tab_name}': "
                    f"{e.response.status_code} — {e.response.text[:300]}"
                ) from e
            time.sleep(0.3)

    @staticmethod
    def _safe_cell(v: Any) -> str:
        """Same null/NaN/inf handling as _df_to_sheet_values, exposed for
        single-column writes."""
        import math
        if v is None:
            return ""
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return ""
        s = str(v)
        if s in ("nan", "NaN", "NaT", "<NA>", "None", "inf", "-inf", ""):
            return ""
        return s

    def write_columns(
        self,
        logical_name: str,
        df: pd.DataFrame,
        columns: list[str],
        start_row: int = 2,
    ) -> None:
        """Update ONLY the given columns of a tab, leaving every other
        column — including any column driven by a live formula, like a
        FILTER() in col A/B — completely untouched.

        `df` must already be in the exact row order that belongs in the
        sheet starting at `start_row` (default row 2, right under the
        header). Each column is written as its own contiguous range
        (e.g. 'C2:C551'). This never clears or rewrites the tab as a whole
        the way write_tab() does.

        Raises ValueError if a requested column isn't found in the live
        header row — silently no-op'ing there would hide a schema drift bug.
        """
        import time
        if df.empty or not columns:
            return

        tab_name = self.tab_names[logical_name]
        ws = self._book.worksheet(tab_name)
        header_row = ws.row_values(1)

        end_row = start_row + len(df) - 1
        if ws.row_count < end_row:
            ws.add_rows(end_row - ws.row_count)

        for name in columns:
            if name not in header_row:
                raise ValueError(
                    f"write_columns: column '{name}' not found in live header "
                    f"row of '{tab_name}' — refusing to guess a position."
                )
            col_idx = header_row.index(name) + 1  # 1-indexed
            col_letter = gspread.utils.rowcol_to_a1(1, col_idx).rstrip("0123456789")
            values = [[self._safe_cell(v)] for v in df[name].tolist()]
            rng = f"{col_letter}{start_row}:{col_letter}{end_row}"
            try:
                ws.update(values, rng, value_input_option="RAW")
            except gspread.exceptions.APIError as e:
                raise RuntimeError(
                    f"write_columns failed on '{name}' ({rng}) in '{tab_name}': "
                    f"{e.response.status_code} — {e.response.text[:300]}"
                ) from e
            time.sleep(0.2)

    def append_rows(
        self, logical_name: str, rows: list[list[Any]], value_input: str = "RAW"
    ) -> None:
        tab_name = self.tab_names[logical_name]
        ws = self._book.worksheet(tab_name)
        ws.append_rows(rows, value_input_option=value_input)

    # ── pipeline state lock (Pipeline State tab) ──────────────

    LOCK_HEADERS = ["status", "running_user", "phase", "started_at", "run_folder"]

    def lock_status(self) -> dict:
        """Return current lock state. Empty/missing tab counts as idle."""
        from datetime import datetime, timedelta
        try:
            ws = self._book.worksheet(self.tab_names["pipeline_state"])
        except gspread.WorksheetNotFound:
            return {"status": "idle"}
        rows = ws.get_all_values()
        if len(rows) < 2:
            return {"status": "idle"}
        data = dict(zip(self.LOCK_HEADERS, rows[1] + [""] * (5 - len(rows[1]))))
        # Auto-expire stale locks (treat any > 6hr-old running lock as idle)
        if data.get("status") == "running" and data.get("started_at"):
            try:
                started = datetime.fromisoformat(data["started_at"])
                if datetime.now() - started > timedelta(hours=6):
                    data["status"] = "idle"
                    data["_stale"] = True
            except ValueError:
                pass
        return data

    def acquire_lock(self, user: str, phase: str, run_folder: str = "") -> bool:
        """Try to acquire the pipeline lock. Returns True if acquired.

        Reads → checks current status → writes if free. Not strictly atomic
        across users (Google Sheets has no CAS), but with 3 internal users
        the chance of a true collision is small. If you see two `running`
        statuses in the Pipeline State tab, treat it as a real conflict.

        Auto-expires stale locks older than 6 hours.
        """
        from datetime import datetime
        current = self.lock_status()
        if current.get("status") == "running" and not current.get("_stale"):
            return False
        try:
            ws = self._book.worksheet(self.tab_names["pipeline_state"])
        except gspread.WorksheetNotFound:
            ws = self._book.add_worksheet(
                title=self.tab_names["pipeline_state"], rows=10, cols=5,
            )
        ws.update(
            [self.LOCK_HEADERS, ["running", user, phase,
             datetime.now().isoformat(timespec="seconds"), run_folder]],
            "A1",
            value_input_option="RAW",
        )
        return True

    def release_lock(self) -> None:
        """Mark the pipeline as idle. Always call in a `finally`."""
        try:
            ws = self._book.worksheet(self.tab_names["pipeline_state"])
        except gspread.WorksheetNotFound:
            return
        ws.update(
            [self.LOCK_HEADERS, ["idle", "", "", "", ""]],
            "A1",
            value_input_option="RAW",
        )
