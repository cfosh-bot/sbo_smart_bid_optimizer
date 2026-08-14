#!/usr/bin/env python3
"""Smart Bid Optimizer — Automated Scheduler

Three workflows, one script:

  6:00 PM   MAINTENANCE   Phase 3 (all lines) — Podcast then Streaming
  1:00 AM   NEW LINE      Phase 1 → 2 → 3 (new lines only) — Podcast then Streaming
                          Waits until 3:00 AM, then:
  3:00 AM   FULL RUN      Full Run → Push — Podcast then Streaming
                          (only if new-line workflow completed successfully today)

New-line detection:
  `state/known_lines_<tactic>.parquet` caches every SF OLI ID + BW LI ID seen
  after a successful Phase 3. Anything in the input sheet not in that cache
  is treated as a new line. The scheduler writes "Yes" to Col I before Phase 3
  and clears it + updates the cache after Phase 3 succeeds.

Completion tracking:
  `state/nightly_status.json` records per-tactic per-date phase outcomes.
  The 3 AM job reads this to decide whether to proceed with Full Run + Push.

Usage:
  python scheduler.py maintenance      # run 6 PM Phase 3 now
  python scheduler.py new_line         # run 1:00 AM sequence now
  python scheduler.py full_and_push    # run 3 AM sequence now
  python scheduler.py nightly          # run full overnight sequence (12:30 AM start)
  python scheduler.py install_launchd  # install macOS launchd agents

Cron (alternative to launchd):
  0  18 * * * /path/to/.venv/bin/python /path/to/scheduler.py maintenance >> logs/sbo_maintenance.log 2>&1
  0  1 * * * /path/to/.venv/bin/python /path/to/scheduler.py nightly >> logs/sbo_nightly.log 2>&1
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Tuneable constants ────────────────────────────────────────────────────

TACTICS = [
    ("Podcast",   "sbo/config/podcast.yaml",  "SHEET_ID_PODCAST"),
    ("Streaming", "sbo/config/streaming.yaml", "SHEET_ID_STREAMING"),
]

MAX_RETRIES     = 2       # retries after first attempt (3 total)
RETRY_DELAY_SEC = 60      # seconds between retries

FULL_RUN_HOUR   = 3
FULL_RUN_MINUTE = 0

# Hard wall-clock timeout per phase (seconds)
PHASE_TIMEOUT_SEC: dict[str, int] = {
    "phase1":    30 * 60,
    "phase2":    20 * 60,
    "phase3":    120 * 60,
    "full":      120 * 60,
    "pushonly":  45 * 60,
}

STATE_DIR    = Path(os.environ.get("STATE_DIR", "state"))
STATUS_FILE  = STATE_DIR / "nightly_status.json"

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()


def notify_slack(message: str) -> None:
    """Post a plain-text message to Slack via Incoming Webhook.

    Uses only the standard library (urllib) — no slack_sdk, no bot user,
    nothing new to install. If SLACK_WEBHOOK_URL isn't set, this is a
    silent no-op so the job never fails because of a missing webhook.
    """
    if not SLACK_WEBHOOK_URL:
        log.info("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return
    try:
        import json as _json
        import urllib.request as _req

        payload = _json.dumps({"text": message}).encode("utf-8")
        request = _req.Request(
            SLACK_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with _req.urlopen(request, timeout=15) as resp:
            if resp.status >= 300:
                log.warning("Slack webhook returned status %s", resp.status)
    except Exception as e:
        log.warning("Slack notification failed (job result unaffected): %s", e)

# Col I header name in the Beeswax Line Item Settings tab (must match exactly)
NEW_LINE_COL = "New Line Indicator - Add Yes"
# Key columns for new-line detection
SF_COL = "SF LI ID"
BW_COL = "BW LI ID"

# ── Logging ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scheduler")


# ── Nightly status tracking ───────────────────────────────────────────────

def _load_status() -> dict:
    try:
        return json.loads(STATUS_FILE.read_text()) if STATUS_FILE.exists() else {}
    except Exception:
        return {}


def _save_status(status: dict) -> None:
    STATE_DIR.mkdir(exist_ok=True)
    STATUS_FILE.write_text(json.dumps(status, indent=2))


def _mark_status(tactic: str, key: str, value: str) -> None:
    """Set status[tactic][key] = value with today's date stamped."""
    status = _load_status()
    today = date.today().isoformat()
    if tactic not in status or status[tactic].get("date") != today:
        status[tactic] = {"date": today}
    status[tactic][key] = value
    status[tactic]["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _save_status(status)
    log.info("[%s] Status → %s = %s", tactic, key, value)


def _check_status(tactic: str, key: str) -> str:
    """Return today's status value for (tactic, key), or 'missing'."""
    status = _load_status()
    today = date.today().isoformat()
    entry = status.get(tactic, {})
    if entry.get("date") != today:
        return "missing"
    return entry.get(key, "missing")


# ── Completion verification via run metadata ──────────────────────────────

def _verify_phase_success(tactic: str, phase: str) -> bool:
    """Check runs/ folder for a successful run of (tactic, phase) today.

    Looks at 00_run_metadata.json for status=success.
    Falls back to logs.txt containing a completion phrase.
    """
    runs_dir = Path(os.environ.get("RUNS_DIR", "runs"))
    today = date.today().strftime("%Y-%m-%d")
    tactic_slug = tactic.lower()

    matching = sorted(
        [d for d in runs_dir.glob(f"{today}_*_{tactic_slug}_{phase}") if d.is_dir()],
        reverse=True,
    )
    for run_dir in matching:
        # Primary: check 00_run_metadata.json
        meta_file = run_dir / "00_run_metadata.json"
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text())
                if meta.get("status") == "success":
                    log.info(
                        "[%s] %s verified via metadata: %s",
                        tactic, phase, run_dir.name,
                    )
                    return True
            except Exception:
                pass

        # Fallback: check logs.txt for completion phrase
        logs_file = run_dir / "logs.txt"
        if logs_file.exists():
            try:
                content = logs_file.read_text()
                completion_phrases = [
                    f"Phase {phase[-1]} done",
                    "mark_complete",
                    "=== Phase",
                    "complete",
                ]
                if any(p.lower() in content.lower() for p in completion_phrases):
                    log.info(
                        "[%s] %s verified via logs: %s",
                        tactic, phase, run_dir.name,
                    )
                    return True
            except Exception:
                pass

    log.warning("[%s] %s — no successful run found today in runs/", tactic, phase)
    return False


# ── New-line detection ────────────────────────────────────────────────────

def _known_lines_path(tactic: str) -> Path:
    return STATE_DIR / f"known_lines_{tactic.lower()}.parquet"


def _load_known_lines(tactic: str) -> set[str]:
    """Return set of known BW LI IDs for this tactic."""
    path = _known_lines_path(tactic)
    if not path.exists():
        return set()
    try:
        df = pd.read_parquet(path)
        return set(df[BW_COL].astype(str).str.strip().tolist())
    except Exception as e:
        log.warning("[%s] Could not load known_lines: %s", tactic, e)
        return set()


def _update_known_lines(tactic: str, current_df: pd.DataFrame) -> None:
    """Overwrite known_lines cache with current input sheet rows."""
    STATE_DIR.mkdir(exist_ok=True)
    keep_cols = [c for c in [SF_COL, BW_COL] if c in current_df.columns]
    if not keep_cols:
        log.warning("[%s] Cannot update known_lines — SF/BW cols not found", tactic)
        return
    df = current_df[keep_cols].copy()
    df = df[df[BW_COL].astype(str).str.strip().ne("")]
    df.to_parquet(_known_lines_path(tactic), index=False)
    log.info("[%s] known_lines updated: %d lines cached", tactic, len(df))


def _find_new_lines(tactic: str, input_df: pd.DataFrame) -> list[str]:
    """Return list of BW LI IDs not in known_lines cache."""
    if BW_COL not in input_df.columns:
        log.warning("[%s] BW LI ID column not found in input sheet", tactic)
        return []
    known = _load_known_lines(tactic)
    all_bw_ids = set(
        input_df[BW_COL].astype(str).str.strip()
        .replace("", pd.NA).dropna().tolist()
    )
    new = sorted(all_bw_ids - known)
    log.info(
        "[%s] New-line detection: %d total, %d known, %d new",
        tactic, len(all_bw_ids), len(known), len(new),
    )
    return new


# ── Col I writer / clearer ────────────────────────────────────────────────

def _write_col_i(tactic: str, config: str, sheet_id: str, new_bw_ids: list[str]) -> bool:
    """Set Col I = 'Yes' for new_bw_ids, blank for all others."""
    try:
        from dotenv import load_dotenv as _ld; _ld()
        from sbo.config_models import load_config
        from sbo.sheets_io import SheetsIO

        cfg = load_config(config)
        sheets = SheetsIO(sheet_id, cfg.sheet_tabs.model_dump())
        df = sheets.read_tab("beeswax_line_item_settings")

        if NEW_LINE_COL not in df.columns:
            log.warning("[%s] Col I ('%s') not found in sheet", tactic, NEW_LINE_COL)
            return False

        new_set = set(str(x) for x in new_bw_ids)
        df[NEW_LINE_COL] = df[BW_COL].astype(str).apply(
            lambda x: "Yes" if x.strip() in new_set else ""
        )
        # Write ONLY Col I — never touch A/B or anything else in the tab.
        sheets.write_columns("beeswax_line_item_settings", df, columns=[NEW_LINE_COL])
        log.info("[%s] Col I set: %d new lines marked 'Yes'", tactic, len(new_bw_ids))
        return True
    except Exception as e:
        log.error("[%s] Failed to write Col I: %s", tactic, e, exc_info=True)
        return False


def _clear_col_i(tactic: str, config: str, sheet_id: str) -> None:
    """Clear Col I for all rows after Phase 3 completes."""
    try:
        from dotenv import load_dotenv as _ld; _ld()
        from sbo.config_models import load_config
        from sbo.sheets_io import SheetsIO

        cfg = load_config(config)
        sheets = SheetsIO(sheet_id, cfg.sheet_tabs.model_dump())
        df = sheets.read_tab("beeswax_line_item_settings")

        if NEW_LINE_COL in df.columns:
            df[NEW_LINE_COL] = ""
            # Write ONLY Col I — never touch A/B or anything else in the tab.
            sheets.write_columns("beeswax_line_item_settings", df, columns=[NEW_LINE_COL])
            log.info("[%s] Col I cleared", tactic)
    except Exception as e:
        log.warning("[%s] Failed to clear Col I: %s", tactic, e)


# ── Child process worker ──────────────────────────────────────────────────

def _run_phase_worker(
    config: str, sheet_id: str, phase: str,
    new_only: bool, result_queue,
) -> None:
    """Runs in a child process. Puts 'ok' or exception on queue."""
    try:
        from dotenv import load_dotenv as _ld; _ld()
        from sbo.pipeline import build_context, close_context
        from sbo import pipeline as pl

        ctx = build_context(config, sheet_id, phase)
        try:
            if phase == "phase3":
                pl.run_phase3(ctx, new_only=new_only)
            elif phase == "phase1":
                pl.run_phase1(ctx)
            elif phase == "phase2":
                pl.run_phase2(ctx)
            elif phase == "full":
                pl.run_full(ctx)
            elif phase == "pushonly":
                pl.run_pushonly(ctx)
            else:
                raise ValueError(f"Unknown phase: {phase}")
        finally:
            close_context(ctx)

        result_queue.put("ok")
    except Exception as e:
        result_queue.put(e)


# ── Core runner with timeout + retry ─────────────────────────────────────

def run_phase_with_retry(
    name: str, config: str, sheet_id: str, phase: str,
    new_only: bool = False,
) -> bool:
    """Run phase in child process with hard timeout and retry. Returns True on success."""
    timeout     = PHASE_TIMEOUT_SEC.get(phase, 30 * 60)
    max_attempts = MAX_RETRIES + 1

    for attempt in range(1, max_attempts + 1):
        label = f"attempt {attempt}/{max_attempts}"
        log.info(
            "  [%s] %s%s — %s (timeout %dm)",
            name, phase, " [new only]" if new_only and phase == "phase3" else "",
            label, timeout // 60,
        )
        result_queue = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=_run_phase_worker,
            args=(config, sheet_id, phase, new_only, result_queue),
            daemon=True,
        )
        proc.start()
        proc.join(timeout=timeout)

        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
            if proc.is_alive():
                proc.kill()
                proc.join()
            log.error(
                "  [%s] %s ✗ timed out after %dm (%s)",
                name, phase, timeout // 60, label,
            )
        else:
            if not result_queue.empty():
                result = result_queue.get_nowait()
                if result == "ok":
                    log.info("  [%s] %s ✓ succeeded (%s)", name, phase, label)
                    return True
                else:
                    log.error(
                        "  [%s] %s ✗ exception (%s): %s",
                        name, phase, label, result,
                    )
            else:
                log.error(
                    "  [%s] %s ✗ no result (exit code %s, %s)",
                    name, phase, proc.exitcode, label,
                )

        if attempt < max_attempts:
            log.info("  [%s] Waiting %ds before retry...", name, RETRY_DELAY_SEC)
            time.sleep(RETRY_DELAY_SEC)

    log.error(
        "  [%s] %s — all %d attempts failed. Giving up.",
        name, phase, max_attempts,
    )
    return False


def _sheet_id(env_var: str, name: str) -> Optional[str]:
    val = os.environ.get(env_var, "").strip()
    if not val:
        log.error("[%s] Missing env var: %s — skipping tactic", name, env_var)
        return None
    return val


def _read_input_sheet(name: str, config: str, sheet_id: str) -> Optional[pd.DataFrame]:
    try:
        from dotenv import load_dotenv as _ld; _ld()
        from sbo.config_models import load_config
        from sbo.sheets_io import SheetsIO
        cfg = load_config(config)
        sheets = SheetsIO(sheet_id, cfg.sheet_tabs.model_dump())
        return sheets.read_tab("beeswax_line_item_settings")
    except Exception as e:
        log.error("[%s] Failed to read input sheet: %s", name, e)
        return None


# ── Wait until 3 AM ──────────────────────────────────────────────────────

def _wait_until_full_run_time() -> None:
    now = datetime.now()
    target = now.replace(
        hour=FULL_RUN_HOUR, minute=FULL_RUN_MINUTE, second=0, microsecond=0
    )
    seconds = (target - now).total_seconds()
    if seconds > 0:
        log.info(
            "Phases complete. Sleeping %.0fm until %02d:%02d AM for Full Run...",
            seconds / 60, FULL_RUN_HOUR, FULL_RUN_MINUTE,
        )
        time.sleep(seconds)
    else:
        log.info("Already past %02d:%02d — starting Full Run immediately.", FULL_RUN_HOUR, FULL_RUN_MINUTE)


# ── Workflows ─────────────────────────────────────────────────────────────

def workflow_maintenance() -> None:
    """6:00 PM — Phase 3 (all lines) for Podcast then Streaming."""
    log.info("=== 6:00 PM Maintenance: Phase 3 (all lines) ===")
    any_failure = False

    for name, config, env_var in TACTICS:
        log.info("── %s ──", name)
        sid = _sheet_id(env_var, name)
        if sid is None:
            any_failure = True
            _mark_status(name, "phase3_maintenance", "failed")
            continue

        _mark_status(name, "phase3_maintenance", "running")
        ok = run_phase_with_retry(name, config, sid, "phase3", new_only=False)

        if ok:
            _mark_status(name, "phase3_maintenance", "done")
            input_df = _read_input_sheet(name, config, sid)
            if input_df is not None:
                _update_known_lines(name, input_df)
        else:
            any_failure = True
            _mark_status(name, "phase3_maintenance", "failed")
            log.error("[%s] Phase 3 maintenance failed", name)

    if any_failure:
        log.error("Maintenance job finished WITH failures")
        sys.exit(1)
    else:
        log.info("Maintenance job complete — Podcast and Streaming OK")


def workflow_new_line() -> None:
    """1:00 AM — Phase 1 → 2 → 3 (new lines only) for Podcast then Streaming."""
    log.info("=== 1:00 AM New Line: Phase 1 / 2 / 3 (new lines only) ===")
    any_failure = False

    for name, config, env_var in TACTICS:
        log.info("── %s ──", name)
        sid = _sheet_id(env_var, name)
        if sid is None:
            any_failure = True
            for key in ("phase1", "phase2", "phase3_new_lines"):
                _mark_status(name, key, "failed")
            continue

        # ── Phase 1 ──────────────────────────────────────────────────────
        _mark_status(name, "phase1", "running")
        ok = run_phase_with_retry(name, config, sid, "phase1")
        if not ok:
            any_failure = True
            _mark_status(name, "phase1", "failed")
            log.warning("[%s] Phase 1 failed — skipping Phase 2 and 3", name)
            for key in ("phase2", "phase3_new_lines"):
                _mark_status(name, key, "skipped")
            continue
        _mark_status(name, "phase1", "done")

        # ── Phase 2 ──────────────────────────────────────────────────────
        _mark_status(name, "phase2", "running")
        ok = run_phase_with_retry(name, config, sid, "phase2")
        if not ok:
            any_failure = True
            _mark_status(name, "phase2", "failed")
            log.warning("[%s] Phase 2 failed — skipping Phase 3", name)
            _mark_status(name, "phase3_new_lines", "skipped")
            continue
        _mark_status(name, "phase2", "done")

        # ── Detect new lines + write Col I ───────────────────────────────
        input_df = _read_input_sheet(name, config, sid)
        if input_df is None:
            any_failure = True
            _mark_status(name, "phase3_new_lines", "failed")
            log.error("[%s] Could not read input sheet for new-line detection", name)
            continue

        new_bw_ids = _find_new_lines(name, input_df)

        if not new_bw_ids:
            log.info("[%s] No new lines detected — skipping Phase 3", name)
            _mark_status(name, "phase3_new_lines", "done")
            _update_known_lines(name, input_df)
            continue

        col_ok = _write_col_i(name, config, sid, new_bw_ids)
        if not col_ok:
            any_failure = True
            _mark_status(name, "phase3_new_lines", "failed")
            log.error("[%s] Failed to write Col I — skipping Phase 3", name)
            continue

        # ── Phase 3 (new lines only) ──────────────────────────────────────
        _mark_status(name, "phase3_new_lines", "running")
        ok = run_phase_with_retry(name, config, sid, "phase3", new_only=True)

        if ok:
            _mark_status(name, "phase3_new_lines", "done")
            _clear_col_i(name, config, sid)
            fresh = _read_input_sheet(name, config, sid)
            if fresh is not None:
                _update_known_lines(name, fresh)
        else:
            any_failure = True
            _mark_status(name, "phase3_new_lines", "failed")
            log.error(
                "[%s] Phase 3 (new lines) failed — Col I left as-is for manual retry",
                name,
            )

    if any_failure:
        log.error("New-line job finished WITH failures")
        notify_slack(
            f"⚠️ SBO new-line workflow (1:00 AM) had failures "
            f"({date.today().isoformat()}) — this may block tonight's Full Run + Push. "
            f"Check logs/sbo_nightly.log."
        )
        sys.exit(1)
    else:
        log.info("New-line job complete — Podcast and Streaming OK")


def workflow_full_and_push() -> None:
    """3:00 AM — Full Run + Push for Podcast then Streaming.

    Runs for a tactic if all three new-line phases are 'done' today.
    If any phase is 'failed' or 'skipped', skips full+push for that tactic
    immediately without waiting. If phases are still 'running' or 'missing',
    rechecks every 15 minutes for up to 2 hours.
    """
    log.info("=== 3:00 AM Full Run + Push ===")
    any_failure = False

    for name, config, env_var in TACTICS:
        log.info("── %s ──", name)

        p1 = _check_status(name, "phase1")
        p2 = _check_status(name, "phase2")
        p3 = _check_status(name, "phase3_new_lines")

        # If any phase already failed/skipped, don't wait — skip immediately
        terminal_states = {"failed", "skipped"}
        if any(s in terminal_states for s in (p1, p2, p3)):
            log.warning(
                "[%s] New-line phases did not complete successfully — skipping full+push "
                "(phase1=%s phase2=%s phase3_new_lines=%s)",
                name, p1, p2, p3,
            )
            any_failure = True
            continue

        # If phases aren't done yet, recheck every 15 min for up to 2 hours
        if not all(s == "done" for s in (p1, p2, p3)):
            recheck_interval = 15 * 60
            recheck_limit = 8  # 8 × 15 min = 2 hours
            recheck_count = 0
            while not all(s == "done" for s in (p1, p2, p3)):
                if recheck_count >= recheck_limit:
                    log.error(
                        "[%s] New-line workflow still not complete after 2hrs — "
                        "phase1=%s phase2=%s phase3_new_lines=%s. Giving up.",
                        name, p1, p2, p3,
                    )
                    any_failure = True
                    break
                # Re-check for terminal states during the wait too
                if any(s in terminal_states for s in (p1, p2, p3)):
                    log.warning(
                        "[%s] New-line phases failed during wait — skipping full+push "
                        "(phase1=%s phase2=%s phase3_new_lines=%s)",
                        name, p1, p2, p3,
                    )
                    any_failure = True
                    break
                log.info(
                    "[%s] Waiting for new-line workflow — "
                    "phase1=%s phase2=%s phase3_new_lines=%s. "
                    "Rechecking in 15m (%d/%d)...",
                    name, p1, p2, p3, recheck_count + 1, recheck_limit,
                )
                time.sleep(recheck_interval)
                recheck_count += 1
                p1 = _check_status(name, "phase1")
                p2 = _check_status(name, "phase2")
                p3 = _check_status(name, "phase3_new_lines")
            if any_failure:
                continue

        sid = _sheet_id(env_var, name)
        if sid is None:
            any_failure = True
            continue

        # Full Run
        full_ok = run_phase_with_retry(name, config, sid, "full")
        if not full_ok:
            any_failure = True
            log.warning("[%s] Full run failed — skipping push", name)
            continue

        # Push
        push_ok = run_phase_with_retry(name, config, sid, "pushonly")
        if not push_ok:
            any_failure = True

    if any_failure:
        log.error("Full+push job finished WITH failures")
        notify_slack(
            f"❌ SBO nightly Full Run + Push finished WITH FAILURES "
            f"({date.today().isoformat()}). Check runs/ and logs/sbo_nightly.log."
        )
        sys.exit(1)
    else:
        log.info("Full+push job complete — Podcast and Streaming OK")
        notify_slack(
            f"✅ SBO nightly Full Run + Push completed successfully "
            f"({date.today().isoformat()}) — Podcast and Streaming both OK."
        )


def workflow_nightly() -> None:
    """Full overnight sequence: new-line workflow then wait until 3 AM for full run."""
    log.info("=== Nightly sequence start (1:00 AM) ===")
    try:
        workflow_new_line()
    except SystemExit:
        log.warning(
            "New-line workflow exited with failures — continuing to Full Run anyway"
        )
    _wait_until_full_run_time()
    workflow_full_and_push()


# ── launchd install (macOS) ───────────────────────────────────────────────

LAUNCHD_PLIST = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/caffeinate</string>
        <string>-si</string>
        <string>{python}</string>
        <string>{script}</string>
        <string>{job}</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>{hour}</integer>
        <key>Minute</key>
        <integer>{minute}</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>{log_dir}/sbo_{job}.log</string>
    <key>StandardErrorPath</key>
    <string>{log_dir}/sbo_{job}.log</string>
    <key>WorkingDirectory</key>
    <string>{cwd}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{path}</string>
    </dict>
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
"""

def install_launchd() -> None:
    """Write and load launchd plists for all three workflows (macOS only)."""
    import shutil, subprocess

    python  = sys.executable
    script  = str(Path(__file__).resolve())
    cwd     = str(Path(__file__).resolve().parent)
    log_dir = str(Path(cwd) / "logs")
    path    = os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")
    Path(log_dir).mkdir(exist_ok=True)

    agents_dir = Path.home() / "Library" / "LaunchAgents"
    agents_dir.mkdir(exist_ok=True)

    jobs = [
        ("com.sbo.maintenance", "maintenance", 18,  0),  # 6:00 PM
        ("com.sbo.nightly",     "nightly",      1,  0),  # 1:00 AM
    ]

    for label, job, hour, minute in jobs:
        plist_path = agents_dir / f"{label}.plist"
        plist_path.write_text(LAUNCHD_PLIST.format(
            label=label, python=python, script=script, job=job,
            hour=hour, minute=minute, log_dir=log_dir, cwd=cwd, path=path,
        ))
        log.info("Wrote %s", plist_path)

        subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
        result = subprocess.run(
            ["launchctl", "load", str(plist_path)],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            log.info("Loaded: %s", label)
        else:
            log.error("Failed to load %s: %s", label, result.stderr.strip())

    log.info("Done.")
    log.info("  6:00 PM → maintenance  (Phase 3 all lines)")
    log.info("  1:00 AM → nightly     (Phase 1→2→3 new lines, wait, Full Run+Push)")
    log.info("Logs → %s/", log_dir)
    log.info("To unload: launchctl unload ~/Library/LaunchAgents/com.sbo.*.plist")


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    job = sys.argv[1]
    dispatch = {
        "maintenance":   workflow_maintenance,
        "new_line":      workflow_new_line,
        "full_and_push": workflow_full_and_push,
        "nightly":       workflow_nightly,
        "install_launchd": install_launchd,
    }
    fn = dispatch.get(job)
    if fn is None:
        log.error(
            "Unknown job: %s. Use: maintenance | new_line | full_and_push | nightly | install_launchd",
            job,
        )
        sys.exit(1)
    fn()
