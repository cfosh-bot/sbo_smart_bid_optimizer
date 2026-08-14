"""One-time script to reset corrupted MP CTV pause state.

Run this from the SBO Python Engine root directory:
    python reset_mp_ctv_pause_state.py

This clears the paused_log and paused_snapshot parquets for the
marketplace_ctv tactic, which were corrupted by failed early runs
that incorrectly marked all lines as paused.

Safe to run — it only touches MP CTV state. Podcast and Streaming
state is completely unaffected.
"""

import shutil
from pathlib import Path

STATE_DIR = Path("state/marketplace_ctv")

files_to_clear = [
    "paused_log.parquet",
    "paused_snapshot.parquet",
]

print(f"Resetting MP CTV pause state in: {STATE_DIR.resolve()}")

for fname in files_to_clear:
    fpath = STATE_DIR / fname
    if fpath.exists():
        # Back it up first just in case
        backup = fpath.with_suffix(".parquet.bak")
        shutil.copy2(fpath, backup)
        fpath.unlink()
        print(f"  Cleared: {fname} (backup saved as {backup.name})")
    else:
        print(f"  Not found (already clean): {fname}")

print("\nDone. Pause state reset. Run the next Full Run fresh.")
print("If you need to restore: rename the .bak files back to .parquet")
