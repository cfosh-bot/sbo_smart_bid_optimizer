#!/bin/bash
# MP CTV daily: Phase 3 (full) -> Full Run -> Push, no review gate.
# Any step failing stops the chain immediately (set -e) so a bad
# Phase 3 or Full Run can never get pushed to Beeswax.

set -e  # exit immediately if any command fails

cd /root/sbo

# Auto-sync with GitHub before running -- avoids a full day's run getting
# BLOCKED by git_guard.py just because a `git pull` hadn't happened yet
# between the last GitHub push and this cron firing (git_guard only ever
# blocks on being behind origin/main; a clean fast-forward here removes
# that case entirely). Only refuses when the tree is ALSO dirty -- that's
# the case actually worth stopping for (someone editing the droplet
# directly), so it's left for manual reconciliation rather than silently
# pulled over.
git fetch origin main
LOCAL_SHA=$(git rev-parse HEAD)
REMOTE_SHA=$(git rev-parse origin/main)
if [ "$LOCAL_SHA" != "$REMOTE_SHA" ]; then
    if [ -n "$(git status --porcelain)" ]; then
        echo "ERROR: droplet is behind origin/main AND has uncommitted local changes -- refusing to auto-pull. Reconcile manually before the next run."
        exit 1
    fi
    echo "Droplet was behind origin/main -- pulling latest before running."
    git pull origin main
fi

set -a
source .env
set +a

PYTHON=/root/sbo/.venv/bin/python
CONFIG=sbo/config/marketplace_ctv.yaml
SID="$SHEET_ID_MARKETPLACE_CTV"

echo "=== MP CTV daily run start: $(date) ==="

$PYTHON -m sbo.pipeline phase3 --config "$CONFIG" --sheet-id "$SID"
$PYTHON -m sbo.pipeline full --config "$CONFIG" --sheet-id "$SID"
$PYTHON -m sbo.pipeline pushonly --config "$CONFIG" --sheet-id "$SID"

# --- Dashboard history (non-critical: log failure but don't fail the run) ---
RUN_DIR=$(ls -dt /root/sbo/runs/$(date +%Y-%m-%d)_*_marketplace_ctv_pushonly 2>/dev/null | head -1)
if [ -n "$RUN_DIR" ]; then
    $PYTHON -m sbo.pacing_history append --run-dir "$RUN_DIR" || echo "WARNING: pacing_history append failed for $RUN_DIR"
    $PYTHON -m sbo.pacing_history cleanup --keep-days 14 || echo "WARNING: pacing_history cleanup failed"
else
    echo "WARNING: could not locate today's pushonly run dir for dashboard history"
fi

echo "=== MP CTV daily run complete: $(date) ==="
