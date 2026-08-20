#!/bin/bash
# Select CTV daily: Phase 3 (sync deal terms) -> Full Run -> Push, no review gate.
# Any step failing stops the chain immediately (set -e) so a bad
# Phase 3 or Full Run can never get pushed to Beeswax.
#
# NOTE: this cadence (auto-running Phase 3 daily, ahead of the Full Run) is
# NOT what the Select CTV Apps Script does today -- there, Phase 1/2/3 are
# manual-only and only the ATR-rebuild -> full-report -> pacing -> push chain
# is automated. This script deliberately mirrors MP CTV's cron pattern
# instead, per an explicit decision to run Select CTV the same way.

set -e  # exit immediately if any command fails

cd /root/sbo
set -a
source .env
set +a

# Slack alert on any failure below -- see run_mp_ctv_daily.sh for the full
# rationale (same fix, same underlying exposure -- a blocked/failed run
# used to just sit silently in this log file until someone happened to check).
LAST_STAGE="startup"
notify_failure() {
    local exit_code=$?
    if [ -n "$SLACK_WEBHOOK_URL" ]; then
        curl -s -X POST -H 'Content-Type: application/json' \
            --data "{\"text\": \":rotating_light: Select CTV daily run FAILED during '${LAST_STAGE}' (exit ${exit_code}) on $(date -u '+%Y-%m-%d %H:%M UTC'). Check /root/sbo/logs/sbo_select_ctv.log on the droplet.\"}" \
            "$SLACK_WEBHOOK_URL" >/dev/null 2>&1 || true
    fi
}
trap notify_failure ERR

# Auto-sync with GitHub before running -- see run_mp_ctv_daily.sh for the
# full rationale (same fix, same underlying git_guard.py exposure).
LAST_STAGE="git sync"
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

PYTHON=/root/sbo/.venv/bin/python
CONFIG=sbo/config/select_ctv.yaml
SID="$SHEET_ID_SELECT_CTV"

echo "=== Select CTV daily run start: $(date) ==="

LAST_STAGE="phase3"
$PYTHON -m sbo.pipeline phase3 --config "$CONFIG" --sheet-id "$SID"
LAST_STAGE="full"
$PYTHON -m sbo.pipeline full --config "$CONFIG" --sheet-id "$SID"
LAST_STAGE="pushonly"
$PYTHON -m sbo.pipeline pushonly --config "$CONFIG" --sheet-id "$SID"

echo "=== Select CTV daily run complete: $(date) ==="
