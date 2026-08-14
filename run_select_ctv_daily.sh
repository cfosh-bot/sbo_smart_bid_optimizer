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

PYTHON=/root/sbo/.venv/bin/python
CONFIG=sbo/config/select_ctv.yaml
SID="$SHEET_ID_SELECT_CTV"

echo "=== Select CTV daily run start: $(date) ==="

$PYTHON -m sbo.pipeline phase3 --config "$CONFIG" --sheet-id "$SID"
$PYTHON -m sbo.pipeline full --config "$CONFIG" --sheet-id "$SID"
$PYTHON -m sbo.pipeline pushonly --config "$CONFIG" --sheet-id "$SID"

echo "=== Select CTV daily run complete: $(date) ==="
