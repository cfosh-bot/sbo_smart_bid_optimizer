#!/bin/bash
# Smart Bid Optimizer launcher — double-click to start.
#
# This is a regular shell script with a .command extension so macOS Finder
# will open it in Terminal on double-click. Drag this file to the Dock
# (or right-click → Make Alias and put the alias on the Desktop) for
# one-click access.
#
# What it does:
#   1. cd to the script's own folder (so it works from anywhere)
#   2. verify .venv + .env are set up (one-time setup is in SHARING.md)
#   3. activate the venv
#   4. launch Streamlit, which opens a browser tab automatically
#   5. when you're done, press Ctrl+C in this Terminal window to stop

set -e

# ── 1. Always cd to where this script lives ─────────────────────────────
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"

# ── 2. Friendly banner ──────────────────────────────────────────────────
clear
echo "================================================================"
echo "  📈  Smart Bid Optimizer"
echo "================================================================"
echo "  Project: $PROJECT_DIR"
echo ""

# ── 3. Sanity checks — fail fast with a useful message ─────────────────
if [ ! -d ".venv" ]; then
    echo "❌  No virtualenv found at .venv/"
    echo ""
    echo "   This usually means first-time setup wasn't completed."
    echo "   Open SHARING.md and follow Steps 1.3 → 1.4 → 1.5."
    echo ""
    echo "   Quick fix:"
    echo "     python3 -m venv .venv"
    echo "     source .venv/bin/activate"
    echo "     pip install -r requirements.txt"
    echo ""
    read -p "Press Enter to close this window..."
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "⚠️   No .env file found."
    echo ""
    echo "   Run this once to create one from the template:"
    echo "     cp .env.example .env"
    echo "   Then edit .env with your Beeswax credentials + Sheet IDs."
    echo "   See SHARING.md Step 1.5."
    echo ""
    read -p "Press Enter to close this window..."
    exit 1
fi

if [ ! -f "credentials/oauth_client.json" ]; then
    echo "⚠️   No Google OAuth client found at credentials/oauth_client.json"
    echo ""
    echo "   Get this file from your team admin (one-time team setup)."
    echo "   See SHARING.md Step 0 + 1.4."
    echo ""
    read -p "Press Enter to close this window..."
    exit 1
fi

if [ ! -f "app.py" ]; then
    echo "❌  app.py not found in $PROJECT_DIR"
    echo "   This script must live alongside app.py — looks like it was moved."
    read -p "Press Enter to close this window..."
    exit 1
fi

# ── 4. Activate venv ───────────────────────────────────────────────────
# shellcheck source=/dev/null
source .venv/bin/activate

# Verify Streamlit is actually installed (catches half-finished setups)
if ! command -v streamlit >/dev/null 2>&1; then
    echo "❌  Streamlit isn't installed in this venv."
    echo "   Run:  pip install -r requirements.txt"
    echo ""
    read -p "Press Enter to close this window..."
    exit 1
fi

# ── 5. Launch ──────────────────────────────────────────────────────────
echo "✅  Launching Streamlit — your browser will open in a moment."
echo ""
echo "    To stop the app: press Ctrl+C in THIS window."
echo "    (Closing the browser tab does NOT stop it.)"
echo ""
echo "----------------------------------------------------------------"

# `streamlit run` blocks until Ctrl+C. When it exits, we drop back here.
streamlit run app.py

echo ""
echo "----------------------------------------------------------------"
echo "  Smart Bid Optimizer stopped."
echo "----------------------------------------------------------------"
read -p "Press Enter to close this window..."
