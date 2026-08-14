# Smart Bid Optimizer — Team Git Workflow Guide

How to set up, run, edit, and stay in sync as a team — without anyone ever
running outdated or conflicting code.

## Overview — Why This Matters

This tool pushes real bid multiplier changes to live Beeswax campaigns
across five products: **Podcast, Streaming, Total Audio, Marketplace CTV,
and Select CTV**. If two people on the team ever ran different versions of
the code on the same day — one person's edit, another person's old copy —
the result could be conflicting or incorrect bid pushes with no easy way
to know why. This has already happened once (that's why the codebase used
to be three separate, drifted copies before this merge).

**The fix built into this tool: GitHub is the single source of truth.**
Every time the pipeline runs — from the command line, from a droplet cron
job, or from the Streamlit app — it automatically checks whether the code
on your computer matches the latest version on GitHub *before doing
anything else*. If it doesn't match, the tool refuses to run and tells you
exactly what's different (`sbo/git_guard.py`).

This document covers:

- **Section 1** — one-time setup for someone starting from scratch
- **Section 2** — checking for updates before you run the tool
- **Section 3** — making and sharing code changes safely
- **Section 4** — what to do when the tool blocks you with a version mismatch
- **Section 5** — the automated nightly runs on the droplet (MP CTV + Select CTV)
- **Section 6** — the five products, at a glance

---

## Section 1: One-Time Setup (Starting From Scratch)

Follow this once per person, per computer.

### Step 1: Get access to the GitHub repository

Ask the repo owner to add you as a collaborator, or confirm you already
have access by visiting:

```
https://github.com/cfosh-bot/sbo_smart_bid_optimizer
```

### Step 2: Install Git

```bash
git --version
```

If you see a version number, skip to Step 3. Otherwise install Git from
[git-scm.com/downloads](https://git-scm.com/downloads).

### Step 3: Install Python 3.11+

```bash
python3 --version
```

### Step 4: Clone the repository

Never download a ZIP of the code from a teammate — always clone directly
from GitHub, so your copy is a real, trackable git repository:

```bash
cd ~/Documents
git clone https://github.com/cfosh-bot/sbo_smart_bid_optimizer.git
cd sbo_smart_bid_optimizer
```

If this prompts for a username and password, GitHub no longer accepts your
account password here — generate a Personal Access Token at
[github.com/settings/tokens](https://github.com/settings/tokens) (scope:
`repo`) and paste that in as the password.

### Step 5: Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Every new terminal session needs `source .venv/bin/activate` run again.

### Step 6: Get the required credentials

You will need, from whoever manages them on the team:

- Beeswax email + password
- The Google Cloud OAuth client secrets / service account (shared securely
  — never over plain Slack/email)
- Each product's Google Sheet ID (from its URL) — Podcast, Streaming,
  Total Audio, Marketplace CTV, Select CTV

### Step 7: Create your local `.env` file

```bash
cp .env.example .env
```

Open `.env` and fill in the real values from Step 6. This file is never
uploaded to GitHub — it's gitignored, unique to your computer.

### Step 8: Run a phase to confirm everything's wired up

```bash
python3 -m sbo.pipeline phase3 --config sbo/config/podcast.yaml
```

If this fails on the git-guard check because you're on a fresh clone with
nothing to compare against yet, that's expected on day one of setting up
the repo — it resolves itself as soon as there's a real `origin/main` to
track against.

---

## Section 2: Checking for Updates Before You Run

This is the step that keeps everyone in sync — and the good news is the
tool does it automatically, every time, before any phase runs.

**What happens automatically:** every call to `build_context()` (which
every CLI phase, cron script, and the Streamlit app goes through) fetches
the latest info from GitHub, compares your computer's code against
`origin/main`, and:

- If they match — proceeds normally, no message.
- If you're behind — refuses to run and writes a detailed audit file
  under `audit/` explaining exactly what's different (see Section 4).

**Checking manually, any time:**

```bash
cd ~/Documents/sbo_smart_bid_optimizer
git fetch
git status
```

If it says "Your branch is up to date with 'origin/main'", you're current.
Otherwise, run `git pull` before doing anything else.

---

## Section 3: Making and Sharing Code Changes Safely

1. **Always pull first**, even if you were "just in here yesterday":
   ```bash
   git pull
   ```
2. **Make your edits.**
3. **Test locally** — run the relevant phase(s) for the product you
   touched and confirm the output looks right before anyone else pulls
   your change.
4. **Commit with a clear message:**
   ```bash
   git add .
   git commit -m "Short, clear description of what changed and why"
   ```
5. **Push:**
   ```bash
   git push
   ```
6. **Tell the team.** For any change to pacing rules, formulas, constants
   in a `sbo/config/*.yaml`, or anything affecting live bid pushes, say so
   in Slack — don't rely on the version-check block as the only notice
   someone gets. It prevents someone from running old code; it doesn't
   replace a heads-up about what changed and why.

**A note on this codebase specifically:** each product's decision logic
lives in its own file on purpose (`multiplier_engine.py` for
Podcast/Streaming/Total Audio, `multiplier_engine_mp_ctv.py` for
Marketplace CTV, `multiplier_engine_select_ctv.py` for Select CTV — same
pattern for `full_report*.py` and `phases*.py`). A change to one product's
file cannot accidentally change another product's live bidding. If you're
tempted to "just branch inside the shared file instead of duplicating a
few lines," don't — that's exactly the coupling this structure avoids.

---

## Section 4: When the Tool Blocks You — Resolving a Version Mismatch

If you try to run a phase and your computer is behind GitHub, you'll see:

```
Stage full BLOCKED — local checkout is 2 commit(s) behind origin/main.
Audit written to: audit/version_diff_2026-08-14_1830.md
```

**What to do:**

1. **Check for your own uncommitted edits first:**
   ```bash
   git status
   ```
   If this shows changes you haven't committed, decide whether to commit
   them (Section 3) or stash them before pulling.
2. **Open the audit file** in `audit/` — it lists exactly which commits
   you're missing and the full diff.
3. **Pull the latest code:** `git pull`
4. **If you have local edits that conflict:** paste the audit file's
   contents into a Claude Code session and ask for help reconciling your
   changes with what's now on GitHub.
5. **Re-run the phase** — once `git status` is clean and up to date, it
   should proceed normally.

---

## Section 5: The Automated Daily Runs (The Droplet)

Marketplace CTV and Select CTV run automatically every night on a
DigitalOcean droplet — a remote Linux server, mechanically identical to
Section 1's setup, just running unattended.

- `run_mp_ctv_daily.sh`: `phase3 → full → pushonly`, no human review gate.
- `run_select_ctv_daily.sh`: same pattern, `phase3 → full → pushonly`.
- Both use `set -e` — any stage failing stops the chain immediately, so a
  bad Phase 3 or Full Run can never get pushed to Beeswax.

Podcast, Streaming, and Total Audio are **not** on this automated droplet
cadence today — they run via `sbo/scheduler.py` (see its module docstring
for the maintenance / new-line / full-run schedule) or manually.

**Nobody should ever edit code directly on the droplet.** All changes
happen on a laptop, get tested locally, and get pushed to GitHub through
Section 3's process. The droplet only ever pulls from GitHub, via the same
git-guard check as everywhere else — if it's behind and has no local
edits, it pulls automatically before running; if it somehow has local
edits too, it refuses to run and logs an error rather than guessing.

**Checking whether last night's run succeeded:**

```bash
ssh <droplet>
cd /root/sbo
cat logs/*.log   # or wherever cron output is redirected
```

Look for a clean completion line and scan for `ERROR`/`BLOCKED` above it.
Also check each product's Google Sheet — `SBO Run Log` tab should show
today's date, and `Bid Optimizer`'s `Update_Status` column should show
fresh `✅ Updated ...` entries.

---

## Section 6: The Five Products, At a Glance

| Product | Config | Category caps? | Publisher cap | Pacing source |
|---|---|---|---|---|
| Podcast | `sbo/config/podcast.yaml` | Yes (dollar caps) | — | SF Data Import |
| Streaming | `sbo/config/streaming.yaml` | Yes (dollar caps) | — | SF Data Import |
| Total Audio | `sbo/config/total_audio.yaml` | Yes, per sub-tactic | — | SF Data Import |
| Marketplace CTV | `sbo/config/marketplace_ctv.yaml` | Yes (537 lines only) | 20% (537) / 40% (other) | SF Data Import |
| Select CTV | `sbo/config/select_ctv.yaml` | No | 40% (uniform) | **Beeswax Select CTV tab** (own pacing formula — no today-subtraction) |

Select CTV also has its own on-target margin-health trim (added
2026-08-14) that the other four products don't have — see
`sbo/multiplier_engine_select_ctv.py`'s module docstring.

---

## Command Reference

| Command | What it does |
|---|---|
| `git clone <url>` | One-time: download the repo on a new computer |
| `source .venv/bin/activate` | Activate the Python environment (every new terminal) |
| `python3 -m sbo.pipeline <phase> --config sbo/config/<product>.yaml` | Run a phase (`full`, `phase1`, `phase2`, `phase3`, `pushonly`) for one product |
| `streamlit run app.py` | Launch the local review dashboard |
| `git status` | See if you're behind GitHub or have uncommitted local edits |
| `git pull` | Download the latest code from GitHub |
| `git add . && git commit -m "..."` | Stage and save your changes locally |
| `git push` | Upload your committed changes to GitHub for everyone else |
| `git fetch` | Check GitHub for updates without changing any local files |

Keep this document somewhere the whole team can reference.
