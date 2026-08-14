# How to Use the Smart Bid Optimizer

Day-to-day operator guide. If you just want to run a bid push, you only
need the **Daily flow** section.

---

## 0. One-time setup (you only do this once)

See [SHARING.md](SHARING.md). After your machine is set up, the rest of
this doc is what you actually do day to day.

### 0.5 Migrate Apps Script state (do this BEFORE your first Full Run)

This step is critical and only happens once per machine. Skip it and your
first Python run will treat every campaign as Day 1, even though Apps
Script has been tracking it for weeks.

**Why this matters:** the Python port stores Day-1/Day-2 baselines, paused
lines, kill log, and pacing history in local Parquet files (`state/`).
On a fresh install those are empty. Apps Script tracks the same data in
sheet tabs. We need to copy that history into Python state once.

**How to migrate:**

1. Open Streamlit (double-click `start_sbo.command`)
2. Make sure the sidebar Sheet ID points at the **original Apps Script
   sheet** (the multi-tab one with `Optimizer First Run Log`,
   `SBO Pacing History`, etc. — not the slimmed-down 8-tab sheet)
3. Go to the **💾 State** tab
4. Click **Import state from sheet**
5. Wait ~30 seconds; check the summary. You should see counts like:
   - `first_run_log: 3,200 rows`
   - `second_run_log: 3,100 rows`
   - `pacing_history: 3,500 rows × 4 dates`
   - `paused_log: 600 rows`
   - `kill_log: 16,000 rows`

**CLI alternative** (if you prefer Terminal):

```bash
source .venv/bin/activate
python -m sbo.state_import --sheet-id <google-sheet-id>
```

After this, run **Full Run** as normal. The first Python run will see all
the migrated history and apply correct Day 2 / pacing-based logic instead
of re-baselining everything.

---

## 1. Starting the app

**Easy way:** double-click `start_sbo.command` in the project folder. A
Terminal window opens, runs the launcher, and your browser tab opens to
the app. Drag the file to your Dock or make a Desktop alias for one-click
access going forward.

**First time** macOS will warn that the file is from an "unidentified
developer". Right-click the file → **Open** → confirm. You only need
to do this once per machine.

**Manual way (if you prefer Terminal):**

```bash
cd "Hail Mary"
source .venv/bin/activate
streamlit run app.py
```

Either way, a browser tab opens at `localhost:8501`.

**To stop the app:** press **Ctrl+C** in the Terminal window. Closing
just the browser tab does NOT stop it — Streamlit keeps running in the
background.

The first time you click **any button** in the app (this session or after
a long break), a Google login popup will appear. Sign in with your work
Google account — that's how the app reads/writes the AM-facing sheet on
your behalf.

---

## 2. The four tabs

| Tab            | What it's for                                 |
| -------------- | --------------------------------------------- |
| **▶ Run**      | Click buttons to run pipeline phases          |
| **🔍 Review**  | See the latest run's decisions before pushing |
| **📁 History** | Past runs with status + logs                  |
| **💾 State**   | Persistent state files (do not delete)        |

The sidebar lets you switch tactics (Podcast, Streaming, MP CTV, Select CTV)
and see/override the Sheet ID.

---

## 3. Daily flow

This is the most common workflow — what you do every morning.

### Step A: Full Run

1. Go to **▶ Run** tab
2. Click **🌅 Full Run**
3. Wait ~5–15 minutes (no longer 4–5 hours like Apps Script)

Behind the scenes this:

- Pulls the All-Time Report from Beeswax
- Builds publisher stats
- Rebuilds the Bid Optimizer DataFrame
- Calculates pacing from BW + Salesforce data
- Runs the multiplier engine
- **Writes the Bid Optimizer to the sheet for review**
- **Does NOT push to Beeswax yet**

If someone else is already running, you'll see a yellow warning at the
top — coordinate before continuing.

### Step B: Review

1. Switch to **🔍 Review** tab
2. Skim the KPI row (total / will-change / held / no-decision counts)
3. Use the filter row to spot-check:
   - **Filter by reason code** — type `PACE_UP_CRITICAL` to see all severe
     underpacers; `LINE_PAUSED` to see new pauses; `OTHER_KILL_HIGH_PACE`
     to see "Other" kills, etc.
   - **Filter by BW LI ID** — type a partial ID to drill into one line
   - **Only changing rows** — hide rows with no multiplier change
4. Open the Google Sheet itself (`Bid Optimizer` tab) for full AM review

If something looks wrong in a row, **stop and investigate**. The run
folder under `runs/<timestamp>_<tactic>_full/` has every API response
captured for replay.

### Step C: Push

1. Back to **▶ Run** tab
2. (Optional) Tick **dry-run push** to do a no-op test first
3. Click **🚀 Push Only**
4. Wait ~2–3 minutes

This reads the latest full run's `04_bid_optimizer.parquet` (so the same
multipliers you reviewed) and pushes them to Beeswax. One GET + one PUT
per modifier, not per deal term.

You'll see a status toast and a log tail when it's done. `runs/<…>_pushonly/`
contains `06_push_results.parquet` listing every term, status, and any errors.

---

## 4. Other phases (less frequent)

### Pacing Only

Refreshes pacing without rebuilding the All-Time Report. Use if pacing
data shifted and you want to recalculate without doing the full pull.

### Phase 1 — Create modifiers

When new line items are added to the **Beeswax Line Item Settings** tab
(rows missing a Bid Modifier ID), Phase 1:

- Looks up each line's targeting expression
- Expands its targeted deal lists into individual deal IDs
- Creates a bid modifier in Beeswax with smart starting multipliers per deal
- Writes the new modifier ID back into the sheet

Run this when AMs paste new line items into the input tab.

### Phase 2 — Patch line items

For each line that has a modifier ID assigned but isn't yet patched:

- Sets `bid_modifier_id` on the line item in Beeswax
- Sets `min_bid = $0.01` and `max_bid = min(cpm_bid × 2, $100)`

Run this right after Phase 1.

### Phase 3 — Sync deal terms

Once a line is live, **its targeting can change** (AMs add/remove deals
from targeted deal lists). Phase 3:

- Walks each line's current targeted deal list
- Compares to the modifier's existing terms
- Adds new terms with smart starting multipliers
- Migrates lines still using old `deal_id_list`-style terms

Tick **new lines only (col I)** to only process rows where col I = "yes"
in the input sheet — useful when AMs flag specific lines for sync.

---

## 5. What if something goes wrong?

### "Auth failed" / "Bandwidth quota exceeded"

Beeswax rate-limited you. The client now retries automatically on transient
errors (5xx, 429). If the failure is sticky, wait 5 minutes and retry.

### Run errored mid-flight

1. Go to **📁 History** tab
2. Find the run with ❌
3. Expand the row to see metadata (which phase failed, what status)
4. Open `runs/<…>/logs.txt` for the full trace
5. Check `runs/<…>/beeswax_raw/` for the last API response captured

If the engine successfully ran but push failed, you can re-run **Push Only**
without redoing the full pipeline — it picks up the latest successful full run.

### Decisions look wrong

1. Open the run folder
2. `05_decisions.parquet` has every per-row `Decision_Reason` text
3. The reason text shows the math: which branch fired, what step was applied,
   whether a category cap clamped it
4. Cross-check against the `Optimizer Reason Key` tab in the sheet (one-time
   doc explaining every reason code)

### State got corrupted

Persistent state lives in `state/`. Files there encode Day-1/Day-2
tracking, pacing history, kill log, paused log.

**Do not delete** these. If you do, every line will treat itself as Day 1
on the next run and pacing trends will reset.

If you absolutely must reset state:

- `state/first_run_log.parquet` + `second_run_log.parquet` — reset = every
  line re-baselines as Day 1
- `state/pacing_history.parquet` — reset = trend modifier (1.2× / 1.4×) is 1.0×
  for ~4 days
- `state/paused_log.parquet` — reset = pause/resume detection re-fires
- `state/kill_log.parquet` — informational only, safe to clear

---

## 6. Glossary

| Term              | What it means                                                                                      |
| ----------------- | -------------------------------------------------------------------------------------------------- |
| **BW LI ID**      | Beeswax line item ID (the integer Beeswax assigns)                                                 |
| **SF LI ID**      | Salesforce line item ID (sourced from SF, used for pacing)                                         |
| **Bid Modifier**  | A Beeswax object that holds per-deal multipliers for one LI                                        |
| **Term**          | One row inside a Bid Modifier — a (deal_id, multiplier) pair                                       |
| **Pacing %**      | (imps_yesterday × days_left + imps_through_yesterday) / goal                                       |
| **Day 1 / Day 2** | First and second time the optimizer touches a line — both use baseline math, not pacing-based math |
| **Priority Mode** | After 4 consecutive days of ≥100% pacing, throttle/kill rules engage on non-priority categories    |
| **Sliding Scale** | Underpacing within ≤14 days jumps to a category dollar-cap target instead of incrementing          |
| **EOC**           | End-of-campaign — last 3 days remaining. Special rules: no kills, urgency boost on bid raises      |

---

## 7. Where things live

```
Hail Mary/
├── runs/                          ← per-run snapshots (open these for QA)
│   └── 2026-04-28_0300_podcast_full/
│       ├── 00_run_metadata.json   ← start/end, status, summary
│       ├── 02_atr.parquet         ← raw All-Time Report
│       ├── 03_publisher_stats.parquet
│       ├── 04_bid_optimizer.parquet  ← what was reviewed
│       ├── 05_decisions.parquet   ← every reason code
│       ├── 06_push_results.parquet  ← what got pushed (for pushonly runs)
│       ├── beeswax_raw/           ← raw API responses (replay material)
│       └── logs.txt
└── state/                         ← persistent (do not delete)
    ├── li_modifier_map.parquet
    ├── first_run_log.parquet
    ├── second_run_log.parquet
    ├── pacing_history.parquet
    ├── paused_log.parquet
    ├── kill_log.parquet
    └── category_cpm_history.parquet
```

To open a parquet file outside Streamlit:

```bash
python3 -c "import pandas as pd; print(pd.read_parquet('runs/.../05_decisions.parquet').head(20))"
```

---

## 8. When to ask for help

- The Streamlit log shows a Python traceback you can't decipher
- A run completes "success" but the multipliers in Beeswax don't match what you saw in Review
- A line item is repeatedly flagged as PRE_FLIGHT_HOLD even though it's delivering
- Pacing % seems off compared to UP

In all cases: copy the run folder name, the date/time, and the BW LI ID
into your message. Everything else is in the run folder for replay.
