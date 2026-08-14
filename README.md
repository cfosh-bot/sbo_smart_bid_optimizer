# Smart Bid Optimizer (Python port)

Python rewrite of the Apps Script SBO. Runs locally with a Streamlit UI;
the Google Sheet stays as the AM-facing surface (8 tabs, slimmed from 20).
Heavy working data lives in per-run `runs/` folders as Parquet for QA.

**4–5 hour Apps Script runs are now ~10–15 min Python runs.** No quotas,
no 30-min walls, no trigger chaining.

---

## 📚 Read me first

| Document | For whom |
|----------|----------|
| **[SHARING.md](SHARING.md)** | First-time setup — give this to every new user |
| **[HOW_TO_USE.md](HOW_TO_USE.md)** | Day-to-day operator guide — what the buttons do |

---

## What it does

```
Google Sheet (AM I/O)  ──gspread──▶  Python pipeline  ──httpx──▶  Beeswax API
                                          │
                                          ├──▶ runs/<timestamp>/      per-run snapshots
                                          └──▶ state/                 persistent state
```

| Phase | What it ports |
|-------|---------------|
| `run_full` | ATR rebuild → publisher stats → Bid Optimizer → pacing → engine |
| `run_pushonly` | Pushes the latest reviewed multipliers to Beeswax |
| `run_pacing_only` | Refreshes pacing without rebuilding ATR |
| `run_phase1` | Creates bid modifiers for new lines |
| `run_phase2` | Patches line items with their modifier IDs + min/max bid |
| `run_phase3` | Syncs modifier terms with current targeted deals |

---

## Quick install (single user)

Detailed first-time setup with OAuth + multi-user concerns: [SHARING.md](SHARING.md).

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill in BW creds + sheet IDs
mkdir -p credentials  # drop OAuth client.json from GCP Console here
```

After setup, **double-click `start_sbo.command`** to launch (drag it to
your Dock for one-click access). First launch on macOS: right-click →
Open to bypass the unsigned-script warning, just once.

Manual launch:
```bash
streamlit run app.py
```

---

## Project layout

```
sbo/
  beeswax_client.py        auth + reports + modifier CRUD (cookie-cached)
  bid_optimizer.py         builds the 30-col Bid Optimizer DataFrame
  config_models.py         pydantic config validation
  config/podcast.yaml      tactic config — caps, prefixes, throttle levels
  full_report.py           ATR + LI/TE + reports → publisher_stats
  multiplier_engine.py     pure decision logic — every priority branch
  pacing.py                3 BW reports + SF goals → fills cols U–Y
  phases.py                Phase 1 / 2 / 3 (create / patch / sync)
  pipeline.py              orchestrator + 6 phase entry points
  push.py                  GET → mutate → PUT (one per modifier, with retries)
  run_storage.py           per-run folder helpers (parquet + raw API + logs)
  sheets_io.py             gspread + per-user OAuth + Pipeline State lock
  state.py                 persistent state (run logs, paused, kill, pacing)
  state_apply.py           applies engine output to state after each run
  utils.py                 shared coercion helpers (safe_float, clean_id)

app.py                     Streamlit UI — Run / Review / History / State tabs
start_sbo.command          double-click launcher for macOS (drag to Dock)

tests/                     pytest suite (49 tests, no Beeswax/Sheets needed)
state/                     persistent state (gitignored, do not delete)
runs/                      per-run output (gitignored)
```

---

## Tests

```bash
python3 -m pytest tests/ -v
```

49 tests, ~0.5 sec, no network. Covers:
- Engine decision branches (FIRST_RUN, DAY2, LAST_3_DAYS, PRIORITY_MODE,
  OTHER kill/throttle, PACE_HOLD, PACE_DOWN/UP, weekend guard, pause/resume,
  pre-flight)
- State persistence (run logs, pacing history, paused/kill, prune)
- Phase 1/2/3 (modifier creation, LI patching, term migration)
- Push (skip-no-change, dry-run, retry on transient errors, error isolation)

---

## Migration status

- [x] `buildFullReport` → `sbo/full_report.py`
- [x] `pullBidModifiers` → `sbo/bid_optimizer.py`
- [x] `calculatePacingFromBW` → `sbo/pacing.py` (formula verified bit-exact vs UP)
- [x] `calculateNewMultipliers` → `sbo/multiplier_engine.py` + `state_apply.py`
- [x] `pushMultipliersToBeewax` → `sbo/push.py` (with retry on 5xx/429)
- [x] Phase 1 / 2 / 3 → `sbo/phases.py`
- [x] Streamlit UI wired (Run, Review with filters, History, State)
- [x] Pipeline State lock for 3-user coordination
- [x] Per-run folders + raw API capture for replay
- [x] Persistent state lifecycle (apply + prune)

---

## What changed in the QA pass

If you've been following along: a focused QA pass found and fixed several
correctness bugs that would have caused subtle drift over time.

**P0 (correctness):**
- Engine output is now persisted to state after every run (`state_apply.py`).
  Without this, Day-1/Day-2 baselines and pacing history would reset every
  run.
- Resumed-line dates are written back to `paused_log` properly.
- Pacing history (the 4-day OVER/UNDER trend) accumulates correctly.
- Expired run-log entries are pruned at the start of every run.
- Push won't blindly overwrite a row whose `Current_Multiplier` is unknown.

**P1 (robustness):**
- Pipeline State lock implemented (acquire/release) for multi-user coordination
- Beeswax client retries on transient 5xx/429 errors
- `BeeswaxClient.close()` reliably called (no httpx connection leaks)
- `run_pushonly` only picks runs whose metadata says `success`/`partial`
- `clean_id` helper replaces `rstrip('.0')` (which also ate real digits)

**P2 (polish):**
- `safe_float` / `maybe_float` consolidated in `utils.py`
- Streamlit Review tab now shows real data with filters + KPIs
- Run history shows status emoji ✅ ⚠️ ❌
