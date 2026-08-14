# How to Share This With a New User

You have 3 operators total. Each needs the steps below the first time
they set up the tool. You only do **Step 0 once for the team** (creating
the OAuth client). Steps 1–6 are per-user.

After this is done, the day-to-day workflow is in [HOW_TO_USE.md](HOW_TO_USE.md).

---

## Step 0 — One-time team setup (the IT/admin task)

Done **once** by whoever owns the team's GCP project. Create a shared
OAuth client that all 3 operators authenticate against.

1. Go to https://console.cloud.google.com → APIs & Services → **Credentials**
2. Pick (or create) a GCP project for the team
3. Click **+ CREATE CREDENTIALS** → **OAuth 2.0 Client ID**
4. Application type: **Desktop app**
5. Name: `Smart Bid Optimizer (desktop)`
6. Click **Create**, then **Download JSON** — name it `oauth_client.json`
7. **Enable APIs**: APIs & Services → Library → enable
   - **Google Sheets API**
   - **Google Drive API** (needed for the Sheets API to open by ID)
8. **Add the 3 operators as test users**: APIs & Services → OAuth consent
   screen → Test users → add each operator's `@unified.com` email
9. Hand the `oauth_client.json` to each operator (Slack DM, secure share, etc.) —
   it's not a secret on its own; it identifies the app, not any user

> **Why test users?** Until your OAuth app goes through Google's verification
> flow, only listed test users can authenticate. For 3 internal users, just
> keep it in test mode forever — no verification needed.

---

## Step 1 — Per-user one-time setup

Each operator does these 6 steps once on their Mac.

### 1.1 Install Python 3.11+ (recommended) or 3.9+

```bash
# Check what you have
python3 --version

# If < 3.9, install via Homebrew
brew install python@3.11
```

### 1.2 Get the code

```bash
# Wherever you keep work projects:
cd ~/Documents
git clone <internal-repo-url> sbo
cd sbo
```

(Or whatever distribution channel you use — internal git, a shared zip, etc.)

### 1.3 Create a virtualenv + install deps

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 1.4 Drop in the OAuth client JSON

```bash
mkdir -p credentials
# Then move the oauth_client.json from Slack into ./credentials/
mv ~/Downloads/oauth_client.json credentials/
```

### 1.5 Configure your `.env`

```bash
cp .env.example .env
```

Open `.env` in your editor and fill in:

```
BEESWAX_EMAIL=your.email@unified.com
BEESWAX_PASSWORD=your-beeswax-password
BEESWAX_API_BASE=https://iheartmedia.api.beeswax.com/rest/v2
BEESWAX_LEGACY_LIST_BASE=https://iheartmedia.api.beeswax.com/rest/list_item

# Each tactic's Sheet ID (the bit between /d/ and /edit in the URL)
SHEET_ID_PODCAST=1abcdef...
SHEET_ID_STREAMING=1ghijkl...
SHEET_ID_MARKETPLACE_CTV=1mnopqr...
SHEET_ID_SELECT_CTV=1stuvwx...
```

> **Each operator uses their own Beeswax credentials.** This way the
> Beeswax audit log shows who pushed what.

### 1.6 First launch — handles the rest

Double-click **`start_sbo.command`** in the project folder.

The first time you do this, macOS will block the launch because the
script isn't signed by an identified developer. To allow it:

- **Right-click** `start_sbo.command` → **Open**
- Click **Open** in the security dialog
- (You only have to do this once per machine)

After that, double-click works normally. A Terminal window opens, the
launcher checks your setup, activates the venv, and starts Streamlit.
A browser tab opens at `localhost:8501` shortly after.

The first time you click any button in the app, a Google OAuth consent
screen appears in a popup. Sign in with your Google account, grant
access, and the app caches your refresh token at `credentials/token.json`.
Subsequent runs reuse it silently for ~6 months until the refresh token
expires.

> **Manual launch (advanced):** if you prefer Terminal:
>
> ```bash
> cd ~/Documents/sbo
> source .venv/bin/activate
> streamlit run app.py
> ```

> **Tip:** drag `start_sbo.command` to your Dock for one-click access.
> Or right-click → Make Alias and put the alias on your Desktop.

You're done. From here on, just **HOW_TO_USE.md** day to day.

---

## What each user has on their machine

After setup, each operator's local layout looks like:

```
sbo/
├── .env                          ← their Beeswax creds + sheet IDs (PRIVATE)
├── .venv/                        ← Python virtualenv (their machine only)
├── credentials/
│   ├── oauth_client.json         ← shared across the 3 users
│   └── token.json                ← their personal Google refresh token (PRIVATE)
├── runs/                         ← their local run history
├── state/                        ← per-tactic persistent state (see below!)
└── ... (sbo/, tests/, app.py, etc — same for everyone)
```

### Important: the `state/` directory is per-machine, not shared

This is the subtle gotcha. The state files in `state/` (Day-1 log,
pacing history, etc.) live on whichever machine ran the pipeline. If
operator A runs the daily flow on their laptop on Monday, then operator
B runs it on theirs on Tuesday, **operator B's state will be empty** —
Tuesday's run will Day-1-baseline every line.

Two ways to handle this:

**Option 1 (simplest, recommended for now): one operator owns the daily run.**
Pick a primary operator who runs the daily Full Run + Push every day.
The other two only run Phase 1/2/3 (which don't depend on daily state).
This matches how your team already operates.

**Option 2: sync `state/` via a shared location.**
Put `state/` on a shared drive (Dropbox, Google Drive Stream, etc.) and
update `.env` to point there:

```
STATE_DIR=/Users/shared/SBO/state
```

Trade-off: file lock contention if two people run simultaneously. Combine
with the **Pipeline State** lock tab (already implemented in the sheet) and
coordinate via Slack.

---

## Updating the code after the first install

When the codebase changes:

```bash
cd ~/Documents/sbo
git pull
source .venv/bin/activate
pip install -r requirements.txt   # in case deps changed
```

That's it. No re-auth needed unless the OAuth scopes change.

---

## Revoking access

If someone leaves the team:

1. **Their Google access**: GCP Console → APIs & Services → OAuth consent
   screen → Test users → remove their email
2. **Their Beeswax access**: handled by your IAM team
3. Their local `state/`, `runs/`, `credentials/token.json` stay on their
   laptop — wipe by deleting the repo, or tell your IT to remote-wipe

---

## Why this design

- **Per-user OAuth**: each operator's Google audit log shows exactly who
  read/wrote which sheet, when. No shared service account = no
  attribution problem.
- **Local Python**: no infra to maintain. Sub-15-min daily runs mean
  laptop-as-server is fine for 3 users.
- **Streamlit UI**: zero CLI typing for non-Python operators.
- **Per-run folders**: every Beeswax response is captured for replay /
  postmortem. Every decision has a reason code.
- **Tests**: 49 tests across the engine + state + push + phases. Refactors
  are safe.

---

## Troubleshooting setup

### `pip install` fails on `pyarrow` (M1/M2 Mac)

```bash
pip install --upgrade pip wheel
pip install -r requirements.txt
```

If still failing:

```bash
brew install apache-arrow
pip install pyarrow
```

### Google OAuth popup says "this app is unverified"

Expected — the OAuth client is in test-user mode. Click "Advanced" →
"Go to (project name) (unsafe)" → continue. This warning is for the
test-user flow only.

### "Access blocked: This app's request is invalid"

Your email isn't on the test-user list. Ask the admin to add you (Step 0.8).

### `BEESWAX_EMAIL not set` error

Your `.env` file isn't being loaded. Check:

- `.env` exists at the repo root (not inside `sbo/`)
- You have `BEESWAX_EMAIL=...` (no quotes, no spaces around `=`)
- You ran `streamlit run app.py` from the repo root

### Streamlit hangs on "loading..."

Usually means OAuth flow is waiting on a browser callback. Check for a
popped-up tab from `localhost`. If blocked, run with:

```bash
streamlit run app.py --server.headless false
```
