"""Git version guard — refuses to run on out-of-sync code.

Same philosophy as the MadHive Daily Budget Tool's git_guard.py: every time
the pipeline runs (CLI, cron script, or Streamlit), it checks the local
checkout against `origin/<branch>` on GitHub BEFORE doing anything else.

    - If local matches origin: proceeds silently, no message.
    - If local is behind origin: refuses to run, writes a markdown audit
      file under `audit/` describing exactly what's missing, and raises
      GitGuardError so the caller can stop cleanly.
    - If local has uncommitted changes: proceeds (a developer actively
      editing code is expected to have a dirty tree) but notes it in the
      audit file if a mismatch is ALSO found, so reconciling the two is
      easier.
    - If the checkout isn't a git repo at all: refuses to run. This tool
      pushes real budget changes to live campaigns — an ad hoc copy that
      isn't a tracked clone has no way to prove it's current.

Called from `sbo.pipeline.build_context()`, the single initializer every
invocation path (CLI, `run_mp_ctv_daily.sh` / `run_select_ctv_daily.sh`,
Streamlit) goes through — so this check runs exactly once, everywhere,
without each entry point needing to remember to call it.

Bypass (local development only, never on the droplet or in CI):
    SBO_SKIP_GIT_GUARD=1
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime
from pathlib import Path


class GitGuardError(RuntimeError):
    """Raised when the local checkout is behind origin or not a git repo."""


def _run(args: list[str], cwd: Path) -> tuple[int, str, str]:
    proc = subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, timeout=30,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _find_repo_root(start: Path) -> Path | None:
    code, out, _ = _run(["git", "rev-parse", "--show-toplevel"], cwd=start)
    if code != 0 or not out:
        return None
    return Path(out)


def check_git_version(
    start_dir: Path | None = None,
    branch: str = "main",
) -> None:
    """Raise GitGuardError if the local checkout is behind origin/<branch>.

    No-ops (returns None) if SBO_SKIP_GIT_GUARD=1 is set — for local
    development only. Never set this on the droplet or in a scheduled job.
    """
    if os.environ.get("SBO_SKIP_GIT_GUARD") == "1":
        return

    start = start_dir or Path(__file__).resolve().parent.parent
    repo_root = _find_repo_root(start)
    if repo_root is None:
        raise GitGuardError(
            f"'{start}' is not inside a git repository. This tool pushes real "
            f"budget changes to live campaigns — running from an ad hoc copy "
            f"instead of a tracked `git clone` can't be verified as current. "
            f"Clone the repo properly, or set SBO_SKIP_GIT_GUARD=1 for local "
            f"scratch work only."
        )

    # Fetch without changing any local files
    code, _, err = _run(["git", "fetch", "origin", branch], cwd=repo_root)
    if code != 0:
        raise GitGuardError(
            f"`git fetch origin {branch}` failed — can't verify this checkout "
            f"is current: {err}"
        )

    _, local_sha, _ = _run(["git", "rev-parse", "HEAD"], cwd=repo_root)
    _, remote_sha, _ = _run(["git", "rev-parse", f"origin/{branch}"], cwd=repo_root)

    if local_sha == remote_sha:
        return  # up to date — proceed silently

    # Behind (or diverged) — count commits and gather the diff for the audit
    _, behind_count, _ = _run(
        ["git", "rev-list", "--count", f"HEAD..origin/{branch}"], cwd=repo_root,
    )
    _, log, _ = _run(
        ["git", "log", "--oneline", f"HEAD..origin/{branch}"], cwd=repo_root,
    )
    _, diffstat, _ = _run(
        ["git", "diff", "--stat", f"HEAD..origin/{branch}"], cwd=repo_root,
    )
    _, full_diff, _ = _run(
        ["git", "diff", f"HEAD..origin/{branch}"], cwd=repo_root,
    )
    _, dirty, _ = _run(["git", "status", "--porcelain"], cwd=repo_root)

    audit_dir = repo_root / "audit"
    audit_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M")
    audit_path = audit_dir / f"version_diff_{ts}.md"

    lines = [
        f"# Version mismatch — {ts}",
        "",
        f"Local checkout is **{behind_count or '?'} commit(s) behind** "
        f"`origin/{branch}`.",
        "",
        "## Missing commits",
        "```",
        log or "(none listed — check `git fetch` output above)",
        "```",
        "",
        "## Files changed",
        "```",
        diffstat or "(no diffstat available)",
        "```",
    ]
    if dirty:
        lines += [
            "",
            "## You also have uncommitted local changes",
            "```",
            dirty,
            "```",
            "",
            "Reconcile these before pulling — see Section 3/4 of the team",
            "git workflow doc, or paste this audit file into a Claude Code",
            "session and ask for help merging your changes with what's on",
            "GitHub now.",
        ]
    lines += [
        "",
        "## Full diff",
        "```diff",
        full_diff or "(no diff available)",
        "```",
    ]
    audit_path.write_text("\n".join(lines))

    raise GitGuardError(
        f"BLOCKED — local checkout is {behind_count or '?'} commit(s) behind "
        f"origin/{branch}. Run `git pull` before continuing. "
        f"Audit written to: {audit_path.relative_to(repo_root)}"
    )
