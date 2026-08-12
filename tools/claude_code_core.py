"""Shared core for every Claude Code launch path — the single launch contract.

Extracted 2026-08-12 from scripts/givi_claude_code_runner.py (hermes-ops), the
one launch path that was built correctly (file-backed capture, killpg, verdict,
lane-busy detection), so that the typed ``claude_code`` tool, the kanban
terminal-claimer and the Givi job runner stop re-implementing the same launch
logic three slightly-different ways. Canon: hermes-ops
infra/false-completion-20260811.md §2 «Второй корень: интеграция существует как
строка шелла» — the difference between the working and the broken launch was
one character (``>`` vs ``| tee``); this module is where that difference is
owned once.

Hard constraints, enforced by tests/tools/test_claude_code_core.py:

* **stdlib only, Python 3.9 compatible.** The claimer and the Givi runner run
  under /usr/bin/python3 (system 3.9, launchd) and import this file from the
  live repo checkout. No hermes-agent imports, no third-party imports.
* **No environment reads and no side effects at import time.** Every consumer
  passes its own paths/policy as arguments; four different test modules load
  the Givi runner under four module names in one pytest run and must keep
  getting identical, stateless behavior.
* **Absence of evidence is reported as absence, never as success** (verdict
  ``unknown``; ``lane busy`` only with positive markers; a poll that cannot
  read a pid treats the slot as busy-unknown, not free).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import shutil
import signal
import subprocess
import time
import uuid
from datetime import datetime, timezone

CORE_VERSION = "1.0.0"

# --------------------------------------------------------------------------
# Shared constants (formerly module globals of the Givi runner)
# --------------------------------------------------------------------------

#: Lines that mean the run did not do its job, whatever the exit code said.
#: Deliberately narrow: each one is unambiguous evidence, not a heuristic about
#: tone. A false "suspect" costs a second look; a false "ok" is what happened
#: 2026-08-08 (``status:"ok", exit_code:0`` around ``FATAL: … is not set``).
FAILURE_MARKERS = (
    "FATAL:",
    "Traceback (most recent call last)",
    "command not found",
    "is not set",
    "No such file or directory",
    "Permission denied",
    "ModuleNotFoundError",
    "npm ERR!",
    "fatal: not a git repository",
)

#: How much of each output file's tail the verdict scan reads.
VERDICT_SCAN_BYTES = 256 * 1024

#: Every way a launcher has said "the lane is busy", newest first. The first
#: marker is printed to BOTH stdout and stderr since 2026-08-09 (a caller that
#: captures only one stream must still see the reason); the rest are legacy
#: wordings that must keep matching (see hermes-ops tests/test_givi_lane_busy.py).
LANE_BUSY_MARKERS = (
    "CLAUDE_CODE_LANE_BUSY",
    "lane still busy",
    "launch refused",
    "Could not acquire Claude Code lock",
)

#: Exit code the launcher uses for "lane busy, retry later".
LANE_BUSY_EXIT_CODE = 75

#: Untracked files a repo needs to actually RUN but never commits; a fresh
#: worktree gets tracked content only, so these are carried in by copy.
WORKTREE_CARRY_FILES = (".env", ".env.local")

#: Defaults for the launch process itself.
DEFAULT_TERM_GRACE_SECONDS = 15
DEFAULT_KILL_GRACE_SECONDS = 30
DEFAULT_POLL_INTERVAL_SECONDS = 5.0

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_iso_ts(value):
    """Parse an ISO timestamp to epoch seconds, or None. Never raises.

    A NAIVE timestamp is interpreted as LOCAL time — gateway_routing rows are
    written naive-local, and the notify-inference windows depend on that
    (forcing UTC here shifted every window by the host's UTC offset and broke
    the Givi inference tests: the refusal windows are part of the contract).
    """
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return None


def human_secs(seconds):
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return "?"
    if seconds < 60:
        return "%ds" % seconds
    if seconds < 3600:
        return "%dm %02ds" % (seconds // 60, seconds % 60)
    return "%dh %02dm" % (seconds // 3600, (seconds % 3600) // 60)


def tail_line(path, limit=180):
    """Last non-empty line of a file, truncated; '' when unreadable/empty."""
    try:
        with open(path, "rb") as f:
            try:
                size = os.fstat(f.fileno()).st_size
                if size > 65536:
                    f.seek(size - 65536)
            except OSError:
                pass
            blob = f.read(65536 + 4096).decode("utf-8", "replace")
    except OSError:
        return ""
    for line in reversed(blob.splitlines()):
        line = line.strip()
        if line:
            return line[:limit]
    return ""


def safe_mtime(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def read_job(path, max_bytes=1_000_000):
    if os.path.getsize(path) > max_bytes:
        raise ValueError("job file exceeds %d bytes" % max_bytes)
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return json.load(f)


def _noop_log(msg):  # pragma: no cover - trivial
    pass


# --------------------------------------------------------------------------
# Claude session ids and argv building (the resume half of the contract)
# --------------------------------------------------------------------------

def mint_claude_session_id():
    """Mint the Claude session id BEFORE launch.

    Minted up front and passed via ``--session-id`` so the id survives crash,
    timeout and SIGKILL without parsing any output stream: resume must never
    depend on the run having died politely.
    """
    return str(uuid.uuid4())


def is_claude_session_id(value):
    return bool(isinstance(value, str) and _UUID_RE.match(value.strip().lower()))


def build_argv(
    launcher,
    prompt,
    output_format="text",
    allowed_tools=None,
    append_system_prompt=None,
    model=None,
    effort=None,
    claude_session_id=None,
    resume_session_id=None,
    max_budget_usd=None,
    fork_session=False,
):
    """Build the launcher argv. Pure; raises ValueError on caller bugs.

    The prompt always goes AFTER a ``--`` argv terminator (no flag injection
    from semi-trusted prompt text). ``claude_session_id`` (fresh run, minted
    id) and ``resume_session_id`` (continue an existing session) are mutually
    exclusive by construction.
    """
    if not launcher:
        raise ValueError("launcher is required")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("prompt (non-empty string) is required")
    if claude_session_id and resume_session_id:
        raise ValueError("claude_session_id and resume_session_id are mutually exclusive")
    for sid, flag in ((claude_session_id, "--session-id"),
                      (resume_session_id, "--resume")):
        if sid is not None and not is_claude_session_id(sid):
            raise ValueError("%s must be a UUID, got %r" % (flag, sid))
    if fork_session and not resume_session_id:
        raise ValueError("fork_session only makes sense with resume_session_id")

    argv = [launcher, "-p", "--output-format", output_format]
    if allowed_tools:
        argv += ["--allowedTools", ",".join(allowed_tools)]
    if append_system_prompt:
        argv += ["--append-system-prompt", append_system_prompt]
    if model:
        argv += ["--model", model]
    if effort:
        argv += ["--effort", effort]
    if claude_session_id:
        argv += ["--session-id", claude_session_id]
    if resume_session_id:
        argv += ["--resume", resume_session_id]
        if fork_session:
            argv += ["--fork-session"]
    if max_budget_usd is not None:
        try:
            budget = float(max_budget_usd)
        except (TypeError, ValueError):
            raise ValueError("max_budget_usd must be a number")
        # Floor 0.01: the 2-decimal wire format renders anything smaller as
        # "0", and what claude does with a $0 budget is undefined.
        if not (0.01 <= budget <= 1000):
            raise ValueError("max_budget_usd out of range [0.01, 1000]")
        argv += ["--max-budget-usd", "%.2f" % budget]
    argv += ["--", prompt]
    return argv


def resume_decision(prev_result, workspace_exists):
    """Whether the NEXT run for the same task should resume the previous
    Claude session instead of starting over.

    Rules (deliberately narrow — resume is a recovery move, not a lifestyle):
    * the previous run must have minted+recorded a ``claude_session_id``;
    * it must have ended ``timeout`` (ran out of time mid-work) — an ``error``
      run is not resumed: its session state is what produced the error;
    * its workspace must still exist (the session's files are its memory);
    * at most ONE resume per chain: a run that was itself a resume never
      chains another (``resumed_from`` present ⇒ start fresh).

    Returns {"resume": bool, "claude_session_id": str|None, "reason": str}.
    """
    prev = prev_result if isinstance(prev_result, dict) else {}
    sid = prev.get("claude_session_id")
    if not is_claude_session_id(sid or ""):
        return {"resume": False, "claude_session_id": None,
                "reason": "no claude_session_id recorded by the previous run"}
    if prev.get("resumed_from"):
        return {"resume": False, "claude_session_id": None,
                "reason": "previous run was already a resume (max 1 per chain)"}
    if prev.get("status") != "timeout":
        return {"resume": False, "claude_session_id": None,
                "reason": "previous run ended %r, only timeout is resumable"
                          % prev.get("status")}
    if not workspace_exists:
        return {"resume": False, "claude_session_id": None,
                "reason": "workspace is gone; session files are its memory"}
    return {"resume": True, "claude_session_id": sid,
            "reason": "previous run timed out mid-work with a live workspace"}


# --------------------------------------------------------------------------
# Verdict and lane-busy detection (evidence over exit codes)
# --------------------------------------------------------------------------

def verdict(status, out_dir):
    """Say whether the run's OUTPUT agrees with its exit code.

    Values: ``ok`` | ``suspect`` | ``failed`` | ``unknown``. ``unknown`` when
    the output cannot be read — absence of evidence is reported as absence,
    never as success. An exit code describes the process; it does not
    describe the work.
    """
    if status != "ok":
        return "failed"
    if not out_dir:
        return "unknown"
    seen = False
    for name in ("output.txt", "stderr.txt"):
        path = os.path.join(out_dir, name)
        try:
            with open(path, "rb") as f:
                try:
                    size = os.fstat(f.fileno()).st_size
                    if size > VERDICT_SCAN_BYTES:
                        f.seek(size - VERDICT_SCAN_BYTES)
                except OSError:
                    pass
                blob = f.read(VERDICT_SCAN_BYTES + 4096).decode("utf-8", "replace")
        except OSError:
            continue
        seen = True
        for marker in FAILURE_MARKERS:
            if marker in blob:
                return "suspect"
    return "ok" if seen else "unknown"


def lane_busy_in_text(rc, *stream_texts):
    """True when exit 75 means "lane busy, try again" — in-memory variant."""
    if rc != LANE_BUSY_EXIT_CODE:
        return False
    for text in stream_texts:
        if text and any(marker in text for marker in LANE_BUSY_MARKERS):
            return True
    return False


def launcher_was_busy(rc, out_dir):
    """True when exit 75 means "lane busy, try again", not "the job failed".

    Both streams are read, not just stderr: 2026-08-09 proved a caller may
    capture only one of them, and the one it captures must be enough. A bare
    75 with no marker is NOT busy — it is claude's own exit code.
    """
    if rc != LANE_BUSY_EXIT_CODE:
        return False
    for name in ("stderr.txt", "output.txt"):
        try:
            with open(os.path.join(out_dir, name), "rb") as f:
                tail = f.read(8192).decode("utf-8", "replace")
        except OSError:
            continue
        if any(marker in tail for marker in LANE_BUSY_MARKERS):
            return True
    return False


# --------------------------------------------------------------------------
# Lane slot probe — THE single source of the launcher's slot layout
# --------------------------------------------------------------------------

def lane_slot_paths(lane_base, slots, scheme="hermes"):
    """Slot lock-dir paths for a launcher's lane.

    ``hermes`` (claude-hermes): slot 1 is ``lane_base`` itself, extras are
    ``lane_base.2..N``. ``givi`` (claude-givi): with slots>1 ALL slots are
    ``lane_base.1..N`` (bare base only when slots==1). Two launchers, two
    layouts — mirrored here once so probes stop drifting per consumer.
    """
    slots = max(1, int(slots))
    if scheme == "givi" and slots > 1:
        return ["%s.%d" % (lane_base, i) for i in range(1, slots + 1)]
    if scheme not in ("hermes", "givi"):
        raise ValueError("unknown lane scheme: %r" % scheme)
    out = [lane_base]
    out += ["%s.%d" % (lane_base, i) for i in range(2, slots + 1)]
    return out


def _pid_alive(pid):
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True


def lane_free(lane_base, slots, scheme="hermes", log_fn=None):
    """Is at least one lane slot free? None = could not tell.

    Read-only probe: creates NOTHING (a probe that takes a slot becomes the
    very competitor it guards against). A dir whose recorded owner pid is dead
    counts as free (the launcher reuses it). Extra slots may still be closed
    by the launcher's memory guard — "free by dirs" is an upper bound and the
    launcher has the final word.
    """
    try:
        free = 0
        for slot in lane_slot_paths(lane_base, slots, scheme):
            if not os.path.isdir(slot):
                free += 1
                continue
            try:
                with open(os.path.join(slot, "pid")) as f:
                    holder = f.read().strip()
            except OSError:
                holder = ""
            if not holder or not _pid_alive(holder):
                free += 1
        return free > 0
    except Exception as e:
        (log_fn or _noop_log)("lane probe failed: %s" % e)
        return None


# --------------------------------------------------------------------------
# Workspaces
# --------------------------------------------------------------------------

def carry_untracked_env(repo, ws, files=WORKTREE_CARRY_FILES, log_fn=None):
    """Copy a repo's untracked .env into a fresh worktree.

    Copied, not symlinked: a symlink would let a job edit — or delete — the
    real repo's secrets through the worktree. Permissions narrowed to owner.
    Silent on absence: most repos have no .env and that is not a fault.
    2026-08-08: a worktree job without the repo's .env reported ``ok/exit 0``
    around ``FATAL: TEST_ANTHROPIC_API_KEY is not set``.
    """
    log_fn = log_fn or _noop_log
    for name in files:
        src = os.path.join(repo, name)
        dst = os.path.join(ws, name)
        if not os.path.isfile(src) or os.path.exists(dst):
            continue
        try:
            shutil.copyfile(src, dst)
            os.chmod(dst, 0o600)
            log_fn("worktree: carried untracked %s into %s" % (name, ws))
        except OSError as exc:
            log_fn("worktree: could NOT carry %s (%s) — a job needing it will fail"
                   % (name, exc))


def worktree_registered(repo, ws, git_bin="/usr/bin/git"):
    lst = subprocess.run([git_bin, "-C", repo, "worktree", "list", "--porcelain"],
                         capture_output=True, text=True, errors="replace", timeout=60)
    if lst.returncode != 0:
        return False
    return any(line.strip() == "worktree " + ws for line in lst.stdout.splitlines())


def remove_worktree(repo, ws, branch, git_bin="/usr/bin/git"):
    """Remove a worktree + its branch. Callers capture the diff first."""
    subprocess.run([git_bin, "-C", repo, "worktree", "remove", "--force", ws],
                   capture_output=True, timeout=120)
    if branch:
        subprocess.run([git_bin, "-C", repo, "branch", "-D", branch],
                       capture_output=True, timeout=60)
    subprocess.run([git_bin, "-C", repo, "worktree", "prune"],
                   capture_output=True, timeout=60)


def prepare_workspace(
    job_id,
    mode,
    repo=None,
    base=None,
    worktrees_root=None,
    scratch_root=None,
    git_bin="/usr/bin/git",
    ws_prefix="cc-",
    branch_prefix="cc/",
    carry_files=WORKTREE_CARRY_FILES,
    log_fn=None,
    reused_base_note="(reused worktree)",
    local_head_note="локальный HEAD",
    origin_down_note="(локальный HEAD — origin недоступен)",
):
    """Build the run's workspace. Returns (ws, branch_or_None, base_used_or_None).

    Branches from the REMOTE default, not from whatever the local clone
    happens to sit on (a month-stale clone must not silently become the base).
    Raises RuntimeError on failure.
    """
    if mode == "scratch":
        if not scratch_root:
            raise RuntimeError("scratch mode requires scratch_root")
        ws = os.path.join(scratch_root, job_id)
        os.makedirs(ws, exist_ok=True)
        return ws, None, None

    if mode != "worktree":
        raise RuntimeError("unknown workspace mode: %r" % mode)
    if not (repo and worktrees_root):
        raise RuntimeError("worktree mode requires repo and worktrees_root")

    ws = os.path.join(worktrees_root, ws_prefix + job_id)
    branch = branch_prefix + job_id
    if os.path.isdir(ws):
        # Requeue/resume reuse: accept only if git already knows this worktree.
        if worktree_registered(repo, ws, git_bin=git_bin):
            return ws, branch, reused_base_note
        raise RuntimeError(
            "workspace path exists but is not a registered worktree: %s" % ws)

    if not base:
        subprocess.run([git_bin, "-C", repo, "fetch", "origin", "--quiet"],
                       capture_output=True, timeout=300)
        for cand in ("origin/HEAD", "origin/main", "origin/master"):
            rc = subprocess.run(
                [git_bin, "-C", repo, "rev-parse", "--verify", "--quiet", cand],
                capture_output=True, timeout=60)
            if rc.returncode == 0:
                base = cand
                break
    args = [git_bin, "-c", "core.hooksPath=/dev/null", "-C", repo,
            "worktree", "add", ws, "-b", branch]
    if base:
        args.append(base)
    add = subprocess.run(args, capture_output=True, text=True,
                         errors="replace", timeout=300)
    if add.returncode != 0 and base:
        # Offline / no remote: fall back to local HEAD but say so in the result.
        add = subprocess.run(args[:-1], capture_output=True, text=True,
                             errors="replace", timeout=300)
        base = origin_down_note if add.returncode == 0 else base
    if add.returncode != 0:
        raise RuntimeError("git worktree add failed: %s"
                           % (add.stderr or add.stdout).strip()[:2000])
    carry_untracked_env(repo, ws, files=carry_files, log_fn=log_fn)
    return ws, branch, (base or local_head_note)


def capture_diff(ws, out_dir, git_bin="/usr/bin/git"):
    """Best-effort diff incl. new files via intent-to-add; never raises.
    Bytes mode throughout: repo content is not guaranteed utf-8."""
    status_text = ""
    try:
        subprocess.run([git_bin, "-C", ws, "add", "-N", "."],
                       capture_output=True, timeout=120)
        try:
            diff = subprocess.run([git_bin, "-C", ws, "diff", "HEAD"],
                                  capture_output=True, timeout=120)
            with open(os.path.join(out_dir, "diff.patch"), "wb") as f:
                f.write(diff.stdout or b"")
        finally:
            subprocess.run([git_bin, "-C", ws, "reset", "-q"],
                           capture_output=True, timeout=120)
        st = subprocess.run([git_bin, "-C", ws, "status", "--porcelain"],
                            capture_output=True, timeout=60)
        status_text = (st.stdout or b"").decode("utf-8", "replace").strip()
    except Exception as e:
        status_text = "diff-capture-failed: %s" % e
        try:
            p = os.path.join(out_dir, "diff.patch")
            if not os.path.exists(p):
                open(p, "wb").close()
        except OSError:
            pass
    return status_text


# --------------------------------------------------------------------------
# Job validation
# --------------------------------------------------------------------------

_MODEL_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,63}$")
_BASE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,100}$")
_CHAT_ID_RE = re.compile(r"^-?\d{1,20}$")
_THREAD_ID_RE = re.compile(r"^\d{1,12}$")

#: Baseline policy knobs; every consumer builds its own dict on top.
DEFAULT_POLICY = {
    "allowed_repo_roots": (),
    "denied_repo_substrings": (),
    "roots_reason": "repo is outside the allowed roots",
    "denied_reason_fmt": ("repo path is denied (%s): it holds Hermes or Givi "
                          "policy source, which a job may not edit"),
    "tool_whitelist": frozenset((
        "Read", "Glob", "Grep", "Edit", "Write", "MultiEdit", "NotebookEdit",
        "Bash", "WebSearch", "WebFetch",
    )),
    # Bash is NOT a default: a job gets a host shell under Tony's credentials
    # only when it asks for it EXPLICITLY (and, on the terminal lane, only when
    # its payload is signed). The default set can read and edit files but not
    # spawn arbitrary processes.
    "default_tools": ("Read", "Glob", "Grep", "Edit", "Write", "MultiEdit"),
    "max_prompt_bytes": 100_000,
    "timeout_bounds": (60, 7200),
    "default_timeout": 1800,
    "effort_levels": ("low", "medium", "high"),
    "git_bin": "/usr/bin/git",
    # Resume/budget fields are opt-in so existing queue consumers (whose key
    # sets are cross-pinned by policy tests) keep normalizing byte-identically.
    "allow_resume": False,
    "allow_budget": False,
}


def validate_job(job, policy):
    """Validate + normalize a job dict against a consumer policy.

    Returns (normalized_job, None) or (None, reason). Unknown fields are
    ignored. Check ORDER inside worktree mode is part of the contract
    (deny-before-exists — a denied path must be refused even if absent).
    """
    pol = dict(DEFAULT_POLICY)
    pol.update(policy or {})

    if not isinstance(job, dict):
        return None, "job JSON must be an object"
    mode = job.get("mode", "scratch")
    if mode not in ("worktree", "scratch"):
        return None, "mode must be 'worktree' or 'scratch'"
    prompt = job.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        return None, "prompt (non-empty string) is required"
    if "\x00" in prompt:
        return None, "prompt contains a NUL byte"
    if len(prompt.encode("utf-8", "replace")) > pol["max_prompt_bytes"]:
        return None, "prompt exceeds %d utf-8 bytes" % pol["max_prompt_bytes"]

    norm = {"mode": mode, "prompt": prompt}

    if mode == "worktree":
        repo = job.get("repo")
        if not isinstance(repo, str) or not repo or "\x00" in repo:
            return None, "worktree mode requires 'repo'"
        real = os.path.realpath(repo)
        if not any(real.startswith(root) for root in pol["allowed_repo_roots"]):
            return None, "%s (got %s)" % (pol["roots_reason"], real)
        hit = [s for s in pol["denied_repo_substrings"] if s in real + "/"]
        if hit:
            return None, pol["denied_reason_fmt"] % hit[0]
        if not os.path.isdir(real):
            return None, "repo dir does not exist: %s" % real
        rc = subprocess.run([pol["git_bin"], "-C", real, "rev-parse", "--git-dir"],
                            capture_output=True, timeout=30)
        if rc.returncode != 0:
            return None, "repo is not a git repository: %s" % real
        norm["repo"] = real

    lo, hi = pol["timeout_bounds"]
    ts = job.get("timeout_seconds", pol["default_timeout"])
    if not isinstance(ts, int) or not (lo <= ts <= hi):
        return None, "timeout_seconds must be an int in %d..%d" % (lo, hi)
    norm["timeout_seconds"] = ts

    tools = job.get("allowed_tools", list(pol["default_tools"]))
    if (not isinstance(tools, list) or not tools
            or not all(isinstance(t, str) and t in pol["tool_whitelist"]
                       for t in tools)):
        return None, ("allowed_tools must be a non-empty subset of %s"
                      % sorted(pol["tool_whitelist"]))
    norm["allowed_tools"] = tools

    model = job.get("model")
    if model is not None:
        if not isinstance(model, str) or not _MODEL_RE.match(model):
            return None, "model failed validation"
        norm["model"] = model

    base = job.get("base")
    if base is not None:
        if not isinstance(base, str) or not _BASE_RE.match(base):
            return None, "base must be a git ref like origin/main"
        norm["base"] = base

    effort = job.get("effort")
    if effort is not None:
        if effort not in pol["effort_levels"]:
            return None, "effort must be one of %s" % (tuple(pol["effort_levels"]),)
        norm["effort"] = effort

    norm["note"] = job.get("note") if isinstance(job.get("note"), str) else None

    notify = job.get("notify")
    if notify is not None:
        if not isinstance(notify, dict) or not notify.get("chat_id"):
            return None, "notify must be an object with chat_id"
        chat_id = str(notify["chat_id"])
        if not _CHAT_ID_RE.match(chat_id):
            return None, "notify.chat_id must be a numeric Telegram chat id"
        thread_id = notify.get("thread_id")
        if thread_id not in (None, "") and not _THREAD_ID_RE.match(str(thread_id)):
            return None, "notify.thread_id must be numeric"
        norm["notify"] = {"chat_id": chat_id, "thread_id": thread_id}

    if pol["allow_budget"]:
        budget = job.get("max_budget_usd")
        if budget is not None:
            if not isinstance(budget, (int, float)) or isinstance(budget, bool) \
                    or not (0.01 <= float(budget) <= 1000):
                return None, "max_budget_usd must be a number in [0.01, 1000]"
            norm["max_budget_usd"] = float(budget)

    if pol["allow_resume"]:
        sid = job.get("resume_claude_session_id")
        if sid is not None:
            if not is_claude_session_id(sid):
                return None, "resume_claude_session_id must be a UUID"
            norm["resume_claude_session_id"] = sid.strip().lower()

    return norm, None


# --------------------------------------------------------------------------
# The launch itself
# --------------------------------------------------------------------------

def run_claude(
    argv,
    cwd,
    out_dir,
    timeout_seconds,
    poll_cb=None,
    poll_interval=DEFAULT_POLL_INTERVAL_SECONDS,
    max_file_bytes=None,
    term_grace_seconds=DEFAULT_TERM_GRACE_SECONDS,
    kill_grace_seconds=DEFAULT_KILL_GRACE_SECONDS,
):
    """Run an already-built launcher argv with file-backed capture.

    The contract everything else hangs off:

    * stdout/stderr stream to ``out_dir/output.txt`` / ``stderr.txt`` — never
      captured to memory (artifacts must survive the supervisor's death);
    * the child gets its own session (``start_new_session=True``) and, on
      deadline, its whole PROCESS GROUP is killed: SIGTERM, wait
      ``term_grace_seconds``, SIGKILL, wait ``kill_grace_seconds``; an
      unkillable child yields ``kill_failed=True`` rather than a hung
      supervisor;
    * a short poll loop (never one blocking wait) so ``poll_cb(elapsed_s)``
      can refresh liveness/status while Claude works;
    * ``max_file_bytes`` (RLIMIT_FSIZE) caps runaway output at the OS level.

    Returns a dict: rc, timed_out, kill_failed, started_at, duration_seconds,
    out_path, err_path.
    """
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "output.txt")
    err_path = os.path.join(out_dir, "stderr.txt")

    preexec = None
    if max_file_bytes:
        def preexec():  # pragma: no cover - runs in the child
            try:
                import resource
                resource.setrlimit(resource.RLIMIT_FSIZE,
                                   (max_file_bytes, max_file_bytes))
            except (ValueError, OSError):
                pass

    started = utcnow_iso()
    t0 = time.time()
    timed_out = False
    kill_failed = False
    with open(out_path, "wb") as out_f, open(err_path, "wb") as err_f:
        proc = subprocess.Popen(argv, cwd=cwd, stdout=out_f, stderr=err_f,
                                start_new_session=True, preexec_fn=preexec)
        deadline = t0 + timeout_seconds
        try:
            while True:
                try:
                    rc = proc.wait(timeout=poll_interval)
                    break
                except subprocess.TimeoutExpired:
                    if time.time() >= deadline:
                        raise
                    if poll_cb is not None:
                        try:
                            poll_cb(time.time() - t0)
                        except Exception:
                            pass  # a status callback must never kill the run
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except OSError:
                pass
            try:
                rc = proc.wait(timeout=term_grace_seconds)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    pass
                try:
                    rc = proc.wait(timeout=kill_grace_seconds)
                except subprocess.TimeoutExpired:
                    # Unkillable (D-state?): give up rather than hang forever.
                    kill_failed = True
                    rc = -1
    return {
        "rc": rc,
        "timed_out": timed_out,
        "kill_failed": kill_failed,
        "started_at": started,
        "duration_seconds": round(time.time() - t0, 1),
        "out_path": out_path,
        "err_path": err_path,
    }


def write_result_json(out_dir, result):
    """Write result.json and guarantee output.txt exists (consumers read it
    unconditionally). Returns the result.json path."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "result.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    out_txt = os.path.join(out_dir, "output.txt")
    if not os.path.exists(out_txt):
        open(out_txt, "wb").close()
    return path


# --------------------------------------------------------------------------
# Structured job payload on a kanban card (typed tool → terminal-claimer)
# --------------------------------------------------------------------------

PAYLOAD_SCHEMA = "claude-code-job-v1"
PAYLOAD_BEGIN = "CLAUDE-CODE-JOB-V1-BEGIN"
PAYLOAD_END = "CLAUDE-CODE-JOB-V1-END"


def format_payload_block(payload):
    """Render a job payload as the sentinel block a card body carries."""
    return "%s\n%s\n%s" % (
        PAYLOAD_BEGIN, json.dumps(payload, ensure_ascii=False, indent=2),
        PAYLOAD_END)


def extract_payload(body_text):
    """Parse a structured job payload out of a card body. Returns dict or
    None. This is the single wire format between the typed ``claude_code``
    tool (producer) and the terminal-claimer (consumer)."""
    if not body_text:
        return None
    try:
        begin = body_text.index(PAYLOAD_BEGIN) + len(PAYLOAD_BEGIN)
        end = body_text.index(PAYLOAD_END, begin)
    except ValueError:
        return None
    try:
        payload = json.loads(body_text[begin:end].strip())
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or payload.get("schema") != PAYLOAD_SCHEMA:
        return None
    return payload


# --------------------------------------------------------------------------
# Payload authentication — WHO produced this job, not what it says
# --------------------------------------------------------------------------
#
# The terminal lane executes cards under Tony's full host credentials (a
# personal claude-hermes launch, outside the docker sandbox and outside every
# Givi gate). But a card body is written by whoever files the card — and the
# semi-trusted workbot (Givi) can file cards with assignee=terminal via
# kanban_create on its own boards. Without authentication, a hand-assembled
# CLAUDE-CODE-JOB-V1 block from Givi would run arbitrary host Bash as Tony.
#
# So a typed job is trusted ONLY if it carries a valid HMAC signature made with
# a key that lives in the personal ~/.hermes/.env — readable by the tool (in
# the personal gateway) and the claimer (launchd, Tony's uid), but NOT by Givi:
# Givi's file tools run inside docker and its own profile .env is separate.
# Absence of a key is fail-CLOSED at the consumer: an unsigned/forged payload is
# refused, never run. (Legacy prose cards without a payload block are a separate,
# pre-existing path constrained by ~/.claude/settings.json.)

PAYLOAD_SIG_FIELD = "sig"
_SIGNING_ENV_KEY = "HERMES_CLAUDE_CODE_SIGNING_KEY"


def _canonical_payload_bytes(payload):
    """Deterministic bytes over every field EXCEPT the signature itself."""
    body = {k: v for k, v in payload.items() if k != PAYLOAD_SIG_FIELD}
    return json.dumps(body, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def sign_payload(payload, key):
    """Return the hex HMAC-SHA256 of the payload under *key*. Raises ValueError
    if the key is missing — signing must never silently produce an empty sig."""
    if not key:
        raise ValueError("signing key is required")
    k = key.encode("utf-8") if isinstance(key, str) else key
    return hmac.new(k, _canonical_payload_bytes(payload), hashlib.sha256).hexdigest()


def payload_signature_valid(payload, key):
    """Constant-time check that payload[sig] matches an HMAC made with *key*.

    False on: no key, no signature, malformed signature, or mismatch. Never
    raises — a consumer must be able to treat any anomaly as "not authentic"."""
    if not key or not isinstance(payload, dict):
        return False
    got = payload.get(PAYLOAD_SIG_FIELD)
    if not isinstance(got, str) or not got:
        return False
    try:
        want = sign_payload(payload, key)
    except Exception:
        return False
    return hmac.compare_digest(got, want)


def read_signing_key(env=None, dotenv_path=None):
    """Resolve the signing key: process env first, then a KEY=VALUE line in the
    personal ~/.hermes/.env (the claimer does not inherit that file's vars).
    Returns the key string or None. Never raises."""
    env = os.environ if env is None else env
    val = env.get(_SIGNING_ENV_KEY)
    if val:
        return val.strip() or None
    path = dotenv_path or os.path.join(os.path.expanduser("~"), ".hermes", ".env")
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line.startswith(_SIGNING_ENV_KEY + "="):
                    return line.split("=", 1)[1].strip().strip("'\"") or None
    except OSError:
        return None
    return None
