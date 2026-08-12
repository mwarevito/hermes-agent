"""Typed ``claude_code`` tool — file a Claude Code job on the kanban terminal
lane instead of composing a shell command.

Path A (agent → ``terminal`` tool → hand-written ``claude-hermes …`` line) was
the only launch path with NO contract in code: the model re-derived the argv
from SKILL.md prose on every run, polled a transport signal that lied, and a
closed pipe read as a finished job (hermes-ops
infra/false-completion-20260811.md). This tool replaces that prose with a
validated job filed as a kanban card on the ``terminal`` lane:

* the terminal-claimer daemon (launchd, OUTSIDE the gateway) executes it via
  tools/claude_code_core.py — file-backed artifacts, killpg, verdict,
  marker-gated lane-busy, resume-on-timeout;
* the card carries ``tasks.session_id`` and a notify subscription, so the
  asking conversation is WOKEN with the result through the existing verified-
  delivery ledger — no polling, no in-gateway supervision that dies with a
  deferred restart;
* the gateway process never supervises the run, so a gateway restart cannot
  orphan or falsely-complete it.

Exposure: hidden everywhere unless the profile opts in with
``HERMES_CLAUDE_CODE_TOOL=1`` (checked live by ``check_fn``). The live repo is
shared by all seven gateways — a new tool must not appear in six bots' schemas
as a deploy side effect.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import uuid

from tools.registry import registry, tool_error
from tools import claude_code_core as core

# Wire format lives in the core (the claimer consumes it under system python
# and must not import this module, which pulls in the gateway registry).
PAYLOAD_SCHEMA = core.PAYLOAD_SCHEMA
PAYLOAD_BEGIN = core.PAYLOAD_BEGIN
PAYLOAD_END = core.PAYLOAD_END
extract_payload = core.extract_payload

_CARD_ID_RE = re.compile(r"\bt_[0-9a-f]{8}\b")
_KANBAN_TIMEOUT = 120

#: Margin added to the job timeout for the card's own --max-runtime. NOTE: the
#: kanban runtime reaper (enforce_max_runtime) filters worker_pid IS NOT NULL,
#: and a terminal claim has a NULL worker_pid — so this cap does not actually
#: gate the run today. It is set generously (and would still be too small for a
#: resume, which can reach ~2x timeout) purely as a harmless upper bound; the
#: real budget is the claimer's own timeout + claim TTL. Kept so a future reaper
#: change does not suddenly clip a legitimate long run.
CARD_RUNTIME_MARGIN_SECONDS = 900


# --------------------------------------------------------------------------
# Live configuration (read at CALL time, never at import — same file is
# imported by all profiles; each resolves its own paths/flags)
# --------------------------------------------------------------------------

def _cfg():
    home = os.path.expanduser("~")
    kanban_root = os.environ.get(
        "HERMES_CLAUDE_CODE_KANBAN_ROOT",
        os.path.join(home, ".hermes", "kanban"))
    return {
        "launcher": os.environ.get(
            "HERMES_CLAUDE_CODE_LAUNCHER",
            os.path.join(home, ".local", "bin", "claude-hermes")),
        "kanban_bin": os.environ.get(
            "HERMES_CLAUDE_CODE_KANBAN_BIN",
            os.path.join(home, ".hermes", "hermes-agent", "venv", "bin", "hermes")),
        "board": os.environ.get("HERMES_CLAUDE_CODE_BOARD", "hermes-infra"),
        "kanban_root": kanban_root,
        "runs_root": os.path.join(kanban_root, "terminal-runs"),
        "claimer_state": os.environ.get(
            "HERMES_CLAUDE_CODE_CLAIMER_STATE",
            os.path.join(home, ".hermes", "state",
                         "kanban-terminal-claimer.json")),
        "state_db": os.environ.get(
            "HERMES_CLAUDE_CODE_STATE_DB",
            os.path.join(os.environ.get("HERMES_HOME",
                                        os.path.join(home, ".hermes")),
                         "state.db")),
        "dotenv": os.environ.get(
            "HERMES_CLAUDE_CODE_DOTENV",
            os.path.join(os.environ.get("HERMES_HOME",
                                        os.path.join(home, ".hermes")), ".env")),
        "allowed_repo_roots": tuple(
            p for p in os.environ.get(
                "HERMES_CLAUDE_CODE_REPO_ROOTS",
                os.path.join(home, "coding") + os.sep + ":" +
                os.path.join(home, "hermes-dev") + os.sep,
            ).split(":") if p),
    }


def _claimer_health(cfg):
    """Read the terminal-claimer's state file: is a consumer alive, and does it
    speak our payload schema? Returns (ok, reason). A card filed when no live
    claimer understands CLAUDE-CODE-JOB-V1 would sit unrun or be mis-read as
    prose — the producer refuses rather than file a job into a void."""
    try:
        with open(cfg["claimer_state"], "r", encoding="utf-8") as f:
            st = json.load(f)
    except OSError:
        return False, "no terminal-claimer state file — the lane daemon is not installed/running"
    except Exception as e:
        return False, "terminal-claimer state unreadable: %s" % e
    age = None
    try:
        import time as _t
        age = _t.time() - float(st.get("updated_at") or 0)
    except Exception:
        age = None
    if age is None or age > 900:
        return False, ("terminal-claimer state is stale (%s) — the lane daemon "
                       "may be down; not filing a job that nothing will run"
                       % ("%.0fs" % age if age is not None else "unknown age"))
    if st.get("payload_schema") != PAYLOAD_SCHEMA:
        return False, ("terminal-claimer speaks %r, tool speaks %r — version "
                       "skew; a typed job would be mis-read"
                       % (st.get("payload_schema"), PAYLOAD_SCHEMA))
    return True, "ok"


def _policy(cfg):
    return {
        "allowed_repo_roots": cfg["allowed_repo_roots"],
        # Denied, in order of how the attack was found (adversarial review
        # 12.08): the live hermes checkout AND the versioned policy source that
        # the gates import at call time (hermes-ops) AND hermes-agent itself.
        # A job that could Edit any of these could rewrite the rule that admits
        # it. Kept identical to the Givi runner's DENIED_REPO_SUBSTRINGS.
        "denied_repo_substrings": ("/.hermes/", "/hermes-agent/", "/hermes-ops/"),
        "roots_reason": "repo must live under ~/coding or ~/hermes-dev",
        "denied_reason_fmt": ("repo path is denied (%s): it holds live Hermes "
                              "code or the policy source the gates import"),
        # Bash is NOT default even for the personal tool: a host shell is
        # requested explicitly (allowed_tools=["...","Bash"]), never handed out
        # silently. The claimer additionally requires a signed payload before it
        # will run anything with Bash.
        "default_tools": ("Read", "Glob", "Grep", "Edit", "Write", "MultiEdit"),
        "effort_levels": ("low", "medium", "high", "xhigh", "max"),
        "allow_budget": True,
        "allow_resume": False,  # resume is the claimer's recovery move, not an input
    }


def check_claude_code_requirements():
    """Expose the tool only where a profile explicitly opted in AND the
    launcher actually exists. Checked live (registry caches ~30s).

    Also require HERMES_HOME to be the personal ~/.hermes: the flag lives in the
    personal .env, but env can leak down an inherited process line into a clinic
    gateway (observed 23.07). Binding to the personal home stops the tool from
    silently appearing — with host Bash — in another bot's schema."""
    if os.environ.get("HERMES_CLAUDE_CODE_TOOL") != "1":
        return False
    home = os.path.expanduser("~")
    hermes_home = os.path.realpath(
        os.environ.get("HERMES_HOME", os.path.join(home, ".hermes")))
    if hermes_home != os.path.realpath(os.path.join(home, ".hermes")):
        return False
    return os.access(_cfg()["launcher"], os.X_OK)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _kanban(cfg, board, *args):
    return subprocess.run(
        [cfg["kanban_bin"], "kanban", "--board", board, *args],
        capture_output=True, text=True, errors="replace",
        timeout=_KANBAN_TIMEOUT)


def _board_db(cfg, board):
    if board == "default":
        return os.path.join(cfg["kanban_root"], "kanban.db")
    return os.path.join(cfg["kanban_root"], "boards", board, "kanban.db")


def _stamp_session_id(cfg, board, card, session_id):
    """``create`` has no flag for ``tasks.session_id`` — and the gateway wakes
    a session ONLY when the task carries it. Stamped directly, same move the
    Givi runner makes for its receipt cards.

    Returns True only when exactly one row changed: an UPDATE that matched
    nothing "succeeds" in sqlite, and reporting that as a stamped wake-up would
    be one more transport signal lying about work."""
    conn = sqlite3.connect(_board_db(cfg, board), timeout=10)
    try:
        cur = conn.execute("UPDATE tasks SET session_id = ? WHERE id = ?",
                           (str(session_id), card))
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def _resolve_origin(cfg, session_id):
    """Find the calling conversation's chat in gateway_routing by session_id.

    Exact-key lookup — NOT the Givi runner's recency inference, which exists
    only because the file-queue there erases the caller's identity. Returns
    {chat_id, chat_type, thread_id} or None. Best-effort, read-only URI."""
    if not session_id:
        return None
    try:
        conn = sqlite3.connect("file:%s?mode=ro" % cfg["state_db"], uri=True,
                               timeout=10)
        try:
            rows = conn.execute(
                "SELECT entry_json FROM gateway_routing "
                "ORDER BY updated_at DESC LIMIT 500").fetchall()
        finally:
            conn.close()
    except Exception:
        return None
    for (raw,) in rows:
        try:
            entry = json.loads(raw)
        except Exception:
            continue
        if entry.get("session_id") != session_id:
            continue
        origin = entry.get("origin") or {}
        if not origin.get("chat_id"):
            return None
        return {
            "chat_id": str(origin["chat_id"]),
            "chat_type": origin.get("chat_type") or "dm",
            "thread_id": origin.get("thread_id"),
        }
    return None


_payload_block = core.format_payload_block


# --------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------

def _run(args, session_id):
    cfg = _cfg()
    board = args.get("board") or cfg["board"]

    # The job runs under Tony's full host credentials, so the claimer will only
    # execute a payload it can authenticate. Refuse to file one we cannot sign —
    # an unsigned payload would just be blocked downstream, wasting a card.
    signing_key = core.read_signing_key(dotenv_path=cfg["dotenv"])
    if not signing_key:
        return tool_error(
            "claude_code signing key is not configured (HERMES_CLAUDE_CODE_"
            "SIGNING_KEY in ~/.hermes/.env); the terminal lane will not run an "
            "unauthenticated job",
            error_type="claude_code_unconfigured")

    # Producer↔consumer handshake: don't file into a void or a schema-skewed lane.
    ok, why = _claimer_health(cfg)
    if not ok:
        return tool_error(why, error_type="claude_code_no_consumer")

    job_input = {
        "mode": "worktree" if args.get("repo") else "scratch",
        "prompt": args.get("prompt"),
    }
    for key in ("repo", "timeout_seconds", "allowed_tools", "base", "model",
                "effort", "note", "max_budget_usd"):
        if args.get(key) is not None:
            job_input[key] = args[key]
    norm, err = core.validate_job(job_input, _policy(cfg))
    if err:
        return tool_error(err, error_type="claude_code_job_invalid")

    job_id = "cc-" + uuid.uuid4().hex[:12]
    payload = dict(norm)
    payload["schema"] = PAYLOAD_SCHEMA
    payload["job_id"] = job_id
    payload["filed_by_session"] = session_id or None
    payload[core.PAYLOAD_SIG_FIELD] = core.sign_payload(payload, signing_key)

    head = (norm.get("note") or norm["prompt"]).strip().splitlines()[0]
    title = "Claude Code: " + (head[:70] + ("…" if len(head) > 70 else ""))
    body = (
        "Typed claude_code job — исполняет terminal-claimer через общий "
        "контракт (tools/claude_code_core.py). Артефакты: %s/<card>/run-N/.\n\n%s"
        % (cfg["runs_root"], _payload_block(payload)))

    create = _kanban(cfg, board, "create", title, "--body", body,
                     "--assignee", "terminal",
                     "--max-runtime",
                     str(norm["timeout_seconds"] + CARD_RUNTIME_MARGIN_SECONDS))
    blob = (create.stdout or "") + (create.stderr or "")
    m = _CARD_ID_RE.search(blob)
    if create.returncode != 0 or not m:
        return tool_error(
            "kanban create failed (rc=%s): %s"
            % (create.returncode, blob.strip()[:400]),
            error_type="claude_code_file_failed")
    card = m.group(0)

    woken = False
    subscribed = False
    if session_id:
        try:
            woken = _stamp_session_id(cfg, board, card, session_id)
        except Exception:
            woken = False
        origin = _resolve_origin(cfg, session_id)
        if origin:
            sub = ["notify-subscribe", card, "--platform", "telegram",
                   "--chat-id", origin["chat_id"],
                   "--chat-type", origin["chat_type"]]
            if origin.get("thread_id"):
                sub += ["--thread-id", str(origin["thread_id"])]
            try:
                subscribed = _kanban(cfg, board, *sub).returncode == 0
            except Exception:
                subscribed = False

    # The wake path fires ONLY inside the notifier's loop over SUBSCRIPTIONS, so
    # a stamped session_id without a subscription wakes no one (adversarial
    # review 12.08). Promise "don't poll" only when BOTH are in place; otherwise
    # tell the model to check status itself — never write "you'll be woken" when
    # the mechanism won't.
    wired = bool(woken and subscribed)
    if wired:
        note = ("Не опрашивай карточку: завершение само разбудит эту сессию "
                "через kanban-нотификатор.")
    elif not session_id:
        note = "session_id недоступен — пробуждения не будет, проверяй status вручную."
    else:
        note = ("Подписка на пробуждение НЕ встала (session_wake=%s, chat_notify=%s) "
                "— автопробуждения не будет, проверяй claude_code(action=status) сам."
                % (woken, subscribed))

    return json.dumps({
        "filed": True,
        "card": card,
        "board": board,
        "job_id": job_id,
        "lane": "terminal",
        "mode": norm["mode"],
        "timeout_seconds": norm["timeout_seconds"],
        "session_wake": woken,
        "chat_notify": subscribed,
        "wake_wired": wired,
        "artifacts_dir": os.path.join(cfg["runs_root"], card),
        "how_to_check": "claude_code(action=status, card_id=%r)" % card,
        "note": note,
    }, ensure_ascii=False)


def _status(args):
    cfg = _cfg()
    board = args.get("board") or cfg["board"]
    card = (args.get("card_id") or "").strip()
    if not _CARD_ID_RE.fullmatch(card):
        return tool_error("card_id must look like t_xxxxxxxx",
                          error_type="claude_code_bad_card")

    out = {"card": card, "board": board}
    show = _kanban(cfg, board, "show", card, "--json")
    if show.returncode == 0:
        try:
            data = json.loads(show.stdout or "{}")
            task = data.get("task", data) if isinstance(data, dict) else {}
            out["task"] = {k: task.get(k) for k in
                           ("status", "assignee", "result", "updated_at",
                            "consecutive_failures") if k in task}
        except json.JSONDecodeError:
            out["task"] = {"raw": (show.stdout or "")[:1000]}
    else:
        out["task_error"] = (show.stderr or show.stdout or "").strip()[:400]

    runs_dir = os.path.join(cfg["runs_root"], card)
    runs = []
    try:
        names = sorted(n for n in os.listdir(runs_dir) if n.startswith("run-"))
    except OSError:
        names = []
    for name in names:
        d = os.path.join(runs_dir, name)
        entry = {"run": name}
        try:
            with open(os.path.join(d, "result.json"), encoding="utf-8") as f:
                entry["result"] = json.load(f)
        except Exception:
            entry["result"] = None
        entry["output_tail"] = core.tail_line(os.path.join(d, "output.txt"))
        runs.append(entry)
    out["runs"] = runs
    out["artifacts_dir"] = runs_dir
    return json.dumps(out, ensure_ascii=False)


def handle_claude_code(args, **kw):
    args = args or {}
    action = args.get("action") or "run"
    session_id = kw.get("session_id") or ""
    if action == "run":
        return _run(args, session_id)
    if action == "status":
        return _status(args)
    return tool_error("action must be 'run' or 'status'",
                      error_type="claude_code_bad_action")


CLAUDE_CODE_SCHEMA = {
    "name": "claude_code",
    "description": (
        "Launch Claude Code as a supervised background job on the kanban "
        "terminal lane (the single typed launch contract — never compose a "
        "claude-hermes shell command). action='run' files the job and returns "
        "the card id; the run executes outside the gateway with file-backed "
        "artifacts, a hard timeout, budget cap and automatic ONE-shot resume "
        "if it times out mid-work. Completion wakes this conversation via the "
        "kanban notifier — do NOT poll. action='status' inspects a filed job."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["run", "status"],
                       "description": "run (default) files a job; status inspects one"},
            "prompt": {"type": "string",
                       "description": "The task for Claude Code (plain text; "
                                      "required for action=run)"},
            "repo": {"type": "string",
                     "description": "Absolute repo path under ~/coding or "
                                    "~/hermes-dev; a fresh worktree is built "
                                    "from origin's default branch. Omit for a "
                                    "scratch workspace."},
            "base": {"type": "string",
                     "description": "Git ref to branch the worktree from "
                                    "(default: origin default branch)"},
            "timeout_seconds": {"type": "integer", "minimum": 60,
                                "maximum": 7200,
                                "description": "Hard budget (default 1800)"},
            "allowed_tools": {"type": "array", "items": {"type": "string"},
                              "description": "Claude Code tool whitelist "
                                             "(default incl. Bash)"},
            "model": {"type": "string",
                      "description": "Model override; omit to honor the "
                                     "configured Claude Code default"},
            "effort": {"type": "string",
                       "enum": ["low", "medium", "high", "xhigh", "max"],
                       "description": "Effort override; omit for default"},
            "max_budget_usd": {"type": "number",
                               "description": "Dollar cap PER RUN (>=0.01), a "
                                              "second fuse after the timeout. A "
                                              "timed-out job resumes once with a "
                                              "fresh cap, so worst-case card "
                                              "spend is up to 2x this."},
            "note": {"type": "string",
                     "description": "Short human label for the card title"},
            "board": {"type": "string",
                      "description": "Kanban board (default hermes-infra)"},
            "card_id": {"type": "string",
                        "description": "action=status: the card to inspect"},
        },
        "required": [],
    },
}

registry.register(
    name="claude_code",
    toolset="terminal",
    schema=CLAUDE_CODE_SCHEMA,
    handler=handle_claude_code,
    check_fn=check_claude_code_requirements,
    emoji="🧰",
)
