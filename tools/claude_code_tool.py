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
import logging
import os
import re
import sqlite3
import subprocess
import uuid

from tools.registry import registry, tool_error
from tools import claude_code_core as core

logger = logging.getLogger(__name__)

#: Single greppable token for every way a filed job can end up unable to wake
#: anyone. 13.08.2026: eight cards were filed, zero subscriptions were written,
#: and both the successes and the failures arrived in silence — the only signal
#: was a ``chat_notify: false`` field inside a success JSON that nothing reads.
#: Anything that costs a wake-up now logs at WARNING under this prefix and
#: leaves a comment on the card itself.
WAKE_LOG_PREFIX = "claude_code WAKE"

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
    """The kanban DB that ``hermes kanban --board <board>`` actually writes to.

    ⚑ This used to hand-build ``<root>/boards/<board>/kanban.db`` and so could
    name a DIFFERENT file than the CLI one line above it. ``kanban_db_path``
    honours ``HERMES_KANBAN_DB`` **before** the ``board`` argument (kanban_db.py
    :1033, "the dispatcher injects this into worker env"), and the dispatcher
    pins that var in every worker it spawns (kanban_db.py :11839). So inside a
    kanban worker the CLI ignores ``--board`` and writes to the pinned board,
    while the hand-built path pointed at the requested one.

    Measured 13.08.2026: a worker pinned to board ``hermes`` filed three typed
    jobs with the default board ``hermes-infra``. All three cards were created
    on ``hermes``; the follow-up UPDATE ran against ``hermes-infra``, matched no
    row, and ``tasks.session_id`` stayed NULL on all three — silently, because
    the caller only recorded a boolean. On ``hermes-infra`` (where the pin and
    the requested board agreed) all five cards of the same day stamped fine.

    Only the pin is adopted here, not the whole of ``kanban_db_path``: that
    function anchors on ``kanban_home()``, while this tool deliberately keeps
    its own ``HERMES_CLAUDE_CODE_KANBAN_ROOT`` seam so a test can never reach
    the real board. The pin is the entire disagreement."""
    pinned = os.environ.get("HERMES_KANBAN_DB", "").strip()
    if pinned:
        return os.path.expanduser(pinned)
    if board == "default":
        return os.path.join(cfg["kanban_root"], "kanban.db")
    return os.path.join(cfg["kanban_root"], "boards", board, "kanban.db")


def _effective_board(db_path, cfg):
    """Name the board the card really landed on, derived from the DB file.

    The response used to echo back the *requested* board even when the env pin
    sent the card elsewhere — a field that reads like a fact and is not one."""
    try:
        parent = os.path.basename(os.path.dirname(db_path))
        root = os.path.realpath(cfg["kanban_root"])
        if os.path.realpath(os.path.dirname(db_path)) == root:
            return "default"
        return parent or "default"
    except Exception:
        return None


def _stamp_session_id(cfg, board, card, session_id):
    """``create`` has no flag for ``tasks.session_id`` — and the gateway wakes
    a session ONLY when the task carries it. Stamped directly, same move the
    Givi runner makes for its receipt cards.

    Returns ``(ok, reason)``. ``ok`` is True only when exactly one row changed:
    an UPDATE that matched nothing "succeeds" in sqlite, and reporting that as a
    stamped wake-up would be one more transport signal lying about work. The
    reason exists because the previous shape (bare bool, caller swallowing every
    exception) is what made the 13.08 miss undiagnosable."""
    db_path = _board_db(cfg, board)
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        cur = conn.execute("UPDATE tasks SET session_id = ? WHERE id = ?",
                           (str(session_id), card))
        conn.commit()
        if cur.rowcount == 1:
            return True, "ok"
        return False, ("UPDATE matched %d rows in %s — the card is not in this "
                       "DB" % (cur.rowcount, db_path))
    finally:
        conn.close()


#: Scan bound for the gateway_routing fallback. The old value (500) was a
#: silent truncation: the table is keyed by session_key and grows with every
#: chat/thread ever seen (272 rows on 13.08.2026), so a busy install would have
#: started missing addresses with no signal at all. The exact json_extract
#: lookup below is tried first and needs no bound; this only guards the
#: compatibility scan used when JSON1 is unavailable.
_ROUTING_SCAN_LIMIT = 20000


def _resolve_origin(cfg, session_id):
    """Find the calling conversation's chat in gateway_routing by session_id.

    Exact-key lookup — NOT the Givi runner's recency inference, which exists
    only because the file-queue there erases the caller's identity. Returns
    {chat_id, chat_type, thread_id} or None. Best-effort, read-only URI.

    ⚑ This answers for FAR fewer callers than it looks. ``gateway_routing`` is
    written only by the gateway session store (gateway/session.py :1656 and the
    full rewrite in ``_save_entries``) and its primary key is the *session_key*
    — ``agent:main:<platform>:<chat_type>:<chat_id>:<thread>`` — not the session
    id. So it holds exactly ONE row per chat/thread: the session that is current
    there. Two whole classes of caller get nothing back:

      * a kanban worker (``sessions.source='kanban'``) is a CLI subprocess and
        never had a row at all;
      * a gateway session that has since been superseded in its own chat had its
        row overwritten by the newer session, even though the chat address it
        wants is sitting in that very row.

    Both were live on 13.08.2026 and between them accounted for all eight cards
    of that day arriving with nobody subscribed. This function is therefore no
    longer the only rung — see :func:`_wire_wake`."""
    if not session_id:
        return None
    entries = []
    try:
        conn = sqlite3.connect("file:%s?mode=ro" % cfg["state_db"], uri=True,
                               timeout=10)
        try:
            try:
                rows = conn.execute(
                    "SELECT entry_json FROM gateway_routing "
                    "WHERE json_extract(entry_json, '$.session_id') = ? "
                    "ORDER BY updated_at DESC LIMIT 1", (session_id,)).fetchall()
            except sqlite3.Error:
                # JSON1 missing — fall back to the scan, but with a bound big
                # enough that hitting it is a real anomaly worth logging.
                rows = conn.execute(
                    "SELECT entry_json FROM gateway_routing "
                    "ORDER BY updated_at DESC LIMIT ?",
                    (_ROUTING_SCAN_LIMIT,)).fetchall()
                if len(rows) >= _ROUTING_SCAN_LIMIT:
                    logger.warning(
                        "%s: gateway_routing scan hit its %d-row bound; an "
                        "address may be missed", WAKE_LOG_PREFIX,
                        _ROUTING_SCAN_LIMIT)
            entries = rows
        finally:
            conn.close()
    except Exception:
        return None
    for (raw,) in entries:
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


def _wire_wake(cfg, board, card, session_id):
    """Register who hears about this job's completion. Returns (ok, how, tried).

    The ladder is not invented here — it is the one ``kanban_create`` has always
    used for its own cards, ``tools.kanban_tools._maybe_auto_subscribe``:

      1. gateway ContextVars (``HERMES_SESSION_PLATFORM``/``_CHAT_ID``) — the
         address the messaging gateway sets before dispatch;
      2. ``HERMES_SESSION_KEY`` decoded — a gateway-born key literally carries
         platform/chat_type/chat_id/thread/user, so it survives the session
         rotation that empties this session's ``gateway_routing`` row;
      3. the worker's OWN card (``HERMES_KANBAN_TASK`` → that card's
         ``kanban_notify_subs`` row) — "a card a worker spawns belongs to the
         same conversation". This is the rung that was missing here, and it is
         precisely the one a kanban worker needs.

    Calling it rather than re-deriving an address also inherits the parts this
    tool never had: the telegram DM-topic reply anchor in ``delivery_metadata``,
    the never-NULL ``notifier_profile`` floor (a NULL owner is the 2026-07-27
    silent-loss class), and the caught-up ``last_event_id`` snapshot.

    Rung 4 keeps today's behaviour: the exact ``gateway_routing`` lookup by
    session id, used only if the shared ladder came up empty."""
    tried = []
    try:
        from hermes_cli import kanban_db as _kb
        from tools import kanban_tools as _kt
    except Exception as exc:
        tried.append("kanban_tools import failed: %r" % (exc,))
        return False, None, tried

    db_path = _board_db(cfg, board)
    conn = None
    try:
        conn = _kb.connect(db_path=_pathlike(db_path))
        if _kt._maybe_auto_subscribe(conn, card):
            # Read the row back rather than trusting the return value: a
            # subscription that "succeeded" without landing a deliverable row is
            # the same lying transport signal this whole tool exists to kill.
            got = _readback_sub(conn, card)
            if got:
                return True, got, tried
            tried.append("kanban auto-subscribe reported success but wrote no "
                         "deliverable row")
        else:
            tried.append(
                "kanban auto-subscribe found no address "
                "(session_platform=%r session_key=%r kanban_task=%r)"
                % (_session_env("HERMES_SESSION_PLATFORM"),
                   bool(_session_env("HERMES_SESSION_KEY")),
                   os.environ.get("HERMES_KANBAN_TASK") or None))

        origin = _resolve_origin(cfg, session_id)
        if not origin:
            tried.append("gateway_routing has no row for session %r"
                         % (session_id or None,))
            return False, None, tried
        _kb.add_notify_sub(
            conn, task_id=card, platform="telegram",
            chat_id=origin["chat_id"], chat_type=origin["chat_type"],
            thread_id=origin.get("thread_id"),
            notifier_profile=_kb.resolve_notifier_profile(),
            delivery_metadata={k: v for k, v in (
                ("thread_id", origin.get("thread_id")),
                ("chat_type", origin.get("chat_type"))) if v} or None)
        got = _readback_sub(conn, card)
        if got:
            return True, got, tried
        tried.append("gateway_routing address written but no row read back")
        return False, None, tried
    except Exception as exc:
        tried.append("subscribe raised: %r" % (exc,))
        return False, None, tried
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _readback_sub(conn, card):
    """``<platform>:<chat_id>`` of a deliverable subscription on ``card``, else
    None.

    ⚑ ``tui`` rows are RETURNED, not dropped. They were dropped on the theory
    that the TUI poller is "a different consumer", but it is a real one: the TUI
    gateway polls ``kanban_notify_subs`` every ``_KANBAN_POLL_SECONDS``
    (``tui_gateway/server.py`` :8999). Dropping them produced the mirror-image
    of the lie this patch exists to kill — a card whose completion WILL be
    delivered, stamped "⚠ БЕЗ ПРОБУЖДЕНИЯ … проверять руками" forever — and, worse,
    sent the caller on to rung 4, which wrote a SECOND subscription row and
    would have delivered the same completion twice.

    A chat row still wins when both exist; the caller says "it lands in the TUI,
    not in Telegram" rather than "nobody will hear about this".
    """
    try:
        row = conn.execute(
            "SELECT platform, chat_id FROM kanban_notify_subs "
            "WHERE task_id = ? AND COALESCE(chat_id, '') != '' "
            "ORDER BY (platform = 'tui'), created_at DESC LIMIT 1",
            (card,)).fetchone()
    except Exception:
        return None
    if not row:
        return None
    return "%s:%s" % (row[0], row[1])


def _pathlike(p):
    """``kanban_db.connect(db_path=...)`` expects a Path; keep the import local
    so this module still imports where pathlib is all we need."""
    from pathlib import Path
    return Path(p)


def _session_env(name):
    """Best-effort read of a gateway session ContextVar, for diagnostics only."""
    try:
        from gateway.session_context import get_session_env
        return get_session_env(name, "") or None
    except Exception:
        return None


def _mark_card_unwired(cfg, board, card, reason):
    """Leave the failure ON THE CARD, not only in a response field.

    ``comment`` is the existing verb for this (hermes_cli/kanban.py :578) and
    shows up in ``kanban show`` / ``kanban tail``. Deliberately NOT the title:
    the claimer feeds the title into the run prompt and (since 13.08) into the
    commit/PR title, so a banner there would leak into published work."""
    text = ("⚠ БЕЗ ПРОБУЖДЕНИЯ: завершение этой карточки никого не разбудит "
            "и никуда не придёт — проверять руками "
            "(claude_code(action=status, card_id=%r)). Причина: %s" % (card, reason))
    try:
        res = _kanban(cfg, board, "comment", card, text, "--author", "claude_code")
    except Exception as exc:
        logger.warning("%s: could not comment the no-wake mark onto %s: %r",
                       WAKE_LOG_PREFIX, card, exc)
        return False
    if res.returncode != 0:
        # The loudest of the three channels was the only one that failed
        # silently: an exception was logged, a non-zero exit code was not. This
        # patch's own rule is "confirm by the fact, not by the return code", and
        # a locked kanban.db / a missing kanban_bin / the 120 s timeout all land
        # here.
        logger.warning("%s: kanban comment rc=%s — the no-wake mark is NOT on "
                       "card %s: %s", WAKE_LOG_PREFIX, res.returncode, card,
                       ((res.stderr or "") + (res.stdout or "")).strip()[:300])
        return False
    return True


_payload_block = core.format_payload_block


# --------------------------------------------------------------------------
# Cancel / no-claude helpers (13.08.2026: «сказал стоп — карточку взяли через
# 4 секунды», infra/orchestration-deadlock-20260813.md §5б: the tool knew only
# run|status, so the bot had no cancel verb at all)
# --------------------------------------------------------------------------

#: Statuses the tool may block DIRECTLY: the card is still in the queue and
#: nobody has claimed it. A ``running`` card is never closed from here —
#: Batch-4 invariant: only the claiming side may close/block a claimed card —
#: so for it the tool only raises the flag and says so on the card.
_CANCELLABLE_QUEUE_STATUSES = ("todo", "ready", "scheduled")


def _card_family(cfg, board, card, cascade):
    """``(targets, statuses, err)`` — the card itself plus (with ``cascade``)
    every DESCENDANT reachable through ``task_links`` (children, their
    children, …). ``statuses`` maps the ids actually present in the board DB
    to their status; an id absent from the map was not found there.

    Read-only sqlite over the same DB file the CLI writes (``_board_db``
    honours the dispatcher's ``HERMES_KANBAN_DB`` pin, so a worker cancelling
    its own children looks at the board the cards really live on). ``err`` is
    a human-readable reason when the DB could not be read — the caller still
    proceeds with what it has (flags do not need the DB), but must say so.
    """
    db_path = _board_db(cfg, board)
    targets = [card]
    statuses = {}
    try:
        conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True,
                               timeout=10)
    except Exception as exc:
        return targets, statuses, "kanban DB unreadable (%s): %r" % (db_path, exc)
    try:
        try:
            if cascade:
                seen = {card}
                frontier = [card]
                while frontier:
                    marks = ",".join("?" * len(frontier))
                    rows = conn.execute(
                        "SELECT parent_id, child_id FROM task_links "
                        "WHERE parent_id IN (%s)" % marks, frontier).fetchall()
                    frontier = []
                    for _parent, child in rows:
                        if child and child not in seen:
                            seen.add(child)
                            targets.append(child)
                            frontier.append(child)
            marks = ",".join("?" * len(targets))
            for tid, status in conn.execute(
                    "SELECT id, status FROM tasks WHERE id IN (%s)" % marks,
                    targets):
                statuses[tid] = status
        except sqlite3.Error as exc:
            return targets, statuses, "kanban DB query failed: %r" % (exc,)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return targets, statuses, None


def _requested_by(session_id):
    return "session:%s" % session_id if session_id else "claude_code tool"


def _cancel(args, session_id):
    """Stop filed work. ``all=true`` raises the lane-wide stop flag; a
    ``card_id`` cancels the card and (``cascade``, default true) its
    descendants. Queue-state cards are blocked right here with
    ``[cancelled] <reason>``; a RUNNING card only gets its flag + a comment —
    the claimer, being the claiming side (Batch-4), is the one that kills the
    process and closes the card."""
    cfg = _cfg()
    board = args.get("board") or cfg["board"]
    reason = (args.get("reason") or "").strip() or "остановлено по команде"
    cascade = args.get("cascade")
    cascade = True if cascade is None else bool(cascade)
    control = core.control_dir()
    requested_by = _requested_by(session_id)

    if args.get("all"):
        try:
            flag_path = core.write_stop_flag(
                control, "stop_all", requested_by=requested_by, reason=reason)
        except Exception as exc:
            return tool_error("could not write the stop-all flag: %r" % (exc,),
                              error_type="claude_code_stop_flag_failed")
        return json.dumps({
            "cancelled": "all",
            "flagged": [str(flag_path)],
            "blocked": [],
            "kill_pending": [],
            "note": ("stop-all поднят: клеймер не возьмёт новые карточки и "
                     "прервёт живой прогон на ближайшем опросе (штатно ≤5 с). "
                     "Снять: claude_code(action='resume_lane')."),
        }, ensure_ascii=False)

    card = (args.get("card_id") or "").strip()
    if not _CARD_ID_RE.fullmatch(card):
        return tool_error("cancel needs card_id (t_xxxxxxxx) or all=true",
                          error_type="claude_code_bad_card")

    targets, statuses, family_err = _card_family(cfg, board, card, cascade)
    flagged, blocked, kill_pending = [], [], []
    skipped, errors = {}, {}
    for tid in targets:
        try:
            core.write_stop_flag(
                control, "cancel", card=tid, board=board,
                requested_by=requested_by, reason=reason,
                cascade_from=(card if tid != card else None))
            flagged.append(tid)
        except Exception as exc:
            # No flag → no claimer-side protection for this id; that is a
            # failure of the cancel, not a footnote.
            errors[tid] = "flag: %r" % (exc,)
            continue
        status = statuses.get(tid)
        if status in _CANCELLABLE_QUEUE_STATUSES:
            try:
                res = _kanban(cfg, board, "block", tid,
                              "[cancelled] %s" % reason)
            except Exception as exc:
                errors[tid] = "block: %r" % (exc,)
                continue
            if res.returncode == 0:
                blocked.append(tid)
            else:
                errors[tid] = "block rc=%s: %s" % (
                    res.returncode,
                    ((res.stderr or "") + (res.stdout or "")).strip()[:200])
        elif status == "running":
            kill_pending.append(tid)
            try:
                res = _kanban(
                    cfg, board, "comment", tid,
                    "⏹ cancel: остановит клеймер — карточка claim-нута, "
                    "закрыть её может только claim-нувшая сторона (Batch-4); "
                    "флаг %s поднят. Причина: %s"
                    % (core.stop_flag_path(control, "cancel", tid).name, reason),
                    "--author", "claude_code")
                if res.returncode != 0:
                    errors[tid] = "comment rc=%s (флаг всё равно поднят)" % res.returncode
            except Exception as exc:
                errors[tid] = "comment: %r (флаг всё равно поднят)" % (exc,)
        else:
            skipped[tid] = status or "not found in %s" % _board_db(cfg, board)
    out = {
        "cancelled": card,
        "cascade": cascade,
        "targets": targets,
        "flagged": flagged,
        "blocked": blocked,
        "kill_pending": kill_pending,
    }
    if skipped:
        out["skipped"] = skipped
    if errors:
        out["errors"] = errors
    if family_err:
        out["warning"] = ("каскад мог быть неполным: %s — отменён только сам "
                          "%s; потомков проверить руками (kanban show)"
                          % (family_err, card))
    return json.dumps(out, ensure_ascii=False)


def _no_claude(args, session_id):
    """«Сделай без Claude Code»: durable per-card flag (the claimer refuses the
    card even after restarts) + an IMMEDIATE ``hermes kanban assign <id>
    default`` attempt so a queued card moves to a plain worker now. Both
    outcomes are reported honestly, per card."""
    cfg = _cfg()
    board = args.get("board") or cfg["board"]
    card = (args.get("card_id") or "").strip()
    if not _CARD_ID_RE.fullmatch(card):
        return tool_error("no_claude needs card_id (t_xxxxxxxx)",
                          error_type="claude_code_bad_card")
    reason = (args.get("reason") or "").strip() or "делать без Claude Code"
    cascade = args.get("cascade")
    cascade = True if cascade is None else bool(cascade)
    control = core.control_dir()
    requested_by = _requested_by(session_id)

    targets, statuses, family_err = _card_family(cfg, board, card, cascade)
    flagged, reassigned = [], []
    errors = {}
    for tid in targets:
        try:
            core.write_stop_flag(
                control, "no_claude", card=tid, board=board,
                requested_by=requested_by, reason=reason,
                cascade_from=(card if tid != card else None))
            flagged.append(tid)
        except Exception as exc:
            errors[tid] = "flag: %r" % (exc,)
            continue
        try:
            res = _kanban(cfg, board, "assign", tid, "default")
        except Exception as exc:
            errors[tid] = "assign: %r (флаг поднят — терминал карточку не возьмёт)" % (exc,)
            continue
        if res.returncode == 0:
            reassigned.append(tid)
        else:
            errors[tid] = "assign rc=%s: %s (флаг поднят — терминал карточку не возьмёт)" % (
                res.returncode,
                ((res.stderr or "") + (res.stdout or "")).strip()[:200])
    out = {
        "card": card,
        "cascade": cascade,
        "targets": targets,
        "flagged": flagged,
        "reassigned": reassigned,
    }
    if errors:
        out["errors"] = errors
    if family_err:
        out["warning"] = ("каскад мог быть неполным: %s — помечен только сам "
                          "%s" % (family_err, card))
    return json.dumps(out, ensure_ascii=False)


def _resume_lane(args):
    """Take the lane-wide stop down (core.resume_lane — the one sanctioned
    deletion). Per-card cancel/no-claude flags stay: re-opening the lane is
    not un-cancelling a card."""
    try:
        resumed = core.resume_lane(core.control_dir())
    except Exception as exc:
        return tool_error("could not remove the stop-all flag: %r" % (exc,),
                          error_type="claude_code_resume_failed")
    return json.dumps({
        "resumed": resumed,
        "note": ("stop-all снят — клеймер снова берёт карточки" if resumed
                 else "stop-all и не стоял; ничего не менялось"),
    }, ensure_ascii=False)


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

    db_path = _board_db(cfg, board)
    board_effective = _effective_board(db_path, cfg) or board
    if board_effective != board:
        # Not cosmetic: the env pin decides where the card really is, and until
        # now the response named the board the caller asked for regardless.
        logger.warning(
            "%s: card %s was filed on board %r, not the requested %r "
            "(HERMES_KANBAN_DB pins %s)",
            WAKE_LOG_PREFIX, card, board_effective, board, db_path)

    woken = False
    stamp_reason = "no session_id on the calling turn"
    if session_id:
        try:
            woken, stamp_reason = _stamp_session_id(cfg, board, card, session_id)
        except Exception as exc:
            woken, stamp_reason = False, "UPDATE raised: %r" % (exc,)
        if not woken:
            logger.warning("%s: session_id NOT stamped on %s (session=%s): %s",
                           WAKE_LOG_PREFIX, card, session_id, stamp_reason)

    # The address ladder is independent of session_id: a kanban worker has no
    # usable session identity but does have the card it is running, and that
    # card knows where its conversation lives.
    subscribed, sub_how, sub_tried = _wire_wake(cfg, board, card, session_id)

    # ⚑ Two DIFFERENT claims, and conflating them cries wolf on a card that will
    # in fact be delivered. Delivery runs entirely off the SUBSCRIPTION: the
    # notifier loop (gateway/kanban_watchers.py :225-240) walks
    # ``kanban_notify_subs`` and never reads ``tasks.session_id``. So:
    #   * no subscription  → nobody hears about this at all. That is the 13.08
    #     failure, and it stays loud: WARNING + a comment on the card + a
    #     top-level "warning" the model cannot read past.
    #   * subscription but no stamped session_id → the result WILL arrive in the
    #     chat; only re-entering THIS session is impossible. That is a soft note,
    #     not a ⚠ banner and not a permanent comment on the card. Any transient
    #     failure of _stamp_session_id (locked DB, board skew) used to land here
    #     and told the model to go back to polling — exactly the polling the
    #     typed tool was built to remove.
    wired = bool(subscribed)
    via_tui = bool(subscribed and str(sub_how or "").startswith("tui:"))
    warning = None
    if subscribed:
        if via_tui:
            note = ("Не опрашивай карточку: завершение придёт в TUI (адрес: %s), "
                    "не в Telegram." % sub_how)
        else:
            note = ("Не опрашивай карточку: завершение придёт в этот разговор "
                    "через kanban-нотификатор (адрес: %s)." % sub_how)
        if not woken:
            note += (" Разбудить именно эту сессию не выйдет (%s) — результат "
                     "всё равно доставится." % stamp_reason)
    else:
        # 13.08.2026: this state was reachable eight times in one day and its
        # only trace was a false-valued field inside a success payload. It is
        # now a WARNING in the log AND a comment on the card AND a top-level
        # "warning" key the model cannot read past.
        why = "chat_notify=false (%s)" % (
            "; ".join(sub_tried) or "no address found")
        if not woken:
            why += "; session_wake=false (%s)" % stamp_reason
        warning = ("ЗАВЕРШЕНИЕ ЭТОЙ ЗАДАЧИ НИКОМУ НЕ ПРИДЁТ. " + why)
        logger.warning("%s: card %s on board %s is NOT wired for wake-up: %s",
                       WAKE_LOG_PREFIX, card, board_effective, why)
        _mark_card_unwired(cfg, board, card, why)
        note = ("⚠ Автопробуждения НЕ будет — сам проверяй "
                "claude_code(action=status, card_id=%r) и сам сообщи результат "
                "человеку." % card)

    out = {
        "filed": True,
        "card": card,
        "board": board_effective,
        "board_requested": board,
        "job_id": job_id,
        "lane": "terminal",
        "mode": norm["mode"],
        "timeout_seconds": norm["timeout_seconds"],
        "session_wake": woken,
        "chat_notify": subscribed,
        "notify_via": sub_how,
        "notify_is_tui": via_tui,
        "wake_wired": wired,
        "artifacts_dir": os.path.join(cfg["runs_root"], card),
        "how_to_check": "claude_code(action=status, card_id=%r)" % card,
        "note": note,
    }
    if warning:
        out["warning"] = warning
    return json.dumps(out, ensure_ascii=False)


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

    # Active stop/cancel flags for this card (and the lane): a status readout
    # that hides a raised flag would report a cancelled job as merely "slow".
    try:
        flags = core.read_stop_flags(core.control_dir(), card)
        out["stop_flags"] = {k: v for k, v in flags.items() if v} or None
    except Exception:
        out["stop_flags"] = None
    return json.dumps(out, ensure_ascii=False)


def handle_claude_code(args, **kw):
    args = args or {}
    action = args.get("action") or "run"
    session_id = kw.get("session_id") or ""
    if action == "run":
        return _run(args, session_id)
    if action == "status":
        return _status(args)
    if action == "cancel":
        return _cancel(args, session_id)
    if action == "no_claude":
        return _no_claude(args, session_id)
    if action == "resume_lane":
        return _resume_lane(args)
    return tool_error(
        "action must be one of 'run', 'status', 'cancel', 'no_claude', "
        "'resume_lane'", error_type="claude_code_bad_action")


CLAUDE_CODE_SCHEMA = {
    "name": "claude_code",
    "description": (
        "Launch Claude Code as a supervised background job on the kanban "
        "terminal lane (the single typed launch contract — never compose a "
        "claude-hermes shell command). action='run' files the job and returns "
        "the card id; the run executes outside the gateway with file-backed "
        "artifacts, a hard timeout, budget cap and automatic ONE-shot resume "
        "if it times out mid-work. Completion wakes this conversation via the "
        "kanban notifier — do NOT poll. action='status' inspects a filed job. "
        "action='cancel' stops work: card_id cancels a card and (cascade, "
        "default true) its descendants — queued cards are blocked here, a "
        "running one is killed by the claimer via a durable flag; all=true "
        "stops the whole lane until resume_lane. action='no_claude' flags a "
        "card «делать без Claude Code» and reassigns it to a plain worker."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string",
                       "enum": ["run", "status", "cancel", "no_claude",
                                "resume_lane"],
                       "description": "run (default) files a job; status "
                                      "inspects one; cancel stops a card or "
                                      "(all=true) the lane; no_claude hands a "
                                      "card to a plain worker; resume_lane "
                                      "lifts a stop-all"},
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
                        "description": "action=status/cancel/no_claude: the "
                                       "card to inspect / stop / hand off"},
            "all": {"type": "boolean",
                    "description": "action=cancel: true stops the WHOLE "
                                   "terminal lane (no new claims, live run "
                                   "aborted) until resume_lane"},
            "reason": {"type": "string",
                       "description": "action=cancel/no_claude: why — lands "
                                      "on the flag, the card comment and the "
                                      "block reason"},
            "cascade": {"type": "boolean",
                        "description": "action=cancel/no_claude: also cover "
                                       "every descendant card via task_links "
                                       "(default true)"},
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
