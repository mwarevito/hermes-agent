"""Authoritative binding context.

The approval must bind to identity the *model* cannot forge. Two tiers:

* Session identity (platform / chat / thread / user / session) comes from the
  gateway's per-turn ``session_context`` contextvars — set from the inbound
  Telegram ``message`` object, never from anything the model emits.
* Profile / ``HERMES_HOME`` are process-global, fixed at startup by
  ``_apply_profile_override`` — immutable for the life of the process.
* Kanban task identity is the subtle one. ``HERMES_KANBAN_TASK`` /
  ``HERMES_KANBAN_RUN_ID`` / ``HERMES_KANBAN_CLAIM_LOCK`` live in the
  environment, which a shell worker can rewrite. So we do **not** trust the env
  alone: we read the task's ``claim_lock`` and ``current_run_id`` straight from
  the kanban SQLite DB and require the env values to match the DB anchor. A
  worker that rewrites ``HERMES_KANBAN_TASK`` to point at a task it doesn't own
  will present env values that disagree with that task's DB row → we fail
  closed. Retries (new run id) and reassignment (new claim lock) change the DB
  anchor, so a grant bound to the old run/claim never matches — no
  child/retry/reassign inheritance.

When there is no kanban task at all (a plain Telegram chat), the task anchor is
absent and binding is to (platform, chat, thread, user, profile, session) only —
a deliberately weaker, explicit scope.
"""

from __future__ import annotations

import os
from typing import Dict, Optional


class ContextError(Exception):
    """Context could not be established authoritatively → gate fails closed."""


def _session(name: str, default: str = "") -> str:
    try:
        from gateway.session_context import get_session_env
        return get_session_env(name, default)
    except Exception:
        return os.environ.get(name, default)


def _profile() -> str:
    # Profile name is derived from HERMES_HOME / HERMES_PROFILE, both fixed at
    # process start. Fall back to the home path so distinct profiles never
    # collide even if the name var is absent.
    prof = os.environ.get("HERMES_PROFILE", "").strip()
    if prof:
        return prof
    home = os.environ.get("HERMES_HOME", "").strip()
    return home or "default"


def _validate_kanban_anchor() -> Dict[str, str]:
    """Return {task_id, run_id, claim_lock} validated against the kanban DB.

    Raises :class:`ContextError` if the env claims a task/run/claim that the DB
    does not corroborate (forgery attempt) → the gate fails closed rather than
    binding to an attacker-chosen task."""
    task_id = (os.environ.get("HERMES_KANBAN_TASK") or "").strip()
    if not task_id:
        return {"task_id": "", "run_id": "", "claim_lock": ""}
    env_run = (os.environ.get("HERMES_KANBAN_RUN_ID") or "").strip()
    env_lock = (os.environ.get("HERMES_KANBAN_CLAIM_LOCK") or "").strip()
    try:
        from hermes_cli import kanban_db
        conn = kanban_db.connect()
        try:
            task = kanban_db.get_task(conn, task_id)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        # Kanban DB unavailable but env claims a task → cannot corroborate →
        # fail closed.
        raise ContextError(f"kanban anchor unverifiable: {exc}") from exc
    if task is None:
        raise ContextError(f"kanban task {task_id!r} not found in DB")
    db_lock = (task.claim_lock or "").strip()
    db_run = str(task.current_run_id or "").strip()
    # The env-provided anchor must exactly match the DB anchor. This is what
    # defeats a worker that rewrote HERMES_KANBAN_* to impersonate another task.
    if db_lock == "" or db_run == "":
        raise ContextError(f"kanban task {task_id!r} has no live claim/run anchor")
    if env_lock != db_lock or env_run != db_run:
        raise ContextError(
            "kanban env anchor does not match DB anchor "
            "(task/run/claim mismatch — possible forgery)"
        )
    return {"task_id": task_id, "run_id": db_run, "claim_lock": db_lock}


def bind_context(*, require_kanban_anchor: bool = False,
                 kanban_validator=None) -> Dict[str, str]:
    """Assemble the authoritative binding context.

    ``kanban_validator`` is injectable for tests; defaults to the real DB check.
    """
    validator = kanban_validator or _validate_kanban_anchor
    anchor = validator()
    if require_kanban_anchor and not anchor.get("task_id"):
        raise ContextError("kanban task anchor required but absent")
    ctx = {
        "platform": _session("HERMES_SESSION_PLATFORM"),
        "chat_id": _session("HERMES_SESSION_CHAT_ID"),
        "thread_id": _session("HERMES_SESSION_THREAD_ID"),
        "user_id": _session("HERMES_SESSION_USER_ID"),
        "session_id": _session("HERMES_SESSION_ID"),
        "profile": _profile(),
        "task_id": anchor.get("task_id", ""),
        "run_id": anchor.get("run_id", ""),
        "claim_lock": anchor.get("claim_lock", ""),
    }
    return ctx
