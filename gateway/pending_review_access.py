"""Who may review staged memory/skill writes from a chat, and where an
owner-only notice about one is allowed to land.

Why this module exists (2026-08-12)
-----------------------------------
The gateway already routes ``/skills`` and ``/memory`` to the shared
write-approval handler (``hermes_cli.write_approval_commands``), but it applied
no identity check: ``gateway.slash_access.SlashAccessPolicy`` only gates
commands when ``allow_admin_from`` is configured, and on every live profile it
is not — so ``policy.enabled`` is False and *every allowed sender* could run
``/skills approve <id>``, read ``/skills diff <id>``, or turn the gate off with
``/skills approval off``.

That is not a theoretical hole. Measured on the M1 the same day:

  * profile ``kivi`` (Gogi — the Llucky sales bot that sits in chats with
    CLIENTS) has ``skills.write_approval: true`` and
    ``TELEGRAM_GROUP_ALLOWED_USERS=*``, and sets no ``allow_admin_from``;
  * so a prospect in a group chat with Gogi could approve a rewrite of the
    bot's own skills, or silently disable the approval gate;
  * and its pending queue held seven un-reviewed proposals from 16–28 June.

Approval is a *policy* boundary, not a convenience: the whole point of staging
a background-review write is that a human — one specific human — decides. So
this module answers exactly two questions, both fail-closed:

  1. ``check_pending_review_access`` — may THIS sender review staged writes?
     Only the resolved profile owner, only from a direct message.
  2. ``owner_notice_chat_id`` — where may a notice ABOUT a staged write be
     delivered? Only the owner's DM. When that cannot be addressed we return
     None and the caller stays silent (and logs) rather than posting into
     whatever chat the turn happened in.

Owner resolution is explicit-first and never widens to "everybody":

  1. ``gateway.pending_approval_owner`` in config.yaml — a single id. Set this
     on any profile with more than one admin; it is the only unambiguous
     answer.
  2. otherwise the FIRST concrete id of the platform's **DM** allowlist
     (``platforms.<p>.extra.allow_from``, else ``<PLATFORM>_ALLOWED_USERS``).
     This is not a guess about ordering: ``hermes setup`` seeds that variable
     with the detected owner's own id (``auto_owner_user_id`` in
     ``hermes_cli/gateway.py``), and later admins are appended.
  3. otherwise **nobody**. A wildcard (``*``) resolves to nobody by
     construction — it names everyone, so it names no owner. The group
     allowlist is never consulted at all.

When resolution yields nobody the review surface refuses and names the config
key to set. That is deliberate: a queue nobody can spend is a visible problem,
and an approval anybody can grant is an invisible one.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# config.yaml dotpath for the explicit owner id.
OWNER_CONFIG_KEY = "gateway.pending_approval_owner"

# chat_type values that mean "1:1 with the bot". Mirrors
# gateway.slash_access._DM_CHAT_TYPES; kept local so a future widening there
# (e.g. adding a multi-user type) cannot silently widen who may approve.
_DM_CHAT_TYPES = frozenset({"dm", "direct", "private"})


def _first_concrete_id(raw: Any) -> Optional[str]:
    """Return the first non-wildcard id in an allowlist value, else None.

    Accepts the shapes an allowlist can arrive in: list/tuple/set, a
    comma-separated string, or a bare scalar (an int user id). A ``*``
    anywhere in the list voids the whole resolution — an allowlist that
    admits everyone cannot identify one owner, and picking a neighbouring
    entry would hand approval to an arbitrary member.
    """
    if raw is None:
        return None
    if isinstance(raw, (list, tuple, set, frozenset)):
        items = [str(i).strip() for i in raw]
    elif isinstance(raw, str):
        items = [s.strip() for s in raw.split(",")]
    else:
        items = [str(raw).strip()]
    items = [i for i in items if i]
    if not items:
        return None
    if any(i == "*" for i in items):
        return None
    return items[0]


def _platform_extra(gateway_config: Any, platform: Any) -> dict:
    """Return ``platforms[platform].extra`` from a GatewayConfig-like object."""
    if gateway_config is None or platform is None:
        return {}
    platforms = getattr(gateway_config, "platforms", None)
    if platforms is None:
        return {}
    try:
        platform_config = platforms.get(platform)
    except Exception:
        return {}
    extra = getattr(platform_config, "extra", None)
    if isinstance(extra, dict):
        return extra
    if isinstance(platform_config, dict):
        return platform_config
    return {}


def _allowed_users_env_name(platform: Any) -> str:
    """Env var holding this platform's DM allowlist.

    Prefers the platform registry (plugins declare their own
    ``allowed_users_env``), falling back to the ``<PLATFORM>_ALLOWED_USERS``
    convention every built-in follows.
    """
    value = str(getattr(platform, "value", platform) or "").strip()
    if not value:
        return ""
    try:
        from gateway.platform_registry import platform_registry

        entry = platform_registry.get(value)
        if entry is not None and getattr(entry, "allowed_users_env", ""):
            return str(entry.allowed_users_env)
    except Exception:
        # Registry unavailable (import cycle, deferred-load failure). The
        # convention below is what every built-in platform uses, so fall
        # through rather than losing owner resolution entirely.
        logger.debug("platform registry lookup failed for %s", value, exc_info=True)
    return f"{value.upper()}_ALLOWED_USERS"


def _gate_env(name: str) -> str:
    """Read a platform allowlist env var with per-profile isolation.

    Uses the same reader the adapters' auth path uses, so a multiplexed
    gateway cannot resolve profile A's owner while serving profile B
    (the first-writer-wins YAML→env bridge, issue #72348).
    """
    if not name:
        return ""
    try:
        from gateway.authz_mixin import _platform_gate_env

        return _platform_gate_env(name, "")
    except Exception:
        logger.debug("_platform_gate_env unavailable for %s", name, exc_info=True)
        return (os.getenv(name) or "").strip()


def _explicit_owner_from_config() -> Optional[str]:
    section, key = OWNER_CONFIG_KEY.split(".", 1)
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        raw = (cfg.get(section) or {}).get(key)
    except Exception:
        # A broken/unreadable config must not silently promote the allowlist
        # fallback into "explicit"; log and let resolution continue.
        logger.warning("Could not read %s from config", OWNER_CONFIG_KEY, exc_info=True)
        return None
    if raw is None:
        return None
    owner = str(raw).strip()
    if not owner or owner == "*":
        return None
    return owner


def resolve_owner_user_id(gateway_config: Any, platform: Any) -> Optional[str]:
    """Return the single user id allowed to approve staged writes, or None.

    See the module docstring for the resolution order. None means "no owner is
    identifiable" — callers must refuse, never widen.
    """
    explicit = _explicit_owner_from_config()
    if explicit:
        return explicit

    extra = _platform_extra(gateway_config, platform)
    # DM allowlist only. group_allow_from is deliberately not consulted: on
    # kivi it is ``*``.
    owner = _first_concrete_id(extra.get("allow_from"))
    if owner:
        return owner

    return _first_concrete_id(_gate_env(_allowed_users_env_name(platform)))


def _scope_is_dm(source: Any) -> bool:
    chat_type = str(getattr(source, "chat_type", "") or "").strip().lower()
    # An empty chat_type is NOT treated as a DM here. gateway.slash_access
    # maps "" to the dm scope for command availability, but approval is a
    # gate: an adapter that failed to classify the chat must not be read as
    # "1:1 with the owner".
    return chat_type in _DM_CHAT_TYPES


def check_pending_review_access(gateway_config: Any, source: Any) -> Optional[str]:
    """Return None when *source* may review staged writes, else refusal text.

    The refusal text is returned (not raised) because every caller is a slash
    command handler whose contract is "return a string to show the user".
    It never reveals the owner's id to a non-owner.
    """
    owner = resolve_owner_user_id(gateway_config, getattr(source, "platform", None))
    if not owner:
        return (
            "Staged-write review is owner-only, and this profile has no owner "
            "that can be identified. Set `" + OWNER_CONFIG_KEY + "` in "
            "config.yaml to the reviewing user's id, then run this again. "
            "(Until then the queue is reviewable from the Hermes CLI on the "
            "host with `hermes` → `/skills pending`.)"
        )
    if not _scope_is_dm(source):
        return (
            "Staged-write review only works in a direct message with the bot, "
            "not in a group or channel."
        )
    user_id = getattr(source, "user_id", None)
    if not user_id or str(user_id) != owner:
        return (
            "Not permitted: only the profile owner can review, approve or "
            "reject staged memory/skill writes."
        )
    return None


def owner_notice_chat_id(
    gateway_config: Any, source: Any, adapter: Any
) -> Optional[str]:
    """Chat id an owner-only notice may be sent to, or None to stay silent.

    * Already in the owner's DM → that chat (deliver in place).
    * Anywhere else → the owner's DM, if the adapter can address a user
      directly (``dm_chat_id_for_user``; the base adapter returns None).
    * Otherwise None. The caller must NOT fall back to ``source.chat_id`` —
      on kivi that is a client chat.
    """
    owner = resolve_owner_user_id(gateway_config, getattr(source, "platform", None))
    if not owner:
        return None
    if _scope_is_dm(source) and str(getattr(source, "user_id", "") or "") == owner:
        return str(getattr(source, "chat_id", "") or "") or None
    resolver = getattr(adapter, "dm_chat_id_for_user", None)
    if not callable(resolver):
        return None
    try:
        target = resolver(owner)
    except Exception:
        logger.warning(
            "dm_chat_id_for_user failed while routing an owner-only notice; "
            "suppressing the notice rather than posting it to chat %s",
            getattr(source, "chat_id", "?"),
            exc_info=True,
        )
        return None
    target = str(target or "").strip()
    return target or None


def make_owner_notice_sender(
    gateway_config: Any, source: Any, adapter: Any, schedule: Any
) -> Optional[Callable[[str], None]]:
    """Build the owner-only delivery callback, or None if there is nowhere safe.

    Returning None is a real answer, not a failure: the caller
    (``agent.background_review.deliver_review_summary``) then keeps the notice
    out of chat entirely and logs it, instead of posting the owner's private
    review queue into whatever chat the turn happened in.

    *schedule* takes the coroutine returned by ``adapter.send`` and hands it to
    the gateway event loop; it is injected so this stays a pure function of its
    inputs and the routing can be tested without a live gateway.

    No thread/topic metadata is attached on purpose: the current chat's thread
    id does not exist in the owner's DM.
    """
    if adapter is None:
        return None
    target = owner_notice_chat_id(gateway_config, source, adapter)
    if not target:
        return None

    def _send(message: str, _chat_id: str = target) -> None:
        schedule(adapter.send(_chat_id, message))

    return _send


__all__ = [
    "OWNER_CONFIG_KEY",
    "check_pending_review_access",
    "make_owner_notice_sender",
    "owner_notice_chat_id",
    "resolve_owner_user_id",
]
