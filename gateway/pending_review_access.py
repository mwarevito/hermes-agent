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
     Only a resolved approver of this profile, only from a direct message.
  2. ``owner_notice_chat_ids`` — where may a notice ABOUT a staged write be
     delivered? Only the approvers' own DMs, all of them. Approvers whose DM
     cannot be addressed are skipped, and when that leaves nothing the caller
     stays silent (and logs) rather than posting into whatever chat the turn
     happened in.

Approver resolution is explicit-first and never widens to "everybody":

  1. ``gateway.pending_approval_owners`` in config.yaml — an explicit LIST of
     ids (2026-08-12; see below). Whenever this key is present it is the whole
     answer, including when it is unusable.
  2. otherwise ``gateway.pending_approval_owner`` — the original single-id key,
     still authoritative when present. It goes through the same parser, so a
     value someone spelled as ``"id1,id2"`` now names two approvers instead of
     one unmatchable pseudo-id; no live profile sets this key at all
     (checked on the M1, 2026-08-12).
  3. otherwise the FIRST concrete id of the platform's **DM** allowlist
     (``platforms.<p>.extra.allow_from``, else ``<PLATFORM>_ALLOWED_USERS``).
     This is not a guess about ordering: ``hermes setup`` seeds that variable
     with the detected owner's own id (``auto_owner_user_id`` in
     ``hermes_cli/gateway.py``), and later admins are appended.
  4. otherwise **nobody**. A wildcard (``*``) resolves to nobody by
     construction — it names everyone, so it names no owner. The group
     allowlist is never consulted at all.

When resolution yields nobody the review surface refuses and names the config
key to set. That is deliberate: a queue nobody can spend is a visible problem,
and an approval anybody can grant is an invisible one.

Several approvers per profile (2026-08-12, Vito's decision)
-----------------------------------------------------------
"Sandro for Gogi and for Givi; Fariza and Yakov may also update Givi." Those
four are already the technical tier in ``profiles/workbot/SOUL.md``, so this
widens *who may spend* an approval, not *what an approval does*: still only a
named id, still only in a DM.

The whole risk of a list-shaped permission lives in its degenerate forms, so
they are all pinned to DENY, never to "everyone" and never to a quiet fallback:

  * key absent          -> byte-for-byte the previous single-approver behaviour;
  * ``[]`` / ``""`` / a mapping / a nested value / ``true`` -> NOBODY;
  * any ``*`` in it     -> NOBODY (it names everyone, so it names no one);
  * unusable list while the old single-id key is also set -> still NOBODY, so a
    typo stays visible instead of silently reverting to one approver.

On ``kivi`` "everyone" would literally mean "every Llucky prospect in the chat"
(``TELEGRAM_GROUP_ALLOWED_USERS=*``), which is why the failure direction is not
a matter of taste here.

Because there is now more than one approver, two things that used to be
implicit are made explicit: the new-proposal notice is delivered to EVERY
approver's DM (``owner_notice_chat_ids``), and every approve/reject/gate-flip
is written to the append-only decision log with the actor's id
(``tools.write_approval.record_decision``).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Optional, Tuple

logger = logging.getLogger(__name__)

# config.yaml dotpath for the explicit owner id (single). Kept as-is: it is
# live-documented and several tests and error strings name it.
OWNER_CONFIG_KEY = "gateway.pending_approval_owner"

# config.yaml dotpath for the explicit approver LIST (2026-08-12). Checked
# BEFORE OWNER_CONFIG_KEY, and authoritative even when unusable — see the
# module docstring for why a broken list must not fall back to anything.
OWNERS_CONFIG_KEY = "gateway.pending_approval_owners"

# Sentinel distinguishing "key absent" from "key present and empty/false".
# The whole default-safety of this module rests on that distinction, so it
# cannot be expressed with a falsy default.
_ABSENT = object()

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


def _concrete_id_list(raw: Any) -> Tuple[str, ...]:
    """Parse a configured approver value into concrete ids; () means nobody.

    Deliberately strict about *shapes* and forgiving about *spelling*
    (2026-08-12). Forgiving: unquoted YAML ints (``- 405154434`` parses as an
    int) and a comma-separated string are both accepted, because those are the
    two ways a human actually mistypes this key and both still name concrete
    ids. Strict: a mapping, a nested collection, a bool or a null element is
    not an id in any spelling, and a ``*`` anywhere voids the whole value — an
    approver list that admits everyone is the exact hole the owner gate closed.

    Returning () for all of those, rather than skipping the bad element, is the
    point: partial acceptance would let ``[nonsense, 405154434]`` look like it
    worked while quietly dropping whatever the author meant by the first entry.
    """
    if raw is None or isinstance(raw, bool) or isinstance(raw, dict):
        return ()
    if isinstance(raw, (list, tuple, set, frozenset)):
        items = []
        for item in raw:
            if item is None or isinstance(item, bool) or isinstance(
                item, (dict, list, tuple, set, frozenset)
            ):
                return ()
            items.append(str(item).strip())
    elif isinstance(raw, str):
        items = [part.strip() for part in raw.split(",")]
    else:
        items = [str(raw).strip()]
    items = [i for i in items if i]
    if not items or any(i == "*" for i in items):
        return ()
    ordered: list = []
    for item in items:
        if item not in ordered:
            ordered.append(item)
    return tuple(ordered)


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


def _config_value(dotpath: str) -> Any:
    """Read one config.yaml dotpath, or ``_ABSENT`` when it is not set.

    ``load_config`` is memoised on the file's (mtime_ns, size), so an edit to
    a live profile's config.yaml is picked up by the running gateway on the
    next call — no restart (verified in-process on the M1, 2026-08-12).
    """
    section, key = dotpath.split(".", 1)
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        return (cfg.get(section) or {}).get(key, _ABSENT)
    except Exception:
        # A broken/unreadable config must not silently promote the allowlist
        # fallback into "explicit"; log and let resolution continue. Reported
        # as absent rather than as an empty list on purpose: "we could not
        # read the file" is not "the author configured nobody".
        logger.warning("Could not read %s from config", dotpath, exc_info=True)
        return _ABSENT


def _explicit_approvers_from_config() -> Optional[Tuple[str, ...]]:
    """Approvers named in config.yaml, or None when neither key is present.

    A present-but-unusable key returns ``()`` — "the author named nobody
    usable", which callers must treat as deny-all. Only None means "fall
    through to the allowlist".
    """
    raw = _config_value(OWNERS_CONFIG_KEY)
    if raw is not _ABSENT:
        return _concrete_id_list(raw)
    raw = _config_value(OWNER_CONFIG_KEY)
    if raw is not _ABSENT:
        return _concrete_id_list(raw)
    return None


def resolve_owner_user_ids(gateway_config: Any, platform: Any) -> Tuple[str, ...]:
    """Return every user id allowed to approve staged writes, in notice order.

    See the module docstring for the resolution order. An empty tuple means
    "no approver is identifiable" — callers must refuse, never widen.
    """
    explicit = _explicit_approvers_from_config()
    if explicit is not None:
        return explicit

    extra = _platform_extra(gateway_config, platform)
    # DM allowlist only. group_allow_from is deliberately not consulted: on
    # kivi it is ``*``. Still the FIRST id only — the allowlist is a list of
    # people who may TALK to the bot, which is a different question from who
    # may approve its self-edits, and widening it here would have handed
    # approval to every admin without anyone deciding that.
    owner = _first_concrete_id(extra.get("allow_from"))
    if owner:
        return (owner,)

    owner = _first_concrete_id(_gate_env(_allowed_users_env_name(platform)))
    return (owner,) if owner else ()


def resolve_owner_user_id(gateway_config: Any, platform: Any) -> Optional[str]:
    """First approver (the notice-order head), or None when there is none.

    ⚑ NOT an access check. With several approvers configured, comparing a
    sender against this value would refuse everyone but the first. Use
    ``resolve_owner_user_ids`` (membership) or ``check_pending_review_access``.
    Kept because the single-approver shape is still the default and callers
    that legitimately want "the primary" should not re-derive it.
    """
    approvers = resolve_owner_user_ids(gateway_config, platform)
    return approvers[0] if approvers else None


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
    approvers = resolve_owner_user_ids(
        gateway_config, getattr(source, "platform", None)
    )
    if not approvers:
        # Same text whether the keys are absent or present-but-unusable: the
        # asker may be a stranger, and the fix is identical either way.
        return (
            "Staged-write review is owner-only, and this profile has no owner "
            "that can be identified. Set `" + OWNERS_CONFIG_KEY + "` in "
            "config.yaml to the list of reviewing user ids, then run this "
            "again. (Until then the queue is reviewable from the Hermes CLI "
            "on the host with `hermes` → `/skills pending`.)"
        )
    if not _scope_is_dm(source):
        return (
            "Staged-write review only works in a direct message with the bot, "
            "not in a group or channel."
        )
    user_id = getattr(source, "user_id", None)
    if not user_id or str(user_id) not in approvers:
        return (
            "Not permitted: only the profile owner can review, approve or "
            "reject staged memory/skill writes."
        )
    return None


def owner_notice_chat_ids(
    gateway_config: Any, source: Any, adapter: Any
) -> Tuple[str, ...]:
    """Every chat an owner-only notice may be sent to; () means stay silent.

    One entry per configured approver, in configuration order:

    * the approver who sent the current turn, when that turn is their own DM →
      that chat (deliver in place);
    * every other approver → their DM, if the adapter can address a user
      directly (``dm_chat_id_for_user``; the base adapter returns None);
    * an approver whose DM cannot be resolved is skipped, not substituted. One
      unreachable reviewer must not silence the reachable ones — and must not
      make the notice fall back to ``source.chat_id``, which on kivi is a chat
      with a Llucky client.

    Fanning out to all of them is the point of the 2026-08-12 change: Sandro
    only learns that Gogi has something waiting if the notice is addressed to
    him, and Gogi's queue had sat unreviewed since June precisely because it
    was addressed to nobody who reads it.
    """
    approvers = resolve_owner_user_ids(
        gateway_config, getattr(source, "platform", None)
    )
    if not approvers:
        return ()
    sender = str(getattr(source, "user_id", "") or "")
    # Only ever consulted for the approver who IS the sender, and only in a
    # DM — so a group chat id can never enter this set.
    in_place = (
        str(getattr(source, "chat_id", "") or "")
        if _scope_is_dm(source) and sender in approvers
        else ""
    )
    resolver = getattr(adapter, "dm_chat_id_for_user", None)
    targets: list = []
    for approver in approvers:
        if in_place and approver == sender:
            target = in_place
        elif callable(resolver):
            try:
                target = str(resolver(approver) or "").strip()
            except Exception:
                logger.warning(
                    "dm_chat_id_for_user failed for approver %s while routing "
                    "an owner-only notice; skipping that approver rather than "
                    "posting to chat %s",
                    approver,
                    getattr(source, "chat_id", "?"),
                    exc_info=True,
                )
                target = ""
        else:
            target = ""
        if target and target not in targets:
            targets.append(target)
    return tuple(targets)


def owner_notice_chat_id(
    gateway_config: Any, source: Any, adapter: Any
) -> Optional[str]:
    """First notice target, or None. See ``owner_notice_chat_ids``.

    ⚑ Delivering to only this one drops the other approvers. Kept for callers
    that genuinely want a single destination.
    """
    targets = owner_notice_chat_ids(gateway_config, source, adapter)
    return targets[0] if targets else None


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

    The callback fans one message out to EVERY approver of the profile
    (2026-08-12). Each send is scheduled independently: an approver who never
    opened a DM with the bot fails at send time, and that failure must not
    swallow the others' copies — it is logged and the loop continues.
    """
    if adapter is None:
        return None
    targets = owner_notice_chat_ids(gateway_config, source, adapter)
    if not targets:
        return None

    def _send(message: str, _chat_ids: Tuple[str, ...] = targets) -> None:
        scheduled = 0
        for chat_id in _chat_ids:
            try:
                schedule(adapter.send(chat_id, message))
                scheduled += 1
            except Exception:
                logger.warning(
                    "Could not schedule the owner-only staged-proposal notice "
                    "for approver chat %s; the proposal is still in the "
                    "pending store: %s",
                    chat_id, message, exc_info=True,
                )
        if not scheduled:
            logger.error(
                "Owner-only staged-proposal notice reached NO approver "
                "(%d target(s) all failed to schedule): %s",
                len(_chat_ids), message,
            )

    return _send


__all__ = [
    "OWNERS_CONFIG_KEY",
    "OWNER_CONFIG_KEY",
    "check_pending_review_access",
    "make_owner_notice_sender",
    "owner_notice_chat_id",
    "owner_notice_chat_ids",
    "resolve_owner_user_id",
    "resolve_owner_user_ids",
]
