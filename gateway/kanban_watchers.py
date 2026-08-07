"""Kanban board watcher methods for GatewayRunner.

Extracted verbatim from ``gateway/run.py`` (god-file decomposition Phase 3).
These are the background-loop methods that subscribe to kanban boards, deliver
notifications/artifacts, and drive the multi-agent dispatcher. They use only
``self`` state, so they live on a mixin that ``GatewayRunner`` inherits — the
``self._kanban_*`` call sites resolve identically via the MRO, making this a
behavior-neutral move that lifts ~1,000 LOC out of run.py.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Optional

from agent.i18n import t

# Match the logger run.py uses (logging.getLogger(__name__) where __name__ ==
# "gateway.run") so extracted log records keep their original logger name.
logger = logging.getLogger("gateway.run")

# Terminal event kinds the notifier delivers. Module-level so tests can extend
# it (e.g. inject a future kind with no message template) to prove the watcher
# never wedges the cursor on an unrenderable-but-leased event.
#
# "status" covers dashboard drag-drop and `_set_status_direct()` writes.
# "archived"/"unblocked" are leased but intentionally SILENT (see SILENT_KINDS):
# they must still be claimed so they can't wedge a later completed/blocked event
# behind an unresolved row. "block_loop_detected" is the triage hand-off that
# exists to force human attention, so it gets its own loud message.
# Platforms that no messaging gateway adapter will ever serve. A subscription on
# one of these is not "waiting for the adapter to connect" — it is dead mail.
_UNDELIVERABLE_PLATFORMS = frozenset({"tui", "cli", ""})

# The notifier ticks every few seconds and a dead subscription stays dead, so an
# unconditional warning per tick floods the log (measured live: 3 rows -> ~2000
# lines/hour). Warn once an hour per subscription: persistent breakage keeps
# resurfacing, but the log stays readable. Keyed per subscription so a second
# broken card is never silenced by the first.
_UNDELIVERABLE_WARN_INTERVAL = 3600.0
_undeliverable_warned: dict = {}


def _should_warn_undeliverable(task_id, platform, chat_id) -> bool:
    key = (str(task_id), str(platform), str(chat_id))
    now = time.time()
    last = _undeliverable_warned.get(key, 0.0)
    if now - last < _UNDELIVERABLE_WARN_INTERVAL:
        return False
    _undeliverable_warned[key] = now
    return True

TERMINAL_KINDS = (
    "completed", "blocked", "gave_up", "crashed", "timed_out",
    "status", "archived", "unblocked", "block_loop_detected",
)

# Terminal kinds that are leased for cursor hygiene but never messaged to the
# user: an archive needs no ping and `unblocked` is an internal transition.
# They are confirmed in the ledger without a send (and are excluded from the
# wake kinds), so they advance the cursor without notifying anyone.
SILENT_KINDS = ("archived", "unblocked")


def _resolve_auto_decompose_settings(
    load_config: Callable[[], Any],
) -> "tuple[bool, int]":
    """Resolve the live (enabled, per_tick) auto-decompose settings.

    Read fresh from config on every dispatcher tick (#49638) so that flipping
    ``kanban.auto_decompose: false`` to STOP runaway fan-out takes effect on the
    next tick instead of requiring a gateway restart. Auto-decompose is a
    safety toggle — a user who sees it create and launch tasks they didn't
    intend reaches for this flag to halt it, and a stale boot-captured value
    silently ignoring that change is the bug reported in #49638.

    Fails **safe**: if the config read raises, return ``(False, 3)`` — a
    transient read error must never re-enable a feature the user turned off,
    nor fall back to the burst-prone default-on behaviour. ``per_tick`` is
    clamped to ``>= 1``.
    """
    try:
        cfg = load_config()
    except Exception:
        return False, 3
    kcfg = cfg.get("kanban", {}) if isinstance(cfg, dict) else {}
    enabled = bool(kcfg.get("auto_decompose", True))
    try:
        per_tick = int(kcfg.get("auto_decompose_per_tick", 3) or 3)
    except (TypeError, ValueError):
        per_tick = 3
    if per_tick < 1:
        per_tick = 1
    return enabled, per_tick

# How often a running task's card is refreshed. The worker's auto-heartbeat is
# rate-limited to 60s, so refreshing much faster buys no new information.
CARD_REFRESH_SECONDS = 45


def _fmt_duration(seconds) -> str:
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return "?"
    if seconds < 60:
        return f"{seconds} сек"
    if seconds < 3600:
        return f"{seconds // 60} мин"
    return f"{seconds // 3600} ч {(seconds % 3600) // 60} мин"


def _render_progress_card(task, *, task_id: str, now: int) -> str:
    """Card text while the task is still running.

    Terminal states deliberately keep their existing upstream wording — this
    renderer is only for the in-flight refresh, which had no message at all
    before (the user saw silence until the task ended).
    """
    title = (getattr(task, "title", None) or task_id)[:120]
    who = getattr(task, "assignee", None)
    tag = f"@{who} " if who else ""
    lines = [f"⚙️ {tag}Kanban {task_id} — работает", title]
    started = getattr(task, "started_at", None) or getattr(task, "created_at", None)
    if started:
        lines.append(f"идёт {_fmt_duration(now - int(started))}")
    hb = getattr(task, "last_heartbeat_at", None)
    if hb:
        lines.append(f"жив, отметился {_fmt_duration(now - int(hb))} назад")
    return "\n".join(lines)



def _acquire_singleton_lock(lock_path) -> "tuple[Optional[object], str]":
    """Take an exclusive, non-blocking advisory lock for the sole dispatcher.

    Only one gateway process machine-wide may run the embedded kanban
    dispatcher: concurrent dispatchers double the reclaim frequency (each
    runs its own ``release_stale_claims`` → promote → dispatch loop), double
    claim-attempt events in the event log, and — with ``wal_autocheckpoint=0`` —
    concurrent manual WAL checkpoints can corrupt index pages. The
    ``dispatch_in_gateway`` config flag is the primary control; this lock is the
    backstop that survives config drift and same-profile restart races.

    Delegates to :func:`gateway.status._try_acquire_file_lock` (``fcntl`` on
    POSIX, ``msvcrt`` on Windows) so the guard is cross-platform.

    Returns ``(handle, "held")`` on success — the caller keeps the file handle
    for the process lifetime and **must** release it via
    :func:`_release_singleton_lock` when done. ``(None, "contended")`` when
    another process holds the lock (caller must NOT dispatch). ``(None,
    "unavailable")`` when locking cannot be performed (non-POSIX filesystem
    without flock, or the status.py helpers are unimportable) — caller falls
    back to config-only control.
    """
    try:
        from gateway.status import _try_acquire_file_lock  # deferred; same package
    except ImportError:
        return None, "unavailable"
    try:
        Path(lock_path).parent.mkdir(parents=True, exist_ok=True)
        handle = open(str(lock_path), "a+", encoding="utf-8")
    except OSError:
        return None, "unavailable"
    if not _try_acquire_file_lock(handle):
        handle.close()
        return None, "contended"
    return handle, "held"


def _release_singleton_lock(handle) -> None:
    """Release a dispatcher singleton lock acquired via :func:`_acquire_singleton_lock`."""
    if handle is None:
        return
    try:
        from gateway.status import _release_file_lock
        _release_file_lock(handle)
    except Exception:
        pass
    try:
        handle.close()
    except Exception:
        pass


def _artifact_paths_from_payload(event_payload) -> list:
    """Batch-4 D2: the ONLY egress source for kanban artifact delivery.

    A file reaches the card's Telegram subscribers only when the worker
    deliberately calls ``kanban_complete(artifacts=[...])``. Scanning the
    free-text ``summary`` / ``task.result`` for file paths was removed: it
    auto-uploaded any path a worker merely *mentioned* (input refs,
    checkpoints, examples) -- including to the 4 clinic/patient bots. Returns
    the explicit string paths in order; callers still validate existence and
    apply ``filter_local_delivery_paths`` before sending.
    """
    out = []
    if isinstance(event_payload, dict):
        raw = event_payload.get("artifacts")
        if isinstance(raw, (list, tuple)):
            for item in raw:
                if isinstance(item, str) and item:
                    out.append(item)
    return out


class GatewayKanbanWatchersMixin:
    """Kanban watcher / notifier / dispatcher loops for GatewayRunner."""

    def _owns_kanban_dispatcher_lock(self) -> bool:
        """Return whether this gateway currently owns the singleton lock."""
        return getattr(self, "_kanban_dispatcher_lock_handle", None) is not None

    def _release_kanban_dispatcher_lock(self) -> None:
        """Clear notifier-visible ownership before releasing the OS lock."""
        handle = getattr(self, "_kanban_dispatcher_lock_handle", None)
        self._kanban_dispatcher_lock_handle = None
        _release_singleton_lock(handle)

    async def _kanban_notifier_watcher(self, interval: float = 5.0) -> None:
        """Poll ``kanban_notify_subs`` and deliver terminal events to users.

        For each subscription row, fetches ``task_events`` newer than the
        stored cursor with kind in the terminal set (``completed``,
        ``blocked``, ``gave_up``, ``crashed``, ``timed_out``). Sends one
        message per new event to ``(platform, chat_id, thread_id)``,
        then advances the cursor. When a task reaches a terminal state
        (``completed`` / ``archived``), the subscription is removed.

        Runs in the gateway event loop; all SQLite work is pushed to a
        thread via ``asyncio.to_thread`` so the loop never blocks on the
        WAL lock. Failures in one tick don't stop subsequent ticks.

        **Multi-board:** iterates every board discovered on disk per
        tick. Each gateway polls only subscriptions owned by profiles whose
        adapters it hosts. The dispatch-owning gateway also handles legacy
        subscriptions without a profile stamp.
        """
        # Dispatch and delivery have separate ownership. A deployment may run
        # one dispatcher while each profile has its own gateway credentials;
        # those adapter-owning gateways must still poll and deliver their own
        # subscriptions. Legacy rows without a notifier_profile are visible
        # only while this process holds the actual singleton dispatcher lock.
        from gateway.config import Platform as _Platform
        try:
            from hermes_cli import kanban_db as _kb
        except Exception:
            logger.warning("kanban notifier: kanban_db not importable; notifier disabled")
            return

        # Terminal kinds come from the MODULE-level ``TERMINAL_KINDS`` (top of
        # this file) instead of a method-local copy: tests monkeypatch it to
        # inject a future kind with no message template and prove the watcher
        # never wedges the cursor on an unrenderable-but-leased event.
        #
        # Subscriptions are removed only when the task reaches a truly final
        # status (done / archived). We used to also unsub on any terminal
        # event kind (gave_up / crashed / timed_out / blocked), but that
        # silently dropped the user out of the loop whenever the dispatcher
        # respawned the task: a worker that crashes, gets reclaimed, runs
        # again, and crashes a second time would only notify on the first
        # crash because the subscription was deleted after the first event.
        # Same shape as the reblock-after-unblock cycle that PR #22941
        # fixed for `blocked`. Keeping the subscription alive until the
        # task is genuinely done lets the durable delivery ledger
        # (kanban_notify_deliveries) handle dedup, and any retry-loop event
        # reaches the user.
        #
        # Delivery durability: instead of advancing the cursor at claim time
        # and hoping the send succeeds, each terminal event is *leased* into
        # kanban_notify_deliveries, delivered, then durably marked 'sent'
        # (cursor advances) or 'failed'/'dead' (cursor holds, retry later).
        # A crash — or a send that returns without reaching the user — can no
        # longer silently skip an event: the lease expires and it is
        # redelivered. For an OWNED subscription, bounded retries then
        # dead-letter (operator-visible via
        # kanban_db.list_dead_letter_deliveries) so one genuinely-dead chat
        # can't wedge the subscription. An OWNERLESS (legacy notifier_profile
        # IS NULL) sub is NEVER dead-lettered here: a wrong-token gateway must
        # not burn the shared retry budget and give up on an event the
        # correctly-tokened gateway could still deliver — it stays durably
        # 'failed' until that gateway delivers and adopts ownership. This is
        # the 2026-07-27 silent-loss fix; the old in-memory MAX_SEND_FAILURES /
        # _kanban_ownerless_skip parking is gone because durability now lives
        # in the DB.
        #
        # The knobs are read here (not via a module-level constant) so a
        # config.yaml override applies; kanban_db carries the code-level
        # fallback, so an older config without them still works. The notifier
        # itself is NOT gated on kanban.dispatch_in_gateway — delivery
        # ownership is independent of dispatch ownership.
        kanban_cfg: dict = {}
        try:
            from hermes_cli.config import load_config as _load_config
            _cfg = _load_config()
            if isinstance(_cfg, dict):
                kanban_cfg = _cfg.get("kanban", {}) or {}
        except Exception as _cfg_exc:
            logger.debug(
                "kanban notifier: config unavailable for delivery knobs (%s); "
                "using kanban_db defaults", _cfg_exc,
            )
        try:
            retry_limit = int(
                kanban_cfg.get("notify_retry_limit", _kb.DEFAULT_NOTIFY_RETRY_LIMIT)
            )
        except (TypeError, ValueError):
            retry_limit = _kb.DEFAULT_NOTIFY_RETRY_LIMIT
        if retry_limit < 1:
            retry_limit = _kb.DEFAULT_NOTIFY_RETRY_LIMIT
        try:
            lease_seconds = int(
                kanban_cfg.get("notify_lease_seconds", _kb.DEFAULT_NOTIFY_LEASE_SECONDS)
            )
        except (TypeError, ValueError):
            lease_seconds = _kb.DEFAULT_NOTIFY_LEASE_SECONDS
        if lease_seconds < 1:
            lease_seconds = _kb.DEFAULT_NOTIFY_LEASE_SECONDS
        notifier_profile = getattr(self, "_kanban_notifier_profile", None)
        if not notifier_profile:
            notifier_profile = self._active_profile_name()
            self._kanban_notifier_profile = notifier_profile

        # Delivery LEASE owner — distinct from subscription OWNERSHIP.
        # ``notifier_profile`` is the profile-scoped ownership identity (shared
        # across restarts, CAS-adopted, used for the foreign-owner skip). The
        # lease token must instead be unique per *running process*, so two
        # concurrent same-profile gateways never see each other's live pending
        # lease as their own and double-deliver. profile:pid:nonce is enough;
        # it is fenced into confirm/failure/release so a stale owner can't
        # mutate a row another process has re-leased.
        lease_owner = getattr(self, "_kanban_lease_owner", None)
        if not lease_owner:
            lease_owner = f"{notifier_profile}:{os.getpid()}:{secrets.token_hex(4)}"
            self._kanban_lease_owner = lease_owner

        # Initial delay so the gateway can finish wiring adapters.
        await asyncio.sleep(5)

        while self._running:
            try:
                def _collect():
                    deliveries: list[dict] = []
                    include_unowned = self._owns_kanban_dispatcher_lock()
                    notifier_profiles = {notifier_profile}
                    notifier_profiles.update(
                        str(profile).strip()
                        for profile in getattr(self, "_profile_adapters", {})
                        if str(profile).strip()
                    )
                    active_platforms = {
                        getattr(platform, "value", str(platform)).lower()
                        for platform in self.adapters.keys()
                    }
                    # Widen to every platform any secondary profile has live,
                    # not just the default profile's. This is only a coarse
                    # pre-filter to skip claiming events for subs nobody can
                    # possibly deliver — the precise per-profile check (via
                    # gateway/authz_mixin.py::_authorization_adapter, which
                    # forbids default-profile fallback) still runs at delivery
                    # time below, rewinding the claim if it resolves to None.
                    # Without this, a subscription owned by a secondary
                    # profile on a platform the DEFAULT profile never
                    # connected (e.g. beta owns discord, default doesn't) was
                    # dropped here before ever being claimed — no rewind
                    # applies to an unclaimed event, so it silently never
                    # retries.
                    for _profile_adapter_map in getattr(self, "_profile_adapters", {}).values():
                        active_platforms.update(
                            getattr(platform, "value", str(platform)).lower()
                            for platform in _profile_adapter_map.keys()
                        )
                    if not active_platforms:
                        logger.debug("kanban notifier: no connected adapters; skipping tick")
                        return deliveries

                    # Enumerate every board on disk, but poll each resolved DB
                    # path once. Multiple slugs can point at the same DB when
                    # HERMES_KANBAN_DB pins the board path; without this guard
                    # one gateway could collect the same subscription/event
                    # more than once before advancing the cursor.
                    try:
                        boards = _kb.list_boards(include_archived=False)
                    except Exception:
                        boards = [_kb.read_board_metadata(_kb.DEFAULT_BOARD)]
                    seen_db_paths: set[str] = set()
                    for board_meta in boards:
                        slug = board_meta.get("slug") or _kb.DEFAULT_BOARD
                        db_path = board_meta.get("db_path")
                        try:
                            resolved_db_path = str(Path(db_path).expanduser().resolve()) if db_path else str(_kb.kanban_db_path(slug).resolve())
                        except Exception:
                            resolved_db_path = f"slug:{slug}"
                        if resolved_db_path in seen_db_paths:
                            logger.debug(
                                "kanban notifier: skipping duplicate board slug %s for DB %s",
                                slug, resolved_db_path,
                            )
                            continue
                        seen_db_paths.add(resolved_db_path)
                        # Zero-subscription early exit: probe the board with a
                        # cheap read-only connection BEFORE the writable
                        # `connect()`. A board with no subscriptions has
                        # nothing to notify, and the writable open (schema
                        # init/migration on first open, WAL/-shm sidecars,
                        # checkpoint traffic) is exactly the per-tick cost
                        # this skip avoids.
                        try:
                            if _kb.count_notify_subs(
                                board=slug,
                                notifier_profiles=notifier_profiles,
                                include_unowned=include_unowned,
                            ) == 0:
                                logger.debug(
                                    "kanban notifier: board %s has no subscriptions owned by %s; skipping open",
                                    slug, sorted(notifier_profiles),
                                )
                                continue
                        except Exception as exc:
                            logger.debug(
                                "kanban notifier: read-only subscription probe failed "
                                "for board %s (%s); falling back to writable open",
                                slug, exc,
                            )
                        try:
                            conn = _kb.connect(board=slug)
                        except Exception as exc:
                            logger.debug("kanban notifier: cannot open board %s: %s", slug, exc)
                            continue
                        try:
                            # `connect()` runs the schema + idempotent migration
                            # on first open per process, so an explicit
                            # `init_db()` here would be redundant. Worse:
                            # `init_db()` deliberately busts the per-process
                            # cache and re-runs the migration on a *second*
                            # connection, which races the first and used to
                            # log a benign but noisy `duplicate column name`
                            # traceback (and intermittent "database is locked"
                            # — issue #21378) on every gateway start against
                            # a legacy DB. `_add_column_if_missing` now
                            # tolerates that race, but we still skip the
                            # redundant call to avoid the wasted work.
                            subs = _kb.list_notify_subs(
                                conn,
                                notifier_profiles=notifier_profiles,
                                include_unowned=include_unowned,
                            )
                            if not subs:
                                logger.debug("kanban notifier: board %s has no subscriptions", slug)
                            for sub in subs:
                                try:
                                    owner_profile = sub.get("notifier_profile") or None
                                    if owner_profile and owner_profile != notifier_profile:
                                        _owner_adapters = getattr(self, "_profile_adapters", {}).get(owner_profile)
                                        if not _owner_adapters:
                                            logger.debug(
                                                "kanban notifier: subscription for %s owned by profile %s; current profile %s has no adapter for it, skipping",
                                                sub.get("task_id"), owner_profile, notifier_profile,
                                            )
                                            continue
                                    platform = (sub.get("platform") or "").lower()
                                    if platform not in active_platforms:
                                        # A disconnected adapter is transient and
                                        # normal (debug). A platform this gateway
                                        # can never serve is a card whose result
                                        # has nowhere to go, forever — say so at
                                        # WARNING. 2026-08-06: three cards sat on
                                        # platform='tui' whose chat_id encoded a
                                        # Telegram address; nothing delivered them
                                        # and nothing complained.
                                        if platform in _UNDELIVERABLE_PLATFORMS and \
                                                _should_warn_undeliverable(
                                                    sub.get("task_id"), platform,
                                                    sub.get("chat_id")):
                                            logger.warning(
                                                "kanban notifier: task %s is subscribed on platform %r "
                                                "(chat_id=%r) which no gateway adapter can deliver — this "
                                                "result will never reach anyone. Re-subscribe the task.",
                                                sub.get("task_id"), platform or "<missing>",
                                                sub.get("chat_id"),
                                            )
                                        else:
                                            logger.debug(
                                                "kanban notifier: subscription for %s on %s skipped; adapter not connected",
                                                sub.get("task_id"), platform or "<missing>",
                                            )
                                        continue
                                    # Durably lease this sub's unseen terminal
                                    # events. The cursor is NOT advanced here —
                                    # only a confirmed send advances it. The
                                    # lease is single-owner (per process) across
                                    # concurrent gateways (even for a still-
                                    # ownerless legacy sub), so a non-delivering
                                    # gateway can't claim-and-skip.
                                    events = _kb.lease_unseen_deliveries(
                                        conn,
                                        task_id=sub["task_id"],
                                        platform=sub["platform"],
                                        chat_id=sub["chat_id"],
                                        thread_id=sub.get("thread_id") or "",
                                        kinds=TERMINAL_KINDS,
                                        claimer=lease_owner,
                                        lease_seconds=lease_seconds,
                                        retry_limit=retry_limit,
                                    )
                                    task = _kb.get_task(conn, sub["task_id"])
                                    if not events:
                                        # No terminal event — but if the task is
                                        # still running, keep its card fresh.
                                        # This path touches no ledger and never
                                        # moves the cursor: a dropped refresh is
                                        # cosmetic, unlike a dropped terminal
                                        # ping.
                                        # NB: no card_message_id requirement.
                                        # Requiring one meant only a task that
                                        # had ALREADY been messaged could get a
                                        # progress card — and the only thing
                                        # that messaged first was the terminal
                                        # event. Nothing was ever sent while
                                        # work was in flight (2026-08-06).
                                        # card_updated_at is 0 when no card
                                        # exists, so the first tick in `running`
                                        # creates it and later ticks edit it.
                                        if (
                                            task is not None
                                            and getattr(task, "status", "") == "running"
                                        ):
                                            last = sub.get("card_updated_at") or 0
                                            if (time.time() - float(last)) >= CARD_REFRESH_SECONDS:
                                                deliveries.append({
                                                    "sub": sub,
                                                    "events": [],
                                                    "task": task,
                                                    "board": slug,
                                                    "owner_profile": owner_profile,
                                                    "progress_only": True,
                                                })
                                        continue
                                    logger.debug(
                                        "kanban notifier: leased %d event(s) for %s on board %s",
                                        len(events), sub["task_id"], slug,
                                    )
                                    deliveries.append({
                                        "sub": sub,
                                        "events": events,
                                        "task": task,
                                        "board": slug,
                                        # Raw ownership at collect time: truthy iff
                                        # confirmed-owned by THIS gateway (foreign
                                        # subs were already skipped above). Falsy =
                                        # ownerless — adopt on first successful send.
                                        "owner_profile": owner_profile,
                                    })
                                except Exception as sub_exc:
                                    # Isolate per-subscription failures so one
                                    # bad subscription cannot block delivery for
                                    # all other subscriptions in this tick.
                                    logger.warning(
                                        "kanban notifier: subscription for %s on board %s failed: %s",
                                        sub.get("task_id"), slug, sub_exc,
                                    )
                        finally:
                            conn.close()
                    return deliveries

                deliveries = await asyncio.to_thread(_collect)
                for d in deliveries:
                    sub = d["sub"]
                    task = d["task"]
                    board_slug = d.get("board")
                    platform_str = (sub["platform"] or "").lower()
                    event_ids = [ev.id for ev in d["events"]]
                    # Confirmed-owned by THIS gateway? Only an owned sub may
                    # dead-letter — an ownerless (legacy NULL) sub whose send
                    # fails here might just be the wrong-token gateway, so it
                    # must stay retryable for whoever can actually deliver.
                    owned = bool(d.get("owner_profile"))
                    try:
                        plat = _Platform(platform_str)
                    except ValueError:
                        # Unknown/undeliverable platform: release the leases so
                        # they revert to unclaimed 'pending' (no attempt burned)
                        # and do NOT advance the cursor. Defensive — such subs
                        # are already filtered by active_platforms in _collect.
                        await asyncio.to_thread(
                            self._kanban_release_leases, sub, event_ids, board_slug, lease_owner,
                        )
                        continue
                    sub_profile = sub.get("notifier_profile") or ""
                    # Route via the SAME chokepoint the authorization path uses
                    # (gateway/authz_mixin.py::_authorization_adapter): a stamped
                    # profile with its own adapter-registry entry must be served
                    # by THAT profile's same-platform adapter and must NOT silently
                    # fall back to the default profile's adapter — otherwise a
                    # secondary profile's task notification is delivered by the
                    # wrong bot (the cross-profile mis-delivery this whole change
                    # exists to fix). The helper returns None only when the profile
                    # (or default) genuinely has no adapter for the platform.
                    adapter = self._authorization_adapter(plat, sub_profile or None)
                    if d.get("progress_only"):
                        # Cosmetic refresh of an existing card: no lease, no
                        # cursor, no unsub. Failures here must never affect
                        # terminal delivery, so they are swallowed and logged.
                        if adapter is not None:
                            try:
                                meta: dict[str, Any] = self._kanban_visible_thread_metadata(
                                    plat, sub, adapter,
                                )
                                await self._kanban_deliver_card(
                                    adapter, sub, board_slug,
                                    _render_progress_card(
                                        task, task_id=sub["task_id"], now=int(time.time()),
                                    ),
                                    meta,
                                )
                            except Exception as exc:
                                logger.debug(
                                    "kanban card refresh failed for %s: %s",
                                    sub["task_id"], exc,
                                )
                        continue
                    if adapter is None:
                        logger.debug(
                            "kanban notifier: adapter %s disconnected before delivery for %s; releasing lease",
                            platform_str, sub["task_id"],
                        )
                        await asyncio.to_thread(
                            self._kanban_release_leases, sub, event_ids, board_slug, lease_owner,
                        )
                        continue
                    title = (task.title if task else sub["task_id"])[:120]
                    board_tag = f"[{board_slug}] " if board_slug else ""
                    from gateway.wake import adapter_supports_push as _adapter_push_ok

                    # Adapters with no push channel (the API server —
                    # ``supports_async_delivery = False``) can NEVER satisfy a
                    # text-send: ``send()`` always reports
                    # SendResult(success=False) by design (see
                    # ApiServerAdapter.send()). Counting that as a delivery
                    # failure would burn the retry budget and eventually
                    # dead-letter an event the wake self-post CAN deliver. So
                    # for non-push adapters the doomed send is skipped and the
                    # event is resolved by the self-post below instead.
                    _is_push_adapter = _adapter_push_ok(adapter)
                    delivered_all = True
                    # Leased events whose text ping was intentionally skipped
                    # for a non-push adapter. Held (NOT confirmed) until the
                    # wake self-post below confirms or fails them — so a failed
                    # self-post can never silently advance the cursor past them.
                    wake_deferred_ids: "list[int]" = []
                    for ev in d["events"]:
                        kind = ev.kind
                        # Identity prefix: attribute terminal pings to the
                        # worker that did the work. Makes fleets (where one
                        # chat subscribes to many tasks) legible at a glance.
                        who = (task.assignee if task and task.assignee else None)
                        tag = f"@{who} " if who else ""
                        if kind == "completed":
                            # Prefer the run's summary (the worker's
                            # intentional human-facing handoff, carried
                            # in the event payload), then fall back to
                            # task.result for legacy rows written before
                            # runs shipped.
                            handoff = ""
                            payload_summary = None
                            if ev.payload and ev.payload.get("summary"):
                                payload_summary = str(ev.payload["summary"])
                            if payload_summary:
                                lines = payload_summary.strip().splitlines()
                                h = lines[0][:200] if lines else payload_summary[:200]
                                handoff = f"\n{h}"
                            elif task and task.result:
                                lines = task.result.strip().splitlines()
                                r = lines[0][:160] if lines else task.result[:160]
                                handoff = f"\n{r}"
                            msg = (
                                f"✔ {board_tag}{tag}Kanban {sub['task_id']} done"
                                f" — {title}{handoff}"
                            )
                        elif kind == "blocked":
                            reason = ""
                            if ev.payload and ev.payload.get("reason"):
                                reason = f": {str(ev.payload['reason'])[:160]}"
                            msg = f"⏸ {board_tag}{tag}Kanban {sub['task_id']} blocked{reason}"
                        elif kind == "gave_up":
                            err = ""
                            if ev.payload and ev.payload.get("error"):
                                err = f"\n{str(ev.payload['error'])[:200]}"
                            msg = (
                                f"✖ {board_tag}{tag}Kanban {sub['task_id']} gave up "
                                f"after repeated spawn failures{err}"
                            )
                        elif kind == "crashed":
                            msg = (
                                f"✖ {board_tag}{tag}Kanban {sub['task_id']} worker crashed "
                                f"(pid gone); dispatcher will retry"
                            )
                        elif kind == "timed_out":
                            limit = 0
                            if ev.payload and ev.payload.get("limit_seconds"):
                                limit = int(ev.payload["limit_seconds"])
                            msg = (
                                f"⏱ {board_tag}{tag}Kanban {sub['task_id']} timed out "
                                f"(max_runtime={limit}s); will retry"
                            )
                        elif kind == "status":
                            new_status = ""
                            if ev.payload and ev.payload.get("status"):
                                new_status = str(ev.payload["status"])
                            msg = f"🔄 {board_tag}{tag}Kanban {sub['task_id']} → {new_status}"
                        elif kind == "block_loop_detected":
                            # A task re-blocked for the same cause past the
                            # recurrence limit and was routed to `triage` for a
                            # human decision. This is the ONE transition that
                            # exists to force human attention, yet it emits no
                            # `blocked`/`status` event — so before adding it to
                            # TERMINAL_KINDS it produced zero notification and
                            # the task stalled in triage silently. Ping loudly.
                            reason = ""
                            recurrences = None
                            if ev.payload:
                                if ev.payload.get("reason"):
                                    reason = f": {str(ev.payload['reason'])[:160]}"
                                recurrences = ev.payload.get("recurrences")
                            rc = f" (blocked {recurrences}x for the same cause)" if recurrences else ""
                            msg = (
                                f"🛑 {board_tag}{tag}Kanban {sub['task_id']} routed to TRIAGE"
                                f" — needs a human decision{rc}{reason}"
                            )
                        else:
                            # Kinds claimed for cursor hygiene but never
                            # messaged: `archived` needs no user ping and
                            # `unblocked` is an internal transition (both are
                            # excluded from _WAKE_KINDS below, so they never
                            # wake the creator either). Anything else landing
                            # here is a terminal kind this build has no message
                            # template for (e.g. a newer kind added to
                            # TERMINAL_KINDS after this gateway was deployed).
                            # Either way the event was LEASED, so leaving it
                            # unresolved would wedge the cursor forever: confirm
                            # it (fenced to our lease) so the cursor can advance
                            # past it, and log loudly for the unrenderable case.
                            # At-least-once is preserved for every kind we
                            # *can* render.
                            if kind not in SILENT_KINDS:
                                logger.warning(
                                    "kanban notifier: no message template for terminal "
                                    "kind %r on %s; confirming to avoid wedging the "
                                    "cursor (upgrade the gateway to render it)",
                                    kind, sub["task_id"],
                                )
                            await asyncio.to_thread(
                                self._kanban_confirm_sent, sub, ev.id, board_slug, lease_owner,
                            )
                            continue
                        # Subscription-level routing (relay/user/scope) is the
                        # base; thread routing is layered on top and only in a
                        # shape that is guaranteed USER-VISIBLE.
                        delivery_metadata = sub.get("delivery_metadata")
                        metadata: dict[str, Any] = (
                            dict(delivery_metadata)
                            if isinstance(delivery_metadata, dict)
                            else {}
                        )
                        visible_thread_meta = self._kanban_visible_thread_metadata(
                            plat, sub, adapter,
                        )
                        if visible_thread_meta:
                            metadata.update(visible_thread_meta)
                        else:
                            # Root-DM downgrade (anchorless private-chat topic,
                            # 2026-08-05 t_0fc6b0dd): strip any thread routing
                            # inherited from delivery_metadata too, or the
                            # invisible/wedged lane comes back through the side
                            # door.
                            for _routing_key in (
                                "thread_id", "message_thread_id",
                                "direct_messages_topic_id",
                                "telegram_direct_messages_topic_id",
                                "telegram_dm_topic_reply_fallback",
                            ):
                                metadata.pop(_routing_key, None)
                        if not _is_push_adapter:
                            logger.debug(
                                "kanban notifier: adapter %s has no push "
                                "channel; skipping text ping for %s, relying "
                                "on wake self-post instead",
                                platform_str, sub["task_id"],
                            )
                            # Held, NOT confirmed: the self-post below is this
                            # event's real delivery and resolves it in the
                            # ledger (confirm on success, failure otherwise).
                            wake_deferred_ids.append(ev.id)
                            continue
                        try:
                            # Delivery goes through THE card for this
                            # subscription (edit-in-place, create on first
                            # use). _kanban_deliver_card raises on a reported
                            # SendResult(success=False) so a silent transient
                            # failure is recorded in the durable ledger as a
                            # failure instead of being confirmed as sent.
                            await self._kanban_deliver_card(
                                adapter, sub, board_slug, msg, metadata,
                            )
                            logger.debug(
                                "kanban notifier: delivered %s event for %s to %s/%s on board %s",
                                kind, sub["task_id"], platform_str, sub["chat_id"], board_slug,
                            )
                            # After delivering the text notification, surface
                            # any artifact paths the worker referenced in
                            # ``kanban_complete(summary=..., artifacts=[...])``
                            # (or the legacy ``result`` field) as native
                            # uploads. ``extract_local_files`` finds bare
                            # absolute paths in the summary;
                            # ``send_document`` / ``send_image_file`` uploads
                            # them. Only fires on the ``completed`` event so
                            # we never spam attachments on retries.
                            if kind == "completed":
                                try:
                                    await self._deliver_kanban_artifacts(
                                        adapter=adapter,
                                        chat_id=sub["chat_id"],
                                        metadata=metadata,
                                        event_payload=getattr(ev, "payload", None),
                                        task=task,
                                    )
                                except Exception as art_exc:
                                    logger.debug(
                                        "kanban notifier: artifact delivery for %s failed: %s",
                                        sub["task_id"], art_exc,
                                    )
                            # Durably mark this event delivered BEFORE the
                            # cursor moves. A crash after this point still sees
                            # the event 'sent' — never re-sent, never lost.
                            # Fenced to our lease: if our lease already expired
                            # and another process re-leased, this is a no-op and
                            # the event is (at-least-once) re-sent by that owner.
                            await asyncio.to_thread(
                                self._kanban_confirm_sent, sub, ev.id, board_slug, lease_owner,
                            )
                        except Exception as exc:
                            # Record a durable failed attempt (never 'sent', so
                            # a send exception can't masquerade as delivered).
                            # Returns 'failed' (retryable), 'dead' (bound reached
                            # — only for an OWNED sub), or 'stale' (our lease was
                            # taken over — no-op). ``allow_dead=owned`` keeps an
                            # ownerless sub retryable so a wrong-token gateway
                            # can't dead-letter an event another gateway could
                            # still deliver.
                            status = await asyncio.to_thread(
                                self._kanban_record_failure,
                                sub, ev.id, str(exc), retry_limit, board_slug,
                                owned, lease_owner,
                            )
                            if status == "dead":
                                logger.warning(
                                    "kanban notifier: DEAD-LETTER — event %s (%s) for "
                                    "%s on %s undeliverable after %d attempts: %s "
                                    "(inspect via kanban_db.list_dead_letter_deliveries)",
                                    ev.id, kind, sub["task_id"], platform_str,
                                    retry_limit, exc,
                                )
                            else:
                                logger.warning(
                                    "kanban notifier: send failed for %s on %s "
                                    "(event %s, will retry): %s",
                                    sub["task_id"], platform_str, ev.id, exc,
                                )
                            # Stop the batch so a retryable event blocks later
                            # events (ordering) and is retried next tick. The
                            # subscription row is NEVER deleted here — durable
                            # dead-lettering, not sub-drop, bounds a dead chat.
                            delivered_all = False
                            break
                    # -------------------------------------------------------
                    # Wake the creator's agent session. Computed once per
                    # subscription batch; used by both adapter classes.
                    # -------------------------------------------------------
                    task_terminal = task and task.status in {"done", "archived"}
                    _WAKE_KINDS = ("completed", "gave_up", "crashed", "timed_out", "blocked")
                    _wake_kinds = {ev.kind for ev in d["events"] if ev.kind in _WAKE_KINDS}
                    _session_key = ""
                    _synth = ""
                    if _wake_kinds:
                        _session_key = getattr(task, "session_id", None) or ""
                    if _wake_kinds and _session_key:
                        _title = (task.title if task else sub["task_id"])[:120]
                        _assignee = task.assignee if task else ""
                        _parts = []
                        if "completed" in _wake_kinds: _parts.append(t("gateway.kanban.wake.completed"))
                        if "gave_up" in _wake_kinds: _parts.append(t("gateway.kanban.wake.gave_up"))
                        if "crashed" in _wake_kinds: _parts.append(t("gateway.kanban.wake.crashed"))
                        if "timed_out" in _wake_kinds: _parts.append(t("gateway.kanban.wake.timed_out"))
                        if "blocked" in _wake_kinds: _parts.append(t("gateway.kanban.wake.blocked"))
                        _status = t("gateway.kanban.wake.status_joiner").join(_parts) or t("gateway.kanban.wake.status_default")
                        _synth = t(
                            "gateway.kanban.wake.message",
                            task_id=sub["task_id"],
                            status=_status,
                            title=_title,
                            assignee=_assignee,
                            board=board_slug,
                        )

                    if not _is_push_adapter and wake_deferred_ids:
                        # Non-push adapter (api_server): the wake self-post IS
                        # the delivery, so it must succeed BEFORE the deferred
                        # events are confirmed — and only confirmed events move
                        # the cursor. A failed self-post therefore cannot
                        # silently lose the event; it stays retryable and, for
                        # an OWNED sub, eventually dead-letters.
                        if _wake_kinds and _session_key:
                            from gateway.wake import deliver_wake

                            try:
                                await deliver_wake(
                                    adapter,
                                    text=_synth,
                                    session_id=_session_key,
                                )
                                logger.info(
                                    "kanban notifier: woke agent for %s on %s/%s profile=%s events=%s",
                                    sub["task_id"], platform_str, sub["chat_id"], sub_profile or "default", _wake_kinds,
                                )
                                for _ev_id in wake_deferred_ids:
                                    await asyncio.to_thread(
                                        self._kanban_confirm_sent,
                                        sub, _ev_id, board_slug, lease_owner,
                                    )
                            except Exception as _wk_err:
                                logger.warning(
                                    "kanban notifier: wake self-post failed for %s "
                                    "(event(s) %s stay retryable): %s",
                                    sub["task_id"], wake_deferred_ids, _wk_err,
                                    exc_info=True,
                                )
                                for _ev_id in wake_deferred_ids:
                                    _fail_status = await asyncio.to_thread(
                                        self._kanban_record_failure,
                                        sub, _ev_id,
                                        f"wake self-post failed: {_wk_err}",
                                        retry_limit, board_slug, owned, lease_owner,
                                    )
                                    if _fail_status == "dead":
                                        logger.warning(
                                            "kanban notifier: DEAD-LETTER — event %s for %s "
                                            "on %s undeliverable after %d wake attempts: %s "
                                            "(inspect via kanban_db.list_dead_letter_deliveries)",
                                            _ev_id, sub["task_id"], platform_str,
                                            retry_limit, _wk_err,
                                        )
                                delivered_all = False
                        else:
                            # Nothing wake-worthy (no wake kind, or the task has
                            # no creator session): a non-push adapter has no
                            # other delivery path, so confirm the leased events
                            # instead of wedging the cursor on them forever.
                            for _ev_id in wake_deferred_ids:
                                await asyncio.to_thread(
                                    self._kanban_confirm_sent,
                                    sub, _ev_id, board_slug, lease_owner,
                                )
                        wake_deferred_ids = []

                    if wake_deferred_ids:
                        # Defensive: events deferred to a self-post that never
                        # ran are released (unclaimed, no attempt burned) so the
                        # next tick re-leases and retries them.
                        await asyncio.to_thread(
                            self._kanban_release_leases,
                            sub, wake_deferred_ids, board_slug, lease_owner,
                        )

                    # Advance the cursor across the contiguous confirmed
                    # ('sent'/'dead') prefix only. A still-pending/failed event
                    # holds the cursor so it stays replayable next tick; a
                    # dead-lettered event is stepped over so it can't wedge the
                    # subscription. This is the sole cursor-advance path — the
                    # lease deliberately does NOT advance it.
                    await asyncio.to_thread(
                        self._kanban_advance_confirmed, sub, TERMINAL_KINDS, board_slug,
                    )
                    if delivered_all:
                        # Every leased event was actually delivered this tick.
                        # Adopt an ownerless sub on first fully-successful
                        # delivery: CAS-stamp notifier_profile to THIS gateway
                        # so it converges to the single process that can reach
                        # this chat. Only a gateway that DID deliver adopts, so
                        # a wrong-token gateway can never steal a legacy NULL
                        # subscription.
                        if not d.get("owner_profile"):
                            try:
                                await asyncio.to_thread(
                                    self._kanban_claim_ownership,
                                    sub,
                                    notifier_profile,
                                    board_slug,
                                )
                            except Exception as own_exc:
                                logger.debug(
                                    "kanban notifier: ownership adopt for %s failed: %s",
                                    sub["task_id"], own_exc,
                                )
                        if _is_push_adapter and _wake_kinds and _session_key:
                            try:
                                from gateway.session import SessionSource
                                from gateway.wake import deliver_wake
                                # Rebuild the creator's real session scope from
                                # the chat_type persisted on the subscription
                                # row (#56580). build_session_key() keys DMs
                                # (":dm:<chat_id>") on a wholly different shape
                                # from group/thread, so the old hardcoded
                                # "group" mis-routed DM/thread creators into a
                                # fresh session. Legacy rows written before the
                                # column existed may still carry chat_type in
                                # delivery_metadata (#60600 rows) — fall back
                                # to that, then to "group" (the historical
                                # default that suits the dashboard/group flows).
                                # handle_message() get_or_create_session's the
                                # target, so a mismatch only ever degrades to a
                                # fresh session, never an exception.
                                _chat_type = str(sub.get("chat_type") or "").strip()
                                if not _chat_type:
                                    _delivery_meta = sub.get("delivery_metadata")
                                    if isinstance(_delivery_meta, dict):
                                        _chat_type = str(
                                            _delivery_meta.get("chat_type") or ""
                                        ).strip()
                                _chat_type = _chat_type or "group"
                                _source = SessionSource(
                                    platform=plat,
                                    chat_id=sub["chat_id"],
                                    chat_type=_chat_type,
                                    thread_id=sub.get("thread_id") or None,
                                    user_id=sub.get("user_id"),
                                    profile=sub_profile or None,
                                )
                                # deliver_wake preserves the synthetic
                                # MessageEvent/handle_message path for
                                # push-capable adapters (the non-push /
                                # self-post branch ran BEFORE the cursor
                                # advance above).
                                await deliver_wake(
                                    adapter,
                                    text=_synth,
                                    session_id=_session_key,
                                    source=_source,
                                )
                                logger.info(
                                    "kanban notifier: woke agent for %s on %s/%s profile=%s events=%s",
                                    sub["task_id"], platform_str, sub["chat_id"], sub_profile or "default", _wake_kinds,
                                )
                            except Exception as _wk_err:
                                # Best-effort: the notification itself already
                                # delivered and the cursor has advanced, so a
                                # broken wake path must not wedge the tick — but
                                # log at WARNING with a traceback rather than
                                # DEBUG so a persistently-failing wake is visible
                                # in normal logs instead of silently no-op'ing.
                                logger.warning(
                                    "kanban notifier: wakeup injection failed for %s: %s",
                                    sub["task_id"], _wk_err, exc_info=True,
                                )
                        # Unsubscribe only when the task has reached a truly
                        # final status (done / archived). For blocked /
                        # gave_up / crashed / timed_out the subscription is
                        # kept alive so the user gets notified again if the
                        # dispatcher respawns the task and it cycles into the
                        # same state. See the longer comment on TERMINAL_KINDS
                        # above for the failure mode this prevents.
                        if task_terminal:
                            await asyncio.to_thread(
                                self._kanban_unsub, sub, board_slug,
                            )
            except Exception as exc:
                logger.warning("kanban notifier tick failed: %s", exc)
            # Sleep with cancellation checks.
            for _ in range(int(max(1, interval))):
                if not self._running:
                    return
                await asyncio.sleep(1)

    def _kanban_advance(
        self, sub: dict, cursor: int, board: Optional[str] = None,
    ) -> None:
        """Sync helper: advance a subscription's cursor. Runs in to_thread.

        ``board`` scopes the DB connection to the board that owns this
        subscription. Unsub cursors in one board can't touch another's.
        """
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            _kb.advance_notify_cursor(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                new_cursor=cursor,
            )
        finally:
            conn.close()

    def _kanban_visible_thread_metadata(self, plat, sub, adapter) -> dict:
        """Thread routing that is guaranteed USER-VISIBLE for watcher sends.

        Watcher sends never have a reply anchor, and a Telegram private-chat
        topic without one has no visible route: a bare thread_id trips the
        adapter's anchor guard (silent ``success=False`` — the 2026-08-05
        t_0fc6b0dd loss), while the canonical fallback metadata
        (``direct_messages_topic_id``, operator-declared topics only) is
        accepted by the Bot API but may render nowhere the user looks (the
        same evening's cron loss). Cover BOTH shapes — operator-declared
        topics (fallback flag set) and ad-hoc user topics (bare thread on a
        private-looking chat id) — by stripping thread routing and sending to
        the root DM: visible-but-unthreaded beats invisible or wedged.
        """
        # Decision cache per routing target: the answer is stable for a given
        # (platform, chat, thread), the helper's cache-miss path re-reads
        # config from disk, and the notifier calls this every ~5s tick per sub
        # — without the cache the root-DM downgrade would also WARN-spam once
        # per tick for the whole lifetime of a task.
        cache = getattr(self, "_kanban_thread_meta_cache", None)
        if cache is None:
            cache = {}
            self._kanban_thread_meta_cache = cache
        key = (str(plat), str(sub.get("chat_id")), str(sub.get("thread_id") or ""))
        if key in cache:
            return dict(cache[key])

        meta = self._thread_metadata_for_target(
            plat, sub["chat_id"], sub.get("thread_id") or None, adapter=adapter,
        ) or {}
        if meta and not meta.get("telegram_reply_to_message_id"):
            from gateway.config import Platform as _Platform
            # Upstream renamed the helper (dropped the leading underscore) in
            # v2026.8.x; keep using the public name.
            from gateway.delivery import looks_like_telegram_private_chat_id
            anchorless_private_topic = meta.get("telegram_dm_topic_reply_fallback") or (
                plat == _Platform.TELEGRAM
                and looks_like_telegram_private_chat_id(str(sub.get("chat_id") or ""))
            )
            if anchorless_private_topic:
                logger.warning(
                    "kanban notifier: DM-topic target %s:%s for task %s has no "
                    "reply anchor; sending to the root DM without thread routing",
                    sub.get("chat_id"), sub.get("thread_id"), sub.get("task_id"),
                )
                meta = {}
        if len(cache) > 256:
            cache.clear()
        cache[key] = dict(meta)
        return dict(meta)

    async def _kanban_deliver_card(
        self, adapter, sub: dict, board: Optional[str], text: str, metadata: dict,
    ) -> None:
        """Deliver into THE card for this subscription, creating it if needed.

        Editing one message keeps a task to a single card instead of a stream
        of pings. The message id is persisted on the subscription row, so a
        gateway restart keeps editing the same card. Any edit failure (card
        deleted, adapter without edit support) falls back to a plain send —
        so delivery semantics are never weaker than before this existed.
        """
        card_id = sub.get("card_message_id")
        if card_id and text == (sub.get("card_text") or ""):
            return  # unchanged; don't spend an edit
        new_id = None
        if card_id and hasattr(adapter, "edit_message"):
            try:
                res = await adapter.edit_message(sub["chat_id"], int(card_id), text)
                if getattr(res, "success", False):
                    new_id = card_id
            except Exception as exc:
                logger.debug("kanban card: edit failed for %s: %s", sub["task_id"], exc)
        if new_id is None:
            res = await adapter.send(sub["chat_id"], text, metadata=metadata)
            # A SendResult(success=False) without an exception must count as a
            # FAILED delivery — adapters signal failure by RETURNING it rather
            # than raising (telegram's DM-topic guard: "requires a reply
            # anchor"). Without this check the caller confirms the event in the
            # durable ledger, the cursor advances, and the event is permanently
            # lost — an unchecked return durably marked lost sends as 'sent'
            # (2026-08-05 incident, t_0fc6b0dd). Adapters returning None (or
            # anything non-SendResult shaped) keep the legacy
            # "no exception == delivered" contract.
            if getattr(res, "success", True) is False:
                raise RuntimeError(
                    f"kanban card send failed for {sub['task_id']}: "
                    f"{getattr(res, 'error', None) or 'send returned success=False'}"
                )
            new_id = getattr(res, "message_id", None) or card_id
        sub["card_message_id"] = str(new_id) if new_id is not None else None
        sub["card_text"] = text
        try:
            await asyncio.to_thread(
                self._kanban_save_card, sub, board, sub["card_message_id"], text,
            )
        except Exception as exc:
            logger.debug("kanban card: persist failed for %s: %s", sub["task_id"], exc)

    def _kanban_save_card(
        self, sub: dict, board: Optional[str], message_id, text: str,
    ) -> None:
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            _kb.save_notify_card(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                message_id=message_id,
                text=text,
            )
        finally:
            conn.close()

    def _kanban_unsub(self, sub: dict, board: Optional[str] = None) -> None:
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            _kb.remove_notify_sub(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
            )
        finally:
            conn.close()

    def _kanban_rewind(
        self,
        sub: dict,
        claimed_cursor: int,
        old_cursor: int,
        board: Optional[str] = None,
    ) -> None:
        """Sync helper: undo a claimed notification cursor after send failure."""
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            _kb.rewind_notify_cursor(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                claimed_cursor=claimed_cursor,
                old_cursor=old_cursor,
            )
        finally:
            conn.close()

    def _kanban_claim_ownership(
        self, sub: dict, notifier_profile: str, board: Optional[str] = None,
    ) -> bool:
        """Sync helper: CAS-stamp notifier_profile on an ownerless sub.

        Runs in to_thread. No-op if the row is already owned (some other
        gateway won the race) or ``notifier_profile`` is empty.
        """
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            return _kb.claim_notify_ownership(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                profile=notifier_profile,
            )
        finally:
            conn.close()

    def _kanban_confirm_sent(
        self, sub: dict, event_id: int, board: Optional[str] = None,
        lease_owner: Optional[str] = None,
    ) -> None:
        """Sync helper: durably mark one leased event 'sent'. Runs in to_thread.

        ``lease_owner`` fences the write to this process's live lease.
        """
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            _kb.confirm_notify_sent(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                event_id=event_id,
                lease_owner=lease_owner,
            )
        finally:
            conn.close()

    def _kanban_record_failure(
        self,
        sub: dict,
        event_id: int,
        error: str,
        retry_limit: int,
        board: Optional[str] = None,
        owned: bool = True,
        lease_owner: Optional[str] = None,
    ) -> str:
        """Sync helper: record a failed delivery attempt / dead-letter.

        Returns ``'failed'`` (retryable), ``'dead'`` (bound reached; only for
        an owned sub), or ``'stale'`` (fenced out). ``owned`` gates the 'dead'
        transition; ``lease_owner`` fences the write. Runs in to_thread.
        """
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            return _kb.record_notify_failure(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                event_id=event_id,
                error=error,
                retry_limit=retry_limit,
                allow_dead=owned,
                lease_owner=lease_owner,
            )
        finally:
            conn.close()

    def _kanban_release_leases(
        self, sub: dict, event_ids: "list[int]", board: Optional[str] = None,
        lease_owner: Optional[str] = None,
    ) -> None:
        """Sync helper: release pending leases without burning an attempt.

        Used when delivery is abandoned for a non-failure reason (adapter
        disconnected between lease and send, unknown platform). ``lease_owner``
        fences the release to this process's live lease. Runs in to_thread.
        """
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            _kb.release_notify_leases(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                event_ids=event_ids,
                lease_owner=lease_owner,
            )
        finally:
            conn.close()

    def _kanban_advance_confirmed(
        self, sub: dict, kinds, board: Optional[str] = None,
    ) -> None:
        """Sync helper: advance the cursor over the confirmed prefix. to_thread.

        Delegates to :func:`kanban_db.advance_notify_cursor_over_confirmed`,
        which only moves the cursor past events durably marked 'sent'/'dead'.
        """
        from hermes_cli import kanban_db as _kb
        conn = _kb.connect(board=board)
        try:
            _kb.advance_notify_cursor_over_confirmed(
                conn,
                task_id=sub["task_id"],
                platform=sub["platform"],
                chat_id=sub["chat_id"],
                thread_id=sub.get("thread_id") or "",
                kinds=kinds,
            )
        finally:
            conn.close()

    async def _deliver_kanban_artifacts(
        self,
        *,
        adapter,
        chat_id: str,
        metadata: dict,
        event_payload: Optional[dict],
        task,
    ) -> None:
        """Upload artifact files referenced by a completed kanban task.

        Workers passing ``kanban_complete(artifacts=[...])`` ship absolute
        file paths through the completion event so downstream humans get
        the deliverable as a native upload instead of a path printed in
        chat.

        Only the explicit ``event_payload['artifacts']`` list is
        delivered (Batch-4 D2). Free-text summary / task.result path
        scanning was removed: it auto-uploaded any path a worker merely
        mentioned to the card's Telegram subscribers (incl. the
        clinic/patient bots).

        Files are deduplicated. A listed path that is missing on disk is
        a lost deliverable (the artifacts list is explicit since Batch-4
        D2), so it is logged at WARNING — never silently dropped — and
        delivery errors are logged but do not break the notifier loop.
        """
        from pathlib import Path as _Path

        candidates: list[str] = []
        seen: set[str] = set()

        def _add(path: str) -> None:
            if not path:
                return
            expanded = os.path.expanduser(path)
            if expanded in seen:
                return
            if not os.path.isfile(expanded):
                logger.warning(
                    "kanban notifier: artifact %s for task %s is missing on "
                    "disk — deliverable NOT sent",
                    expanded, getattr(task, "id", "?"),
                )
                return
            seen.add(expanded)
            candidates.append(expanded)

        # D2 (Batch-4): explicit, validated artifacts list ONLY.
        for _p in _artifact_paths_from_payload(event_payload):
            _add(_p)

        if not candidates:
            return

        from gateway.platforms.base import BasePlatformAdapter
        candidates = BasePlatformAdapter.filter_local_delivery_paths(candidates)
        if not candidates:
            return

        _IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
        _VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".3gp"}

        from urllib.parse import quote as _quote

        # Partition images so they ride a single send_multiple_images call
        # on platforms that support batch image uploads (Signal/Slack RPCs).
        image_paths = [p for p in candidates if _Path(p).suffix.lower() in _IMAGE_EXTS]
        other_paths = [p for p in candidates if _Path(p).suffix.lower() not in _IMAGE_EXTS]

        # Adapters can signal failure by RETURNING success=False instead of
        # raising — check every result so a lost deliverable is at least
        # loudly logged, never silently counted as delivered.
        task_id = getattr(task, "id", "?")

        if image_paths:
            try:
                batch = [(f"file://{_quote(p)}", "") for p in image_paths]
                res = await adapter.send_multiple_images(
                    chat_id=chat_id, images=batch, metadata=metadata,
                )
                if res is not None and getattr(res, "success", True) is False:
                    logger.warning(
                        "kanban notifier: image batch upload for task %s "
                        "returned failure: %s — deliverable NOT sent",
                        task_id, getattr(res, "error", None),
                    )
            except Exception as exc:
                logger.warning(
                    "kanban notifier: image batch upload for task %s failed: %s",
                    task_id, exc,
                )

        for path in other_paths:
            ext = _Path(path).suffix.lower()
            try:
                if ext in _VIDEO_EXTS:
                    res = await adapter.send_video(
                        chat_id=chat_id, video_path=path, metadata=metadata,
                    )
                else:
                    res = await adapter.send_document(
                        chat_id=chat_id, file_path=path, metadata=metadata,
                    )
                if res is not None and getattr(res, "success", True) is False:
                    logger.warning(
                        "kanban notifier: artifact upload (%s) for task %s "
                        "returned failure: %s — deliverable NOT sent",
                        path, task_id, getattr(res, "error", None),
                    )
            except Exception as exc:
                logger.warning(
                    "kanban notifier: artifact upload (%s) for task %s failed: %s",
                    path, task_id, exc,
                )

    async def _kanban_dispatcher_watcher(self) -> None:
        """Embedded kanban dispatcher — one tick every `dispatch_interval_seconds`.

        Gated by `kanban.dispatch_in_gateway` in config.yaml (default True).
        When true, the gateway hosts the single dispatcher for this profile:
        no separate `hermes kanban daemon` process needed. When false, the
        loop exits immediately and an external daemon is expected.

        Each tick calls :func:`kanban_db.dispatch_once` inside
        ``asyncio.to_thread`` so the SQLite WAL lock never blocks the
        event loop. Failures in one tick don't stop subsequent ticks —
        same pattern as `_kanban_notifier_watcher`.

        Shutdown: the loop checks ``self._running`` between ticks; gateway
        stop() flips it to False and cancels pending tasks, and the
        in-flight ``to_thread`` returns on its own after the current
        ``dispatch_once`` call finishes (typically <1ms on an idle board).
        """
        # Read config once at boot. If the user flips the flag later, they
        # restart the gateway; same pattern as every other background
        # watcher here. Honours HERMES_KANBAN_DISPATCH_IN_GATEWAY env var
        # as an escape hatch (false-y value disables without editing YAML).
        try:
            from hermes_cli.config import load_config as _load_config
        except Exception:
            logger.warning("kanban dispatcher: config loader unavailable; disabled")
            return
        env_override = os.environ.get("HERMES_KANBAN_DISPATCH_IN_GATEWAY", "").strip().lower()
        if env_override in {"0", "false", "no", "off"}:
            logger.info("kanban dispatcher: disabled via HERMES_KANBAN_DISPATCH_IN_GATEWAY env")
            return

        try:
            cfg = _load_config()
        except Exception as exc:
            logger.warning("kanban dispatcher: cannot load config (%s); disabled", exc)
            return
        kanban_cfg = cfg.get("kanban", {}) if isinstance(cfg, dict) else {}
        if not kanban_cfg.get("dispatch_in_gateway", True):
            logger.info(
                "kanban dispatcher: disabled via config kanban.dispatch_in_gateway=false"
            )
            return

        try:
            from hermes_cli import kanban_db as _kb
        except Exception:
            logger.warning("kanban dispatcher: kanban_db not importable; dispatcher disabled")
            return

        # Single-dispatcher backstop. dispatch_in_gateway defaults to true, so a
        # new profile gateway (or a same-profile restart race) can silently
        # start a second dispatcher; concurrent dispatchers double reclaim
        # frequency, double claim-attempt events, and — with
        # wal_autocheckpoint=0 — concurrent manual WAL checkpoints can corrupt
        # index pages. The lock lives at the machine-global kanban root
        # (shared across profiles by design), so it serialises ALL gateways.
        self._kanban_dispatcher_lock_handle = None
        _lock_path = _kb.kanban_home() / "kanban" / ".dispatcher.lock"
        _lock_handle, _lock_state = _acquire_singleton_lock(_lock_path)
        if _lock_state == "contended":
            logger.info(
                "kanban dispatcher: another gateway already holds the dispatcher "
                "lock (%s); this gateway will NOT dispatch.", _lock_path,
            )
            return
        if _lock_state == "held":
            self._kanban_dispatcher_lock_handle = _lock_handle  # hold for process lifetime
            logger.info("kanban dispatcher: holding singleton dispatcher lock (%s)", _lock_path)
        else:
            logger.warning(
                "kanban dispatcher: advisory lock unavailable at %s; proceeding "
                "on config control alone.", _lock_path,
            )

        try:
            interval = float(kanban_cfg.get("dispatch_interval_seconds", 60) or 60)
        except (ValueError, TypeError):
            logger.warning(
                "kanban dispatcher: invalid dispatch_interval_seconds=%r, using default 60",
                kanban_cfg.get("dispatch_interval_seconds"),
            )
            interval = 60.0
        interval = max(interval, 1.0)  # sanity floor — tighter than this is a footgun

        # Read max_spawn config to limit concurrent kanban tasks
        max_spawn = kanban_cfg.get("max_spawn", None)
        if max_spawn is not None:
            logger.info("kanban dispatcher: max_spawn=%s", max_spawn)

        # Cap the number of simultaneously running tasks so slow workers
        # (local LLMs, resource-constrained hosts) don't pile up and time
        # out. When set, the dispatcher skips spawning when the board
        # already has this many tasks in 'running' status.
        raw_max_in_progress = kanban_cfg.get("max_in_progress", None)
        max_in_progress = None
        if raw_max_in_progress is not None:
            try:
                max_in_progress = int(raw_max_in_progress)
            except (TypeError, ValueError):
                logger.warning(
                    "kanban dispatcher: invalid kanban.max_in_progress=%r; ignoring",
                    raw_max_in_progress,
                )
                max_in_progress = None
            else:
                if max_in_progress < 1:
                    logger.warning(
                        "kanban dispatcher: kanban.max_in_progress=%r is below 1; ignoring",
                        raw_max_in_progress,
                    )
                    max_in_progress = None
                else:
                    logger.info("kanban dispatcher: max_in_progress=%s", max_in_progress)

        raw_failure_limit = kanban_cfg.get("failure_limit", _kb.DEFAULT_FAILURE_LIMIT)
        try:
            failure_limit = int(raw_failure_limit)
        except (TypeError, ValueError):
            logger.warning(
                "kanban dispatcher: invalid kanban.failure_limit=%r; using default %d",
                raw_failure_limit,
                _kb.DEFAULT_FAILURE_LIMIT,
            )
            failure_limit = _kb.DEFAULT_FAILURE_LIMIT
        if failure_limit < 1:
            logger.warning(
                "kanban dispatcher: kanban.failure_limit=%r is below 1; using default %d",
                raw_failure_limit,
                _kb.DEFAULT_FAILURE_LIMIT,
            )
            failure_limit = _kb.DEFAULT_FAILURE_LIMIT

        # Read stale_timeout_seconds — 0 disables stale detection.
        raw_stale = kanban_cfg.get("dispatch_stale_timeout_seconds", 0)
        try:
            stale_timeout_seconds = int(raw_stale or 0)
        except (TypeError, ValueError):
            logger.warning(
                "kanban dispatcher: invalid kanban.dispatch_stale_timeout_seconds=%r; "
                "disabling stale detection",
                raw_stale,
            )
            stale_timeout_seconds = 0

        # FIX C (2026-07-28): worker-scoped wall-clock cap the dispatcher
        # stamps on claimed tasks with no explicit runtime limit. Unset ->
        # code default; 0/negative -> disabled. Only dispatcher-claimed
        # kanban tasks; interactive/agent sessions are unaffected.
        raw_default_runtime = kanban_cfg.get(
            "default_max_runtime_seconds", _kb.DEFAULT_WORKER_MAX_RUNTIME_SECONDS)
        try:
            default_max_runtime_seconds = int(raw_default_runtime)
        except (TypeError, ValueError):
            logger.warning(
                "kanban dispatcher: invalid kanban.default_max_runtime_seconds=%r; "
                "using default %d", raw_default_runtime, _kb.DEFAULT_WORKER_MAX_RUNTIME_SECONDS)
            default_max_runtime_seconds = _kb.DEFAULT_WORKER_MAX_RUNTIME_SECONDS
        if default_max_runtime_seconds <= 0:
            default_max_runtime_seconds = None
            logger.info("kanban dispatcher: default worker runtime cap disabled")
        else:
            logger.info("kanban dispatcher: default_max_runtime_seconds=%d", default_max_runtime_seconds)

        # Read kanban.default_assignee — fallback profile for tasks
        # created without an explicit assignee (e.g. via the dashboard).
        # When set, the dispatcher applies it to unassigned ready tasks
        # instead of skipping them indefinitely (#27145). Empty string
        # (the schema default) means "no fallback, keep skipping" —
        # backward-compatible with existing installs.
        default_assignee = (kanban_cfg.get("default_assignee") or "").strip() or None
        if default_assignee:
            logger.info(
                "kanban dispatcher: default_assignee=%r (unassigned ready tasks "
                "will route to this profile)",
                default_assignee,
            )

        # Read kanban.max_in_progress_per_profile — per-profile concurrency
        # cap (#21582). When set, no single profile gets more than N
        # workers running at once, even if the global max_in_progress
        # would allow it. Prevents one profile's local model / API quota
        # / browser pool from being overwhelmed by a fan-out.
        raw_per_profile = kanban_cfg.get("max_in_progress_per_profile", None)
        max_in_progress_per_profile = None
        if raw_per_profile is not None:
            try:
                max_in_progress_per_profile = int(raw_per_profile)
            except (TypeError, ValueError):
                logger.warning(
                    "kanban dispatcher: invalid kanban.max_in_progress_per_profile=%r; ignoring",
                    raw_per_profile,
                )
                max_in_progress_per_profile = None
            else:
                if max_in_progress_per_profile < 1:
                    logger.warning(
                        "kanban dispatcher: kanban.max_in_progress_per_profile=%r is below 1; ignoring",
                        raw_per_profile,
                    )
                    max_in_progress_per_profile = None
                else:
                    logger.info(
                        "kanban dispatcher: max_in_progress_per_profile=%d",
                        max_in_progress_per_profile,
                    )

        # Initial delay so the gateway finishes wiring adapters before the
        # dispatcher spawns workers (those workers may hit gateway notify
        # subscriptions etc.). Matches the notifier watcher's delay.
        await asyncio.sleep(5)

        # Health telemetry mirrored from `_cmd_daemon`: warn when ready
        # queue is non-empty but spawns are 0 for N consecutive ticks —
        # usually means broken PATH, missing venv, or credential loss.
        HEALTH_WINDOW = 6
        bad_ticks = 0
        last_warn_at = 0
        # Avoid hot-looping corrupt-looking board DBs, but do not suppress
        # same-fingerprint retries forever: transient WAL/open races can
        # surface as "database disk image is malformed" for one tick.
        CORRUPT_BOARD_RETRY_AFTER_SECONDS = 300
        disabled_corrupt_boards: dict[
            str, tuple[tuple[str, int | None, int | None], float]
        ] = {}

        def _board_db_fingerprint(slug: str) -> tuple[str, int | None, int | None]:
            path = _kb.kanban_db_path(slug)
            try:
                resolved = str(path.expanduser().resolve())
            except Exception:
                resolved = str(path)
            try:
                stat = path.stat()
            except OSError:
                return (resolved, None, None)
            return (resolved, stat.st_mtime_ns, stat.st_size)

        def _is_corrupt_board_db_error(exc: Exception) -> bool:
            corrupt_guard_error = getattr(_kb, "KanbanDbCorruptError", None)
            if corrupt_guard_error is not None and isinstance(exc, corrupt_guard_error):
                return True
            if not isinstance(exc, sqlite3.DatabaseError):
                return False
            msg = str(exc).lower()
            return (
                "file is not a database" in msg
                or "database disk image is malformed" in msg
            )

        def _tick_once_for_board(slug: str) -> "Optional[object]":
            """Run one dispatch_once for a specific board.

            Runs in a worker thread via `asyncio.to_thread`. `board=slug`
            is passed through `dispatch_once` so `resolve_workspace` and
            `_default_spawn` see the right paths. The per-board DB is
            opened explicitly so concurrent boards never share a
            connection handle or accidentally claim across each other.
            """
            conn = None
            fingerprint = _board_db_fingerprint(slug)
            disabled_entry = disabled_corrupt_boards.get(slug)
            if disabled_entry is not None:
                disabled_fingerprint, disabled_at = disabled_entry
                age = time.monotonic() - disabled_at
                if (
                    disabled_fingerprint == fingerprint
                    and age < CORRUPT_BOARD_RETRY_AFTER_SECONDS
                ):
                    return None
                if disabled_fingerprint == fingerprint:
                    logger.info(
                        "kanban dispatcher: board %s database fingerprint unchanged "
                        "after %.0fs quarantine; retrying dispatch",
                        slug,
                        age,
                    )
                else:
                    logger.info(
                        "kanban dispatcher: board %s database changed; retrying dispatch",
                        slug,
                    )
                disabled_corrupt_boards.pop(slug, None)
            try:
                conn = _kb.connect(board=slug)
                # `connect()` runs the schema + idempotent migration on
                # first open per process; the previous explicit
                # `init_db()` call here busted the per-process cache and
                # re-ran the migration on a second connection, racing
                # the first. See the matching comment in
                # `_kanban_notifier_watcher` and issue #21378.
                return _kb.dispatch_once(
                    conn,
                    board=slug,
                    max_spawn=max_spawn,
                    max_in_progress=max_in_progress,
                    failure_limit=failure_limit,
                    stale_timeout_seconds=stale_timeout_seconds,
                    default_assignee=default_assignee,
                    max_in_progress_per_profile=max_in_progress_per_profile,
                    default_max_runtime_seconds=default_max_runtime_seconds,
                )
            except sqlite3.DatabaseError as exc:
                if _is_corrupt_board_db_error(exc):
                    disabled_corrupt_boards[slug] = (fingerprint, time.monotonic())
                    logger.error(
                        "kanban dispatcher: board %s database %s is not a valid "
                        "SQLite database; pausing dispatch for this board until "
                        "the file changes, the gateway restarts, or the "
                        "quarantine timer expires. Move or restore the file, "
                        "then run `hermes kanban init` if you need a fresh board.",
                        slug,
                        fingerprint[0],
                    )
                    return None
                logger.exception("kanban dispatcher: tick failed on board %s", slug)
                return None
            except Exception as exc:
                if _is_corrupt_board_db_error(exc):
                    disabled_corrupt_boards[slug] = (fingerprint, time.monotonic())
                    logger.error(
                        "kanban dispatcher: board %s database %s is not a valid "
                        "SQLite database; pausing dispatch for this board until "
                        "the file changes, the gateway restarts, or the "
                        "quarantine timer expires. Move or restore the file, "
                        "then run `hermes kanban init` if you need a fresh board.",
                        slug,
                        fingerprint[0],
                    )
                    return None
                logger.exception("kanban dispatcher: tick failed on board %s", slug)
                return None
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        def _tick_once() -> "list[tuple[str, Optional[object]]]":
            """Run one dispatch_once per board. Returns (slug, result) pairs.

            Enumerating boards on every tick keeps the dispatcher honest
            when users create a new board mid-run: no restart required,
            the next tick picks it up automatically.
            """
            try:
                boards = _kb.list_boards(include_archived=False)
            except Exception:
                boards = [_kb.read_board_metadata(_kb.DEFAULT_BOARD)]
            out: list[tuple[str, "Optional[object]"]] = []
            for b in boards:
                slug = b.get("slug") or _kb.DEFAULT_BOARD
                out.append((slug, _tick_once_for_board(slug)))
            return out

        def _ready_nonempty() -> bool:
            """Cheap probe: is there at least one ready+assigned+unclaimed
            task on ANY board whose assignee maps to a real Hermes profile
            (i.e. one the dispatcher would actually spawn for)?

            Tasks assigned to control-plane lanes (e.g. ``orion-cc``,
            ``orion-research``) are pulled by terminals via
            ``claim_task`` directly and never spawnable, so a queue full
            of those is "correctly idle", not "stuck". Filtering them out
            here keeps the stuck-warn fire only on real failures (broken
            PATH, missing venv, credential loss for a real Hermes profile).
            """
            try:
                boards = _kb.list_boards(include_archived=False)
            except Exception:
                boards = [_kb.read_board_metadata(_kb.DEFAULT_BOARD)]
            for b in boards:
                slug = b.get("slug") or _kb.DEFAULT_BOARD
                conn = None
                try:
                    conn = _kb.connect(board=slug)
                    if _kb.has_spawnable_ready(conn):
                        return True
                    if _kb.has_spawnable_review(conn):
                        return True
                except Exception:
                    continue
                finally:
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
            return False

        # Auto-decompose: turn fresh triage tasks into ready workgraphs
        # before the dispatcher fans out workers. Gated by
        # ``kanban.auto_decompose`` (default True). Capped by
        # ``kanban.auto_decompose_per_tick`` (default 3) so a bulk-load
        # of triage tasks doesn't burst-spend the aux LLM in one tick;
        # remainder defers to subsequent ticks.
        #
        # The flag is re-read from config EVERY tick (#49638) rather than
        # captured once at boot. Auto-decompose is a safety toggle: a user who
        # sees it fan out and run tasks they didn't intend reaches for
        # ``kanban.auto_decompose: false`` to STOP it — and that must take
        # effect on the next tick, not require a gateway restart. (Reported:
        # auto-decompose created and launched destructive tasks while the user
        # was still typing the task description, and the flag "couldn't be
        # disabled" because the gateway had captured its boot-time value.)
        def _read_auto_decompose_settings() -> tuple[bool, int]:
            """Re-resolve (enabled, per_tick) from current config each tick."""
            return _resolve_auto_decompose_settings(_load_config)

        def _auto_decompose_tick(auto_decompose_per_tick: int) -> int:
            """Run the auto-decomposer for up to N triage tasks across all
            boards. Returns the number of triage tasks that were
            successfully decomposed or specified this tick.
            """
            try:
                from hermes_cli import kanban_decompose as _decomp
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "kanban auto-decompose: import failed (%s); skipping", exc,
                )
                return 0
            try:
                boards = _kb.list_boards(include_archived=False)
            except Exception:
                boards = [_kb.read_board_metadata(_kb.DEFAULT_BOARD)]
            attempted = 0
            successes = 0
            for b in boards:
                slug = b.get("slug") or _kb.DEFAULT_BOARD
                if attempted >= auto_decompose_per_tick:
                    break
                # Pin this board for the duration of the call — same
                # pattern as the dashboard specify endpoint. The
                # decomposer module connects with no board kwarg and
                # relies on the env var.
                prev_env = os.environ.get("HERMES_KANBAN_BOARD")
                try:
                    os.environ["HERMES_KANBAN_BOARD"] = slug
                    try:
                        triage_ids = _decomp.list_triage_ids()
                    except Exception as exc:
                        logger.debug(
                            "kanban auto-decompose: list_triage_ids failed on board %s (%s)",
                            slug, exc,
                        )
                        triage_ids = []
                    for tid in triage_ids:
                        if attempted >= auto_decompose_per_tick:
                            break
                        attempted += 1
                        try:
                            outcome = _decomp.decompose_task(
                                tid, author="auto-decomposer",
                            )
                        except Exception:
                            logger.exception(
                                "kanban auto-decompose: decompose_task crashed on %s",
                                tid,
                            )
                            continue
                        if outcome.ok:
                            successes += 1
                            if outcome.fanout and outcome.child_ids:
                                logger.info(
                                    "kanban auto-decompose [%s]: %s → %d children",
                                    slug, tid, len(outcome.child_ids),
                                )
                            else:
                                logger.info(
                                    "kanban auto-decompose [%s]: %s → single task (no fanout)",
                                    slug, tid,
                                )
                        else:
                            # Common no-op reasons (no aux client configured) shouldn't
                            # spam logs every tick. Log at debug.
                            logger.debug(
                                "kanban auto-decompose [%s]: %s skipped: %s",
                                slug, tid, outcome.reason,
                            )
                finally:
                    if prev_env is None:
                        os.environ.pop("HERMES_KANBAN_BOARD", None)
                    else:
                        os.environ["HERMES_KANBAN_BOARD"] = prev_env
            return successes

        logger.info(
            "kanban dispatcher: embedded in gateway (interval=%.1fs)", interval
        )
        while self._running:
            try:
                # Reap zombie children before per-board work so a board DB
                # failure cannot block cleanup of unrelated workers.
                pids = await asyncio.to_thread(_kb.reap_worker_zombies)
                if pids:
                    logger.info(
                        "kanban dispatcher: reaped %d zombie worker(s), pids=%s",
                        len(pids),
                        pids,
                    )
            except Exception:
                logger.exception("kanban dispatcher: zombie reaper failed")

            try:
                # Re-read the auto-decompose toggle live each tick so a user
                # flipping kanban.auto_decompose=false to STOP runaway fan-out
                # takes effect on the next tick, not on gateway restart (#49638).
                _ad_enabled, _ad_per_tick = _read_auto_decompose_settings()
                if _ad_enabled:
                    await asyncio.to_thread(_auto_decompose_tick, _ad_per_tick)
                results = await asyncio.to_thread(_tick_once)
                any_spawned = False
                for slug, res in (results or []):
                    if res is not None and getattr(res, "spawned", None):
                        any_spawned = True
                        # Quiet by default — only log when something actually
                        # happened, so an idle gateway stays silent.
                        logger.info(
                            "kanban dispatcher [%s]: spawned=%d reclaimed=%d "
                            "crashed=%d timed_out=%d promoted=%d auto_blocked=%d",
                            slug,
                            len(res.spawned),
                            res.reclaimed,
                            len(res.crashed) if hasattr(res.crashed, "__len__") else 0,
                            len(res.timed_out) if hasattr(res.timed_out, "__len__") else 0,
                            res.promoted,
                            len(res.auto_blocked) if hasattr(res.auto_blocked, "__len__") else 0,
                        )
                # Health telemetry (aggregate across boards)
                ready_pending = await asyncio.to_thread(_ready_nonempty)
                if ready_pending and not any_spawned:
                    bad_ticks += 1
                else:
                    bad_ticks = 0
                if bad_ticks >= HEALTH_WINDOW:
                    now = int(time.time())
                    if now - last_warn_at >= 300:
                        logger.warning(
                            "kanban dispatcher stuck: ready queue non-empty for "
                            "%d consecutive ticks but 0 workers spawned. Check "
                            "profile health (venv, PATH, credentials) and "
                            "`hermes kanban list --status ready`.",
                            bad_ticks,
                        )
                        last_warn_at = now
            except asyncio.CancelledError:
                logger.debug("kanban dispatcher: cancelled")
                self._release_kanban_dispatcher_lock()
                raise
            except Exception:
                logger.exception("kanban dispatcher: unexpected watcher error")

            # Sleep in 1s slices so shutdown is snappy — otherwise a stop()
            # waits up to `interval` seconds for the current sleep to finish.
            slept = 0.0
            while slept < interval and self._running:
                await asyncio.sleep(min(1.0, interval - slept))
                slept += 1.0

        self._release_kanban_dispatcher_lock()
