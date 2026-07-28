"""Generic interactive approval-card bus (gateway ↔ in-process producer bridge).

A producer running inside a tool-execution middleware (synchronous, on the
agent thread) sometimes needs to push an interactive approval card to the
user's *bound* chat/thread and later receive the button click. Only the
gateway platform adapter can actually talk to Telegram/Slack/etc. and owns the
event loop. This module is the thin, generic seam between the two:

* A gateway platform adapter registers a *card sender* for its platform at
  connect time (:func:`register_card_sender`) and drops it at disconnect. The
  sender is a plain callable ``fn(card) -> bool`` returning True iff the card
  was delivered.
* Any in-process producer (e.g. the prod-approvals gate) constructs an
  :class:`ApprovalCard` and calls :func:`deliver_card`; the bus routes it to the
  sender registered for ``card.platform``.

Button *clicks* travel back through the plugin manager's generic gateway
action-handler registry (``PluginContext.register_gateway_action_handler`` →
``dispatch_gateway_action``); this module only concerns outbound delivery and
the small result type both sides share.

Nothing here is Telegram- or prod-approvals-specific. It is process-global
(one registry per gateway process / profile), thread-safe, and daemon-free.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# A single button: (label, callback_data). callback_data must be <= 64 bytes
# (Telegram's hard limit); producers are responsible for staying within it.
Button = Tuple[str, str]


@dataclass
class ApprovalCard:
    """An interactive card to deliver to one authoritative chat/thread."""

    platform: str
    chat_id: str
    text: str
    buttons: List[List[Button]] = field(default_factory=list)  # rows of buttons
    thread_id: str = ""
    # Opaque correlation key (e.g. a nonce) for logging/dedupe. Never shown.
    key: str = ""


@dataclass
class GatewayActionResult:
    """Return value of a plugin gateway-action (button click) handler."""

    handled: bool = True
    answer_text: str = ""              # short toast shown to the clicker
    edit_text: Optional[str] = None    # if set, replace the card body with this
    remove_buttons: bool = True        # drop the keyboard after resolution


# platform -> sender callable(card) -> bool
_senders: Dict[str, Callable[[ApprovalCard], bool]] = {}
_lock = threading.RLock()


def register_card_sender(platform: str, sender: Callable[[ApprovalCard], bool]) -> None:
    """Register (or replace) the card sender for *platform*.

    Called by a gateway adapter once it is connected and owns a live event
    loop. Replacing an existing sender is intentional: a reconnect installs a
    fresh sender bound to the new loop.
    """
    if not platform or not callable(sender):
        raise ValueError("register_card_sender requires a platform and a callable sender")
    with _lock:
        _senders[platform] = sender
    logger.debug("approval_cards: registered card sender for platform %s", platform)


def unregister_card_sender(platform: str, sender: Optional[Callable] = None) -> None:
    """Remove the sender for *platform* (only if it matches *sender* when given)."""
    with _lock:
        cur = _senders.get(platform)
        if cur is None:
            return
        if sender is not None and cur is not sender:
            return
        _senders.pop(platform, None)
    logger.debug("approval_cards: unregistered card sender for platform %s", platform)


def has_sender(platform: str) -> bool:
    with _lock:
        return platform in _senders


def deliver_card(card: ApprovalCard) -> bool:
    """Deliver *card* through the registered sender for its platform.

    Returns True iff a sender was present and reported success. Never raises:
    a missing sender or a sender exception yields ``False`` so the producer can
    fall back (and, for a security gate, still fail closed by blocking).
    """
    with _lock:
        sender = _senders.get(card.platform)
    if sender is None:
        logger.debug("approval_cards: no sender for platform %s; card not delivered", card.platform)
        return False
    try:
        return bool(sender(card))
    except Exception:  # pragma: no cover - defensive; sender must be robust
        logger.warning("approval_cards: sender for %s raised", card.platform, exc_info=True)
        return False


def _reset_for_tests() -> None:
    """Clear all senders (test hygiene only)."""
    with _lock:
        _senders.clear()
