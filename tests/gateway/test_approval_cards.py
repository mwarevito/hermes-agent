"""Unit tests for the generic gateway approval-card bus."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from gateway import approval_cards as ac


@pytest.fixture(autouse=True)
def _clean():
    ac._reset_for_tests()
    yield
    ac._reset_for_tests()


def _card(platform="telegram", chat="1"):
    return ac.ApprovalCard(platform=platform, chat_id=chat, text="x",
                           buttons=[[("Approve", "pa:approve:n1")]])


def test_no_sender_returns_false():
    assert ac.has_sender("telegram") is False
    assert ac.deliver_card(_card()) is False


def test_register_and_deliver_routes_by_platform():
    seen = []
    ac.register_card_sender("telegram", lambda c: (seen.append(c) or True))
    assert ac.has_sender("telegram") is True
    assert ac.deliver_card(_card()) is True
    assert len(seen) == 1
    # A card for a platform with no sender is not delivered.
    assert ac.deliver_card(_card(platform="slack")) is False


def test_sender_exception_is_swallowed_as_false():
    def boom(_c):
        raise RuntimeError("send failed")

    ac.register_card_sender("telegram", boom)
    assert ac.deliver_card(_card()) is False  # never raises → gate can fail closed


def test_unregister_only_removes_matching_sender():
    s1 = lambda c: True
    s2 = lambda c: True
    ac.register_card_sender("telegram", s1)
    # Unregister with a different sender ref is a no-op.
    ac.unregister_card_sender("telegram", s2)
    assert ac.has_sender("telegram") is True
    # Unregister the real one removes it.
    ac.unregister_card_sender("telegram", s1)
    assert ac.has_sender("telegram") is False


def test_register_replaces_on_reconnect():
    ac.register_card_sender("telegram", lambda c: True)
    seen = []
    ac.register_card_sender("telegram", lambda c: (seen.append(c) or True))
    ac.deliver_card(_card())
    assert len(seen) == 1  # the second (reconnect) sender is the live one


def test_register_rejects_bad_args():
    with pytest.raises(ValueError):
        ac.register_card_sender("", lambda c: True)
    with pytest.raises(ValueError):
        ac.register_card_sender("telegram", None)  # type: ignore[arg-type]
