"""Integration tests for prod-approvals through the REAL framework surfaces.

Unlike test_prod_approvals.py (which drives the plugin's helpers directly),
these load the plugin through the real :class:`PluginManager`, deliver cards
through the real ``gateway.approval_cards`` bus, resolve clicks through the real
``dispatch_gateway_action`` path, and route a Telegram inline-button click
through the real ``TelegramAdapter._handle_callback_query``. This is the proof
that the one-tap approval works end-to-end, not just in isolated helpers.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)

SVC = "11111111-2222-3333-4444-555555555555"
CMD_SET = f"railway variables --set DATABASE_URL=postgres://secret --service {SVC}"
CMD_RESTART = f"railway redeploy --service {SVC} --yes"


# ---------------------------------------------------------------------------
# Minimal Telegram mock so the adapter imports (mirrors test_telegram_*).
# ---------------------------------------------------------------------------
def _ensure_telegram_mock():
    if "telegram" in sys.modules and hasattr(sys.modules["telegram"], "__file__"):
        return
    mod = MagicMock()
    mod.ext.ContextTypes.DEFAULT_TYPE = type(None)
    mod.constants.ParseMode.MARKDOWN = "Markdown"
    mod.constants.ParseMode.MARKDOWN_V2 = "MarkdownV2"
    mod.constants.ParseMode.HTML = "HTML"
    mod.constants.ChatType.PRIVATE = "private"
    mod.constants.ChatType.GROUP = "group"
    mod.constants.ChatType.SUPERGROUP = "supergroup"
    mod.constants.ChatType.CHANNEL = "channel"
    mod.error.NetworkError = type("NetworkError", (OSError,), {})
    mod.error.TimedOut = type("TimedOut", (OSError,), {})
    mod.error.BadRequest = type("BadRequest", (Exception,), {})
    for name in ("telegram", "telegram.ext", "telegram.constants", "telegram.request"):
        sys.modules.setdefault(name, mod)
    sys.modules.setdefault("telegram.error", mod.error)


@pytest.fixture
def home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME + session env so bind_context/store are hermetic."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_PROFILE", "prodtest")
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "telegram")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "555")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "42")
    monkeypatch.setenv("HERMES_SESSION_ID", "sess-int")
    # No kanban anchor in this scenario (plain chat) → weaker but explicit scope.
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    return tmp_path


@pytest.fixture
def real_manager(home, monkeypatch):
    """Load ONLY prod-approvals into a FRESH PluginManager via the real
    directory loader (the same code path the deploy uses for bundled plugins),
    and point the module-level gateway-dispatch helpers at it. Using a fresh
    manager — not the process-global one — keeps the global registry (and
    every other test that relies on it) completely untouched."""
    from hermes_cli import plugins as hp
    from gateway import approval_cards

    mgr = hp.PluginManager()
    manifests = mgr._scan_directory(hp.get_bundled_plugins_dir(), source="bundled")
    manifest = next(m for m in manifests if m.name == "prod-approvals")
    mgr._load_plugin(manifest)

    # The gateway callback path imports these module-level helpers at call
    # time; route them at our isolated manager so the real Telegram dispatch
    # exercises our handler without triggering a global discovery sweep.
    monkeypatch.setattr(hp, "get_gateway_action_handlers",
                        lambda: list(mgr._gateway_action_handlers))
    monkeypatch.setattr(hp, "dispatch_gateway_action",
                        lambda data, clicker: mgr.dispatch_gateway_action(data, clicker))

    yield mgr, manifest

    from plugins.prod_approvals import heartbeat
    heartbeat.stop_singleton()
    approval_cards._reset_for_tests()


# ---------------------------------------------------------------------------
# 1. Real discovery + activation
# ---------------------------------------------------------------------------

def test_deploy_files_are_repo_resident_and_discoverable():
    """The plugin + backstop + install/rollback ship in-repo under the bundled
    plugins dir, so the normal source deploy carries them (no hand-edited live
    source)."""
    from hermes_cli.plugins import get_bundled_plugins_dir
    root = get_bundled_plugins_dir() / "prod_approvals"
    assert (root / "plugin.yaml").exists()
    assert (root / "__init__.py").exists()
    assert (root / "backstop" / "gate-prod-writes.sh").exists()
    assert (root / "deploy" / "install.sh").exists()
    assert (root / "deploy" / "rollback.sh").exists()


def test_real_plugin_discovery_wires_all_surfaces(real_manager):
    mgr, manifest = real_manager
    loaded = mgr._plugins.get(manifest.key or manifest.name)
    assert loaded is not None and loaded.error is None and loaded.enabled is True
    assert mgr._middleware.get("tool_execution")
    assert "approve-prod" in mgr._plugin_commands
    assert any(p == "pa:" for (p, _cb, _pl) in mgr._gateway_action_handlers)
    assert "prod_bundle_request" in mgr._plugin_tool_names
    # heartbeat marker present + fresh → the backstop will defer to the gate
    import os
    from plugins.prod_approvals import heartbeat
    base = Path(os.environ["HERMES_HOME"]) / "prod_approvals"
    assert heartbeat.is_fresh(base) is True


# ---------------------------------------------------------------------------
# 2. Real card bus delivery + real dispatch resolution (one-tap, no relay)
# ---------------------------------------------------------------------------

def _capture_sender():
    cards = []

    def sender(card):
        cards.append(card)
        return True

    sender.cards = cards
    return sender


def _sink():
    calls = []

    def next_call(args):
        calls.append(args)
        return json.dumps({"ok": True, "exit_code": 0, "stdout": "done"})

    next_call.calls = calls
    return next_call


def _clicker(chat_id="555", user_id="42", thread_id=""):
    return {
        "platform": "telegram",
        "chat_id": chat_id,
        "thread_id": thread_id,
        "user_id": user_id,
        "user_name": "Vito",
    }


def test_card_delivered_and_click_dispatched_end_to_end(real_manager):
    from gateway import approval_cards
    from plugins.prod_approvals import gate

    mgr, _ = real_manager
    sender = _capture_sender()
    approval_cards.register_card_sender("telegram", sender)

    nc = _sink()
    # Gate through the REAL default card bus (no deliver_card injection).
    r = json.loads(gate.evaluate("terminal", {"command": CMD_SET}, nc))
    assert r["blocked"] is True and r["card_delivered"] is True
    assert nc.calls == []
    assert len(sender.cards) == 1
    card = sender.cards[0]
    assert card.chat_id == "555"
    datas = [d for row in card.buttons for (_l, d) in row]
    assert f"pa:approve:{r['nonce']}" in datas
    assert "postgres://secret" not in card.text  # no secret on the wire

    # Click the approve button → real manager dispatch → real callback handler.
    result = mgr.dispatch_gateway_action(f"pa:approve:{r['nonce']}", _clicker())
    assert result is not None and getattr(result, "answer_text", "")

    # Re-issue: now executes exactly once.
    r2 = gate.evaluate("terminal", {"command": CMD_SET}, nc)
    assert json.loads(r2)["exit_code"] == 0
    assert len(nc.calls) == 1


def test_two_pending_cards_resolve_independently(real_manager):
    from gateway import approval_cards
    from plugins.prod_approvals import gate

    mgr, _ = real_manager
    sender = _capture_sender()
    approval_cards.register_card_sender("telegram", sender)

    nc = _sink()
    r_set = json.loads(gate.evaluate("terminal", {"command": CMD_SET}, nc))
    r_restart = json.loads(gate.evaluate("terminal", {"command": CMD_RESTART}, nc))
    assert r_set["nonce"] != r_restart["nonce"]
    assert len(sender.cards) == 2

    # Approve ONLY the restart card's nonce.
    mgr.dispatch_gateway_action(f"pa:approve:{r_restart['nonce']}", _clicker())

    # The set command is still blocked; only restart runs.
    assert json.loads(gate.evaluate("terminal", {"command": CMD_SET}, nc))["blocked"] is True
    assert json.loads(gate.evaluate("terminal", {"command": CMD_RESTART}, nc))["exit_code"] == 0
    assert len(nc.calls) == 1


def test_click_from_other_user_rejected(real_manager):
    from gateway import approval_cards
    from plugins.prod_approvals import gate

    mgr, _ = real_manager
    approval_cards.register_card_sender("telegram", _capture_sender())
    nc = _sink()
    r = json.loads(gate.evaluate("terminal", {"command": CMD_SET}, nc))

    # A different user in a different chat taps the button.
    result = mgr.dispatch_gateway_action(
        f"pa:approve:{r['nonce']}", _clicker(chat_id="999", user_id="7")
    )
    assert result is not None and "not authorized" in result.answer_text.lower()
    # Still blocked when re-issued in the real bound context.
    assert json.loads(gate.evaluate("terminal", {"command": CMD_SET}, nc))["blocked"] is True
    assert nc.calls == []


# ---------------------------------------------------------------------------
# 3. Real Telegram callback routing → plugin dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_telegram_callback_query_routes_to_plugin(real_manager, monkeypatch):
    _ensure_telegram_mock()
    from gateway import approval_cards
    from gateway.platforms.telegram import TelegramAdapter
    from gateway.config import PlatformConfig
    from plugins.prod_approvals import gate

    mgr, _ = real_manager
    approval_cards.register_card_sender("telegram", _capture_sender())

    # Create a real pending request whose bound chat/user matches the clicker.
    nc = _sink()
    r = json.loads(gate.evaluate("terminal", {"command": CMD_SET}, nc))
    nonce = r["nonce"]

    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="t", extra={}))
    adapter._bot = AsyncMock()
    adapter._app = MagicMock()
    # Authorize any clicker for this test (gateway-level allow-list is separate).
    monkeypatch.setattr(adapter, "_is_callback_user_authorized", lambda *a, **k: True)
    monkeypatch.setattr(adapter, "resume_typing_for_chat", lambda *a, **k: None)
    monkeypatch.setattr(adapter, "format_message", lambda s: s)

    query = MagicMock()
    query.data = f"pa:approve:{nonce}"
    query.from_user = SimpleNamespace(id=42, first_name="Vito")
    query.message = SimpleNamespace(chat_id=555, message_thread_id=None,
                                    chat=SimpleNamespace(type="private"))
    query.answer = AsyncMock()
    query.edit_message_text = AsyncMock()
    update = SimpleNamespace(callback_query=query)

    await adapter._handle_callback_query(update, None)

    # The button click resolved through the real generic dispatch → approved.
    query.answer.assert_awaited()
    from plugins.prod_approvals.resolve import _open_store
    st = _open_store()
    try:
        assert st.get(nonce).state == "approved"
    finally:
        st.close()
    # And the gate now runs it exactly once.
    assert json.loads(gate.evaluate("terminal", {"command": CMD_SET}, nc))["exit_code"] == 0
    assert len(nc.calls) == 1


# ---------------------------------------------------------------------------
# 4. Telegram send_action_card renders nonce buttons (real adapter method)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_action_card_builds_inline_keyboard():
    _ensure_telegram_mock()
    from gateway.platforms.telegram import TelegramAdapter
    from gateway.config import PlatformConfig
    from gateway.approval_cards import ApprovalCard

    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="t", extra={}))
    msg = MagicMock()
    msg.message_id = 7
    adapter._bot = AsyncMock()
    adapter._bot.send_message = AsyncMock(return_value=msg)

    card = ApprovalCard(
        platform="telegram", chat_id="555", text="approve me",
        buttons=[[("✅ Approve", "pa:approve:abc"), ("❌ Deny", "pa:deny:abc")]],
    )
    res = await adapter.send_action_card(card)
    assert res.success is True
    kwargs = adapter._bot.send_message.call_args[1]
    assert kwargs["chat_id"] == 555
    assert kwargs["reply_markup"] is not None
