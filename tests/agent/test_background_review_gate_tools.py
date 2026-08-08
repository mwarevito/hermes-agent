"""The background review must never be able to deadlock against a tool gate.

2026-08-08: five times in one day a background review refused a tool with
"classify this turn first" while refusing ``novel_task_classify`` itself as
non-whitelisted. The turn could neither proceed nor satisfy the gate.

The fix whitelists the ceremony tools of any plugin that registers a
``pre_tool_call`` hook — the class of "gate" plugins — rather than naming one
plugin. These tests pin the CLASS behaviour, so a future gate plugin inherits it
and a renamed tool does not silently fall out of the whitelist.
"""

from types import SimpleNamespace

import pytest

from agent.background_review import _gate_ceremony_tool_names


class _Manager:
    def __init__(self, plugins):
        self._plugins = plugins


def _plugin(*, enabled=True, hooks=(), tools=()):
    return SimpleNamespace(
        enabled=enabled,
        hooks_registered=list(hooks),
        tools_registered=list(tools),
    )


@pytest.fixture
def patch_manager(monkeypatch):
    def _install(plugins):
        import hermes_cli.plugins as plugins_mod

        monkeypatch.setattr(
            plugins_mod, "get_plugin_manager", lambda: _Manager(plugins), raising=False
        )

    return _install


def test_gate_plugin_ceremony_tools_are_collected(patch_manager):
    patch_manager(
        {
            "novel-task-gate": _plugin(
                hooks=["pre_tool_call", "post_tool_call"],
                tools=[
                    "novel_task_gate_status",
                    "novel_task_classify",
                    "novel_task_register_plan",
                    "novel_task_resolve_plan",
                ],
            )
        }
    )
    assert _gate_ceremony_tool_names() == {
        "novel_task_gate_status",
        "novel_task_classify",
        "novel_task_register_plan",
        "novel_task_resolve_plan",
    }


def test_non_gate_plugin_tools_are_not_whitelisted(patch_manager):
    """A plugin that cannot refuse a tool cannot deadlock one — it gets nothing.

    This is the half that keeps the fix from becoming "whitelist every plugin".
    """
    patch_manager(
        {
            "instantly": _plugin(
                hooks=["post_tool_call"],
                tools=["instantly_send_campaign"],
            )
        }
    )
    assert _gate_ceremony_tool_names() == set()


def test_disabled_gate_grants_nothing(patch_manager):
    patch_manager(
        {
            "novel-task-gate": _plugin(
                enabled=False,
                hooks=["pre_tool_call"],
                tools=["novel_task_classify"],
            )
        }
    )
    assert _gate_ceremony_tool_names() == set()


def test_declared_but_unregistered_hook_grants_nothing(patch_manager):
    """The manifest is a claim; runtime registration is the fact.

    A plugin whose plugin.yaml lists pre_tool_call but which never registered it
    is not a gate and must not widen the review's whitelist.
    """
    patch_manager(
        {
            "claims-a-hook": _plugin(
                hooks=[],  # nothing actually registered
                tools=["something_powerful"],
            )
        }
    )
    assert _gate_ceremony_tool_names() == set()


def test_multiple_gates_are_unioned(patch_manager):
    patch_manager(
        {
            "novel-task-gate": _plugin(
                hooks=["pre_tool_call"], tools=["novel_task_classify"]
            ),
            "verify-before-done": _plugin(
                hooks=["pre_tool_call"], tools=["verify_claim"]
            ),
        }
    )
    assert _gate_ceremony_tool_names() == {"novel_task_classify", "verify_claim"}


def test_registry_failure_is_closed_not_open(monkeypatch):
    """An unreadable registry must not open the whitelist.

    Failing open here would silently hand a background review every plugin tool
    on any import hiccup — the opposite of what a whitelist is for.
    """
    import hermes_cli.plugins as plugins_mod

    def _boom():
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(plugins_mod, "get_plugin_manager", _boom, raising=False)
    assert _gate_ceremony_tool_names() == set()


def test_missing_attributes_do_not_raise(patch_manager):
    """Old/foreign plugin objects lacking the fields must degrade, not crash."""
    patch_manager({"ancient": SimpleNamespace(enabled=True)})
    assert _gate_ceremony_tool_names() == set()
