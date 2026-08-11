"""Wall-clock budget for one LOGICAL provider call (plan item 7, 2026-08-11).

Every timeout in the stack is a per-ATTEMPT limit, and the retry loop
multiplies it: with the 1500s absolute stream ceiling and the default three
attempts, one logical call could legitimately occupy 75 minutes while the
session reported itself healthy the whole time. Nothing measured the logical
call as a whole.

``HERMES_API_CALL_TOTAL_BUDGET_SECONDS`` bounds it, deliberately as a
*don't-start-another-attempt* gate rather than a kill: an in-flight attempt is
never interrupted by it, so a healthy slow generation cannot be chopped in
half by a budget the operator forgot about. Only the multiplication is bounded.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from run_agent import AIAgent


def _tool_defs(*names):
    return [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": "test tool",
                "parameters": {"type": "object", "properties": {}},
            },
        }
        for name in names
    ]


def _response(*, content, finish_reason="stop"):
    message = SimpleNamespace(content=content, tool_calls=None)
    choice = SimpleNamespace(message=message, finish_reason=finish_reason)
    return SimpleNamespace(choices=[choice], model="test/model", usage=None)


def _make_agent():
    with (
        patch("run_agent.get_tool_definitions", return_value=_tool_defs("web_search")),
        patch("run_agent.check_toolset_requirements", return_value={}),
        patch("run_agent.OpenAI"),
    ):
        agent = AIAgent(
            api_key="test-key",
            base_url="https://openrouter.ai/api/v1/",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
        )
    agent._cached_system_prompt = "You are helpful."
    agent._use_prompt_caching = False
    agent.compression_enabled = False
    agent.save_trajectories = False
    agent.valid_tool_names = {"web_search"}
    agent.client = MagicMock()
    return agent


def _run(agent, prompt="do the task"):
    with (
        patch("run_agent.handle_function_call", return_value="ok"),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        return agent.run_conversation(prompt)


def test_budget_stops_the_retry_multiplication(monkeypatch):
    """Once the logical call has burned its budget, no further attempt starts.

    Without the budget the loop spends max_retries × per-attempt-timeout on a
    provider that is already proven slow-and-failing.
    """
    monkeypatch.setenv("HERMES_API_CALL_TOTAL_BUDGET_SECONDS", "1")

    agent = _make_agent()
    agent._api_max_retries = 3

    attempts = {"n": 0}

    def _slow_failure(*args, **kwargs):
        attempts["n"] += 1
        time.sleep(0.6)
        raise ConnectionError("connection reset by peer")

    agent.client.chat.completions.create.side_effect = _slow_failure

    result = _run(agent)

    assert attempts["n"] == 1, (
        f"budget ignored — provider was called {attempts['n']} times"
    )
    assert result.get("failed") is True, result
    assert "budget" in str(result.get("error", "")).lower(), result.get("error")


def test_budget_does_not_preempt_a_healthy_first_attempt(monkeypatch):
    """A single slow-but-successful attempt over budget still returns.

    The budget gates the START of a retry, never an attempt already running —
    a budget that could truncate a healthy long generation would be worse than
    the multiplication it prevents.
    """
    monkeypatch.setenv("HERMES_API_CALL_TOTAL_BUDGET_SECONDS", "0.2")

    agent = _make_agent()
    agent._api_max_retries = 3

    def _slow_success(*args, **kwargs):
        time.sleep(0.6)
        return _response(content="Finished the slow work.")

    agent.client.chat.completions.create.side_effect = _slow_success

    result = _run(agent)
    assert result["final_response"] == "Finished the slow work."


def test_budget_can_be_disabled(monkeypatch):
    """``0`` restores the pre-budget behaviour: retries run to max_retries."""
    monkeypatch.setenv("HERMES_API_CALL_TOTAL_BUDGET_SECONDS", "0")

    agent = _make_agent()
    agent._api_max_retries = 2

    attempts = {"n": 0}

    def _fail_then_succeed(*args, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            time.sleep(0.6)
            raise ConnectionError("connection reset by peer")
        return _response(content="Recovered on retry.")

    agent.client.chat.completions.create.side_effect = _fail_then_succeed

    result = _run(agent)
    assert attempts["n"] == 2
    assert result["final_response"] == "Recovered on retry."


def test_shipped_default_budget_is_finite_and_generous():
    """The default must be ON, and above one full per-attempt ceiling.

    An env-only default can be zeroed in a diff with nothing failing. It also
    has to exceed the 1500s stream ceiling, otherwise it would forbid retrying
    after a single attempt that legitimately used its whole ceiling.
    """
    from agent import conversation_loop as cl
    from agent import chat_completion_helpers as h

    default = cl._API_CALL_TOTAL_BUDGET_DEFAULT_SECONDS
    assert default > 0, "the logical-call budget is disabled by default"
    assert default != float("inf")
    assert default > h._STREAM_HARD_TIMEOUT_DEFAULT_SECONDS, (
        "budget below one per-attempt ceiling would forbid every retry"
    )
