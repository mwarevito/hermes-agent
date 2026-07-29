"""Batch-4 D4 — the max-iterations CHECKPOINT prompt is for the ROOT kanban
worker only. A delegated subagent (plan critic) inherits HERMES_KANBAN_TASK via
os.environ but must get the plain summary, not a resumable worker checkpoint.
"""
from __future__ import annotations

from agent.chat_completion_helpers import _max_iterations_summary_request


class _FakeAgent:
    def __init__(self, parent=None):
        self._parent_session_id = parent


def test_root_worker_gets_checkpoint(monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_x")
    req = _max_iterations_summary_request(_FakeAgent(parent=None))
    assert "CHECKPOINT" in req
    assert "NEXT STEP" in req


def test_subagent_gets_plain_summary(monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_x")
    req = _max_iterations_summary_request(_FakeAgent(parent="worker_sess_123"))
    assert "CHECKPOINT" not in req
    assert "summarizing" in req


def test_non_worker_gets_plain_summary(monkeypatch):
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    req = _max_iterations_summary_request(_FakeAgent(parent=None))
    assert "CHECKPOINT" not in req


def test_agent_without_parent_attr_defaults_to_root(monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_x")

    class _Bare:
        pass

    req = _max_iterations_summary_request(_Bare())
    assert "CHECKPOINT" in req
