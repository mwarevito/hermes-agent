"""Batch-4 D2 — kanban artifact egress is limited to the explicit artifacts list.

Regression for the Codex-#1 egress blind spot: a worker that merely *mentions* a
file path in its completion summary / result must NOT have that file auto-
uploaded to the card's Telegram subscribers (which include the clinic/patient
bots). Only ``kanban_complete(artifacts=[...])`` ships a file.
"""
from __future__ import annotations

from gateway.kanban_watchers import _artifact_paths_from_payload


def test_explicit_artifacts_are_delivered():
    assert _artifact_paths_from_payload(
        {"artifacts": ["/a/b.pdf", "/c/d.png"]}
    ) == ["/a/b.pdf", "/c/d.png"]


def test_summary_paths_are_not_egress():
    # THE incident: a path named in prose must never auto-upload.
    assert _artifact_paths_from_payload(
        {"summary": "done — wrote /home/user/secret_report.pdf"}
    ) == []


def test_result_is_not_a_source():
    assert _artifact_paths_from_payload({"result": "/x/y.txt"}) == []


def test_non_dict_payload_is_empty():
    assert _artifact_paths_from_payload(None) == []
    assert _artifact_paths_from_payload("nope") == []
    assert _artifact_paths_from_payload([]) == []


def test_non_string_artifacts_skipped():
    assert _artifact_paths_from_payload(
        {"artifacts": ["/a.pdf", 123, None, ""]}
    ) == ["/a.pdf"]


def test_artifacts_and_summary_only_uses_artifacts():
    # Both present -> only the explicit list ships; the summary path is ignored.
    assert _artifact_paths_from_payload(
        {"artifacts": ["/deliverable.pdf"], "summary": "also see /tmp/input.csv"}
    ) == ["/deliverable.pdf"]
