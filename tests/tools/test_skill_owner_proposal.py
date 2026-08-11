"""Autonomous curation of a USER-OWNED skill must become a proposal, not a hole.

Field evidence (personal bot, 2026-08-08 → 2026-08-11): 47 background-review
writes were refused in 3.2 days across 10 skills with

    "the skill is not curator-managed (created_by=None).
     User-owned skills are off-limits to autonomous curation."

and the user never learned any of it happened — ``summarize_background_review_actions``
only walked SUCCESSFUL tool results, so three days of "the bot cannot improve
anything" rendered as silence. Three separate defects, one per test class here:

1. the ownership preflight ran BEFORE the staging gate, so the staging
   machinery that already exists in ``skill_manage`` was unreachable;
2. refusals never reached the user-facing review summary;
3. ``skills_list`` exposed no ``curator_managed`` flag, so neither the agent
   nor the user could tell which skills autonomous curation may touch.
"""

import json
from contextlib import contextmanager
from unittest.mock import patch

from tools.skill_manager_tool import _create_skill, skill_manage
from tools.skills_tool import skills_list


def _summarize(*args, **kwargs):
    # Imported lazily: pulling agent.background_review in at collection time
    # perturbs the module-import graph that tests/tools/test_tool_search.py
    # snapshots, and two of its cases go red in a whole-directory run
    # (measured 2026-08-11: 26 -> 28 failures, both in test_tool_search).
    from agent.background_review import summarize_background_review_actions

    return summarize_background_review_actions(*args, **kwargs)


VALID_SKILL_CONTENT = """\
---
name: test-skill
description: A test skill for unit testing.
---

# Test Skill

Step 1: Do the thing.
"""


@contextmanager
def _skill_dir(tmp_path):
    with patch("tools.skill_manager_tool.SKILLS_DIR", tmp_path), \
         patch("agent.skill_utils.get_all_skills_dirs", return_value=[tmp_path]):
        yield


def _bg(skills_root, **kwargs):
    """Run one skill_manage call as the autonomous background review fork."""
    from tools.skill_manager_tool import mark_background_review_skill_read
    from tools.skill_provenance import (
        BACKGROUND_REVIEW,
        reset_current_write_origin,
        set_current_write_origin,
    )

    token = set_current_write_origin(BACKGROUND_REVIEW)
    try:
        mark_background_review_skill_read(
            skills_root / kwargs["name"] / "SKILL.md"
        )
        return json.loads(skill_manage(**kwargs))
    finally:
        reset_current_write_origin(token)


def _make_skill(skills_dir, name):
    d = skills_dir / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: Description for {name}.\n---\n\n"
        f"# {name}\n\nStep 1: Do the thing.\n",
        encoding="utf-8",
    )
    return d


# ---------------------------------------------------------------------------
# 1. Stage the proposal instead of dropping the improvement
# ---------------------------------------------------------------------------


class TestUserOwnedSkillWriteIsProposed:

    def test_background_patch_of_user_owned_skill_is_staged(self, tmp_path, monkeypatch):
        """The 47-refusal shape: real ``.usage.json`` (absent → not managed)."""
        from tools import write_approval as wa

        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        name = "llucky-owner-influencer-outreach"
        with _skill_dir(root):
            _create_skill(name, VALID_SKILL_CONTENT)
            res = _bg(
                root, action="patch", name=name,
                old_string="Do the thing.", new_string="Do the better thing.",
            )

        assert res.get("success") is True, res
        assert res.get("staged") is True, res
        assert res.get("pending_id"), res

        # A proposal must not touch the owner's file.
        on_disk = (root / name / "SKILL.md").read_text(encoding="utf-8")
        assert "Do the thing." in on_disk
        assert "Do the better thing." not in on_disk

        pending = wa.list_pending(wa.SKILLS)
        assert len(pending) == 1, pending
        assert pending[0]["payload"]["name"] == name
        assert pending[0]["payload"]["action"] == "patch"
        assert pending[0]["payload"]["new_string"] == "Do the better thing."
        assert pending[0]["origin"] == "background_review"

    def test_repeated_identical_proposal_does_not_pile_up(self, tmp_path, monkeypatch):
        """The review fork retries every pass; one pending record, not N."""
        from tools import write_approval as wa

        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        with _skill_dir(root):
            _create_skill("kanban-operations", VALID_SKILL_CONTENT)
            first = _bg(
                root, action="patch", name="kanban-operations",
                old_string="Do the thing.", new_string="Do the better thing.",
            )
            second = _bg(
                root, action="patch", name="kanban-operations",
                old_string="Do the thing.", new_string="Do the better thing.",
            )

        assert first["staged"] is True and second["staged"] is True
        assert first["pending_id"] == second["pending_id"]
        assert len(wa.list_pending(wa.SKILLS)) == 1

    def test_approved_proposal_applies_to_disk(self, tmp_path, monkeypatch):
        """Approval is the whole point: the staged patch must replay."""
        from tools import write_approval as wa
        from tools.skill_manager_tool import apply_skill_pending

        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        with _skill_dir(root):
            _create_skill("team-bot-operations", VALID_SKILL_CONTENT)
            _bg(
                root, action="patch", name="team-bot-operations",
                old_string="Do the thing.", new_string="Do the better thing.",
            )
            record = wa.list_pending(wa.SKILLS)[0]
            applied = json.loads(apply_skill_pending(record["payload"]))

        assert applied.get("success") is True, applied
        assert "Do the better thing." in (
            root / "team-bot-operations" / "SKILL.md"
        ).read_text(encoding="utf-8")

    def test_pinned_skill_is_still_a_hard_refusal(self, tmp_path, monkeypatch):
        """Pin means "no user consented to this"; it must not become a proposal."""
        from tools import write_approval as wa

        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        def _pinned(skill_name):
            return {"pinned": True}

        with _skill_dir(root):
            _create_skill("pinned-skill", VALID_SKILL_CONTENT)
            with patch("tools.skill_usage.get_record", side_effect=_pinned):
                res = _bg(
                    root, action="patch", name="pinned-skill",
                    old_string="Do the thing.", new_string="Nope.",
                )

        assert res.get("success") is False, res
        assert "pinned" in res["error"].lower()
        assert wa.list_pending(wa.SKILLS) == []

    def test_background_delete_of_user_owned_skill_is_still_refused(self, tmp_path, monkeypatch):
        """Improvements become proposals; autonomous deletion does not."""
        from tools import write_approval as wa

        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        with _skill_dir(root):
            _create_skill("doomed-skill", VALID_SKILL_CONTENT)
            res = _bg(
                root, action="delete", name="doomed-skill",
                absorbed_into="some-umbrella",
            )

        assert res.get("success") is False, res
        assert "curator-managed" in res["error"]
        assert (root / "doomed-skill" / "SKILL.md").exists()
        assert wa.list_pending(wa.SKILLS) == []


# ---------------------------------------------------------------------------
# 2. Report refusals, not only successes
# ---------------------------------------------------------------------------


def _call(tcid, name, action="patch", tool="skill_manage"):
    args = {"action": action, "name": name}
    return {
        "role": "assistant",
        "tool_calls": [{
            "id": tcid,
            "function": {"name": tool, "arguments": json.dumps(args)},
        }],
    }


def _result(tcid, payload):
    return {"role": "tool", "tool_call_id": tcid, "content": json.dumps(payload)}


class TestRefusalsReachTheUser:

    def test_refused_skill_write_is_surfaced(self):
        refusal = {
            "success": False,
            "refusal_class": "ownership",
            "error": (
                "Refusing background curator patch for skill "
                "'kanban-operations': the skill is not curator-managed "
                "(created_by=None). User-owned skills are off-limits to "
                "autonomous curation. Run `hermes curator adopt "
                "kanban-operations` to opt it in."
            ),
        }
        messages = [_call("c1", "kanban-operations"), _result("c1", refusal)]

        actions = _summarize(messages, prior_snapshot=[])

        assert actions, "a refused skill write produced no user-visible action"
        joined = " ".join(actions)
        assert "kanban-operations" in joined
        assert "not saved" in joined.lower()

    def test_staged_proposal_is_surfaced(self):
        staged = {
            "success": True,
            "staged": True,
            "pending_id": "abc123",
            "gist": "sharpen the outreach step",
            "message": "Staged for approval.",
        }
        messages = [
            _call("c1", "llucky-owner-influencer-outreach"),
            _result("c1", staged),
        ]

        actions = _summarize(messages, prior_snapshot=[])

        joined = " ".join(actions)
        assert "llucky-owner-influencer-outreach" in joined
        assert "approval" in joined.lower()

    def test_successful_write_summary_unchanged(self):
        ok = {"success": True, "message": "Skill 'x' updated."}
        messages = [_call("c1", "x", action="edit"), _result("c1", ok)]

        actions = _summarize(messages, prior_snapshot=[])

        assert actions == ["Skill 'x' updated."]

    def test_notification_mode_off_still_silent(self):
        refusal = {"success": False, "error": "Refusing background curator patch"}
        messages = [_call("c1", "x"), _result("c1", refusal)]

        assert _summarize(
            messages, prior_snapshot=[], notification_mode="off"
        ) == []


# ---------------------------------------------------------------------------
# 3. skills_list must say which skills autonomous curation may touch
# ---------------------------------------------------------------------------


class TestSkillsListCuratorManagedFlag:

    def test_flag_distinguishes_managed_from_user_owned(self, tmp_path, monkeypatch):
        from tools.skill_usage import mark_agent_created

        home = tmp_path / ".hermes"
        skills = home / "skills"
        skills.mkdir(parents=True)
        monkeypatch.setenv("HERMES_HOME", str(home))

        _make_skill(skills, "agent-owned")
        _make_skill(skills, "user-owned")
        mark_agent_created("agent-owned")

        data = json.loads(skills_list())
        by_name = {s["name"]: s for s in data["skills"]}

        assert by_name["agent-owned"]["curator_managed"] is True
        assert by_name["user-owned"]["curator_managed"] is False

    def test_unreadable_usage_file_yields_explicit_unknown(self, tmp_path, monkeypatch):
        """A missing key and False are indistinguishable to every reader.

        Swallowing the failure marked every skill user-owned by omission, so
        the degraded listing has to say ``None`` rather than say nothing.
        """
        home = tmp_path / ".hermes"
        skills = home / "skills"
        skills.mkdir(parents=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        _make_skill(skills, "some-skill")

        with patch("tools.skill_usage.load_usage", side_effect=OSError("boom")):
            data = json.loads(skills_list())

        by_name = {s["name"]: s for s in data["skills"]}
        assert "curator_managed" in by_name["some-skill"], by_name["some-skill"]
        assert by_name["some-skill"]["curator_managed"] is None


# ---------------------------------------------------------------------------
# 4. Only the refusals the USER can resolve are allowed into the chat
# ---------------------------------------------------------------------------


def _mem_call(tcid, action="add", target="memory"):
    return {
        "role": "assistant",
        "tool_calls": [{
            "id": tcid,
            "function": {
                "name": "memory",
                "arguments": json.dumps(
                    {"action": action, "target": target, "content": "x"}
                ),
            },
        }],
    }


class TestOnlyOwnershipRefusalsAreAnnounced:
    """Narrowed 2026-08-11 after adversarial review of the same day's fix.

    Announcing EVERY refusal is not a smaller version of announcing the right
    ones — it is a different, worse behaviour. Profile ``kivi`` (Gogi, the
    sales bot sitting in chats with Llucky CLIENTS) has no
    ``display.memory_notifications`` key and therefore takes the "on" default
    (hermes_cli/config_defaults.py, gateway/run.py), so a prospect would have
    read "Skill 'apple-notes' patch not saved: Refusing background curator
    patch for bundled skill" and, with a full store, a "Memory is full" line
    after every single turn.

    Ownership is the one class a user can actually resolve
    (``hermes curator adopt <name>``); the rest are the system working as
    designed. Classification is structural — ``refusal_class`` set where the
    refusal is built — never a grep over the error prose.
    """

    def test_ownership_refusal_is_announced(self):
        refusal = {
            "success": False,
            "refusal_class": "ownership",
            "error": (
                "Refusing background curator patch for skill 'kanban-operations': "
                "the skill is not curator-managed (created_by=None)."
            ),
        }
        actions = _summarize(
            [_call("c1", "kanban-operations"), _result("c1", refusal)],
            prior_snapshot=[],
        )
        assert any("kanban-operations" in a for a in actions), actions

    def test_bundled_refusal_is_silent(self):
        refusal = {
            "success": False,
            "refusal_class": "bundled",
            "error": (
                "Refusing background curator patch for bundled skill 'apple-notes'."
            ),
        }
        actions = _summarize(
            [_call("c1", "apple-notes"), _result("c1", refusal)],
            prior_snapshot=[],
        )
        assert actions == [], actions

    def test_pinned_refusal_is_silent(self):
        refusal = {
            "success": False,
            "refusal_class": "pinned",
            "error": (
                "Refusing background curator patch for pinned skill 'x': "
                "pinned skills are off-limits to autonomous maintenance."
            ),
        }
        actions = _summarize(
            [_call("c1", "x"), _result("c1", refusal)], prior_snapshot=[]
        )
        assert actions == [], actions

    def test_read_before_write_refusal_is_silent(self):
        refusal = {
            "success": False,
            "refusal_class": "read-before-write",
            "_read_before_write_required": True,
            "error": (
                "Refusing background curator patch for skill 'x': the current "
                "SKILL.md content has not been loaded in this review turn."
            ),
        }
        actions = _summarize(
            [_call("c1", "x"), _result("c1", refusal)], prior_snapshot=[]
        )
        assert actions == [], actions

    def test_memory_tool_error_is_silent(self):
        """The every-turn case: a full store answers success=False forever."""
        refusal = {"success": False, "error": "Memory is full (2200 char limit)."}
        actions = _summarize(
            [_mem_call("c1"), _result("c1", refusal)], prior_snapshot=[]
        )
        assert actions == [], actions

    def test_unclassified_refusal_is_silent(self):
        """Default direction for any refusal added later without a class."""
        refusal = {"success": False, "error": "Something went sideways."}
        actions = _summarize(
            [_call("c1", "x"), _result("c1", refusal)], prior_snapshot=[]
        )
        assert actions == [], actions

    def test_ownership_refusal_without_call_arguments_is_still_announced(self):
        """A tool result with no tool_call_id has no recoverable arguments.

        The first version guarded the line with ``if _what and reason`` and
        dropped exactly this case in silence.
        """
        refusal = {
            "success": False,
            "refusal_class": "ownership",
            "error": (
                "Refusing background curator patch for skill 'orphaned': "
                "the skill is not curator-managed (no usage record)."
            ),
        }
        actions = _summarize(
            [{"role": "tool", "content": json.dumps(refusal)}], prior_snapshot=[]
        )
        assert len(actions) == 1, actions
        assert "not saved" in actions[0].lower()
        assert "orphaned" in actions[0]

    def test_real_ownership_refusal_is_classified_and_carries_no_private_key(
        self, tmp_path, monkeypatch
    ):
        """End-to-end: the wire contract between skill_manage and the summary.

        Asserted against the real tool so renaming the field in one file
        cannot silently disconnect the two.
        """
        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        with _skill_dir(root):
            _create_skill("doomed-skill", VALID_SKILL_CONTENT)
            res = _bg(
                root, action="delete", name="doomed-skill",
                absorbed_into="some-umbrella",
            )

        assert res["success"] is False, res
        assert res.get("refusal_class") == "ownership", res
        assert "_stage_for_owner" not in res, res

        actions = _summarize(
            [_call("c1", "doomed-skill", action="delete"), _result("c1", res)],
            prior_snapshot=[],
        )
        assert any("doomed-skill" in a for a in actions), actions

    def test_real_bundled_refusal_is_classified_and_stays_silent(
        self, tmp_path, monkeypatch
    ):
        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        with _skill_dir(root):
            _create_skill("apple-notes", VALID_SKILL_CONTENT)
            with patch("tools.skill_usage.is_bundled", return_value=True):
                res = _bg(
                    root, action="patch", name="apple-notes",
                    old_string="Do the thing.", new_string="Nope.",
                )

        assert res["success"] is False, res
        assert res.get("refusal_class") == "bundled", res
        assert _summarize(
            [_call("c1", "apple-notes"), _result("c1", res)], prior_snapshot=[]
        ) == []

    def test_staged_proposal_carries_no_private_routing_key(
        self, tmp_path, monkeypatch
    ):
        home = tmp_path / ".hermes"
        (home / "skills").mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        root = tmp_path / "skills-root"
        root.mkdir()

        with _skill_dir(root):
            _create_skill("proposed-skill", VALID_SKILL_CONTENT)
            res = _bg(
                root, action="patch", name="proposed-skill",
                old_string="Do the thing.", new_string="Do the better thing.",
            )

        assert res.get("staged") is True, res
        assert "_stage_for_owner" not in res, res
