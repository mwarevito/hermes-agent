"""A review worker is only handed a skill that actually resolves for its home.

Preloading a missing skill is FATAL at CLI startup, so every ``--skills``
injection has to be gated. This patch has been live and load-bearing for weeks
with no test naming it, which means the version scanner could not see it and an
upgrade would have dropped it silently — the "Jul 1" class. These tests exist so
that stops being true.

The negative half matters as much: answering True for a skill that is not there
turns a spawn into a crash loop, so "not found" must stay not-found.
"""
import pytest

from hermes_cli.kanban_db import _skill_available


def _skill(root, category, name):
    d = root / "skills" / category / name
    d.mkdir(parents=True)
    (d / "SKILL.md").write_text("# %s\n" % name)


def test_skill_in_the_canonical_devops_slot_is_found(tmp_path):
    _skill(tmp_path, "devops", "sdlc-review")
    assert _skill_available(str(tmp_path), "sdlc-review") is True


def test_skill_in_any_other_category_is_still_found(tmp_path):
    """Skills get reorganised; the guard must not depend on one folder."""
    _skill(tmp_path, "productivity", "sdlc-review")
    assert _skill_available(str(tmp_path), "sdlc-review") is True


def test_skill_nested_several_levels_deep_is_found(tmp_path):
    deep = tmp_path / "skills" / "a" / "b" / "c" / "sdlc-review"
    deep.mkdir(parents=True)
    (deep / "SKILL.md").write_text("# deep\n")
    assert _skill_available(str(tmp_path), "sdlc-review") is True


def test_a_missing_skill_is_not_available(tmp_path):
    """The whole point: a false True turns a spawn into a fatal startup."""
    (tmp_path / "skills").mkdir()
    assert _skill_available(str(tmp_path), "sdlc-review") is False


def test_a_directory_without_SKILL_md_does_not_count(tmp_path):
    (tmp_path / "skills" / "devops" / "sdlc-review").mkdir(parents=True)
    assert _skill_available(str(tmp_path), "sdlc-review") is False


def test_a_home_without_a_skills_dir_is_not_available(tmp_path):
    assert _skill_available(str(tmp_path), "sdlc-review") is False


def test_a_nonexistent_home_is_not_available(tmp_path):
    assert _skill_available(str(tmp_path / "nope"), "sdlc-review") is False


def test_a_similarly_named_skill_is_not_a_match(tmp_path):
    _skill(tmp_path, "devops", "sdlc-review-v2")
    assert _skill_available(str(tmp_path), "sdlc-review") is False


def test_SKILL_md_as_a_directory_does_not_count(tmp_path):
    (tmp_path / "skills" / "devops" / "sdlc-review" / "SKILL.md").mkdir(parents=True)
    assert _skill_available(str(tmp_path), "sdlc-review") is False


def test_none_home_falls_back_to_the_default_hermes_home(tmp_path, monkeypatch):
    """A worker with no explicit HERMES_HOME still resolves against ~/.hermes."""
    monkeypatch.setenv("HOME", str(tmp_path))
    fake_home = tmp_path / ".hermes"
    (fake_home / "skills" / "devops" / "sdlc-review").mkdir(parents=True)
    (fake_home / "skills" / "devops" / "sdlc-review" / "SKILL.md").write_text("# x\n")
    assert _skill_available(None, "sdlc-review") is True


def test_none_home_without_the_skill_is_not_available(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / ".hermes" / "skills").mkdir(parents=True)
    assert _skill_available(None, "sdlc-review") is False
