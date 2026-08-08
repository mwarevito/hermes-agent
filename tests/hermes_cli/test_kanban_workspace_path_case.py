"""A repo root must equal itself even when the case differs.

2026-08-08: task t_923aa7f6 refused to start twice with "not inside a git repo"
for a path that WAS a git repo. The two spellings differed only in case
(``/Users/tony/Coding/...`` vs ``/Users/tony/coding/...``). macOS folds case, so
both name one directory — but ``Path.resolve()`` keeps the case it was handed and
``Path.__eq__`` compares strings, so the repo root failed to equal itself. A whole
plan was lost and the task chain had to be recreated as "(corrected)".

``_same_dir`` asks the filesystem instead of the string. These tests pin the
identity, the negative case, and the behaviour when a path does not exist — where
there is no inode to compare and the answer must degrade rather than crash.
"""
import os
import pathlib
import sys

import pytest

from hermes_cli.kanban_db import _same_dir


def test_a_directory_is_the_same_as_itself(tmp_path):
    assert _same_dir(tmp_path, tmp_path)


def test_two_different_directories_are_not_the_same(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    assert not _same_dir(a, b)


def test_a_symlink_to_a_repo_is_the_same_directory(tmp_path):
    """The same authority that folds case also resolves symlinks."""
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    assert _same_dir(real, link)


def test_case_difference_on_a_case_insensitive_filesystem(tmp_path):
    """The incident's exact shape. Skipped where case actually distinguishes."""
    lower = tmp_path / "coding"
    lower.mkdir()
    upper = tmp_path / "Coding"
    if not upper.exists():
        pytest.skip("case-sensitive filesystem: the two paths are genuinely different")
    assert _same_dir(lower, upper), (
        "the same directory spelled with different case must compare equal — "
        "this is what made a repo root fail to be its own repo root"
    )


def test_a_missing_path_falls_back_to_case_folded_comparison(tmp_path):
    """No inode to compare: answer from the string, folding case where the OS does."""
    a = tmp_path / "gone" / "coding"
    b = tmp_path / "gone" / "coding"
    assert _same_dir(a, b)


def test_missing_paths_that_differ_are_still_not_the_same(tmp_path):
    assert not _same_dir(tmp_path / "gone" / "x", tmp_path / "gone" / "y")


def test_none_is_never_the_same_as_anything(tmp_path):
    assert not _same_dir(None, tmp_path)
    assert not _same_dir(tmp_path, None)
    assert not _same_dir(None, None)


def test_case_folded_fallback_matches_the_platform(tmp_path):
    """On a case-folding platform the missing-path fallback must fold too."""
    a = tmp_path / "gone" / "Coding"
    b = tmp_path / "gone" / "coding"
    expected = os.path.normcase(str(a)) == os.path.normcase(str(b))
    assert _same_dir(a, b) is expected
