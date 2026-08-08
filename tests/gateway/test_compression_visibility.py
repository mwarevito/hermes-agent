"""The reader must be able to see that the session is running on a summary.

2026-08-03: each compaction discards 60-75 messages, and nothing said so. A
conversation that has silently lost its middle looks exactly like one that has
not — which is how "бот теряет середину разговора" gets diagnosed as the model
being stupid instead of the history being gone.

The footer now carries ``сжато ×N``. The silent case is pinned just as hard: a
session that has never compacted must show nothing, or the indicator becomes
noise and stops being read.

No test named this patch until now, so the version scanner could not see it and
an upgrade would have dropped it silently.
"""
import pytest

from gateway.runtime_footer import format_runtime_footer


def _footer(**kw):
    base = dict(
        model="gpt-5.5",
        context_tokens=1000,
        context_length=100000,
        fields=("compressions",),
    )
    base.update(kw)
    return format_runtime_footer(**base)


def test_a_compacted_session_says_so():
    assert "сжато ×3" in _footer(compression_count=3)


def test_a_single_compaction_is_still_announced():
    """The first compaction is the one that surprises people."""
    assert "сжато ×1" in _footer(compression_count=1)


def test_an_uncompacted_session_shows_nothing():
    """Silence is the default; an always-on counter would stop being read."""
    assert _footer(compression_count=0) == ""


def test_a_missing_count_shows_nothing():
    assert _footer() == ""


def test_a_negative_count_is_not_rendered():
    """Defensive: a bad counter must not print 'сжато ×-1'."""
    assert _footer(compression_count=-1) == ""


def test_the_indicator_coexists_with_other_fields():
    out = format_runtime_footer(
        model="gpt-5.5",
        context_tokens=50_000,
        context_length=100_000,
        compression_count=2,
        fields=("model", "compressions"),
    )
    assert "gpt-5.5" in out and "сжато ×2" in out


def test_the_field_is_opt_in_by_name():
    """A footer configured without the field must not grow one."""
    out = format_runtime_footer(
        model="gpt-5.5",
        context_tokens=1000,
        context_length=100000,
        compression_count=5,
        fields=("model",),
    )
    assert "сжато" not in out
