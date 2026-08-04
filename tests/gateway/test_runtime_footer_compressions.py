"""The runtime footer must surface how many times the session was compacted.

2026-08-03: four compactions in 47 minutes silently discarded 60-75 messages
each; on Telegram there was NO signal at all (the compaction status text is
suppressed by _TELEGRAM_NOISY_STATUS_RE and the repeat warning was CLI-only),
so the user kept talking to a session that had lost the middle of the
conversation.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gateway.runtime_footer import (  # noqa: E402
    build_footer_line,
    format_runtime_footer,
)


def test_compressions_field_rendered_when_nonzero():
    line = format_runtime_footer(
        model="gpt-5.6-sol",
        context_tokens=100,
        context_length=1000,
        provider="openai-codex",
        compression_count=3,
        fields=("model", "context_pct", "compressions"),
    )
    assert "сжато ×3" in line
    assert "10%" in line


def test_compressions_field_hidden_at_zero():
    line = format_runtime_footer(
        model="gpt-5.6-sol",
        context_tokens=100,
        context_length=1000,
        compression_count=0,
        fields=("model", "compressions"),
    )
    assert "сжато" not in line


def test_compressions_absent_from_fields_is_not_rendered():
    line = format_runtime_footer(
        model="gpt-5.6-sol",
        context_tokens=100,
        context_length=1000,
        compression_count=5,
        fields=("model", "context_pct"),
    )
    assert "сжато" not in line


def test_default_fields_unchanged_for_other_profiles():
    """Other bots must not suddenly grow a new footer field."""
    line = format_runtime_footer(
        model="gpt-5.5",
        context_tokens=50,
        context_length=100,
        compression_count=9,
    )
    assert "сжато" not in line


def test_build_footer_line_passes_compression_count_through():
    cfg = {
        "display": {
            "runtime_footer": {
                "enabled": True,
                "fields": ["model", "compressions"],
            }
        }
    }
    line = build_footer_line(
        user_config=cfg,
        platform_key="telegram",
        model="gpt-5.6-sol",
        context_tokens=10,
        context_length=100,
        compression_count=2,
    )
    assert "сжато ×2" in line


def test_build_footer_line_defaults_to_zero_when_omitted():
    cfg = {
        "display": {
            "runtime_footer": {"enabled": True, "fields": ["model", "compressions"]}
        }
    }
    line = build_footer_line(
        user_config=cfg,
        platform_key="telegram",
        model="gpt-5.6-sol",
        context_tokens=10,
        context_length=100,
    )
    assert "сжато" not in line
    assert line, "the footer still renders its other fields"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
