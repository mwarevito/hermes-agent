"""Tests for the composable ``transform_llm_output`` plugin hook."""
from pathlib import Path

import yaml

import hermes_cli.plugins as plugins_mod
from hermes_cli.plugins import PluginManager, VALID_HOOKS


def _make_enabled_plugin(hermes_home: Path, name: str, register_body: str) -> Path:
    plugin_dir = hermes_home / "plugins" / name
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.yaml").write_text(
        yaml.safe_dump({"name": name, "version": "0.1.0"}), encoding="utf-8",
    )
    (plugin_dir / "__init__.py").write_text(
        "def register(ctx):\n" f"    {register_body}\n", encoding="utf-8",
    )
    cfg_path = hermes_home / "config.yaml"
    cfg = yaml.safe_load(cfg_path.read_text()) or {} if cfg_path.exists() else {}
    cfg.setdefault("plugins", {}).setdefault("enabled", []).append(name)
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return plugin_dir


def test_transform_llm_output_in_valid_hooks():
    assert "transform_llm_output" in VALID_HOOKS


def test_hook_receives_expected_kwargs(tmp_path, monkeypatch):
    hermes_home = tmp_path / "hermes_test"
    hermes_home.mkdir(exist_ok=True)
    _make_enabled_plugin(
        hermes_home,
        "capture_hook",
        register_body=(
            'ctx.register_hook("transform_llm_output", '
            'lambda **kw: f"{kw[\'response_text\']}|{kw[\'session_id\']}|'
            '{kw[\'model\']}|{kw[\'platform\']}")'
        ),
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    mgr = PluginManager()
    mgr.discover_and_load()

    final, changed = mgr.transform_llm_output(
        "hello world", session_id="s1", model="m", platform="cli"
    )
    assert changed is True
    assert final == "hello world|s1|m|cli"


def test_multiple_transforms_compose_in_discovery_order(tmp_path, monkeypatch):
    hermes_home = tmp_path / "hermes_test"
    hermes_home.mkdir(exist_ok=True)
    _make_enabled_plugin(
        hermes_home,
        "a_first",
        'ctx.register_hook("transform_llm_output", lambda **kw: kw["response_text"] + "|first")',
    )
    _make_enabled_plugin(
        hermes_home,
        "b_second",
        'ctx.register_hook("transform_llm_output", lambda **kw: kw["response_text"] + "|second")',
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    mgr = PluginManager()
    mgr.discover_and_load()

    final, changed = mgr.transform_llm_output(
        "original", session_id="s", model="m", platform="cli"
    )
    assert changed is True
    assert final == "original|first|second"


def test_none_empty_and_non_string_are_pass_through(tmp_path, monkeypatch):
    hermes_home = tmp_path / "hermes_test"
    hermes_home.mkdir(exist_ok=True)
    _make_enabled_plugin(
        hermes_home,
        "a_none",
        'ctx.register_hook("transform_llm_output", lambda **kw: None)',
    )
    _make_enabled_plugin(
        hermes_home,
        "b_empty",
        'ctx.register_hook("transform_llm_output", lambda **kw: "")',
    )
    _make_enabled_plugin(
        hermes_home,
        "c_dict",
        'ctx.register_hook("transform_llm_output", lambda **kw: {"bad": True})',
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    mgr = PluginManager()
    mgr.discover_and_load()

    final, changed = mgr.transform_llm_output("original", session_id="s")
    assert changed is False
    assert final == "original"


def test_hook_exception_does_not_break_later_transform(tmp_path, monkeypatch):
    hermes_home = tmp_path / "hermes_test"
    hermes_home.mkdir(exist_ok=True)
    _make_enabled_plugin(
        hermes_home,
        "a_raising",
        register_body=(
            'def _boom(**kw):\n'
            '        raise RuntimeError("boom")\n'
            '    ctx.register_hook("transform_llm_output", _boom)'
        ),
    )
    _make_enabled_plugin(
        hermes_home,
        "b_after",
        'ctx.register_hook("transform_llm_output", lambda **kw: kw["response_text"] + "|after")',
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    mgr = PluginManager()
    mgr.discover_and_load()

    final, changed = mgr.transform_llm_output("keep me", session_id="s")
    assert changed is True
    assert final == "keep me|after"


def test_no_plugins_leaves_response_unchanged(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes_empty"))
    plugins_mod._plugin_manager = PluginManager()
    final, changed = plugins_mod._plugin_manager.transform_llm_output("unchanged")
    assert changed is False
    assert final == "unchanged"
