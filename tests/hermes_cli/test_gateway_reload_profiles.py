"""Tests for the sanctioned ``hermes gateway reload-profiles`` lifecycle verb.

Covered invariants:

* **Authorization** — refuses unless the active profile is the default/root
  Hermes home.
* **Allowlist** — empty by default (no-op); only the intersection of
  ``gateway.reload_profiles`` with actually-registered profiles is touched.
* **Ordering** — foreign profiles first, the current default gateway LAST.
* **Drain-only** — foreign profiles go through the drain-aware SIGUSR1 helper
  and the default through the sanctioned self-restart path; no
  kill/stop/pkill/launchctl-bootout primitive is ever invoked.
* **Parser / dispatch** — the verb parses and dispatches to the handler.
* **Real main wiring** — the parser is exercised with the *exact* keyword
  arguments ``hermes_cli.main`` passes (``cmd_gateway``, ``cmd_proxy``,
  ``cmd_gateway_enroll``), guarding the incident where a stale parser
  signature dropped ``cmd_gateway_enroll`` and crashed every gateway CLI.
* **Approval regression** — ``reload-profiles`` is NOT matched by the
  dangerous-command guard while ``stop``/``restart`` still are.
"""

import argparse
import inspect
import os
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import pytest

import hermes_cli.gateway as gateway
from hermes_cli.subcommands.gateway import build_gateway_parser


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _fake_profiles(names):
    return [SimpleNamespace(name=n) for n in names]


def _fake_running(mapping):
    """mapping: {profile_name: pid} -> list of ProfileGatewayProcess-likes."""
    return [
        SimpleNamespace(profile=name, path=None, pid=pid)
        for name, pid in mapping.items()
    ]


@pytest.fixture
def recorder(monkeypatch):
    """Wire the reload path so signalling is recorded, never really sent.

    Returns a dict with ``sigusr1`` (foreign drain calls, ordered) and
    ``self`` (default self-restart calls).  Any accidental use of a
    destructive primitive raises immediately.
    """
    calls = {"sigusr1": [], "self": [], "drain_timeout": []}

    def fake_graceful(pid, drain_timeout):
        calls["sigusr1"].append(pid)
        calls["drain_timeout"].append(drain_timeout)
        return True

    def fake_self(pid):
        calls["self"].append(pid)
        return True

    monkeypatch.setattr(gateway, "_graceful_restart_via_sigusr1", fake_graceful)
    monkeypatch.setattr(gateway, "_request_gateway_self_restart", fake_self)
    monkeypatch.setattr(gateway, "_get_restart_drain_timeout", lambda: 60.0)

    # Any destructive primitive must never be reached by this path.
    def _boom(name):
        def _fn(*a, **k):  # pragma: no cover - only hit on regression
            raise AssertionError(f"reload-profiles must not call {name}")
        return _fn

    monkeypatch.setattr(gateway, "kill_gateway_processes", _boom("kill_gateway_processes"))
    monkeypatch.setattr(gateway, "stop_profile_gateway", _boom("stop_profile_gateway"))
    monkeypatch.setattr(gateway, "systemd_stop", _boom("systemd_stop"))
    monkeypatch.setattr(gateway, "launchd_stop", _boom("launchd_stop"))
    return calls


def _set_active(monkeypatch, name):
    monkeypatch.setattr(
        "hermes_cli.profiles.get_active_profile_name", lambda: name
    )


def _set_registered(monkeypatch, names):
    monkeypatch.setattr(
        "hermes_cli.profiles.list_profiles", lambda: _fake_profiles(names)
    )


def _set_config(monkeypatch, reload_profiles):
    cfg = {}
    if reload_profiles is not None:
        cfg = {"gateway": {"reload_profiles": reload_profiles}}
    monkeypatch.setattr(gateway, "read_raw_config", lambda: cfg)


def _set_running(monkeypatch, mapping):
    monkeypatch.setattr(
        gateway, "find_profile_gateway_processes", lambda: _fake_running(mapping)
    )


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------


def test_refuses_from_named_profile(monkeypatch, recorder, capsys):
    _set_active(monkeypatch, "kivi")
    _set_config(monkeypatch, ["kivi", "default"])
    _set_registered(monkeypatch, ["default", "kivi"])
    _set_running(monkeypatch, {"default": 100, "kivi": 200})

    rc = gateway.run_gateway_reload_profiles()

    assert rc == 1
    assert recorder["sigusr1"] == []
    assert recorder["self"] == []
    assert "may only run from the default profile" in capsys.readouterr().out


def test_refuses_from_custom_home(monkeypatch, recorder):
    _set_active(monkeypatch, "custom")
    _set_config(monkeypatch, ["default"])
    _set_registered(monkeypatch, ["default"])
    _set_running(monkeypatch, {"default": 100})

    assert gateway.run_gateway_reload_profiles() == 1
    assert recorder["sigusr1"] == []
    assert recorder["self"] == []


# ---------------------------------------------------------------------------
# Allowlist behaviour
# ---------------------------------------------------------------------------


def test_empty_allowlist_is_noop(monkeypatch, recorder, capsys):
    _set_active(monkeypatch, "default")
    _set_config(monkeypatch, [])  # empty
    _set_registered(monkeypatch, ["default", "kivi"])
    _set_running(monkeypatch, {"default": 100, "kivi": 200})

    rc = gateway.run_gateway_reload_profiles()

    assert rc == 0
    assert recorder["sigusr1"] == []
    assert recorder["self"] == []
    assert "empty" in capsys.readouterr().out.lower()


def test_missing_gateway_section_is_noop(monkeypatch, recorder):
    _set_active(monkeypatch, "default")
    _set_config(monkeypatch, None)  # no gateway key at all
    _set_registered(monkeypatch, ["default", "kivi"])
    _set_running(monkeypatch, {"default": 100, "kivi": 200})

    assert gateway.run_gateway_reload_profiles() == 0
    assert recorder["sigusr1"] == []
    assert recorder["self"] == []


def test_allowlist_intersection_ignores_unregistered(monkeypatch, recorder, capsys):
    _set_active(monkeypatch, "default")
    # ``ghost`` is allowlisted but not registered -> ignored.
    _set_config(monkeypatch, ["kivi", "ghost"])
    _set_registered(monkeypatch, ["default", "kivi", "workbot"])
    _set_running(monkeypatch, {"default": 100, "kivi": 200, "workbot": 300})

    rc = gateway.run_gateway_reload_profiles()

    assert rc == 0
    # Only kivi reloaded; workbot not in allowlist, ghost not registered.
    assert recorder["sigusr1"] == [200]
    assert recorder["self"] == []  # default not in allowlist
    out = capsys.readouterr().out
    assert "ghost" in out  # warned about the unknown entry


def test_allowlist_dedupes_and_preserves_order(monkeypatch):
    _set_config(monkeypatch, ["b", "a", "b", "  ", "a", 123, "c"])
    assert gateway._get_reload_profiles_allowlist() == ["b", "a", "c"]


def test_non_list_allowlist_is_empty(monkeypatch):
    monkeypatch.setattr(
        gateway, "read_raw_config", lambda: {"gateway": {"reload_profiles": "kivi"}}
    )
    assert gateway._get_reload_profiles_allowlist() == []


# ---------------------------------------------------------------------------
# Ordering + drain-only mechanism
# ---------------------------------------------------------------------------


def test_default_reloaded_last_via_self_restart(monkeypatch, recorder):
    _set_active(monkeypatch, "default")
    # default deliberately listed FIRST to prove it is still reloaded last.
    _set_config(monkeypatch, ["default", "kivi", "workbot"])
    _set_registered(monkeypatch, ["default", "kivi", "workbot"])
    _set_running(monkeypatch, {"default": 100, "kivi": 200, "workbot": 300})

    rc = gateway.run_gateway_reload_profiles()

    assert rc == 0
    # Foreign profiles drained in allowlist order, default NOT among them.
    assert recorder["sigusr1"] == [200, 300]
    # Default handled by the sanctioned self-restart path, last.
    assert recorder["self"] == [100]
    # Drain timeout threaded through from config.
    assert recorder["drain_timeout"] == [60.0, 60.0]


def test_self_restart_falls_back_to_drain(monkeypatch, recorder):
    _set_active(monkeypatch, "default")
    _set_config(monkeypatch, ["default"])
    _set_registered(monkeypatch, ["default"])
    _set_running(monkeypatch, {"default": 100})

    # Simulate CLI not being a child of the gateway: self-restart declines.
    monkeypatch.setattr(gateway, "_request_gateway_self_restart", lambda pid: False)

    rc = gateway.run_gateway_reload_profiles()

    assert rc == 0
    # Falls back to the same drain-aware SIGUSR1 path (still drain-only).
    assert recorder["sigusr1"] == [100]


def test_skips_profiles_without_running_gateway(monkeypatch, recorder, capsys):
    _set_active(monkeypatch, "default")
    _set_config(monkeypatch, ["kivi", "workbot"])
    _set_registered(monkeypatch, ["default", "kivi", "workbot"])
    # workbot registered + allowlisted but not currently running.
    _set_running(monkeypatch, {"default": 100, "kivi": 200})

    rc = gateway.run_gateway_reload_profiles()

    assert rc == 0
    assert recorder["sigusr1"] == [200]
    assert "workbot" in capsys.readouterr().out


def test_dry_run_signals_nothing(monkeypatch, recorder, capsys):
    _set_active(monkeypatch, "default")
    _set_config(monkeypatch, ["kivi", "default"])
    _set_registered(monkeypatch, ["default", "kivi"])
    _set_running(monkeypatch, {"default": 100, "kivi": 200})

    rc = gateway.run_gateway_reload_profiles(dry_run=True)

    assert rc == 0
    assert recorder["sigusr1"] == []
    assert recorder["self"] == []
    assert "dry-run" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# Parser / dispatch — exercised with the EXACT keyword args main.py passes.
# ---------------------------------------------------------------------------


# The full keyword set ``hermes_cli.main.main()`` passes to
# ``build_gateway_parser`` (see the call at the "gateway + proxy commands"
# block).  Building the parser with anything less than this set is exactly the
# regression that crashed every gateway CLI, so the helper below mirrors the
# live caller contract in full.
def _build_parser():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    build_gateway_parser(
        sub,
        cmd_gateway=lambda a: None,
        cmd_proxy=lambda a: None,
        cmd_gateway_enroll=lambda a: None,
    )
    return parser


def test_parser_accepts_reload_profiles():
    args = _build_parser().parse_args(["gateway", "reload-profiles"])
    assert args.gateway_command == "reload-profiles"
    assert args.dry_run is False


def test_parser_accepts_dry_run_flag():
    args = _build_parser().parse_args(["gateway", "reload-profiles", "--dry-run"])
    assert args.dry_run is True


def test_dispatch_invokes_handler(monkeypatch):
    called = {}

    def fake_handler(dry_run=False):
        called["dry_run"] = dry_run
        return 0

    monkeypatch.setattr(gateway, "run_gateway_reload_profiles", fake_handler)
    args = SimpleNamespace(gateway_command="reload-profiles", dry_run=True)
    gateway._gateway_command_inner(args)
    assert called == {"dry_run": True}


def test_dispatch_exits_nonzero_on_refusal(monkeypatch):
    monkeypatch.setattr(gateway, "run_gateway_reload_profiles", lambda dry_run=False: 1)
    args = SimpleNamespace(gateway_command="reload-profiles", dry_run=False)
    with pytest.raises(SystemExit) as exc:
        gateway._gateway_command_inner(args)
    assert exc.value.code == 1


# ---------------------------------------------------------------------------
# Real main-wiring regression: guard the exact incident (parser signature must
# accept cmd_gateway_enroll and every other keyword the live main() passes).
# ---------------------------------------------------------------------------


def test_build_gateway_parser_signature_accepts_main_keyword_args():
    """The parser must accept exactly the keywords the live caller passes.

    Reproduces the incident at the interface level: a deployed parser whose
    signature did not accept ``cmd_gateway_enroll`` while ``main.py`` called
    ``build_gateway_parser(..., cmd_gateway_enroll=...)`` — a TypeError that
    crashed every gateway CLI.  ``Signature.bind`` fails loudly if any of the
    live keywords are dropped or renamed.
    """
    sig = inspect.signature(build_gateway_parser)
    # Must bind cleanly with the full live keyword set (dummy subparsers arg).
    sig.bind(
        argparse.ArgumentParser().add_subparsers(dest="command"),
        cmd_gateway=lambda a: None,
        cmd_proxy=lambda a: None,
        cmd_gateway_enroll=lambda a: None,
    )
    assert "cmd_gateway_enroll" in sig.parameters


def test_real_main_wiring_builds_gateway_parser():
    """Drive the parser build through the real ``hermes_cli.main`` handlers.

    Imports the actual ``cmd_gateway_enroll`` / ``cmd_gateway`` / ``cmd_proxy``
    callables from ``hermes_cli.main`` and replicates the live call so a future
    signature drift (the incident) is caught with the real objects, not
    stand-ins — then confirms the new verb parses end-to-end.
    """
    import hermes_cli.main as main

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    build_gateway_parser(
        sub,
        cmd_gateway=main.cmd_gateway,
        cmd_proxy=main.cmd_proxy,
        cmd_gateway_enroll=main.cmd_gateway_enroll,
    )
    args = parser.parse_args(["gateway", "reload-profiles", "--dry-run"])
    assert args.gateway_command == "reload-profiles"
    assert args.dry_run is True


# ---------------------------------------------------------------------------
# Subprocess CLI startup smoke test — proves `python -m hermes_cli.main` starts
# through the REAL main() wiring (the live build_gateway_parser call) without a
# TypeError and without signalling any live process.
# ---------------------------------------------------------------------------


def _run_cli(args, home):
    env = dict(os.environ)
    env["HERMES_HOME"] = home
    return subprocess.run(
        [sys.executable, "-m", "hermes_cli.main", *args],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )


def test_cli_help_starts_without_typeerror():
    with tempfile.TemporaryDirectory() as home:
        res = _run_cli(["--help"], home)
    assert res.returncode == 0, res.stderr
    assert "TypeError" not in res.stderr
    assert "Traceback" not in res.stderr
    assert "usage:" in res.stdout.lower()


def test_cli_reload_profiles_dry_run_starts_and_signals_nothing():
    """`gateway reload-profiles --dry-run` on a fresh temp HERMES_HOME reaches
    the empty-allowlist no-op: it starts through the real main() wiring without
    a TypeError and, with no allowlist configured, signals no live process."""
    with tempfile.TemporaryDirectory() as home:
        res = _run_cli(["gateway", "reload-profiles", "--dry-run"], home)
    assert res.returncode == 0, res.stderr
    assert "TypeError" not in res.stderr
    assert "Traceback" not in res.stderr
    combined = (res.stdout + res.stderr).lower()
    # Empty allowlist by default -> explicit no-op, nothing signalled.
    assert "reload_profiles is empty" in combined or "nothing to reload" in combined


# ---------------------------------------------------------------------------
# Approval-guard regression: reload-profiles allowed, stop/restart still guarded
# ---------------------------------------------------------------------------


def test_reload_profiles_not_flagged_dangerous():
    from tools.approval import detect_dangerous_command

    is_dangerous, _, _ = detect_dangerous_command("hermes gateway reload-profiles")
    assert is_dangerous is False


def test_stop_and_restart_still_flagged_dangerous():
    from tools.approval import detect_dangerous_command

    for cmd in ("hermes gateway stop", "hermes gateway restart", "hermes gateway restart --all"):
        is_dangerous, _, _ = detect_dangerous_command(cmd)
        assert is_dangerous is True, cmd
