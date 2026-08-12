"""More than one named approver per profile — and never anyone wider.

Why this exists (2026-08-12, same day as the owner gate itself)
--------------------------------------------------------------
The owner gate shipped this morning pinned staged-write review to exactly ONE
resolved id. Vito's decision the same afternoon: Sandro reviews on ``kivi``
(Gogi) and on ``workbot`` (Givi); Fariza and Yakov review on ``workbot`` only.
Those four are already the "technical tier" in ``profiles/workbot/SOUL.md``, so
this widens who may spend an approval, not what an approval does.

The dangerous half of that change is the DEFAULT. A list-shaped permission
whose empty/typo'd/missing forms fail OPEN puts the hole straight back: on
``kivi`` (``TELEGRAM_GROUP_ALLOWED_USERS=*``) "everyone" means "every Llucky
prospect in the chat". So the rules pinned here are, in order of importance:

  1. no list configured  -> byte-for-byte the previous single-owner behaviour;
  2. list present but empty / wildcard / malformed -> NOBODY may approve;
  3. only ids named for THIS profile may approve, and only in a DM;
  4. the new-proposal notice reaches EVERY approver of the profile and no one
     else — checked against the live kivi shape, where the chat the turn
     happened in belongs to a client.
"""

import json
import os
import shutil
import tempfile
from types import SimpleNamespace

import pytest
import yaml

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource


VITO = "405154434"
SANDRO = "453088539"
FARIZA = "6573730577"
YAKOV = "186773197"
CLIENT = "999888777"

# The live allowlists, read off the M1 on 2026-08-12.
KIVI_ALLOWED = VITO + "," + SANDRO
WORKBOT_ALLOWED = ",".join([VITO, SANDRO, FARIZA, YAKOV])
KIVI_APPROVERS = [VITO, SANDRO]
WORKBOT_APPROVERS = [VITO, SANDRO, FARIZA, YAKOV]

CLIENT_CHAT = "-1003948395916"  # a real Gogi client group id shape


@pytest.fixture
def hermes_home(monkeypatch):
    d = tempfile.mkdtemp(prefix="hermes_approvers_")
    home = os.path.join(d, ".hermes")
    os.makedirs(home)
    monkeypatch.setenv("HERMES_HOME", home)
    yield home
    shutil.rmtree(d, ignore_errors=True)


def _write_gateway_config(home, **gateway_keys):
    """Put keys under ``gateway:`` in config.yaml the way Vito edits it.

    Deliberately a real file write rather than ``save_config`` of a merged
    dict: the point of several of these tests is what happens when the key is
    ABSENT, and a merged round-trip would materialise every default.
    """
    path = os.path.join(home, "config.yaml")
    data = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    data.setdefault("gateway", {}).update(gateway_keys)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True)


def _cfg(allow_from=None, group_allow_from=None):
    extra = {}
    if allow_from is not None:
        extra["allow_from"] = allow_from
    if group_allow_from is not None:
        extra["group_allow_from"] = group_allow_from
    return SimpleNamespace(platforms={Platform.TELEGRAM: SimpleNamespace(extra=extra)})


def _source(user_id=VITO, chat_type="dm", chat_id=None):
    return SessionSource(
        platform=Platform.TELEGRAM,
        chat_id=str(chat_id if chat_id is not None else user_id),
        chat_type=chat_type,
        user_id=user_id,
        user_name="tester",
    )


def _event(text, user_id=VITO, chat_type="dm", chat_id=None):
    return MessageEvent(text=text, source=_source(user_id, chat_type, chat_id))


def _handler(cfg):
    from gateway.slash_commands import GatewaySlashCommandsMixin
    h = GatewaySlashCommandsMixin.__new__(GatewaySlashCommandsMixin)
    h.config = cfg
    h._session_key_for_source = lambda source: "telegram:test"
    h._evict_cached_agent = lambda key: None
    return h


def _install_skill(home, name="report-writer", body="step one\nstep two\n"):
    d = os.path.join(home, "skills", name)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "SKILL.md"), "w", encoding="utf-8") as f:
        f.write(
            "---\nname: " + name + "\ndescription: writes reports\n---\n"
            "# " + name + "\n" + body
        )
    return d


def _stage_patch(name="report-writer", old="step one", new="step one (verified)"):
    from tools import write_approval as wa
    payload = {"action": "patch", "name": name,
               "old_string": old, "new_string": new}
    return wa.stage_write(wa.SKILLS, payload,
                          summary="patch " + name + " SKILL.md (+1/-1 lines)",
                          origin="background_review")


def _audit(home):
    path = os.path.join(home, "pending", "decisions.jsonl")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


# ---------------------------------------------------------------------------
# 1. The default must not move
# ---------------------------------------------------------------------------

def test_no_list_configured_is_exactly_the_previous_single_owner(hermes_home,
                                                                 monkeypatch):
    """No ``pending_approval_owners`` key -> one owner, the first allowlist id."""
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", WORKBOT_ALLOWED)
    cfg = _cfg()
    assert pra.resolve_owner_user_ids(cfg, Platform.TELEGRAM) == (VITO,)
    assert pra.check_pending_review_access(cfg, _source(user_id=VITO)) is None
    for other in (SANDRO, FARIZA, YAKOV, CLIENT):
        refusal = pra.check_pending_review_access(cfg, _source(user_id=other))
        assert refusal and "owner" in refusal.lower(), (other, refusal)


def test_scalar_key_still_names_exactly_one_approver(hermes_home, monkeypatch):
    """Back-compat: the shipped single-id key keeps working, and stays single."""
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", WORKBOT_ALLOWED)
    _write_gateway_config(hermes_home, pending_approval_owner=SANDRO)
    cfg = _cfg()
    assert pra.resolve_owner_user_ids(cfg, Platform.TELEGRAM) == (SANDRO,)
    assert pra.check_pending_review_access(cfg, _source(user_id=SANDRO)) is None
    assert pra.check_pending_review_access(cfg, _source(user_id=VITO))


# ---------------------------------------------------------------------------
# 2. Empty / malformed list = nobody. This is the test that matters most:
#    getting it wrong restores the hole the owner gate just closed.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value, label", [
    ([], "empty list"),
    ("", "empty string"),
    ("   ", "whitespace"),
    (["*"], "wildcard element"),
    ("*", "bare wildcard"),
    ([VITO, "*"], "wildcard smuggled in beside a real id"),
    ({}, "mapping instead of a list"),
    ({"vito": VITO}, "mapping of ids"),
    ([None], "null element"),
    ([[VITO]], "nested list"),
    (True, "boolean"),
])
def test_unusable_list_denies_everyone_and_never_widens(hermes_home, monkeypatch,
                                                        value, label):
    """A broken approver list must deny ALL, not admit all — and not fall back.

    Falling back to the allowlist would be quieter but wrong twice over: it
    hides the typo, and on a profile whose DM allowlist starts with someone
    else it would hand approval to an id Vito never named.
    """
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", WORKBOT_ALLOWED)
    _write_gateway_config(hermes_home, pending_approval_owners=value)
    cfg = _cfg(allow_from=WORKBOT_ALLOWED)
    assert pra.resolve_owner_user_ids(cfg, Platform.TELEGRAM) == (), label
    for who in (VITO, SANDRO, FARIZA, YAKOV, CLIENT):
        refusal = pra.check_pending_review_access(cfg, _source(user_id=who))
        assert refusal, (label, who)
        assert "pending_approval_owner" in refusal, (label, who, refusal)


def test_broken_list_is_not_rescued_by_the_scalar_key(hermes_home):
    """Both keys set, list unusable -> deny. A typo must stay visible."""
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home, pending_approval_owner=VITO,
                          pending_approval_owners=[])
    cfg = _cfg(allow_from=WORKBOT_ALLOWED)
    assert pra.resolve_owner_user_ids(cfg, Platform.TELEGRAM) == ()
    assert pra.check_pending_review_access(cfg, _source(user_id=VITO))


def test_list_wins_over_scalar_and_over_the_allowlist(hermes_home, monkeypatch):
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", WORKBOT_ALLOWED)
    _write_gateway_config(hermes_home, pending_approval_owner=YAKOV,
                          pending_approval_owners=[VITO, SANDRO])
    assert pra.resolve_owner_user_ids(_cfg(), Platform.TELEGRAM) == (VITO, SANDRO)


def test_list_accepts_yaml_ints_and_a_comma_string(hermes_home):
    """``- 405154434`` unquoted is an int in YAML; a typo'd scalar is a string."""
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home,
                          pending_approval_owners=[int(VITO), int(SANDRO)])
    assert pra.resolve_owner_user_ids(_cfg(), Platform.TELEGRAM) == (VITO, SANDRO)
    _write_gateway_config(hermes_home,
                          pending_approval_owners=VITO + ", " + SANDRO)
    assert pra.resolve_owner_user_ids(_cfg(), Platform.TELEGRAM) == (VITO, SANDRO)


# ---------------------------------------------------------------------------
# 3. Vito's decision, per profile
# ---------------------------------------------------------------------------

def test_kivi_roster_admits_vito_and_sandro_only(hermes_home, monkeypatch):
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", KIVI_ALLOWED)
    monkeypatch.setenv("TELEGRAM_GROUP_ALLOWED_USERS", "*")
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    cfg = _cfg()
    for who in (VITO, SANDRO):
        assert pra.check_pending_review_access(cfg, _source(user_id=who)) is None, who
    for who in (FARIZA, YAKOV, CLIENT):
        refusal = pra.check_pending_review_access(cfg, _source(user_id=who))
        assert refusal and "owner" in refusal.lower(), who


def test_workbot_roster_admits_all_four(hermes_home, monkeypatch):
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", WORKBOT_ALLOWED)
    _write_gateway_config(hermes_home, pending_approval_owners=WORKBOT_APPROVERS)
    cfg = _cfg()
    for who in WORKBOT_APPROVERS:
        assert pra.check_pending_review_access(cfg, _source(user_id=who)) is None, who
    assert pra.check_pending_review_access(cfg, _source(user_id=CLIENT))


@pytest.mark.parametrize("who", [VITO, SANDRO, FARIZA, YAKOV])
def test_a_group_refuses_every_approver_including_vito(hermes_home, who):
    """Approval is private. Group membership is not the approvers' to control."""
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home, pending_approval_owners=WORKBOT_APPROVERS)
    refusal = pra.check_pending_review_access(
        _cfg(group_allow_from="*"),
        _source(user_id=who, chat_type="group", chat_id=CLIENT_CHAT),
    )
    assert refusal and "direct message" in refusal.lower(), (who, refusal)


# ---------------------------------------------------------------------------
# 4. End to end through the gateway handler: the write lands, the audit names
#    who landed it.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sandro_approves_on_kivi_and_the_audit_names_him(hermes_home,
                                                               monkeypatch):
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", KIVI_ALLOWED)
    monkeypatch.setenv("TELEGRAM_GROUP_ALLOWED_USERS", "*")
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    d = _install_skill(hermes_home)
    rec = _stage_patch()

    h = _handler(_cfg())
    out = await h._handle_skills_command(
        _event("/skills approve " + rec["id"], user_id=SANDRO)
    )
    assert "Approved 1" in out, out
    with open(os.path.join(d, "SKILL.md"), encoding="utf-8") as f:
        assert "step one (verified)" in f.read()

    entries = _audit(hermes_home)
    assert len(entries) == 1, entries
    e = entries[0]
    assert e["actor"] == SANDRO, e
    assert e["decision"] == "approve" and e["pending_id"] == rec["id"], e
    assert e["subsystem"] == "skills" and e["applied"] is True, e


@pytest.mark.asyncio
@pytest.mark.parametrize("who", [SANDRO, FARIZA, YAKOV])
async def test_the_workbot_tier_can_approve_and_is_recorded(hermes_home,
                                                            monkeypatch, who):
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", WORKBOT_ALLOWED)
    _write_gateway_config(hermes_home, pending_approval_owners=WORKBOT_APPROVERS)
    d = _install_skill(hermes_home)
    rec = _stage_patch()

    h = _handler(_cfg())
    out = await h._handle_skills_command(
        _event("/skills approve " + rec["id"], user_id=who)
    )
    assert "Approved 1" in out, out
    with open(os.path.join(d, "SKILL.md"), encoding="utf-8") as f:
        assert "step one (verified)" in f.read()
    assert [e["actor"] for e in _audit(hermes_home)] == [who]


@pytest.mark.asyncio
@pytest.mark.parametrize("who", [FARIZA, YAKOV])
async def test_the_workbot_tier_is_refused_on_kivi(hermes_home, monkeypatch, who):
    """Vito named them for Givi only. Gogi's queue is not theirs to spend."""
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", KIVI_ALLOWED)
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    d = _install_skill(hermes_home)
    rec = _stage_patch()

    h = _handler(_cfg())
    out = await h._handle_skills_command(
        _event("/skills approve " + rec["id"], user_id=who)
    )
    assert "Approved" not in out, out
    with open(os.path.join(d, "SKILL.md"), encoding="utf-8") as f:
        assert "step one (verified)" not in f.read()
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is not None
    assert _audit(hermes_home) == []


@pytest.mark.asyncio
async def test_rejection_records_who_rejected(hermes_home):
    _write_gateway_config(hermes_home, pending_approval_owners=WORKBOT_APPROVERS)
    _install_skill(hermes_home)
    rec = _stage_patch()

    h = _handler(_cfg())
    out = await h._handle_skills_command(
        _event("/skills reject " + rec["id"], user_id=FARIZA)
    )
    assert "Rejected" in out, out
    entries = _audit(hermes_home)
    assert len(entries) == 1, entries
    assert entries[0]["actor"] == FARIZA, entries
    assert entries[0]["decision"] == "reject", entries


@pytest.mark.asyncio
async def test_memory_approval_is_recorded_too(hermes_home):
    from tools import write_approval as wa
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    rec = wa.stage_write(
        wa.MEMORY, {"action": "add", "target": "memory", "content": "a note"},
        summary="a note", origin="background_review",
    )
    h = _handler(_cfg())
    out = await h._handle_memory_command(
        _event("/memory approve " + rec["id"], user_id=SANDRO)
    )
    assert "Approved 1" in out, out
    entries = _audit(hermes_home)
    assert [(e["actor"], e["subsystem"]) for e in entries] == [(SANDRO, "memory")]


@pytest.mark.asyncio
async def test_flipping_the_gate_is_recorded(hermes_home):
    """With four approvers, "who turned the gate off" must not be guesswork."""
    _write_gateway_config(hermes_home, pending_approval_owners=WORKBOT_APPROVERS)
    h = _handler(_cfg())
    out = await h._handle_skills_command(
        _event("/skills approval on", user_id=YAKOV)
    )
    assert "set to" in out, out
    entries = _audit(hermes_home)
    assert len(entries) == 1, entries
    assert entries[0]["actor"] == YAKOV, entries
    assert entries[0]["decision"] == "approval_on", entries


@pytest.mark.asyncio
@pytest.mark.parametrize("roster", [KIVI_APPROVERS, WORKBOT_APPROVERS],
                         ids=["kivi", "workbot"])
async def test_an_outsider_learns_nothing_on_either_profile(hermes_home,
                                                            monkeypatch, roster):
    """Refusal must be identical whether or not a queue exists — no oracle."""
    monkeypatch.setenv("TELEGRAM_GROUP_ALLOWED_USERS", "*")
    _write_gateway_config(hermes_home, pending_approval_owners=roster)
    h = _handler(_cfg())
    outsider = dict(user_id=CLIENT, chat_type="dm", chat_id=CLIENT)

    empty = await h._handle_skills_command(_event("/skills pending", **outsider))

    _install_skill(hermes_home)
    rec = _stage_patch()
    full = await h._handle_skills_command(_event("/skills pending", **outsider))

    assert empty == full, (empty, full)
    assert rec["id"] not in full and "report-writer" not in full, full
    assert "approval on" not in full and "write_approval" not in full, full
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is not None
    assert _audit(hermes_home) == []


@pytest.mark.asyncio
async def test_an_unwritable_decision_log_is_reported_not_swallowed(hermes_home):
    """If the decision cannot be recorded, the approver has to be TOLD.

    The write itself has already landed by then, so silence here would produce
    exactly the state this change exists to prevent: a skill rewritten on disk
    with nobody's name on it. The reply must say so; the change must still be
    real (a half-applied approval would be worse than an unlogged one).
    """
    from tools import write_approval as wa
    # A directory where the log file goes: every append raises.
    os.makedirs(os.path.join(hermes_home, "pending", wa.DECISION_LOG_NAME),
                exist_ok=True)
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    d = _install_skill(hermes_home)
    rec = _stage_patch()

    h = _handler(_cfg())
    out = await h._handle_skills_command(
        _event("/skills approve " + rec["id"], user_id=SANDRO)
    )
    assert "Approved 1" in out, out
    assert "NOT recorded" in out, out
    with open(os.path.join(d, "SKILL.md"), encoding="utf-8") as f:
        assert "step one (verified)" in f.read()


def test_read_decisions_round_trips_and_never_hides_a_bad_line(hermes_home):
    from tools import write_approval as wa
    assert wa.read_decisions() == []
    assert wa.record_decision("skills", "abc123", "approve", actor=SANDRO,
                              actor_channel="telegram", applied=True) == ""
    with open(wa.decision_log_path(), "a", encoding="utf-8") as f:
        f.write("{not json}\n")
    assert wa.record_decision("memory", "def456", "reject", actor=None) == ""

    entries = wa.read_decisions()
    assert len(entries) == 3, entries
    assert entries[0]["actor"] == SANDRO and entries[0]["applied"] is True
    assert entries[1]["_unparsed"] == "{not json}", entries[1]
    # No identity given -> written down as unknown, not omitted.
    assert entries[2]["actor"] == "unknown" and entries[2]["decision"] == "reject"
    assert [e.get("pending_id") for e in wa.read_decisions(limit=1)] == ["def456"]


# ---------------------------------------------------------------------------
# 5. The notice reaches every approver — and cannot reach a client chat
# ---------------------------------------------------------------------------

class _RecordingAdapter:
    def __init__(self, unreachable=()):
        self.sent = []
        self._unreachable = set(unreachable)

    def dm_chat_id_for_user(self, user_id):
        if str(user_id) in self._unreachable:
            return None
        return str(user_id)

    def send(self, chat_id, content):
        self.sent.append((str(chat_id), content))
        return ("coro", chat_id, content)


def test_notice_reaches_every_kivi_approver_and_never_the_client_chat(hermes_home,
                                                                      monkeypatch):
    """The live Gogi shape: the turn happens in a client group.

    Measured here rather than argued: the set of chat ids the sender is even
    CAPABLE of addressing is compared against the client chat id.
    """
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", KIVI_ALLOWED)
    monkeypatch.setenv("TELEGRAM_GROUP_ALLOWED_USERS", "*")
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)

    cfg = _cfg()
    src = _source(user_id=CLIENT, chat_type="group", chat_id=CLIENT_CHAT)
    assert pra.owner_notice_chat_ids(cfg, src, _RecordingAdapter()) == (VITO, SANDRO)

    adapter = _RecordingAdapter()
    scheduled = []
    send = pra.make_owner_notice_sender(cfg, src, adapter, scheduled.append)
    assert send is not None
    send("⏸ proposal /skills approve 8f2bb0dc")

    assert [c for c, _ in adapter.sent] == [VITO, SANDRO], adapter.sent
    assert all(chat != CLIENT_CHAT for chat, _ in adapter.sent), adapter.sent
    assert all(chat != CLIENT for chat, _ in adapter.sent), adapter.sent
    assert len(scheduled) == 2, scheduled


def test_notice_reaches_all_four_on_workbot(hermes_home):
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home, pending_approval_owners=WORKBOT_APPROVERS)
    adapter = _RecordingAdapter()
    send = pra.make_owner_notice_sender(
        _cfg(), _source(user_id=VITO, chat_type="group", chat_id="-100777"),
        adapter, lambda coro: None,
    )
    assert send is not None
    send("⏸ proposal")
    assert [c for c, _ in adapter.sent] == WORKBOT_APPROVERS, adapter.sent


def test_notice_from_an_approver_in_a_client_group_uses_dms_only(hermes_home):
    """Vito asking Gogi something inside a client group is still a client group.

    "The sender is an approver" must not be enough to make the current chat a
    delivery target — only "the sender is an approver AND this is their DM".
    """
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    adapter = _RecordingAdapter()
    send = pra.make_owner_notice_sender(
        _cfg(group_allow_from="*"),
        _source(user_id=VITO, chat_type="group", chat_id=CLIENT_CHAT),
        adapter, lambda coro: None,
    )
    assert send is not None
    send("⏸ proposal")
    assert [c for c, _ in adapter.sent] == [VITO, SANDRO], adapter.sent
    assert all(chat != CLIENT_CHAT for chat, _ in adapter.sent), adapter.sent


def test_notice_in_one_approver_dm_still_reaches_the_others(hermes_home):
    """Sandro's DM is where the turn happened; Vito must still be told."""
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    adapter = _RecordingAdapter()
    send = pra.make_owner_notice_sender(
        _cfg(), _source(user_id=SANDRO, chat_type="dm", chat_id=SANDRO),
        adapter, lambda coro: None,
    )
    send("⏸ proposal")
    assert sorted(c for c, _ in adapter.sent) == sorted([VITO, SANDRO]), adapter.sent
    assert len(adapter.sent) == 2, adapter.sent


def test_one_unreachable_approver_does_not_silence_the_rest(hermes_home):
    """Sandro may never have opened a DM with Gogi. Vito still gets the notice."""
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    adapter = _RecordingAdapter(unreachable={SANDRO})
    send = pra.make_owner_notice_sender(
        _cfg(), _source(user_id=CLIENT, chat_type="group", chat_id=CLIENT_CHAT),
        adapter, lambda coro: None,
    )
    assert send is not None
    send("⏸ proposal")
    assert [c for c, _ in adapter.sent] == [VITO], adapter.sent


def test_adapter_that_cannot_dm_at_all_yields_no_targets(hermes_home):
    """An unported platform adapter has no ``dm_chat_id_for_user`` at all.

    "I don't know how to DM anyone" must resolve to silence, not to the room
    the bot happens to be standing in.
    """
    from gateway import pending_review_access as pra

    class _NoDmSupport:
        def send(self, chat_id, content):  # pragma: no cover - must not run
            raise AssertionError("nothing may be addressed on this adapter")

    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    src = _source(user_id=CLIENT, chat_type="group", chat_id=CLIENT_CHAT)
    assert pra.owner_notice_chat_ids(_cfg(), src, _NoDmSupport()) == ()
    assert pra.make_owner_notice_sender(
        _cfg(), src, _NoDmSupport(), lambda coro: None
    ) is None


def test_no_approver_reachable_means_silence_not_a_fallback(hermes_home):
    from gateway import pending_review_access as pra
    _write_gateway_config(hermes_home, pending_approval_owners=KIVI_APPROVERS)
    adapter = _RecordingAdapter(unreachable={VITO, SANDRO})
    assert pra.make_owner_notice_sender(
        _cfg(), _source(user_id=CLIENT, chat_type="group", chat_id=CLIENT_CHAT),
        adapter, lambda coro: None,
    ) is None


def test_unusable_list_means_no_notice_rail_at_all(hermes_home, monkeypatch):
    """Deny-all has to cover delivery too, or the queue is announced to nobody
    identifiable — but never to the room."""
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", KIVI_ALLOWED)
    _write_gateway_config(hermes_home, pending_approval_owners=[])
    adapter = _RecordingAdapter()
    assert pra.owner_notice_chat_ids(
        _cfg(), _source(user_id=CLIENT, chat_type="group", chat_id=CLIENT_CHAT),
        adapter,
    ) == ()
    assert pra.make_owner_notice_sender(
        _cfg(), _source(user_id=CLIENT, chat_type="group", chat_id=CLIENT_CHAT),
        adapter, lambda coro: None,
    ) is None
