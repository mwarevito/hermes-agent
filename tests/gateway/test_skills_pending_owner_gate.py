"""Owner-only gate + real-diff listing for the gateway staged-write review.

Why this exists (2026-08-12). The gateway DOES route ``/skills`` and
``/memory`` to the shared write-approval handler, but it applied no identity
check at all: ``SlashAccessPolicy`` gating is disabled whenever
``allow_admin_from`` is unset (which is the case on every live profile), so
every *allowed sender* could run ``/skills approve <id>``, ``/skills diff
<id>`` and even ``/skills approval off``.

Measured on the M1 the same day: profile ``kivi`` (Gogi, the Llucky sales bot
that sits in chats with CLIENTS) has ``skills.write_approval: true``,
``TELEGRAM_GROUP_ALLOWED_USERS=*`` and no ``allow_admin_from`` — so a prospect
in a group chat could commit a rewrite of the bot's own skills, or silently
turn the approval gate off. These tests pin approval to ONE resolved owner,
in a DM, and pin the listing to a real on-disk diff so approving is an
informed act rather than a name-only guess.
"""

import os
import shutil
import tempfile
from types import SimpleNamespace

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource


OWNER = "405154434"
OTHER_ADMIN = "453088539"
CLIENT = "999888777"


@pytest.fixture
def hermes_home(monkeypatch):
    d = tempfile.mkdtemp(prefix="hermes_pending_owner_")
    home = os.path.join(d, ".hermes")
    os.makedirs(home)
    monkeypatch.setenv("HERMES_HOME", home)
    yield home
    shutil.rmtree(d, ignore_errors=True)


def _cfg(allow_from=None, group_allow_from=None):
    """Minimal GatewayConfig stand-in: only ``platforms[p].extra`` is read."""
    extra = {}
    if allow_from is not None:
        extra["allow_from"] = allow_from
    if group_allow_from is not None:
        extra["group_allow_from"] = group_allow_from
    return SimpleNamespace(platforms={Platform.TELEGRAM: SimpleNamespace(extra=extra)})


def _source(user_id=OWNER, chat_type="dm", chat_id=None):
    return SessionSource(
        platform=Platform.TELEGRAM,
        chat_id=str(chat_id if chat_id is not None else user_id),
        chat_type=chat_type,
        user_id=user_id,
        user_name="tester",
    )


def _event(text, user_id=OWNER, chat_type="dm", chat_id=None):
    return MessageEvent(text=text, source=_source(user_id, chat_type, chat_id))


# ---------------------------------------------------------------------------
# Owner resolution
# ---------------------------------------------------------------------------

def test_owner_from_single_dm_allowlist(hermes_home):
    from gateway import pending_review_access as pra
    cfg = _cfg(allow_from=OWNER)
    assert pra.resolve_owner_user_id(cfg, Platform.TELEGRAM) == OWNER


def test_owner_is_first_entry_when_allowlist_has_several(hermes_home):
    """kivi/workbot shape: the owner is the id ``hermes setup`` seeded first."""
    from gateway import pending_review_access as pra
    cfg = _cfg(allow_from=OWNER + "," + OTHER_ADMIN)
    assert pra.resolve_owner_user_id(cfg, Platform.TELEGRAM) == OWNER


def test_explicit_config_key_wins_over_allowlist(hermes_home):
    from gateway import pending_review_access as pra
    import hermes_cli.config as cfgmod
    c = cfgmod.load_config()
    c.setdefault("gateway", {})["pending_approval_owner"] = OTHER_ADMIN
    cfgmod.save_config(c)
    cfg = _cfg(allow_from=OWNER + "," + OTHER_ADMIN)
    assert pra.resolve_owner_user_id(cfg, Platform.TELEGRAM) == OTHER_ADMIN


def test_wildcard_allowlist_yields_no_owner(hermes_home):
    """``*`` names everybody, so it names no owner — must not resolve."""
    from gateway import pending_review_access as pra
    assert pra.resolve_owner_user_id(_cfg(allow_from="*"), Platform.TELEGRAM) is None


def test_group_allowlist_is_never_an_owner_source(hermes_home, monkeypatch):
    """kivi: TELEGRAM_GROUP_ALLOWED_USERS=* must not leak into owner resolution."""
    from gateway import pending_review_access as pra
    monkeypatch.delenv("TELEGRAM_ALLOWED_USERS", raising=False)
    cfg = _cfg(group_allow_from="*")
    assert pra.resolve_owner_user_id(cfg, Platform.TELEGRAM) is None


def test_owner_falls_back_to_env_allowlist(hermes_home, monkeypatch):
    """Live profiles set TELEGRAM_ALLOWED_USERS in .env, not config.yaml."""
    from gateway import pending_review_access as pra
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", OWNER + "," + OTHER_ADMIN)
    assert pra.resolve_owner_user_id(_cfg(), Platform.TELEGRAM) == OWNER


# ---------------------------------------------------------------------------
# Access check
# ---------------------------------------------------------------------------

def test_owner_in_dm_is_allowed(hermes_home):
    from gateway import pending_review_access as pra
    assert pra.check_pending_review_access(_cfg(allow_from=OWNER), _source()) is None


def test_second_admin_in_dm_is_refused(hermes_home):
    from gateway import pending_review_access as pra
    refusal = pra.check_pending_review_access(
        _cfg(allow_from=OWNER + "," + OTHER_ADMIN), _source(user_id=OTHER_ADMIN)
    )
    assert refusal and "owner" in refusal.lower()


def test_owner_in_group_is_refused(hermes_home):
    from gateway import pending_review_access as pra
    refusal = pra.check_pending_review_access(
        _cfg(allow_from=OWNER), _source(chat_type="group", chat_id="-100777")
    )
    assert refusal and "direct message" in refusal.lower()


def test_unresolvable_owner_refuses_and_names_the_config_key(hermes_home, monkeypatch):
    from gateway import pending_review_access as pra
    monkeypatch.delenv("TELEGRAM_ALLOWED_USERS", raising=False)
    refusal = pra.check_pending_review_access(_cfg(allow_from="*"), _source())
    assert refusal and "pending_approval_owner" in refusal


# ---------------------------------------------------------------------------
# Owner-only notice target (kivi: never the client chat)
# ---------------------------------------------------------------------------

class _DmCapableAdapter:
    def dm_chat_id_for_user(self, user_id):
        return str(user_id)


class _NoDmAdapter:
    """Base-adapter behaviour: cannot address a user directly."""

    def dm_chat_id_for_user(self, user_id):
        return None


def test_owner_notice_in_client_group_goes_to_owner_dm(hermes_home):
    """kivi shape: turn happens in a client chat → notice must NOT go there."""
    from gateway import pending_review_access as pra
    client_chat = "-1001234567890"
    target = pra.owner_notice_chat_id(
        _cfg(allow_from=OWNER + "," + OTHER_ADMIN),
        _source(user_id=CLIENT, chat_type="group", chat_id=client_chat),
        _DmCapableAdapter(),
    )
    assert target == OWNER
    assert target != client_chat


def test_owner_notice_suppressed_when_dm_unreachable(hermes_home):
    from gateway import pending_review_access as pra
    assert pra.owner_notice_chat_id(
        _cfg(allow_from=OWNER),
        _source(user_id=CLIENT, chat_type="group", chat_id="-100999"),
        _NoDmAdapter(),
    ) is None


def test_owner_notice_delivers_in_place_in_owner_dm(hermes_home):
    from gateway import pending_review_access as pra
    assert pra.owner_notice_chat_id(
        _cfg(allow_from=OWNER), _source(), _NoDmAdapter()
    ) == OWNER


# ---------------------------------------------------------------------------
# Gateway handler: /skills pending | approve | reject
# ---------------------------------------------------------------------------

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


@pytest.mark.asyncio
async def test_owner_pending_list_shows_real_diff(hermes_home):
    _install_skill(hermes_home)
    rec = _stage_patch()
    h = _handler(_cfg(allow_from=OWNER))
    out = await h._handle_skills_command(_event("/skills pending"))
    assert rec["id"] in out, out
    # The real unified diff against what is on disk, not just the skill name.
    assert "-step one" in out and "+step one (verified)" in out, out
    # A ready-to-paste approval command, so the owner never composes an id.
    assert "/skills approve " + rec["id"] in out, out


@pytest.mark.asyncio
async def test_owner_approve_lands_on_disk(hermes_home):
    d = _install_skill(hermes_home)
    rec = _stage_patch()
    h = _handler(_cfg(allow_from=OWNER))
    out = await h._handle_skills_command(_event("/skills approve " + rec["id"]))
    assert "Approved 1" in out, out
    with open(os.path.join(d, "SKILL.md"), encoding="utf-8") as f:
        assert "step one (verified)" in f.read()
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is None


@pytest.mark.asyncio
async def test_owner_reject_removes_the_record(hermes_home):
    _install_skill(hermes_home)
    rec = _stage_patch()
    h = _handler(_cfg(allow_from=OWNER))
    out = await h._handle_skills_command(_event("/skills reject " + rec["id"]))
    assert "Rejected" in out, out
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is None
    assert wa.pending_count(wa.SKILLS) == 0


@pytest.mark.asyncio
async def test_non_owner_cannot_approve_and_record_survives(hermes_home):
    d = _install_skill(hermes_home)
    rec = _stage_patch()
    h = _handler(_cfg(allow_from=OWNER + "," + OTHER_ADMIN))
    out = await h._handle_skills_command(
        _event("/skills approve " + rec["id"], user_id=OTHER_ADMIN)
    )
    assert "Approved" not in out, out
    assert "owner" in out.lower(), out
    with open(os.path.join(d, "SKILL.md"), encoding="utf-8") as f:
        assert "step one (verified)" not in f.read()
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is not None


@pytest.mark.asyncio
async def test_client_in_group_cannot_read_or_flip_the_gate(hermes_home):
    """kivi: TELEGRAM_GROUP_ALLOWED_USERS=* let any prospect run these."""
    _install_skill(hermes_home)
    rec = _stage_patch()
    h = _handler(_cfg(allow_from=OWNER + "," + OTHER_ADMIN, group_allow_from="*"))
    src_kwargs = dict(user_id=CLIENT, chat_type="group", chat_id="-1001234567890")

    listing = await h._handle_skills_command(_event("/skills pending", **src_kwargs))
    assert rec["id"] not in listing, listing

    diffed = await h._handle_skills_command(
        _event("/skills diff " + rec["id"], **src_kwargs)
    )
    assert "step one" not in diffed, diffed

    flipped = await h._handle_skills_command(
        _event("/skills approval off", **src_kwargs)
    )
    assert "set to" not in flipped, flipped
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is not None


@pytest.mark.asyncio
async def test_non_owner_cannot_approve_memory_either(hermes_home):
    """Same hole, same class: /memory is the other half of the shared handler."""
    from tools import write_approval as wa
    rec = wa.stage_write(
        wa.MEMORY, {"action": "add", "target": "memory", "content": "secret note"},
        summary="secret note", origin="background_review",
    )
    h = _handler(_cfg(allow_from=OWNER + "," + OTHER_ADMIN))
    out = await h._handle_memory_command(
        _event("/memory approve " + rec["id"], user_id=OTHER_ADMIN)
    )
    assert "Approved" not in out, out
    assert wa.get_pending(wa.MEMORY, rec["id"]) is not None


# ---------------------------------------------------------------------------
# The rail the gateway actually installs
# ---------------------------------------------------------------------------

class _RecordingAdapter:
    """Adapter that can DM a user and records where sends were addressed."""

    def __init__(self):
        self.sent = []

    def dm_chat_id_for_user(self, user_id):
        return str(user_id)

    def send(self, chat_id, content):
        self.sent.append((str(chat_id), content))
        return ("coro", chat_id, content)


def test_owner_sender_addresses_the_owner_not_the_client_chat(hermes_home):
    """The exact kivi shape: client group chat, notice must land in Vito's DM."""
    from gateway import pending_review_access as pra

    adapter = _RecordingAdapter()
    scheduled = []
    send = pra.make_owner_notice_sender(
        _cfg(allow_from=OWNER + "," + OTHER_ADMIN),
        _source(user_id=CLIENT, chat_type="group", chat_id="-1001234567890"),
        adapter,
        scheduled.append,
    )
    assert send is not None
    send("⏸ proposal /skills approve 8f2bb0dc")
    assert adapter.sent == [(OWNER, "⏸ proposal /skills approve 8f2bb0dc")]
    assert scheduled, "the send coroutine must be handed to the loop"
    assert all(chat != "-1001234567890" for chat, _ in adapter.sent)


def test_owner_sender_is_none_when_owner_dm_unreachable(hermes_home):
    from gateway import pending_review_access as pra

    assert pra.make_owner_notice_sender(
        _cfg(allow_from=OWNER),
        _source(user_id=CLIENT, chat_type="group", chat_id="-100999"),
        _NoDmAdapter(),
        lambda coro: None,
    ) is None


def test_owner_sender_is_none_without_an_adapter(hermes_home):
    from gateway import pending_review_access as pra

    assert pra.make_owner_notice_sender(
        _cfg(allow_from=OWNER), _source(), None, lambda coro: None
    ) is None


def test_base_adapter_cannot_promise_a_dm():
    """Default must be "unknown", so an unported platform never leaks."""
    from gateway.platforms.base import BasePlatformAdapter

    assert BasePlatformAdapter.dm_chat_id_for_user(None, OWNER) is None


def test_telegram_adapter_dms_by_user_id():
    from plugins.platforms.telegram.adapter import TelegramAdapter

    assert TelegramAdapter.dm_chat_id_for_user(None, OWNER) == OWNER
    assert TelegramAdapter.dm_chat_id_for_user(None, "  ") is None


# ---------------------------------------------------------------------------
# The refusal must not be an oracle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_refusal_does_not_reveal_whether_a_queue_exists(hermes_home):
    """A non-owner must get the SAME answer empty queue or full.

    Otherwise the refusal is an oracle: "there is something to approve" is
    itself information about the bot's internals, and on kivi the asker is a
    Llucky prospect. Compared byte-for-byte on purpose.
    """
    h = _handler(_cfg(allow_from=OWNER + "," + OTHER_ADMIN))
    outsider = dict(user_id=CLIENT, chat_type="dm", chat_id=CLIENT)

    empty = await h._handle_skills_command(_event("/skills pending", **outsider))

    _install_skill(hermes_home)
    _stage_patch()
    _stage_patch(old="step two", new="step two (verified)")
    from tools import write_approval as wa
    assert wa.pending_count(wa.SKILLS) == 2

    full = await h._handle_skills_command(_event("/skills pending", **outsider))
    assert empty == full, (empty, full)
    assert "step one" not in full and "report-writer" not in full, full


@pytest.mark.asyncio
async def test_non_owner_never_sees_the_approval_is_off_hint(hermes_home):
    """The hint itself teaches a stranger that a gate exists and how to flip it.

    With the gate off and nothing staged the handler's first act used to be
    "Skill write approval is off ... Enable it with /skills approval on".
    The owner check has to come BEFORE that, or the refusal leaks the lesson
    it is trying to withhold.
    """
    from tools import write_approval as wa
    assert wa.write_approval_enabled(wa.SKILLS) is False
    assert wa.pending_count(wa.SKILLS) == 0

    h = _handler(_cfg(allow_from=OWNER))
    out = await h._handle_skills_command(
        _event("/skills pending", user_id=CLIENT, chat_type="dm", chat_id=CLIENT)
    )
    assert "approval on" not in out, out
    assert "write_approval" not in out, out
    assert "owner" in out.lower(), out


@pytest.mark.asyncio
async def test_owner_in_a_group_cannot_use_the_handler(hermes_home):
    """Approval is a private decision. Even the owner may not spend it in a group.

    Anyone in the room reads the reply, and on Telegram group membership is
    not the owner's to control. The record must survive untouched.
    """
    d = _install_skill(hermes_home)
    rec = _stage_patch()
    h = _handler(_cfg(allow_from=OWNER, group_allow_from="*"))
    group = dict(user_id=OWNER, chat_type="group", chat_id="-1001234567890")

    listing = await h._handle_skills_command(_event("/skills pending", **group))
    assert rec["id"] not in listing, listing
    assert "direct message" in listing.lower(), listing

    approved = await h._handle_skills_command(
        _event("/skills approve " + rec["id"], **group)
    )
    assert "Approved" not in approved, approved
    with open(os.path.join(d, "SKILL.md"), encoding="utf-8") as f:
        assert "step one (verified)" not in f.read()
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is not None


# ---------------------------------------------------------------------------
# The live kivi shape, spelled out
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_kivi_live_shape_client_refused_and_notice_goes_to_vito(
    hermes_home, monkeypatch
):
    """Reproduces profile kivi as measured on the M1 on 2026-08-12.

    ``TELEGRAM_ALLOWED_USERS=405154434,453088539`` and
    ``TELEGRAM_GROUP_ALLOWED_USERS=*`` in profiles/kivi/.env, no
    ``platforms.telegram.extra`` and no ``gateway.pending_approval_owner`` in
    profiles/kivi/config.yaml, ``skills.write_approval: true``. The wildcard
    means gateway.authz_mixin admits ANY Telegram sender, so without an owner
    gate a prospect could approve. Both halves are asserted here: the client
    is refused, and the proposal notice is addressed to Vito's DM rather than
    to the client chat.
    """
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "405154434,453088539")
    monkeypatch.setenv("TELEGRAM_GROUP_ALLOWED_USERS", "*")
    cfg = _cfg()  # no platforms.telegram.extra, exactly like kivi
    client_chat = "-1003948395916"

    from gateway import pending_review_access as pra
    assert pra.resolve_owner_user_id(cfg, Platform.TELEGRAM) == "405154434"

    _install_skill(hermes_home)
    rec = _stage_patch()
    h = _handler(cfg)
    out = await h._handle_skills_command(
        _event("/skills approve " + rec["id"], user_id=CLIENT,
               chat_type="group", chat_id=client_chat)
    )
    assert "Approved" not in out, out
    from tools import write_approval as wa
    assert wa.get_pending(wa.SKILLS, rec["id"]) is not None

    adapter = _RecordingAdapter()
    send = pra.make_owner_notice_sender(
        cfg,
        _source(user_id=CLIENT, chat_type="group", chat_id=client_chat),
        adapter,
        lambda coro: None,
    )
    assert send is not None
    send("proposal /skills approve " + rec["id"])
    assert adapter.sent == [("405154434", "proposal /skills approve " + rec["id"])]
    assert all(chat != client_chat for chat, _ in adapter.sent), adapter.sent
