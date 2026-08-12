"""Descriptor regression: a pool that stops serving requests must be drained.

WHY THIS FILE EXISTS
--------------------
2026-08-11 19:30 BST the personal gateway suffocated on its 256-descriptor
limit.  lsof at the moment of suffocation: 101 sockets in CLOSE_WAIT, 62 of
them to 149.154.166.110 — which on this host is BOTH the address the system
resolver returns for api.telegram.org and the seed fallback IP.

CLOSE_WAIT means the peer sent FIN and our side never called close().  Three
things were measured on the M1 on 2026-08-12 against a local stub, and they
are what this file encodes:

1. An httpx/httpcore pool NEVER reaps a peer-closed keep-alive socket on its
   own.  ``keepalive_expiry`` is enforced only while a request is passing
   THROUGH that pool.  With ``keepalive_expiry=2.0`` ten FINned sockets were
   still in CLOSE_WAIT after 6s of idling; only ``aclose()`` dropped them to 0.
2. ``TelegramFallbackTransport`` owns N+1 independent pools (``_primary`` plus
   one per fallback IP) but routes each request to exactly ONE of them.  Once a
   sticky fallback IP is established, every request short-circuits on it and
   ``_primary`` is never touched again — so whatever keep-alive sockets it held
   when the sticky flipped stay in CLOSE_WAIT for the life of the process.
   In the incident window the sticky was (re)established 25 times.
3. ``httpx.AsyncHTTPTransport`` is reusable after ``aclose()`` — the pool just
   empties — so draining an idle pool costs nothing but the sockets.

Only the fallback-IP failure branch recycles its pool (``_reset_fallback``, 66
firings in the incident window).  The primary branch never did: 209 "Primary
api.telegram.org connection failed" firings in the same window, zero resets.

The scenario below is that shape, offline: no DNS, no TLS, no api.telegram.org.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import socket
import time

import httpx
import psutil
import pytest

import plugins.platforms.telegram.telegram_network as tnet


# ---------------------------------------------------------------------------
# Local stub server — the only "network" this file touches
# ---------------------------------------------------------------------------

class StubTelegram:
    """HTTP/1.1 keep-alive stub that can FIN its side on demand.

    ``/slow`` holds the connection busy so a concurrent request is forced to
    open a NEW one — that is how the incident's primary path failed while its
    pool still held live keep-alive sockets.
    """

    def __init__(self) -> None:
        self.server: asyncio.AbstractServer | None = None
        self.port: int = 0
        self.writers: list[asyncio.StreamWriter] = []
        self.slow_delay = 1.5

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.append(writer)
        try:
            while True:
                try:
                    head = await reader.readuntil(b"\r\n\r\n")
                except Exception:
                    return
                if b"/slow" in head.split(b"\r\n", 1)[0]:
                    await asyncio.sleep(self.slow_delay)
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                await writer.drain()
        except Exception:
            return
        finally:
            try:
                writer.close()
            except Exception:
                pass

    def stop_listening(self) -> None:
        """Refuse NEW connections; leave established ones alone."""
        if self.server is not None:
            self.server.close()

    def fin_all(self) -> None:
        """Peer-side FIN on every accepted socket (Telegram dropping idles)."""
        for writer in list(self.writers):
            try:
                writer.close()
            except Exception:
                pass
        self.writers.clear()

    async def shutdown(self) -> None:
        self.fin_all()
        if self.server is not None:
            self.server.close()


def _close_wait_to(port: int) -> int:
    return sum(
        1
        for conn in psutil.Process().net_connections(kind="tcp")
        if conn.status == psutil.CONN_CLOSE_WAIT and conn.raddr and conn.raddr.port == port
    )


def _established_to(port: int) -> int:
    return sum(
        1
        for conn in psutil.Process().net_connections(kind="tcp")
        if conn.status == psutil.CONN_ESTABLISHED and conn.raddr and conn.raddr.port == port
    )


def _rewrite_to_stub(request: httpx.Request, ip: str, *, port: int) -> httpx.Request:
    """Stand-in for ``_rewrite_request_for_ip`` that lands on the stub's port.

    The real rewriter keeps the port and only swaps the host; a loopback test
    cannot give two Telegram IPs two different listeners on one port, so the
    fallback leg is redirected by port instead.  Host header and SNI are
    preserved exactly as the real one does — ``TestRewriteRequestForIp`` in
    ``test_telegram_network.py`` covers that contract itself.
    """
    original_host = request.url.host
    url = request.url.copy_with(host="127.0.0.1", port=port)
    headers = request.headers.copy()
    headers["host"] = original_host
    extensions = dict(request.extensions)
    extensions["sni_hostname"] = original_host
    return httpx.Request(
        method=request.method,
        url=url,
        headers=headers,
        stream=request.stream,
        extensions=extensions,
    )


@pytest.mark.asyncio
async def test_idle_primary_pool_releases_peer_closed_sockets(monkeypatch):
    """After the sticky fallback takes over, the primary pool must not hold fds.

    Reproduces the 2026-08-11 shape end to end:

      A. the primary (DNS) path serves traffic and its pool fills with
         keep-alive sockets;
      B. a burst forces one NEW primary connection, which fails — exactly the
         209 "Primary api.telegram.org connection failed" events — so the
         fallback IP takes over and becomes sticky;
      C. the peer FINs the primary pool's now-idle sockets;
      D. traffic keeps flowing on the sticky IP.

    At the end of D not one descriptor may still be held for the primary peer.
    """
    primary = StubTelegram()
    fallback = StubTelegram()
    await primary.start()
    await fallback.start()

    # Offline: the "Telegram host" is the primary stub, the "fallback IP" is a
    # real routable-looking address redirected to the fallback stub.
    monkeypatch.setattr(tnet, "_TELEGRAM_API_HOST", "127.0.0.1")
    monkeypatch.setattr(tnet, "_resolve_proxy_url", lambda target_hosts=None: None)
    monkeypatch.setattr(
        tnet, "_rewrite_request_for_ip", functools.partial(_rewrite_to_stub, port=fallback.port)
    )

    transport = tnet.TelegramFallbackTransport(
        ["149.154.166.110"],
        limits=httpx.Limits(
            max_connections=512, max_keepalive_connections=10, keepalive_expiry=30.0
        ),
    )
    client = httpx.AsyncClient(transport=transport)

    base = f"http://127.0.0.1:{primary.port}"

    async def get(path: str) -> httpx.Response:
        response = await client.get(f"{base}{path}")
        await response.aread()
        return response

    try:
        # ── A. primary path serves; its pool fills with keep-alive sockets ──
        await asyncio.gather(*[get("/fast") for _ in range(10)])
        assert transport._sticky_ip is None
        assert _established_to(primary.port) == 10

        # ── B. a burst forces a NEW primary connection, which is refused ────
        slow = [asyncio.create_task(get("/slow")) for _ in range(10)]
        await asyncio.sleep(0.3)          # let them occupy all ten pooled sockets
        primary.stop_listening()
        await asyncio.sleep(0.05)

        forced = await get("/fast")       # primary must fail -> fallback serves
        assert forced.status_code == 200
        assert transport._sticky_ip == "149.154.166.110"
        await asyncio.gather(*slow)

        # ── C. the peer FINs the primary pool's now-idle sockets ────────────
        primary.fin_all()
        await asyncio.sleep(0.3)
        stranded = _close_wait_to(primary.port)
        assert stranded > 0, (
            "test setup failed: the stub did not strand any socket in CLOSE_WAIT"
        )

        # ── D. traffic keeps flowing on the sticky IP ───────────────────────
        for _ in range(10):
            assert (await get("/fast")).status_code == 200

        await asyncio.sleep(0.2)
        assert _close_wait_to(primary.port) == 0, (
            f"{_close_wait_to(primary.port)} descriptor(s) still held for a peer "
            f"that closed its side ({stranded} were stranded in step C); this is "
            "the leak that exhausted the gateway's 256 fds on 2026-08-11"
        )
    finally:
        await client.aclose()
        await primary.shutdown()
        await fallback.shutdown()


class _WedgedClose:
    """Pool delegate whose ``aclose()`` never returns (a wedged CLOSE_WAIT socket)."""

    def __init__(self, inner):
        self._inner = inner
        self.close_attempts = 0

    async def handle_async_request(self, request):
        return await self._inner.handle_async_request(request)

    async def aclose(self):
        self.close_attempts += 1
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_wedged_idle_pool_does_not_stall_the_request_path(monkeypatch):
    """Draining an idle pool must be bounded, and must not be retried forever.

    The reaper runs on the request path, and a pool holding a wedged
    CLOSE_WAIT socket can make ``close()`` hang indefinitely — the hazard that
    already forced ``TelegramAdapter._drain_polling_connections`` to bound its
    own shutdown (#66377). An unbounded reaper would convert a leaked
    descriptor into a frozen bot, which is strictly worse.

    Two things are asserted, because one without the other is a trap:
      * the request still completes, in well under the wedge;
      * the wedged pool is attempted ONCE — otherwise every later request pays
        ``_IDLE_POOL_CLOSE_TIMEOUT`` again.
    """
    fallback = StubTelegram()
    await fallback.start()

    monkeypatch.setattr(tnet, "_TELEGRAM_API_HOST", "127.0.0.1")
    monkeypatch.setattr(tnet, "_resolve_proxy_url", lambda target_hosts=None: None)
    monkeypatch.setattr(
        tnet, "_rewrite_request_for_ip", functools.partial(_rewrite_to_stub, port=fallback.port)
    )
    # raising=False so that a build WITHOUT the reaper fails on the contract
    # below, not on the setattr — a setup error is not a red test.
    monkeypatch.setattr(tnet, "_IDLE_POOL_CLOSE_TIMEOUT", 0.3, raising=False)

    transport = tnet.TelegramFallbackTransport(["149.154.166.110"])
    wedged = _WedgedClose(transport._primary)
    transport._primary = wedged
    client = httpx.AsyncClient(transport=transport)

    # Nothing listens on this port, so the primary attempt fails and the
    # fallback serves — leaving the wedged primary pool idle and reapable.
    dead = socket.socket()
    dead.bind(("127.0.0.1", 0))
    dead_port = dead.getsockname()[1]
    dead.close()

    try:
        started = time.monotonic()
        for _ in range(4):
            # Bounded on purpose: an UNBOUNDED drain makes this hang forever,
            # and a hanging suite is not a test result. Five seconds is far
            # outside anything the bounded path can produce.
            response = await asyncio.wait_for(
                client.get(f"http://127.0.0.1:{dead_port}/x"), timeout=5.0
            )
            await response.aread()
            assert response.status_code == 200
        elapsed = time.monotonic() - started

        assert wedged.close_attempts == 1, (
            f"the wedged pool was re-drained {wedged.close_attempts} times; every "
            "request after the first pays the full close timeout again"
        )
        assert elapsed < 2.0, (
            f"four requests took {elapsed:.1f}s — the reaper is stalling the "
            "request path on a pool that will not close"
        )
    finally:
        # transport.aclose() is deliberately UNBOUNDED — its callers bound it
        # (TelegramAdapter._drain_polling_connections wraps it in wait_for), and
        # here the primary pool never returns from close. Bounding it is also
        # the point: because aclose() closes the fallbacks BEFORE the primary,
        # a bounded caller still gets the fallback descriptors back.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(client.aclose(), timeout=1.0)
        await fallback.shutdown()
