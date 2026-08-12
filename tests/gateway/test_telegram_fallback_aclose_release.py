"""Real-socket regression test — TelegramFallbackTransport must release every
pool it owns when one pool's close fails or wedges.

Background (M1 personal gateway, 2026-08-11 19:37 BST)
------------------------------------------------------
The gateway ran out of its 256 file descriptors.  ``lsof`` at the moment of
strangulation showed 101 sockets in ``CLOSE_WAIT``, 62 of them to the Telegram
DC 149.154.166.110 — the sticky fallback IP the bot had been pinned to while
system DNS was broken.  ``CLOSE_WAIT`` means the peer sent FIN and *our* side
never called ``close()``.

``TelegramFallbackTransport`` owns N+1 independent httpx pools: ``_primary``
plus one ``_fallbacks[ip]`` per fallback IP.  The pre-fix ``aclose()`` was::

    await self._primary.aclose()          # (1)
    async with self._fallback_lock:       # (2) never reached if (1) fails
        ...
    for transport in transports:          # (3) aborts on the first failure
        await transport.aclose()

so a single unhealthy pool stranded all the others.  That is not a theoretical
ordering nit: the adapter *already* assumes this close can wedge —
``TelegramAdapter._drain_polling_connections`` wraps it in
``asyncio.wait_for(polling_req.shutdown(), _DRAIN_TIMEOUT)`` with the comment
"a wedged CLOSE-WAIT socket can make this close hang forever".  And on a broken
network the wedged pool is precisely ``_primary`` (DNS dead), while the pools
holding the live sockets are the fallbacks.  Worse, when (2) is skipped the
fallback pools stay referenced by ``self._fallbacks``, and PTB reuses the same
transport object across ``HTTPXRequest.initialize()`` — so those descriptors
are not even reclaimable by GC.

Contract asserted here (mutation-survivable)
--------------------------------------------
With real sockets against a peer that half-closes after responding: when the
primary pool's ``aclose()`` raises, or wedges and the caller bounds the wait
(exactly what ``_drain_polling_connections`` does), the fallback pool's sockets
must still be closed — zero client sockets left to the peer.  Restore either
half of the old body (primary first, or no per-pool ``try``) and both tests
fail with the CLOSE_WAIT sockets still held.

The peer runs in a SEPARATE PROCESS on purpose: an in-process stub server would
also own the accepted server-side sockets and the count would measure nothing.
No test here touches the real Telegram or the network beyond loopback.
"""

import asyncio
import socket
import subprocess
import sys
import time

import httpx
import psutil
import pytest

import plugins.platforms.telegram.telegram_network as tnet

# A peer that answers one request per connection, lets the client park the
# connection back in its keep-alive pool, and only THEN half-closes (FIN) while
# keeping the socket referenced.  That is the production shape — Telegram
# dropping an idle keep-alive socket — and it is what leaves the client side in
# CLOSE_WAIT: a FIN that arrives *during* the response is consumed by httpcore
# and the connection is torn down straight away.  Prints its port on stdout.
_PEER_HOLD_BEFORE_FIN_S = 0.4

_HALF_CLOSING_PEER = r"""
import socket, sys, threading, time

HOLD = float(sys.argv[1])
held = []

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", 0))
srv.listen(64)
sys.stdout.write("%d\n" % srv.getsockname()[1])
sys.stdout.flush()


def serve(conn):
    try:
        conn.settimeout(10)
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                return
            buf += chunk
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"
        )
        time.sleep(HOLD)
        conn.shutdown(socket.SHUT_WR)
        held.append(conn)
    except Exception:
        pass


while True:
    try:
        c, _ = srv.accept()
    except Exception:
        break
    threading.Thread(target=serve, args=(c,), daemon=True).start()
"""

_FALLBACK_IP = "149.154.167.220"  # never dialled: the rewrite is redirected

# How many peer-FIN'd sockets the setup must park before the assertion means
# anything.  Four concurrent requests open four pooled connections; requiring
# two keeps the test honest without depending on exact pool reuse.
_MIN_PARKED = 2


class _WedgedClose:
    """Real pool delegate whose ``aclose()`` never returns."""

    def __init__(self, inner):
        self._inner = inner

    async def handle_async_request(self, request):
        return await self._inner.handle_async_request(request)

    async def aclose(self):
        await asyncio.Event().wait()


class _FailingClose:
    """Real pool delegate whose ``aclose()`` raises."""

    def __init__(self, inner):
        self._inner = inner

    async def handle_async_request(self, request):
        return await self._inner.handle_async_request(request)

    async def aclose(self):
        raise RuntimeError("simulated wedged pool close")


def _client_sockets_to(port):
    """Sockets *this* process holds towards ``port`` (any TCP state)."""
    return [
        conn.status
        for conn in psutil.Process().net_connections(kind="tcp")
        if conn.raddr and conn.raddr.port == port
    ]


def _free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


async def _wait_for_sockets(port, predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    states = _client_sockets_to(port)
    while not predicate(states) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        states = _client_sockets_to(port)
    return states


@pytest.fixture
def half_closing_peer():
    proc = subprocess.Popen(
        [sys.executable, "-c", _HALF_CLOSING_PEER, str(_PEER_HOLD_BEFORE_FIN_S)],
        stdout=subprocess.PIPE,
    )
    try:
        port = int(proc.stdout.readline().decode().strip())
        yield port
    finally:
        proc.kill()
        proc.wait()


@pytest.fixture
def local_only(monkeypatch, half_closing_peer):
    """Point the transport's whole request path at loopback.

    ``_TELEGRAM_API_HOST`` becomes 127.0.0.1 so the fallback ladder engages,
    and the IP-rewrite seam redirects the fallback attempt to the local peer.
    Everything else in ``handle_async_request`` — pool creation, sticky-IP
    bookkeeping, error classification, ``aclose()`` — runs unmodified.
    """
    monkeypatch.setattr(tnet, "_TELEGRAM_API_HOST", "127.0.0.1")
    monkeypatch.setattr(tnet, "_resolve_proxy_url", lambda *a, **k: None)

    def _rewrite(request, ip):
        url = request.url.copy_with(host="127.0.0.1", port=half_closing_peer)
        headers = request.headers.copy()
        headers["host"] = "api.telegram.org"
        extensions = dict(request.extensions)
        extensions["sni_hostname"] = "api.telegram.org"
        return httpx.Request(
            method=request.method,
            url=url,
            headers=headers,
            stream=request.stream,
            extensions=extensions,
        )

    monkeypatch.setattr(tnet, "_rewrite_request_for_ip", _rewrite)
    return half_closing_peer


async def _fill_fallback_pool(transport, dead_port, count=4):
    """Drive ``count`` concurrent requests so the fallback pool holds sockets."""

    async def one(index):
        request = httpx.Request(
            "GET", f"http://127.0.0.1:{dead_port}/botTOKEN/getUpdates?i={index}"
        )
        try:
            response = await transport.handle_async_request(request)
            await response.aread()
            await response.aclose()
        except Exception:
            # A pooled connection the peer already FIN'd surfaces as ReadError.
            # Irrelevant here: the socket is what this test measures.
            pass

    await asyncio.gather(*(one(i) for i in range(count)))


def _build_transport():
    # Same shape the adapter builds: a caller-supplied limits kwarg with the
    # gateway's tuned keepalive.
    limits = httpx.Limits(
        max_connections=512, max_keepalive_connections=10, keepalive_expiry=2.0
    )
    transport = tnet.TelegramFallbackTransport([_FALLBACK_IP], limits=limits)
    assert transport._fallback_ips == [_FALLBACK_IP], (
        "fallback IP was normalised away — test setup is wrong"
    )
    return transport


@pytest.mark.asyncio
async def test_failing_primary_close_still_releases_fallback_sockets(local_only):
    peer_port = local_only
    dead_port = _free_port()  # refused → primary attempt fails → fallback used
    transport = _build_transport()
    transport._primary = _FailingClose(transport._primary)

    await _fill_fallback_pool(transport, dead_port)
    held = await _wait_for_sockets(
        peer_port, lambda s: s.count("CLOSE_WAIT") >= _MIN_PARKED
    )
    assert held.count("CLOSE_WAIT") >= _MIN_PARKED, (
        f"setup parked no CLOSE_WAIT sockets on the peer (saw {held}) — the "
        f"assertion below would pass vacuously"
    )

    with pytest.raises(RuntimeError):
        await transport.aclose()

    left = await _wait_for_sockets(peer_port, lambda s: not s)
    assert left == [], (
        f"aclose() left {len(left)} socket(s) {left} open to the peer after the "
        f"primary pool's close failed — the fallback pool was never closed "
        f"(2026-08-11 CLOSE_WAIT fd leak)"
    )


@pytest.mark.asyncio
async def test_wedged_primary_close_still_releases_fallback_sockets(local_only):
    peer_port = local_only
    dead_port = _free_port()
    transport = _build_transport()
    transport._primary = _WedgedClose(transport._primary)

    await _fill_fallback_pool(transport, dead_port)
    held = await _wait_for_sockets(
        peer_port, lambda s: s.count("CLOSE_WAIT") >= _MIN_PARKED
    )
    assert held.count("CLOSE_WAIT") >= _MIN_PARKED, (
        f"setup parked no CLOSE_WAIT sockets on the peer (saw {held}) — the "
        f"assertion below would pass vacuously"
    )

    # Exactly what TelegramAdapter._drain_polling_connections does with
    # _DRAIN_TIMEOUT: bound the close and move on.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(transport.aclose(), timeout=1.0)

    left = await _wait_for_sockets(peer_port, lambda s: not s)
    assert left == [], (
        f"aclose() left {len(left)} socket(s) {left} open to the peer after the "
        f"primary pool's close wedged — the fallback pool must be closed before "
        f"the pool that can hang (2026-08-11 CLOSE_WAIT fd leak)"
    )


@pytest.mark.asyncio
async def test_one_failing_pool_does_not_strand_the_others(local_only):
    """Isolation, not just ordering: one bad pool must not skip the rest.

    Ordering alone is not enough — the fallback pools are closed in dict order,
    so a pool that raises must not abort the loop before the pools behind it.
    Drop the per-pool ``try`` in ``aclose()`` and this fails while the two
    ordering tests above still pass.
    """
    peer_port = local_only
    dead_port = _free_port()
    transport = _build_transport()

    await _fill_fallback_pool(transport, dead_port)
    held = await _wait_for_sockets(
        peer_port, lambda s: s.count("CLOSE_WAIT") >= _MIN_PARKED
    )
    assert held.count("CLOSE_WAIT") >= _MIN_PARKED, (
        f"setup parked no CLOSE_WAIT sockets on the peer (saw {held}) — the "
        f"assertion below would pass vacuously"
    )

    # A second, socket-free pool whose close raises, ordered ahead of the real
    # one (dict order is close order).
    transport._fallbacks = {
        "149.154.166.110": _FailingClose(httpx.AsyncHTTPTransport()),
        **transport._fallbacks,
    }

    with pytest.raises(RuntimeError):
        await transport.aclose()

    left = await _wait_for_sockets(peer_port, lambda s: not s)
    assert left == [], (
        f"aclose() stopped at the first failing pool and left {len(left)} "
        f"socket(s) {left} open in the pools behind it"
    )
