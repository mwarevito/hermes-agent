"""Telegram-specific network helpers.

Provides a hostname-preserving fallback transport for networks where
api.telegram.org resolves to an endpoint that is unreachable from the current
host. The transport keeps the logical request host and TLS SNI as
api.telegram.org while retrying the TCP connection against one or more fallback
IPv4 addresses.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
from typing import Iterable, Optional

import httpx

logger = logging.getLogger(__name__)

_TELEGRAM_API_HOST = "api.telegram.org"

# Key used for the primary (system-DNS) pool in the in-flight bookkeeping. A
# fallback IP can never collide with it: "" is not a valid address.
_PRIMARY_POOL_KEY = ""

# Ceiling on draining ONE idle pool. This runs on the request path, and a pool
# holding a wedged CLOSE_WAIT socket can make close() hang forever — the same
# hazard that made TelegramAdapter._drain_polling_connections bound its own
# shutdown (#66377). An idle pool closes in microseconds, so this is three
# orders of magnitude of headroom; its real job is to cap what a pool that will
# not close can cost the bot — half a second, once, and never again for that
# pool.
_IDLE_POOL_CLOSE_TIMEOUT = 0.5

# DNS-over-HTTPS providers used to discover Telegram API IPs that may differ
# from the (potentially unreachable) IP returned by the local system resolver.
_DOH_TIMEOUT = 4.0  # seconds — bounded so connect() isn't noticeably delayed

_DOH_PROVIDERS: list[dict] = [
    {
        "url": "https://dns.google/resolve",
        "params": {"name": _TELEGRAM_API_HOST, "type": "A"},
        "headers": {},
    },
    {
        "url": "https://cloudflare-dns.com/dns-query",
        "params": {"name": _TELEGRAM_API_HOST, "type": "A"},
        "headers": {"Accept": "application/dns-json"},
    },
]

# Last-resort IPs when DoH is also blocked.  These are stable Telegram Bot API
# endpoints in the 149.154.160.0/20 block (same seed used by OpenClaw).
_SEED_FALLBACK_IPS: list[str] = ["149.154.166.110", "149.154.167.220"]


def _resolve_proxy_url(target_hosts=None) -> str | None:
    # Delegate to shared implementation (env vars + macOS system proxy detection)
    from gateway.platforms.base import resolve_proxy_url
    return resolve_proxy_url("TELEGRAM_PROXY", target_hosts=target_hosts)


class TelegramFallbackTransport(httpx.AsyncBaseTransport):
    """Retry Telegram Bot API requests via fallback IPs while preserving TLS/SNI.

    Requests continue to target https://api.telegram.org/... logically, but on
    connect failures the underlying TCP connection is retried against a known
    reachable IP. This is effectively the programmatic equivalent of
    ``curl --resolve api.telegram.org:443:<ip>``.
    """

    # Bound every pool. httpx defaults to 100 connections per pool, so a wedged
    # endpoint plus the seed IPs can outgrow the process file-descriptor limit
    # on its own (#63311).
    _POOL_LIMITS = httpx.Limits(max_connections=8, max_keepalive_connections=4)

    def __init__(self, fallback_ips: Iterable[str], **transport_kwargs):
        self._fallback_ips = list(dict.fromkeys(_normalize_fallback_ips(fallback_ips)))
        proxy_url = _resolve_proxy_url(target_hosts=[_TELEGRAM_API_HOST, *self._fallback_ips])
        if proxy_url and "proxy" not in transport_kwargs:
            transport_kwargs["proxy"] = proxy_url
        transport_kwargs.setdefault("limits", self._POOL_LIMITS)
        self._transport_kwargs = transport_kwargs
        self._primary = httpx.AsyncHTTPTransport(**transport_kwargs)
        # Built on demand and discarded on failure — see _reset_fallback.
        self._fallbacks: dict[str, httpx.AsyncHTTPTransport] = {}
        self._fallback_lock = asyncio.Lock()
        self._sticky_ip: Optional[str] = None
        self._sticky_lock = asyncio.Lock()
        # Pools with work still on them, keyed like _pool_key(). A pool is
        # counted from the moment its attempt starts until the response body is
        # closed, so the idle reaper below can never cut a streaming download.
        self._pool_inflight: dict[str, int] = {}
        # Pools whose drain failed or timed out. They are skipped by the idle
        # reaper afterwards — retrying a close that hangs would put
        # _IDLE_POOL_CLOSE_TIMEOUT on every subsequent request — and cleared
        # again the moment the pool serves a request, i.e. proves it is healthy.
        self._undrainable_pools: set[str] = set()
        # Pool that served the previous request: the one the idle reaper spares,
        # because it is almost certainly the one this request will use too.
        self._last_active_key: Optional[str] = None

    async def _get_fallback(self, ip: str) -> httpx.AsyncHTTPTransport:
        async with self._fallback_lock:
            transport = self._fallbacks.get(ip)
            if transport is None:
                transport = httpx.AsyncHTTPTransport(**self._transport_kwargs)
                self._fallbacks[ip] = transport
            return transport

    async def _reset_fallback(self, ip: str) -> None:
        """Discard a failed fallback pool so its dead sockets are released.

        A connect that reaches ESTABLISHED and is then closed by the peer leaves
        its socket in CLOSE_WAIT inside the pool. Retaining the poisoned pool
        leaks one descriptor per retry until the process hits its file limit and
        can no longer accept connections or resolve DNS (#63311).
        """
        async with self._fallback_lock:
            transport = self._fallbacks.pop(ip, None)
        if transport is None:
            return
        try:
            await transport.aclose()
        except Exception as exc:  # closing a broken pool must never mask the real error
            # The pool is already out of _fallbacks, so nothing will ever try to
            # close it again: whatever sockets it still holds are leaked for the
            # life of the process. That is a descriptor budget being spent, not
            # a detail — it is reported, never whispered.
            logger.warning(
                "[Telegram] Fallback pool %s refused to close (%s: %s); its sockets "
                "are leaked until the process exits",
                ip, type(exc).__name__, exc,
            )

    @staticmethod
    def _pool_key(ip: Optional[str]) -> str:
        """Identity of the pool serving an attempt (``""`` = the primary pool)."""
        return ip if ip is not None else _PRIMARY_POOL_KEY

    def _acquire_pool(self, key: str) -> None:
        self._pool_inflight[key] = self._pool_inflight.get(key, 0) + 1

    def _release_pool(self, key: str) -> None:
        remaining = self._pool_inflight.get(key, 0) - 1
        if remaining > 0:
            self._pool_inflight[key] = remaining
        else:
            self._pool_inflight.pop(key, None)

    def _pools_by_key(self) -> list[tuple[str, httpx.AsyncHTTPTransport]]:
        pools: list[tuple[str, httpx.AsyncHTTPTransport]] = [
            (_PRIMARY_POOL_KEY, self._primary)
        ]
        pools.extend(self._fallbacks.items())
        return pools

    async def _drain_unused_pools(self, active_key: Optional[str]) -> None:
        """Close every pool this transport owns except the one still in use.

        This is the descriptor half of the fallback ladder, and it exists
        because of a property of httpx that is easy to assume away: a pool
        NEVER reaps a peer-closed keep-alive socket on its own.
        ``keepalive_expiry`` is only enforced while a request is passing
        *through* that pool. Measured on the M1 2026-08-12 against a stub that
        FINs idle sockets: with ``keepalive_expiry=2.0`` ten sockets were still
        in CLOSE_WAIT after six seconds of idling; only ``aclose()`` freed them.

        This transport owns N+1 pools but routes each request to exactly ONE of
        them, so the others are idle by construction. Once a sticky fallback IP
        is established every request short-circuits on it and ``_primary`` is
        never touched again — and the sockets it was holding at that moment are
        held forever. That is the 2026-08-11 19:30 EMFILE incident: 101 sockets
        in CLOSE_WAIT, 62 of them to 149.154.166.110, which on this host is both
        the system-DNS answer for api.telegram.org and the seed fallback IP.
        The fallback-IP failure branch already recycled its pool
        (``_reset_fallback``, 66 firings in the incident window); the primary
        branch reset nothing across 209 firings of its own.

        Closing does NOT discard the transport object: ``AsyncHTTPTransport``
        is reusable after ``aclose()`` (verified 2026-08-12 — the pool simply
        empties), so no SSL context is rebuilt and the next attempt cannot fail
        on rebuilding one under descriptor pressure.

        A pool with work in flight is never touched: ``_pool_inflight`` counts
        an attempt from its start until the response body is closed.
        """
        for key, transport in self._pools_by_key():
            if key == active_key or self._pool_inflight.get(key):
                continue
            if key in self._undrainable_pools:
                continue
            try:
                await asyncio.wait_for(
                    transport.aclose(), timeout=_IDLE_POOL_CLOSE_TIMEOUT
                )
            except Exception as exc:
                # Never mask the request's own outcome, but never hide this
                # either: a pool that will not close is descriptors we cannot
                # get back. Reported once, then skipped — a close that hangs
                # must not be re-awaited on every future request.
                self._undrainable_pools.add(key)
                logger.warning(
                    "[Telegram] Idle pool %s refused to close (%s: %s); its sockets "
                    "stay open until this pool serves a request again or the "
                    "process exits, and it will not be re-drained until then",
                    key or "primary", type(exc).__name__, exc,
                )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.host != _TELEGRAM_API_HOST or not self._fallback_ips:
            return await self._primary.handle_async_request(request)

        sticky_ip = self._sticky_ip
        attempt_order: list[Optional[str]] = [sticky_ip] if sticky_ip else [None]
        if sticky_ip:
            attempt_order.append(None)  # retry primary DNS after sticky failure
        for ip in self._fallback_ips:
            if ip != sticky_ip:
                attempt_order.append(ip)

        # Drain the pools the PREVIOUS request left idle, before this one
        # starts. Deliberately not afterwards: a drain sitting between "response
        # ready" and "response returned" delays handing the body to the caller,
        # and keeps the connection checked out of its pool across that delay —
        # so a peer FIN that would have landed on a parked keep-alive socket
        # lands mid-response instead. Same descriptors freed, none of that.
        await self._drain_unused_pools(self._last_active_key)

        last_error: Exception | None = None
        for ip in attempt_order:
            key = self._pool_key(ip)
            candidate = request if ip is None else _rewrite_request_for_ip(request, ip)
            transport = self._primary if ip is None else await self._get_fallback(ip)
            self._acquire_pool(key)
            try:
                response = await transport.handle_async_request(candidate)
            except Exception as exc:
                self._release_pool(key)
                last_error = exc
                if not _is_retryable_connect_error(exc):
                    raise
                if ip is not None and ip == self._sticky_ip:
                    async with self._sticky_lock:
                        if self._sticky_ip == ip:
                            self._sticky_ip = None
                            logger.warning(
                                "[Telegram] Sticky fallback IP %s failed; resetting to primary DNS path",
                                ip,
                            )
                if ip is None:
                    logger.warning(
                        "[Telegram] Primary api.telegram.org connection failed (%s); trying fallback IPs %s",
                        exc,
                        ", ".join(self._fallback_ips),
                    )
                    continue
                logger.warning("[Telegram] Fallback IP %s failed: %s", ip, exc)
                await self._reset_fallback(ip)
                continue

            # The pool stays "in use" until the caller closes the body, so a
            # concurrent request's drain cannot cut a response still arriving.
            self._last_active_key = key
            # It served a request, so whatever made its close fail before
            # is no longer assumed: let the reaper try it again.
            self._undrainable_pools.discard(key)
            _attach_pool_release(response, self, key)
            if ip is not None and self._sticky_ip != ip:
                async with self._sticky_lock:
                    if self._sticky_ip != ip:
                        self._sticky_ip = ip
                        logger.warning(
                            "[Telegram] Primary api.telegram.org path unreachable; using sticky fallback IP %s",
                            ip,
                        )
            return response

        if last_error is None:
            raise RuntimeError("All Telegram fallback IPs exhausted but no error was recorded")
        raise last_error

    async def aclose(self) -> None:
        """Close every pool this transport owns, isolating per-pool failures.

        This transport owns N+1 independent httpx pools (``_primary`` plus one
        per fallback IP), so the order and the failure isolation are the whole
        contract, not style.

        The previous body closed ``_primary`` first, unguarded, and then looped
        over the fallbacks unguarded. Either an exception or a wedged close on
        one pool stranded all the pools behind it — and a stranded fallback pool
        stays referenced by ``self._fallbacks`` (PTB reuses this transport
        object across ``HTTPXRequest.initialize()``), so its descriptors are not
        even reclaimable by GC. Measured on the M1 2026-08-12 against a peer
        that FINs an idle keep-alive socket: 4/4 fallback sockets stayed in
        CLOSE_WAIT; with this body, 0/4. That is the shape of the 2026-08-11
        19:37 BST EMFILE incident (101 CLOSE_WAIT sockets, 62 of them to the
        sticky fallback IP 149.154.166.110).

        ``_primary`` is closed LAST on purpose: when the network breaks it is
        the pool most likely to be the wedged one (system DNS dead, traffic
        pinned to a fallback IP), and the caller may bound this close —
        ``TelegramAdapter._drain_polling_connections`` wraps it in
        ``asyncio.wait_for(..., _DRAIN_TIMEOUT)``. Closing the fallbacks first
        means a bounded caller still gets their descriptors back.

        A pool that refuses to close means leaked descriptors, so failures are
        logged at WARNING and the first one is re-raised — never swallowed.
        """
        async with self._fallback_lock:
            transports: list = list(self._fallbacks.values())
            self._fallbacks.clear()
        transports.append(self._primary)

        first_error: Exception | None = None
        for transport in transports:
            try:
                await transport.aclose()
            except Exception as exc:
                logger.warning(
                    "[Telegram] Failed to close an httpx pool (%s: %s); its sockets "
                    "stay open until the process exits",
                    type(exc).__name__,
                    exc,
                )
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error


def _normalize_fallback_ips(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        raw = str(value).strip()
        if not raw:
            continue
        try:
            addr = ipaddress.ip_address(raw)
        except ValueError:
            logger.warning("Ignoring invalid Telegram fallback IP: %r", raw)
            continue
        if addr.version != 4:
            logger.warning("Ignoring non-IPv4 Telegram fallback IP: %s", raw)
            continue
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_unspecified:
            logger.warning("Ignoring private/internal Telegram fallback IP: %s", raw)
            continue
        normalized.append(str(addr))
    return normalized


def parse_fallback_ip_env(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [part.strip() for part in value.split(",")]
    return _normalize_fallback_ips(parts)


def _resolve_system_dns() -> set[str]:
    """Return the IPv4 addresses that the OS resolver gives for api.telegram.org."""
    try:
        results = socket.getaddrinfo(_TELEGRAM_API_HOST, 443, socket.AF_INET)
        return {addr[4][0] for addr in results}
    except Exception:
        return set()


async def _query_doh_provider(
    client: httpx.AsyncClient, provider: dict
) -> list[str]:
    """Query one DoH provider and return A-record IPs."""
    try:
        resp = await client.get(
            provider["url"], params=provider["params"], headers=provider["headers"]
        )
        resp.raise_for_status()
        data = resp.json()
        ips: list[str] = []
        for answer in data.get("Answer", []):
            if answer.get("type") != 1:  # A record
                continue
            raw = answer.get("data", "").strip()
            try:
                ipaddress.ip_address(raw)
                ips.append(raw)
            except ValueError:
                continue
        return ips
    except Exception as exc:
        logger.debug("DoH query to %s failed: %s", provider["url"], exc)
        return []


async def discover_fallback_ips() -> list[str]:
    """Auto-discover Telegram API IPs via DNS-over-HTTPS.

    Resolves api.telegram.org through Google and Cloudflare DoH and returns all
    unique A records.  IPs that match the local system resolver are kept rather
    than excluded: in many networks the system-DNS IP is the most reliable path
    to api.telegram.org and a transient primary-path failure should be retried
    against the same address via the IP-rewrite path before the seed list is
    consulted (#14520).  Falls back to a hardcoded seed list only when DoH
    yields no usable answers.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(_DOH_TIMEOUT)) as client:
        doh_tasks = [_query_doh_provider(client, p) for p in _DOH_PROVIDERS]
        system_dns_task = asyncio.ensure_future(asyncio.to_thread(_resolve_system_dns))
        results = await asyncio.gather(*doh_tasks, return_exceptions=True)

    # The system-resolver leg runs socket.getaddrinfo in a worker thread with
    # no timeout of its own — a wedged OS resolver (broken VPN/DNS) can sit for
    # minutes. Its result only feeds the no-usable-answers log line below, so
    # it must never gate discovery: bound it and move on (#63309). The DoH legs
    # are already bounded by the client timeout above.
    system_ips: set[str] = set()
    try:
        system_result = await asyncio.wait_for(system_dns_task, timeout=_DOH_TIMEOUT)
        if isinstance(system_result, set):
            system_ips = system_result
    except Exception:
        logger.debug("System-DNS resolution for %s did not complete in time", _TELEGRAM_API_HOST)

    doh_ips: list[str] = []
    for r in results:
        if isinstance(r, list):
            doh_ips.extend(r)

    # Deduplicate preserving order
    seen: set[str] = set()
    candidates: list[str] = []
    for ip in doh_ips:
        if ip not in seen:
            seen.add(ip)
            candidates.append(ip)

    # Validate through existing normalization
    validated = _normalize_fallback_ips(candidates)

    if validated:
        logger.debug("Discovered Telegram fallback IPs via DoH: %s", ", ".join(validated))
        return validated

    logger.info(
        "DoH discovery yielded no usable IPs (system DNS: %s); using seed fallback IPs %s",
        ", ".join(system_ips) or "unknown",
        ", ".join(_SEED_FALLBACK_IPS),
    )
    return list(_SEED_FALLBACK_IPS)


def _rewrite_request_for_ip(request: httpx.Request, ip: str) -> httpx.Request:
    original_host = request.url.host or _TELEGRAM_API_HOST
    url = request.url.copy_with(host=ip)
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


class _PoolReleasingStream(httpx.AsyncByteStream):
    """Marks a pool busy until the response body it carries is closed.

    ``handle_async_request`` returns as soon as the response HEADERS are in, but
    the connection stays checked out of the pool until the body is drained. The
    idle-pool reaper therefore cannot use "the transport call returned" as the
    end of the attempt — a concurrent request finishing elsewhere would close
    the pool underneath an in-progress download. Release happens on
    ``aclose()``, which httpx guarantees for every response it hands out.
    """

    def __init__(self, inner, release) -> None:
        self._inner = inner
        self._release = release
        self._released = False

    async def __aiter__(self):
        async for chunk in self._inner:
            yield chunk

    async def aclose(self) -> None:
        try:
            inner_aclose = getattr(self._inner, "aclose", None)
            if inner_aclose is not None:
                await inner_aclose()
        finally:
            if not self._released:
                self._released = True
                self._release()


def _attach_pool_release(response: httpx.Response, transport, key: str) -> None:
    """Keep ``key`` marked in-flight until ``response``'s body is closed."""
    stream = getattr(response, "stream", None)
    if stream is None or not hasattr(stream, "__aiter__"):
        # Nothing to wait for (a fully-materialised response): the attempt is
        # over the moment we get here.
        transport._release_pool(key)
        return
    response.stream = _PoolReleasingStream(stream, lambda: transport._release_pool(key))


def _is_retryable_connect_error(exc: Exception) -> bool:
    return isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError))
