"""event_bus.py — thread- and asyncio-aware dispatch over blinker.

Why this exists
---------------
blinker delivers signals synchronously on the publisher's thread. For
TSIv2 that's a problem: pyads notifications fire on the AmsRouter daemon
thread and pysickudt receivers fire on UDP receiver threads. Doing
blocking work (DB writes, PLC writes, RWS HTTP calls) on those threads
risks dropping data or jamming the receiver.

EventBus offers three subscription modes so each subscriber declares its
own thread requirements at registration time:

* ``"sync"``   — runs on the publisher's thread (blinker default). Use
                 only for trivial work: caching a value, setting a flag.
* ``"thread"`` — handler runs on a small ThreadPoolExecutor. Use for any
                 blocking I/O. Safe to call from AMS/UDP receiver threads.
* ``"async"``  — handler is a coroutine; scheduled onto the NiceGUI
                 asyncio loop via ``loop.call_soon_threadsafe``. Use for
                 UI updates from panels.

Errors in any one handler are caught and logged; they do not propagate
to other handlers or to the publisher.

Lifecycle: construct the bus eagerly (it's safe to subscribe before
``start()``); call ``start(loop)`` from the NiceGUI ``@app.on_startup``
hook so it captures the running event loop and spins up the worker pool;
call ``stop()`` from ``@app.on_shutdown``.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Literal

from blinker import Signal

log = logging.getLogger(__name__)

Mode = Literal["sync", "thread", "async"]


class EventBus:
    def __init__(self) -> None:
        self._loop:     asyncio.AbstractEventLoop | None = None
        self._executor: ThreadPoolExecutor | None       = None
        # Strong refs to wrapper closures; blinker would otherwise GC them
        # because we connect with weak=False but still — keeping our own
        # list also lets ``stop()`` disconnect everything cleanly.
        self._wrappers: list[tuple[Signal, Callable]]    = []
        self._lock                                       = threading.Lock()
        self._started                                    = False

    # ── lifecycle ─────────────────────────────────────────────────────

    def start(self, loop: asyncio.AbstractEventLoop, *, workers: int = 2) -> None:
        with self._lock:
            if self._started:
                return
            self._loop     = loop
            self._executor = ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="bus-worker",
            )
            self._started  = True
        log.info("EventBus started (workers=%d)", workers)

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._started = False
            executor      = self._executor
            self._executor = None
            self._loop     = None
            wrappers       = list(self._wrappers)
            self._wrappers.clear()
        for sig, w in wrappers:
            try:
                sig.disconnect(w)
            except Exception:
                pass
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)
        log.info("EventBus stopped")

    # ── publish ───────────────────────────────────────────────────────

    def publish(self, signal: Signal, payload: Any) -> None:
        """Fire `signal` with `payload`. Never raises; errors in
        synchronous subscribers are caught inside the wrapper."""
        signal.send(self, payload=payload)

    # ── subscribe ─────────────────────────────────────────────────────

    def subscribe(
        self,
        signal:  Signal,
        handler: Callable[[Any], Any],
        *,
        mode: Mode = "sync",
    ) -> Callable:
        """Connect `handler` to `signal`. Returns the wrapper that was
        actually connected to blinker — pass it to ``unsubscribe`` to
        remove this subscription."""
        if mode == "sync":
            wrapper = self._make_sync(handler)
        elif mode == "thread":
            wrapper = self._make_thread(handler)
        elif mode == "async":
            wrapper = self._make_async(handler)
        else:                       # pragma: no cover — typing covers this
            raise ValueError(f"unknown mode: {mode!r}")

        # weak=False so blinker doesn't drop the closure when this scope
        # ends; we also stash it so ``stop()`` can disconnect cleanly.
        signal.connect(wrapper, weak=False)
        with self._lock:
            self._wrappers.append((signal, wrapper))
        return wrapper

    def unsubscribe(self, signal: Signal, wrapper: Callable) -> None:
        try:
            signal.disconnect(wrapper)
        except Exception:
            pass
        with self._lock:
            self._wrappers = [(s, w) for s, w in self._wrappers if w is not wrapper]

    def subscription(
        self,
        signal:  Signal,
        handler: Callable[[Any], Any],
        *,
        mode: Mode = "sync",
    ) -> Callable[[], None]:
        """Subscribe and return a 0-arg unsubscribe callable.

        Convenience over `subscribe()` — publishers usually only want an
        undo handle, not the wrapper object. Idempotent: calling the
        returned function twice is a no-op.
        """
        wrapper = self.subscribe(signal, handler, mode=mode)
        called = [False]
        def _undo() -> None:
            if called[0]:
                return
            called[0] = True
            self.unsubscribe(signal, wrapper)
        return _undo

    # ── internals ─────────────────────────────────────────────────────

    @staticmethod
    def _make_sync(handler: Callable[[Any], Any]) -> Callable:
        def _on(_sender, *, payload):
            try:
                handler(payload)
            except Exception:
                log.exception("EventBus sync handler %r raised", handler)
        return _on

    def _make_thread(self, handler: Callable[[Any], Any]) -> Callable:
        def _on(_sender, *, payload):
            ex = self._executor
            if ex is None:
                # Bus not started (or already stopped). Drop silently —
                # no producer should be running in that window anyway.
                return
            def _run():
                try:
                    handler(payload)
                except Exception:
                    log.exception("EventBus thread handler %r raised", handler)
            try:
                ex.submit(_run)
            except RuntimeError:
                # Executor was shut down between the None check and submit.
                pass
        return _on

    def _make_async(self, handler: Callable[[Any], Any]) -> Callable:
        def _on(_sender, *, payload):
            loop = self._loop
            if loop is None:
                return
            def _schedule():
                try:
                    result = handler(payload)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(_guard(handler, result))
                except Exception:
                    log.exception("EventBus async handler %r raised", handler)
            try:
                loop.call_soon_threadsafe(_schedule)
            except RuntimeError:
                # Loop closed between the None check and the call.
                pass
        return _on


async def _guard(handler: Callable, coro) -> None:
    try:
        await coro
    except Exception:
        log.exception("EventBus async handler %r raised", handler)
