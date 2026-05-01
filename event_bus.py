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
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Literal

from blinker import Signal

log = logging.getLogger(__name__)

Mode = Literal["sync", "thread", "async"]

_MISSING = object()  # sentinel: getattr default that compares unequal to everything


class EventBus:
    def __init__(self, *, slow_handler_ms: float | None = 50.0) -> None:
        """Construct an EventBus.

        ``slow_handler_ms`` — log a WARNING when a handler dispatch
        takes at least this many milliseconds. Catches the "accidental
        blocking work in mode='sync' / mode='thread'" bug class.
        Set to ``None`` to disable timing entirely (skips the
        ``perf_counter`` call on the hot path).
        """
        self._loop:     asyncio.AbstractEventLoop | None = None
        self._executor: ThreadPoolExecutor | None       = None
        # Strong refs to wrapper closures; blinker would otherwise GC them
        # because we connect with weak=False but still — keeping our own
        # list also lets ``stop()`` disconnect everything cleanly.
        self._wrappers: list[tuple[Signal, Callable]]    = []
        self._lock                                       = threading.Lock()
        self._started                                    = False
        self._slow_ms:  float | None                     = slow_handler_ms

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

    def subscribe_filtered(
        self,
        signal:  Signal,
        handler: Callable[[Any], Any],
        *,
        mode: Mode = "sync",
        **filters: Any,
    ) -> Callable[[], None]:
        """Subscribe with payload-attribute equality filters.

        For every ``attr=value`` kwarg, the handler runs only when
        ``getattr(payload, attr) == value``. Returns a 0-arg
        unsubscribe handle, like :meth:`subscription`.

        Why: the dominant pattern across PLC subscribers is::

            def _on(payload):
                if payload.alias == MY_ALIAS:
                    ...
            bus.subscription(plc_signal_changed, _on, mode="thread")

        which puts the filter inside each handler — easy to forget,
        and the handler still gets dispatched (a worker-thread submit)
        for every unrelated alias. ``subscribe_filtered`` lifts the
        check to the wrapper so handlers receive only matching events.

        A missing attribute on the payload never matches; the handler
        is silently skipped rather than raising AttributeError.
        """
        if not filters:
            return self.subscription(signal, handler, mode=mode)

        items = tuple(filters.items())          # bind once

        def _filtered(payload):
            for attr, want in items:
                if getattr(payload, attr, _MISSING) != want:
                    return
            handler(payload)

        return self.subscription(signal, _filtered, mode=mode)

    # ── internals ─────────────────────────────────────────────────────

    def _record_elapsed(self, handler: Callable, mode: str, started: float) -> None:
        if self._slow_ms is None:
            return
        ms = (time.perf_counter() - started) * 1000.0
        if ms >= self._slow_ms:
            log.warning(
                "EventBus slow %s handler %r took %.1f ms (threshold=%.1f ms)",
                mode, handler, ms, self._slow_ms,
            )

    def _make_sync(self, handler: Callable[[Any], Any]) -> Callable:
        def _on(_sender, *, payload):
            t = time.perf_counter()
            try:
                handler(payload)
            except Exception:
                log.exception("EventBus sync handler %r raised", handler)
            finally:
                self._record_elapsed(handler, "sync", t)
        return _on

    def _make_thread(self, handler: Callable[[Any], Any]) -> Callable:
        def _on(_sender, *, payload):
            ex = self._executor
            if ex is None:
                # Bus not started (or already stopped). Drop silently —
                # no producer should be running in that window anyway.
                return
            def _run():
                t = time.perf_counter()
                try:
                    handler(payload)
                except Exception:
                    log.exception("EventBus thread handler %r raised", handler)
                finally:
                    self._record_elapsed(handler, "thread", t)
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
                t = time.perf_counter()
                try:
                    result = handler(payload)
                except Exception:
                    log.exception("EventBus async handler %r raised", handler)
                    self._record_elapsed(handler, "async", t)
                    return
                if asyncio.iscoroutine(result):
                    # Timing spans the awaited coroutine too; finalize in _guard.
                    asyncio.create_task(_guard(self, handler, result, t))
                else:
                    self._record_elapsed(handler, "async", t)
            try:
                loop.call_soon_threadsafe(_schedule)
            except RuntimeError:
                # Loop closed between the None check and the call.
                pass
        return _on


async def _guard(bus: "EventBus", handler: Callable, coro, started: float) -> None:
    try:
        await coro
    except Exception:
        log.exception("EventBus async handler %r raised", handler)
    finally:
        bus._record_elapsed(handler, "async", started)
