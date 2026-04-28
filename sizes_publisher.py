"""sizes_publisher.py — bridge active size ids from the PLC to setpoints.

For each configured (table, code_alias, setpoints_alias) mapping:
    PLC writes nId → Python looks up sizes.get(table, nId) → Python writes
    the matching ST_SizeSetpoints struct.

One instance handles N mappings (typically one per table — cardboard and
others). Mirrors RecipePublisher in spirit but is multi-table.

Threading: notifications run on pyads's AmsRouter thread; making sync ADS
writes from there deadlocks (response handler == caller). So the
notification callback only enqueues; a dedicated worker thread does the
actual plc.write off the AmsRouter.
"""
from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass

from sizes_store import Size, SizesStore
from twincat_comm import TwinCATComm

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SizeSetpointConfig:
    table:           str   # "cardboard" or "others"
    code_alias:      str   # PLC writes a DINT here
    setpoints_alias: str   # Python writes ST_SizeSetpoints here
    cycle_ms:        int = 100


def _size_to_struct(s: Size) -> dict:
    """Size → ST_SizeSetpoints dict (4 × INT).

    Adding a setpoint: append a field here AND in [structs.ST_SizeSetpoints]
    in plc_signals.toml. No other code changes.
    """
    return {
        "nWidthMm":  int(s.width_mm),
        "nLengthMm": int(s.length_mm),
        "nWidthIn":  int(s.width_in),
        "nLengthIn": int(s.length_in),
    }


# Sentinel used as a poison pill to wake the worker on stop.
_STOP = object()


class SizesPublisher:
    """N PLC subscriptions → sizes.get(table, id) → struct write.

    Writes happen on a worker thread, never on pyads's notification thread.
    """

    def __init__(
        self,
        sizes: SizesStore,
        plc:   TwinCATComm,
        mappings: list[SizeSetpointConfig],
    ):
        self.sizes = sizes
        self.plc = plc
        self.mappings = list(mappings)
        self._handles: list[tuple[int, int]] = []
        self._queue: queue.Queue = queue.Queue()
        self._worker: threading.Thread | None = None
        self._stop_evt = threading.Event()

    def start(self) -> None:
        aliases: list[str] = []
        for m in self.mappings:
            aliases += [m.code_alias, m.setpoints_alias]
        self.plc.validate(aliases)

        # Worker first so initial enqueues have a consumer.
        self._stop_evt.clear()
        self._worker = threading.Thread(
            target=self._run, daemon=True, name="sizes-pub-writer",
        )
        self._worker.start()

        for m in self.mappings:
            try:
                current = self.plc.read(m.code_alias)
                self._queue.put((m, int(current)))
            except Exception as e:
                log.warning("initial %s size read failed: %s", m.table, e)

            try:
                handles = self.plc.subscribe(
                    m.code_alias,
                    lambda _alias, val, m=m: self._queue.put((m, int(val))),
                    cycle_time_ms=m.cycle_ms,
                    on_change=True,
                )
                self._handles.append(handles)
            except Exception as e:
                log.warning(
                    "%s size subscription failed (%s); disabled until "
                    "'%s' is available on the PLC",
                    m.table, e, m.code_alias,
                )

    def stop(self) -> None:
        # Unsubscribe first so no new items get enqueued.
        for h in self._handles:
            try: self.plc.unsubscribe(h)
            except Exception as e: log.warning("unsubscribe size failed: %s", e)
        self._handles.clear()

        # Wake the worker and join.
        self._stop_evt.set()
        self._queue.put(_STOP)
        if self._worker is not None:
            self._worker.join(timeout=2.0)
            self._worker = None

    # ---- worker -------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is _STOP:
                break
            m, sid = item
            try:
                self._apply(m, sid)
            except Exception:
                log.exception("sizes writer crashed on (%s, %s)",
                              getattr(m, "table", "?"), sid)

    def _apply(self, m: SizeSetpointConfig, sid: int) -> None:
        if sid <= 0:
            # 0 means "no selection" (autoincrement starts at 1). Don't warn.
            return
        size = self.sizes.get(m.table, sid)
        if size is None:
            log.warning(
                "%s size id %s not found in DB — no setpoints written",
                m.table, sid,
            )
            return
        try:
            self.plc.write(m.setpoints_alias, _size_to_struct(size))
            log.info("%s size %s pushed (%s)", m.table, sid, size.name)
        except Exception as e:
            log.warning("%s size setpoints write failed: %s", m.table, e)
