"""unit_logger.py — append-only SQLite log of every unit (box) measured.

When an EventBus is supplied, subscribes to `signals.sick_unit_event`
(every UnitEvent → one row) and `signals.plc_signal_changed` for the
recipe-code alias (kept cached, never read synchronously from the
receiver thread). When no bus is supplied, falls back to the legacy
`bridge.on_event` + `plc.read()` path so existing tests and headless
configurations keep working.

Each row is enriched with:
  - the cached active recipe code (if a recipe alias is configured), and
  - the latest snapshot paths from each camera (if a SnapshotArchive is
    provided).

A dedicated writer thread + Queue keeps DB I/O off the receiver thread.
The logger never imports RecipesStore — recipe context is just an integer
code; tolerance evaluation is downstream.
"""
from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

M_TO_MM = 1000
_QUEUE_MAX = 1000
_SCHEMA = """
CREATE TABLE IF NOT EXISTS unit (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL DEFAULT (datetime('now')),
    recipe_code     INTEGER,
    width_mm        INTEGER, height_mm    INTEGER, offset_mm     INTEGER,
    length_mm       INTEGER, samples      INTEGER, duration_s    REAL,
    width_min_mm    INTEGER, width_max_mm INTEGER,
    height_min_mm   INTEGER, height_max_mm INTEGER,
    offset_min_mm   INTEGER, offset_max_mm INTEGER,
    in_tolerance    INTEGER,
    snap_entry_path TEXT,
    snap_exit_path  TEXT,
    raw_event_json  TEXT
);
CREATE INDEX IF NOT EXISTS idx_unit_ts     ON unit(ts);
CREATE INDEX IF NOT EXISTS idx_unit_recipe ON unit(recipe_code);
"""

_INSERT_SQL = """
INSERT INTO unit (
    recipe_code,
    width_mm, height_mm, offset_mm,
    length_mm, samples, duration_s,
    width_min_mm,  width_max_mm,
    height_min_mm, height_max_mm,
    offset_min_mm, offset_max_mm,
    in_tolerance,
    snap_entry_path, snap_exit_path,
    raw_event_json
) VALUES (
    :recipe_code,
    :width_mm, :height_mm, :offset_mm,
    :length_mm, :samples, :duration_s,
    :width_min_mm,  :width_max_mm,
    :height_min_mm, :height_max_mm,
    :offset_min_mm, :offset_max_mm,
    :in_tolerance,
    :snap_entry_path, :snap_exit_path,
    :raw_event_json
)
"""


@dataclass(frozen=True)
class UnitLoggerConfig:
    db_path:   str
    keep_days: int = 0       # 0 = keep forever


def _mm(m: float | None) -> int | None:
    return None if m is None else int(round(m * M_TO_MM))


def _event_to_dict(ev: Any) -> dict:
    """UnitEvent (or any dataclass-ish) → JSON-friendly dict."""
    if is_dataclass(ev):
        return asdict(ev)
    return {k: getattr(ev, k) for k in (
        "entered_at", "exited_at", "duration_s", "n_samples", "length_m",
        "width_mean_m",  "width_min_m",  "width_max_m",
        "height_mean_m", "height_min_m", "height_max_m",
        "offset_mean_m", "offset_min_m", "offset_max_m",
    ) if hasattr(ev, k)}


class UnitLogger:
    """Append-only unit log. Subscribes to bridge.on_event."""

    def __init__(
        self,
        cfg: UnitLoggerConfig,
        bridge,
        plc,
        *,
        recipe_alias: str | None = None,
        archive=None,
        bus=None,
    ):
        self.cfg = cfg
        self.bridge = bridge
        self.plc = plc
        self.recipe_alias = recipe_alias
        self.archive = archive
        self._bus = bus
        self._q: queue.Queue[dict | None] = queue.Queue(maxsize=_QUEUE_MAX)
        self._writer: threading.Thread | None = None
        self._unsub = None
        # Bus-mode subscriptions; tracked so stop() can disconnect cleanly.
        self._bus_subs: list = []
        # Cached recipe code from PlcSignalChanged subscription. Read by
        # _build_row on the writer thread, written by the bus dispatch.
        self._recipe_code: int | None = None
        self._recipe_lock = threading.Lock()
        self._read_conn: sqlite3.Connection | None = None
        self._read_lock = threading.Lock()

    @classmethod
    def from_config(
        cls, cfg: UnitLoggerConfig, *, bridge, plc,
        recipe_alias: str | None = None, archive=None, bus=None,
    ) -> "UnitLogger":
        return cls(cfg, bridge, plc,
                   recipe_alias=recipe_alias, archive=archive, bus=bus)

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        Path(self.cfg.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.cfg.db_path) as init:
            init.executescript(_SCHEMA)
            if self.cfg.keep_days > 0:
                init.execute(
                    "DELETE FROM unit WHERE ts < datetime('now', ?)",
                    (f"-{self.cfg.keep_days} days",),
                )
            init.commit()

        self._read_conn = sqlite3.connect(
            self.cfg.db_path, check_same_thread=False
        )
        self._read_conn.row_factory = sqlite3.Row

        self._writer = threading.Thread(
            target=self._run, daemon=True, name="unit-writer",
        )
        self._writer.start()

        if self._bus is not None:
            from events import signals
            # Unit events: thread mode so DB queueing happens off the
            # receiver thread (the bus already executor-hops; this is just
            # the receiver-thread → executor handoff).
            self._bus_subs.append((
                signals.sick_unit_event,
                self._bus.subscribe(signals.sick_unit_event,
                                    lambda p: self._on_event(p.event),
                                    mode="thread"),
            ))
            # Recipe code cache: every PlcSignalChanged for our alias
            # updates the local cache. No more synchronous plc.read() in
            # the unit-event path.
            if self.recipe_alias:
                self._bus_subs.append((
                    signals.plc_signal_changed,
                    self._bus.subscribe(signals.plc_signal_changed,
                                        self._on_plc_signal,
                                        mode="thread"),
                ))
                # Without an ADS notification the bus never fires for
                # this alias. Idempotent per alias — recipe_publisher
                # may also call this and only one notification is
                # created. Cleanup is handled by plc.close().
                try:
                    self.plc.ensure_published(self.recipe_alias)
                except Exception as e:
                    log.debug("unit_logger: ensure_published(%s) failed: %s",
                              self.recipe_alias, e)
                # Bootstrap the cache so the first unit event after start
                # already has a recipe code (if the PLC is reachable).
                try:
                    with self._recipe_lock:
                        self._recipe_code = int(self.plc.read(self.recipe_alias))
                except Exception as e:
                    log.debug("unit_logger: bootstrap recipe read failed: %s", e)
        else:
            # Legacy path — keep working for headless tests / configs that
            # don't construct an EventBus.
            self._unsub = self.bridge.on_event(self._on_event)

    def stop(self) -> None:
        for sig, w in self._bus_subs:
            try: self._bus.unsubscribe(sig, w)
            except Exception: pass
        self._bus_subs.clear()
        if self._unsub is not None:
            try: self._unsub()
            except Exception: pass
            self._unsub = None
        if self._writer is not None:
            self._q.put(None)
            self._writer.join(timeout=2.0)
            self._writer = None
        if self._read_conn is not None:
            try: self._read_conn.close()
            except Exception: pass
            self._read_conn = None

    # ---- bus handlers -------------------------------------------------------

    def _on_plc_signal(self, payload) -> None:
        if payload.alias != self.recipe_alias:
            return
        try:
            code = int(payload.value)
        except (TypeError, ValueError):
            return
        with self._recipe_lock:
            self._recipe_code = code

    # ---- public API ---------------------------------------------------------

    def recent(self, limit: int = 100) -> list[dict]:
        with self._read_lock:
            if self._read_conn is None:
                return []
            cur = self._read_conn.execute(
                "SELECT * FROM unit ORDER BY id DESC LIMIT ?", (int(limit),)
            )
            return [dict(r) for r in cur.fetchall()]

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        with self._read_lock:
            if self._read_conn is None:
                return []
            cur = self._read_conn.execute(sql, list(params))
            return [dict(r) for r in cur.fetchall()]

    # ---- internals ----------------------------------------------------------

    def _on_event(self, ev) -> None:
        try:
            self._q.put_nowait(self._build_row(ev))
        except queue.Full:
            log.error("unit queue full — dropping event")
        except Exception as e:
            log.warning("unit_logger build_row failed: %s", e)

    def _build_row(self, ev) -> dict:
        return {
            "recipe_code":     self._read_recipe_code(),
            "width_mm":        _mm(getattr(ev, "width_mean_m",  None)),
            "height_mm":       _mm(getattr(ev, "height_mean_m", None)),
            "offset_mm":       _mm(getattr(ev, "offset_mean_m", None)),
            "length_mm":       _mm(getattr(ev, "length_m",      None)),
            "samples":         int(getattr(ev, "n_samples", 0) or 0),
            "duration_s":      float(getattr(ev, "duration_s", 0.0) or 0.0),
            "width_min_mm":    _mm(getattr(ev, "width_min_m",  None)),
            "width_max_mm":    _mm(getattr(ev, "width_max_m",  None)),
            "height_min_mm":   _mm(getattr(ev, "height_min_m", None)),
            "height_max_mm":   _mm(getattr(ev, "height_max_m", None)),
            "offset_min_mm":   _mm(getattr(ev, "offset_min_m", None)),
            "offset_max_mm":   _mm(getattr(ev, "offset_max_m", None)),
            "in_tolerance":    None,                # populated downstream
            "snap_entry_path": self._latest_path("entry"),
            "snap_exit_path":  self._latest_path("exit"),
            "raw_event_json":  json.dumps(_event_to_dict(ev), default=str),
        }

    def _read_recipe_code(self) -> int | None:
        if not self.recipe_alias:
            return None
        if self._bus is not None:
            with self._recipe_lock:
                return self._recipe_code
        try:
            return int(self.plc.read(self.recipe_alias))
        except Exception as e:
            log.debug("unit_logger: recipe_code read failed: %s", e)
            return None

    def _latest_path(self, name: str) -> str | None:
        if self.archive is None:
            return None
        try:
            return self.archive.latest_path(name)
        except Exception:
            return None

    def _run(self) -> None:
        conn = sqlite3.connect(self.cfg.db_path)
        try:
            while True:
                row = self._q.get()
                if row is None:
                    break
                try:
                    conn.execute(_INSERT_SQL, row)
                    conn.commit()
                except Exception as e:
                    log.warning("unit writer insert failed: %s", e)
        finally:
            try: conn.close()
            except Exception: pass
