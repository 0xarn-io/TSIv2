"""robot_status_log.py — append-only SQLite log of ABB robot controller state.

Subscribes to `signals.robot_status_changed` for change-driven rows and
runs a periodic heartbeat thread (`tick_period_s`, default 30 s) that
snapshots `RobotMonitor.status()` for time-in-state aggregation. Each row
is tagged `source = 'change' | 'tick'`.

If `bypass_alias` is set, also subscribes to `signals.plc_signal_changed`
filtered by that alias and stamps the latest bypass value onto every
row. Bypass changes also enqueue a row of their own so transitions
land in the timeline.

Read API powers `robot_status_panel.py`:
  * recent()         — paginated raw history
  * time_in_state()  — SUM(seconds) per opmode over a window
  * transitions()    — chronological change rows
  * shift_summary()  — Ready / Enabled / Bypass / E-stop minutes per shift
  * daily_summary()  — per-day minutes table

Pattern mirrors errors_store.py: queue + writer thread, separate read
connection, retention prune in start(). The store is a pure subscriber —
it never publishes any signal, so no OWNERS edit in events.py is needed.

Usage in db_orchestrator.py:
    rsl = RobotStatusLog.from_config(cfg.robot_status_log,
                                     bus=bus, monitor=robot)
    rsl.start(); ...; rsl.stop()
"""
from __future__ import annotations

import logging
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

_QUEUE_MAX = 1000
# Two-phase schema: CREATE first (with the full column list for fresh
# DBs), then ALTER for legacy DBs that pre-date the bypass column, then
# indexes — the bypass index can only be built after the column exists.
_SCHEMA_CREATE = """
CREATE TABLE IF NOT EXISTS robot_status_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL DEFAULT (datetime('now')),
    opmode       TEXT    NOT NULL,
    ctrl_state   TEXT    NOT NULL,
    exec_state   TEXT    NOT NULL,
    speed_ratio  INTEGER NOT NULL,
    is_ready     INTEGER NOT NULL,
    bypass       INTEGER NOT NULL DEFAULT 0,
    source       TEXT    NOT NULL
);
"""
_SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_rsl_ts     ON robot_status_log(ts);
CREATE INDEX IF NOT EXISTS idx_rsl_opmode ON robot_status_log(opmode);
CREATE INDEX IF NOT EXISTS idx_rsl_source ON robot_status_log(source);
CREATE INDEX IF NOT EXISTS idx_rsl_bypass ON robot_status_log(bypass);
"""


@dataclass(frozen=True)
class RobotStatusLogConfig:
    db_path:       str
    keep_days:     int   = 90
    tick_period_s: float = 30.0
    bypass_alias:  str | None = None


class RobotStatusLog:
    """Append-only robot status log. Bus subscriber + heartbeat sampler."""

    def __init__(self, cfg: RobotStatusLogConfig, *, bus=None, monitor=None):
        self.cfg = cfg
        self._bus = bus
        self._monitor = monitor
        self._q: queue.Queue[dict | None] = queue.Queue(maxsize=_QUEUE_MAX)
        self._writer: threading.Thread | None = None
        self._tick:   threading.Thread | None = None
        self._stop_tick: threading.Event | None = None
        self._unsub_status = None
        self._unsub_bypass = None
        self._bypass: bool = False
        self._read_conn: sqlite3.Connection | None = None
        self._read_lock = threading.Lock()

    @classmethod
    def from_config(
        cls, cfg: RobotStatusLogConfig, *, bus=None, monitor=None,
    ) -> "RobotStatusLog":
        return cls(cfg, bus=bus, monitor=monitor)

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        Path(self.cfg.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.cfg.db_path) as init:
            init.executescript(_SCHEMA_CREATE)
            # Migrate older DBs that predate the bypass column. ALTER ... ADD
            # COLUMN with NOT NULL DEFAULT is fine on SQLite — existing rows
            # take the default. duplicate-column errors mean we're already
            # migrated; swallow them.
            try:
                init.execute(
                    "ALTER TABLE robot_status_log "
                    "ADD COLUMN bypass INTEGER NOT NULL DEFAULT 0"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            init.executescript(_SCHEMA_INDEXES)
            if self.cfg.keep_days > 0:
                init.execute(
                    "DELETE FROM robot_status_log WHERE ts < datetime('now', ?)",
                    (f"-{self.cfg.keep_days} days",),
                )
            init.commit()

        self._read_conn = sqlite3.connect(
            self.cfg.db_path, check_same_thread=False,
        )
        self._read_conn.row_factory = sqlite3.Row

        self._writer = threading.Thread(
            target=self._run_writer, daemon=True, name="robot-status-writer",
        )
        self._writer.start()

        if self._bus is not None:
            from events import signals
            self._unsub_status = self._bus.subscription(
                signals.robot_status_changed, self._on_change, mode="thread",
            )
            if self.cfg.bypass_alias:
                self._unsub_bypass = self._bus.subscribe_filtered(
                    signals.plc_signal_changed, self._on_bypass,
                    mode="thread", alias=self.cfg.bypass_alias,
                )

        if self._monitor is not None and self.cfg.tick_period_s > 0:
            self._stop_tick = threading.Event()
            self._tick = threading.Thread(
                target=self._run_tick, daemon=True, name="robot-status-tick",
            )
            self._tick.start()

    def stop(self) -> None:
        for unsub_attr in ("_unsub_status", "_unsub_bypass"):
            unsub = getattr(self, unsub_attr, None)
            if unsub is not None:
                try: unsub()
                except Exception: pass
                setattr(self, unsub_attr, None)
        if self._stop_tick is not None:
            self._stop_tick.set()
        if self._tick is not None:
            self._tick.join(timeout=2.0)
            self._tick = None
        self._stop_tick = None
        if self._writer is not None:
            self._q.put(None)
            self._writer.join(timeout=2.0)
            self._writer = None
        if self._read_conn is not None:
            try: self._read_conn.close()
            except Exception: pass
            self._read_conn = None

    # ---- read API -----------------------------------------------------------

    def recent(
        self,
        limit: int = 200,
        *,
        opmode: str | None = None,
        source: str | None = None,
    ) -> list[dict]:
        sql = "SELECT * FROM robot_status_log"
        clauses, params = [], []
        if opmode:
            clauses.append("opmode = ?"); params.append(opmode)
        if source:
            clauses.append("source = ?"); params.append(source)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        return self._read(sql, params)

    def time_in_state(self, since_iso: str) -> list[dict]:
        """SUM(seconds) per safety state over [since_iso, now].

        Categories are mutually exclusive and ranked by severity so each
        span lands in exactly one bucket:

          * `estop`    — ctrl_state contains 'emergencystop'
          * `bypass`   — bypass = 1 (and not estop)
          * `enabled`  — neither of the above

        Bypass / enabled is the operationally meaningful axis (it's what
        the safety circuit cares about); opmode breakdowns belong in the
        history view, not the headline summary.

        Each row's "duration" = next row's ts − this row's ts (LEAD over
        id). The final row is open-ended so we close it with `now`. Rows
        whose ts predates `since_iso` are clipped to start at `since_iso`."""
        sql = """
            WITH win AS (
                SELECT id, ts, ctrl_state, bypass FROM robot_status_log
                 WHERE ts >= ?
                 UNION ALL
                SELECT id, ?, ctrl_state, bypass FROM robot_status_log
                 WHERE id = (
                    SELECT MAX(id) FROM robot_status_log WHERE ts < ?
                 )
            ),
            spans AS (
                SELECT
                    CASE
                        WHEN LOWER(ctrl_state) LIKE '%emergencystop%' THEN 'estop'
                        WHEN bypass = 1                                THEN 'bypass'
                        ELSE                                                'enabled'
                    END AS state,
                    (julianday(COALESCE(LEAD(ts) OVER (ORDER BY ts, id),
                                        datetime('now')))
                     - julianday(ts)) * 86400.0 AS dur_s
                FROM win
            )
            SELECT state, ROUND(SUM(dur_s), 1) AS seconds
              FROM spans
             GROUP BY state
             ORDER BY
                 CASE state
                     WHEN 'estop'   THEN 0
                     WHEN 'bypass'  THEN 1
                     WHEN 'enabled' THEN 2
                     ELSE                3
                 END
        """
        return self._read(sql, [since_iso, since_iso, since_iso])

    def transitions(self, since_iso: str, limit: int = 500) -> list[dict]:
        sql = """
            SELECT id, ts, opmode, ctrl_state, exec_state, speed_ratio, is_ready
              FROM robot_status_log
             WHERE source = 'change' AND ts >= ?
             ORDER BY id DESC
             LIMIT ?
        """
        return self._read(sql, [since_iso, int(limit)])

    def daily_summary(self, days: int = 14) -> list[dict]:
        """Per-day minute totals, computed via LEAD-based span widths.

        `num_stops` counts change-rows whose exec_state moved from
        'running' → not-running."""
        sql = """
            WITH bounded AS (
                SELECT id, ts, opmode, ctrl_state, exec_state, speed_ratio,
                       is_ready, bypass
                  FROM robot_status_log
                 WHERE ts >= datetime('now', ?)
            ),
            spans AS (
                SELECT
                    DATE(ts) AS day,
                    opmode, ctrl_state, exec_state, speed_ratio,
                    is_ready, bypass,
                    (julianday(COALESCE(LEAD(ts) OVER (ORDER BY id),
                                        datetime('now')))
                     - julianday(ts)) * 1440.0 AS dur_m
                FROM bounded
            )
            SELECT
                day,
                ROUND(SUM(CASE WHEN opmode='AUTO' THEN dur_m ELSE 0 END), 1)
                    AS auto_minutes,
                ROUND(SUM(CASE WHEN opmode='MANR' THEN dur_m ELSE 0 END), 1)
                    AS manr_minutes,
                ROUND(SUM(CASE WHEN opmode='MANF' THEN dur_m ELSE 0 END), 1)
                    AS manf_minutes,
                ROUND(SUM(CASE WHEN exec_state='running' THEN dur_m ELSE 0 END), 1)
                    AS running_minutes,
                ROUND(SUM(CASE WHEN ctrl_state='motoron' THEN dur_m ELSE 0 END), 1)
                    AS motors_on_minutes,
                ROUND(SUM(CASE WHEN LOWER(ctrl_state) LIKE '%emergencystop%'
                               THEN dur_m ELSE 0 END), 1)
                    AS estop_minutes,
                ROUND(SUM(CASE WHEN bypass = 1 THEN dur_m ELSE 0 END), 1)
                    AS bypass_minutes,
                ROUND(AVG(speed_ratio), 1) AS avg_speed_ratio
              FROM spans
             GROUP BY day
             ORDER BY day DESC
        """
        rows = self._read(sql, [f"-{int(days)} days"])

        stops_sql = """
            WITH ch AS (
                SELECT DATE(ts) AS day, exec_state,
                       LAG(exec_state) OVER (ORDER BY id) AS prev_exec
                  FROM robot_status_log
                 WHERE source = 'change' AND ts >= datetime('now', ?)
            )
            SELECT day, COUNT(*) AS num_stops
              FROM ch
             WHERE prev_exec = 'running' AND exec_state <> 'running'
             GROUP BY day
        """
        stops = {r["day"]: r["num_stops"]
                 for r in self._read(stops_sql, [f"-{int(days)} days"])}
        for r in rows:
            r["num_stops"] = stops.get(r["day"], 0)
        return rows

    # Shifts within a calendar day. S3 22:00 → 06:00 of the next day.
    # (label, start_hour, end_hour) — hour offsets from midnight of the
    # selected date; values >24 mean "next day at h-24".
    SHIFTS = (
        ("S1 06–14",  6, 14),
        ("S2 14–22", 14, 22),
        ("S3 22–06", 22, 30),
    )

    def shift_summary(self, date_iso: str) -> list[dict]:
        """One row per shift for the given calendar date (YYYY-MM-DD).

        Same minute totals as `daily_summary`, bounded to a fixed 8-hour
        shift window. The night shift (S3) crosses midnight: 22:00 of
        `date_iso` → 06:00 of the next day. Spans crossing the window
        edges are clipped, and a shift that hasn't ended is clipped at
        `now` so partial totals still make sense."""
        from datetime import datetime, timedelta

        try:
            day0 = datetime.strptime(date_iso, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(
                f"date_iso must be YYYY-MM-DD, got {date_iso!r}"
            ) from e

        rows: list[dict] = []
        for label, h_start, h_end in self.SHIFTS:
            start_ts = (day0 + timedelta(hours=h_start)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            end_ts = (day0 + timedelta(hours=h_end)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            # The last row whose ts <= start_ts establishes "what state
            # the robot was in when the shift began" — its tail counts
            # toward the shift. Spans are clipped to
            # [start_ts, MIN(LEAD(ts), end_ts, now())].
            sql = """
                WITH bounded AS (
                    SELECT id, ts, ctrl_state, is_ready, bypass
                      FROM robot_status_log
                     WHERE ts < ?
                       AND id >= COALESCE(
                           (SELECT MAX(id) FROM robot_status_log WHERE ts <= ?),
                           0
                       )
                ),
                spans AS (
                    SELECT
                        ctrl_state, is_ready, bypass,
                        MAX(?, ts) AS span_start,
                        MIN(
                            COALESCE(LEAD(ts) OVER (ORDER BY id),
                                     datetime('now')),
                            ?,
                            datetime('now')
                        ) AS span_end
                    FROM bounded
                ),
                clipped AS (
                    SELECT ctrl_state, is_ready, bypass,
                           (julianday(span_end) - julianday(span_start))
                           * 1440.0 AS dur_m
                      FROM spans
                     WHERE span_end > span_start
                )
                SELECT
                    ROUND(SUM(CASE WHEN is_ready = 1 THEN dur_m ELSE 0 END), 1)
                        AS ready_minutes,
                    ROUND(SUM(CASE WHEN bypass = 1 THEN dur_m ELSE 0 END), 1)
                        AS bypass_minutes,
                    ROUND(SUM(CASE WHEN bypass = 0 THEN dur_m ELSE 0 END), 1)
                        AS enabled_minutes,
                    ROUND(SUM(CASE WHEN LOWER(ctrl_state) LIKE '%emergencystop%'
                                   THEN dur_m ELSE 0 END), 1)
                        AS estop_minutes,
                    ROUND(SUM(dur_m), 1) AS total_minutes
                  FROM clipped
            """
            agg = self._read(sql, [end_ts, start_ts, start_ts, end_ts])
            row = dict(agg[0]) if agg else {}
            for k in ("ready_minutes", "bypass_minutes", "enabled_minutes",
                      "estop_minutes", "total_minutes"):
                row[k] = float(row.get(k) or 0.0)

            row["shift"]    = label
            row["start_ts"] = start_ts
            row["end_ts"]   = end_ts
            rows.append(row)
        return rows

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        return self._read(sql, list(params))

    # ---- internals ----------------------------------------------------------

    def _on_change(self, payload) -> None:
        # payload is events.RobotStatusChanged; .status is RobotStatus
        try:
            self._enqueue(payload.status, source="change")
        except Exception as e:
            log.warning("robot_status_log: change handler failed: %s", e)

    def _on_bypass(self, payload) -> None:
        """plc_signal_changed handler — only invoked when alias matches.

        Cache the new bypass value and enqueue a row immediately so the
        timeline records the transition. Pulls a current RobotStatus
        snapshot from the monitor if available; otherwise the row will
        carry the cached / default fields."""
        try:
            self._bypass = bool(payload.value)
            status = self._monitor.status() if self._monitor is not None else None
            self._enqueue(status, source="change")
        except Exception as e:
            log.warning("robot_status_log: bypass handler failed: %s", e)

    def _run_tick(self) -> None:
        period_s = float(self.cfg.tick_period_s)
        # First tick after `period_s`, not immediately — gives RobotMonitor
        # a chance to do its first poll.
        while not self._stop_tick.wait(period_s):
            try:
                self._enqueue(self._monitor.status(), source="tick")
            except Exception as e:
                log.warning("robot_status_log: tick failed: %s", e)

    def _enqueue(self, status, source: str) -> None:
        row = {
            "opmode":      str(getattr(status, "opmode",     "unknown")),
            "ctrl_state":  str(getattr(status, "ctrl_state", "unknown")),
            "exec_state":  str(getattr(status, "exec_state", "unknown")),
            "speed_ratio": int(getattr(status, "speed_ratio", 0) or 0),
            "is_ready":    int(bool(getattr(status, "is_ready", False))),
            "bypass":      int(bool(self._bypass)),
            "source":      source,
        }
        try:
            self._q.put_nowait(row)
        except queue.Full:
            log.error("robot_status_log queue full — dropping (source=%s)", source)

    def _read(self, sql: str, params: list[Any]) -> list[dict]:
        if self._read_conn is None:
            return []
        with self._read_lock:
            cur = self._read_conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def _run_writer(self) -> None:
        conn = sqlite3.connect(self.cfg.db_path)
        try:
            while True:
                row = self._q.get()
                if row is None:
                    break
                try:
                    conn.execute(
                        """INSERT INTO robot_status_log
                           (opmode, ctrl_state, exec_state, speed_ratio,
                            is_ready, bypass, source)
                           VALUES (:opmode, :ctrl_state, :exec_state,
                                   :speed_ratio, :is_ready, :bypass,
                                   :source)""",
                        row,
                    )
                    conn.commit()
                except Exception as e:
                    log.warning("robot_status_log writer insert failed: %s", e)
        finally:
            try: conn.close()
            except Exception: pass
