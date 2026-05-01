"""errors_store.py — append-only SQLite log of errors raised by any device.

Each device (PLC, robot, Python, scanners, cameras…) owns its own catalog
of error codes; this DB only stores the *occurrences*. Callers supply the
title/description text at log time.

Usage:
    errors = ErrorsStore.from_config(cfg.errors_log)
    errors.start()
    ...
    errors.log("python", code=42, title="camera offline",
               severity="warning", message=str(exc))
    ...
    errors.stop()

`log()` only enqueues — a dedicated writer thread drains the queue. It
never raises (a failure here must not crash the caller).
"""
from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

_VALID_SEVERITIES = ("info", "warning", "error", "critical")
_QUEUE_MAX = 1000
_SCHEMA = """
CREATE TABLE IF NOT EXISTS error_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               TEXT    NOT NULL DEFAULT (datetime('now')),
    device           TEXT    NOT NULL,
    subsystem        TEXT,
    code             INTEGER NOT NULL,
    title            TEXT    NOT NULL,
    description      TEXT,
    severity         TEXT    NOT NULL DEFAULT 'error'
                     CHECK (severity IN ('info','warning','error','critical')),
    message          TEXT,
    raw_context_json TEXT,
    recipe_code      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_err_ts       ON error_log(ts);
CREATE INDEX IF NOT EXISTS idx_err_device   ON error_log(device, subsystem);
CREATE INDEX IF NOT EXISTS idx_err_severity ON error_log(severity);
"""


@dataclass(frozen=True)
class ErrorsConfig:
    db_path:   str
    keep_days: int = 0          # 0 = keep forever


class ErrorsStore:
    """Append-only error log. Single .log() entry point; writer thread drains."""

    def __init__(self, cfg: ErrorsConfig):
        self.cfg = cfg
        self._q: queue.Queue[dict | None] = queue.Queue(maxsize=_QUEUE_MAX)
        self._writer: threading.Thread | None = None
        self._read_conn: sqlite3.Connection | None = None
        self._read_lock = threading.Lock()

    @classmethod
    def from_config(cls, cfg: ErrorsConfig) -> "ErrorsStore":
        return cls(cfg)

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        Path(self.cfg.db_path).parent.mkdir(parents=True, exist_ok=True)
        # One-shot init on the calling thread: schema + retention prune.
        with sqlite3.connect(self.cfg.db_path) as init:
            init.executescript(_SCHEMA)
            if self.cfg.keep_days > 0:
                init.execute(
                    "DELETE FROM error_log WHERE ts < datetime('now', ?)",
                    (f"-{self.cfg.keep_days} days",),
                )
            init.commit()

        # Read-only connection for queries — writer thread keeps its own conn.
        self._read_conn = sqlite3.connect(
            self.cfg.db_path, check_same_thread=False
        )
        self._read_conn.row_factory = sqlite3.Row

        self._writer = threading.Thread(
            target=self._run, daemon=True, name="errors-writer",
        )
        self._writer.start()

    def stop(self) -> None:
        if self._writer is not None:
            self._q.put(None)               # sentinel
            self._writer.join(timeout=2.0)
            self._writer = None
        if self._read_conn is not None:
            try: self._read_conn.close()
            except Exception: pass
            self._read_conn = None

    # ---- public API ---------------------------------------------------------

    def log(
        self,
        device: str,
        code: int,
        title: str,
        *,
        subsystem: str | None = None,
        description: str | None = None,
        severity: str = "error",
        message: str | None = None,
        context: dict | None = None,
        recipe_code: int | None = None,
    ) -> None:
        """Enqueue one error. Never raises."""
        try:
            if severity not in _VALID_SEVERITIES:
                # Keep the row; just normalise severity to 'error' so the
                # CHECK constraint can't reject it.
                log.warning("errors.log: invalid severity %r — coerced to 'error'", severity)
                severity = "error"
            row = {
                "device":           device,
                "subsystem":        subsystem,
                "code":             int(code),
                "title":            title,
                "description":      description,
                "severity":         severity,
                "message":          message,
                "raw_context_json": json.dumps(context, default=str) if context else None,
                "recipe_code":      recipe_code,
            }
            self._q.put_nowait(row)
        except queue.Full:
            log.error("errors queue full — dropping: device=%s code=%s title=%s",
                      device, code, title)
        except Exception as e:
            log.error("errors.log failed: %s", e)

    def recent(
        self,
        limit: int = 100,
        *,
        severity: str | None = None,
        device: str | None = None,
    ) -> list[dict]:
        """Read most recent rows. Filters are optional."""
        sql = "SELECT * FROM error_log"
        clauses, params = [], []
        if severity:
            clauses.append("severity = ?"); params.append(severity)
        if device:
            clauses.append("device = ?");   params.append(device)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        return self._read(sql, params)

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        """Escape hatch for ad-hoc SELECTs (read-only)."""
        return self._read(sql, list(params))

    # ---- internals ----------------------------------------------------------

    def _read(self, sql: str, params: list[Any]) -> list[dict]:
        if self._read_conn is None:
            return []
        with self._read_lock:
            cur = self._read_conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def _run(self) -> None:
        conn = sqlite3.connect(self.cfg.db_path)
        try:
            while True:
                row = self._q.get()
                if row is None:
                    break
                try:
                    conn.execute(
                        """INSERT INTO error_log
                           (device, subsystem, code, title, description,
                            severity, message, raw_context_json, recipe_code)
                           VALUES (:device, :subsystem, :code, :title, :description,
                                   :severity, :message, :raw_context_json, :recipe_code)""",
                        row,
                    )
                    conn.commit()
                except Exception as e:
                    log.warning("errors writer insert failed: %s", e)
        finally:
            try: conn.close()
            except Exception: pass
