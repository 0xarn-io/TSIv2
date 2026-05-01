"""robot_errors.py — mirror the ABB controller's RWS event log into ErrorsStore.

Periodically pulls /rw/elog/{domain} via RobotMonitor.fetch_errors() and
forwards every entry that isn't already in the local error_log table.
Dedup is keyed on the elog `seq` field, persisted into
errors_store.error_log.raw_context_json so a process restart doesn't
re-import history.

Usage in Main.py:
    elog = RobotElogPoller(robot, db.errors,
                           poll_ms=cfg.robot.elog_poll_ms,
                           include_info=cfg.robot.elog_include_info)
    elog.start()
    ...
    elog.stop()

No nicegui import — strictly data layer.
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any

log = logging.getLogger(__name__)


# ABB elog 'type' field → ErrorsStore severity name.
_ELOG_TYPE_TO_SEVERITY = {
    "INFO":  "info",
    "WARN":  "warning",
    "ERROR": "error",
}


class RobotElogPoller:
    """Periodic RWS event-log → ErrorsStore mirror with seq-based dedup."""

    def __init__(
        self,
        monitor,                  # robot_status.RobotMonitor (duck-typed)
        errors_store,             # errors_store.ErrorsStore   (duck-typed)
        *,
        poll_ms:      int  = 5000,
        domain:       int  = 0,
        limit:        int  = 50,
        include_info: bool = False,
        bus=None,
    ):
        self.monitor       = monitor
        self.errors_store  = errors_store
        self.poll_ms       = int(poll_ms)
        self.domain        = int(domain)
        self.limit         = int(limit)
        self.include_info  = bool(include_info)
        self._bus          = bus

        self._seen_seqs: set[str] = set()
        self._stop:    threading.Event   | None = None
        self._thread:  threading.Thread  | None = None

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        self._seen_seqs = self._load_seen()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="robot-elog-poller",
        )
        self._thread.start()

    def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ---- internals ----------------------------------------------------------

    def _load_seen(self) -> set[str]:
        """Pre-populate the seen-seq set from rows already in error_log."""
        try:
            rows = self.errors_store.query(
                "SELECT raw_context_json FROM error_log "
                "WHERE device = ? AND subsystem = ?",
                ("robot", "elog"),
            )
        except Exception as e:
            log.warning("elog: failed to load seen seqs: %s", e)
            return set()

        seen: set[str] = set()
        for r in rows:
            raw = r.get("raw_context_json")
            if not raw:
                continue
            try:
                ctx = json.loads(raw)
            except Exception:
                continue
            seq = ctx.get("seq")
            if seq:
                seen.add(str(seq))
        return seen

    def _run(self) -> None:
        period_s = self.poll_ms / 1000.0
        # Tick once immediately, then on every period; stop if event fires.
        self._poll_once()
        while not self._stop.wait(period_s):
            self._poll_once()

    def _poll_once(self) -> None:
        try:
            entries = self.monitor.fetch_errors(
                domain=self.domain,
                limit=self.limit,
                include_info=self.include_info,
            )
        except Exception as e:
            log.warning("elog fetch failed: %s", e)
            return

        for entry in _sorted_oldest_first(entries):
            seq = str(entry.get("seq") or "")
            if not seq:
                # Without a seq we can't dedupe — drop rather than spam logs.
                continue
            if seq in self._seen_seqs:
                continue

            severity = _ELOG_TYPE_TO_SEVERITY.get(
                str(entry.get("type") or "").upper(), "error",
            )
            code = _safe_int(entry.get("code"))
            try:
                self.errors_store.log(
                    device="robot",
                    subsystem="elog",
                    code=code,
                    title=str(entry.get("title") or "(no title)"),
                    severity=severity,
                    message=str(entry.get("desc") or "") or None,
                    context={
                        "seq":    seq,
                        "ts":     entry.get("ts"),
                        "action": entry.get("action"),
                        "type":   entry.get("type"),
                    },
                )
                self._seen_seqs.add(seq)
                if self._bus is not None:
                    from events import RobotErrorLogged, signals
                    self._bus.publish(signals.robot_error_logged,
                                      RobotErrorLogged(entry=dict(entry)))
            except Exception as ex:
                log.warning("elog log failed for seq=%s: %s", seq, ex)


# ---- helpers ----------------------------------------------------------------

def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _sorted_oldest_first(entries: list[dict]) -> list[dict]:
    """Sort an elog batch oldest-first so logs land in chronological order.

    Elog seq values look like '0/12345' (numeric, monotonic). Fall back to
    raw string compare if anything unexpected shows up.
    """
    def sort_key(e: dict):
        s = str(e.get("seq") or "")
        # Most RWS firmwares: seq is a plain integer or 'domain/n' fragment.
        # Take the trailing integer portion if present.
        tail = s.rsplit("/", 1)[-1]
        try:
            return (0, int(tail))
        except ValueError:
            return (1, s)
    return sorted(entries, key=sort_key)
