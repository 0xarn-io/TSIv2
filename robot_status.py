"""robot_status.py — ABB OmniCore RWS monitor.

Two ways to use it from Main.py:

    monitor = RobotMonitor.from_config(cfg.robot)
    monitor.start()                    # background poll loop
    monitor.stop()

    monitor.status()                   # current snapshot (thread-safe)
    monitor.fetch_errors(limit=20)     # one-shot event-log pull

Add [robot] to app_config.toml to enable; omit to skip the module
entirely.

HTTP / session lives in `rws_client.RWSClient`; this module owns the
domain-specific polling + state caching.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from robot_variables import RobotVariableConfig
from rws_client      import RWSClient

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RobotConfig:
    ip:                  str
    username:            str  = "Admin"
    password:            str  = "robotics"
    verify_ssl:          bool = False
    poll_ms:             int  = 2000
    timeout_s:           float = 2.0
    # Elog mirror — set elog_poll_ms = 0 to disable.
    elog_poll_ms:        int  = 5000
    elog_domain:         int  = 0
    elog_limit:          int  = 50
    elog_include_info:   bool = False
    # Master arrays mirror — set master_poll_ms = 0 to disable.
    master_poll_ms:      int  = 2000
    master_task:         str  = "T_ROB1"
    master_module:       str  = "Stations"
    master_symbol:       str  = "Master"
    master_dims_symbol:  str  = "Master_Dimmensions"
    vars:                tuple[RobotVariableConfig, ...] = field(default_factory=tuple)


@dataclass
class RobotStatus:
    ctrl_state:  str = "unknown"   # motoron / motoroff / guardstop / ...
    opmode:      str = "unknown"   # AUTO / MANR / MANF
    exec_state:  str = "unknown"   # running / stopped / ...
    speed_ratio: int = 0           # 0–100
    last_polled: float = 0.0       # time.monotonic() of last successful poll

    # ---- derived bool flags (the PLC-friendly view) ----

    @property
    def motors_on(self) -> bool:
        return self.ctrl_state == "motoron"

    @property
    def auto_mode(self) -> bool:
        return self.opmode == "AUTO"

    @property
    def running(self) -> bool:
        return self.exec_state == "running"

    @property
    def guard_stop(self) -> bool:
        return self.ctrl_state == "guardstop"

    @property
    def estop(self) -> bool:
        return "emergencystop" in self.ctrl_state.lower()

    @property
    def is_ready(self) -> bool:
        """All-good gate: motors on AND auto mode AND RAPID running."""
        return self.motors_on and self.auto_mode and self.running


class RobotMonitor:
    """Polls ABB RWS status in a background thread; publishes via callbacks."""

    def __init__(self, cfg: RobotConfig, *, client: RWSClient | None = None):
        self.cfg = cfg
        # Allow tests + Main.py to share a single RWSClient across modules.
        self.client = client or RWSClient(cfg)
        self._status = RobotStatus()
        self._lock = threading.Lock()
        self._stop: threading.Event | None = None
        self._thread: threading.Thread | None = None
        self._change_cbs: list[Callable[[RobotStatus], None]] = []

    @classmethod
    def from_config(cls, cfg: RobotConfig) -> "RobotMonitor":
        return cls(cfg)

    # ---- lifecycle ----

    def start(self) -> None:
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="robot-monitor",
        )
        self._thread.start()

    def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()

    # ---- public API ----

    def status(self) -> RobotStatus:
        """Return a copy of the current status (safe for callers to keep)."""
        with self._lock:
            return RobotStatus(**self._status.__dict__)

    def on_change(
        self, cb: Callable[[RobotStatus], None],
    ) -> Callable[[], None]:
        """Fires whenever any tracked field changes. Returns an unsubscribe fn."""
        self._change_cbs.append(cb)
        return lambda: self._change_cbs.remove(cb)

    def fetch_errors(
        self,
        domain: int = 0,
        limit: int = 50,
        include_info: bool = False,
    ) -> list[dict]:
        """One-shot event-log pull. Returns a list of dicts (warnings + errors).

        Some controllers occasionally emit a malformed JSON elog batch
        (an event message contains an unescaped quote/backslash). Halve
        the limit and retry rather than dropping the whole tick.
        """
        path = f"/rw/elog/{domain}"
        obj = self._get(path, params={"lang": "en", "lim": limit}, silent=True)
        if obj is None and limit > 4:
            # Retry with a smaller window — usually skips the bad event.
            smaller = max(4, limit // 4)
            obj = self._get(path, params={"lang": "en", "lim": smaller}, silent=True)
        if obj is None:
            log.warning("RWS GET %s failed (lim=%s): malformed response", path, limit)
            return []
        items = obj.get("_embedded", {}).get("resources", [])
        TYPES = {"1": "INFO", "2": "WARN", "3": "ERROR"}
        keep = {"1", "2", "3"} if include_info else {"2", "3"}
        return [
            {
                "seq":    it.get("_title", ""),
                "type":   TYPES.get(str(it.get("msgtype", "")), "?"),
                "code":   it.get("code", ""),
                "ts":     it.get("tstamp", ""),
                "title":  it.get("title", ""),
                "desc":   it.get("desc", ""),
                "action": it.get("actions", ""),
            }
            for it in items
            if str(it.get("msgtype")) in keep
        ]

    # ---- internals ----

    def _run(self) -> None:
        period_s = self.cfg.poll_ms / 1000.0
        while not self._stop.wait(period_s):
            self._poll()

    def _poll(self) -> None:
        prev = self.status()

        # Mirror the working RWS test script exactly: try multiple paths and
        # multiple field names, keep quiet about per-path 404s, only warn
        # when every variant fails.
        ctrl = self._get_first(
            "/rw/panel/ctrl-state",
            "/rw/panel/ctrlstate",
            "/rw/system/ctrl-state",
        )
        opm  = self._get("/rw/panel/opmode")
        exe  = self._get("/rw/rapid/execution")
        spd  = self._get("/rw/panel/speedratio")

        new = RobotStatus(
            ctrl_state  = _state(ctrl, "ctrlstate", "ctrl-state", "state"),
            opmode      = _state(opm,  "opmode"),
            exec_state  = _state(exe,  "ctrlexecstate"),
            speed_ratio = _state_int(spd, "speedratio"),
            last_polled = time.monotonic(),
        )
        with self._lock:
            self._status = new

        # Compare on the fields we care about (skip last_polled — always changes).
        if (new.ctrl_state, new.opmode, new.exec_state, new.speed_ratio) != (
            prev.ctrl_state, prev.opmode, prev.exec_state, prev.speed_ratio
        ):
            log.info(
                "robot: ready=%d (%s/%s/%s @ %d%%)",
                int(new.is_ready), new.ctrl_state, new.opmode, new.exec_state,
                new.speed_ratio,
            )
            for cb in list(self._change_cbs):
                try:
                    cb(new)
                except Exception as e:
                    log.warning("robot on_change cb failed: %s", e)

    # GET helpers — kept as instance methods (preserved patch targets in tests)
    # but delegate to the shared RWSClient. _get_first walks via self._get so
    # callers (and tests) that patch m._get still see all variants.
    def _get(self, path: str, params=None, silent: bool = False) -> Optional[dict]:
        return self.client.get(path, params=params, silent=silent)

    def _get_first(self, *paths: str, params=None) -> Optional[dict]:
        for p in paths:
            obj = self._get(p, params=params, silent=True)
            if obj is not None:
                return obj
        log.warning("RWS GET (all variants failed): %s", list(paths))
        return None


def _state(obj, *keys, default: str = "unknown") -> str:
    """Extract a string field from an RWS HAL+JSON 'state[0]' block."""
    if not obj:
        return default
    s = obj.get("state", [{}])
    s = s[0] if s else {}
    for k in keys:
        if k in s:
            return str(s[k])
    return default


def _state_int(obj, *keys, default: int = 0) -> int:
    val = _state(obj, *keys, default="")
    try:
        return int(val)
    except (TypeError, ValueError):
        return default
