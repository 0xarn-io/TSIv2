"""robot_variables.py — declarative RAPID symbol monitor + writer.

Configure a list of `[[robot.vars]]` in app_config.toml. This module
polls each entry on its own cadence, exposes the latest typed value
to subscribers, and (per-entry) routes changes to:

    "ui"   — fire on_change callbacks (the panel listens)
    "log"  — append a row to ErrorsStore as severity=info
    "plc"  — write the value to a TwinCAT alias (recipe-publisher style)

Read-only entries are polled but reject writes; RW entries can be
updated via `monitor.set(alias, value)`.

No NiceGUI dependency.
"""
from __future__ import annotations

import logging
import threading
import time
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable

from rws_client import RWSClient

log = logging.getLogger(__name__)


_VALID_TYPES   = ("num", "bool", "string")
_VALID_MODES   = ("r", "rw")
_VALID_TARGETS = ("ui", "log", "plc")


@dataclass(frozen=True)
class RobotVariableConfig:
    alias:     str                       # local handle ("speed_setpoint")
    task:      str                       # RAPID task ("T_ROB1")
    module:    str                       # RAPID module ("MainModule")
    symbol:    str                       # RAPID variable name
    type:      str  = "num"              # "num" | "bool" | "string"
    mode:      str  = "r"                # "r" | "rw"
    poll_ms:   int  = 2000
    targets:   tuple[str, ...] = ("ui",) # subset of _VALID_TARGETS
    plc_alias: str | None = None         # required when "plc" in targets

    def __post_init__(self) -> None:
        if self.type not in _VALID_TYPES:
            raise ValueError(
                f"robot var '{self.alias}': type {self.type!r} not in {_VALID_TYPES}"
            )
        if self.mode not in _VALID_MODES:
            raise ValueError(
                f"robot var '{self.alias}': mode {self.mode!r} not in {_VALID_MODES}"
            )
        unknown = set(self.targets) - set(_VALID_TARGETS)
        if unknown:
            raise ValueError(
                f"robot var '{self.alias}': unknown target(s) {sorted(unknown)}"
            )
        if "plc" in self.targets and not self.plc_alias:
            raise ValueError(
                f"robot var '{self.alias}': plc_alias required when 'plc' in targets"
            )


class RobotVariablesMonitor:
    """Polls + writes RAPID symbols. One thread, per-var poll cadence."""

    def __init__(
        self,
        client: RWSClient,
        vars: list[RobotVariableConfig],
        *,
        errors_store=None,
        plc=None,
        bus=None,
    ):
        self.client = client
        self.vars: dict[str, RobotVariableConfig] = {v.alias: v for v in vars}
        if len(self.vars) != len(vars):
            raise ValueError("robot vars: duplicate alias")
        self._errors = errors_store
        self._plc = plc
        self._bus = bus

        self._values:    dict[str, Any]   = {}        # last typed value
        self._last_poll: dict[str, float] = {}        # monotonic
        self._cbs: dict[str, list[Callable[[Any], None]]] = {
            v.alias: [] for v in vars
        }

        self._lock = threading.Lock()
        self._stop: threading.Event | None = None
        self._thread: threading.Thread | None = None

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        if not self.vars:
            return
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="robot-vars",
        )
        self._thread.start()

    def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ---- public API ---------------------------------------------------------

    def get(self, alias: str) -> Any | None:
        with self._lock:
            return self._values.get(alias)

    def set(self, alias: str, value: Any) -> None:
        cfg = self._require(alias)
        if cfg.mode != "rw":
            raise PermissionError(f"robot var '{alias}' is read-only")
        encoded = _encode(cfg, value)
        ok = self.client.write_rapid(cfg.task, cfg.module, cfg.symbol, encoded)
        if not ok:
            raise RuntimeError(f"RWS write failed for '{alias}'")
        # Update the local cache immediately; the next poll will confirm.
        self._observe(cfg, _coerce(cfg, encoded))

    def on_change(
        self, alias: str, cb: Callable[[Any], None],
    ) -> Callable[[], None]:
        self._require(alias)
        self._cbs[alias].append(cb)
        return lambda: self._cbs[alias].remove(cb)

    def aliases(self) -> list[str]:
        return list(self.vars.keys())

    def config(self, alias: str) -> RobotVariableConfig:
        return self._require(alias)

    # ---- internals ----------------------------------------------------------

    def _require(self, alias: str) -> RobotVariableConfig:
        cfg = self.vars.get(alias)
        if cfg is None:
            raise KeyError(f"unknown robot var '{alias}'")
        return cfg

    def _run(self) -> None:
        # Tick at the gcd-ish of the poll cadences; here, fastest var / 4.
        tick = max(0.05, min(v.poll_ms for v in self.vars.values()) / 1000.0 / 4)
        while not self._stop.wait(tick):
            self._poll_due()

    def _poll_due(self) -> None:
        now = time.monotonic()
        for cfg in self.vars.values():
            last = self._last_poll.get(cfg.alias, 0.0)
            if (now - last) * 1000.0 < cfg.poll_ms:
                continue
            self._last_poll[cfg.alias] = now
            raw = self.client.read_rapid(cfg.task, cfg.module, cfg.symbol)
            if raw is None:
                continue
            try:
                value = _coerce(cfg, raw)
            except Exception as e:
                log.warning("robot var '%s': coerce %r failed: %s",
                            cfg.alias, raw, e)
                continue
            self._observe(cfg, value)

    def _observe(self, cfg: RobotVariableConfig, value: Any) -> None:
        """Update cache + dispatch to targets if the value changed."""
        with self._lock:
            prev = self._values.get(cfg.alias, _MISSING)
            self._values[cfg.alias] = value
        if prev == value:
            return
        self._dispatch(cfg, prev, value)
        if self._bus is not None:
            from events import RobotVarChanged, signals
            self._bus.publish(signals.robot_var_changed, RobotVarChanged(
                alias=cfg.alias,
                value=value,
                prev=(None if prev is _MISSING else prev),
            ))

    def _dispatch(self, cfg: RobotVariableConfig, prev: Any, value: Any) -> None:
        if "ui" in cfg.targets:
            for cb in list(self._cbs[cfg.alias]):
                try:
                    cb(value)
                except Exception as e:
                    log.warning("robot var '%s' on_change cb failed: %s",
                                cfg.alias, e)
        if "log" in cfg.targets and self._errors is not None:
            try:
                self._errors.log(
                    device="robot",
                    subsystem="vars",
                    code=_alias_code(cfg.alias),
                    title=cfg.alias,
                    severity="info",
                    message=str(value),
                )
            except Exception as e:
                log.warning("robot var '%s' log dispatch failed: %s",
                            cfg.alias, e)
        if "plc" in cfg.targets and self._plc is not None and cfg.plc_alias:
            try:
                self._plc.write(cfg.plc_alias, value)
            except Exception as e:
                log.warning("robot var '%s' plc dispatch failed: %s",
                            cfg.alias, e)


# ---- helpers ----------------------------------------------------------------

_MISSING = object()


def _coerce(cfg: RobotVariableConfig, raw: str) -> Any:
    """RWS string → typed value."""
    s = raw.strip()
    if cfg.type == "num":
        # RAPID `num` values can be int or float. Prefer int when possible.
        try:
            if "." in s or "e" in s.lower():
                return float(s)
            return int(s)
        except ValueError:
            return float(s)
    if cfg.type == "bool":
        return s.upper() in ("TRUE", "1")
    # string: RAPID returns strings quoted ("hello"); strip them.
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    return s


def _encode(cfg: RobotVariableConfig, value: Any) -> str:
    """Typed value → RAPID-formatted string for an RWS write."""
    if cfg.type == "num":
        if isinstance(value, bool):
            raise ValueError("num field received bool")
        return str(int(value)) if float(value).is_integer() else str(float(value))
    if cfg.type == "bool":
        return "TRUE" if value else "FALSE"
    # string
    s = str(value)
    return f'"{s}"'


def _alias_code(alias: str) -> int:
    """Stable small integer for ErrorsStore.log(code=...)."""
    return zlib.crc32(alias.encode("utf-8")) & 0x7fffffff
