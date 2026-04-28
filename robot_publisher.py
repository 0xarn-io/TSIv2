"""robot_publisher.py — publish RobotStatus to a TwinCAT struct.

Pattern matches sick_publisher / camera_publisher: subscribe to a state
source, write to a PLC alias on change. Lets the PLC interlock on robot
state via a single atomic struct write.

Add [plc.robot_status] to app_config.toml to enable; omit to skip.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from robot_status import RobotMonitor, RobotStatus
from twincat_comm import TwinCATComm

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RobotStatusConfig:
    status_alias: str   # PLC alias of an ST_RobotStatus struct


def _status_to_struct(s: RobotStatus) -> dict:
    """RobotStatus → ST_RobotStatus dict.

    Adding a field: append it here AND in [structs.ST_RobotStatus] in
    plc_signals.toml. No other code changes.
    """
    return {
        "bReady":      s.is_ready,
        "bMotorsOn":   s.motors_on,
        "bAutoMode":   s.auto_mode,
        "bRunning":    s.running,
        "bGuardStop":  s.guard_stop,
        "bEStop":      s.estop,
        "nSpeedRatio": int(s.speed_ratio),
    }


class RobotPublisher:
    """RobotMonitor → ST_RobotStatus struct on the PLC, atomic per change."""

    def __init__(
        self,
        monitor: RobotMonitor,
        plc: TwinCATComm,
        cfg: RobotStatusConfig,
    ):
        self.monitor = monitor
        self.plc = plc
        self.cfg = cfg
        self._unsub: Callable[[], None] | None = None

    def start(self) -> None:
        self.plc.validate([self.cfg.status_alias])
        # Push current state once so the struct isn't stale on PLC at boot.
        self._publish(self.monitor.status())
        self._unsub = self.monitor.on_change(self._publish)

    def stop(self) -> None:
        if self._unsub is not None:
            self._unsub()
            self._unsub = None

    # ---- internals ----

    def _publish(self, status: RobotStatus) -> None:
        try:
            self.plc.write(self.cfg.status_alias, _status_to_struct(status))
        except Exception as e:
            log.warning("robot status write failed: %s", e)
