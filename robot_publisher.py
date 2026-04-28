"""robot_publisher.py — mirror RobotMonitor.is_ready to a PLC bool.

Pattern matches sick_publisher / camera_publisher: subscribe to a state
source, write to a PLC alias on change. Lets the PLC interlock on the
robot being ready.

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
    ready_alias: str   # PLC alias (in plc_signals.toml) to receive is_ready


class RobotPublisher:
    """RobotMonitor.is_ready → plc.write(ready_alias, bool) on every change."""

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
        self.plc.validate([self.cfg.ready_alias])
        # Push the current state once so the PLC's bool isn't stale at boot.
        self._publish(self.monitor.status())
        self._unsub = self.monitor.on_change(self._publish)

    def stop(self) -> None:
        if self._unsub is not None:
            self._unsub()
            self._unsub = None

    # ---- internals ----

    def _publish(self, status: RobotStatus) -> None:
        try:
            self.plc.write(self.cfg.ready_alias, bool(status.is_ready))
        except Exception as e:
            log.warning("robot ready write failed: %s", e)
