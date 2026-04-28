"""sizes_publisher.py — bridge active size ids from the PLC to setpoints.

For each configured (table, code_alias, setpoints_alias) mapping:
    PLC writes nId → Python looks up sizes.get(table, nId) → Python writes
    the matching ST_SizeSetpoints struct.

One instance handles N mappings (typically one per table — cardboard and
others). Mirrors RecipePublisher in spirit but is multi-table.

Behaviour matches RecipePublisher: missing PLC symbols are logged, not
fatal — the rest of the data layer stays up.
"""
from __future__ import annotations

import logging
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
    """Size → ST_SizeSetpoints dict.

    Adding a setpoint: append a field here AND in [structs.ST_SizeSetpoints]
    in plc_signals.toml. No other code changes.
    """
    return {
        "nWidthMm":  int(s.width_mm),
        "nLengthMm": int(s.length_mm),
        "fWidthIn":  float(s.width_in),
        "fLengthIn": float(s.length_in),
    }


class SizesPublisher:
    """N PLC subscriptions → sizes.get(table, id) → struct write."""

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

    def start(self) -> None:
        # Validate all aliases up-front (cheap, TOML-side check).
        aliases: list[str] = []
        for m in self.mappings:
            aliases += [m.code_alias, m.setpoints_alias]
        self.plc.validate(aliases)

        for m in self.mappings:
            # Push current size once so the PLC isn't stale at boot.
            try:
                current = self.plc.read(m.code_alias)
                self._apply(m, int(current))
            except Exception as e:
                log.warning("initial %s size read failed: %s", m.table, e)

            try:
                handles = self.plc.subscribe(
                    m.code_alias,
                    lambda _alias, val, m=m: self._apply(m, int(val)),
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
        for h in self._handles:
            try: self.plc.unsubscribe(h)
            except Exception as e: log.warning("unsubscribe size failed: %s", e)
        self._handles.clear()

    # ---- internals ----------------------------------------------------------

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
