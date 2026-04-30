"""robot_master.py — live two-way mirror of the robot's Master arrays
into the local SizesStore.

The robot owns two PERS arrays (RAPID):
    Master            : string[20,1]   — slot name
    Master_Dimmensions: num[20,3]      — [width_mm, length_mm, wood01]

Slot indexing is 1..20 in RAPID; we use 0..19 internally and translate at
the boundary.

Mirroring rules:
    Robot slot has data       → upsert_slot in DB (wood flag picks the table).
    Robot slot is empty       → clear_slot in DB.
    DB row added/updated      → push (name, w, l, wood) to that slot.
    DB row deleted            → write empty values to that slot.

Loop-back guard: the monitor maintains a per-slot `_last_pushed` snapshot;
robot polls that come back with the same value the monitor just wrote
are skipped, so a DB→robot push doesn't immediately re-fire a robot→DB
write of the same data. DB-side mutations done while applying an inbound
robot snapshot are wrapped in `sizes.silent()` so on_change doesn't echo
back to the robot.

No nicegui import — strictly data layer.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from sizes_store import (
    SLOT_COUNT, Size, SizesChange, SizesStore,
    is_wood_table, table_for_wood,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Slot:
    name:      str
    width_mm:  int
    length_mm: int
    wood:      bool

    @property
    def empty(self) -> bool:
        return (
            not self.name
            and self.width_mm == 0
            and self.length_mm == 0
        )


_EMPTY_SLOT = _Slot(name="", width_mm=0, length_mm=0, wood=False)


class RobotMasterMonitor:
    """Live two-way sync between robot Master arrays and SizesStore."""

    def __init__(
        self,
        client,                            # rws_client.RWSClient (duck-typed)
        sizes:  SizesStore,
        *,
        task:           str = "T_ROB1",
        module:         str = "MainModule",
        master_symbol:  str = "Master",
        dims_symbol:    str = "Master_Dimmensions",
        poll_ms:        int = 2000,
    ):
        self.client       = client
        self.sizes        = sizes
        self.task         = task
        self.module       = module
        self.master_symbol = master_symbol
        self.dims_symbol  = dims_symbol
        self.poll_ms      = int(poll_ms)

        # Last value we wrote (or last value we observed from the robot).
        # Lets us suppress no-op pushes and ignore loop-back reads.
        self._last_robot: list[_Slot] = [_EMPTY_SLOT] * SLOT_COUNT

        self._stop:   threading.Event   | None = None
        self._thread: threading.Thread  | None = None
        self._unsub_db: callable | None = None

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        # Subscribe to DB edits so our outbound mirror runs synchronously
        # the moment something is added/updated/deleted in the panel.
        self._unsub_db = self.sizes.on_change(self._on_db_change)
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="robot-master",
        )
        self._thread.start()

    def stop(self) -> None:
        if self._unsub_db is not None:
            try: self._unsub_db()
            except Exception: pass
            self._unsub_db = None
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ---- inbound poll (robot → DB) ----------------------------------------

    def _run(self) -> None:
        period_s = self.poll_ms / 1000.0
        # Tick once immediately, then on every period.
        self._poll_once()
        while not self._stop.wait(period_s):
            self._poll_once()

    def _poll_once(self) -> None:
        try:
            slots = self._read_robot()
        except Exception as e:
            log.warning("robot master read failed: %s", e)
            return
        if slots is None:
            return
        for i, slot in enumerate(slots):
            prev = self._last_robot[i]
            if slot == prev:
                continue
            self._apply_inbound(i, slot)
            self._last_robot[i] = slot

    def _read_robot(self) -> list[_Slot] | None:
        master = self.client.read_rapid_array(
            self.task, self.module, self.master_symbol,
        )
        dims = self.client.read_rapid_array(
            self.task, self.module, self.dims_symbol,
        )
        if master is None or dims is None:
            return None
        slots: list[_Slot] = []
        for i in range(SLOT_COUNT):
            try:
                # RAPID `string{20,1}` → [["name"], ["other"], …]; unwrap
                # the inner list.
                name_cell = master[i] if i < len(master) else [""]
                name = (
                    name_cell[0] if isinstance(name_cell, list) and name_cell
                    else name_cell
                ) or ""
                dims_row = dims[i] if i < len(dims) else [0, 0, 0]
                w  = int(dims_row[0]) if len(dims_row) > 0 else 0
                ln = int(dims_row[1]) if len(dims_row) > 1 else 0
                wd = bool(int(dims_row[2])) if len(dims_row) > 2 else False
            except Exception as e:
                log.warning("robot master parse slot %d failed: %s", i, e)
                slots.append(_EMPTY_SLOT)
                continue
            slots.append(_Slot(
                name=str(name), width_mm=w, length_mm=ln, wood=wd,
            ))
        return slots

    def _apply_inbound(self, slot: int, robot: _Slot) -> None:
        """Apply a single robot slot's value to the DB without echoing back."""
        with self.sizes.silent():
            try:
                if robot.empty:
                    self.sizes.clear_slot(slot)
                else:
                    self.sizes.upsert_slot(
                        slot,
                        name      = robot.name,
                        width_mm  = robot.width_mm,
                        length_mm = robot.length_mm,
                        wood      = robot.wood,
                    )
            except Exception as e:
                log.warning(
                    "robot master inbound apply failed slot=%d (%r): %s",
                    slot, robot, e,
                )

    # ---- outbound (DB → robot) --------------------------------------------

    def _on_db_change(self, ev: SizesChange) -> None:
        """Handler for SizesStore.on_change; fires from the editing thread."""
        # On delete we don't have a Size — but we know the sid; we have to
        # find which slot it occupied via _last_robot…  simpler: scan the DB
        # for any other row at any slot, and clear slots that no longer
        # have a DB owner. Since deletes are rare, full reconciliation is
        # cheap enough.
        if ev.op == "delete":
            self._reconcile()
            return

        s = ev.size
        if s is None or s.slot is None:
            # Add/update of a slot-less row — nothing to push.
            return
        slot = int(s.slot)
        if not (0 <= slot < SLOT_COUNT):
            return
        wood = is_wood_table(ev.table)
        new_slot = _Slot(
            name=s.name, width_mm=int(s.width_mm),
            length_mm=int(s.length_mm), wood=wood,
        )
        if self._last_robot[slot] == new_slot:
            return
        self._push_slot(slot, new_slot)

    def _reconcile(self) -> None:
        """After a delete, clear robot slots that no longer have a DB row."""
        for slot in range(SLOT_COUNT):
            current = self.sizes.get_slot(slot)
            if current is None:
                if not self._last_robot[slot].empty:
                    self._push_slot(slot, _EMPTY_SLOT)
                continue
            table, size = current
            wood = is_wood_table(table)
            new_slot = _Slot(
                name=size.name, width_mm=int(size.width_mm),
                length_mm=int(size.length_mm), wood=wood,
            )
            if self._last_robot[slot] != new_slot:
                self._push_slot(slot, new_slot)

    def _push_slot(self, slot: int, new_slot: _Slot) -> None:
        """Write a single slot's name + dims back to the robot (full-array
        rewrite — RWS doesn't expose per-element writes for arrays).
        """
        # Compose the full arrays from the cache, with this slot replaced.
        master_array = []
        dims_array   = []
        for i, prev in enumerate(self._last_robot):
            cur = new_slot if i == slot else prev
            master_array.append([cur.name])
            dims_array.append([cur.width_mm, cur.length_mm,
                               1 if cur.wood else 0])
        try:
            ok_m = self.client.write_rapid_array(
                self.task, self.module, self.master_symbol, master_array,
            )
            ok_d = self.client.write_rapid_array(
                self.task, self.module, self.dims_symbol, dims_array,
            )
            if not (ok_m and ok_d):
                log.warning(
                    "robot master push slot=%d incomplete (master=%s dims=%s)",
                    slot, ok_m, ok_d,
                )
                return
        except Exception as e:
            log.warning("robot master push failed slot=%d: %s", slot, e)
            return
        # Update cache so the next poll's compare sees the value as fresh.
        self._last_robot[slot] = new_slot
