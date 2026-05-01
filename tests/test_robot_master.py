"""Tests for RobotMasterMonitor — inbound (robot → DB) and outbound (DB → robot)."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from robot_master import RobotMasterMonitor, _Slot, _EMPTY_SLOT
from sizes_store  import Size, SizesConfig, SizesStore


def _store(tmp_path: Path) -> SizesStore:
    s = SizesStore.from_config(SizesConfig(db_path=str(tmp_path / "sizes.db")))
    s.start()
    return s


def _client(master=None, dims=None) -> MagicMock:
    c = MagicMock()
    c.read_rapid_array_by_index.side_effect = lambda task, mod, sym, count: (
        master if sym == "Master" else dims
    )
    c.write_rapid_array.return_value = True
    return c


def _make(tmp_path: Path, *, master, dims):
    sizes  = _store(tmp_path)
    client = _client(master, dims)
    m = RobotMasterMonitor(client, sizes, poll_ms=1000)
    return m, client, sizes


# ---- inbound (robot → DB) --------------------------------------------------

def test_inbound_imports_non_empty_slots(tmp_path: Path):
    m, _, sizes = _make(tmp_path,
        master=[["35x70"], [""], ["Wood"], ["test"]] + [[""]] * 16,
        dims  =[[889, 1778, 0], [0, 0, 0], [1000, 1000, 1], [1, 1, 0]]
              + [[0, 0, 0]] * 16,
    )
    try:
        m._poll_once()

        rows = {r.name: r for r in sizes.list()}
        # Slot 0 → "35x70", station3=False
        assert "35x70" in rows
        assert rows["35x70"].slot     == 0
        assert rows["35x70"].width_mm == 889
        assert rows["35x70"].station3 is False
        # Slot 2 → "Wood", station3=True
        assert "Wood" in rows
        assert rows["Wood"].slot     == 2
        assert rows["Wood"].station3 is True
        # Slot 3 → "test", station3=False
        assert "test" in rows
        assert rows["test"].station3 is False
    finally:
        sizes.stop()


def test_inbound_skip_unchanged_slots(tmp_path: Path):
    """Second poll with identical robot data shouldn't re-emit DB writes."""
    master = [["A"]] + [[""]] * 19
    dims   = [[1, 2, 0]] + [[0, 0, 0]] * 19
    m, _, sizes = _make(tmp_path, master=master, dims=dims)
    try:
        events: list = []
        sizes.on_change(events.append)
        m._poll_once()
        # Mirror runs in silent mode → events stay empty.
        assert events == []
        first_count = len(sizes.list())
        m._poll_once()                                # same data
        assert len(sizes.list()) == first_count
    finally:
        sizes.stop()


def test_inbound_clears_slot_when_robot_empties(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[["A"]] + [[""]] * 19,
        dims  =[[1, 2, 0]] + [[0, 0, 0]] * 19,
    )
    try:
        m._poll_once()
        assert sizes.get_slot(0) is not None
        # Robot empties slot 0 in next snapshot.
        client.read_rapid_array_by_index.side_effect = lambda task, mod, sym, count: (
            [[""]] * 20 if sym == "Master" else [[0, 0, 0]] * 20
        )
        m._poll_once()
        assert sizes.get_slot(0) is None
    finally:
        sizes.stop()


def test_inbound_updates_station3_flag(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[["X"]] + [[""]] * 19,
        dims  =[[100, 200, 0]] + [[0, 0, 0]] * 19,
    )
    try:
        m._poll_once()
        got = sizes.get_slot(0)
        assert got is not None and got.station3 is False
        # Robot flips station3=1.
        client.read_rapid_array_by_index.side_effect = lambda task, mod, sym, count: (
            [["X"]] + [[""]] * 19 if sym == "Master"
            else [[100, 200, 1]] + [[0, 0, 0]] * 19
        )
        m._poll_once()
        got = sizes.get_slot(0)
        assert got is not None and got.station3 is True
    finally:
        sizes.stop()


def test_inbound_does_not_loop_back_to_robot(tmp_path: Path):
    """Apply path uses sizes.silent() so on_change shouldn't push back."""
    m, client, sizes = _make(tmp_path,
        master=[["A"]] + [[""]] * 19,
        dims  =[[1, 2, 0]] + [[0, 0, 0]] * 19,
    )
    try:
        m.start()
        # Wait for the immediate first poll.
        import time
        time.sleep(0.2)
        m.stop()
        # Inbound writes happen via apply; they must not have triggered any
        # write_rapid_array call (which would be a loop-back).
        client.write_rapid_array.assert_not_called()
    finally:
        sizes.stop()


# ---- outbound (DB → robot) -------------------------------------------------

def test_db_add_pushes_to_robot(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[[""]] * 20, dims=[[0, 0, 0]] * 20,
    )
    try:
        m._poll_once()                            # seed cache
        client.write_rapid_array.reset_mock()
        m._unsub_db = sizes.on_change(m._on_db_change)

        sizes.add(Size(name="A", width_mm=100, length_mm=200, slot=5))

        assert client.write_rapid_array.call_count == 2  # master + dims
        master_call = next(
            c for c in client.write_rapid_array.call_args_list
            if c.args[2] == "Master"
        )
        assert master_call.args[3][5] == ["A"]
        dims_call = next(
            c for c in client.write_rapid_array.call_args_list
            if c.args[2] == "Master_Dimmensions"
        )
        assert dims_call.args[3][5] == [100, 200, 0]
    finally:
        sizes.stop()


def test_db_delete_clears_robot_slot(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[[""]] * 20, dims=[[0, 0, 0]] * 20,
    )
    try:
        m._poll_once()
        m._unsub_db = sizes.on_change(m._on_db_change)
        sid = sizes.add(Size(name="A", width_mm=100, length_mm=200, slot=2))
        client.write_rapid_array.reset_mock()

        sizes.delete(sid)

        # The reconcile loop pushes new arrays where slot 2 is cleared.
        master_call = next(
            c for c in client.write_rapid_array.call_args_list
            if c.args[2] == "Master"
        )
        assert master_call.args[3][2] == [""]
        dims_call = next(
            c for c in client.write_rapid_array.call_args_list
            if c.args[2] == "Master_Dimmensions"
        )
        assert dims_call.args[3][2] == [0, 0, 0]
    finally:
        sizes.stop()


def test_db_upsert_pushes_correct_station3_flag(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[[""]] * 20, dims=[[0, 0, 0]] * 20,
    )
    try:
        m._poll_once()
        m._unsub_db = sizes.on_change(m._on_db_change)
        # upsert with station3=True → robot row should have flag = 1.
        sizes.upsert_slot(7, "Wood", 1000, 1000, station3=True)
        dims_call = next(
            c for c in client.write_rapid_array.call_args_list
            if c.args[2] == "Master_Dimmensions"
        )
        assert dims_call.args[3][7] == [1000, 1000, 1]
    finally:
        sizes.stop()


def test_outbound_skips_when_value_unchanged(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[["A"]] + [[""]] * 19,
        dims  =[[1, 2, 0]] + [[0, 0, 0]] * 19,
    )
    try:
        m._poll_once()                                # seed cache from robot
        m._unsub_db = sizes.on_change(m._on_db_change)
        client.write_rapid_array.reset_mock()
        # DB now contains the row from inbound. Re-upsert with identical
        # values: SizesStore returns no-op, no on_change emit, no push.
        sizes.upsert_slot(0, "A", 1, 2, station3=False)
        client.write_rapid_array.assert_not_called()
    finally:
        sizes.stop()


# ---- bus-mode: origin-tag loop-back guard --------------------------------
# Bus mode replaces the silent() context with origin tagging. When
# _apply_inbound writes to the store with origin="robot_master", the bus
# subscriber sees that origin in the SizesChanged payload and ignores it,
# so the change isn't echoed back to the robot.

import asyncio
import threading
import time
from event_bus import EventBus


def _bus_loop():
    loop = asyncio.new_event_loop()
    started = threading.Event()
    def _run():
        asyncio.set_event_loop(loop)
        loop.call_soon(started.set)
        loop.run_forever()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    started.wait(timeout=1.0)
    return loop, t


def test_bus_mode_inbound_does_not_echo_back_via_origin_filter(tmp_path: Path):
    """Apply path tags origin="robot_master"; bus subscriber filters it out.
    No write_rapid_array calls should fire as a result of inbound polls."""
    bus = EventBus()
    loop, t = _bus_loop()
    bus.start(loop)
    sizes = SizesStore.from_config(
        SizesConfig(db_path=str(tmp_path / "sizes.db")), bus=bus,
    )
    sizes.start()
    client = _client(
        master=[["A"]] + [[""]] * 19,
        dims  =[[1, 2, 0]] + [[0, 0, 0]] * 19,
    )
    m = RobotMasterMonitor(client, sizes, poll_ms=1000, bus=bus)
    try:
        m.start()
        m._poll_once()
        # Wait for bus dispatch to settle.
        time.sleep(0.1)
        # The inbound apply tagged origin="robot_master"; subscriber must
        # have ignored it — no outbound writes.
        client.write_rapid_array.assert_not_called()
    finally:
        m.stop()
        sizes.stop()
        bus.stop()
        loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=2.0)
        loop.close()


def test_bus_mode_user_edit_pushes_to_robot(tmp_path: Path):
    """A user edit (origin="user") IS pushed to the robot via the bus."""
    bus = EventBus()
    loop, t = _bus_loop()
    bus.start(loop)
    sizes = SizesStore.from_config(
        SizesConfig(db_path=str(tmp_path / "sizes.db")), bus=bus,
    )
    sizes.start()
    client = _client(
        master=[[""]] * 20, dims=[[0, 0, 0]] * 20,
    )
    m = RobotMasterMonitor(client, sizes, poll_ms=1000, bus=bus)
    try:
        m.start()
        m._poll_once()              # seed cache (all empty)
        client.write_rapid_array.reset_mock()
        # User edit → SizesChanged with origin="user" (default).
        sizes.add(Size(name="Z", width_mm=100, length_mm=200,
                       slot=5, station3=False))
        for _ in range(50):
            if client.write_rapid_array.called:
                break
            time.sleep(0.02)
        # Bus subscriber should have pushed the new slot.
        assert client.write_rapid_array.called
        master_call = next(
            c for c in client.write_rapid_array.call_args_list
            if c.args[2] == "Master"
        )
        assert master_call.args[3][5] == ["Z"]
    finally:
        m.stop()
        sizes.stop()
        bus.stop()
        loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=2.0)
        loop.close()


def test_poll_once_releases_mastership_at_end(tmp_path: Path):
    """Each poll cycle ends with an explicit release — defensive against a
    leaked lock from a prior write whose release POST silently failed."""
    master = [['A']] + [['']] * 19
    dims   = [[1, 2, 0]] + [[0, 0, 0]] * 19
    m, client, sizes = _make(tmp_path, master=master, dims=dims)
    try:
        m._poll_once()
        client.release_mastership.assert_called_once()
    finally:
        sizes.stop()


def test_push_slot_releases_mastership_after_writes(tmp_path: Path):
    """After the master+dims write batch, release mastership explicitly."""
    master = [['']] * 20
    dims   = [[0, 0, 0]] * 20
    m, client, sizes = _make(tmp_path, master=master, dims=dims)
    try:
        m._poll_once()                                # seed cache
        m._push_slot(0, _Slot(name="X", width_mm=1, length_mm=2,
                              station3=False))
        # release_mastership called once per poll + once per push.
        assert client.release_mastership.call_count >= 2
    finally:
        sizes.stop()

