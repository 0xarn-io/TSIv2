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

        # Slot 0 → cardboard ("35x70", wood=0)
        cb = sizes.list("cardboard")
        names_cb = {r.name: r for r in cb}
        assert "35x70" in names_cb
        assert names_cb["35x70"].slot == 0
        assert names_cb["35x70"].width_mm == 889
        # Slot 2 → others ("Wood", wood=1)
        oth = sizes.list("others")
        names_oth = {r.name: r for r in oth}
        assert "Wood" in names_oth
        assert names_oth["Wood"].slot == 2
        # Slot 3 → cardboard ("test", wood=0)
        assert "test" in names_cb
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
        first_count = len(sizes.list("cardboard"))
        m._poll_once()                                # same data
        assert len(sizes.list("cardboard")) == first_count
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


def test_inbound_moves_row_when_wood_flag_flips(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[["X"]] + [[""]] * 19,
        dims  =[[100, 200, 0]] + [[0, 0, 0]] * 19,
    )
    try:
        m._poll_once()
        assert sizes.get_slot(0)[0] == "cardboard"
        # Robot flips wood=1.
        client.read_rapid_array_by_index.side_effect = lambda task, mod, sym, count: (
            [["X"]] + [[""]] * 19 if sym == "Master"
            else [[100, 200, 1]] + [[0, 0, 0]] * 19
        )
        m._poll_once()
        assert sizes.get_slot(0)[0] == "others"
        # Cardboard table no longer holds slot 0.
        assert all(r.slot != 0 for r in sizes.list("cardboard"))
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

        sizes.add("cardboard", Size(name="A", width_mm=100, length_mm=200, slot=5))

        assert client.write_rapid_array.call_count == 2  # master + dims
        # Master array contains "A" at slot 5.
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
        sid = sizes.add("cardboard",
                        Size(name="A", width_mm=100, length_mm=200, slot=2))
        client.write_rapid_array.reset_mock()

        sizes.delete("cardboard", sid)

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


def test_db_update_to_other_table_pushes_correct_wood_flag(tmp_path: Path):
    m, client, sizes = _make(tmp_path,
        master=[[""]] * 20, dims=[[0, 0, 0]] * 20,
    )
    try:
        m._poll_once()
        m._unsub_db = sizes.on_change(m._on_db_change)
        # upsert with wood=true → row lands in others, robot row should
        # have wood flag = 1.
        sizes.upsert_slot(7, "Wood", 1000, 1000, wood=True)
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
        sizes.upsert_slot(0, "A", 1, 2, wood=False)
        client.write_rapid_array.assert_not_called()
    finally:
        sizes.stop()
