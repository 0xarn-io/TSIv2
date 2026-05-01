"""Tests for TwinCATConfig parsing + TwinCATComm validation/struct logic."""
from __future__ import annotations

import ctypes
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from twincat_comm import (
    PRIMITIVE_TYPES, StructDef, TwinCATComm, TwinCATConfig, VarDef,
)


def test_from_toml_parses_ams(signals_toml: Path):
    cfg = TwinCATConfig.from_toml(signals_toml)
    assert cfg.net_id == "1.2.3.4.1.1"
    assert cfg.port == 851


def test_from_toml_builds_structs(signals_toml: Path):
    cfg = TwinCATConfig.from_toml(signals_toml)
    assert "ST_SickEvent" in cfg.structs
    assert "ST_SickLive" in cfg.structs
    live = cfg.structs["ST_SickLive"]
    assert list(live.fields) == ["nWidth", "nHeight", "nOffset"]
    # 3 packed DINTs = 12 bytes
    assert live.size == 12


def test_struct_alignment_matches_twincat3_default():
    """ST_SickEvent must layout to 52 bytes (TwinCAT 3 default pack_mode=8).

    BOOL(1) + pad(3) + 10×DINT(4) + REAL(4) + DINT(4) = 52
    Wrong _pack_ = 1 gave 49 bytes and shifted every field after bNew → garbage.
    """
    import tempfile, textwrap
    with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as f:
        f.write(textwrap.dedent("""
            [ams]
            net_id = "1.2.3.4.1.1"
            port = 851
            [structs.ST_SickEvent]
            bNew=  "BOOL"
            nLength= "DINT"
            nWidthMean= "DINT"
            nWidthMin=  "DINT"
            nWidthMax=  "DINT"
            nHeightMean="DINT"
            nHeightMin= "DINT"
            nHeightMax= "DINT"
            nOffsetMean="DINT"
            nOffsetMin= "DINT"
            nOffsetMax= "DINT"
            fDuration=  "REAL"
            nSamples=   "DINT"
            [groups.x]
            prefix = ""
            [groups.x.vars]
            ev = { name = "Ev", type = "ST_SickEvent" }
        """))
        path = f.name
    cfg = TwinCATConfig.from_toml(path)
    assert cfg.structs["ST_SickEvent"].size == 52


def test_from_toml_builds_aliases(signals_toml: Path):
    cfg = TwinCATConfig.from_toml(signals_toml)
    assert set(cfg.variables) == {
        "sick.event", "sick.live", "sick.enable",
        "health.heartbeat", "robot.status",
        "recipe.code", "recipe.setpoints",
    }
    enable = cfg.variables["sick.enable"]
    assert enable.symbol == "GVL_Sick.bEnable"
    assert enable.is_struct is False
    setpoints = cfg.variables["recipe.setpoints"]
    assert setpoints.symbol    == "GVL.RecipeSetpoints"
    assert setpoints.type_name == "ST_RecipeSetpoints"
    assert setpoints.is_struct is True


def test_unknown_type_raises(tmp_path: Path):
    p = tmp_path / "bad.toml"
    p.write_text("""
[ams]
net_id = "1.1.1.1.1.1"
port = 851

[groups.x]
prefix = ""

[groups.x.vars]
foo = { name = "Foo", type = "WIDGET" }
""")
    with pytest.raises(ValueError, match="neither a primitive nor a defined struct"):
        TwinCATConfig.from_toml(p)


def test_validate_passes_when_aliases_present(signals_toml: Path):
    comm = TwinCATComm.from_toml(signals_toml)
    comm.validate(["sick.event", "sick.live", "sick.enable"])


def test_validate_raises_on_missing_alias(signals_toml: Path):
    comm = TwinCATComm.from_toml(signals_toml)
    with pytest.raises(KeyError, match="missing required alias"):
        comm.validate(["sick.event", "nonexistent.alias"])


def test_pack_struct_full_dict_no_read(signals_toml: Path):
    """When all fields are supplied, _pack_struct must not call ADS read."""
    comm = TwinCATComm.from_toml(signals_toml)
    v = comm.config.variables["sick.live"]

    def boom(*_a, **_kw):
        raise AssertionError("read_by_name must not be called for full dict")
    comm._conn.read_by_name = boom

    packed = comm._pack_struct(v, {"nWidth": 711, "nHeight": 1800, "nOffset": 0})
    assert packed.nWidth == 711
    assert packed.nHeight == 1800
    assert packed.nOffset == 0


def test_pack_struct_partial_dict_triggers_read(signals_toml: Path):
    """Partial dicts should fall through to read-modify-write."""
    comm = TwinCATComm.from_toml(signals_toml)
    v = comm.config.variables["sick.live"]
    cls_ = v.plc_type
    seen = {"called": False}

    def fake_read(symbol, ptype):
        seen["called"] = True
        out = cls_()
        out.nWidth = 100; out.nHeight = 200; out.nOffset = 300
        return out
    comm._conn.read_by_name = fake_read

    packed = comm._pack_struct(v, {"nWidth": 999})
    assert seen["called"] is True
    assert packed.nWidth == 999
    assert packed.nHeight == 200      # preserved
    assert packed.nOffset == 300      # preserved


def test_pack_struct_unknown_field(signals_toml: Path):
    comm = TwinCATComm.from_toml(signals_toml)
    v = comm.config.variables["sick.live"]
    with pytest.raises(KeyError, match="Unknown fields"):
        comm._pack_struct(v, {"nBogus": 1})


def test_unpack_struct_returns_dict(signals_toml: Path):
    comm = TwinCATComm.from_toml(signals_toml)
    v = comm.config.variables["sick.live"]
    cls_ = v.plc_type
    raw = cls_(); raw.nWidth = 1; raw.nHeight = 2; raw.nOffset = 3
    out = comm._unpack(v, raw)
    assert out == {"nWidth": 1, "nHeight": 2, "nOffset": 3}


def test_unpack_primitive_passthrough(signals_toml: Path):
    comm = TwinCATComm.from_toml(signals_toml)
    v = comm.config.variables["sick.enable"]
    assert comm._unpack(v, True) is True


def test_resolve_unknown_alias_lists_known(signals_toml: Path):
    comm = TwinCATComm.from_toml(signals_toml)
    with pytest.raises(KeyError, match="sick.enable"):
        comm._resolve("does.not.exist")


def test_log_signals_default_off(signals_toml: Path):
    cfg = TwinCATConfig.from_toml(signals_toml)
    assert cfg.log_signals is False


def test_log_signals_can_be_enabled(tmp_path: Path):
    p = tmp_path / "sig.toml"
    p.write_text("""
[ams]
net_id = "1.2.3.4.1.1"
port = 851
log_signals = true

[groups.x]
prefix = ""
[groups.x.vars]
flag = { name = "Foo", type = "BOOL" }
""")
    cfg = TwinCATConfig.from_toml(p)
    assert cfg.log_signals is True


def test_read_logs_when_enabled(signals_toml: Path, caplog):
    import logging
    comm = TwinCATComm.from_toml(signals_toml)
    comm.config = TwinCATConfig(
        net_id=comm.config.net_id, port=comm.config.port, log_signals=True,
        structs=comm.config.structs, variables=comm.config.variables,
    )
    comm._conn.read_by_name = lambda *_a, **_kw: True
    with caplog.at_level(logging.INFO, logger="twincat_comm"):
        comm.read("sick.enable")
    assert any("ads R sick.enable" in r.message for r in caplog.records)


def test_read_silent_when_disabled(signals_toml: Path, caplog):
    import logging
    comm = TwinCATComm.from_toml(signals_toml)  # log_signals defaults to False
    comm._conn.read_by_name = lambda *_a, **_kw: True
    with caplog.at_level(logging.INFO, logger="twincat_comm"):
        comm.read("sick.enable")
    assert not any("ads R" in r.message for r in caplog.records)


def test_write_logs_when_enabled(signals_toml: Path, caplog):
    import logging
    comm = TwinCATComm.from_toml(signals_toml)
    comm.config = TwinCATConfig(
        net_id=comm.config.net_id, port=comm.config.port, log_signals=True,
        structs=comm.config.structs, variables=comm.config.variables,
    )
    comm._conn.write_by_name = lambda *_a, **_kw: None
    with caplog.at_level(logging.INFO, logger="twincat_comm"):
        comm.write("sick.enable", True)
    assert any("ads W sick.enable" in r.message for r in caplog.records)


def test_primitive_types_complete():
    """Make sure all referenced PLCTYPE_* exist on pyads (smoke check)."""
    for name, (plc_const, ctype) in PRIMITIVE_TYPES.items():
        assert plc_const is not None, name
        assert ctypes.sizeof(ctype) > 0, name


# ---- bus integration: subscribe → bus.publish round-trip --------------------

class _FakeAdsConn:
    """Stand-in for pyads.Connection that captures registered notification
    callbacks so tests can fire them and exercise the bus round-trip."""

    def __init__(self):
        self.callbacks: dict[str, callable] = {}     # symbol -> _cb
        self._next_handle = 1

    def notification(self, _plc_type):
        # Real pyads decorator wraps the cb to unmarshal C types; for the
        # test we want the raw cb so we can hand it ready-unpacked values.
        return lambda cb: cb

    def add_device_notification(self, symbol, _attr, cb):
        self.callbacks[symbol] = cb
        h = (self._next_handle, self._next_handle + 1)
        self._next_handle += 2
        return h

    def del_device_notification(self, *_a):
        pass

    def fire(self, symbol, value):
        """Simulate an ADS notification firing for `symbol`."""
        self.callbacks[symbol](handle=0, name=symbol, timestamp=0, value=value)


def test_ensure_published_registers_notification_and_bus_emits(signals_toml):
    """Integration: ensure_published() registers an ADS notification, and
    when that notification fires (via the captured cb), PlcSignalChanged
    is published on the bus with the right alias + value.

    This catches the exact bug fixed in this PR: bus subscribers that
    call only bus.subscribe() and never plc.ensure_published() will
    silently never receive anything in production."""
    import asyncio
    from event_bus import EventBus
    from events import signals as bus_signals

    bus = EventBus()
    loop = asyncio.new_event_loop()
    bus.start(loop)
    try:
        comm = TwinCATComm.from_toml(signals_toml, bus=bus)
        comm._conn = _FakeAdsConn()

        received: list = []
        bus.subscribe(bus_signals.plc_signal_changed,
                      lambda p: received.append(p), mode="sync")

        # No notification yet → bus emits nothing even when fired.
        # (alias 'recipe.code' resolves to 'GVL.nRecipeCode' per signals_toml.)
        assert "GVL.nRecipeCode" not in comm._conn.callbacks

        # Register via ensure_published; check the underlying pyads
        # notification got created.
        comm.ensure_published("recipe.code")
        assert "GVL.nRecipeCode" in comm._conn.callbacks

        # Fire a value change and verify the bus subscriber receives it.
        comm._conn.fire("GVL.nRecipeCode", 42)
        assert len(received) == 1
        assert received[0].alias == "recipe.code"
        assert received[0].value == 42

        # Idempotent per alias: two ensure_published calls = one cb.
        before = comm._conn.callbacks["GVL.nRecipeCode"]
        comm.ensure_published("recipe.code")
        assert comm._conn.callbacks["GVL.nRecipeCode"] is before
    finally:
        bus.stop()
        loop.close()


def test_recipe_publisher_bus_round_trip(signals_toml, tmp_path):
    """End-to-end: real TwinCATComm + fake ADS conn + real RecipePublisher
    on the bus. Operator changes recipe.code on PLC → fake notification
    fires → bus emits → recipe_publisher writes setpoints. This is the
    full path that v1 of the bus migration silently broke."""
    import asyncio
    from event_bus import EventBus
    from recipe_publisher import RecipePublisher, RecipePublisherConfig
    from recipes_store import Recipe

    bus = EventBus()
    loop = asyncio.new_event_loop()
    bus.start(loop)
    try:
        comm = TwinCATComm.from_toml(signals_toml, bus=bus)
        comm._conn = _FakeAdsConn()
        # Bypass the real pyads write path so we can observe what was written.
        writes = []
        comm.write = lambda alias, val: writes.append((alias, val))
        # Bootstrap reads `recipe.code`; route it through the fake.
        comm.read = lambda alias: 0 if alias == "recipe.code" else None

        recipes = MagicMock()
        recipes.get.return_value = Recipe(code=7, x_topsheet_length=999)

        pub = RecipePublisher(recipes, comm, RecipePublisherConfig(
            code_alias="recipe.code", setpoints_alias="recipe.setpoints",
        ), bus=bus)
        pub.start()
        try:
            # Notification was registered against the real symbol.
            assert "GVL.nRecipeCode" in comm._conn.callbacks

            # Operator changes the code on the PLC.
            comm._conn.fire("GVL.nRecipeCode", 7)

            # Bus dispatch is mode='thread'; wait for the write.
            import time
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and not writes:
                time.sleep(0.01)
            assert len(writes) >= 1
            alias, struct = writes[-1]
            assert alias == "recipe.setpoints"
            assert struct["nXTopsheetLength"] == 999
        finally:
            pub.stop()
    finally:
        bus.stop()
        loop.close()
