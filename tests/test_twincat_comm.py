"""Tests for TwinCATConfig parsing + TwinCATComm validation/struct logic."""
from __future__ import annotations

import ctypes
from pathlib import Path

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


def test_ensure_published_swallows_subscribe_errors(signals_toml: Path, caplog):
    """Connection-time ADS errors must not propagate from ensure_published.

    A missing PLC at startup would otherwise tear down unrelated
    components (UI, DB, robot) via NiceGUI's @app.on_startup hook.
    """
    import logging
    from event_bus import EventBus
    bus = EventBus()
    comm = TwinCATComm.from_toml(signals_toml, bus=bus)
    def _boom(*_a, **_kw):
        raise RuntimeError("ADS down")
    comm._conn.add_device_notification = _boom
    with caplog.at_level(logging.WARNING, logger="twincat_comm"):
        comm.ensure_published("sick.enable")          # must not raise
    assert any(
        "ensure_published(sick.enable) failed" in r.message
        for r in caplog.records
    )


def test_ensure_published_no_op_without_bus(signals_toml: Path):
    """No bus configured ⇒ ensure_published is a silent no-op (unchanged)."""
    comm = TwinCATComm.from_toml(signals_toml)        # no bus=
    def _boom(*_a, **_kw):
        raise RuntimeError("would not be reached")
    comm._conn.add_device_notification = _boom
    comm.ensure_published("sick.enable")              # must not raise, must not call
