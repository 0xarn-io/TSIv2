"""twincat_comm.py — TOML-driven pyads wrapper for TwinCAT vars, with UDT support.

Public API:
    comm = TwinCATComm.from_toml("plc_signals.toml")
    comm.validate(["sick.event", "sick.live"])   # fail-fast at startup
    with comm:
        comm.write("sick.live", {"nWidth": 711, "nHeight": 1800, "nOffset": 0})
        comm.subscribe("sick.enable", lambda alias, val: print(alias, val))
"""
from __future__ import annotations

import ctypes
import logging
import time
import tomllib
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pyads

log = logging.getLogger(__name__)


def _short(v: Any) -> str:
    """Compact one-line repr — keeps struct dicts readable in log lines."""
    if isinstance(v, dict):
        return "{" + " ".join(f"{k}={val}" for k, val in v.items()) + "}"
    return repr(v)


# IEC primitive type → (pyads PLCTYPE_*, ctypes type for struct packing)
PRIMITIVE_TYPES: dict[str, tuple[int, type]] = {
    "BOOL":  (pyads.PLCTYPE_BOOL,  ctypes.c_bool),
    "BYTE":  (pyads.PLCTYPE_BYTE,  ctypes.c_uint8),
    "WORD":  (pyads.PLCTYPE_WORD,  ctypes.c_uint16),
    "DWORD": (pyads.PLCTYPE_DWORD, ctypes.c_uint32),
    "SINT":  (pyads.PLCTYPE_SINT,  ctypes.c_int8),
    "USINT": (pyads.PLCTYPE_USINT, ctypes.c_uint8),
    "INT":   (pyads.PLCTYPE_INT,   ctypes.c_int16),
    "UINT":  (pyads.PLCTYPE_UINT,  ctypes.c_uint16),
    "DINT":  (pyads.PLCTYPE_DINT,  ctypes.c_int32),
    "UDINT": (pyads.PLCTYPE_UDINT, ctypes.c_uint32),
    "LINT":  (pyads.PLCTYPE_LINT,  ctypes.c_int64),
    "ULINT": (pyads.PLCTYPE_ULINT, ctypes.c_uint64),
    "REAL":  (pyads.PLCTYPE_REAL,  ctypes.c_float),
    "LREAL": (pyads.PLCTYPE_LREAL, ctypes.c_double),
    "TIME":  (pyads.PLCTYPE_TIME,  ctypes.c_uint32),
    "DATE":  (pyads.PLCTYPE_DATE,  ctypes.c_uint32),
}


@dataclass(frozen=True)
class StructDef:
    name: str
    fields: "OrderedDict[str, str]"
    ctypes_class: type

    @property
    def size(self) -> int:
        return ctypes.sizeof(self.ctypes_class)


@dataclass(frozen=True)
class VarDef:
    alias: str
    symbol: str
    type_name: str
    plc_type: Any
    is_struct: bool


@dataclass
class TwinCATConfig:
    net_id: str
    port: int
    log_signals: bool = False
    timeout_ms: int = 5000          # per-call ADS response timeout
    structs: dict[str, StructDef] = field(default_factory=dict)
    variables: dict[str, VarDef] = field(default_factory=dict)

    @classmethod
    def from_toml(cls, path: str | Path) -> "TwinCATConfig":
        path = Path(path)
        with path.open("rb") as f:
            data = tomllib.load(f)

        try:
            net_id = data["ams"]["net_id"]
            port = int(data["ams"]["port"])
        except KeyError as e:
            raise ValueError(f"[ams] missing required key: {e}") from e

        log_signals = bool(data["ams"].get("log_signals", False))
        timeout_ms  = int(data["ams"].get("timeout_ms", 5000))
        structs = cls._build_structs(data.get("structs", {}))
        variables = cls._build_vars(data.get("groups", {}), structs)
        return cls(
            net_id=net_id, port=port,
            log_signals=log_signals, timeout_ms=timeout_ms,
            structs=structs, variables=variables,
        )

    @staticmethod
    def _build_structs(specs: dict[str, dict[str, str]]) -> dict[str, StructDef]:
        out: dict[str, StructDef] = {}
        for sname, sfields in specs.items():
            if not sfields:
                raise ValueError(f"Struct '{sname}' has no fields")

            ctypes_fields = []
            ordered = OrderedDict()
            for fname, ftype in sfields.items():
                ftype_u = ftype.upper()
                if ftype_u not in PRIMITIVE_TYPES:
                    raise ValueError(
                        f"Struct '{sname}.{fname}': type '{ftype}' is not a "
                        f"primitive. Nested structs not yet supported."
                    )
                ordered[fname] = ftype_u
                ctypes_fields.append((fname, PRIMITIVE_TYPES[ftype_u][1]))

            # TwinCAT 3 default pack_mode is 8. If a struct in the PLC has
            # {attribute 'pack_mode' := '1'} (or 2/4), set _pack_ accordingly
            # — easiest is to add a TOML option later if you actually use it.
            cls_ = type(
                sname,
                (ctypes.Structure,),
                {"_pack_": 8, "_fields_": ctypes_fields},
            )
            out[sname] = StructDef(name=sname, fields=ordered, ctypes_class=cls_)
        return out

    @staticmethod
    def _build_vars(
        groups: dict[str, dict[str, Any]],
        structs: dict[str, StructDef],
    ) -> dict[str, VarDef]:
        out: dict[str, VarDef] = {}
        for gname, group in groups.items():
            prefix = group.get("prefix", "").strip()
            for alias, spec in group.get("vars", {}).items():
                full_alias = f"{gname}.{alias}"
                try:
                    name = spec["name"]
                    type_name = spec["type"]
                except (KeyError, TypeError):
                    raise ValueError(
                        f"Var '{full_alias}' must define 'name' and 'type'"
                    ) from None

                symbol = f"{prefix}.{name}" if prefix else name
                tu = type_name.upper()

                if tu in PRIMITIVE_TYPES:
                    plc_type = PRIMITIVE_TYPES[tu][0]
                    is_struct = False
                elif type_name in structs:
                    plc_type = structs[type_name].ctypes_class
                    is_struct = True
                else:
                    raise ValueError(
                        f"Var '{full_alias}': type '{type_name}' is neither a "
                        f"primitive nor a defined struct. "
                        f"Known structs: {list(structs)}"
                    )

                out[full_alias] = VarDef(
                    alias=full_alias, symbol=symbol, type_name=type_name,
                    plc_type=plc_type, is_struct=is_struct,
                )
        return out


class TwinCATComm:
    """pyads.Connection wrapper driven by a TOML var list. Supports UDTs."""

    def __init__(self, config: TwinCATConfig, *, bus=None):
        self.config = config
        self._bus   = bus
        self._conn = pyads.Connection(config.net_id, config.port)
        # alias -> (notif_handle, user_handle)
        self._notifications: dict[str, tuple[int, int]] = {}

    @classmethod
    def from_toml(cls, path: str | Path, *, bus=None) -> "TwinCATComm":
        return cls(TwinCATConfig.from_toml(path), bus=bus)

    # ---- lifecycle ------------------------------------------------------

    def open(self) -> None:
        self._conn.open()
        try:
            self._conn.set_timeout(self.config.timeout_ms)
        except Exception as e:
            log.warning("set_timeout(%s ms) failed: %s",
                        self.config.timeout_ms, e)

    def close(self) -> None:
        for handles in list(self._notifications.values()):
            try:
                self._conn.del_device_notification(*handles)
            except pyads.ADSError:
                pass
        self._notifications.clear()
        self._conn.close()

    def __enter__(self) -> "TwinCATComm":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ---- validation -----------------------------------------------------

    def validate(self, required_aliases: list[str]) -> None:
        """Raise if any alias is missing from the loaded TOML. Call at startup."""
        missing = [a for a in required_aliases if a not in self.config.variables]
        if missing:
            known = sorted(self.config.variables)
            raise KeyError(
                f"plc_signals.toml is missing required alias(es): {missing}. "
                f"Known aliases: {known}"
            )

    # ---- read / write ---------------------------------------------------

    def _resolve(self, alias: str) -> VarDef:
        try:
            return self.config.variables[alias]
        except KeyError:
            raise KeyError(
                f"Unknown var '{alias}'. Known: {sorted(self.config.variables)}"
            ) from None

    def read(self, alias: str) -> Any:
        v = self._resolve(alias)
        raw = self._conn.read_by_name(v.symbol, v.plc_type)
        val = self._unpack(v, raw)
        if self.config.log_signals:
            log.info("ads R %s = %s", alias, _short(val))
        return val

    def write(self, alias: str, value: Any) -> None:
        v = self._resolve(alias)
        if v.is_struct:
            packed = self._pack_struct(v, value)
            self._conn.write_by_name(v.symbol, packed, v.plc_type)
        else:
            self._conn.write_by_name(v.symbol, value, v.plc_type)
        if self.config.log_signals:
            log.info("ads W %s = %s", alias, _short(value))

    def _unpack(self, v: VarDef, raw: Any) -> Any:
        if not v.is_struct:
            return raw
        return {f: getattr(raw, f) for f, _ in raw._fields_}

    def _pack_struct(self, v: VarDef, value: Any) -> Any:
        cls_ = v.plc_type
        if isinstance(value, cls_):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"Writing struct '{v.alias}' requires a dict, got {type(value)}"
            )

        struct_def = self.config.structs[v.type_name]
        known = set(struct_def.fields)
        unknown = set(value) - known
        if unknown:
            raise KeyError(
                f"Unknown fields for {v.type_name}: {unknown}. "
                f"Valid: {sorted(known)}"
            )

        # Partial dict → read-modify-write so untouched fields aren't zeroed.
        # Costs one extra ADS round-trip per partial write.
        if set(value.keys()) != known:
            current = self._conn.read_by_name(v.symbol, cls_)
        else:
            current = cls_()

        for fname, fval in value.items():
            setattr(current, fname, fval)
        return current

    # ---- notifications --------------------------------------------------

    def subscribe(
        self,
        alias: str,
        callback: Callable[[str, Any], None],
        *,
        cycle_time_ms: int = 100,
        max_delay_ms: int = 100,
        on_change: bool = True,
    ) -> tuple[int, int]:
        v = self._resolve(alias)
        length = (ctypes.sizeof(v.plc_type) if v.is_struct
                  else ctypes.sizeof(PRIMITIVE_TYPES[v.type_name.upper()][1]))

        attr = pyads.NotificationAttrib(
            length=length,
            trans_mode=(pyads.ADSTRANS_SERVERONCHA if on_change
                        else pyads.ADSTRANS_SERVERCYCLE),
            max_delay=max_delay_ms / 1000,
            cycle_time=cycle_time_ms / 1000,
        )

        log_signals = self.config.log_signals

        bus = self._bus

        @self._conn.notification(v.plc_type)
        def _cb(handle, name, timestamp, value):
            unpacked = self._unpack(v, value)
            if log_signals:
                log.info("ads N %s = %s", alias, _short(unpacked))
            # Bridge mode: fire the legacy callback first, publish second.
            # publish() returns immediately; thread-mode subscribers
            # offload work so the AmsRouter thread isn't blocked.
            callback(alias, unpacked)
            if bus is not None:
                from events import PlcSignalChanged, signals
                bus.publish(signals.plc_signal_changed, PlcSignalChanged(
                    alias=alias, value=unpacked, ts=time.time(),
                ))

        handles = self._conn.add_device_notification(v.symbol, attr, _cb)
        self._notifications[alias] = handles
        return handles

    def unsubscribe(self, handles: tuple[int, int]) -> None:
        self._conn.del_device_notification(*handles)
        for k, v in list(self._notifications.items()):
            if v == handles:
                del self._notifications[k]
                break
