# twincat_comm.py
"""TOML-driven pyads wrapper for TwinCAT variable lists, with UDT support."""
from __future__ import annotations

import ctypes
import tomllib
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pyads


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

        structs = cls._build_structs(data.get("structs", {}))
        variables = cls._build_vars(data.get("groups", {}), structs)
        return cls(net_id=net_id, port=port, structs=structs, variables=variables)

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

            # TwinCAT default: 1-byte packed. If you use {attribute 'pack_mode'},
            # adjust _pack_ to match (1, 2, 4, or 8).
            cls_ = type(
                sname,
                (ctypes.Structure,),
                {"_pack_": 1, "_fields_": ctypes_fields},
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

    def __init__(self, config: TwinCATConfig):
        self.config = config
        self._conn = pyads.Connection(config.net_id, config.port)
        # alias -> (notif_handle, user_handle)
        self._notifications: dict[str, tuple[int, int]] = {}

    def __enter__(self) -> "TwinCATComm":
        self._conn.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        for handles in list(self._notifications.values()):
            try:
                self._conn.del_device_notification(*handles)
            except pyads.ADSError:
                pass
        self._notifications.clear()
        self._conn.close()

    @classmethod
    def from_toml(cls, path: str | Path) -> "TwinCATComm":
        return cls(TwinCATConfig.from_toml(path))

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
        return self._unpack(v, raw)

    def write(self, alias: str, value: Any) -> None:
        v = self._resolve(alias)
        if v.is_struct:
            packed = self._pack_struct(v, value)
            self._conn.write_by_name(v.symbol, packed, v.plc_type)
        else:
            self._conn.write_by_name(v.symbol, value, v.plc_type)

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

        if set(value.keys()) != known:
            current = self._conn.read_by_name(v.symbol, cls_)
        else:
            current = cls_()

        for fname, fval in value.items():
            setattr(current, fname, fval)
        return current

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

        # NotificationAttrib takes seconds; we accept ms and convert.
        attr = pyads.NotificationAttrib(
            length=length,
            trans_mode=(pyads.ADSTRANS_SERVERONCHA if on_change
                        else pyads.ADSTRANS_SERVERCYCLE),
            max_delay=max_delay_ms / 1000,
            cycle_time=cycle_time_ms / 1000,
        )

        # Fresh closure per subscribe call — alias and callback are captured
        # safely. If you ever refactor this into a loop creating multiple
        # callbacks, bind them as default args: def _cb(..., a=alias, c=callback)
        @self._conn.notification(v.plc_type)
        def _cb(handle, name, timestamp, value):
            callback(alias, self._unpack(v, value))

        handles = self._conn.add_device_notification(v.symbol, attr, _cb)
        self._notifications[alias] = handles
        return handles

    def unsubscribe(self, handles: tuple[int, int]) -> None:
        self._conn.del_device_notification(*handles)
        # remove by value match
        for k, v in list(self._notifications.items()):
            if v == handles:
                del self._notifications[k]
                break