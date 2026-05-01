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


def _field_resolves(ftype: str, built: dict) -> bool:
    """A struct field's type is resolvable when it's a primitive or already-built struct."""
    return ftype.upper() in PRIMITIVE_TYPES or ftype in built


def _resolve_field(
    sname: str, fname: str, ftype: str, built: dict,
) -> tuple[str, type]:
    """Return (canonical_type_string, ctypes_class) for a struct field.

    Primitives are uppercased so the ftype string is canonical; struct
    names stay as-is (case-sensitive lookup).
    """
    ftype_u = ftype.upper()
    if ftype_u in PRIMITIVE_TYPES:
        return ftype_u, PRIMITIVE_TYPES[ftype_u][1]
    if ftype in built:
        return ftype, built[ftype].ctypes_class
    raise ValueError(
        f"Struct '{sname}.{fname}': type '{ftype}' is neither a primitive "
        f"nor a known struct"
    )


def _unpack_struct(raw: Any) -> dict:
    """Recursively dict-ify a ctypes.Structure value.

    Nested struct fields become nested dicts; primitives pass through.
    Used by TwinCATComm._unpack to produce caller-friendly read shapes.
    """
    out: dict = {}
    for fname, _ in raw._fields_:
        val = getattr(raw, fname)
        if isinstance(val, ctypes.Structure):
            out[fname] = _unpack_struct(val)
        else:
            out[fname] = val
    return out


def _populate_struct(
    target: Any, value: dict, struct_def: "StructDef", all_structs: dict,
) -> None:
    """Apply value dict's entries into target ctypes.Structure.

    For nested struct fields where the value is a dict, recurse into the
    existing nested struct (modifying it in place — getattr on a struct
    field returns a reference that shares memory with the parent). For
    primitives, plain setattr.
    """
    for fname, fval in value.items():
        ftype = struct_def.fields[fname]
        if ftype in all_structs:
            nested_def = all_structs[ftype]
            if isinstance(fval, dict):
                unknown = set(fval) - set(nested_def.fields)
                if unknown:
                    raise KeyError(
                        f"Unknown fields for {ftype}: {unknown}. "
                        f"Valid: {sorted(nested_def.fields)}"
                    )
                nested = getattr(target, fname)
                _populate_struct(nested, fval, nested_def, all_structs)
            elif isinstance(fval, nested_def.ctypes_class):
                setattr(target, fname, fval)
            else:
                raise TypeError(
                    f"Field '{fname}' (type {ftype}) requires a dict or "
                    f"{nested_def.ctypes_class.__name__}, got {type(fval).__name__}"
                )
        else:
            setattr(target, fname, fval)


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
        """Build ctypes.Structure classes from the TOML [structs.*] section.

        Supports nested struct fields: a field's type may be either a
        primitive (BOOL, DINT, REAL, ...) or the name of another struct
        defined in the same TOML. Iterates in rounds; each round builds
        any struct whose field types are all resolved (primitives or
        already-built structs). Stops when no progress is made. This
        accepts any TOML ordering — the user can define ST_BeltUnitData
        before ST_TopsheetData and it still resolves.
        """
        out: dict[str, StructDef] = {}
        pending: dict[str, dict[str, str]] = dict(specs)

        max_rounds = len(pending) + 1
        for _round in range(max_rounds):
            if not pending:
                break
            built_this_round = False
            for sname in list(pending):
                sfields = pending[sname]
                if not sfields:
                    raise ValueError(f"Struct '{sname}' has no fields")
                if not all(_field_resolves(ft, out) for ft in sfields.values()):
                    continue                          # deps not ready yet

                ctypes_fields = []
                ordered: "OrderedDict[str, str]" = OrderedDict()
                for fname, ftype in sfields.items():
                    ftype_norm, ctype = _resolve_field(sname, fname, ftype, out)
                    ordered[fname] = ftype_norm
                    ctypes_fields.append((fname, ctype))

                # TwinCAT 3 default pack_mode is 8. If a struct in the PLC has
                # {attribute 'pack_mode' := '1'} (or 2/4), set _pack_ accordingly
                # — easiest is to add a TOML option later if you actually use it.
                cls_ = type(
                    sname,
                    (ctypes.Structure,),
                    {"_pack_": 8, "_fields_": ctypes_fields},
                )
                out[sname] = StructDef(name=sname, fields=ordered, ctypes_class=cls_)
                del pending[sname]
                built_this_round = True

            if not built_this_round:
                unresolved = []
                for sname, sfields in pending.items():
                    bad = [
                        f"{fname}={ftype}" for fname, ftype in sfields.items()
                        if not _field_resolves(ftype, out)
                    ]
                    unresolved.append(f"{sname}({', '.join(bad)})")
                raise ValueError(
                    f"Struct(s) reference unknown types: {'; '.join(unresolved)}. "
                    f"Defined structs: {sorted(out)}, "
                    f"primitives: {sorted(PRIMITIVE_TYPES)}"
                )
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
        return _unpack_struct(raw)

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

        # Read-modify-write whenever the write isn't a guaranteed full overwrite:
        #   * top-level dict missing fields, OR
        #   * any value is a (possibly partial) nested dict
        # Costs one extra ADS round-trip per such write.
        needs_rmw = (
            set(value.keys()) != known
            or any(isinstance(fv, dict) for fv in value.values())
        )
        if needs_rmw:
            current = self._conn.read_by_name(v.symbol, cls_)
        else:
            current = cls_()

        _populate_struct(current, value, struct_def, self.config.structs)
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

    def ensure_published(self, alias: str, *, cycle_time_ms: int = 100) -> None:
        """Register a bus-publishing-only ADS notification for `alias`.

        Bus-mode subscribers should call this in their start() to declare
        "I want PlcSignalChanged events for this alias" without owning a
        per-callback subscription. The underlying `subscribe()` already
        publishes; we just need a notification registered so the controller
        actually fires it. Idempotent — second call for the same alias
        returns without re-registering.

        Requires `bus` to be set on the TwinCATComm — without a bus, this
        is a no-op (the registration would publish to nowhere).

        Connection-time ADS errors (PLC unreachable, missing routes) are
        logged as warnings rather than raised. ensure_published is
        declarative — callers say "I want events for this alias when the
        PLC has them"; if the PLC isn't there yet, that intent is still
        satisfied (no events, but no events is the correct answer).
        Surfacing the error would force every caller to wrap the call,
        and cascading the failure tears down startup of unrelated
        components (UI, DB, robot).
        """
        if self._bus is None:
            return
        if alias in self._notifications:
            return
        try:
            self.subscribe(
                alias, lambda _a, _v: None,
                cycle_time_ms=cycle_time_ms, on_change=True,
            )
        except Exception as e:
            log.warning(
                "ensure_published(%s) failed (%s); no PLC events will be "
                "published for this alias until the PLC is reachable",
                alias, e,
            )
