"""rws_client.py — single home for ABB OmniCore RWS HTTP plumbing.

Owns the requests.Session (HTTP Basic auth, optional TLS verify, JSON-HAL
Accept header). Used by both RobotMonitor (status poller) and
RobotVariablesMonitor (RAPID symbol read/write) so we have one auth
flow, one timeout knob, one place to fix RWS quirks.

Reads:
    rws.read_rapid(task, module, symbol) -> str | None

Writes:
    rws.write_rapid(task, module, symbol, value) -> bool
        Wraps mastership request → write → release in one call so
        callers don't have to know about it.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

import requests
import urllib3
from requests.auth import HTTPBasicAuth

log = logging.getLogger(__name__)


class RWSClient:
    """Thin wrapper around an authenticated RWS session."""

    def __init__(self, cfg) -> None:
        # cfg is a RobotConfig (avoid the import to keep this module
        # decoupled — only attribute access is used).
        self.cfg = cfg
        self._session = self._make_session()

    # ---- HTTP primitives ----------------------------------------------------

    def _make_session(self) -> requests.Session:
        if not self.cfg.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        s = requests.Session()
        s.auth   = HTTPBasicAuth(self.cfg.username, self.cfg.password)
        s.verify = self.cfg.verify_ssl
        s.headers.update({"Accept": "application/hal+json;v=2.0"})
        return s

    def _url(self, path: str) -> str:
        return f"https://{self.cfg.ip}{path}"

    def get(
        self, path: str, *, params=None, silent: bool = False,
        timeout: float | None = None, accept: str | None = None,
    ) -> Optional[dict]:
        """JSON GET. Returns parsed dict on 200, else None (logged unless silent).

        `accept` overrides the session-level Accept header for this call only.
        """
        try:
            headers = {"Accept": accept} if accept else None
            r = self._session.get(
                self._url(path), params=params, headers=headers,
                timeout=timeout or self.cfg.timeout_s,
            )
            if r.status_code == 200:
                return r.json()
            if not silent:
                log.warning("RWS GET %s -> %s", path, r.status_code)
        except Exception as e:
            if not silent:
                log.warning("RWS GET %s failed: %s", path, e)
        return None

    def get_first(self, *paths: str, params=None) -> Optional[dict]:
        """Try paths in order, return the first 200. Warn only if all fail."""
        for p in paths:
            obj = self.get(p, params=params, silent=True)
            if obj is not None:
                return obj
        log.warning("RWS GET (all variants failed): %s", list(paths))
        return None

    def post(
        self, path: str, *, params=None, data=None, silent: bool = False,
        timeout: float | None = None,
    ) -> Optional[requests.Response]:
        """Form-encoded POST. Returns the Response (caller checks .ok), or None."""
        try:
            # Mirror the OmniCore SDK exactly: explicit Content-Type with
            # the v=2.0 suffix the controller insists on for some POSTs.
            r = self._session.post(
                self._url(path), params=params, data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded;v=2.0"},
                timeout=timeout or self.cfg.timeout_s,
            )
            if not r.ok and not silent:
                log.warning(
                    "RWS POST %s -> %s (%s)", path, r.status_code,
                    (r.text or "")[:200],
                )
            return r
        except Exception as e:
            if not silent:
                log.warning("RWS POST %s failed: %s", path, e)
        return None

    # ---- RAPID symbol convenience ------------------------------------------

    @staticmethod
    def _rapid_path(task: str, module: str, symbol: str,
                    subresource: str = "data") -> str:
        """OmniCore RWS uses plain slashes between path segments and puts
        the subresource (data / properties) at the end:

            /rw/rapid/symbol/RAPID/T_ROB1/Stations/Master/data

        This matches what the OmniCore JS SDK (omnicore-sdk.js
        getSymbolUrl) builds for both reads and writes.
        """
        return f"/rw/rapid/symbol/RAPID/{task}/{module}/{symbol}/{subresource}"

    def read_rapid(
        self, task: str, module: str, symbol: str,
    ) -> Optional[str]:
        """Read a RAPID symbol's raw value string. Returns None on failure."""
        obj = self.get(self._rapid_path(task, module, symbol, "data"))
        if obj is None:
            return None
        return _extract_rapid_value(obj)

    def write_rapid(
        self, task: str, module: str, symbol: str, value: str,
    ) -> bool:
        """Write a RAPID symbol value. Acquires + releases mastership.

        OmniCore's POST `.../data` with `value=…` body suffices — no
        `?action=set` query needed.
        """
        if not self._mastership("request"):
            return False
        try:
            r = self.post(
                self._rapid_path(task, module, symbol, "data"),
                data={"value": value},
            )
            return bool(r is not None and r.ok)
        finally:
            self._mastership("release", silent=True)

    # ---- RAPID array convenience -------------------------------------------

    def read_rapid_array(
        self, task: str, module: str, symbol: str,
    ) -> Any | None:
        """Read a RAPID array symbol; returns a nested Python list."""
        raw = self.read_rapid(task, module, symbol)
        if raw is None:
            return None
        try:
            return parse_rapid_array(raw)
        except Exception as e:
            log.warning("RAPID array parse failed for %s/%s/%s: %s",
                        task, module, symbol, e)
            return None

    def write_rapid_array(
        self, task: str, module: str, symbol: str, value: Any,
    ) -> bool:
        """Write a (possibly-nested) Python list to a RAPID array symbol."""
        return self.write_rapid(
            task, module, symbol, format_rapid_array(value),
        )

    # ---- mastership helpers ------------------------------------------------

    def _mastership(self, action: str, *, silent: bool = False) -> bool:
        r = self.post(
            "/rw/mastership/edit", params={"action": action}, silent=silent,
        )
        return bool(r is not None and r.ok)


# ---- module helpers --------------------------------------------------------

def parse_rapid_array(s: str) -> Any:
    """Parse a RAPID array literal (e.g. `[[889,1778,1],[711,1778,1]]`).

    Supports nested arrays of `num`, `bool`, and `string` (quoted).
    Returns a nested Python list / int / float / bool / str.
    """
    pos, value = _parse_value(s, 0)
    rest = s[pos:].strip()
    if rest:
        raise ValueError(f"trailing content in RAPID literal: {rest!r}")
    return value


def format_rapid_array(value: Any) -> str:
    """Inverse of parse_rapid_array. Strings get RAPID quote-and-escape."""
    if isinstance(value, list):
        return "[" + ",".join(format_rapid_array(v) for v in value) + "]"
    if isinstance(value, bool):                      # bool is an int subclass
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        # Whole floats serialize as int to match RAPID's num literal style.
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    raise TypeError(f"unsupported RAPID value: {type(value).__name__}")


# ---- internals --------------------------------------------------------------

def _parse_value(s: str, pos: int) -> tuple[int, Any]:
    pos = _skip_ws(s, pos)
    if pos >= len(s):
        raise ValueError("unexpected end of RAPID literal")
    ch = s[pos]
    if ch == "[":
        return _parse_array(s, pos)
    if ch == '"':
        return _parse_string(s, pos)
    return _parse_scalar(s, pos)


def _parse_array(s: str, pos: int) -> tuple[int, list]:
    assert s[pos] == "["
    pos += 1
    items: list = []
    pos = _skip_ws(s, pos)
    if pos < len(s) and s[pos] == "]":
        return pos + 1, items
    while True:
        pos, v = _parse_value(s, pos)
        items.append(v)
        pos = _skip_ws(s, pos)
        if pos >= len(s):
            raise ValueError("unterminated RAPID array")
        if s[pos] == ",":
            pos += 1
            continue
        if s[pos] == "]":
            return pos + 1, items
        raise ValueError(f"unexpected char {s[pos]!r} at pos {pos}")


def _parse_string(s: str, pos: int) -> tuple[int, str]:
    assert s[pos] == '"'
    pos += 1
    buf: list[str] = []
    while pos < len(s):
        ch = s[pos]
        if ch == "\\" and pos + 1 < len(s):
            buf.append(s[pos + 1])
            pos += 2
            continue
        if ch == '"':
            return pos + 1, "".join(buf)
        buf.append(ch)
        pos += 1
    raise ValueError("unterminated RAPID string")


def _parse_scalar(s: str, pos: int) -> tuple[int, Any]:
    end = pos
    while end < len(s) and s[end] not in ",]":
        end += 1
    raw = s[pos:end].strip()
    if not raw:
        raise ValueError(f"empty scalar at pos {pos}")
    upper = raw.upper()
    if upper == "TRUE":  return end, True
    if upper == "FALSE": return end, False
    try:
        if "." in raw or "e" in raw.lower():
            return end, float(raw)
        return end, int(raw)
    except ValueError as e:
        raise ValueError(f"bad RAPID scalar {raw!r}: {e}") from e


def _skip_ws(s: str, pos: int) -> int:
    while pos < len(s) and s[pos] in " \t\r\n":
        pos += 1
    return pos


def _extract_rapid_value(obj: dict) -> Optional[str]:
    """Pull the `value` field out of an RWS RAPID symbol response.

    OmniCore returns:
        {"_links": …, "status": …, "state": [{"_type": "rap-data", "value": "…"}]}
    Older shapes have surfaced under `_embedded._state[0].value` and
    `_embedded.resources[0].value`; check all three.
    """
    # Top-level `state` (current OmniCore shape).
    state = obj.get("state")
    if isinstance(state, list) and state and isinstance(state[0], dict):
        v = state[0].get("value")
        if v is not None:
            return str(v)

    embedded = obj.get("_embedded") or {}
    for key in ("_state", "state", "resources"):
        block = embedded.get(key)
        if isinstance(block, list) and block and isinstance(block[0], dict):
            v = block[0].get("value")
            if v is not None:
                return str(v)
    if "value" in obj:
        return str(obj["value"])

    log.warning(
        "RAPID value not found; response keys=%s embedded keys=%s",
        list(obj.keys()),
        list(embedded.keys()) if isinstance(embedded, dict) else None,
    )
    return None
