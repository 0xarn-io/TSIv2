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
        timeout: float | None = None,
    ) -> Optional[dict]:
        """JSON GET. Returns parsed dict on 200, else None (logged unless silent)."""
        try:
            r = self._session.get(
                self._url(path), params=params,
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
            r = self._session.post(
                self._url(path), params=params, data=data,
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
    def _rapid_path(task: str, module: str, symbol: str) -> str:
        return f"/rw/rapid/symbol/data/RAPID/{task}/{module}/{symbol}"

    def read_rapid(
        self, task: str, module: str, symbol: str,
    ) -> Optional[str]:
        """Read a RAPID symbol's raw value string. Returns None on failure."""
        obj = self.get(self._rapid_path(task, module, symbol))
        if obj is None:
            return None
        return _extract_rapid_value(obj)

    def write_rapid(
        self, task: str, module: str, symbol: str, value: str,
    ) -> bool:
        """Write a RAPID symbol value. Acquires + releases mastership.

        `value` is the RAPID-formatted string (e.g. "42", "TRUE",
        "\"hello\""). Returns True on a successful write.
        """
        if not self._mastership("request"):
            return False
        try:
            r = self.post(
                self._rapid_path(task, module, symbol),
                params={"action": "set"},
                data={"value": value},
            )
            return bool(r is not None and r.ok)
        finally:
            self._mastership("release", silent=True)

    # ---- mastership helpers ------------------------------------------------

    def _mastership(self, action: str, *, silent: bool = False) -> bool:
        r = self.post(
            "/rw/mastership/edit", params={"action": action}, silent=silent,
        )
        return bool(r is not None and r.ok)


# ---- module helpers --------------------------------------------------------

def _extract_rapid_value(obj: dict) -> Optional[str]:
    """Pull the `value` field out of an RWS RAPID symbol response.

    RWS shapes vary across firmware versions; check several common ones.
    """
    # HAL+JSON shape: _embedded._state[0].value
    state = (
        obj.get("_embedded", {}).get("_state")
        or obj.get("_embedded", {}).get("state")
    )
    if isinstance(state, list) and state and isinstance(state[0], dict):
        v = state[0].get("value")
        if v is not None:
            return str(v)
    # Flat shape: {"value": "..."}.
    if "value" in obj:
        return str(obj["value"])
    return None
