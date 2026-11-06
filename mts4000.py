"""Viavi T-BERD/MTS 4000 V2 OTDR driver.

This talks to the instrument through Viavi's *official* remote-control command
set: SCPI-style ASCII commands over TCP.  Two sockets are used:

``8000``
    The controller ("ISU") port.  Used to discover the installed modules and
    to ask which port carries the OTDR function.  The OTDR is normally at
    ``pwrside|oppside, slic1``.

``MOD:FUNC:PORT?`` (e.g. ``8002``)
    The "Fiber Optics" (FO) port.  All OTDR setup, acquisition, result-table
    and trace commands go here.  The port is assigned per connection and is
    therefore always re-discovered on :meth:`connect`.

The HTTP endpoint ``http://<host>/info`` is also read (when reachable) for the
asset/model/serial/firmware inventory; it is optional and only used by
:meth:`identify`.

Verified against
----------------
``VIAVI Solutions,MTS 4000 V2,EBAH13125,ISU,23.62``
(OTDR module ``4126 B`` firmware ``FO_Qt,23.62``, SW ``V23.62``).

Quick start
-----------
>>> from tcdona3.mts4000 import MTS4000
>>> with MTS4000("10.10.10.150") as otdr:
...     otdr.configure(wavelength=1550, pulse_width="100ns",
...                    range_km=20, acquisition_time_s=10)
...     result = otdr.acquire(timeout=120, exclusive=True)
...     result["trace"]["loss_db"]      # numpy array, dB
...     result["events"]["events"]      # parsed event table
...     result["sor"]                  # raw Bellcore .SOR bytes

Important operational notes
---------------------------
* **The instrument has one shared screen.**  Selecting the OTDR tab
  (``STATus:TABCurrent SM_Otdr``) changes what a local operator sees, and a
  local operator changing tabs can invalidate a session.  The driver
  re-selects the OTDR tab before configuring/acquiring.
* **Other fiber functions may block acquisition.**  If the OSA, source or
  power meter are active, the platform can refuse an OTDR start.  Pass
  ``exclusive=True`` to :meth:`acquire` (or call :meth:`exclusive_otdr`)
  to switch them off first.
* Commands are case-insensitive; this module uses the canonical shortened
  headers from the Viavi programming manual (``OTDSetup``, ``CURve``,
  ``OTDResult``, ...).
"""

import copy
import csv
import json
import math
import re
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np

DEFAULT_HOST = "10.10.10.150"
CONTROL_PORT = 8000
OTDR_FUNCTION = "OTDR"
OTDR_TAB = "SM_Otdr"
# Default output folder for acquisition results and plots.  ``zresults`` is
# git-ignored, so measurements do not pollute the repository.
DEFAULT_RESULTS_DIR = "zresults/test_mts4000"

# Positions to probe when auto-discovering the module carrying the OTDR.
_POSITIONS = (
    "oppside,slic1",
    "pwrside,slic1",
    "oppside,slic2",
    "pwrside,slic2",
    "oppside,slic3",
    "pwrside,slic3",
    "oppside,slic4",
    "pwrside,slic4",
)

# Wavelength keyword accepted by OTDSetup:LASer.
_WAVELENGTHS = {
    850: "L850",
    1300: "L1300",
    1310: "L1310",
    1470: "L1470",
    1480: "L1480",
    1550: "L1550",
    1625: "L1625",
    1650: "L1650",
}

# Pulse-width keyword accepted by OTDSetup:PULSe, keyed by nanoseconds.
_PULSES = {
    2: "P2NS",
    3: "P3NS",
    5: "P5NS",
    10: "P10NS",
    20: "P20NS",
    30: "P30NS",
    50: "P50NS",
    100: "P100NS",
    200: "P200NS",
    300: "P300NS",
    500: "P500NS",
    1000: "P1US",
    2000: "P2US",
    3000: "P3US",
    10000: "P10US",
    20000: "P20US",
}

_RESOLUTION_MODES = {
    "auto": "AUTO",
    "hdyn": "HDYN",
    "hres": "HRES",
    "manual": "MANU",
    "manu": "MANU",
}

_UNIT_TO_METRES = {
    "m": 1.0,
    "metre": 1.0,
    "meter": 1.0,
    "km": 1000.0,
    "kft": 304.8,
    "kfeet": 304.8,
    "ft": 0.3048,
    "feet": 0.3048,
    "mi": 1609.344,
    "mile": 1609.344,
    "miles": 1609.344,
}

# Canonical column names for the OTDR event table.  The instrument's header
# (``TABle:TITle?``) omits the event-name column, while each data row contains
# it, hence the 9-entry layout below.
_EVENT_COLUMNS = (
    "event",
    "event_name",
    "distance_km",
    "loss_db",
    "reflectance_db",
    "slope_db_per_km",
    "section_loss_db",
    "section_km",
    "total_loss_db",
)


class MTS4000Error(RuntimeError):
    """Raised for transport failures or SCPI command errors."""


class MTS4000TransportError(MTS4000Error):
    """The connection failed, closed or timed out.

    The socket it happened on is closed at once: a reply that arrives late
    would otherwise be read as the answer to the next command.  The driver
    opens a new session on its next command.
    """


# --------------------------------------------------------------------------- #
# value normalisation helpers
# --------------------------------------------------------------------------- #
def normalise_wavelength(value: Any) -> str:
    """Return the ``OTDSetup:LASer`` keyword for *value*.

    Accepts ``1550``, ``"1550nm"``, ``"L1550"``, ``"ALL"`` and multi-lambda
    forms such as ``"1310/1550"`` or ``"1310_1550"``.
    """
    if value is None:
        raise ValueError("wavelength is required")
    if isinstance(value, str):
        text = value.strip().upper().replace("NM", "").replace(" ", "")
        if text in {"ALL", "AUTO"}:
            return text
        if text.startswith("L") and text[1:].replace("_", "").isdigit():
            return text
        parts = [p for p in re.split(r"[/_,+;]", text) if p]
        if len(parts) > 1:
            keys = [normalise_wavelength(p) for p in parts]
            return "_".join(k[1:] for k in keys).join(("L", ""))
        value = float(text)
    lam = int(round(float(value)))
    if lam not in _WAVELENGTHS:
        allowed = ", ".join(str(k) for k in sorted(_WAVELENGTHS))
        raise ValueError(f"unsupported wavelength {value!r}; expected one of {allowed} nm")
    return _WAVELENGTHS[lam]


def normalise_pulse_width(value: Any) -> str:
    """Return the ``OTDSetup:PULSe`` keyword for *value*.

    Accepts ``200``, ``"200ns"``, ``"0.2us"``, ``"P200NS"``, etc.
    """
    if value is None:
        raise ValueError("pulse_width is required")
    if isinstance(value, str):
        text = value.strip().upper().replace(" ", "")
        # A keyword such as P200NS / P1US is checked against the table like any other value.
        match = re.fullmatch(r"P?(\d+(?:\.\d+)?)(NS|US)?", text)
        if not match:
            raise ValueError(f"cannot parse pulse width {value!r}")
        amount = float(match.group(1))
        unit = match.group(2) or "NS"
        ns = amount * (1000.0 if unit == "US" else 1.0)
    else:
        ns = float(value)
    nearest = min(_PULSES, key=lambda k: abs(k - ns))
    if abs(nearest - ns) > 1e-9 * max(1.0, nearest):
        raise ValueError(
            f"unsupported pulse width {value!r}; expected one of "
            + ", ".join(f"{k}ns" for k in sorted(_PULSES))
        )
    return _PULSES[nearest]


def normalise_resolution(value: Any) -> str:
    """Return the ``OTDSetup:RESolution`` argument for *value*.

    Accepts ``"auto"``/``"hres"``/``"hdyn"``, a number in metres (manual
    mode), or a ``(mode, metres)`` pair.
    """
    if value is None:
        raise ValueError("resolution is required")
    if isinstance(value, (tuple, list)):
        if len(value) != 2:
            raise ValueError("resolution tuple must be (mode, metres)")
        mode, metres = value
        mode_kw = _RESOLUTION_MODES.get(str(mode).strip().lower())
        if mode_kw is None:
            raise ValueError(f"unknown resolution mode {mode!r}")
        return f"{mode_kw},{_resolution_metres(metres, mode_kw):g}"
    if isinstance(value, str):
        text = value.strip()
        if "," in text:
            mode, _, metres = text.partition(",")
            return normalise_resolution((mode, metres))
        lowered = text.lower()
        if lowered in _RESOLUTION_MODES:
            return f"{_RESOLUTION_MODES[lowered]},0"
        try:
            metres = float(text)
        except ValueError as exc:
            raise ValueError(
                f"unknown resolution {value!r}; use auto/hres/hdyn or metres"
            ) from exc
        return f"MANU,{_resolution_metres(metres, 'MANU'):g}"
    return f"MANU,{_resolution_metres(value, 'MANU'):g}"


def _resolution_metres(value: Any, mode: str) -> float:
    metres = _finite(value, "resolution")
    if mode == "MANU" and metres <= 0:
        raise ValueError(f"a manual resolution must be > 0 m, not {value!r}")
    if metres < 0:
        raise ValueError(f"resolution must not be negative, not {value!r}")
    return metres


def _finite(value: Any, name: str) -> float:
    """``value`` as a finite float, or ValueError naming ``name``."""
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number, not {value!r}") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number, not {value!r}")
    return number


def _normalise_acquisition_mode(value: Any) -> str:
    text = str(value).strip().lower()
    if text in {"manual", "man"}:
        return "MANual"
    if text in {"auto", "automatic"}:
        return "AUTO"
    raise ValueError(f"unknown acquisition mode {value!r}; use 'manual' or 'auto'")


def _normalise_units(value: Any) -> str:
    text = str(value).strip().lower()
    mapping = {
        "km": "KM",
        "kilometre": "KM",
        "kilometer": "KM",
        "kft": "KFEet",
        "kfeet": "KFEet",
        "mile": "MILes",
        "miles": "MILes",
        "mi": "MILes",
        "m": "METre",
        "metre": "METre",
        "meter": "METre",
        "ft": "FEEt",
        "feet": "FEEt",
    }
    if text not in mapping:
        raise ValueError(f"unknown units {value!r}")
    return mapping[text]


# --------------------------------------------------------------------------- #
# low-level SCPI transport
# --------------------------------------------------------------------------- #
class _SCPISocket:
    """Line-oriented SCPI socket with IEEE-488.2 definite-length block reads.

    A transport failure (timeout, reset, closed by the peer) closes the socket
    and raises :class:`MTS4000TransportError`; a closed socket is never read
    again, so replies cannot be paired with the wrong command.
    """

    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._buf = b""
        self._after_block = False

    def connect(self) -> "_SCPISocket":
        if self._sock is not None:
            return self
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._sock.settimeout(self.timeout)
        self._buf = b""
        self._after_block = False
        return self

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None
        self._buf = b""
        self._after_block = False

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def _fail(self, message: str, exc: Optional[BaseException] = None) -> None:
        self.close()
        raise MTS4000TransportError(message) from exc

    def _recv(self) -> None:
        if self._sock is None:
            raise MTS4000TransportError(f"{self.host}:{self.port} is not connected")
        try:
            chunk = self._sock.recv(65536)
        except socket.timeout as exc:
            self._fail(f"timeout talking to {self.host}:{self.port}", exc)
        except OSError as exc:
            self._fail(f"connection error on {self.host}:{self.port}: {exc}", exc)
        if not chunk:
            self._fail(f"{self.host}:{self.port} closed the connection")
        self._buf += chunk

    def send(self, command: str) -> None:
        if self._sock is None:
            raise MTS4000TransportError(f"{self.host}:{self.port} is not connected")
        try:
            self._sock.sendall(command.strip().encode("ascii") + b"\r\n")
        except OSError as exc:
            self._fail(f"failed to send {command!r}: {exc}", exc)

    def _drop_block_terminator(self, wait: bool = True) -> None:
        """Drop the CRLF (or LF) some firmware sends after a block.

        TCP may deliver it after the block, even split between CR and LF, so it
        is dropped when the next reply is read rather than only when it came
        with the block.  Firmware that sends nothing after a block is fine: the
        next reply then does not start with CR or LF.
        """
        while self._after_block:
            if self._buf.startswith(b"\r\n"):
                self._buf = self._buf[2:]
            elif self._buf.startswith(b"\n"):
                self._buf = self._buf[1:]
            elif self._buf in (b"", b"\r"):
                if not wait:
                    return
                self._recv()
                continue
            self._after_block = False

    def read_line(self) -> str:
        self._drop_block_terminator()
        while b"\n" not in self._buf:
            self._recv()
        line, self._buf = self._buf.split(b"\n", 1)
        return line.decode("utf-8", errors="replace").strip()

    def read_block(self) -> bytes:
        """Read an IEEE-488.2 ``#<n><length><payload>`` block."""
        self._drop_block_terminator()
        while len(self._buf) < 2:
            self._recv()
        if self._buf[:1] != b"#":
            # Not a block: surface the (probably error) line instead.
            line = self.read_line()
            raise MTS4000Error(f"expected a binary block, got {line!r}")
        if not self._buf[1:2].isdigit():
            self._fail(f"malformed block header {self._buf[:2]!r} from {self.host}:{self.port}")
        ndigits = int(chr(self._buf[1]))
        if ndigits == 0:
            self._fail("indefinite-length SCPI blocks are not supported")
        while len(self._buf) < 2 + ndigits:
            self._recv()
        length = int(self._buf[2 : 2 + ndigits])
        total = 2 + ndigits + length
        while len(self._buf) < total:
            self._recv()
        payload = self._buf[2 + ndigits : total]
        self._buf = self._buf[total:]
        self._after_block = True
        self._drop_block_terminator(wait=False)
        return payload

    def query(self, command: str) -> str:
        self.send(command)
        return self.read_line()

    def query_block(self, command: str) -> bytes:
        self.send(command)
        return self.read_block()


# --------------------------------------------------------------------------- #
# parsing helpers (offline-testable)
# --------------------------------------------------------------------------- #
def decode_curve_block(payload: bytes) -> np.ndarray:
    """Decode ``CURve:BUFfer?`` payload (4 ASCII hex chars per point)."""
    if len(payload) % 4:
        raise MTS4000Error(
            f"curve buffer length {len(payload)} is not a multiple of 4"
        )
    try:
        big_endian = np.frombuffer(bytes.fromhex(payload.decode("ascii")), dtype=">i2")
    except (UnicodeDecodeError, ValueError) as exc:
        raise MTS4000Error(f"curve buffer is not ASCII hex: {exc}") from exc
    return big_endian.astype(np.int16)


def parse_event_table(header: str, lines: Iterable[str]) -> List[Dict[str, Any]]:
    """Parse the OTDR event table into canonical dictionaries.

    The device header omits the event-name column, so a row such as::

        1,First Connector,0.000,0.766,-45.82,,,0.000,0.000

    maps to ``event=1``, ``event_name='First Connector'``,
    ``distance_km=0.0``, ``loss_db=0.766``, ``reflectance_db=-45.82``,
    ``section_km=0.0`` and ``total_loss_db=0.0``.  A defensive 8-field
    fallback is kept for firmware that does not emit the name column.
    """
    header_fields = [h.strip() for h in next(csv.reader([header]))] if header else []
    events: List[Dict[str, Any]] = []
    for line in lines:
        if not line or not line.strip():
            continue
        fields = [f.strip() for f in next(csv.reader([line]))]
        # Some firmware prefixes values with '~' as a display marker.
        fields = [f.lstrip("~").strip() for f in fields]
        row: Dict[str, Any] = {"raw": line.strip()}
        if len(fields) >= 9:
            for idx, name in enumerate(_EVENT_COLUMNS):
                value = fields[idx]
                if idx in (0, 1):
                    row[name] = value or None
                else:
                    row[name] = _maybe_float(value)
            # Header labels (Event, Distance km, ...) are offset by the
            # unlabeled event-name column.
            header_map = {0: 0, **{i: i + 1 for i in range(1, len(header_fields))}}
        else:
            legacy = (
                "event",
                "distance_km",
                "loss_db",
                "reflectance_db",
                "slope_db_per_km",
                "unused",
                "section_km",
                "total_loss_db",
            )
            row["event_name"] = None
            for idx, name in enumerate(legacy):
                value = fields[idx] if idx < len(fields) else ""
                row[name] = value or None if idx == 0 else _maybe_float(value)
            header_map = {i: i for i in range(len(header_fields))}
        for hidx, name in enumerate(header_fields):
            key = name.strip() or f"col{hidx}"
            if key in row or key in _EVENT_COLUMNS or not key:
                continue
            fidx = header_map.get(hidx, hidx)
            value = fields[fidx] if fidx < len(fields) else ""
            row[key] = value or None
        events.append(row)
    return events


def _maybe_float(text: str) -> Optional[float]:
    text = text.strip()
    if text in {"", "-", "~"}:
        return None
    try:
        return float(text.replace(" ", ""))
    except ValueError:
        return text


def _parse_resolution(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    parts = [p.strip() for p in text.split(",")]
    result: Dict[str, Any] = {"mode": parts[0] if parts else None, "raw": text}
    if len(parts) > 1:
        result["value_m"] = _maybe_float(parts[1])
    return result


# --------------------------------------------------------------------------- #
# main driver
# --------------------------------------------------------------------------- #
class OTDRConfig(object):
    """Snapshot of the OTDR acquisition settings.

    A plain class rather than a dataclass, so that the module runs on Python 3.6 (ol02); :meth:`as_dict`
    stands for ``dataclasses.asdict``.
    """

    FIELDS = (
        "acquisition_mode",
        "wavelength",
        "available_lasers",
        "pulse_width",
        "range_km",
        "auto_range",
        "resolution",
        "acquisition_time_s",
        "short_acquisition",
        "index_of_refraction",
        "units",
        "splice_detection",
        "reflectance_detection",
        "end_of_fiber",
    )

    def __init__(
        self,
        acquisition_mode: Optional[str] = None,
        wavelength: Optional[str] = None,
        available_lasers: Optional[List[Dict[str, Any]]] = None,
        pulse_width: Optional[str] = None,
        range_km: Optional[float] = None,
        auto_range: Optional[bool] = None,
        resolution: Optional[Dict[str, Any]] = None,
        acquisition_time_s: Optional[float] = None,
        short_acquisition: Optional[bool] = None,
        index_of_refraction: Optional[Dict[str, float]] = None,
        units: Optional[str] = None,
        splice_detection: Optional[int] = None,
        reflectance_detection: Optional[int] = None,
        end_of_fiber: Optional[int] = None,
    ):
        self.acquisition_mode = acquisition_mode
        self.wavelength = wavelength
        self.available_lasers = list(available_lasers or [])
        self.pulse_width = pulse_width
        self.range_km = range_km
        self.auto_range = auto_range
        self.resolution = resolution
        self.acquisition_time_s = acquisition_time_s
        self.short_acquisition = short_acquisition
        self.index_of_refraction = dict(index_of_refraction or {})
        self.units = units
        self.splice_detection = splice_detection
        self.reflectance_detection = reflectance_detection
        self.end_of_fiber = end_of_fiber

    def as_dict(self) -> Dict[str, Any]:
        """The settings as a dictionary (a deep copy), like ``dataclasses.asdict``."""
        return {name: copy.deepcopy(getattr(self, name)) for name in self.FIELDS}

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OTDRConfig):
            return NotImplemented
        return self.as_dict() == other.as_dict()

    def __ne__(self, other: object) -> bool:
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    __hash__ = None  # mutable, like a dataclass with eq=True

    def __repr__(self) -> str:
        return "OTDRConfig(%s)" % ", ".join(
            "%s=%r" % (name, getattr(self, name)) for name in self.FIELDS
        )


class MTS4000:
    """Configure, acquire and read results from a Viavi MTS/T-BERD OTDR."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        *,
        control_port: int = CONTROL_PORT,
        module_position: Optional[str] = None,
        function: str = OTDR_FUNCTION,
        timeout: float = 10.0,
        settle_s: float = 0.4,
        auto_connect: bool = False,
    ):
        self.host = host
        self.control_port = control_port
        self.module_position = module_position
        self.function = function
        self.timeout = timeout
        self.settle_s = settle_s
        self.data_port: Optional[int] = None
        self.scpi_idn: Optional[str] = None
        self._ctl: Optional[_SCPISocket] = None
        self._data: Optional[_SCPISocket] = None
        if auto_connect:
            self.connect()

    # -- lifecycle ---------------------------------------------------------
    def connect(self) -> "MTS4000":
        """Open the session: control port, OTDR function on, data port, OTDR tab.

        Also called on the next command after a transport failure closed the
        data socket.  When it fails, everything it opened is closed again, so
        the next call starts afresh instead of finding a half-open session.
        """
        if self._data is not None and self._data.connected:
            return self
        try:
            control = self._open_control()
            position = self.module_position or self._discover_position()
            self.module_position = position
            control.send(f'MOD:FUNC:SELECT {position},"{self.function}",ON')
            time.sleep(self.settle_s)
            port_text = control.query(f'MOD:FUNC:PORT? {position},"{self.function}"')
            try:
                self.data_port = int(port_text.strip())
            except ValueError as exc:
                raise MTS4000Error(
                    f"could not resolve {self.function} port (reply {port_text!r})"
                ) from exc
            if self._data is not None:
                self._data.close()
            self._data = _SCPISocket(self.host, self.data_port, self.timeout).connect()
            self._data.send("*REM")
            self.scpi_idn = self._data.query("*IDN?")
            # Reading clears any stale bits left by a previous session/operator.
            self._data.query("*ESR?")
            self.select_otdr_tab()
        except BaseException:
            self.close()
            raise
        return self

    def _open_control(self) -> _SCPISocket:
        """The control socket, opened again (in remote mode) if a failure closed it."""
        if self._ctl is None or not self._ctl.connected:
            self._ctl = _SCPISocket(self.host, self.control_port, self.timeout).connect()
            self._ctl.send("*REM")  # enter remote mode
        return self._ctl

    def _discover_position(self) -> str:
        first_error: Optional[Exception] = None
        for position in _POSITIONS:
            try:
                functions = self._open_control().query(f"MOD:FUNC:LIST?{position}")
            except MTS4000Error as exc:
                first_error = first_error or exc
                continue
            names = [n.strip().strip('"') for n in functions.split(",")]
            if self.function in names:
                return position
        raise MTS4000Error(
            f"no module with function {self.function!r} found on {self.host}"
        ) from first_error

    def close(self) -> None:
        if self._data is not None:
            self._data.close()
            self._data = None
        if self._ctl is not None:
            self._ctl.close()
            self._ctl = None

    def __enter__(self) -> "MTS4000":
        return self.connect()

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- low-level access --------------------------------------------------
    def _require_data(self) -> _SCPISocket:
        if self._data is None or not self._data.connected:
            self.connect()
        assert self._data is not None
        return self._data

    @property
    def data(self) -> _SCPISocket:
        return self._require_data()

    @property
    def control(self) -> _SCPISocket:
        if self._ctl is None and self._data is None:
            self.connect()
        return self._open_control()

    def write(self, command: str, *, check: bool = True) -> None:
        """Send a set command and (optionally) verify the event register."""
        self.data.send(command)
        time.sleep(self.settle_s)
        if check:
            self.check_error(command)

    def query(self, command: str) -> str:
        """Send a query and return its line response."""
        return self.data.query(command)

    def query_block(self, command: str) -> bytes:
        """Send a query that returns an IEEE-488.2 definite-length block."""
        return self.data.query_block(command)

    def _safe_query(self, command: str) -> Optional[str]:
        """Query an optional setting, tolerating unsupported commands.

        An unsupported/empty reply sets the SCPI event-status error bit, so the
        register is consumed here to keep it from being mistaken for a later
        configuration failure.  A query that times out also gives ``None``: its
        socket is closed (see :class:`MTS4000TransportError`) and the next
        command opens a new session, so a late reply is never read as the
        answer to another query.
        """
        try:
            value = self.query(command)
        except MTS4000TransportError:
            return None
        if value == "":
            try:
                self.query("*ESR?")  # clear a possible "undefined header" bit
            except MTS4000TransportError:
                pass
            return None
        return value

    def check_error(self, context: str = "") -> None:
        """Raise if the standard event status register reports an error."""
        esr = int(self.query("*ESR?") or 0)
        if esr:
            detail = self._safe_query("STATus:ERR?") or ""
            hint = ""
            if "other Fiber functions" in detail:
                hint = (
                    " (another function such as OSA/SOURCE/POWERMETER is active; "
                    "call exclusive_otdr() or acquire(exclusive=True))"
                )
            if esr & 0x10:  # execution error
                hint += (
                    " (execution error: the instrument rejected the value; "
                    "check that the wavelength/pulse/range is supported by the "
                    "installed module)"
                )
            raise MTS4000Error(
                f"instrument error after {context!r}: ESR={esr}, {detail}{hint}"
            )

    # -- identity ----------------------------------------------------------
    def identify(self) -> Dict[str, Any]:
        """Return SCPI identity plus the instrument's HTTP inventory."""
        self._require_data()
        idn_parts = [p.strip() for p in (self.scpi_idn or "").split(",")]
        info: Dict[str, Any] = {
            "host": self.host,
            "module_position": self.module_position,
            "control_port": self.control_port,
            "data_port": self.data_port,
            "scpi_idn": self.scpi_idn,
            "vendor": idn_parts[0] if len(idn_parts) > 0 else None,
            "model": idn_parts[1] if len(idn_parts) > 1 else None,
            "serial": idn_parts[2] if len(idn_parts) > 2 else None,
            "firmware": idn_parts[-1] if len(idn_parts) > 3 else None,
        }
        try:
            with urllib.request.urlopen(
                f"http://{self.host}/info", timeout=min(self.timeout, 5.0)
            ) as response:
                info["http_info"] = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, ValueError) as exc:
            info["http_info"] = None
            info["http_info_error"] = str(exc)
        return info

    # -- function / tab management ----------------------------------------
    def select_otdr_tab(self) -> None:
        """Make the OTDR tab the active function on the shared FO port."""
        self._require_data().send(f"STATus:TABCurrent {OTDR_TAB}")
        time.sleep(self.settle_s)
        current = self._safe_query("STATus:TABCurrent?")
        if current and not current.startswith("SM_O"):
            raise MTS4000Error(
                f"could not select {OTDR_TAB} tab (current tab {current!r}); "
                "a local operator may be using the instrument"
            )

    def function_states(self) -> Dict[str, Dict[str, Optional[bool]]]:
        """Return ON/OFF state for the fiber functions on every module."""
        states: Dict[str, Dict[str, Optional[bool]]] = {}
        for position in _POSITIONS:
            try:
                listing = self.control.query(f"MOD:FUNC:LIST?{position}")
            except MTS4000Error:
                continue
            names = [n.strip().strip('"') for n in listing.split(",") if n.strip()]
            if not names:
                continue
            states[position] = {}
            for name in names:
                try:
                    reply = self.control.query(
                        f'MOD:FUNC:SELECT? {position},"{name}"'
                    )
                except MTS4000Error:
                    reply = ""
                states[position][name] = (
                    True if reply.strip().upper() == "ON" else False if reply else None
                )
        return states

    def set_function(
        self, function: str, enabled: bool, *, position: Optional[str] = None
    ) -> None:
        """Switch a fiber function on a module ON/OFF (control port)."""
        position = position or self.module_position
        if position is None:
            raise ValueError("module position is not known")
        self.control.send(
            f'MOD:FUNC:SELECT {position},"{function}",{"ON" if enabled else "OFF"}'
        )
        time.sleep(self.settle_s)

    def exclusive_otdr(
        self,
        *,
        disable: Sequence[str] = ("OSA", "SOURCE", "POWERMETER", "OTDR-Y"),
    ) -> Dict[str, Dict[str, Optional[bool]]]:
        """Turn off conflicting fiber functions and ensure the OTDR is ON.

        This changes the instrument's function state, so use it deliberately.
        Returns the state map after the change.
        """
        states = self.function_states()
        for position, functions in states.items():
            for name, on in functions.items():
                if on and name in disable:
                    self.set_function(name, False, position=position)
        if self.module_position:
            self.set_function(self.function, True, position=self.module_position)
        return self.function_states()

    # -- configuration -----------------------------------------------------
    def _read_lasers(self) -> List[Dict[str, Any]]:
        """Return the laser set advertised by the installed module."""
        lasers: List[Dict[str, Any]] = []
        count = self._safe_query("OTDSetup:LSET:NB?")
        try:
            n = int(count) if count else 0
        except ValueError:
            n = 0
        for i in range(1, n + 1):
            name = self._safe_query(f"OTDSetup:LSET:NAME? {i}")
            wavelength = self._safe_query(f"OTDSetup:LSET:LAMbda? {i}")
            if not name or name.startswith("???"):
                continue
            lasers.append(
                {
                    "name": name.strip(),
                    "wavelength_nm": _maybe_float(wavelength or ""),
                }
            )
        return lasers

    def _supported_lasers(self) -> List[str]:
        """Return the laser keywords advertised by the installed module."""
        return [
            str(laser["name"]).strip().upper()
            for laser in self._read_lasers()
            if laser.get("name")
        ]

    def _validate_wavelength(self, keyword: str) -> None:
        """Reject a wavelength keyword the installed module does not advertise.

        Sending an unsupported ``OTDSetup:LASer`` value only yields a generic
        SCPI execution error (``ESR=16``).  Checking the module's laser set
        first turns that into an actionable message.
        """
        if not keyword or keyword in {"ALL", "AUTO"}:
            return
        supported = self._supported_lasers()
        if not supported:
            return  # module did not report a laser set; let the instrument decide
        supported_digits = {
            name[1:] if name.startswith("L") else name for name in supported
        }
        digits = keyword[1:] if keyword.startswith("L") else keyword
        missing = [
            part for part in digits.split("_") if part and part not in supported_digits
        ]
        if missing:
            raise ValueError(
                f"wavelength {keyword!r} is not available on this module; "
                f"supported lasers: {', '.join(supported)}"
            )

    def get_config(self) -> OTDRConfig:
        """Read the current acquisition configuration."""
        self.select_otdr_tab()
        lasers = self._read_lasers()
        indices: Dict[str, float] = {}
        for laser in lasers:
            name = laser["name"]
            index = self._safe_query(f"OTDSetup:N? {name}")
            if index is not None:
                indices[name] = float(index)
        auto_range = self._safe_query("OTDSetup:RAUto?")
        short_acq = self._safe_query("OTDSetup:SACQuisition?")
        return OTDRConfig(
            acquisition_mode=self._safe_query("OTDSetup:ACQuisition?"),
            wavelength=self._safe_query("OTDSetup:LASer?"),
            available_lasers=lasers,
            pulse_width=self._safe_query("OTDSetup:PULSe?"),
            range_km=_to_float(self._safe_query("OTDSetup:KMRange?")),
            auto_range=None if auto_range is None else auto_range.upper() == "YES",
            resolution=_parse_resolution(self._safe_query("OTDSetup:RESolution?") or ""),
            acquisition_time_s=_to_float(self._safe_query("OTDSetup:MAXTime?")),
            short_acquisition=None if short_acq is None else short_acq.upper() == "YES",
            index_of_refraction=indices,
            units=self._safe_query("OTDSetup:UNITs?"),
            splice_detection=_to_int(self._safe_query("OTDSetup:DSPLice?")),
            reflectance_detection=_to_int(self._safe_query("OTDSetup:DREFlectance?")),
            end_of_fiber=_to_int(self._safe_query("OTDSetup:DFEnd?")),
        )

    def configure(
        self,
        *,
        wavelength: Any = None,
        pulse_width: Any = None,
        range_km: Optional[float] = None,
        auto_range: Optional[bool] = None,
        resolution: Any = None,
        acquisition_mode: Optional[str] = None,
        acquisition_time_s: Optional[Union[float, str]] = None,
        short_acquisition: Optional[bool] = None,
        index_of_refraction: Optional[float] = None,
        splice_detection_db: Optional[float] = None,
        reflectance_detection_db: Optional[float] = None,
        end_of_fiber_db: Optional[float] = None,
        units: Optional[str] = None,
        check: bool = True,
    ) -> OTDRConfig:
        """Apply acquisition settings and return the resulting configuration.

        Parameters
        ----------
        wavelength:
            ``1550``, ``"1550nm"``, ``"L1550"`` or multi-lambda.
        pulse_width:
            ``100`` (ns), ``"100ns"``, ``"0.1us"`` or ``"P100NS"``.
        range_km:
            Manual range in km (implies ``auto_range=False``).
        auto_range:
            ``True``/``False`` for ``OTDSetup:RAUto``.
        resolution:
            ``"auto"``, ``"hres"``, ``"hdyn"``, metres, or ``(mode, metres)``.
        acquisition_mode:
            ``"manual"`` or ``"auto"``.
        acquisition_time_s:
            Averaging time in seconds; ``"auto"`` (-2) or ``"realtime"`` (-1).
        short_acquisition:
            ``OTDSetup:SACQuisition``.
        index_of_refraction:
            Group index; applied to the selected (or current) wavelength.
        splice_detection_db, reflectance_detection_db, end_of_fiber_db:
            Event-detection thresholds.
        units:
            Display units (``km``, ``kft``, ``miles``, ``m``, ``ft``).

        The values the driver can check (keywords, the lasers the module
        advertises, positive and finite numbers) are checked before the first
        setting is written.  A value only the instrument can refuse (a range
        the module does not offer, for example) raises :class:`MTS4000Error`
        after the settings before it were written.
        """
        self.select_otdr_tab()
        if range_km is not None and auto_range is True:
            raise ValueError("range_km and auto_range=True are mutually exclusive")

        # Build (and so validate) every command first; only reads happen here.
        commands: List[str] = []
        if wavelength is not None:
            try:
                target_lambda = normalise_wavelength(wavelength)
            except ValueError as exc:
                supported = self._supported_lasers()
                if supported:
                    raise ValueError(
                        f"{exc}; module supports: {', '.join(supported)}"
                    ) from None
                raise
            self._validate_wavelength(target_lambda)
            commands.append(f"OTDSetup:LASer {target_lambda}")
        else:
            target_lambda = None
        if pulse_width is not None:
            pulse = normalise_pulse_width(pulse_width)
            if target_lambda and target_lambda not in {"ALL", "AUTO"}:
                commands.append(f"OTDSetup:LPULse {target_lambda},{pulse}")
            else:
                commands.append(f"OTDSetup:PULSe {pulse}")
        if range_km is not None:
            km = _finite(range_km, "range_km")
            if km <= 0:
                raise ValueError(f"range_km must be > 0, not {range_km!r}")
            commands.append("OTDSetup:RAUto NO")
            commands.append(f"OTDSetup:KMRange {km:g}")
        elif auto_range is not None:
            commands.append(f"OTDSetup:RAUto {'YES' if auto_range else 'NO'}")
        if resolution is not None:
            commands.append(f"OTDSetup:RESolution {normalise_resolution(resolution)}")
        if acquisition_mode is not None:
            commands.append(
                f"OTDSetup:ACQuisition {_normalise_acquisition_mode(acquisition_mode)}"
            )
        if acquisition_time_s is not None:
            commands.append(
                f"OTDSetup:MAXTime {_normalise_acquisition_time(acquisition_time_s)}"
            )
        if short_acquisition is not None:
            commands.append(
                f"OTDSetup:SACQuisition {'YES' if short_acquisition else 'NO'}"
            )
        if index_of_refraction is not None:
            if not 1.3 <= float(index_of_refraction) <= 1.7:
                raise ValueError("index_of_refraction must be between 1.3 and 1.7")
            lam = (
                target_lambda
                if target_lambda is not None
                else self._safe_query("OTDSetup:LASer?")
            )
            if not lam or lam in {"ALL", "AUTO"}:
                raise ValueError(
                    "select a single wavelength before setting index_of_refraction"
                )
            commands.append(f"OTDSetup:N {lam},{float(index_of_refraction):.5f}")
        if splice_detection_db is not None:
            commands.append(
                "OTDSetup:DSPLice "
                f"{int(round(_finite(splice_detection_db, 'splice_detection_db') * 100))}"
            )
        if reflectance_detection_db is not None:
            commands.append(
                "OTDSetup:DREFlectance "
                f"{int(round(_finite(reflectance_detection_db, 'reflectance_detection_db')))}"
            )
        if end_of_fiber_db is not None:
            commands.append(
                f"OTDSetup:DFEnd {int(round(_finite(end_of_fiber_db, 'end_of_fiber_db')))}"
            )
        if units is not None:
            commands.append(f"OTDSetup:UNITs {_normalise_units(units)}")

        if check:
            self.query("*ESR?")  # discard stale bits from optional queries
        for command in commands:
            self.write(command, check=check)

        return self.get_config()

    # -- acquisition -------------------------------------------------------
    def acquisition_status(self) -> str:
        """Return ``IN_PROGRESS``, ``STOPPED`` or ``READ ONLY``."""
        return self.query("STATus:ACQ?").strip().upper()

    def start_acquisition(self) -> None:
        """Start an averaged acquisition (``KEY BEGinacq``)."""
        self.select_otdr_tab()
        self.write("KEY BEGinacq", check=False)
        time.sleep(0.2)

    def stop_acquisition(self) -> None:
        """Stop a running acquisition (``KEY HALTacq``)."""
        self.write("KEY HALTacq", check=False)

    def wait_for_acquisition(
        self, *, timeout: float = 600.0, poll: float = 1.0, start_grace: float = 5.0
    ) -> str:
        """Block until the acquisition stops.

        ``start_grace`` tolerates the short delay between ``KEY BEGinacq`` and
        the status becoming ``IN_PROGRESS``.
        """
        return self._wait_for_acquisition(timeout, poll, start_grace)[0]

    def _wait_for_acquisition(
        self, timeout: float, poll: float, start_grace: float
    ) -> Tuple[str, bool]:
        """``(last status, whether IN_PROGRESS was seen)`` once the acquisition stops."""
        deadline = time.monotonic() + timeout
        grace_deadline = time.monotonic() + min(start_grace, timeout)
        seen_progress = False
        last = ""
        while time.monotonic() < deadline:
            last = self.acquisition_status()
            if last == "IN_PROGRESS":
                seen_progress = True
            elif last == "STOPPED":
                if seen_progress or time.monotonic() >= grace_deadline:
                    return last, seen_progress
            elif last == "READ ONLY":
                raise MTS4000Error(
                    "acquisition is READ ONLY; the OTDR tab is not the active "
                    "result tab or another function holds the display"
                )
            time.sleep(poll)
        raise TimeoutError(
            f"acquisition did not finish within {timeout}s (last status {last!r})"
        )

    def acquire(
        self,
        *,
        timeout: float = 600.0,
        poll: float = 1.0,
        exclusive: bool = False,
        settle: float = 0.3,
        start_grace: float = 5.0,
    ) -> Dict[str, Any]:
        """Run one acquisition and return :meth:`collect`.

        Raises :class:`MTS4000Error` when the acquisition did not start: the
        status never showed ``IN_PROGRESS`` and the result's date
        (``OTDResult:DATE?``) is still the previous one, so :meth:`collect`
        would return the previous measurement.

        When waiting fails (timeout, lost connection, Ctrl-C), the acquisition
        is stopped (``KEY HALTacq``, on a new session if needed) before the
        error is raised, so the laser is not left firing; a stop that cannot
        be confirmed is reported on stderr.
        """
        if exclusive:
            self.exclusive_otdr()
        previous_date = self._safe_query("OTDResult:DATE?")
        self.start_acquisition()
        try:
            _, started = self._wait_for_acquisition(timeout, poll, start_grace)
        except BaseException:
            self._halt_after_failure()
            raise
        if not started and self._safe_query("OTDResult:DATE?") == previous_date:
            # The instrument's own reason, when it gives one (ESR and STATus:ERR?).
            self.check_error("KEY BEGinacq")
            raise MTS4000Error(
                "the acquisition did not start: the status never showed "
                f"IN_PROGRESS and the result is still the previous one "
                f"(date {previous_date!r})"
            )
        time.sleep(settle)
        return self.collect()

    def _halt_after_failure(self, attempts: int = 5) -> None:
        """Stop the acquisition :meth:`acquire` started, after waiting for it failed.

        Never raises (the caller re-raises the original error): a stop that
        cannot be sent or confirmed is reported on stderr.
        """
        try:
            self.stop_acquisition()
            for _ in range(attempts):
                if self.acquisition_status() != "IN_PROGRESS":
                    return
                time.sleep(1.0)
            problem = "it still reports IN_PROGRESS"
        except Exception as exc:  # noqa: BLE001 - reported; the original error is raised
            problem = f"{type(exc).__name__}: {exc}"
        print(
            f"[MTS4000] the acquisition on {self.host} could not be confirmed "
            f"stopped ({problem}); stop it at the instrument",
            file=sys.stderr,
        )

    # -- results -----------------------------------------------------------
    def get_result_metadata(self) -> Dict[str, Any]:
        """Read the metadata of the current acquisition."""
        return {
            "module": self._safe_query("OTDResult:MODUle?"),
            "date": self._safe_query("OTDResult:DATE?"),
            "pulse": self._safe_query("OTDResult:PULSe?"),
            "range_km": _to_float(self._safe_query("OTDResult:RANGe?")),
            "wavelength": self._safe_query("OTDResult:LASer?"),
            "resolution": self._safe_query("OTDResult:RESO?"),
            "acquisition_time_s": _to_float(self._safe_query("OTDResult:ATIMe?")),
            "index_of_refraction": _to_float(self._safe_query("OTDResult:N?")),
            "backscatter_coefficient": _to_float(
                self._safe_query("OTDResult:K?")
            ),
            "cw_level": _to_float(self._safe_query("OTDResult:CWLevel?")),
            "acquisition_mode": self._safe_query("OTDResult:AQMode?"),
        }

    def get_trace(self) -> Dict[str, Any]:
        """Return the current OTDR trace as raw counts and calibrated arrays.

        ``loss_db = y_scale * raw + y_offset`` (coefficients read from the
        instrument).  ``distance_m`` is derived from ``x_offset``/``x_scale``
        converted from the instrument's native ``x_unit``.
        """
        size = int(self.query("CURve:SIZE?"))
        x_offset = float(self.query("CURve:XOFFset?"))
        x_scale = float(self.query("CURve:XSCale?"))
        y_offset = float(self.query("CURve:YOFFset?"))
        y_scale = float(self.query("CURve:YSCale?"))
        x_unit = self._safe_query("CURve:XUNit?") or "m"
        y_unit = self._safe_query("CURve:YUNit?") or "dB"
        raw = decode_curve_block(self.query_block("CURve:BUFfer?"))
        if raw.size != size:
            raise MTS4000Error(
                f"trace size mismatch: header says {size}, buffer has {raw.size}"
            )
        x = x_offset + x_scale * np.arange(raw.size, dtype=float)
        factor = _UNIT_TO_METRES.get(x_unit.strip().lower(), 1.0)
        return {
            "size": size,
            "raw": raw,
            "loss_db": y_scale * raw.astype(float) + y_offset,
            "x": x,
            "x_unit": x_unit,
            "distance_m": x * factor,
            "y_unit": y_unit,
            "x_offset": x_offset,
            "x_scale": x_scale,
            "y_offset": y_offset,
            "y_scale": y_scale,
        }

    def get_events(self) -> Dict[str, Any]:
        """Return the parsed OTDR event table."""
        self.select_otdr_tab()
        header = self._safe_query("TABle:TITle?") or ""
        summary = self._safe_query("TABle:TOP?") or ""
        size_text = self._safe_query("TABle:SIZe?")
        size = int(size_text) if size_text else 0
        lines: List[str] = []
        # Table row 0 is the active row; real rows start at 1.
        for index in range(1, size + 1):
            line = self._safe_query(f"TABle:LINe? {index}")
            if line:
                lines.append(line)
        return {
            "size": size,
            "header": header,
            "summary": summary,
            "events": parse_event_table(header, lines),
        }

    def get_sor(self) -> bytes:
        """Return the current acquisition as a Bellcore GR-196 ``.SOR`` blob.

        Uses ``OTDResult:SSOR?``, which prefixes the binary payload with a
        7-digit IEEE-488.2 length header (unlike ``OTDResult:SOR?``).
        """
        payload = self.query_block("OTDResult:SSOR?")
        if not payload:
            raise MTS4000Error("instrument returned an empty SOR buffer")
        return payload

    def collect(
        self,
        *,
        include_trace: bool = True,
        include_events: bool = True,
        include_sor: bool = True,
    ) -> Dict[str, Any]:
        """Collect configuration, metadata, trace, events and SOR data."""
        result: Dict[str, Any] = {
            "identity": {
                "host": self.host,
                "position": self.module_position,
                "data_port": self.data_port,
                "scpi_idn": self.scpi_idn,
            },
            "config": self.get_config().as_dict(),
            "metadata": self.get_result_metadata(),
        }
        if include_trace:
            result["trace"] = self.get_trace()
        if include_events:
            result["events"] = self.get_events()
        if include_sor:
            result["sor"] = self.get_sor()
        return result

    def save_result(
        self,
        directory: Union[str, Path] = ".",
        stem: Optional[str] = None,
        *,
        include_trace: bool = True,
        include_events: bool = True,
        include_sor: bool = True,
        result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Path]:
        """Write ``.sor``, ``_trace.csv`` and ``.json`` result files.

        If *result* is supplied (for example the dict returned by
        :meth:`acquire`), that snapshot is saved; otherwise the current
        acquisition is collected first.
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        if result is None:
            result = self.collect(
                include_trace=include_trace,
                include_events=include_events,
                include_sor=include_sor,
            )
        if stem is None:
            stamp = time.strftime("%Y%m%d_%H%M%S")
            lam = (result.get("metadata", {}).get("wavelength") or "otdr").replace(" ", "")
            serial = (result.get("identity", {}).get("scpi_idn") or "").split(",")
            serial = serial[2] if len(serial) > 2 else "unknown"
            stem = f"otdr_{serial}_{lam}_{stamp}"
        stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", stem)
        paths: Dict[str, Path] = {}

        json_payload = {
            "identity": result.get("identity"),
            "config": result.get("config"),
            "metadata": result.get("metadata"),
            "events": result.get("events"),
        }
        if include_trace and "trace" in result:
            trace = result["trace"]
            json_payload["trace_info"] = {
                k: (v.tolist() if k == "raw" else v)
                for k, v in trace.items()
                if k not in {"loss_db", "x", "distance_m", "raw"}
            }
            json_payload["trace_info"]["size"] = trace["size"]
            csv_path = directory / f"{stem}_trace.csv"
            np.savetxt(
                csv_path,
                np.column_stack([trace["distance_m"], trace["loss_db"]]),
                delimiter=",",
                header="distance_m,loss_db",
                comments="",
            )
            paths["trace_csv"] = csv_path
        json_path = directory / f"{stem}.json"
        json_path.write_text(json.dumps(json_payload, indent=2, default=_json_default))
        paths["json"] = json_path
        if include_sor and "sor" in result:
            sor_path = directory / f"{stem}.sor"
            sor_path.write_bytes(result["sor"])
            paths["sor"] = sor_path
        return paths


# --------------------------------------------------------------------------- #
# small converters
# --------------------------------------------------------------------------- #
def _to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _normalise_acquisition_time(value: Union[float, str]) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"auto", "automatic"}:
            return "-2"
        if text in {"realtime", "real-time", "real"}:
            return "-1"
        value = float(text)
    seconds = float(value)
    if seconds in (-2.0, -1.0):
        # What OTDSetup:MAXTime? reports for auto/realtime, so that a value read
        # with get_config() can be written back.
        return str(int(seconds))
    if seconds <= 0:
        raise ValueError(
            "acquisition_time_s must be > 0, 'auto' (-2) or 'realtime' (-1)"
        )
    return str(int(round(seconds)))


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value)!r}")


def connect(host: str = DEFAULT_HOST, **kwargs: Any) -> MTS4000:
    """Convenience constructor: ``connect(host)`` == ``MTS4000(host).connect()``."""
    return MTS4000(host, **kwargs).connect()


# --------------------------------------------------------------------------- #
# public client-side API
# --------------------------------------------------------------------------- #
__all__ = [
    "MTS4000",
    "MTS4000Error",
    "MTS4000TransportError",
    "OTDRConfig",
    "DEFAULT_HOST",
    "CONTROL_PORT",
    "DEFAULT_RESULTS_DIR",
    "connect",
    "decode_curve_block",
    "parse_event_table",
    "normalise_wavelength",
    "normalise_pulse_width",
    "normalise_resolution",
]
