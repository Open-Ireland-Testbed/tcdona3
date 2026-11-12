#!/usr/bin/env python3
"""gNMI API for NEC NOS coherent line transceivers.

The defaults use the OpenConfig terminal-device and platform data models.
Confirm the supported paths with ``gnmic -a HOST:57400 ... capabilities`` and
override ``GNMIPaths`` when the NEC NOS model differs from these defaults.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Protocol


class OperationalMode(Enum):
    MODE_200G_QPSK = 2304
    MODE_400G_16QAM = 4308

    @property
    def rate(self) -> str:
        return {2304: "200G", 4308: "400G"}.get(self.value, "Unknown")

    @property
    def modulation(self) -> str:
        return {2304: "DP-QPSK", 4308: "DP-16QAM"}.get(self.value, "Unknown")

    def __str__(self) -> str:
        return f"{self.rate} {self.modulation} (O-FEC)"


@dataclass(frozen=True)
class LineEndpoints:
    """Names and keys used by the device's OpenConfig data model."""

    line_port: str
    line_opt: str
    line_transceiver: str
    logical_channel: Optional[str] = "111040101"
    physical_channel: int = 0

    @classmethod
    def default(
        cls, lc: int = 1, port: int = 1, logical_ch: str = "111040101"
    ) -> "LineEndpoints":
        return cls(
            line_port=f"linecard-{lc}-line-port-{port}",
            line_opt=f"linecard-{lc}-line-opt-{port}-1",
            line_transceiver=f"linecard-{lc}-line-transceiver-{port}",
            logical_channel=logical_ch,
        )


@dataclass(frozen=True)
class GNMIPaths:
    """gNMI paths for a line interface.

    NEC NOS releases can advertise vendor-specific path layouts. Supply this
    object with paths from ``gnmic capabilities`` if they differ from the
    OpenConfig defaults.
    """

    line_port: str
    line_opt: str
    line_transceiver: str
    logical_channel: Optional[str]

    @classmethod
    def openconfig(cls, ep: LineEndpoints) -> "GNMIPaths":
        component = "/components/component[name={}]"
        logical = (
            "/terminal-device/logical-channels/channel[index={}]".format(
                ep.logical_channel
            )
            if ep.logical_channel
            else None
        )
        return cls(
            line_port=component.format(ep.line_port),
            line_opt=component.format(ep.line_opt) + "/optical-channel",
            line_transceiver=(
                component.format(ep.line_transceiver)
                + "/transceiver/physical-channels/channel[index={}]".format(
                    ep.physical_channel
                )
            ),
            logical_channel=logical,
        )


@dataclass(frozen=True)
class LineConfig:
    operational_mode: int
    frequency_mhz: int
    target_output_power_dbm: float

    @classmethod
    def mode_200g(
        cls, freq_thz: float = 193.5, power_dbm: float = -2.0
    ) -> "LineConfig":
        return cls(
            OperationalMode.MODE_200G_QPSK.value, int(freq_thz * 1_000_000), power_dbm
        )

    @classmethod
    def mode_400g(
        cls, freq_thz: float = 193.5, power_dbm: float = -2.0
    ) -> "LineConfig":
        return cls(
            OperationalMode.MODE_400G_16QAM.value, int(freq_thz * 1_000_000), power_dbm
        )

    @property
    def frequency_thz(self) -> float:
        return self.frequency_mhz / 1_000_000

    @property
    def mode_info(self) -> str:
        try:
            return str(OperationalMode(self.operational_mode))
        except ValueError:
            return f"Unknown mode ({self.operational_mode})"


@dataclass(frozen=True)
class TransceiverStatus:
    timestamp: float
    is_up: bool
    operational_mode: Optional[int]
    mode_info: str
    frequency_thz: Optional[float]
    admin_enabled: bool
    oper_status: str
    tx_laser_on: bool
    output_power_dbm: Optional[float]
    input_power_dbm: Optional[float]
    osnr_db: Optional[float]
    pre_fec_ber: Optional[float]
    post_fec_ber: Optional[float]
    cd_ps_nm: Optional[float]
    pdl_db: Optional[float]
    cfo_mhz: Optional[float]
    prbs_enabled: bool
    prbs_ber: Optional[float]

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


class GNMIClient(Protocol):
    def get(self, *, path: list[str], encoding: str) -> dict[str, Any]: ...

    def set(
        self, *, update: list[tuple[str, Any]], encoding: str
    ) -> dict[str, Any]: ...


class GNMISession:
    """Managed TLS gNMI connection using pygnmi.

    Set ``insecure=True`` only for lab devices using insecure gNMI transport.
    pygnmi is imported on connection so this module remains importable for
    static analysis and tests without a live gNMI installation.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 57400,
        insecure: bool = False,
        timeout: float = 10.0,
    ) -> None:
        self.host, self.username, self.password = host, username, password
        self.port, self.insecure, self.timeout = port, insecure, timeout
        self._context: Any = None
        self._client: Optional[GNMIClient] = None

    @property
    def connected(self) -> bool:
        return self._client is not None

    def connect(self) -> "GNMISession":
        if self._client is None:
            from pygnmi.client import gNMIclient

            self._context = gNMIclient(
                target=(self.host, self.port),
                username=self.username,
                password=self.password,
                insecure=self.insecure,
                gnmi_timeout=self.timeout,
            )
            self._client = self._context.__enter__()
        return self

    def close(self) -> None:
        if self._context is not None:
            self._context.__exit__(None, None, None)
        self._context, self._client = None, None

    def get(self, paths: list[str]) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("gNMI is not connected. Call connect() first.")
        return self._client.get(path=paths, encoding="json_ietf")

    def update(self, path: str, value: Any) -> None:
        if self._client is None:
            raise RuntimeError("gNMI is not connected. Call connect() first.")
        self._client.set(update=[(path, value)], encoding="json_ietf")

    def __enter__(self) -> "GNMISession":
        return self.connect()

    def __exit__(self, *args: Any) -> None:
        self.close()


class NECLineGNMIAPI:
    """Control an NEC coherent line transceiver through gNMI."""

    def __init__(
        self,
        gnmi: GNMISession,
        endpoints: Optional[LineEndpoints] = None,
        paths: Optional[GNMIPaths] = None,
    ) -> None:
        self.gnmi = gnmi
        self.ep = endpoints or LineEndpoints.default()
        self.paths = paths or GNMIPaths.openconfig(self.ep)

    def set_operational_mode(self, mode: int) -> None:
        self._update(self.paths.line_opt, "config/operational-mode", mode)

    def set_mode_200g(self) -> None:
        self.set_operational_mode(OperationalMode.MODE_200G_QPSK.value)

    def set_mode_400g(self) -> None:
        self.set_operational_mode(OperationalMode.MODE_400G_16QAM.value)

    def set_frequency_mhz(self, frequency_mhz: int) -> None:
        self._update(self.paths.line_opt, "config/frequency", frequency_mhz)

    def set_frequency_thz(self, frequency_thz: float) -> None:
        self.set_frequency_mhz(int(frequency_thz * 1_000_000))

    def set_power_dbm(self, power_dbm: float) -> None:
        self._update(self.paths.line_opt, "config/target-output-power", power_dbm)

    def laser_on(self) -> None:
        self._update(self.paths.line_transceiver, "config/tx-laser", True)

    def laser_off(self) -> None:
        self._update(self.paths.line_transceiver, "config/tx-laser", False)

    def set_admin_enabled(self, enabled: bool = True) -> None:
        self._update(
            self.paths.line_port,
            "config/admin-state",
            "ENABLED" if enabled else "DISABLED",
        )

    def prbs_on(self) -> None:
        self._set_prbs(True)

    def prbs_off(self) -> None:
        self._set_prbs(False)

    def configure(
        self,
        config: LineConfig,
        laser_on: bool = True,
        wait_for_lock: bool = True,
        timeout_s: float = 30.0,
    ) -> TransceiverStatus:
        if self.paths.logical_channel:
            self.prbs_off()
        self.laser_off()
        self.set_operational_mode(config.operational_mode)
        self.set_frequency_mhz(config.frequency_mhz)
        self.set_power_dbm(config.target_output_power_dbm)
        self.set_admin_enabled()
        if laser_on:
            self.laser_on()
        return (
            self._wait_for_lock(timeout_s)
            if laser_on and wait_for_lock
            else self.get_status()
        )

    def get_status(self) -> TransceiverStatus:
        roots = [self.paths.line_port, self.paths.line_opt, self.paths.line_transceiver]
        if self.paths.logical_channel:
            roots.append(self.paths.logical_channel)
        values = _flatten(self.gnmi.get(roots))
        mode = _as_int(values.get("operational-mode"))
        frequency_mhz = _as_int(values.get("frequency"))
        output_power, input_power = _as_float(values.get("output-power")), _as_float(
            values.get("input-power")
        )
        oper_status = str(values.get("oper-status", "UNKNOWN"))
        admin_enabled = _enum(values.get("admin-state")) == "ENABLED"
        tx_laser_on = _as_bool(values.get("tx-laser"))
        active = (
            _enum(oper_status) == "ACTIVE" or _enum(values.get("link-state")) == "UP"
        )
        try:
            mode_info = str(OperationalMode(mode)) if mode is not None else "Unknown"
        except ValueError:
            mode_info = f"Mode {mode}"
        return TransceiverStatus(
            timestamp=time.time(),
            is_up=active
            and (output_power is None or output_power > -50)
            and (input_power is None or input_power > -30),
            operational_mode=mode,
            mode_info=mode_info,
            frequency_thz=(
                frequency_mhz / 1_000_000 if frequency_mhz is not None else None
            ),
            admin_enabled=admin_enabled,
            oper_status=oper_status,
            tx_laser_on=tx_laser_on,
            output_power_dbm=output_power,
            input_power_dbm=input_power,
            osnr_db=_as_float(values.get("osnr")),
            pre_fec_ber=_as_float(values.get("pre-fec-ber")),
            post_fec_ber=_as_float(values.get("post-fec-ber")),
            cd_ps_nm=_as_float(values.get("chromatic-dispersion/instant")),
            pdl_db=_as_float(values.get("polarization-dependent-loss/instant")),
            cfo_mhz=_as_float(values.get("carrier-frequency-offset/instant")),
            prbs_enabled=_as_bool(values.get("test-signal")),
            prbs_ber=_as_float(values.get("bit-error-rate")),
        )

    def _set_prbs(self, enabled: bool) -> None:
        if not self.paths.logical_channel:
            raise ValueError("logical_channel is not configured")
        self._update(self.paths.logical_channel, "config/test-signal", enabled)

    def _update(self, root: str, leaf: str, value: Any) -> None:
        self.gnmi.update(f"{root}/{leaf}", value)

    def _wait_for_lock(self, timeout_s: float) -> TransceiverStatus:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            status = self.get_status()
            if status.is_up:
                return status
            time.sleep(2)
        return self.get_status()


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten JSON-IETF Get output while retaining nested leaf paths."""
    result: dict[str, Any] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            name = f"{prefix}/{key}" if prefix else key
            result.update(_flatten(child, name))
    elif isinstance(value, list):
        for child in value:
            result.update(_flatten(child, prefix))
    else:
        result[prefix] = value
        result.setdefault(prefix.rsplit("/", 1)[-1], value)
    return result


def _enum(value: Any) -> str:
    return str(value or "").rsplit(":", 1)[-1].upper()


def _as_bool(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# Keep the principal API name stable for callers of nec_nos_api.py.
NECLineAPI = NECLineGNMIAPI
