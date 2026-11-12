#!/usr/bin/env python3
"""Read-only gNMI monitoring API for Galileo Terra Flex.

This module deliberately exposes gNMI Get operations only. It contains no
Set, update, replace, delete, or subscribe operation and cannot write device
configuration.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Optional


class ReadOnlyGNMISession:
    """A gNMI session restricted to Get requests.

    Galileo Terra Flex advertises JSON encoding and normally uses insecure
    gNMI on port 51052 in the lab environment.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 51052,
        insecure: bool = True,
        timeout: float = 10.0,
    ) -> None:
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.insecure = insecure
        self.timeout = timeout
        self._connected = False

    @classmethod
    def from_environment(cls) -> "ReadOnlyGNMISession":
        """Build a session from GALILEO, GNMI_PORT, GNMI_USER, and GNMI_PASS."""
        required = ("GALILEO", "GNMI_USER", "GNMI_PASS")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise RuntimeError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        return cls(
            host=os.environ["GALILEO"],
            port=int(os.environ.get("GNMI_PORT", "51052")),
            username=os.environ["GNMI_USER"],
            password=os.environ["GNMI_PASS"],
        )

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self) -> "ReadOnlyGNMISession":
        self._connected = True
        return self

    def close(self) -> None:
        self._connected = False

    def get(self, paths: list[str], datatype: str = "STATE") -> list[dict[str, Any]]:
        """Read state or configuration data through a gNMI Get request."""
        if not self._connected:
            raise RuntimeError("gNMI is not connected. Call connect() first.")
        request_type = datatype.lower()
        if request_type not in {"all", "config", "state", "operational"}:
            raise ValueError(f"Unsupported gNMI Get data type: {datatype}")
        command = [
            "gnmic",
            "-a",
            f"{self.host}:{self.port}",
            "-u",
            self.username,
            "-p",
            self.password,
        ]
        if self.insecure:
            command.append("--insecure")
        command.extend(["--encoding", "json", "get"])
        for path in paths:
            command.extend(["--path", path])
        command.extend(["--type", request_type])
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=self.timeout,
        )
        return json.loads(completed.stdout)

    def __enter__(self) -> "ReadOnlyGNMISession":
        return self.connect()

    def __exit__(self, *args: Any) -> None:
        self.close()


@dataclass(frozen=True)
class TerraFlexLineStatus:
    timestamp: float
    name: str
    oper_status: str
    operational_mode: Optional[int]
    frequency_mhz: Optional[int]
    target_output_power_dbm: Optional[float]
    output_power_dbm: Optional[float]
    input_power_dbm: Optional[float]
    osnr_db: Optional[float]
    pre_fec_ber: Optional[float]
    post_fec_ber: Optional[float]
    chromatic_dispersion_ps_nm: Optional[float]
    polarization_dependent_loss_db: Optional[float]
    carrier_frequency_offset_mhz: Optional[float]
    laser_shutdown: Optional[bool]
    logical_channel_link_state: Optional[str]

    @property
    def frequency_thz(self) -> Optional[float]:
        return (
            self.frequency_mhz / 1_000_000 if self.frequency_mhz is not None else None
        )

    @property
    def is_up(self) -> bool:
        return (
            _enum(self.oper_status) == "ACTIVE"
            or _enum(self.logical_channel_link_state) == "UP"
        )


@dataclass(frozen=True)
class TerraFlexComponent:
    """Platform inventory summary for one Terra Flex component."""

    name: str
    component_type: Optional[str]
    oper_status: Optional[str]
    parent: Optional[str]
    location: Optional[str]
    description: Optional[str]


@dataclass(frozen=True)
class TerraFlexLogicalChannelStatus:
    """Operational summary for one terminal-device logical channel."""

    index: int
    name: Optional[str]
    admin_state: Optional[str]
    link_state: Optional[str]
    logical_channel_type: Optional[str]
    rate_class: Optional[str]
    tributary_protocol: Optional[str]
    test_signal_enabled: Optional[bool]
    pre_fec_ber: Optional[float]
    post_fec_ber: Optional[float]
    esnr_db: Optional[float]
    q_value_db: Optional[float]

    @property
    def is_up(self) -> bool:
        return _enum(self.link_state) == "UP"


class GalileoTerraFlexAPI:
    """Read-only API for Galileo Terra Flex platform and line monitoring."""

    COMPONENT_ROOT = "/openconfig-platform:components/component[name={name}]"
    LOGICAL_CHANNEL_ROOT = (
        "/openconfig-terminal-device:terminal-device/logical-channels/"
        "channel[index={index}]"
    )
    ALARMS_ROOT = "/openconfig-alarms:alarms"
    COMPONENTS_ROOT = "/openconfig-platform:components"
    LOGICAL_CHANNELS_ROOT = (
        "/openconfig-terminal-device:terminal-device/logical-channels"
    )
    SYSTEM_ROOT = "/openconfig-system:system"

    def __init__(self, gnmi: ReadOnlyGNMISession) -> None:
        self.gnmi = gnmi

    def get_component(self, name: str, datatype: str = "STATE") -> dict[str, Any]:
        """Read one platform component, such as ``linecard-1-line-opt-1-1``."""
        return self.gnmi.get([self.COMPONENT_ROOT.format(name=name)], datatype)

    def get_system(self, datatype: str = "STATE") -> dict[str, Any]:
        """Read the OpenConfig system tree."""
        return self.gnmi.get([self.SYSTEM_ROOT], datatype)

    def list_components(self) -> list[TerraFlexComponent]:
        """Read platform inventory and return summaries for all components."""
        response = self.gnmi.get([self.COMPONENTS_ROOT])
        return [
            _component_from_data(component)
            for component in _objects(response, "component")
        ]

    def list_optical_channels(self) -> list[TerraFlexLineStatus]:
        """Read every optical-channel component and its current QoT metrics."""
        response = self.gnmi.get([self.COMPONENTS_ROOT])
        return [
            _line_status_from_component(component)
            for component in _objects(response, "component")
            if _enum(_value(_flatten(component), "type")) == "OPTICAL_CHANNEL"
        ]

    def get_logical_channel(
        self, index: int, datatype: str = "STATE"
    ) -> dict[str, Any]:
        """Read one terminal-device logical channel."""
        return self.gnmi.get([self.LOGICAL_CHANNEL_ROOT.format(index=index)], datatype)

    def list_logical_channels(self) -> list[TerraFlexLogicalChannelStatus]:
        """Read operational summaries for all terminal-device logical channels."""
        response = self.gnmi.get([self.LOGICAL_CHANNELS_ROOT])
        return [
            _logical_channel_from_data(channel)
            for channel in _objects(response, "channel")
            if _as_int(_value(_flatten(channel), "index")) is not None
        ]

    def get_alarms(self) -> dict[str, Any]:
        """Read the OpenConfig alarm tree."""
        return self.gnmi.get([self.ALARMS_ROOT])

    def get_line_status(
        self, optical_channel: str, logical_channel_index: Optional[int] = None
    ) -> TerraFlexLineStatus:
        """Read coherent-line configuration, optical metrics, and link state."""
        paths = [self.COMPONENT_ROOT.format(name=optical_channel)]
        if logical_channel_index is not None:
            paths.append(self.LOGICAL_CHANNEL_ROOT.format(index=logical_channel_index))
        values = _flatten(self.gnmi.get(paths))
        return _line_status_from_values(values, optical_channel)

    def poll_line_status(
        self,
        optical_channel: str,
        logical_channel_index: Optional[int] = None,
        interval_s: float = 2.0,
    ):
        """Yield periodic line snapshots using successive gNMI Get requests."""
        if interval_s <= 0:
            raise ValueError("interval_s must be greater than zero")
        while True:
            yield self.get_line_status(optical_channel, logical_channel_index)
            time.sleep(interval_s)


def _line_status_from_component(component: dict[str, Any]) -> TerraFlexLineStatus:
    values = _flatten(component)
    name = str(_value(values, "name", "UNKNOWN"))
    return _line_status_from_values(values, name)


def _line_status_from_values(
    values: dict[str, Any], optical_channel: str
) -> TerraFlexLineStatus:
    return TerraFlexLineStatus(
        timestamp=time.time(),
        name=optical_channel,
        oper_status=str(_value(values, "oper-status", "UNKNOWN")),
        operational_mode=_as_int(_value(values, "operational-mode")),
        frequency_mhz=_as_int(_value(values, "frequency")),
        target_output_power_dbm=_as_float(_value(values, "target-output-power")),
        output_power_dbm=_as_float(_value(values, "output-power/instant")),
        input_power_dbm=_as_float(_value(values, "input-power/instant")),
        osnr_db=_as_float(_value(values, "osnr/instant")),
        pre_fec_ber=_as_float(_value(values, "pre-fec-ber/instant")),
        post_fec_ber=_as_float(_value(values, "post-fec-ber/instant")),
        chromatic_dispersion_ps_nm=_as_float(
            _value(values, "chromatic-dispersion/instant")
        ),
        polarization_dependent_loss_db=_as_float(
            _value(values, "polarization-dependent-loss/instant")
        ),
        carrier_frequency_offset_mhz=_as_float(
            _value(values, "carrier-frequency-offset/instant")
        ),
        laser_shutdown=_as_optional_bool(_value(values, "laser-shutdown")),
        logical_channel_link_state=_optional_string(_value(values, "link-state")),
    )


def _component_from_data(component: dict[str, Any]) -> TerraFlexComponent:
    values = _flatten(component)
    return TerraFlexComponent(
        name=str(_value(values, "name", "UNKNOWN")),
        component_type=_optional_string(_value(values, "type")),
        oper_status=_optional_string(_value(values, "oper-status")),
        parent=_optional_string(_value(values, "parent")),
        location=_optional_string(_value(values, "location")),
        description=_optional_string(_value(values, "description")),
    )


def _logical_channel_from_data(
    channel: dict[str, Any],
) -> TerraFlexLogicalChannelStatus:
    values = _flatten(channel)
    index = _as_int(_value(values, "index"))
    assert index is not None
    return TerraFlexLogicalChannelStatus(
        index=index,
        name=_optional_string(_value(values, "name")),
        admin_state=_optional_string(_value(values, "admin-state")),
        link_state=_optional_string(_value(values, "link-state")),
        logical_channel_type=_optional_string(_value(values, "logical-channel-type")),
        rate_class=_optional_string(_value(values, "rate-class")),
        tributary_protocol=_optional_string(_value(values, "trib-protocol")),
        test_signal_enabled=_as_optional_bool(_value(values, "test-signal")),
        pre_fec_ber=_as_float(_value(values, "pre-fec-ber/instant")),
        post_fec_ber=_as_float(_value(values, "post-fec-ber/instant")),
        esnr_db=_as_float(_value(values, "esnr/instant")),
        q_value_db=_as_float(_value(values, "q-value/instant")),
    )


def _objects(value: Any, name: str) -> list[dict[str, Any]]:
    """Extract objects from a module-qualified OpenConfig list name."""
    result: list[dict[str, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key.rsplit(":", 1)[-1] == name and isinstance(child, list):
                result.extend(item for item in child if isinstance(item, dict))
            result.extend(_objects(child, name))
    elif isinstance(value, list):
        for child in value:
            result.extend(_objects(child, name))
    return result


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten gNMI JSON output while keeping short and nested leaf keys."""
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


def _value(values: dict[str, Any], suffix: str, default: Any = None) -> Any:
    """Find a leaf by its suffix in either raw or decoded gNMI JSON."""
    if suffix in values:
        return values[suffix]
    for path, value in reversed(list(values.items())):
        if path.endswith(f"/{suffix}"):
            return value
    return default


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


def _as_optional_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    return value is True or str(value).lower() == "true"


def _optional_string(value: Any) -> Optional[str]:
    return str(value) if value is not None else None
