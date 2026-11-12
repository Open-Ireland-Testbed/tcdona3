"""gNMI Set-query API for Galileo Terra Flex."""

from __future__ import annotations

import json
import math
import os
import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class TerraFlexSetQuery:
    """A type-annotated gNMI Set request."""

    path: str
    value_type: str
    value: str
    command: tuple[str, ...]


class GalileoTerraFlexWriteQueries:
    """Build and execute Terra Flex configuration requests."""

    COMPONENT_ROOT = "/openconfig-platform:components/component[name={name}]"
    LOGICAL_CHANNEL_ROOT = (
        "/openconfig-terminal-device:terminal-device/logical-channels/"
        "channel[index={index}]"
    )

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 51052,
        insecure: bool = True,
    ) -> None:
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.insecure = insecure

    @classmethod
    def from_environment(cls) -> "GalileoTerraFlexWriteQueries":
        """Build a query generator from GALILEO, GNMI_PORT, GNMI_USER, and GNMI_PASS."""
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

    def set_operational_mode(
        self, optical_channel: str, operational_mode: int
    ) -> TerraFlexSetQuery:
        """Build a query to set the optical channel operational mode."""
        _positive("operational_mode", operational_mode)
        return self._update(
            self._optical_channel_path(optical_channel, "operational-mode"),
            "uint",
            operational_mode,
        )

    def set_frequency_mhz(
        self, optical_channel: str, frequency_mhz: int
    ) -> TerraFlexSetQuery:
        """Build a query to set laser frequency in MHz."""
        _positive("frequency_mhz", frequency_mhz)
        return self._update(
            self._optical_channel_path(optical_channel, "frequency"),
            "uint",
            frequency_mhz,
        )

    def set_power_dbm(
        self, optical_channel: str, power_dbm: float
    ) -> TerraFlexSetQuery:
        """Build a query to set target optical output power in dBm."""
        if not math.isfinite(power_dbm):
            raise ValueError("power_dbm must be finite")
        return self._update(
            self._optical_channel_path(optical_channel, "target-output-power"),
            "decimal",
            power_dbm,
        )

    def set_admin_enabled(
        self, line_port: str, enabled: bool = True
    ) -> TerraFlexSetQuery:
        """Build a query to set a client or line optical-port admin state."""
        path = (
            self.COMPONENT_ROOT.format(name=line_port)
            + "/port/openconfig-transport-line-common:optical-port/config/admin-state"
        )
        return self._update(path, "string", "ENABLED" if enabled else "DISABLED")

    def set_tx_laser(
        self, transceiver: str, enabled: bool, physical_channel: int = 1
    ) -> TerraFlexSetQuery:
        """Build a query to enable or disable a physical-channel transmit laser."""
        _positive("physical_channel", physical_channel)
        path = (
            self.COMPONENT_ROOT.format(name=transceiver)
            + "/openconfig-transceiver:transceiver/physical-channels"
            + f"/channel[index={physical_channel}]"
            + "/config/tx-laser"
        )
        return self._update(path, "bool", enabled)

    def set_prbs(self, logical_channel_index: int, enabled: bool) -> TerraFlexSetQuery:
        """Build a query to enable or disable a logical-channel PRBS signal."""
        _positive("logical_channel_index", logical_channel_index)
        path = self.LOGICAL_CHANNEL_ROOT.format(index=logical_channel_index)
        return self._update(path + "/config/test-signal", "bool", enabled)

    def configure_line(
        self,
        *,
        line_port: str,
        optical_channel: str,
        transceiver: str,
        operational_mode: int,
        frequency_mhz: int,
        power_dbm: float,
        physical_channel: int = 1,
        enable_laser: bool = True,
    ) -> tuple[TerraFlexSetQuery, ...]:
        """Build the recommended ordering of queries for a complete line configuration.

        The returned queries are not run until passed to :meth:`execute`.
        """
        queries = [
            self.set_tx_laser(transceiver, False, physical_channel),
            self.set_operational_mode(optical_channel, operational_mode),
            self.set_frequency_mhz(optical_channel, frequency_mhz),
            self.set_power_dbm(optical_channel, power_dbm),
            self.set_admin_enabled(line_port),
        ]
        if enable_laser:
            queries.append(self.set_tx_laser(transceiver, True, physical_channel))
        return tuple(queries)

    def execute(self, query: TerraFlexSetQuery) -> str:
        """Send a Set query to the configured device and return its output."""
        completed = subprocess.run(
            query.command,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout

    def _optical_channel_path(self, optical_channel: str, leaf: str) -> str:
        return (
            self.COMPONENT_ROOT.format(name=optical_channel)
            + f"/openconfig-terminal-device:optical-channel/config/{leaf}"
        )

    def _update(self, path: str, value_type: str, value: object) -> TerraFlexSetQuery:
        string_value = str(value).lower() if isinstance(value, bool) else str(value)
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
        command.extend(
            [
                "--encoding",
                "json",
                "set",
                "--update",
                # --encoding json alone does not select the TypedValue encoding.
                # NEC NOS rejects string_val/uint_val/bool_val; use json_val.
                f"{path}:::json:::{json.dumps(value)}",
            ]
        )
        return TerraFlexSetQuery(path, value_type, string_value, tuple(command))


def _positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
