"""
polatis.py – NETCONF client and DB-backed wrapper for H+S Polatis optical switch.

Layers:

  - PolatisNetconf:
      Low-level NETCONF/YANG client (direct port IDs, no DB).

  - Polatis:
      High-level wrapper using MySQL provisioning DB for:
        * name → port mapping (device_table)
        * ownership checks (check_patch_owners)
      All switching done via NETCONF underneath.

DB expectations (same as legacy telnet API):
  - MySQL database 'provdb'
  - Table device_table(polatis_name, In_Port, Out_Port, ...)
  - Table ports_new(Name, Owner, ...)
  - /etc/secure_keys/mysql_key.key for admin user/pass (release_ports)

Usage examples:

  from polatis import PolatisNetconf, Polatis

  # Direct NETCONF (ports only)
  with PolatisNetconf(host="10.10.10.28", username="admin", password="root") as pol:
      print(pol.get_power(229))
      pol.create_cross_connect(229, 405)

  # Name/DB-based API (legacy semantics on top of NETCONF)
  with Polatis() as pol_db:
      pol_db.apply_patch_list([("SRC_DEVICE", "DST_DEVICE")])
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import csv
import logging
import os
import getpass
import time
from datetime import datetime

from ncclient import manager
from lxml import etree
import mysql.connector

from tcdona3.utils import check_patch_owners

logger = logging.getLogger(__name__)

OPSW_NS = "http://www.polatis.com/yang/optical-switch"
OPSW = f"{{{OPSW_NS}}}"  # QName helper, e.g. OPSW + "port" -> "{ns}port"
NC_NS = "urn:ietf:params:xml:ns:netconf:base:1.0"


# ----------------------------------------------------------------------
# Low-level NETCONF client (direct port access, no DB)
# ----------------------------------------------------------------------


class PolatisNetconf:
    """
    NETCONF client for a Polatis optical circuit switch.

    Works directly with numeric port IDs and YANG trees:
      - /opm-power
      - /opm-config
      - /port-config
      - /cross-connects
      - /voa
    """

    def __init__(
        self,
        host: str = "10.10.10.28",
        port: int = 830,
        username: str = "admin",
        password: str = "root",
        timeout: int = 30,
        allow_agent: bool = False,
        look_for_keys: bool = False,
        hostkey_verify: bool = False,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.allow_agent = allow_agent
        self.look_for_keys = look_for_keys
        self.hostkey_verify = hostkey_verify

        self._mgr: Optional[manager.Manager] = None

    # ------------------------------------------------------------------
    # Connection handling
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the NETCONF session if not already connected."""
        if self._mgr is not None and self._mgr.connected:
            return

        logger.info("Connecting NETCONF session to %s:%s", self.host, self.port)
        try:
            self._mgr = manager.connect(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                hostkey_verify=self.hostkey_verify,
                allow_agent=self.allow_agent,
                look_for_keys=self.look_for_keys,
                timeout=self.timeout,
            )
        except Exception:
            logger.exception("Failed to establish NETCONF session")
            raise

        logger.info("NETCONF session established")

    def close(self) -> None:
        """Close the NETCONF session if open."""
        if self._mgr is not None:
            logger.info("Closing NETCONF session")
            try:
                if self._mgr.connected:
                    self._mgr.close_session()
            except Exception:
                logger.exception("Error while closing NETCONF session")
            finally:
                self._mgr = None
                logger.info("NETCONF session closed")

    def __enter__(self) -> "PolatisNetconf":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def mgr(self) -> manager.Manager:
        """Return an active ncclient Manager, ensuring connection."""
        if self._mgr is None or not self._mgr.connected:
            self.connect()
        assert self._mgr is not None
        return self._mgr

    def _subtree_get(self, xml_subtree: str) -> etree._Element:
        """
        Perform a NETCONF <get> with a subtree filter and
        return the <data> element as lxml.
        """
        logger.debug("NETCONF GET filter:\n%s", xml_subtree)
        reply = self.mgr.get(filter=("subtree", xml_subtree))
        data_ele = reply.data_ele
        logger.debug(
            "NETCONF GET response:\n%s",
            etree.tostring(data_ele, pretty_print=True).decode(errors="ignore"),
        )
        return data_ele

    def _edit(self, config: str) -> None:
        """
        Perform a NETCONF <edit-config> on running with the given config subtree.
        """
        logger.info("NETCONF edit-config:\n%s", config)
        try:
            reply = self.mgr.edit_config(target="running", config=config)
            logger.debug("edit-config reply: %s", reply)
        except Exception:
            logger.exception("edit-config failed")
            raise

    # ------------------------------------------------------------------
    # Power monitoring API  (opm-power / opm-config)
    # ------------------------------------------------------------------

    def get_port_power(self, port_id: int) -> float:
        """
        Return measured optical power (dBm) for a single port.

        Uses /opm-power/port[port-id]/power.

        Raises KeyError if the port has no OPM entry.
        """
        logger.info("Retrieving power for port %s", port_id)

        subtree = f"""
            <opm-power xmlns="{OPSW_NS}">
              <port>
                <port-id>{port_id}</port-id>
                <power/>
              </port>
            </opm-power>
        """

        data = self._subtree_get(subtree)

        power_elem = data.find(
            f".//{OPSW}opm-power/{OPSW}port[{OPSW}port-id='{port_id}']/{OPSW}power"
        )
        if power_elem is None or power_elem.text is None:
            logger.warning("No power reading found for port %s", port_id)
            raise KeyError(f"No power reading for port {port_id}")

        power = float(power_elem.text)
        logger.info("Port %s power = %.2f dBm", port_id, power)
        return power

    def get_all_power(self) -> Dict[int, float]:
        """
        Return power readings for all ports with OPM.

        Returns dict[port-id] = power_dBm.
        """
        logger.info("Retrieving power for all ports")

        subtree = f"""
            <opm-power xmlns="{OPSW_NS}">
              <port>
                <port-id/>
                <power/>
              </port>
            </opm-power>
        """

        data = self._subtree_get(subtree)
        result: Dict[int, float] = {}

        for port_elem in data.findall(f".//{OPSW}opm-power/{OPSW}port"):
            pid_elem = port_elem.find(f"{OPSW}port-id")
            pwr_elem = port_elem.find(f"{OPSW}power")
            if pid_elem is None or pid_elem.text is None:
                continue
            if pwr_elem is None or pwr_elem.text is None:
                continue
            try:
                pid = int(pid_elem.text)
                pwr = float(pwr_elem.text)
            except ValueError:
                continue
            result[pid] = pwr

        logger.info("Retrieved power for %d ports", len(result))
        return result

    def get_power(self, port_id: Optional[int] = None):
        """
        Convenience wrapper.

        - If port_id is provided, returns float (single port).
        - If port_id is None, returns dict[port-id] = power.
        """
        if port_id is None:
            return self.get_all_power()
        return self.get_port_power(port_id)

    # ------------------------------------------------------------------
    # Port configuration API (port-config)
    # ------------------------------------------------------------------

    def get_port_status(self, port_id: int) -> Optional[str]:
        """
        Return operational status for a port from /port-config.

        Returns the status string or None if not present.
        """
        logger.info("Retrieving status for port %s", port_id)

        subtree = f"""
            <port-config xmlns="{OPSW_NS}">
              <port>
                <port-id>{port_id}</port-id>
                <status/>
              </port>
            </port-config>
        """

        data = self._subtree_get(subtree)
        status_elem = data.find(
            f".//{OPSW}port-config/{OPSW}port[{OPSW}port-id='{port_id}']/{OPSW}status"
        )
        status = status_elem.text if status_elem is not None else None
        logger.info("Port %s status = %s", port_id, status)
        return status

    # ------------------------------------------------------------------
    # Cross-connects API (cross-connects) – single and batched
    # ------------------------------------------------------------------

    def get_cross_connects(self) -> List[Tuple[int, int]]:
        """
        Return all cross-connect pairs.

        Data comes from /cross-connects/pair[ingress]/egress.
        """
        logger.info("Retrieving all cross-connects")

        subtree = f"""
            <cross-connects xmlns="{OPSW_NS}">
              <pair>
                <ingress/>
                <egress/>
              </pair>
            </cross-connects>
        """

        data = self._subtree_get(subtree)
        pairs: List[Tuple[int, int]] = []

        for pair_elem in data.findall(f".//{OPSW}cross-connects/{OPSW}pair"):
            in_elem = pair_elem.find(f"{OPSW}ingress")
            eg_elem = pair_elem.find(f"{OPSW}egress")
            if in_elem is None or in_elem.text is None:
                continue
            if eg_elem is None or eg_elem.text is None:
                continue
            try:
                ingress = int(in_elem.text)
                egress = int(eg_elem.text)
            except ValueError:
                continue
            pairs.append((ingress, egress))

        logger.info("Retrieved %d cross-connect(s)", len(pairs))
        return pairs

    def create_cross_connects(self, pairs: Sequence[Tuple[int, int]]) -> None:
        """
        Create multiple cross-connects in one NETCONF edit-config.

        pairs: iterable of (ingress, egress) port IDs.
        """
        pairs = list(pairs)
        if not pairs:
            logger.info("create_cross_connects called with empty list; skipping")
            return

        logger.info("Creating %d cross-connect(s): %s", len(pairs), pairs)

        pair_xml = "\n".join(
            f"      <pair><ingress>{ing}</ingress><egress>{eg}</egress></pair>"
            for ing, eg in pairs
        )

        config = f"""
          <nc:config xmlns:nc="{NC_NS}">
            <cross-connects xmlns="{OPSW_NS}">
{pair_xml}
            </cross-connects>
          </nc:config>
        """

        self._edit(config)
        logger.info("Created %d cross-connect(s)", len(pairs))

    def delete_cross_connects(self, ingresses: Sequence[int]) -> None:
        """
        Delete multiple cross-connects by ingress in one NETCONF edit-config.

        ingresses: iterable of ingress port IDs.
        """
        ingresses = list(ingresses)
        if not ingresses:
            logger.info("delete_cross_connects called with empty list; skipping")
            return

        logger.info(
            "Deleting %d cross-connect(s) by ingress: %s",
            len(ingresses),
            ingresses,
        )

        pair_xml = "\n".join(
            f'      <pair nc:operation="delete"><ingress>{ing}</ingress></pair>'
            for ing in ingresses
        )

        config = f"""
          <nc:config xmlns:nc="{NC_NS}">
            <cross-connects xmlns="{OPSW_NS}">
{pair_xml}
            </cross-connects>
          </nc:config>
        """

        self._edit(config)
        logger.info("Deleted %d cross-connect(s)", len(ingresses))

    def create_cross_connect(self, ingress: int, egress: int) -> None:
        """Single cross-connect convenience wrapper."""
        self.create_cross_connects([(ingress, egress)])

    def delete_cross_connect(self, ingress: int) -> None:
        """Single delete convenience wrapper."""
        self.delete_cross_connects([ingress])

    # ------------------------------------------------------------------
    # Cross-connect existence API
    # ------------------------------------------------------------------

    def has_cross_connect(self, ingress: int, egress: Optional[int] = None) -> bool:
        """
        Check if a cross-connect exists.

        If egress is None:
            returns True if any pair with this ingress exists.
        If egress is not None:
            returns True only if a pair with this exact (ingress, egress) exists.
        """
        logger.info(
            "Checking cross-connect existence: ingress=%s%s",
            ingress,
            f", egress={egress}" if egress is not None else "",
        )

        if egress is None:
            subtree = f"""
                <cross-connects xmlns="{OPSW_NS}">
                  <pair>
                    <ingress>{ingress}</ingress>
                    <egress/>
                  </pair>
                </cross-connects>
            """
        else:
            subtree = f"""
                <cross-connects xmlns="{OPSW_NS}">
                  <pair>
                    <ingress>{ingress}</ingress>
                    <egress>{egress}</egress>
                  </pair>
                </cross-connects>
            """

        data = self._subtree_get(subtree)

        pairs = data.findall(f".//{OPSW}cross-connects/{OPSW}pair")
        if not pairs:
            logger.info("Cross-connect does not exist")
            return False

        for pair_elem in pairs:
            in_elem = pair_elem.find(f"{OPSW}ingress")
            eg_elem = pair_elem.find(f"{OPSW}egress")

            if in_elem is None or in_elem.text is None:
                continue

            try:
                in_val = int(in_elem.text)
            except ValueError:
                continue

            if in_val != ingress:
                continue

            if egress is None:
                logger.info("Cross-connect exists for ingress=%s", ingress)
                return True

            if eg_elem is None or eg_elem.text is None:
                continue

            try:
                eg_val = int(eg_elem.text)
            except ValueError:
                continue

            if eg_val == egress:
                logger.info("Cross-connect exists: %s -> %s", ingress, egress)
                return True

        logger.info("Cross-connect does not exist")
        return False

    def connection_exists(self, ingress: int, egress: Optional[int] = None) -> bool:
        """Alias for has_cross_connect()."""
        return self.has_cross_connect(ingress, egress)

    # ------------------------------------------------------------------
    # OPM configuration API (opm-config)
    # ------------------------------------------------------------------

    def set_opm_config(
        self,
        port_id: int,
        wavelength_nm: Optional[float] = None,
        offset_db: Optional[float] = None,
    ) -> None:
        """
        Configure OPM for a port (/opm-config/port).
        """
        if wavelength_nm is None and offset_db is None:
            logger.info("set_opm_config called with no changes; skipping")
            return

        logger.info(
            "Setting OPM config for port %s (wavelength=%s, offset=%s)",
            port_id,
            wavelength_nm,
            offset_db,
        )

        parts = [f'<opm-config xmlns="{OPSW_NS}">', "<port>"]
        parts.append(f"<port-id>{port_id}</port-id>")

        if wavelength_nm is not None:
            parts.append(f"<wavelength>{wavelength_nm:.2f}</wavelength>")
        if offset_db is not None:
            parts.append(f"<offset>{offset_db:.2f}</offset>")

        parts.append("</port>")
        parts.append("</opm-config>")
        opm_cfg = "".join(parts)

        config = f"""
          <nc:config xmlns:nc="{NC_NS}">
            {opm_cfg}
          </nc:config>
        """
        self._edit(config)
        logger.info("OPM config set for port %s", port_id)

    # ------------------------------------------------------------------
    # VOA configuration API (voa)
    # ------------------------------------------------------------------

    def set_voa(
        self,
        port_id: int,
        mode: str,
        atten_level: Optional[float] = None,
        reference_port: Optional[int] = None,
    ) -> None:
        """
        Configure Variable Optical Attenuation (VOA) for a port.

        YANG:
          /voa/port[port-id]/atten-mode
          /voa/port[port-id]/atten-level
          /voa/port[port-id]/reference-port
        """
        valid_modes = {
            "VOA_MODE_NONE",
            "VOA_MODE_RELATIVE",
            "VOA_MODE_ABSOLUTE",
            "VOA_MODE_CONVERGED",
            "VOA_MODE_MAXIMUM",
            "VOA_MODE_FIXED",
        }
        if mode not in valid_modes:
            raise ValueError(f"Invalid VOA mode {mode!r}")

        if mode in {"VOA_MODE_ABSOLUTE", "VOA_MODE_RELATIVE", "VOA_MODE_CONVERGED"}:
            if atten_level is None:
                raise ValueError(f"atten_level is required for mode {mode}")
        else:
            atten_level = None

        if mode != "VOA_MODE_RELATIVE":
            reference_port = None

        logger.info(
            "Setting VOA: port=%s mode=%s atten_level=%s reference_port=%s",
            port_id,
            mode,
            atten_level,
            reference_port,
        )

        parts = [
            f'<voa xmlns="{OPSW_NS}">',
            "  <port>",
            f"    <port-id>{port_id}</port-id>",
            f"    <atten-mode>{mode}</atten-mode>",
        ]

        if atten_level is not None:
            parts.append(f"    <atten-level>{atten_level:.2f}</atten-level>")

        if reference_port is not None:
            parts.append(f"    <reference-port>{reference_port}</reference-port>")

        parts.append("  </port>")
        parts.append("</voa>")
        voa_cfg = "\n".join(parts)

        config = f"""
          <nc:config xmlns:nc="{NC_NS}">
            {voa_cfg}
          </nc:config>
        """

        self._edit(config)
        logger.info(
            "VOA configured: port=%s mode=%s atten_level=%s reference_port=%s",
            port_id,
            mode,
            atten_level,
            reference_port,
        )


# ----------------------------------------------------------------------
# DB-backed API – name → port mapping, ownership, etc.
# ----------------------------------------------------------------------


def _timestamped(fname: str, fmt: str = "%Y-%m-%d_{fname}") -> str:
    return datetime.now().strftime(fmt).format(fname=fname)


class Polatis:
    """
    DB-aware wrapper around PolatisNetconf, mimicking the legacy telnet API.

    - Uses MySQL 'provdb' to resolve logical device names to Polatis ports.
    - Enforces ownership via tcdona3.utils.check_patch_owners.
    - Uses NETCONF for all switch operations (no telnet).
    """

    def __init__(
        self,
        polatis: Optional[PolatisNetconf] = None,
        db_host: str = "127.0.0.1",
        db_user: str = "testbed",
        db_password: str = "mypassword",
        db_name: str = "provdb",
        log_file_dir: str = "/tmp",
    ) -> None:
        self.pol = polatis or PolatisNetconf()
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.log_file_dir = log_file_dir

    def __enter__(self) -> "Polatis":
        self.pol.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.pol.close()

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _db_connect(self, admin: bool = False):
        """
        Get a MySQL connection.

        If admin=True, read credentials from /etc/secure_keys/mysql_key.key.
        """
        if admin:
            with open("/etc/secure_keys/mysql_key.key", "r") as f:
                lines = f.readlines()
                user = lines[0].strip()
                pw = lines[1].strip()
        else:
            user = self.db_user
            pw = self.db_password

        return mysql.connector.connect(
            host=self.db_host, user=user, password=pw, database=self.db_name
        )

    # ------------------------------------------------------------------
    # File-based logger similar to legacy API
    # ------------------------------------------------------------------

    def logger(self, message: str) -> None:
        now = datetime.now()
        path = os.path.join(self.log_file_dir, _timestamped("polatis.log"))
        with open(path, "a") as outf:
            outf.write(
                "%s %s\t%s\t%s\n"
                % (
                    now.strftime("%Y/%m/%d %H:%M:%S"),
                    os.getlogin(),
                    getpass.getuser(),
                    message,
                )
            )

    # ------------------------------------------------------------------
    # Name → port mapping (from MySQL device_table)
    # ------------------------------------------------------------------

    def get_inport(self, inx: str) -> int:
        """
        Retrieves the mapped Polatis *input* port number for a given component name.

        Uses device_table.Out_Port where polatis_name = <inx>.
        """
        conn = self._db_connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT `Out_Port` FROM device_table WHERE polatis_name = %s", (inx,)
            )
            row = cursor.fetchone()
            if not row:
                raise KeyError(f"No Out_Port mapping for device {inx!r}")
            return int(row[0])
        finally:
            conn.close()

    def get_outport(self, outx: str) -> int:
        """
        Retrieves the mapped Polatis *output* port number for a given component name.

        Uses device_table.In_Port where polatis_name = <outx>.
        """
        conn = self._db_connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT `In_Port` FROM device_table WHERE polatis_name = %s", (outx,)
            )
            row = cursor.fetchone()
            if not row:
                raise KeyError(f"No In_Port mapping for device {outx!r}")
            return int(row[0])
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Power helpers using NETCONF
    # ------------------------------------------------------------------

    def get_port_power(self, port: int) -> float:
        """Direct port power via NETCONF."""
        return self.pol.get_port_power(port)

    def get_device_power(self, equipment: str, io: str) -> float:
        """
        Get the input/output power of a device by name.

        io: "in"  => map via get_outport()
            "out" => map via get_inport()
        """
        if io == "in":
            port = self.get_outport(equipment)
        elif io == "out":
            port = self.get_inport(equipment)
        else:
            raise ValueError("io must be 'in' or 'out'")
        return self.get_port_power(port)

    # ------------------------------------------------------------------
    # Patch / unpatch with ownership checks (DB + NETCONF, batched)
    # ------------------------------------------------------------------

    def apply_patch_list(self, patch_list: List[Tuple[str, str]]) -> None:
        """
        Apply a list of patches.

        patch_list: list of (input_device_name, output_device_name).

        Optimized: resolves all names to ports and sends a single
        NETCONF edit-config containing all <pair> entries.
        """
        if not isinstance(patch_list, list):
            raise Exception("Argument patch_list must be a list of tuples of patches")
        if len(patch_list) == 0:
            raise Exception("Argument patch_list must not be empty")

        if not check_patch_owners(patch_list):
            logger.error("apply_patch_list failed: patch owners check failed")
            raise Exception(
                "apply_patch_list failed, some (or all) ports are not available. "
                "Please contact admin."
            )

        conn = self._db_connect()
        cursor = conn.cursor()
        try:
            pairs: List[Tuple[int, int]] = []
            mapping: List[Tuple[str, str, int, int]] = []

            # Resolve names → port IDs
            for input_comp, output_comp in patch_list:
                cursor.execute(
                    "SELECT `Out_Port` FROM device_table WHERE polatis_name = %s",
                    (input_comp,),
                )
                row = cursor.fetchone()
                if not row:
                    raise Exception(f"No Out_Port mapping for {input_comp}")
                inp = int(row[0])

                cursor.execute(
                    "SELECT `In_Port` FROM device_table WHERE polatis_name = %s",
                    (output_comp,),
                )
                row = cursor.fetchone()
                if not row:
                    raise Exception(f"No In_Port mapping for {output_comp}")
                outp = int(row[0])

                pairs.append((inp, outp))
                mapping.append((input_comp, output_comp, inp, outp))

            # Single NETCONF edit-config for all cross-connects
            self.pol.create_cross_connects(pairs)
            time.sleep(1)  # allow OPM to update

            # Read back power and print
            for input_comp, output_comp, inp, outp in mapping:
                inpower = self.get_port_power(inp)
                outpower = self.get_port_power(outp)
                data = "%s (%s): %.2f dBm ---> %s (%s): %.2f dBm" % (
                    input_comp,
                    inp,
                    inpower,
                    output_comp,
                    outp,
                    outpower,
                )
                print(data)
                # self.logger("Connect %s" % (data))

        finally:
            cursor.close()
            conn.close()

    def disconnect_devices(self, equipment_1: str, equipment_2: str) -> None:
        """Disconnect a single pair of named devices."""
        self.disconnect_patch_list([(equipment_1, equipment_2)])

    def disconnect_patch_list(self, patch_list: List[Tuple[str, str]]) -> None:
        """
        Disconnect a list of patches based on device names.

        Optimized: resolves all input names → ingress ports and sends a single
        NETCONF edit-config containing all delete <pair> entries.
        """
        if not isinstance(patch_list, list):
            raise Exception("Argument patch_list must be a list of tuples of patches")
        if len(patch_list) == 0:
            raise Exception("Argument patch_list must not be empty")

        if not check_patch_owners(patch_list):
            logger.error("disconnect_patch_list failed: patch owners check failed")
            raise Exception(
                "apply_patch_list failed, some (or all) ports are not available. "
                "Please contact admin."
            )

        conn = self._db_connect()
        cursor = conn.cursor()
        try:
            ingresses: List[int] = []

            for input_comp, _output_comp in patch_list:
                cursor.execute(
                    "SELECT `Out_Port` FROM device_table WHERE polatis_name = %s",
                    (input_comp,),
                )
                row = cursor.fetchone()
                if not row:
                    raise Exception(f"No Out_Port mapping for {input_comp}")
                inp = int(row[0])
                ingresses.append(inp)

            # Single NETCONF edit-config for all deletes
            self.pol.delete_cross_connects(ingresses)
            time.sleep(1)

        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Admin: release ports (ownership in ports_new)
    # ------------------------------------------------------------------

    def release_ports(self, patch_list: List[Tuple[str, str]], username: str) -> None:
        """
        ADMIN ONLY: Set Owner field in ports_new for the provided names.

        Updates ports_new.Owner = <username> for each Name in patch_list.
        """
        conn = self._db_connect(admin=True)
        cursor = conn.cursor()
        try:
            for patch in patch_list:
                for name in patch:
                    if name == "NULL":
                        continue

                    cursor.execute(
                        'UPDATE ports_new SET Owner = %s WHERE Name = %s',
                        (username, name),
                    )

            conn.commit()
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Patch table helpers (print/CSV/list)
    # ------------------------------------------------------------------

    def _get_patch_data(self, patch_list: List[Tuple[str, str]]) -> List[List]:
        """
        Internal method to gather patch data:

        Returns:
          [
            [input_name, "Out", in_port, in_power],
            [output_name, "In", out_port, out_power],
            ...
          ]
        """
        if not isinstance(patch_list, list):
            raise Exception("Argument patch_list must be a list of tuples of patches")
        if len(patch_list) == 0:
            raise Exception("Argument patch_list must not be empty")

        data: List[List] = []
        for inx, outx in patch_list:
            inp = self.get_inport(inx)
            outp = self.get_outport(outx)
            inpower = self.get_port_power(inp)
            outpower = self.get_port_power(outp)
            data.append([inx, "Out", inp, inpower])
            data.append([outx, "In", outp, outpower])
        return data

    def print_patch_table(self, patch_list: List[Tuple[str, str]]) -> None:
        """
        Print a human-readable table of patch_list with live power values.
        """
        if not isinstance(patch_list, list):
            raise Exception("Argument patch_list must be a list of tuples of patches")
        if len(patch_list) == 0:
            raise Exception("Argument patch_list must not be empty")

        for inx, outx in patch_list:
            inp = self.get_inport(inx)
            outp = self.get_outport(outx)
            inpower = self.get_port_power(inp)
            outpower = self.get_port_power(outp)
            data = f"{inx}({inp}): {inpower:.2f} dBm ----> {outx}({outp}): {outpower:.2f} dBm"
            print(data)

    def get_patch_table_csv(self, patch_list: List[Tuple[str, str]], filename: str) -> None:
        """
        Write patch table (with power) to CSV file.
        """
        data = self._get_patch_data(patch_list)
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def get_patch_table_list(self, patch_list: List[Tuple[str, str]]) -> List[List]:
        """
        Return patch table as a list of rows (for further processing).
        """
        return self._get_patch_data(patch_list)

