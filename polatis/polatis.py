"""
Unified Polatis API (NETCONF + DB) with legacy-compatible interface.

Primary class:
    Polatis          – unified API
Backward-compatible alias:
    PolatisNetconf   – kept for existing imports

Features:
  - Power monitoring          (opm-power)
  - VOA control               (voa)
  - OPM configuration         (opm-config)
  - Cross-connect management  (cross-connects)
  - Port shutter control      (polatis-switch RPC)
  - DB-backed device mapping  (device_table, ports_new)
  - Legacy method names kept as aliases where possible

Unsupported legacy telnet-only behaviours raise NotImplementedError.
"""

import logging
from typing import Dict, List, Tuple, Optional

import mysql.connector
from ncclient import manager
from lxml import etree
from tcdona3.utils import check_patch_owners

logger = logging.getLogger(__name__)

OPSW_NS = "http://www.polatis.com/yang/optical-switch"
PLTS_NS = "http://www.polatis.com/yang/polatis-switch"
NC_NS = "urn:ietf:params:xml:ns:netconf:base:1.0"

OPSW = f"{{{OPSW_NS}}}"
PLTS = f"{{{PLTS_NS}}}"
NC = f"{{{NC_NS}}}"


class Polatis:
    """
    Unified Polatis API using NETCONF + MySQL, with compatibility
    to the legacy telnet-based Polatis class.

    - Connection management via NETCONF
    - DB used for logical device <-> port mappings and ownership
    """

    # ------------------------------------------------------------------
    # INIT / LOGIN / LOGOUT (legacy-compatible surface)
    # ------------------------------------------------------------------

    def __init__(
        self,
        host: str = "10.10.10.28",
        port: int = 830,
        username: str = "admin",
        password: str = "root",
        db_host: str = "127.0.0.1",
        db_user: str = "testbed",
        db_pass: str = "mypassword",
        db_name: str = "provdb",
    ):
        """
        Parameters
        ----------
        host : str
            Switch management IP.
        port : int
            NETCONF port (Polatis default: 830).
        username : str
            NETCONF username.
        password : str
            NETCONF password.
        db_host, db_user, db_pass, db_name :
            MySQL connection parameters for provisioning database.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.db_host = db_host
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name

        self._mgr: Optional[manager.Manager] = None

    # Legacy public API compatibility
    def login(self):
        """Legacy method name – mapped to NETCONF connect()."""
        self.connect()

    def logout(self):
        """Legacy method name – mapped to NETCONF close()."""
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # NETCONF connection helpers
    # ------------------------------------------------------------------

    def connect(self):
        """Ensure a NETCONF session is established."""
        if self._mgr is not None and self._mgr.connected:
            return

        logger.info("Connecting NETCONF session to %s:%s", self.host, self.port)
        self._mgr = manager.connect(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            hostkey_verify=False,
            allow_agent=False,
            look_for_keys=False,
            timeout=30,
        )
        logger.info("NETCONF session established")

    def close(self):
        """Close NETCONF session if open."""
        if self._mgr is not None:
            try:
                if self._mgr.connected:
                    logger.info("Closing NETCONF session")
                    self._mgr.close_session()
            except Exception:
                logger.exception("Error while closing NETCONF session")
        self._mgr = None

    def __enter__(self) -> "Polatis":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    @property
    def mgr(self) -> manager.Manager:
        """Return active ncclient Manager, reconnecting if needed."""
        if self._mgr is None or not self._mgr.connected:
            self.connect()
        return self._mgr

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _subtree_get(self, xml_subtree: str) -> etree._Element:
        """Perform NETCONF <get> with subtree filter; return <data> element."""
        logger.debug("NETCONF GET filter:\n%s", xml_subtree)
        reply = self.mgr.get(filter=("subtree", xml_subtree))
        data_ele = reply.data_ele
        logger.debug(
            "NETCONF GET response:\n%s",
            etree.tostring(data_ele, pretty_print=True).decode(errors="ignore"),
        )
        return data_ele

    def _edit(self, config_xml: str):
        """Perform NETCONF <edit-config> on running datastore."""
        logger.info("NETCONF edit-config:\n%s", config_xml)
        try:
            reply = self.mgr.edit_config(target="running", config=config_xml)
            logger.debug("edit-config reply: %s", reply)
        except Exception:
            logger.exception("edit-config failed")
            raise

    def _rpc(self, rpc_xml: str):
        """Send a raw NETCONF <rpc> (used for shutter RPCs)."""
        logger.info("NETCONF RPC:\n%s", rpc_xml)
        try:
            return self.mgr.dispatch(etree.fromstring(rpc_xml))
        except Exception:
            logger.exception("RPC failed")
            raise

    # ------------------------------------------------------------------
    # DB Helpers
    # ------------------------------------------------------------------

    def _db(self):
        """Return a new MySQL connection."""
        return mysql.connector.connect(
            host=self.db_host,
            user=self.db_user,
            password=self.db_pass,
            database=self.db_name,
        )

    # ------------------------------------------------------------------
    # Power monitoring (opm-power)
    # ------------------------------------------------------------------

    def get_port_power(self, port: int) -> float:
        """
        Get optical power (dBm) for a single port using /opm-power.
        """
        subtree = f"""
        <opm-power xmlns="{OPSW_NS}">
          <port>
            <port-id>{port}</port-id>
            <power/>
          </port>
        </opm-power>
        """
        data = self._subtree_get(subtree)
        pwr_elem = data.find(
            f".//{OPSW}opm-power/{OPSW}port[{OPSW}port-id='{port}']/{OPSW}power"
        )
        if pwr_elem is None or pwr_elem.text is None:
            raise KeyError(f"No power reading for port {port}")
        power = float(pwr_elem.text)
        logger.info("Port %s power = %.2f dBm", port, power)
        return power

    def get_all_power(self) -> Dict[int, float]:
        """
        Get power for all ports with OPM entries.
        """
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
            pid = port_elem.find(f"{OPSW}port-id")
            pwr = port_elem.find(f"{OPSW}power")
            if pid is None or pid.text is None:
                continue
            if pwr is None or pwr.text is None:
                continue
            try:
                pid_val = int(pid.text)
                pwr_val = float(pwr.text)
            except ValueError:
                continue
            result[pid_val] = pwr_val
        logger.info("Retrieved power for %d ports", len(result))
        return result

    # Legacy PMON state – not representable via YANG
    def get_all_pmon(self):
        raise NotImplementedError(
            "PMON details (monmode, averaging time, full calibration state) "
            "are not exposed via Polatis YANG models."
        )

    # ------------------------------------------------------------------
    # OPM config (wavelength / offset) – opm-config
    # ------------------------------------------------------------------

    def set_opm_config(
        self,
        port: int,
        wavelength_nm: Optional[float] = None,
        offset_db: Optional[float] = None,
    ):
        """
        Set OPM configuration (wavelength, offset) for a port.
        """
        if wavelength_nm is None and offset_db is None:
            logger.info("set_opm_config called with no changes; skipping")
            return

        parts = [
            f'<opm-config xmlns="{OPSW_NS}"><port>',
            f"<port-id>{port}</port-id>",
        ]
        if wavelength_nm is not None:
            parts.append(f"<wavelength>{float(wavelength_nm):.2f}</wavelength>")
        if offset_db is not None:
            parts.append(f"<offset>{float(offset_db):.2f}</offset>")
        parts.append("</port></opm-config>")

        cfg = f'<nc:config xmlns:nc="{NC_NS}">' + "".join(parts) + "</nc:config>"
        self._edit(cfg)

    # ------------------------------------------------------------------
    # VOA (voa)
    # ------------------------------------------------------------------

    def get_all_atten(self) -> Dict[int, float]:
        """
        Return VOA attenuation levels for all ports that have one set.
        """
        subtree = f"""
        <voa xmlns="{OPSW_NS}">
          <port>
            <port-id/>
            <atten-level/>
          </port>
        </voa>
        """
        data = self._subtree_get(subtree)
        out: Dict[int, float] = {}
        for port_elem in data.findall(f".//{OPSW}voa/{OPSW}port"):
            pid = port_elem.find(f"{OPSW}port-id")
            lvl = port_elem.find(f"{OPSW}atten-level")
            if pid is None or pid.text is None:
                continue
            if lvl is None or lvl.text is None:
                continue
            try:
                out[int(pid.text)] = float(lvl.text)
            except ValueError:
                continue
        return out

    def set_voa(
        self,
        port: int,
        mode: str,
        atten_level: Optional[float] = None,
        reference_port: Optional[int] = None,
    ):
        """
        Configure VOA for a given port.

        mode must be one of:
          VOA_MODE_NONE, VOA_MODE_RELATIVE, VOA_MODE_ABSOLUTE,
          VOA_MODE_CONVERGED, VOA_MODE_MAXIMUM, VOA_MODE_FIXED
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

        parts = [
            f'<voa xmlns="{OPSW_NS}"><port>',
            f"<port-id>{port}</port-id>",
            f"<atten-mode>{mode}</atten-mode>",
        ]
        if atten_level is not None:
            parts.append(f"<atten-level>{float(atten_level):.2f}</atten-level>")
        if reference_port is not None:
            parts.append(f"<reference-port>{reference_port}</reference-port>")
        parts.append("</port></voa>")

        cfg = f'<nc:config xmlns:nc="{NC_NS}">' + "".join(parts) + "</nc:config>"
        self._edit(cfg)

    # ------------------------------------------------------------------
    # Port labels / status (port-config)
    # ------------------------------------------------------------------

    def get_all_labels(self) -> Dict[int, str]:
        """
        Return dict[port-id] -> label.
        """
        subtree = f"""
        <port-config xmlns="{OPSW_NS}">
          <port>
            <port-id/>
            <label/>
          </port>
        </port-config>
        """
        data = self._subtree_get(subtree)
        out: Dict[int, str] = {}
        for port_elem in data.findall(f".//{OPSW}port-config/{OPSW}port"):
            pid = port_elem.find(f"{OPSW}port-id")
            lbl = port_elem.find(f"{OPSW}label")
            if pid is None or pid.text is None:
                continue
            if lbl is None or lbl.text is None:
                continue
            out[int(pid.text)] = lbl.text
        return out

    def get_port_status(self, port: int) -> Optional[str]:
        """
        Return status string for a port (ENABLED / DISABLED / FAILED / etc.).
        """
        subtree = f"""
        <port-config xmlns="{OPSW_NS}">
          <port>
            <port-id>{port}</port-id>
            <status/>
          </port>
        </port-config>
        """
        data = self._subtree_get(subtree)
        status_elem = data.find(
            f".//{OPSW}port-config/{OPSW}port[{OPSW}port-id='{port}']/{OPSW}status"
        )
        return status_elem.text if status_elem is not None else None

    # ------------------------------------------------------------------
    # Shutter Control (polatis-switch RPC)
    # ------------------------------------------------------------------

    def enable_port(self, port: int):
        """
        Enable a port shutter via polatis-switch RPC.
        """
        rpc = f"""
        <port-shutter-set-state xmlns="{PLTS_NS}">
          <port-enab>{port}</port-enab>
        </port-shutter-set-state>
        """
        self._rpc(rpc)

    def disable_port(self, port: int):
        """
        Disable a port shutter via polatis-switch RPC.
        """
        rpc = f"""
        <port-shutter-set-state xmlns="{PLTS_NS}">
          <port-disab>{port}</port-disab>
        </port-shutter-set-state>
        """
        self._rpc(rpc)

    # Legacy private names
    __enable_port = enable_port
    __disable_port = disable_port

    def get_all_shutter(self):
        """
        Legacy method; shutter state is not exposed in YANG.
        """
        raise NotImplementedError(
            "Shutter state is not exposed via the Polatis YANG models."
        )

    def __disable_all(self):
        raise NotImplementedError("No YANG support for disable_all().")

    def __enable_all(self):
        raise NotImplementedError("No YANG support for enable_all().")

    # ------------------------------------------------------------------
    # Cross-connects (cross-connects)
    # ------------------------------------------------------------------

    def create_cross_connect(self, ingress: int, egress: int):
        """
        Create a cross-connect ingress -> egress.
        """
        cfg = f"""
        <nc:config xmlns:nc="{NC_NS}">
          <cross-connects xmlns="{OPSW_NS}">
            <pair>
              <ingress>{ingress}</ingress>
              <egress>{egress}</egress>
            </pair>
          </cross-connects>
        </nc:config>
        """
        self._edit(cfg)

    def delete_cross_connect(self, ingress: int):
        """
        Delete a cross-connect identified by ingress port.
        """
        cfg = f"""
        <nc:config xmlns:nc="{NC_NS}">
          <cross-connects xmlns="{OPSW_NS}">
            <pair nc:operation="delete">
              <ingress>{ingress}</ingress>
            </pair>
          </cross-connects>
        </nc:config>
        """
        self._edit(cfg)

    # Legacy aliases
    __conn = create_cross_connect
    __disconn = delete_cross_connect
    __fullconn = create_cross_connect
    __fulldisconn = delete_cross_connect

    def get_cross_connects(self) -> List[Tuple[int, int]]:
        """
        Return a list of (ingress, egress) tuples for all cross-connects.
        """
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
            i = pair_elem.find(f"{OPSW}ingress")
            e = pair_elem.find(f"{OPSW}egress")
            if i is None or i.text is None:
                continue
            if e is None or e.text is None:
                continue
            try:
                ingress = int(i.text)
                egress = int(e.text)
            except ValueError:
                continue
            pairs.append((ingress, egress))
        return pairs

    # Legacy name
    get_all_patch = get_cross_connects

    def connection_exists(self, ingress: int, egress: Optional[int] = None) -> bool:
        """
        Check if a cross-connect exists for ingress (and optionally egress).
        """
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
        pairs = data.findall(f".//{OPSW}pair")
        if not pairs:
            return False

        for p in pairs:
            i = p.find(f"{OPSW}ingress")
            e = p.find(f"{OPSW}egress")
            if i is None or i.text is None:
                continue
            try:
                i_val = int(i.text)
            except ValueError:
                continue
            if i_val != ingress:
                continue
            if egress is None:
                return True
            if e is None or e.text is None:
                continue
            try:
                e_val = int(e.text)
            except ValueError:
                continue
            if e_val == egress:
                return True
        return False

    # ------------------------------------------------------------------
    # DB-based device mapping / ownership
    # ------------------------------------------------------------------

    def get_inport(self, inx: str) -> int:
        """
        Map logical device name -> Polatis OUT port (legacy behaviour).
        """
        db = self._db()
        cur = db.cursor()
        cur.execute(
            "SELECT Out_Port FROM device_table WHERE polatis_name=%s", (inx,)
        )
        row = cur.fetchone()
        cur.close()
        db.close()
        if not row:
            raise KeyError(f"No device_table entry for {inx}")
        return int(row[0])

    def get_outport(self, outx: str) -> int:
        """
        Map logical device name -> Polatis IN port (legacy behaviour).
        """
        db = self._db()
        cur = db.cursor()
        cur.execute(
            "SELECT In_Port FROM device_table WHERE polatis_name=%s", (outx,)
        )
        row = cur.fetchone()
        cur.close()
        db.close()
        if not row:
            raise KeyError(f"No device_table entry for {outx}")
        return int(row[0])

    def get_device_power(self, equipment: str, io: str) -> float:
        """
        Get power for device input or output side (legacy semantics).
        """
        if io == "in":
            port = self.get_outport(equipment)
        elif io == "out":
            port = self.get_inport(equipment)
        else:
            raise ValueError("io must be 'in' or 'out'")
        return self.get_port_power(port)

    def release_ports(self, patch_list, username: str):
        """
        ADMIN ONLY: update ports_new.Owner for all names in patch_list.
        """
        db = self._db()
        cur = db.cursor()
        for patch in patch_list:
            for name in patch:
                if name == "NULL":
                    continue
                cur.execute(
                    "UPDATE ports_new SET Owner=%s WHERE Name=%s",
                    (username, name),
                )
        db.commit()
        cur.close()
        db.close()

    # ------------------------------------------------------------------
    # Patch application (batched edit-config)
    # ------------------------------------------------------------------

    def apply_patch_list(self, patch_list: List[Tuple[str, str]]):
        """
        Apply a list of device pairs using DB mapping + NETCONF.

        patch_list is a list of (input_device_name, output_device_name).
        """
        if not isinstance(patch_list, list) or not patch_list:
            raise Exception("patch_list must be a non-empty list of tuples")

        if not check_patch_owners(patch_list):
            raise Exception(
                "apply_patch_list failed, some (or all) ports are not available. Please contact admin."
            )

        pairs_xml: List[str] = []
        for (input_comp, output_comp) in patch_list:
            inp = self.get_inport(input_comp)
            outp = self.get_outport(output_comp)
            pairs_xml.append(
                f"<pair><ingress>{inp}</ingress><egress>{outp}</egress></pair>"
            )

        cfg = (
            f'<nc:config xmlns:nc="{NC_NS}">'
            f'<cross-connects xmlns="{OPSW_NS}">'
            + "".join(pairs_xml)
            + "</cross-connects></nc:config>"
        )
        self._edit(cfg)

    def disconnect_devices(self, equipment_1: str, equipment_2: str):
        """
        Legacy helper – disconnect a single device pair.
        """
        self.disconnect_patch_list([(equipment_1, equipment_2)])

    def disconnect_patch_list(self, patch_list: List[Tuple[str, str]]):
        """
        Disconnect a list of device pairs using DB + NETCONF.
        """
        if not isinstance(patch_list, list) or not patch_list:
            raise Exception("patch_list must be a non-empty list of tuples")

        if not check_patch_owners(patch_list):
            raise Exception(
                "disconnect_patch_list failed, some (or all) ports are not available. Please contact admin."
            )

        pairs_xml: List[str] = []
        for (input_comp, output_comp) in patch_list:
            # legacy semantics: delete by ingress only
            inp = self.get_inport(input_comp)
            pairs_xml.append(
                f'<pair nc:operation="delete"><ingress>{inp}</ingress></pair>'
            )

        cfg = (
            f'<nc:config xmlns:nc="{NC_NS}">'
            f'<cross-connects xmlns="{OPSW_NS}">'
            + "".join(pairs_xml)
            + "</cross-connects></nc:config>"
        )
        self._edit(cfg)

    # ------------------------------------------------------------------
    # Patch table utilities (for reporting / CSV)
    # ------------------------------------------------------------------

    def _get_patch_data(self, patch_list: List[Tuple[str, str]]):
        """
        Internal helper: return list rows of:
        [device_name, 'Out'/'In', port_number, power_dBm]
        """
        if not isinstance(patch_list, list) or not patch_list:
            raise Exception("patch_list must be a non-empty list of tuples")

        rows: List[List] = []
        for (inx, outx) in patch_list:
            inp = self.get_inport(inx)
            outp = self.get_outport(outx)
            inpower = self.get_port_power(inp)
            outpower = self.get_port_power(outp)
            rows.append([inx, "Out", inp, inpower])
            rows.append([outx, "In", outp, outpower])
        return rows

    def get_patch_table_list(self, patch_list: List[Tuple[str, str]]):
        return self._get_patch_data(patch_list)

    def get_patch_table_csv(self, patch_list: List[Tuple[str, str]], filename: str):
        import csv

        rows = self._get_patch_data(patch_list)
        with open(filename, "w") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    def print_patch_table(self, patch_list: List[Tuple[str, str]]):
        if not isinstance(patch_list, list) or not patch_list:
            raise Exception("patch_list must be a non-empty list of tuples")

        for patch in patch_list:
            inx, outx = patch
            inp = self.get_inport(inx)
            outp = self.get_outport(outx)
            inpower = self.get_port_power(int(inp))
            outpower = self.get_port_power(int(outp))
            data = f"{inx}({inp}): {inpower:.2f} dBm ----> {outx}({outp}): {outpower:.2f} dBm"
            print(data)

    # ------------------------------------------------------------------
    # Legacy methods with no clean NETCONF equivalent
    # ------------------------------------------------------------------

    def __sendcmd(self, line):
        raise NotImplementedError("Legacy telnet __sendcmd() is not supported.")

    def __settimeout(self, timeout=60):
        raise NotImplementedError("No NETCONF equivalent for settimeout().")

    def __clearallconn(self):
        raise NotImplementedError("No NETCONF 'clear all connections' primitive.")

    def get_NE_type(self):
        raise NotImplementedError("NE type is not exposed via NETCONF YANG models.")

    def test_all_power(self):
        raise NotImplementedError("Continuous polling loop not provided here.")

    def getall(self):
        raise NotImplementedError("Legacy aggregate getall() not implemented.")

    def report_all(self):
        raise NotImplementedError("Legacy report_all() not implemented.")


# Backward-compatible alias
PolatisNetconf = Polatis

