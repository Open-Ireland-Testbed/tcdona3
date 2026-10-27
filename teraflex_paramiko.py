import paramiko
import time
import re
from typing import Any, Dict, List

PROMPT_PATTERN = r"[>#]\s*$"
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_PAGER_RE = re.compile(r"--\s*more\s*--|\(END\)", re.IGNORECASE)

#: The FEC layer of each logical interface type.
OTUC_LAYER = {"ot100": "otuc1-p", "ot200": "otuc2-p", "ot300": "otuc3-p", "ot400": "otuc4-p",
              "ot500": "otuc5-p", "ot600": "otuc6-p"}

_TABLE_HDR_RE = re.compile(r"^pm\s+(.+?)\s+table:\s*$")
_VALUE_RE = re.compile(r"^(?P<val>[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)\s*(?P<unit>\S+(?:\s\S+)?)?$")
_ELAPSED_RE = re.compile(r"(\d+(?::\d+)*)\s+([dhms](?::[dhms])*)(?:\s|$)")
_SECONDS = {"d": 86400, "h": 3600, "m": 60, "s": 1}

#: QoT metric -> (PM layer, pattern of its pm-name), for :meth:`TeraflexSSH.get_qot_snapshot`.
QOT_FIELDS = {
    "tx_power": ("opt-phy", r"^opt-tx-pwr$"),
    "rx_power_total": ("opt-phy", r"^opt-rx-pwr$"),
    "rx_power_channel": ("otsi", r"^opt-rx-pwr$"),
    "snr": ("otsi", r"^signal-to-noise-ratio$"),
    "osnr": ("otsi", r"^optical-signal-to-noise-ratio$"),
    "q_factor": ("otsi", r"^q-factor$"),
    "pdl": ("otsi", r"polarization-dependent-loss"),
    "dgd": ("otsi", r"differential-group-delay"),
    "cfo": ("otsi", r"carrier-frequency-offset"),
    # the dispersion the DSP compensates, the opposite of the link's accumulated dispersion
    "cdc": ("otsi", r"^chromatic-dispersion-compensation$"),
    "sop_rate": ("otsi", r"state-of-polarization|(^|-)sop(-|$)"),
    "fec_ber_live": ("otuc", r"^fec-ber$"),
    "fec_ber_mean": ("otuc", r"^fec-ber-mean$"),
    "fec_corrected_errors": ("otuc", r"fec-corrected-errors"),
    "fec_uncorrected_blocks": ("otuc", r"fec-uncorrected-blocks"),
}


def otuc_layer(logical_type):
    """The FEC layer of a logical interface type (``ot400`` -> ``otuc4-p``)."""
    return OTUC_LAYER.get(str(logical_type).lower(), "otuc4-p")


def _elapsed_seconds(text):
    match = _ELAPSED_RE.search(text)
    if not match:
        return None
    values, units = match.group(1).split(":"), match.group(2).split(":")
    if len(values) != len(units):
        return None
    return sum(int(v) * _SECONDS.get(u, 0) for v, u in zip(values, units))


def parse_pm_output(raw, layer=None):
    """Parse a ``show ... pm current`` output into one table per monitored entity and interval:
    ``{"layer", "table", "interval", "profile", "suspect", "elapsed_s", "pms": {name: {"value", "unit", "loc",
    "raw"}}}``. The current value is read in the ``curr-value`` column (the threshold columns follow it);
    ``N/A`` is None.
    """
    tables, name, cur, expect_entity, columns = [], None, None, False, None
    for line in _ANSI_RE.sub("", raw).replace("\r", "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        header = _TABLE_HDR_RE.match(stripped)
        if header:
            name, cur, expect_entity, columns = header.group(1), None, False, None
            continue
        if name is None:
            continue
        if stripped.startswith("mon-entity"):
            expect_entity, columns = True, None
            continue
        if expect_entity:
            tok = stripped.split()
            cur = {"layer": layer, "table": name, "interval": tok[1] if len(tok) > 1 else None,
                   "profile": tok[2] if len(tok) > 2 else None, "suspect": None,
                   "elapsed_s": _elapsed_seconds(stripped), "pms": {}}
            if cur["profile"]:
                cur["table"] = "%s:%s" % (name, cur["profile"])
            cur["suspect"] = False if "not-suspect" in tok else True if "suspect" in tok else None
            tables.append(cur)
            expect_entity = False
            continue
        if cur is None:
            continue
        if stripped.startswith("pm-name"):
            start = line.find("curr-value")
            after = re.search(r"\S", line[start + len("curr-value"):]) if start >= 0 else None
            columns = (start, start + len("curr-value") + after.start() if after else None)
            continue
        if columns is None or columns[0] < 0:
            continue
        tok = stripped.split()
        if len(tok) < 2:
            continue
        cell = line[columns[0]:columns[1]].strip() if columns[1] else line[columns[0]:].strip()
        value = unit = None
        match = _VALUE_RE.match(cell)
        if match:
            value, unit = float(match.group("val")), match.group("unit")
        cur["pms"][tok[0]] = {"value": value, "unit": unit, "loc": tok[1], "raw": cell}
    return tables


def _interval_rank(table):
    interval = (table.get("interval") or "").lower()
    if interval == "live" or table["table"].startswith("nint-"):
        return 0
    if "15" in interval:
        return 1
    return 2


def find_pm(tables, layer, pattern):
    """The first numeric PM of ``layer`` whose name matches ``pattern``, live tables first, or None:
    ``{"pm_name", "value", "unit", "raw", "table", "interval", "suspect"}``."""
    rx = re.compile(pattern)
    hits = []
    for table in tables:
        if layer and table["layer"] != layer:
            continue
        for name, entry in table["pms"].items():
            if rx.search(name) and entry["value"] is not None:
                hits.append((_interval_rank(table), len(hits), table, name, entry))
    if not hits:
        return None
    hits.sort(key=lambda hit: hit[:2])
    _, _, table, name, entry = hits[0]
    return {"pm_name": name, "value": entry["value"], "unit": entry["unit"], "raw": entry["raw"],
            "table": table["table"], "interval": table["interval"], "suspect": table["suspect"]}

DEVICE_MAP = {
    "tf_1": {"ip": "10.10.10.92", "line_port": "1/1/n1"},
    "tf_2": {"ip": "10.10.10.92", "line_port": "1/1/n2"},
    "tf_3": {"ip": "10.10.10.92", "line_port": "1/2/n1"},
    "tf_4": {"ip": "10.10.10.92", "line_port": "1/2/n2"},
}

class TeraflexSSH:
    """A TeraFlex line port over SSH (the CLI of the device).

    :param tf_name: ``tf_1`` to ``tf_4`` (see ``DEVICE_MAP``: the line port, and the address unless ``host``).
    :param timeout: Time allowed to connect and for each command's output [s].
    :param host: Optional address overriding the one of ``DEVICE_MAP``.

    The driver can be used as a context manager; :meth:`close` ends the session.
    """

    def __init__(self, tf_name: str, username: str, password: str, timeout: int = 10, host=None):
        print("Teraflex Paramiko Initialised...")
        if tf_name not in DEVICE_MAP:
            raise ValueError(f"Unknown TFlex device '{tf_name}'")
        cfg = DEVICE_MAP[tf_name]
        self.host = host or cfg["ip"]
        self.line_port = cfg["line_port"]

        self.username = username
        self.password = password
        self.timeout = timeout
        self._connect()

    def _connect(self):
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(self.host, username=self.username,
                                password=self.password, timeout=self.timeout,
                                look_for_keys=False, allow_agent=False)
            self.channel = self.client.invoke_shell()
            time.sleep(1)
            if self.channel.recv_ready():
                self.channel.recv(65535)
        except paramiko.AuthenticationException as e:
            raise Exception(f"Authentication failed for {self.host}: {e}")
        except paramiko.SSHException as e:
            raise Exception(f"SSH connection failed to {self.host}: {e}")
        except Exception as e:
            raise Exception(f"Failed to connect to {self.host}: {e}")

    def _send(self, cmd: str) -> str:
        """Send ``cmd`` and return its output up to the prompt (what came in ``timeout`` s when no prompt did).

        The prompt is looked for on the last line of the whole output (not of the last chunk, which may hold
        only part of it); a pager prompt is answered and removed; a channel the device closed ends the read.
        """
        self.channel.send(cmd.strip() + "\n")
        buff = ""
        end_time = time.time() + self.timeout
        while time.time() < end_time:
            if self.channel.recv_ready():
                data = self.channel.recv(65535)
                if not data:
                    break
                part = data.decode("utf-8", errors="ignore")
                if _PAGER_RE.search(part):
                    self.channel.send(" ")
                    part = _PAGER_RE.sub("", part)
                buff += part
                lines = _ANSI_RE.sub("", buff).rstrip("\r\n").splitlines()
                if lines and re.search(PROMPT_PATTERN, lines[-1]):
                    break
            elif getattr(self.channel, "closed", False) or self.channel.exit_status_ready():
                break
            else:
                time.sleep(0.1)
        return buff

    def close(self):
        """Close the shell and the SSH connection; closing never raises."""
        for handle in (getattr(self, "channel", None), getattr(self, "client", None)):
            try:
                if handle is not None:
                    handle.close()
            except Exception as error:
                print("closing the TeraFlex session: %s" % error)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def pm_commands(self, logical: str = "ot400") -> Dict[str, str]:
        """The ``show ... pm current`` command of each PM layer of this line port."""
        full_if = f"{self.line_port}/{logical}"
        return {"opt-phy": f"show interface {self.line_port} opt-phy pm current",
                "otsi": f"show interface {full_if} otsia otsi 1 pm current",
                "otuc": f"show interface {full_if} {otuc_layer(logical)} pm current"}

    def get_pm(self, layer: str = "otsi", logical: str = "ot400"):
        """Every PM table of one layer (``opt-phy``, ``otsi`` or ``otuc``), parsed (see :func:`parse_pm_output`)."""
        return parse_pm_output(self._send(self.pm_commands(logical)[layer]), layer)

    def get_all_pm(self, logical: str = "ot400"):
        """Every PM table of the opt-phy, otsi and otuc layers."""
        tables = []
        for layer, command in self.pm_commands(logical).items():
            tables += parse_pm_output(self._send(command), layer)
        return tables

    def get_qot_snapshot(self, logical: str = "ot400", tables=None) -> Dict[str, Any]:
        """The QoT metrics of :data:`QOT_FIELDS` as floats (None when not reported), live values first, with
        ``units`` and ``source`` (``layer:table:pm-name (interval[, SUSPECT])``) per metric."""
        if tables is None:
            tables = self.get_all_pm(logical)
        snapshot = {"timestamp": time.time(), "port": self.line_port, "logical_type": logical,
                    "units": {}, "source": {}}
        for key, (layer, pattern) in QOT_FIELDS.items():
            hit = find_pm(tables, layer, pattern)
            snapshot[key] = hit["value"] if hit else None
            if hit:
                snapshot["units"][key] = hit["unit"]
                flag = ", SUSPECT" if hit["suspect"] else ""
                snapshot["source"][key] = "%s:%s:%s (%s%s)" % (layer, hit["table"], hit["pm_name"],
                                                                hit["interval"], flag)
        return snapshot

    def port_config_commands(self, logical: str = "ot400") -> List[str]:
        """The ``show interface`` views that hold the configuration of this line port: the port, its optical
        layer, its logical interface, its OTSi and the OTSi's optical channel."""
        full_if = f"{self.line_port}/{logical}"
        return [f"show interface {self.line_port}", f"show interface {self.line_port} opt-phy",
                f"show interface {full_if}", f"show interface {full_if} otsia otsi 1",
                f"show interface {full_if} otsia otsi 1 optical-channel-configuration"]

    def get_current_config(self, logical: str = "ot400") -> str:
        """The running configuration (``show running-config``), without the echo of the command and the prompt.

        A TeraFlex CLI that does not know ``show running-config`` answers it with an error; then the result is
        the configuration of this line port instead: each view of :meth:`port_config_commands`, after the
        command that produced it.
        """
        raw = self._send("show running-config")
        if "Error(" not in raw:
            return self._clean_output(raw)
        return "\n".join("%s\n%s" % (command, self._clean_output(self._send(command)))
                         for command in self.port_config_commands(logical))

    def read_pm_data(self) -> Dict[str, Any]:
        print(f"show interface {self.line_port} opt-phy pm current ...")
        raw = self._send(f"show interface {self.line_port} opt-phy pm current")
        return self._parse_pm(raw)

    @staticmethod
    def _clean_output(raw: str) -> str:
        lines = raw.splitlines()
        return "\n".join(lines[1:-1])

    @staticmethod
    def _parse_pm(raw: str) -> Dict[str, Any]:
        """
        Parses only the 'live' PM data from Teraflex CLI output.
        """
        # Extract the "live" block only
        live_block = re.search(r"mon-entity\s+interval\s+pm-profile.*?opt-phy\s+live.*?(?=\r\n\r\n|\Z)", raw, re.DOTALL)
        if not live_block:
            return {"rx_power": None, "tx_power": None, "raw_output": raw}

        block = live_block.group(0)

        rx = re.search(r"opt-rx-pwr\s+\S+\s+([-\d.]+)\s*dBm", block)
        tx = re.search(r"opt-tx-pwr\s+\S+\s+([-\d.]+)\s*dBm", block)

        return {
            "rx_power": float(rx.group(1)) if rx else None,
            "tx_power": float(tx.group(1)) if tx else None,
            "raw_output": block.strip()
        }

    def return_current_config(self, logical: str = "ot400") -> Dict[str, Any]:
        """
        Pull PM data for this device's line port, then parse:
          • rx_power, tx_power      from opt‑phy live
          • snr, osnr, q_factor      from otsia/QualityTF live
          • fec_ber_live, fec_ber_15min  from the FEC layer (otuc<n>-p) live & 15min
        Values are the numbers as the device prints them (str), None when not reported.
        """
        commands = self.pm_commands(logical)
        fec_layer = otuc_layer(logical)

        # 1) Grab each block separately
        raw_phy   = self._send(commands["opt-phy"])
        raw_otsi  = self._send(commands["otsi"])
        raw_fec   = self._send(commands["otuc"])

        # print(f"RAW PHY: {raw_phy}")
        # print(f"RAW OTSI: {raw_otsi}")
        # print(f"RAW FEC: {raw_fec}")

        results = {
            "rx_power":      None,
            "tx_power":      None,
            "rx_power_15min":   None,
            "tx_power_15min":   None,
            "snr":           None,
            "osnr":          None,
            "q_factor":      None,
            "fec_ber_live":  None,
            "fec_ber_15min": None
        }

        # 2) Parse opt-phy live block (rx/tx)
        phy_blk = re.search(
            r"mon-entity\s+interval\s+pm-profile.*?opt-phy\s+live.*?(?=\n\s*\n|\Z)",
            raw_phy, re.DOTALL
        )
        if phy_blk:
            blk = phy_blk.group(0)
            m = re.search(r"opt-rx-pwr\s+\S+\s+(-?\d+(\.\d+)?)\s*dBm", blk)
            if m: results["rx_power"] = f"{m.group(1)}"
            m = re.search(r"opt-tx-pwr\s+\S+\s+(-?\d+(\.\d+)?)\s*dBm", blk)
            if m: results["tx_power"] = f"{m.group(1)}"

        phy_15 = re.search(
            r"mon-entity\s+interval\s+pm-profile.*?opt-phy\s+15min.*?(?=\n\s*\n|\Z)",
            raw_phy, re.DOTALL
        )
        if phy_15:
            blk15 = phy_15.group(0)
            m = re.search(r"opt-rx-pwr-mean\s+\S+\s+(-?\d+(\.\d+)?)\s*dBm", blk15)
            if m: results["rx_power_15min"] = m.group(1)
            m = re.search(r"opt-tx-pwr-mean\s+\S+\s+(-?\d+(\.\d+)?)\s*dBm", blk15)
            if m: results["tx_power_15min"] = m.group(1)

        # 3) Parse the otsi tables (snr, osnr, q_factor): the exact pm-name, live tables first, negative
        #    values included; whatever the pm-profile (QualityTF, QualityTF400g16Q...)
        otsi_tables = parse_pm_output(raw_otsi, "otsi")
        for key in ("snr", "osnr", "q_factor"):
            hit = find_pm(otsi_tables, "otsi", QOT_FIELDS[key][1])
            if hit:
                results[key] = hit["raw"].split()[0]

        # 4) Parse FEC blocks from raw_fec
        fec_live = re.search(
            re.escape(fec_layer) + r"\s+live.*?fec-ber\s+\S+\s+([0-9.eE+-]+)",
            raw_fec, re.DOTALL
        )
        if fec_live:
            results["fec_ber_live"] = fec_live.group(1)

        fec_15 = re.search(r"fec-ber-mean\s+\S+\s+([0-9.eE-]+)", raw_fec)
        if fec_15:
            results["fec_ber_15min"] = fec_15.group(1)

        return results

