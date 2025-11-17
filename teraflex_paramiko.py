import paramiko
import time
import re
from typing import Dict, Any

PROMPT_PATTERN = r"[>#]\s*$"

DEVICE_MAP = {
    "tf_1": {"ip": "10.10.10.92", "line_port": "1/1/n1"},
    "tf_2": {"ip": "10.10.10.92", "line_port": "1/1/n2"},
    "tf_3": {"ip": "10.10.10.92", "line_port": "1/2/n1"},
    "tf_4": {"ip": "10.10.10.92", "line_port": "1/2/n2"},
}

class TeraflexSSH:
    def __init__(self, tf_name: str, username: str, password: str, timeout: int = 10):
        print("Teraflex Paramiko Initialised...")
        if tf_name not in DEVICE_MAP:
            raise ValueError(f"Unknown TFlex device '{tf_name}'")
        cfg = DEVICE_MAP[tf_name]
        self.host = cfg["ip"]
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
        self.channel.send(cmd.strip() + "\n")
        buff = ""
        end_time = time.time() + self.timeout
        while time.time() < end_time:
            if self.channel.recv_ready():
                part = self.channel.recv(65535).decode("utf-8", errors="ignore")
                buff += part
                # exit if prompt appears after command
                if re.search(PROMPT_PATTERN, part.splitlines()[-1]):
                    break
            else:
                time.sleep(0.1)
        return buff

    def close(self):
        self.client.close()

    def get_current_config(self) -> str:
        raw = self._send("show running-config")
        return self._clean_output(raw)

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
          • fec_ber_live, fec_ber_15min  from otuc4-p live & 15min
        """
        full_if = f"{self.line_port}/{logical}"

        # 1) Grab each block separately
        raw_phy   = self._send(f"show interface {self.line_port}             opt-phy pm current")
        raw_otsi  = self._send(f"show interface {full_if}                   otsia otsi 1 pm current")
        raw_fec   = self._send(f"show interface {full_if}                   otuc4-p pm current")

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

        # 3) Parse otsia/QualityTF live block (snr, osnr, q_factor)
        #    there are two possible pm-profiles: QualityTF and QualityTF400g16Q…
        #    we'll just scan the entire raw_otsi for the metrics
        m = re.search(r"signal-to-noise-ratio\s+\S+\s+(\d+(\.\d+)?)\s*dB", raw_otsi)
        if m: results["snr"] = f"{m.group(1)}"
        m = re.search(r"optical-signal-to-noise-ratio\s+\S+\s+(\d+(\.\d+)?)\s*dB", raw_otsi)
        if m: results["osnr"] = f"{m.group(1)}"
        m = re.search(r"q-factor\s+\S+\s+(\d+(\.\d+)?)\s*dB", raw_otsi)
        if m: results["q_factor"] = f"{m.group(1)}"

        # 4) Parse FEC blocks from raw_fec
        fec_live = re.search(
            r"otuc4-p\s+live.*?fec-ber\s+\S+\s+([0-9.eE-]+)",
            raw_fec, re.DOTALL
        )
        if fec_live:
            results["fec_ber_live"] = fec_live.group(1)

        fec_15 = re.search(r"fec-ber-mean\s+\S+\s+([0-9.eE-]+)", raw_fec)
        if fec_15:
            results["fec_ber_15min"] = fec_15.group(1)

        return results

