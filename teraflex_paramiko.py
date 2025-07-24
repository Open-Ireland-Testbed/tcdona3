import paramiko
import time
import re
from typing import Dict, Any

PROMPT_PATTERN = r"[>#]\s*$"

DEVICE_MAP = {
    "tf_1": {"ip": "10.10.10.91", "line_port": "1/1/n1"},
    "tf_2": {"ip": "10.10.10.92", "line_port": "1/1/n2"},
    "tf_3": {"ip": "10.10.10.93", "line_port": "1/2/n1"},
    "tf_4": {"ip": "10.10.10.94", "line_port": "1/2/n2"},
}

class TeraflexSSH:
    def __init__(self, tf_name: str, username: str, password: str, timeout: int = 10):
        print("Teraflex Paramiko Initialised")
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
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.host, username=self.username,
                            password=self.password, timeout=self.timeout,
                            look_for_keys=False, allow_agent=False)
        self.channel = self.client.invoke_shell()
        time.sleep(1)
        if self.channel.recv_ready():
            self.channel.recv(65535)

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


    @staticmethod
    def parse_rx_summary(self, logical: str = "ot400") -> Dict[str, Any]:
        """
        SSH to the device, pull PM summary for Rx/Tx, Q‑factor, OSNR, SNR, FEC BER,
        then parse only the 'live' block.
        """
        full_if = f"{self.line_port}/{logical}"
        commands = [
            f"show interface {self.line_port} opt-phy pm current",    # Tx/Rx power
            f"show interface {full_if} otsia otsi 1 pm current",      # Q‑factor, OSNR, SNR
            f"show interface {full_if} otuc4-p pm current"            # FEC BER
        ]

        # Run each command over SSH, concatenate outputs
        raw = "\n".join(self._send(cmd) for cmd in commands)

        # Extract only the “live” PM block
        live_match = re.search(
            r"mon-entity\s+interval\s+pm-profile.*?opt-phy\s+live.*?(?=\r\n\r\n|\Z)",
            raw,
            re.DOTALL
        )
        block = live_match.group(0) if live_match else raw

        # Initialize results
        results = {
            "rx_power":      None,
            "tx_power":      None,
            "snr":           None,
            "osnr":          None,
            "q_factor":      None,
            "fec_ber_live":  None,
            "fec_ber_15min": None
        }

        # Parse metrics
        m = re.search(r"opt-rx-pwr\s+\S+\s+(-?\d+(\.\d+)?)\s*dBm", block)
        if m: results["rx_power"] = m.group(1) + " dBm"

        m = re.search(r"opt-tx-pwr\s+\S+\s+(-?\d+(\.\d+)?)\s*dBm", block)
        if m: results["tx_power"] = m.group(1) + " dBm"

        m = re.search(r"signal-to-noise-ratio\s+\S+\s+(\d+(\.\d+)?)", block)
        if m: results["snr"] = m.group(1) + " dB"

        m = re.search(r"optical-signal-to-noise-ratio\s+\S+\s+(\d+(\.\d+)?)", block)
        if m: results["osnr"] = m.group(1) + " dB"

        m = re.search(r"q-factor\s+\S+\s+(\d+(\.\d+)?)", block)
        if m: results["q_factor"] = m.group(1) + " dB"

        m = re.search(r"fec-ber\s+\S+\s+([0-9.eE-]+)(?!.*15min)", block)
        if m: results["fec_ber_live"] = m.group(1)

        m = re.search(r"fec-ber-mean\s+\S+\s+([0-9.eE-]+)", block)
        if m: results["fec_ber_15min"] = m.group(1)

        return results

