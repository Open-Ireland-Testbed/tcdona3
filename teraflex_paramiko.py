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
        print("Sending [show rx]...")
        raw = self._send("show rx")
        return self._parse_pm(raw)

    @staticmethod
    def _clean_output(raw: str) -> str:
        lines = raw.splitlines()
        return "\n".join(lines[1:-1])

    @staticmethod
    def _parse_pm(raw: str) -> Dict[str, Any]:
        rx = re.search(r"Rx Power\s*:\s*([-\d.]+) dBm", raw)
        q  = re.search(r"Q[- ]?factor\s*:\s*([-\d.]+) dB", raw)
        return {
            "rx_power": float(rx.group(1)) if rx else None,
            "q_factor": float(q.group(1)) if q else None,
            "raw_output": raw,
        }

