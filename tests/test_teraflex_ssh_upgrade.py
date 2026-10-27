"""The features ``TeraflexSSH`` gained in 1.1.0: a prompt-aware reader and the generic PM parser."""

import pytest

import tcdona3.teraflex_paramiko as tf_module
from tcdona3.teraflex_paramiko import TeraflexSSH, find_pm, otuc_layer, parse_pm_output

import teraflex_data as data
from fakes import FakeShell
from test_teraflex_ssh_compat import FakeClient, outputs


@pytest.fixture
def shell(monkeypatch):
    """Installs a fake SSH client; ``state['make']`` builds its shell."""
    state = {"make": lambda: FakeShell(data.device({}))}
    FakeClient.instances = []

    def client():
        state["shell"] = state["make"]()
        return FakeClient(state["shell"])

    monkeypatch.setattr(tf_module.paramiko, "SSHClient", client)
    return state


def test_the_host_can_override_the_device_map(shell):
    TeraflexSSH("tf_2", "<user>", "<password>", host="192.0.2.7")
    assert FakeClient.instances[0].connected[0] == "192.0.2.7"


def test_a_prompt_split_across_chunks_is_found(shell):
    shell["make"] = lambda: FakeShell(data.device({"opt-phy pm current": data.PM_OPT_PHY}), chunk=7)
    tf = TeraflexSSH("tf_1", "<user>", "<password>", timeout=2)
    assert tf.read_pm_data()["rx_power"] == -12.5


def test_a_pager_is_answered_and_removed(shell):
    class PagedShell(FakeShell):
        def send(self, text):
            if text == " ":
                self.sent.append(text)
                self.buffer += ("  opt-tx-pwr  nend  0.5 dBm\r\n\r\n" + data.PROMPT).encode()
                return 1
            self.sent.append(text)
            head = data.PM_OPT_PHY.split("  opt-tx-pwr")[0].replace("\n", "\r\n")
            self.buffer += (text.strip() + "\r\n" + head + "--More--").encode()
            return len(text)

    shell["make"] = lambda: PagedShell({})
    tf = TeraflexSSH("tf_1", "<user>", "<password>", timeout=2)
    raw = tf._send("show interface 1/1/n1 opt-phy pm current")
    assert "--More--" not in raw and "opt-tx-pwr" in raw
    assert shell["shell"].sent[-1] == " "


def test_a_channel_closed_by_the_device_ends_the_read(shell, monkeypatch):
    class ClosingShell(FakeShell):
        def send(self, text):
            self.sent.append(text)
            self.buffer += b"partial output"
            self.closed = True
            return len(text)

        def recv_ready(self):
            return bool(self.buffer)

    shell["make"] = lambda: ClosingShell({})
    tf = TeraflexSSH("tf_1", "<user>", "<password>", timeout=2)
    clock = iter(range(0, 10 ** 6))
    monkeypatch.setattr(tf_module.time, "time", lambda: next(clock) * 0.001)
    assert tf._send("show x") == "partial output"


def test_close_never_raises_and_context_manager(shell):
    with TeraflexSSH("tf_1", "<user>", "<password>") as tf:
        pass
    assert FakeClient.instances[0].closed and shell["shell"].closed

    def broken():
        raise OSError("gone")
    tf.client.close = broken
    tf.close()


# --------------------------------------------------------------------------------------------------- parser
def test_parse_pm_output_tables_and_values():
    tables = parse_pm_output(data.PM_OPT_PHY.replace("\n", "\r\n"), "opt-phy")
    assert [(t["table"], t["interval"], t["suspect"]) for t in tables] == [
        ("current:IFTFnw", "live", False), ("current:Power", "15min", True)]
    assert tables[0]["elapsed_s"] == 86400 + 2 * 3600 + 3 * 60 + 4
    pms = tables[0]["pms"]
    assert (pms["opt-tx-pwr"]["value"], pms["opt-tx-pwr"]["unit"]) == (0.5, "dBm")
    assert pms["opt-rx-pwr"]["value"] == -12.5


def test_not_available_is_none_and_live_wins():
    tables = parse_pm_output(data.pm_otsi(stale_first=True), "otsi")
    assert tables[1]["pms"]["carrier-frequency-offset"]["value"] is None
    hit = find_pm(tables, "otsi", r"^q-factor$")
    assert (hit["value"], hit["interval"], hit["raw"]) == (9.4, "live", "9.40 dB")


def test_otuc_layer():
    assert otuc_layer("OT200") == "otuc2-p" and otuc_layer("unknown") == "otuc4-p"


def test_get_qot_snapshot(shell):
    shell["make"] = lambda: FakeShell(data.device(outputs()))
    tf = TeraflexSSH("tf_1", "<user>", "<password>", timeout=2)
    snapshot = tf.get_qot_snapshot()
    assert (snapshot["tx_power"], snapshot["rx_power_total"]) == (0.5, -12.5)
    assert (snapshot["snr"], snapshot["osnr"], snapshot["q_factor"]) == (18.2, 30.1, 9.4)
    assert (snapshot["fec_ber_live"], snapshot["fec_ber_mean"]) == (1.2e-4, 1.5e-4)
    assert snapshot["cfo"] is None and snapshot["units"]["snr"] == "dB"
    assert snapshot["source"]["q_factor"] == "otsi:current:QualityTF:q-factor (live)"
    assert (snapshot["port"], snapshot["logical_type"]) == ("1/1/n1", "ot400")


def test_get_pm_of_one_layer(shell):
    shell["make"] = lambda: FakeShell(data.device(outputs(otuc_layer="otuc2-p")))
    tf = TeraflexSSH("tf_1", "<user>", "<password>", timeout=2)
    tables = tf.get_pm("otuc", "ot200")
    assert [t["interval"] for t in tables] == ["live", "15min"]
    assert " ".join(shell["shell"].sent[-1].split()) == "show interface 1/1/n1/ot200 otuc2-p pm current"


def test_get_current_config_falls_back_to_the_port_views(shell):
    """A TeraFlex CLI without ``show running-config`` answers it with an error: the port views come instead."""
    shell["make"] = lambda: FakeShell(data.device({
        "running-config": "Error(400): Wrong command syntax. Unknown token(s): 'running-config'.\n",
        "optical-channel-configuration": "frequency 193.1\n", "otsi 1": "otsi view\n",
        "opt-phy": "opt-phy view\n", "ot400": "logical view\n", "1/1/n1": "port view\n"}))
    tf = TeraflexSSH("tf_1", "<user>", "<password>", timeout=2)
    config = tf.get_current_config()
    assert "Error(" not in config
    assert config.splitlines() == [
        "show interface 1/1/n1", "port view", "show interface 1/1/n1 opt-phy", "opt-phy view",
        "show interface 1/1/n1/ot400", "logical view", "show interface 1/1/n1/ot400 otsia otsi 1", "otsi view",
        "show interface 1/1/n1/ot400 otsia otsi 1 optical-channel-configuration", "frequency 193.1"]


def test_get_current_config_follows_the_logical_interface(shell):
    shell["make"] = lambda: FakeShell(data.device({"running-config": "Error(400): unknown\n"}))
    tf = TeraflexSSH("tf_2", "<user>", "<password>", timeout=2)
    tf.get_current_config("ot200")
    assert "show interface 1/1/n2/ot200 otsia otsi 1" in [" ".join(t.split()) for t in shell["shell"].sent]
