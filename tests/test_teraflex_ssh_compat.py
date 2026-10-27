"""Backward compatibility of ``tcdona3.teraflex_paramiko.TeraflexSSH`` (TeraFlex over SSH, CLI)."""

import pytest

import tcdona3.teraflex_paramiko as tf_module
from tcdona3.teraflex_paramiko import TeraflexSSH

import teraflex_data as data
from fakes import FakeShell


class FakeClient(object):
    instances = []

    def __init__(self, shell, failure=None):
        self.shell, self.failure = shell, failure
        self.connected = None
        self.closed = False
        self.policy = None
        FakeClient.instances.append(self)

    def set_missing_host_key_policy(self, policy):
        self.policy = policy

    def connect(self, host, **kwargs):
        if self.failure is not None:
            raise self.failure
        self.connected = (host, kwargs)

    def invoke_shell(self, *args, **kwargs):
        return self.shell

    def get_transport(self):
        return None

    def close(self):
        self.closed = True


@pytest.fixture
def ssh(monkeypatch):
    """Installs a fake SSH client over a shell answering ``state['outputs']``."""
    state = {"outputs": {}, "failure": None}
    FakeClient.instances = []

    def client():
        shell = FakeShell(data.device(state["outputs"]))
        state["shell"] = shell
        return FakeClient(shell, state["failure"])

    monkeypatch.setattr(tf_module.paramiko, "SSHClient", client)
    return state


def new(ssh, name="tf_1", **outputs):
    ssh["outputs"].update(outputs)
    return TeraflexSSH(name, "<user>", "<password>", timeout=2)


def sent_commands(ssh):
    return [" ".join(text.split()) for text in ssh["shell"].sent]


def test_device_map_keeps_its_names_and_line_ports():
    assert {name: entry["line_port"] for name, entry in tf_module.DEVICE_MAP.items()} == {
        "tf_1": "1/1/n1", "tf_2": "1/1/n2", "tf_3": "1/2/n1", "tf_4": "1/2/n2"}


def test_an_unknown_device_is_refused(ssh):
    with pytest.raises(ValueError):
        new(ssh, "tf_9")
    assert FakeClient.instances == []


def test_the_session_is_opened_with_the_given_credentials(ssh):
    tf = new(ssh, "tf_3")
    host, kwargs = FakeClient.instances[0].connected
    assert host == tf_module.DEVICE_MAP["tf_3"]["ip"]
    assert (kwargs["username"], kwargs["password"], kwargs["timeout"]) == ("<user>", "<password>", 2)
    assert kwargs["look_for_keys"] is False and kwargs["allow_agent"] is False
    assert (tf.host, tf.line_port, tf.timeout) == (host, "1/2/n1", 2)


def test_a_refused_login_is_an_exception_naming_the_host(ssh):
    ssh["failure"] = tf_module.paramiko.AuthenticationException("refused")
    with pytest.raises(Exception, match="Authentication failed"):
        new(ssh)


def test_read_pm_data(ssh):
    tf = new(ssh, **{"opt-phy pm current": data.PM_OPT_PHY})
    pm = tf.read_pm_data()
    assert set(pm) == {"rx_power", "tx_power", "raw_output"}
    assert (pm["rx_power"], pm["tx_power"]) == (-12.5, 0.5)
    assert "opt-phy     live" in pm["raw_output"]
    assert sent_commands(ssh) == ["show interface 1/1/n1 opt-phy pm current"]


def test_read_pm_data_without_a_live_block(ssh):
    tf = new(ssh, **{"opt-phy pm current": "no pm\n"})
    pm = tf.read_pm_data()
    assert (pm["rx_power"], pm["tx_power"]) == (None, None)
    assert "no pm" in pm["raw_output"]


def test_get_current_config_drops_the_echo_and_the_prompt(ssh):
    tf = new(ssh, **{"show running-config": "line one\nline two\n"})
    assert tf.get_current_config() == "line one\nline two"


def outputs(otsi=None, otuc_layer="otuc4-p"):
    return {"opt-phy pm current": data.PM_OPT_PHY, "otsia otsi 1 pm current": otsi or data.pm_otsi(),
            otuc_layer + " pm current": data.pm_otuc(otuc_layer)}


def test_return_current_config_keys_and_string_values(ssh):
    tf = new(ssh, **outputs())
    result = tf.return_current_config()
    assert result == {"rx_power": "-12.5", "tx_power": "0.5", "rx_power_15min": "-12.9", "tx_power_15min": "0.4",
                      "snr": "18.20", "osnr": "30.10", "q_factor": "9.40",
                      "fec_ber_live": "1.2E-04", "fec_ber_15min": "1.5E-04"}
    assert sent_commands(ssh) == ["show interface 1/1/n1 opt-phy pm current",
                                  "show interface 1/1/n1/ot400 otsia otsi 1 pm current",
                                  "show interface 1/1/n1/ot400 otuc4-p pm current"]


def test_return_current_config_on_another_line_port(ssh):
    tf = new(ssh, "tf_2", **outputs())
    tf.return_current_config("ot400")
    assert sent_commands(ssh)[1] == "show interface 1/1/n2/ot400 otsia otsi 1 pm current"


def test_snr_is_not_read_from_the_osnr_row(ssh):
    tf = new(ssh, **outputs(data.pm_otsi(osnr_first=True)))
    assert tf.return_current_config()["snr"] == "18.20"


def test_the_live_value_wins_over_an_earlier_interval(ssh):
    tf = new(ssh, **outputs(data.pm_otsi(stale_first=True)))
    assert tf.return_current_config()["q_factor"] == "9.40"


def test_a_negative_snr_is_read(ssh):
    tf = new(ssh, **outputs(data.pm_otsi(snr="-1.50")))
    assert tf.return_current_config()["snr"] == "-1.50"


def test_the_fec_layer_follows_the_logical_interface(ssh):
    tf = new(ssh, **outputs(otuc_layer="otuc2-p"))
    result = tf.return_current_config("ot200")
    assert sent_commands(ssh)[2] == "show interface 1/1/n1/ot200 otuc2-p pm current"
    assert result["fec_ber_live"] == "1.2E-04"


def test_close_closes_the_client(ssh):
    tf = new(ssh)
    tf.close()
    assert FakeClient.instances[0].closed
