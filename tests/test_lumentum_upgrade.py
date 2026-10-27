"""The features ``Lumentum`` gained in 1.1.0: session options, new readers and writers."""

import pytest

import tcdona3.lumentum as lumentum_module
from tcdona3 import _netconf
from tcdona3.lumentum import Lumentum

import lumentum_data as data
from fakes import FakeNetconfManager, Reply, leaves, payload


class TransportError(Exception):
    """Named like ncclient's: a dropped session."""


class TimeoutExpiredError(Exception):
    """Named like ncclient's: a reply that did not come in time."""


@pytest.fixture
def managers(monkeypatch):
    """``manager.connect`` hands out the fakes of ``queue`` in turn (a new one when it is empty)."""
    queue, calls = [], []

    def connect(**kwargs):
        calls.append(kwargs)
        return queue.pop(0) if queue else FakeNetconfManager()

    monkeypatch.setattr(lumentum_module, "check_patch_owners", lambda patches: True)
    monkeypatch.setattr(lumentum_module.manager, "connect", connect)
    return queue, calls


@pytest.fixture
def roadm(managers):
    queue, _ = managers
    fake = FakeNetconfManager()
    queue.append(fake)
    return Lumentum("roadm_1"), fake


# ------------------------------------------------------------------------------------------------- session
def test_the_timeout_is_passed_to_connect_only_when_given(managers):
    _, calls = managers
    Lumentum("roadm_1")
    Lumentum("roadm_1", timeout=45)
    assert "timeout" not in calls[0] and calls[1]["timeout"] == 45


def test_by_default_the_manager_is_the_raw_one(roadm):
    device, fake = roadm
    assert device.m is fake


def test_reconnect_reopens_a_dropped_session_and_repeats_a_read(managers):
    queue, calls = managers
    first = FakeNetconfManager(fail_on={"get": TransportError("session closed")})
    second = FakeNetconfManager(replies={"get": Reply(data.connections(data.connection(1, 3, "1", "2")))})
    queue.extend([first, second])
    device = Lumentum("roadm_1", reconnect=True)
    assert isinstance(device.m, _netconf.ReconnectingSession)
    conns = device.wss_get_connections()
    assert conns["mux"]["conn-1"]["connection"] == 3
    assert len(calls) == 2 and calls[1] == calls[0]
    assert second.names() == ["get"]


def test_reconnect_resends_a_wss_edit_whose_reply_timed_out(managers):
    queue, _ = managers
    fake = FakeNetconfManager(fail_on={"dispatch": [TimeoutExpiredError("no reply")]})
    queue.append(fake)
    device = Lumentum("roadm_1", reconnect=True)
    device.set_mux_block(4)
    assert fake.names() == ["dispatch", "dispatch"]


def test_without_reconnect_a_timed_out_wss_edit_is_not_resent(roadm):
    device, fake = roadm
    fake.fail_on["dispatch"] = [TimeoutExpiredError("no reply")]
    with pytest.raises(RuntimeError):
        device.set_mux_block(4)
    assert fake.names() == ["dispatch"]


def test_reconnect_never_repeats_a_delete(managers):
    queue, _ = managers
    first = FakeNetconfManager(fail_on={"dispatch": TransportError("session closed")})
    queue.extend([first, FakeNetconfManager()])
    device = Lumentum("roadm_1", reconnect=True)
    with pytest.raises(ValueError):
        device.wss_delete_connection(1, "all")
    assert device.m.manager.names() == []


def test_close_and_context_manager(managers):
    queue, _ = managers
    fake = FakeNetconfManager()
    queue.append(fake)
    with Lumentum("roadm_1") as device:
        assert device.m is fake
    assert fake.closed
    device.close()


def test_close_ignores_a_session_already_gone(roadm):
    device, fake = roadm

    def gone():
        raise TransportError("session closed")
    fake.close_session = gone
    device.close()


def test_disable_als_reports_whether_the_device_accepted(roadm):
    device, fake = roadm
    assert device.disable_als() is True
    fake.fail_on["dispatch"] = RuntimeError("refused")
    assert device.disable_als() is False


# ------------------------------------------------------------------------------------------------- EDFA
@pytest.mark.parametrize("getter,leaf,value", [("get_maintenance_state", "maintenance-state", "out-of-service"),
                                               ("get_gain_switch_mode", "gain-switch-mode", "high-gain")])
def test_config_leaf_getters(roadm, getter, leaf, value):
    device, fake = roadm
    fake.replies["get_config"] = Reply(data.edfa_config_leaf(2, leaf, value))
    assert getattr(device, getter)(2) == value
    sent = leaves(payload(fake.calls_to("get_config")[0], "filter"))
    assert sent["dn"] == "ne=1;chassis=1;card=1;edfa=2" and leaf in sent


@pytest.mark.parametrize("setter,leaf,value", [("set_maintenance_state", "maintenance-state", "out-of-service"),
                                               ("set_gain_switch_mode", "gain-switch-mode", "low-gain")])
def test_config_leaf_setters_write_one_leaf(roadm, setter, leaf, value):
    device, fake = roadm
    getattr(device, setter)(1, value)
    call = fake.calls_to("edit_config")[0]
    assert call.kwargs["target"] == "running"
    assert leaves(payload(call)) == {"dn": "ne=1;chassis=1;card=1;edfa=1", leaf: value}


@pytest.mark.parametrize("setter,value", [("set_maintenance_state", "maintenance"),
                                          ("set_gain_switch_mode", "mid-gain")])
def test_config_leaf_setters_refuse_unknown_values(roadm, setter, value):
    device, fake = roadm
    with pytest.raises(ValueError):
        getattr(device, setter)(1, value)
    assert fake.calls == []


def test_a_config_leaf_write_error_is_a_runtime_error(roadm):
    device, fake = roadm
    fake.fail_on["edit_config"] = ValueError("refused")
    with pytest.raises(RuntimeError):
        device.set_maintenance_state(1, "in-service")


@pytest.mark.parametrize("method,module", [("get_mux_target_tilt", 1), ("get_demux_target_tilt", 2)])
def test_target_tilt_is_a_float(roadm, method, module):
    device, fake = roadm
    fake.replies["get_config"] = Reply(data.edfa_config_leaf(module, "target-gain-tilt", "-1.5"))
    assert getattr(device, method)() == -1.5
    fake.fail_on["get_config"] = RuntimeError("rpc error")
    assert getattr(device, method)() is None


def test_edfa_state_of_each_module(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.EDFAS)
    assert device.edfa_state(1)["input-power"] == "-5.0"
    assert device.edfa_state(2)["voas"]["voa"]["voa-attentuation"] == "0.5"


def test_line_port_strips_prefixes(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.PORTS)
    port = device.line_port()
    assert port["state"]["outvoa-actual-attenuation"] == "1.0"
    assert port["state"]["input-power"] == "-18.5"
    assert port["config"] == {}


def test_set_outvoa_attenuation(roadm):
    device, fake = roadm
    device.set_outvoa_attenuation(2.25)
    sent = leaves(payload(fake.calls_to("edit_config")[0]))
    assert sent == {"dn": "ne=1;chassis=1;card=1;port=3001", "outvoa-target-attenuation": "2.2"} or \
        sent == {"dn": "ne=1;chassis=1;card=1;port=3001", "outvoa-target-attenuation": "2.3"}


@pytest.mark.parametrize("value", [-0.5, float("nan"), float("inf")])
def test_set_outvoa_attenuation_refuses_invalid_values(roadm, value):
    device, fake = roadm
    with pytest.raises(ValueError):
        device.set_outvoa_attenuation(value)
    assert fake.calls == []


# --------------------------------------------------------------------------------------------- WSS / OCM
def test_connections_carry_the_number_they_were_created_with(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.connections(data.connection(2, 11, "1", "2"), data.connection(2, 12, "3", "4"),
                                                 data.connection(1, 5, "1", "2")))
    conns = device.wss_get_connections()
    assert [conns["demux"][k]["connection"] for k in ("conn-1", "conn-2")] == [11, 12]
    assert conns["mux"]["conn-1"]["connection"] == 5


def test_parallel_lists_of_connections_and_attenuations(roadm):
    device, fake = roadm
    device.wss_atten(2, 5201, [1, 2], [0.5, 1.5])
    text = payload(fake.calls_to("edit_config")[0])
    assert "module=2;connection=2</dn>" in text and "<attenuation>1.5</attenuation>" in text


def test_parallel_lists_must_have_the_same_length(roadm):
    device, fake = roadm
    with pytest.raises(ValueError):
        device.wss_atten(2, 5201, [1, 2], [0.5])
    assert fake.calls == []


def test_one_monitored_channel(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.monitored_channels(data.monitored(3101, 1, "-22.0", "191350.0")))
    assert device.wss_get_monitored_channels()["demux"]["mon-1"]["power"] == -22.0


def test_monitored_channel_rows_of_one_port(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.monitored_channels(
        data.monitored(6201, 1, "-20.5", "191350.0"), data.monitored(3101, 1, "-22.0", "191350.0")))
    rows = device.monitored_channel_rows(6201)
    assert rows == [{"start-freq": 191325.0, "end-freq": 191375.0, "power": -20.5, "valid-data": "true",
                     "entity-description": "CH1"}]


def test_get_frequency_in_hz(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.monitored_channels(data.monitored(6201, 1, "-20.5", "191350.0"),
                                                        data.monitored(6201, 2, "-20.5", "191400.0")))
    assert device.get_frequency() == [191350.0e9, 191400.0e9]


def test_the_login_can_come_from_the_environment(managers, monkeypatch):
    _, calls = managers
    monkeypatch.setenv("LUMENTUM_USERNAME", "<user>")
    monkeypatch.setenv("LUMENTUM_PASSWORD", "<password>")
    Lumentum("roadm_1")
    assert (calls[-1]["username"], calls[-1]["password"]) == ("<user>", "<password>")


def test_without_the_environment_the_login_is_the_default(managers, monkeypatch):
    _, calls = managers
    monkeypatch.delenv("LUMENTUM_USERNAME", raising=False)
    monkeypatch.delenv("LUMENTUM_PASSWORD", raising=False)
    Lumentum("roadm_1")
    assert (calls[-1]["username"], calls[-1]["password"]) == (lumentum_module.LUMENTUM_USERNAME,
                                                              lumentum_module.LUMENTUM_PASSWORD)
