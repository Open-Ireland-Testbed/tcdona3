"""Backward compatibility of ``tcdona3.lumentum.Lumentum``: what it sends to the ROADM and what it returns.

Every test talks to a :class:`FakeNetconfManager`; a test marked ``xfail(strict=True)`` pins a known bug of
the driver: it fails today and must pass once the bug is fixed (the marker is then removed).
"""

import warnings

import pytest

import tcdona3.lumentum as lumentum_module
from tcdona3.lumentum import Lumentum

import lumentum_data as data
from fakes import FakeNetconfManager, Reply, leaves, payload


@pytest.fixture
def connect(monkeypatch):
    """Patches the authorisation check and ``manager.connect``; returns the fake and the connect calls."""
    fake = FakeNetconfManager()
    calls = []

    def connect(**kwargs):
        calls.append(kwargs)
        return fake

    monkeypatch.setattr(lumentum_module, "check_patch_owners", lambda patches: True)
    monkeypatch.setattr(lumentum_module.manager, "connect", connect)
    return fake, calls


@pytest.fixture
def roadm(connect):
    fake, _ = connect
    return Lumentum("roadm_1"), fake


def edits(fake):
    return [leaves(payload(call)) for call in fake.calls_to("edit_config")]


def dispatched(fake):
    return [leaves(payload(call, "rpc")) for call in fake.calls_to("dispatch")]


# ------------------------------------------------------------------------------------------------ construction
def test_module_constants_keep_their_values():
    assert lumentum_module.LUMENTUM_DEFAULT_WSS_LOSS == "4.0"
    assert lumentum_module.LUMENTUM_ZERO_WSS_LOSS == "0.0"
    assert (lumentum_module.LUMENTUM_MUX, lumentum_module.LUMENTUM_DEMUX) == (1, 2)
    assert lumentum_module.LUMENTUM_MUX_OUTPUT_PORT == 4201
    assert lumentum_module.LUMENTUM_DEMUX_INPUT_PORT == 5101
    assert lumentum_module.LUMENTUM_INSERVICE == "in-service"
    assert lumentum_module.LUMENTUM_CHANNEL_QUANTITY == 95
    assert list(lumentum_module.LUMENTUM_CHANNEL_ALL) == list(range(1, 96))
    assert lumentum_module.LUMENTUM_WSS_CHANNEL_FREQ_CENTER_LIST[0] == 191350.0
    assert lumentum_module.LUMENTUM_WSS_CHANNEL_FREQ_CENTER_LIST[-1] == 191350.0 + 94 * 50.0


def test_an_unknown_roadm_name_is_refused(connect):
    _, calls = connect
    with pytest.raises(ValueError):
        Lumentum("roadm_unknown")
    assert calls == []


def test_an_unauthorised_user_is_refused_before_connecting(connect, monkeypatch):
    _, calls = connect
    monkeypatch.setattr(lumentum_module, "check_patch_owners", lambda patches: False)
    with pytest.raises(Exception, match="not authorized"):
        Lumentum("roadm_1")
    assert calls == []


def test_the_authorisation_checks_the_line_patch_of_the_roadm(connect, monkeypatch):
    checked = []
    monkeypatch.setattr(lumentum_module, "check_patch_owners", lambda patches: checked.append(patches) or True)
    Lumentum("roadm_3")
    assert checked == [[("roadm_3_p1", "roadm_3_line")]]


def test_the_session_is_opened_on_the_mapped_address_port_830(connect):
    _, calls = connect
    Lumentum("roadm_2")
    assert len(calls) == 1
    assert calls[0]["host"] == lumentum_module.ip_map["roadm_2"]
    assert calls[0]["port"] == 830
    assert calls[0]["hostkey_verify"] is False
    assert {"username", "password"} <= set(calls[0])


def test_the_public_state_of_a_new_driver(roadm):
    device, fake = roadm
    assert device.m is fake
    assert device.device_name == "roadm_1"
    assert device.DEBUG is False
    assert device.edfa_info == {"booster": {}, "preamp": {}}
    assert device.wss_connections == {"mux": {}, "demux": {}}
    assert device.port_info == {}
    assert (device.mux_additional_attn, device.demux_additional_attn) == (0, 0)


# ----------------------------------------------------------------------------------------------------- EDFA
def test_disable_als_dispatches_on_the_booster_for_ten_seconds_by_default(roadm):
    device, fake = roadm
    device.disable_als()
    sent = dispatched(fake)[0]
    assert sent["dn"] == "ne=1;chassis=1;card=1;edfa=1"
    assert sent["timeout-period"] == "10"


def test_disable_als_swallows_an_rpc_error(roadm):
    device, fake = roadm
    fake.fail_on["dispatch"] = RuntimeError("rpc error")
    device.disable_als(30)


EDFA_FLOATS = ("target_power", "target_gain", "target_gain_tilt", "input_power", "output_power",
               "voa_input_power", "voa_output_power", "voa_attenuation")


def test_edfa_get_info_keeps_its_keys_types_and_deprecation(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.EDFAS)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        info = device.edfa_get_info()
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
    assert set(info) == {"booster", "preamp"}
    assert set(info["booster"]) == {"control_mode", "maintenance-state"} | set(EDFA_FLOATS)
    assert info["booster"]["control_mode"] == "constant-power"
    assert info["preamp"]["control_mode"] == "constant-gain"
    assert info["booster"]["target_gain"] == 15.0 and info["preamp"]["input_power"] == -20.0
    assert all(isinstance(info["booster"][key], float) for key in EDFA_FLOATS)
    assert info is device.edfa_info


def test_get_edfa_info_keys_and_values(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.EDFAS)
    info = device.get_edfa_info()
    assert set(info["preamp"]) == {"control_mode", "maintenance_state"} | set(EDFA_FLOATS)
    assert info["preamp"]["maintenance_state"] == "in-service"
    assert info["booster"]["voa_attenuation"] == 0.5
    assert fake.calls[-1].kwargs["filter"][0] == "subtree"


@pytest.mark.parametrize("method", ["edfa_get_info", "get_edfa_info"])
def test_an_edfa_info_rpc_error_is_a_runtime_error(roadm, method):
    device, fake = roadm
    fake.fail_on["get"] = ValueError("rpc error")
    with pytest.raises(RuntimeError):
        getattr(device, method)()


@pytest.mark.parametrize("method,module,leaf", [
    ("get_mux_target_gain", 1, "target-gain"), ("get_demux_target_gain", 2, "target-gain"),
    ("get_mux_target_power", 1, "target-power"), ("get_demux_target_power", 2, "target-power"),
])
def test_target_getters_read_running_config_of_their_module_as_float(roadm, method, module, leaf):
    device, fake = roadm
    fake.replies["get_config"] = Reply(data.edfa_config_leaf(module, leaf, "12.5"))
    value = getattr(device, method)()
    assert value == 12.5 and isinstance(value, float)
    call = fake.calls_to("get_config")[0]
    assert call.kwargs["source"] == "running"
    sent = leaves(payload(call, "filter"))
    assert sent["dn"] == "ne=1;chassis=1;card=1;edfa=%d" % module
    assert leaf in sent


@pytest.mark.parametrize("method", ["get_mux_target_gain", "get_demux_target_power"])
def test_target_getters_return_none_on_error(roadm, method):
    device, fake = roadm
    fake.fail_on["get_config"] = RuntimeError("rpc error")
    assert getattr(device, method)() is None


@pytest.mark.parametrize("method,module,leaf", [
    ("get_mux_edfa_input_power", 1, "input-power"), ("get_mux_edfa_output_power", 1, "output-power"),
    ("get_demux_edfa_input_power", 2, "input-power"), ("get_demux_edfa_output_power", 2, "output-power"),
])
def test_edfa_power_getters_read_state_of_their_module(roadm, method, module, leaf):
    device, fake = roadm
    fake.replies["get"] = Reply(data.edfa_state_leaf(module, leaf, "-3.25"))
    assert getattr(device, method)() == -3.25
    assert leaves(payload(fake.calls_to("get")[0], "filter"))["dn"] == "ne=1;chassis=1;card=1;edfa=%d" % module


def test_edfa_power_getter_returns_none_on_error(roadm):
    device, fake = roadm
    fake.fail_on["get"] = RuntimeError("rpc error")
    assert device.get_mux_edfa_input_power() is None


def test_debug_edfa_returns_the_parsed_reply(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.EDFAS)
    parsed = device.debug_edfa(DEBUG=False)
    assert len(parsed["data"]["edfas"]["edfa"]) == 2


@pytest.mark.parametrize("method,module,mode,leaf,value", [
    ("set_mux_constant_power", 1, "constant-power", "target-power", "12.0"),
    ("set_demux_constant_power", 2, "constant-power", "target-power", "12.0"),
    ("set_mux_constant_gain", 1, "constant-gain", "target-gain", "12.0"),
])
def test_control_mode_setters_take_the_module_out_of_service_then_configure_it(roadm, method, module, mode,
                                                                               leaf, value):
    device, fake = roadm
    getattr(device, method)(12.0, 0.5)
    first, second = edits(fake)
    dn = "ne=1;chassis=1;card=1;edfa=%d" % module
    assert first == {"dn": dn, "maintenance-state": "out-of-service"}
    assert second["dn"] == dn and second["maintenance-state"] == "in-service"
    assert second["control-mode"] == mode and second[leaf] == value and second["target-gain-tilt"] == "0.5"
    assert all(call.kwargs["target"] == "running" for call in fake.calls_to("edit_config"))


def test_set_demux_constant_gain_takes_the_preamp_out_of_service(roadm):
    device, fake = roadm
    device.set_demux_constant_gain(20.0)
    assert edits(fake)[0]["dn"] == "ne=1;chassis=1;card=1;edfa=2"


def test_set_demux_constant_gain_configures_the_preamp(roadm):
    device, fake = roadm
    device.set_demux_constant_gain(20.0)
    configured = edits(fake)[-1]
    assert configured["dn"] == "ne=1;chassis=1;card=1;edfa=2"
    assert configured["control-mode"] == "constant-gain" and configured["target-gain"] == "20.0"


@pytest.mark.parametrize("method,module,gain_mode", [
    ("set_mux_low_gain_mode", 1, "low-gain"), ("set_mux_high_gain_mode", 1, "high-gain"),
    ("set_demux_low_gain_mode", 2, "low-gain"), ("set_demux_high_gain_mode", 2, "high-gain"),
])
def test_gain_mode_setters_configure_their_module(roadm, method, module, gain_mode):
    device, fake = roadm
    getattr(device, method)()
    configured = edits(fake)[-1]
    assert configured["dn"] == "ne=1;chassis=1;card=1;edfa=%d" % module
    assert configured["gain-switch-mode"] == gain_mode and configured["control-mode"] == "constant-power"


@pytest.mark.parametrize("method", ["set_demux_low_gain_mode", "set_demux_high_gain_mode"])
def test_demux_gain_modes_take_the_preamp_out_of_service(roadm, method):
    device, fake = roadm
    getattr(device, method)()
    assert edits(fake)[0]["dn"] == "ne=1;chassis=1;card=1;edfa=2"


def test_edfa_config_constant_power_sequence(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.EDFAS)
    device.edfa_config("preamp", control_mode="constant-power", target_power=3.0, gain_switch_mode="high-gain")
    sent = edits(fake)
    assert sent[0] == {"dn": "ne=1;chassis=1;card=1;edfa=2", "maintenance-state": "out-of-service"}
    assert sent[1]["control-mode"] == "constant-power" and sent[1]["target-power"] == "3.0"
    assert sent[1]["gain-switch-mode"] == "high-gain"
    assert len(sent) == 2


def test_edfa_config_los_and_loo_add_their_steps(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.EDFAS)
    device.edfa_config("booster", control_mode="constant-gain", target_gain=17.0, los_shutdown="false",
                       optical_loo_threshold=-40.0)
    sent = edits(fake)
    assert len(sent) == 6
    assert sent[1]["target-gain"] == "17.0"
    assert sent[3]["los-shutdown"] == "false"
    assert sent[5]["optical-loo-threshold"] == "-40.0"


def test_edfa_config_refuses_an_unknown_module(roadm):
    device, _ = roadm
    with pytest.raises(Exception, match="Invalid edfa_module"):
        device.edfa_config("middle")


def test_edfa_config_raises_a_runtime_error_on_an_rpc_error(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.EDFAS)
    fake.fail_on["edit_config"] = RuntimeError("rpc error")
    with pytest.raises(RuntimeError):
        device.edfa_config("booster")


# ------------------------------------------------------------------------------------------------------ ports
def _check_ports(info, others=()):
    assert set(info) == {"3001", "4101", "5201"} | set(others)
    assert info["3001"] == {"entity-description": "Optical Line", "operational-state": "in-service",
                            "input-power": -18.5, "output-power": 9.5, "outvoa-actual-attenuation": 1.0}
    assert info["4101"]["input-power"] == -3.0 and "output-power" not in info["4101"]
    assert info["5201"]["output-power"] == -7.0 and "input-power" not in info["5201"]


def test_get_ports_info_shape(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.PORTS)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _check_ports(device.get_ports_info())


def test_ports_get_info_shape(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.PORTS)
    info = device.ports_get_info()
    _check_ports(info, others=["6201"])
    assert info["6201"] == {"entity-description": "Monitor", "operational-state": "in-service"}


@pytest.mark.parametrize("method", ["get_ports_info", "ports_get_info"])
def test_a_ports_rpc_error_is_a_runtime_error(roadm, method):
    device, fake = roadm
    fake.fail_on["get"] = ValueError("rpc error")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pytest.raises(RuntimeError):
            getattr(device, method)()


# ---------------------------------------------------------------------------------------- WSS connections
CONNECTION_KEYS = {"id", "connection-id", "start-freq", "end-freq", "attenuation", "blocked", "input-port",
                   "input-power", "input-valid-data", "output-port", "output-power", "output-valid-data"}


def test_one_mux_connection(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.connections(data.connection(1, 7, "193000.0", "193050.0")))
    conns = device.wss_get_connections()
    assert conns["demux"] == {}
    entry = conns["mux"]["conn-1"]
    assert set(entry) >= CONNECTION_KEYS
    assert entry["id"] == 1 and entry["connection-id"] == "CH7"
    assert (entry["start-freq"], entry["end-freq"], entry["attenuation"]) == (193000.0, 193050.0, 4.0)
    assert (entry["input-port"], entry["output-port"]) == (4101, 4201)
    assert entry["blocked"] == "false" and entry["input-valid-data"] == "true"
    assert (entry["input-power"], entry["output-power"]) == (-10.0, -14.0)


def test_several_connections_of_one_module_are_numbered_from_zero(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.connections(data.connection(2, 1, "1", "2"), data.connection(2, 2, "3", "4")))
    conns = device.wss_get_connections()
    assert [conns["demux"][name]["id"] for name in ("conn-1", "conn-2")] == [0, 1]
    assert conns["demux"]["conn-2"]["output-port"] == 5201


def test_mixed_connections_are_numbered_from_one_per_module(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.connections(
        data.connection(1, 1, "1", "2"), data.connection(2, 1, "1", "2"), data.connection(1, 2, "3", "4")))
    conns = device.wss_get_connections()
    assert [conns["mux"][k]["id"] for k in ("conn-1", "conn-2")] == [1, 2]
    assert conns["demux"]["conn-1"]["id"] == 1


def test_no_connections(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.connections())
    assert device.wss_get_connections() == {"mux": {}, "demux": {}}


def test_a_connections_rpc_error_is_a_runtime_error(roadm):
    device, fake = roadm
    fake.fail_on["get"] = ValueError("rpc error")
    with pytest.raises(RuntimeError):
        device.wss_get_connections()


def test_connection_power_lists(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.connections(data.connection(1, 1, "1", "2", pin="-9.0", pout="-13.0")))
    assert device.get_mux_connection_input_power() == [(1, -9.0)]
    assert device.get_mux_connection_output_power() == [(1, -13.0)]
    assert device.get_demux_connection_input_power() == []
    assert device.wss_get_connections_input_power("other") is None


def test_monitored_channels(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.monitored_channels(
        data.monitored(6201, 1, "-20.5", "191350.0"), data.monitored(3101, 1, "-22.0", "191350.0"),
        data.monitored(6201, 2, "-21.0", "191400.0")))
    channels = device.wss_get_monitored_channels()
    assert channels["mux"]["mon-2"] == {"id": 2, "power": -21.0, "frequency": 191400.0}
    assert channels["demux"]["mon-1"]["power"] == -22.0
    assert device.get_mux_monitored_power() == [(1, -20.5), (2, -21.0)]
    assert device.get_demux_monitored_power() == [(1, -22.0)]
    assert device.wss_get_monitored_power("other") is None


def test_no_monitored_channels_is_empty(roadm):
    device, fake = roadm
    fake.replies["get"] = Reply(data.monitored_channels())
    assert device.wss_get_monitored_channels() == {"mux": {}, "demux": {}}


# ------------------------------------------------------------------------------------------- WSS edits
def test_wss_add_connection_payload(roadm):
    device, fake = roadm
    device.wss_add_connection(1, 3, 193000.0, 193050.0, 4.0, "false", "in-service", 4105, 4201, "CH3")
    sent = dispatched(fake)[0]
    assert sent["dn"] == "ne=1;chassis=1;card=1;module=1;connection=3"
    assert (sent["start-freq"], sent["end-freq"], sent["attenuation"]) == ("193000.0", "193050.0", "4.0")
    assert sent["input-port-reference"] == "ne=1;chassis=1;card=1;port=4105"
    assert sent["output-port-reference"] == "ne=1;chassis=1;card=1;port=4201"
    assert sent["custom-name"] == "CH3" and sent["blocked"] == "false"


@pytest.mark.parametrize("args", [(1, 3, 1, 2, 0, "false", "in-service", 4101, 4202, "x"),
                                  (2, 3, 1, 2, 0, "false", "in-service", 5102, 5201, "x"),
                                  (3, 3, 1, 2, 0, "false", "in-service", 4101, 4201, "x")])
def test_wss_add_connection_refuses_wrong_ports(roadm, args):
    device, fake = roadm
    with pytest.raises(Exception):
        device.wss_add_connection(*args)
    assert fake.calls == []


def test_wss_add_connection_rpc_error_is_a_runtime_error(roadm):
    device, fake = roadm
    fake.fail_on["dispatch"] = ValueError("rpc error")
    with pytest.raises(RuntimeError):
        device.wss_add_connection(1, 3, 1, 2, 0, "false", "in-service", 4101, 4201, "x")


@pytest.mark.parametrize("method,module,blocked", [("set_mux_block", 1, "true"), ("set_mux_unblock", 1, "false"),
                                                   ("set_demux_block", 2, "true"),
                                                   ("set_demux_unblock", 2, "false")])
def test_block_setters(roadm, method, module, blocked):
    device, fake = roadm
    getattr(device, method)([4, 5])
    sent = dispatched(fake)
    assert [s["dn"] for s in sent] == ["ne=1;chassis=1;card=1;module=%d;connection=%d" % (module, c)
                                       for c in (4, 5)]
    assert all(s["blocked"] == blocked for s in sent)


def test_scalar_attenuation_is_dispatched(roadm):
    device, fake = roadm
    device.set_demux_atten(6, 2.5)
    assert dispatched(fake)[0] == {"dn": "ne=1;chassis=1;card=1;module=2;connection=6", "attenuation": "2.5"}


def test_scalar_attenuation_defaults_to_zero(roadm):
    device, fake = roadm
    device.set_mux_atten(6)
    assert dispatched(fake)[0]["attenuation"] == "0.0"


def test_a_list_of_channel_attenuation_pairs_is_one_edit(roadm):
    device, fake = roadm
    device.wss_atten(1, 4101, [(1, 1.5), (2, 3.0)])
    call = fake.calls_to("edit_config")[0]
    assert call.kwargs["target"] == "running"
    text = payload(call)
    assert text.count("<connection>") == 2
    assert "connection=1</dn>" in text and "<attenuation>3.0</attenuation>" in text


def test_attenuation_rpc_error_is_a_value_error(roadm):
    device, fake = roadm
    fake.fail_on["dispatch"] = RuntimeError("rpc error")
    with pytest.raises(ValueError):
        device.set_mux_atten(1, 1.0)


def test_block_status_list_turns_booleans_into_yang_booleans(roadm):
    device, fake = roadm
    device.set_mux_block_status([(1, True), (2, "false")])
    text = payload(fake.calls_to("edit_config")[0])
    assert "<blocked>true</blocked>" in text and "<blocked>false</blocked>" in text
    assert "module=1;connection=2" in text


def test_set_mux_port_offsets_the_port_number(roadm):
    device, fake = roadm
    device.set_mux_port(3, 5)
    assert dispatched(fake)[0]["input-port-reference"] == "ne=1;chassis=1;card=1;port=4105"


def test_wss_outport_on_a_list(roadm):
    device, fake = roadm
    device.wss_outport(2, [1, 2], 5203)
    assert [s["output-port-reference"] for s in dispatched(fake)] == ["ne=1;chassis=1;card=1;port=5203"] * 2


def test_set_demux_port_sets_the_output_port(roadm):
    device, fake = roadm
    device.set_demux_port(3, 2)
    assert dispatched(fake)[0]["output-port-reference"] == "ne=1;chassis=1;card=1;port=5202"


@pytest.mark.parametrize("connection_id,rpc,dn", [
    ("all", "remove-all-connections", "ne=1;chassis=1;card=1;module=2"),
    (7, "delete-connection", "ne=1;chassis=1;card=1;module=2;connection=7"),
])
def test_wss_delete_connection(roadm, connection_id, rpc, dn):
    device, fake = roadm
    device.wss_delete_connection(2, connection_id)
    text = payload(fake.calls_to("dispatch")[0], "rpc")
    assert rpc in text and leaves(text)["dn"] == dn


def test_wss_delete_connection_rpc_error_is_a_value_error(roadm):
    device, fake = roadm
    fake.fail_on["dispatch"] = RuntimeError("rpc error")
    with pytest.raises(ValueError):
        device.wss_delete_connection(1, "all")


# ------------------------------------------------------------------------------------------------- grids
def test_wss_connection_validates_and_prints_itself():
    conn = Lumentum.WSSConnection(1, "1", "in-service", "false", 4101, 4201, "1", "2", "4.00", "CH1")
    assert repr(conn).startswith("WSSConnection(wss_id=1, connection_id='1'")
    assert str(conn) == repr(conn)
    for bad in [(3, "1", "in-service", "false", 4101, 4201), (1, "1", "in-service", "false", 4100, 4201),
                (2, "1", "in-service", "false", 5101, 5221), (1, "1", "broken", "false", 4101, 4201),
                (1, "1", "in-service", "no", 4101, 4201)]:
        with pytest.raises(Exception):
            Lumentum.WSSConnection(*(bad + ("1", "2", "4.00", "CH1")))


def test_dwdm_grid_defaults(roadm):
    device, _ = roadm
    grid = device.wss_gen_connections_dwdm(1, 4101, 4201, open_channels=[1, 3],
                                           channel_additional_attenuations={3: 1.5})
    assert len(grid) == 95
    first, third = grid[0], grid[2]
    assert (first.connection_id, first.name, first.operation) == ("1", "CH1", "in-service")
    assert (first.start_freq, first.end_freq) == ("191325.0", "191375.0")
    assert first.attenuation == "4.00" and first.blocked == "false"
    assert grid[1].blocked == "true"
    assert third.attenuation == "5.50"


def test_make_grid_both_replaces_every_connection(roadm):
    device, fake = roadm
    device.make_grid("both", [2], True, 4103, 5204)
    assert [payload(c, "rpc").count("remove-all-connections") > 0 for c in fake.calls_to("dispatch")] == [True] * 2
    mux, demux = [payload(c) for c in fake.calls_to("edit_config")]
    assert mux.count("<connection>") == 95 and demux.count("<connection>") == 95
    assert "port=4103</input-port-reference>" in mux and "port=4201</output-port-reference>" in mux
    assert "port=5101</input-port-reference>" in demux and "port=5204</output-port-reference>" in demux
    assert "<attenuation>4.00</attenuation>" in mux


def test_make_grid_mux_only(roadm):
    device, fake = roadm
    device.make_grid(device="mux", open_channels=[1])
    assert len(fake.calls_to("dispatch")) == 1 and len(fake.calls_to("edit_config")) == 1


def test_apply_grids_map_ports(roadm):
    device, fake = roadm
    device.apply_mux_grid([(2, 193000.0, 193100.0, 3.0)])
    device.apply_demux_grid([(4, 193000.0, 193100.0, 1.0)])
    mux, demux = [payload(c) for c in fake.calls_to("edit_config")]
    assert "port=4102</input-port-reference>" in mux and "<attenuation>3.00</attenuation>" in mux
    assert "port=5204</output-port-reference>" in demux and "<custom-name>CH1</custom-name>" in demux


def test_wide_channels(roadm):
    device, _ = roadm
    wide = device.generate_wide_channel_demux((3, 4), 5202, loss=2.0, blocked=True)
    assert (wide.connection_id, wide.blocked, wide.output_port) == ("3", "true", 5202)
    assert (wide.start_freq, wide.end_freq, wide.attenuation) == ("191425.0", "191525.0", "2.00")
    grid = device.operator_flex_grid_mux_connections({(1, 2): 3}, open_channels=[1], channel_quantity=4)
    assert [(c.connection_id, c.input_port, c.blocked) for c in grid] == [
        ("1", 4103, "false"), ("3", 4101, "true"), ("4", 4101, "true")]
