"""``Lumentum.fetch_all_telemetry`` and the target-gain check of constant-gain mode (added in 1.2.0)."""

import pytest

import tcdona3.lumentum as lumentum_module
from tcdona3.lumentum import Lumentum

import lumentum_data as data
from fakes import FakeNetconfManager, Reply

CONNECTIONS = data.connections(data.connection(1, 7, "191350.0", "191400.0"),
                               data.connection(2, 3, "191350.0", "191400.0"))
MONITORED = data.monitored_channels(data.monitored(6201, 1, "-5.0", "191375.0"),
                                    data.monitored(3101, 1, "-7.0", "191375.0"))


def _reply_to_get(call):
    subtree = str(call.kwargs.get("filter"))
    if "monitored-channel" in subtree:
        return Reply(MONITORED)
    if "connection" in subtree:
        return Reply(CONNECTIONS)
    return Reply(data.EDFAS)


@pytest.fixture
def connect(monkeypatch):
    """``manager.connect`` returns one fake answering the three telemetry gets; returns (fake, connect calls)."""
    fake = FakeNetconfManager(replies={"get": _reply_to_get})
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


def edit_count(fake):
    return len(fake.calls_to("edit_config"))


# ------------------------------------------------------------------------------------------- telemetry
def test_fetch_all_reads_the_three_sources_as_their_own_methods_do(roadm):
    device, fake = roadm
    connections, monitored, edfas = device.fetch_all_telemetry()
    assert len(fake.calls_to("get")) == 3
    assert connections == device.wss_get_connections()
    assert monitored == device.wss_get_monitored_channels()
    assert edfas == device.get_edfa_info()


def test_fetch_keeps_the_connection_number(roadm):
    device, _ = roadm
    connections, _, _ = device.fetch_all_telemetry("connections")
    assert connections["mux"]["conn-1"]["connection"] == 7
    assert connections["demux"]["conn-1"]["connection"] == 3


@pytest.mark.parametrize("fetch,expected", [
    ("monitor", (False, True, False)),
    ("monitored-channels", (False, True, False)),
    ("EDFAS", (False, False, True)),
    (["Connections", "edfa"], (True, False, True)),
    ("ALL", (True, True, True)),
    (["all"], (True, True, True)),
])
def test_fetch_reads_only_what_is_asked_and_returns_none_for_the_rest(roadm, fetch, expected):
    device, fake = roadm
    device.fetch_all_telemetry("all")  # caches filled: a part not asked for must still come back as None
    before = len(fake.calls_to("get"))
    result = device.fetch_all_telemetry(fetch)
    assert tuple(part is not None for part in result) == expected
    assert len(fake.calls_to("get")) - before == sum(expected)


@pytest.mark.parametrize("fetch", ["power", ["edfa", "ocm"], []])
def test_fetch_refuses_an_unknown_source_before_any_rpc(roadm, fetch):
    device, fake = roadm
    with pytest.raises(ValueError):
        device.fetch_all_telemetry(fetch)
    assert fake.calls_to("get") == []


# ------------------------------------------------------------------------------------------- target gain
@pytest.mark.parametrize("method,gain", [
    ("set_mux_constant_gain", 4.0), ("set_mux_constant_gain", 5.5), ("set_mux_constant_gain", 27.0),
    ("set_demux_constant_gain", 10.0), ("set_demux_constant_gain", 30.5),
])
def test_gains_within_the_device_range_are_written(roadm, method, gain):
    device, fake = roadm
    getattr(device, method)(gain)
    assert edit_count(fake) == 2


@pytest.mark.parametrize("method,gain", [
    ("set_mux_constant_gain", 3.9), ("set_mux_constant_gain", 27.5), ("set_demux_constant_gain", 9.5),
    ("set_demux_constant_gain", 31.0), ("set_mux_constant_gain", float("nan")), ("set_mux_constant_gain", "x"),
])
def test_a_gain_outside_the_device_range_is_refused_before_the_amplifier_is_taken_out_of_service(
        roadm, method, gain):
    device, fake = roadm
    with pytest.raises(ValueError):
        getattr(device, method)(gain)
    assert edit_count(fake) == 0


def test_edfa_config_checks_the_gain_only_in_constant_gain_mode(roadm):
    device, fake = roadm
    with pytest.raises(ValueError, match="booster target_gain"):
        device.edfa_config("booster", control_mode="constant-gain", target_gain=2.0)
    assert edit_count(fake) == 0
    device.edfa_config("booster", control_mode="constant-power", target_power=10.0)
    assert edit_count(fake) == 2


def test_narrower_gain_limits_are_an_option(connect):
    fake, _ = connect
    device = Lumentum("roadm_1", gain_limits={"booster": (7.0, 27.0), "preamp": (11.0, 30.0)})
    with pytest.raises(ValueError, match="between 7.0 and 27.0"):
        device.set_mux_constant_gain(5.5)
    with pytest.raises(ValueError, match="between 11.0 and 30.0"):
        device.set_demux_constant_gain(30.5)
    assert edit_count(fake) == 0
    device.set_mux_constant_gain(7.0)
    assert edit_count(fake) == 2


def test_gain_limits_may_name_one_module(connect):
    device = Lumentum("roadm_1", gain_limits={"preamp": (11.0, 30.0)})
    assert device.gain_limits == {"booster": (4.0, 27.0), "preamp": (11.0, 30.0)}


@pytest.mark.parametrize("gain_limits", [
    {"booster": (3.0, 27.0)}, {"preamp": (11.0, 31.0)}, {"booster": (20.0, 10.0)}, {"amp": (5.0, 10.0)},
    {"booster": (7.0,)}, {"booster": "high"},
])
def test_gain_limits_outside_the_device_range_are_refused_before_connecting(connect, gain_limits):
    _, calls = connect
    with pytest.raises(ValueError):
        Lumentum("roadm_1", gain_limits=gain_limits)
    assert calls == []
