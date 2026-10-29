"""Backward compatibility of ``tcdona3.yokogawa.YokogawaOSA``: the SCPI it writes and what it returns."""

import json

import pytest

import tcdona3.yokogawa as yokogawa_module
from tcdona3.yokogawa import YokogawaOSA

from fakes import FakeResourceManager, FakeVisaResource


@pytest.fixture
def instrument(monkeypatch):
    resource = FakeVisaResource({"*IDN?": "YOKOGAWA,AQ6370E,0,1.0", ":SENSe:BANDwidth?": "1.000E-10",
                                 ":CALCULATE:AUTO?": "1", ":TRACE:Y? TRA": "-50.0,-40.5,-60.25",
                                 ":TRACE:X? TRA": "1.5500E-06,1.5501E-06,1.5502E-06",
                                 ":stat:oper:even?": "1"})
    manager = FakeResourceManager(resource)
    monkeypatch.setattr(yokogawa_module.pyvisa, "ResourceManager", lambda *args: manager)
    monkeypatch.setattr(yokogawa_module, "check_patch_owners", lambda patches: True)
    return resource, manager


@pytest.fixture
def osa(instrument):
    resource, manager = instrument
    osa = YokogawaOSA("192.0.2.1")
    osa.connect()
    del resource.writes[:]
    del resource.queries[:]
    return osa, resource


def test_construction_does_not_connect(instrument):
    _, manager = instrument
    osa = YokogawaOSA("192.0.2.1", 10002)
    assert (osa.ip, osa.port, osa.osa) == ("192.0.2.1", 10002, None)
    assert manager.opened == []


def test_connect_opens_a_socket_resource_and_logs_in(instrument):
    resource, manager = instrument
    osa = YokogawaOSA("192.0.2.1")
    osa.connect()
    assert manager.opened[0][0] == "TCPIP::192.0.2.1::10001::SOCKET"
    assert osa.osa is resource
    assert (resource.read_termination, resource.write_termination) == ("\n", "\n")
    assert resource.queries[0] == 'open "anonymous"' and len(resource.queries) == 2
    assert resource.writes == ["CFORM1"]


def test_connect_refuses_an_unauthorised_user(instrument, monkeypatch):
    _, manager = instrument
    monkeypatch.setattr(yokogawa_module, "check_patch_owners", lambda patches: False)
    with pytest.raises(Exception, match="not authorized"):
        YokogawaOSA("192.0.2.1").connect()
    assert manager.opened == []


@pytest.mark.parametrize("call,expected", [
    (lambda o: o.set_peak_search(), ":CALCulate:MARKer:MAXimum"),
    (lambda o: o.set_resolution_bandwidth(), ":SENSe:BANDwidth 0.1NM"),
    (lambda o: o.set_resolution_bandwidth("0.02NM"), ":SENSe:BANDwidth 0.02NM"),
    (lambda o: o.set_attenuator(True), "ATTenuator ON"),
    (lambda o: o.set_attenuator(False), "ATTenuator OFF"),
    (lambda o: o.set_auto_measure(True), ":CALCULATE:AUTO ON"),
    (lambda o: o.set_center_wavelength(1550.12), ":DISPLAY:TRACE:X:CENTER 1550.120NM"),
    (lambda o: o.set_wavelength_center(1550.12), ":SENSE:WAVELENGTH:CENTER 1550.120NM"),
    (lambda o: o.set_auto_sweep_points(False), ":SENS:SWEeP:POINTS:AUTO OFF"),
    (lambda o: o.set_sweep_points(1001), ":SENSe:SWEep:SEGMent:POINts 1001"),
    (lambda o: o.set_sweep_mode("single"), ":INITiate:SMODe SINGLE"),
    (lambda o: o.set_sweep_mode("Repeat"), ":INITiate:SMODe REPEAT"),
    (lambda o: o.abort(), ":ABORt"),
    (lambda o: o.sweep(), "INIT:IMM"),
    (lambda o: o.store_memory(), ':MMEMORY:STORE:MEMORY 1,CSV,"test001",INTERNAL'),
    (lambda o: o.store_trace("t", "EXTERNAL"), ':MMEMORY:STORE:TRACE TRA,CSV,"t",EXTERNAL'),
    (lambda o: o.store_graphics(), ':MMEMORY:STORE:GRAPHICS COLOR,BMP,"test001",INTERNAL'),
])
def test_setters_write_their_command(osa, call, expected):
    device, resource = osa
    call(device)
    assert resource.writes == [expected]


def test_set_wavelength_range_writes_start_then_stop(osa):
    device, resource = osa
    device.set_wavelength_range(1530.0, 1565.5)
    assert resource.writes == [":SENSE:WAVELENGTH:START 1530.000NM", ":SENSE:WAVELENGTH:STOP 1565.500NM"]


def _number_nm(command):
    return float(command.split()[-1][:-2])


def test_set_wavelength_span_writes_the_span_in_nm(osa):
    device, resource = osa
    device.set_wavelength_span(10.0)
    assert resource.writes[0].startswith(":SENSE:WAVELENGTH:SPAN ") and _number_nm(resource.writes[0]) == 10.0


def test_set_wavelength_span_keeps_its_decimals(osa):
    device, resource = osa
    device.set_wavelength_span(0.25)
    assert _number_nm(resource.writes[0]) == 0.25


def test_set_sweep_mode_refuses_an_unknown_mode(osa):
    device, resource = osa
    with pytest.raises(ValueError):
        device.set_sweep_mode("fast")
    assert resource.writes == []


@pytest.mark.parametrize("method,query,answer", [
    ("identify", "*IDN?", "YOKOGAWA,AQ6370E,0,1.0"),
    ("get_resolution_bandwidth", ":SENSe:BANDwidth?", "1.000E-10"),
    ("get_auto_measure_status", ":CALCULATE:AUTO?", "1"),
    ("get_trace_data", ":TRACE:Y? TRA", "-50.0,-40.5,-60.25"),
    ("get_wavelength_data", ":TRACE:X? TRA", "1.5500E-06,1.5501E-06,1.5502E-06"),
])
def test_getters_return_the_raw_answer(osa, method, query, answer):
    device, resource = osa
    assert getattr(device, method)() == answer
    assert resource.queries == [query]


def test_trace_getters_take_the_trace_name(osa):
    device, resource = osa
    device.get_trace_data("TRB")
    device.get_wavelength_data(trace_name="TRC")
    assert resource.queries == [":TRACE:Y? TRB", ":TRACE:X? TRC"]


def test_wait_for_sweep_complete_starts_a_sweep_and_polls(osa):
    device, resource = osa
    answers = iter(["0", "0", "1"])
    resource.answers = lambda command: next(answers)
    device.wait_for_sweep_complete()
    assert resource.writes == ["*CLS; :init"]
    assert resource.queries == [":stat:oper:even?"] * 3


def test_wait_for_sweep_complete_reads_bit_zero(osa):
    device, resource = osa
    answers = iter(["16", "0", "1"])
    resource.answers = lambda command: next(answers)
    device.wait_for_sweep_complete()
    assert len(resource.queries) == 3


def test_save_trace_data_writes_json_and_figure(osa, tmp_path):
    device, _ = osa
    json_path, fig_path = tmp_path / "trace.json", tmp_path / "spectrum.png"
    wavelength, trace = device.save_trace_data(json_path=str(json_path), fig_path=str(fig_path))
    assert wavelength == [1.55e-06, 1.5501e-06, 1.5502e-06]
    assert trace == [-50.0, -40.5, -60.25]
    with open(str(json_path)) as handle:
        saved = json.load(handle)
    assert saved == {"wavelength": ["1.5500E-06", "1.5501E-06", "1.5502E-06"], "trace": ["-50.0", "-40.5", "-60.25"]}
    assert fig_path.exists()
