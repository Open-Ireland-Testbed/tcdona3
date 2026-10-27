"""The typed methods ``YokogawaOSA`` gained in 1.1.0: validated setters, nm readers, read_trace."""

import numpy as np
import pytest

import tcdona3.yokogawa as yokogawa_module
from tcdona3.yokogawa import YokogawaOSA

from fakes import FakeResourceManager, FakeVisaResource


@pytest.fixture
def instrument(monkeypatch):
    resource = FakeVisaResource({})
    manager = FakeResourceManager(resource)
    monkeypatch.setattr(yokogawa_module.pyvisa, "ResourceManager", lambda *args: manager)
    monkeypatch.setattr(yokogawa_module, "check_patch_owners", lambda patches: True)
    return resource, manager


@pytest.fixture
def osa(instrument):
    resource, _ = instrument
    device = YokogawaOSA("192.0.2.1")
    device.connect()
    del resource.writes[:]
    del resource.queries[:]
    return device, resource


def test_timeout_and_login_options(instrument):
    resource, _ = instrument
    device = YokogawaOSA("192.0.2.1", timeout_ms=30000, user="<user>", password="<password>")
    device.connect()
    assert resource.timeout == 30000
    assert resource.queries == ['open "<user>"', "<password>"]


def test_close_and_context_manager(instrument):
    resource, manager = instrument
    with YokogawaOSA("192.0.2.1") as device:
        assert device.osa is resource
    assert resource.closed and manager.closed and device.osa is None
    device.close()


@pytest.mark.parametrize("call,expected", [
    (lambda o: o.set_span(12.5), ":SENSe:WAVelength:SPAN 12.500NM"),
    (lambda o: o.set_start_wavelength(1530), ":SENSe:WAVelength:STARt 1530.000NM"),
    (lambda o: o.set_stop_wavelength("1565.25"), ":SENSe:WAVelength:STOP 1565.250NM"),
    (lambda o: o.set_resolution(0.02), ":SENSe:BANDwidth:RESolution 0.020NM"),
    (lambda o: o.set_resolution_correction(True), ":SENSe:SETTing:CORRection ON"),
    (lambda o: o.set_sensitivity("HIGH1"), ":SENSe:SENSe HIGH1"),
    (lambda o: o.set_sensitivity("normal"), ":SENSe:SENSe NORMal"),
    (lambda o: o.set_chopper("switch"), ":SENSe:CHOPper SWITch"),
    (lambda o: o.set_sweep_speed("2x"), ":SENSe:SWEep:SPEed 2x"),
    (lambda o: o.set_average_count(10), ":SENSe:AVERage:COUNt 10"),
    (lambda o: o.set_sampling_points(20001), ":SENSe:SWEep:POINts 20001"),
    (lambda o: o.set_sampling_step(0.002), ":SENSe:SWEep:STEP 0.0020NM"),
    (lambda o: o.set_smoothing(False), ":SENSe:SETTing:SMOothing OFF"),
    (lambda o: o.set_level_offset(1.5), ":SENSe:CORRection:LEVel:SHIFt 1.500DB"),
    (lambda o: o.set_reference_level(-10.0), ":DISPlay:TRACe:Y1:SCALe:RLEVel -10.0DBM"),
    (lambda o: o.set_wavelength_offset(0.01), ":SENSe:CORRection:WAVelength:SHIFt 0.010NM"),
    (lambda o: o.set_wavelength_medium("vacuum"), ":SENSe:CORRection:RVELocity:MEDium VACuum"),
    (lambda o: o.set_trace_attribute("max"), ":TRACe:ATTRibute:TRA MAX"),
    (lambda o: o.sweep_stop(), ":ABORt"),
])
def test_typed_setters(osa, call, expected):
    device, resource = osa
    call(device)
    assert resource.writes == [expected]


def test_sweep_single_and_repeat(osa):
    device, resource = osa
    device.sweep_single()
    device.sweep_repeat()
    assert resource.writes == [":INITiate:SMODe SINGLE", ":init", ":INITiate:SMODe REPEAT", ":init"]


@pytest.mark.parametrize("call", [
    lambda o: o.set_span(2000), lambda o: o.set_start_wavelength(500), lambda o: o.set_stop_wavelength("x"),
    lambda o: o.set_resolution(0.03), lambda o: o.set_resolution_correction("on"),
    lambda o: o.set_sensitivity("ultra"), lambda o: o.set_average_count(0), lambda o: o.set_average_count(2.5),
    lambda o: o.set_sampling_points(50), lambda o: o.set_sampling_step(0.0005),
    lambda o: o.set_sampling_step(0.0015), lambda o: o.set_level_offset(float("nan")),
    lambda o: o.set_wavelength_medium("water"), lambda o: o.set_trace_attribute("hold"),
])
def test_invalid_values_are_refused_before_writing(osa, call):
    device, resource = osa
    with pytest.raises(ValueError):
        call(device)
    assert resource.writes == []


@pytest.mark.parametrize("method,query,answer,expected", [
    ("get_center_wavelength", ":SENSe:WAVelength:CENTer?", "1.5500E-06", 1550.0),
    ("get_span", ":SENSe:WAVelength:SPAN?", "1.0000E-08", 10.0),
    ("get_resolution", ":SENSe:BANDwidth:RESolution?", "2.000E-11", 0.02),
    ("get_resolution_correction", ":SENSe:SETTing:CORRection?", "1", True),
    ("get_sensitivity", ":SENSe:SENSe?", "3", "high1"),
    ("get_chopper", ":SENSe:CHOPper?", "0", "off"),
    ("get_sweep_speed", ":SENSe:SWEep:SPEed?", "1", "2x"),
    ("get_sweep_mode", ":INITiate:SMODe?", "2", "repeat"),
    ("get_average_count", ":SENSe:AVERage:COUNt?", "10", 10),
    ("get_sampling_auto", ":SENSe:SWEep:POINts:AUTO?", "0", False),
    ("get_sampling_points", ":SENSe:SWEep:POINts?", "20001", 20001),
    ("get_smoothing", ":SENSe:SETTing:SMOothing?", "1", True),
    ("get_level_offset", ":SENSe:CORRection:LEVel:SHIFt?", "1.5", 1.5),
    ("get_reference_level", ":DISPlay:TRACe:Y1:SCALe:RLEVel?", "-10.0", -10.0),
    ("get_wavelength_medium", ":SENSe:CORRection:RVELocity:MEDium?", "1", "vacuum"),
    ("get_trace_attribute", ":TRACe:ATTRibute:TRA?", "2", "max"),
    ("get_auto_measure", ":CALCulate:AUTO?", "0", False),
])
def test_typed_readers(osa, method, query, answer, expected):
    device, resource = osa
    resource.answers = {query: answer + "\n"}
    value = getattr(device, method)()
    assert value == pytest.approx(expected) if isinstance(expected, float) else value == expected
    assert resource.queries == [query]


def test_an_unexpected_label_is_an_error(osa):
    device, resource = osa
    resource.answers = {":SENSe:SENSe?": "99"}
    with pytest.raises(ValueError):
        device.get_sensitivity()


def test_sampling_step_is_derived_from_span_and_points(osa):
    device, resource = osa
    resource.answers = {":SENSe:WAVelength:SPAN?": "3.6E-08", ":SENSe:SWEep:POINts?": "20001"}
    assert device.get_sampling_step() == pytest.approx(36.0 / 20000)


def test_read_trace_sweeps_once_and_returns_nm_and_dbm(osa):
    device, resource = osa
    states = iter(["16", "0", "1"])
    answers = {"TRAC:DATA:X? TRA": "1.5500E-06,1.5501E-06", "TRAC:DATA:Y? TRA": "-50.0,-40.5"}
    resource.answers = lambda command: next(states) if command == ":stat:oper:even?" else answers[command]
    wavelength, power = device.read_trace()
    np.testing.assert_allclose(wavelength, [1550.0, 1550.1])
    np.testing.assert_allclose(power, [-50.0, -40.5])
    assert resource.writes == [":init:smode 1", "*CLS", ":init"]
    assert resource.queries.count(":stat:oper:even?") == 3


def test_wait_for_sweep_complete_times_out(osa, monkeypatch):
    device, resource = osa
    resource.answers = {":stat:oper:even?": "0"}
    clock = iter(range(0, 1000, 5))
    monkeypatch.setattr(yokogawa_module.time, "time", lambda: next(clock))
    with pytest.raises(TimeoutError):
        device.wait_for_sweep_complete(timeout_s=20)
