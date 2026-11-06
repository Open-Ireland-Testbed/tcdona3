"""Software tests of ``tcdona3.mts4000``: parsing, configuration and acquisition over a fake SCPI socket.

No instrument is reached: :class:`fakes.FakeScpiTransport` stands for the two SCPI sockets of the
driver and answers from a scripted responder.
"""

import socket

import numpy as np
import pytest

import tcdona3
import tcdona3.mts4000 as mts4000

from fakes import FakeScpiTransport, curve_payload

IDN = "VIAVI Solutions,MTS 4000 V2,EBAH13125,ISU,23.62"

EVENT_HEADER = (
    "Event,Distance  km,Loss dB,Reflect. dB,Slope  dB/km,,Section km,"
    "T. Loss dB"
)
EVENT_ROWS = [
    "  1,First Connector,    0.000,~  0.766,~ -45.82,,,    0.000,  0.000",
    "2,Fiber End,   27.026,, -29.77,  0.332,,   27.026,  8.971",
]


def data_responder(command, session):
    """Answer the OTDR data-port queries, following the session state for settings."""
    state = session.state
    answers = {
        "STATus:TABCurrent?": "SM_Otdr",
        "*ESR?": "0",
        "*IDN?": IDN,
        "OTDSetup:LSET:NB?": "2",
        "OTDSetup:LSET:NAME? 1": "L1310",
        "OTDSetup:LSET:LAMbda? 1": "1310.0",
        "OTDSetup:LSET:NAME? 2": "L1550",
        "OTDSetup:LSET:LAMbda? 2": "1550.0",
        "OTDSetup:N? L1310": "1.46800",
        "OTDSetup:N? L1550": "1.46800",
        "OTDSetup:RAUto?": state["auto_range"],
        "OTDSetup:SACQuisition?": "NO",
        "OTDSetup:ACQuisition?": state["acquisition_mode"],
        "OTDSetup:LASer?": state["laser"],
        "OTDSetup:PULSe?": state["pulse"],
        "OTDSetup:KMRange?": state["range_km"],
        "OTDSetup:RESolution?": state["resolution"],
        "OTDSetup:MAXTime?": state["max_time"],
        "OTDSetup:UNITs?": state["units"],
        "OTDSetup:DSPLice?": state["splice"],
        "OTDSetup:DREFlectance?": state["reflectance"],
        "OTDSetup:DFEnd?": state["end_of_fiber"],
        "OTDResult:MODUle?": "4126 B",
        "OTDResult:DATE?": "2026-11-05 12:00:00",
        "OTDResult:PULSe?": "P100NS",
        "OTDResult:RANGe?": "20",
        "OTDResult:LASer?": "L1550",
        "OTDResult:RESO?": "HRES,0.6",
        "OTDResult:ATIMe?": "30",
        "OTDResult:N?": "1.468",
        "OTDResult:K?": "-79.0",
        "OTDResult:CWLevel?": "-20.0",
        "OTDResult:AQMode?": "MAN",
        "CURve:SIZE?": "4",
        "CURve:XOFFset?": "0",
        "CURve:XSCale?": "0.5",
        "CURve:YOFFset?": "-10",
        "CURve:YSCale?": "0.01",
        "CURve:XUNit?": "m",
        "CURve:YUNit?": "dB",
        "TABle:TITle?": EVENT_HEADER,
        "TABle:TOP?": "2 events",
        "TABle:SIZe?": "2",
        "TABle:LINe? 1": EVENT_ROWS[0],
        "TABle:LINe? 2": EVENT_ROWS[1],
    }
    return answers.get(command, "")


def control_responder(command, session):
    if command.startswith("MOD:FUNC:LIST?"):
        return '"OTDR","OSA","SOURCE"' if command.endswith("oppside,slic1") else ""
    if command.startswith("MOD:FUNC:PORT?"):
        return "8002"
    if command.startswith("MOD:FUNC:SELECT?"):
        return "ON"
    return ""


def make_device(monkeypatch, statuses=None):
    """An ``MTS4000`` wired to fake control/data transports, without a network connect."""
    data = FakeScpiTransport(
        responder=data_responder,
        blocks={
            "CURve:BUFfer?": curve_payload([100, -200, 300, 0]),
            "OTDResult:SSOR?": b"Bellcore-SOR",
        },
        statuses=statuses,
    )
    control = FakeScpiTransport(responder=control_responder)
    device = mts4000.MTS4000("192.0.2.10")
    device._ctl = control
    device._data = data
    device.data_port = 8002
    device.module_position = "oppside,slic1"
    device.scpi_idn = IDN
    return device, control, data


@pytest.fixture
def device(monkeypatch):
    return make_device(monkeypatch)


def test_package_exports_the_driver():
    assert tcdona3.MTS4000 is mts4000.MTS4000
    assert tcdona3.MTS4000Error is mts4000.MTS4000Error
    assert "MTS4000" in tcdona3.__all__


@pytest.mark.parametrize("value,expected", [
    (1310, "L1310"),
    ("1550nm", "L1550"),
    ("L1310", "L1310"),
    ("1310/1550", "L1310_1550"),
    ("all", "ALL"),
])
def test_normalise_wavelength(value, expected):
    assert mts4000.normalise_wavelength(value) == expected


def test_normalise_wavelength_rejects_unknown():
    with pytest.raises(ValueError):
        mts4000.normalise_wavelength(1490)


@pytest.mark.parametrize("value,expected", [
    (100, "P100NS"),
    ("100ns", "P100NS"),
    ("0.1us", "P100NS"),
    ("2us", "P2US"),
    ("P3US", "P3US"),
])
def test_normalise_pulse_width(value, expected):
    assert mts4000.normalise_pulse_width(value) == expected


def test_normalise_resolution():
    assert mts4000.normalise_resolution("hres") == "HRES,0"
    assert mts4000.normalise_resolution(("manu", 0.62)) == "MANU,0.62"


def test_normalise_acquisition_time():
    assert mts4000._normalise_acquisition_time(30) == "30"
    assert mts4000._normalise_acquisition_time("auto") == "-2"
    assert mts4000._normalise_acquisition_time("realtime") == "-1"
    with pytest.raises(ValueError):
        mts4000._normalise_acquisition_time(0)


def test_decode_curve_block():
    assert mts4000.decode_curve_block(b"191AFB2E").tolist() == [6426, -1234]


def test_parse_event_table():
    events = mts4000.parse_event_table(EVENT_HEADER, EVENT_ROWS)
    assert events[0]["event_name"] == "First Connector"
    assert events[0]["loss_db"] == pytest.approx(0.766)
    assert events[1]["distance_km"] == pytest.approx(27.026)
    assert events[1]["total_loss_db"] == pytest.approx(8.971)


def test_connect_discovers_the_module_and_data_port(monkeypatch):
    control = FakeScpiTransport(responder=control_responder)
    data = FakeScpiTransport(responder=data_responder)
    sockets = {"control": control, "data": data}

    def factory(host, port, timeout=10.0):
        return control if port == 8000 else data

    monkeypatch.setattr(mts4000, "_SCPISocket", factory)
    device = mts4000.MTS4000("192.0.2.10")
    device.connect()

    assert device.module_position == "oppside,slic1"
    assert device.data_port == 8002
    assert device.scpi_idn == IDN
    assert '"OTDR"' in " ".join(control.sent)
    assert "STATus:TABCurrent SM_Otdr" in data.sent


def test_get_config_reads_settings(device):
    device, _, _ = device
    config = device.get_config()
    assert config.acquisition_mode == "MAN"
    assert config.wavelength == "L1550"
    assert config.pulse_width == "P100NS"
    assert config.range_km == pytest.approx(20.0)
    assert config.acquisition_time_s == pytest.approx(30.0)
    assert config.units == "KM"
    assert [laser["name"] for laser in config.available_lasers] == ["L1310", "L1550"]


def test_configure_writes_settings_and_returns_the_new_config(device):
    device, _, data = device
    config = device.configure(
        wavelength="1550nm",
        pulse_width="2us",
        range_km=260,
        acquisition_time_s=30,
        units="km",
    )
    assert "OTDSetup:LASer L1550" in data.sent
    assert "OTDSetup:LPULse L1550,P2US" in data.sent
    assert "OTDSetup:RAUto NO" in data.sent
    assert "OTDSetup:KMRange 260" in data.sent
    assert "OTDSetup:MAXTime 30" in data.sent
    assert config.wavelength == "L1550"
    assert config.pulse_width == "P2US"
    assert config.range_km == pytest.approx(260.0)


def test_configure_refuses_range_with_auto_range(device):
    device, _, data = device
    with pytest.raises(ValueError):
        device.configure(range_km=10, auto_range=True)
    assert not any("KMRange" in command for command in data.sent)


def test_configure_refuses_an_unadvertised_wavelength(device):
    device, _, _ = device
    with pytest.raises(ValueError, match="L1625"):
        device.configure(wavelength=1625)


def test_check_error_reports_the_event_register(monkeypatch):
    def responder(command, session):
        if command == "*ESR?":
            return "16"
        if command == "STATus:ERR?":
            return "execution error, other Fiber functions are active"
        return data_responder(command, session)

    device, _, _ = make_device(monkeypatch)
    device._data.responder = responder
    with pytest.raises(mts4000.MTS4000Error, match="ESR=16"):
        device.write("OTDSetup:MAXTime 30")


def test_get_trace_scales_counts(device):
    device, _, _ = device
    trace = device.get_trace()
    assert trace["size"] == 4
    assert trace["raw"].tolist() == [100, -200, 300, 0]
    np.testing.assert_allclose(trace["loss_db"], [0.01 * 100 - 10, -12, -7, -10])
    np.testing.assert_allclose(trace["distance_m"], [0.0, 0.5, 1.0, 1.5])


def test_get_events_parses_the_table(device):
    device, _, _ = device
    events = device.get_events()
    assert events["size"] == 2
    assert events["events"][1]["event_name"] == "Fiber End"
    assert events["events"][1]["distance_km"] == pytest.approx(27.026)


def test_get_sor_returns_the_payload(monkeypatch):
    device, _, data = make_device(monkeypatch)
    data.blocks["OTDResult:SSOR?"] = b"Bellcore-SOR"
    assert device.get_sor() == b"Bellcore-SOR"


def test_acquire_waits_and_collects(monkeypatch):
    device, _, _ = make_device(monkeypatch, statuses=("IN_PROGRESS", "STOPPED"))
    result = device.acquire(timeout=5.0, poll=0.0)
    assert result["identity"]["scpi_idn"] == IDN
    assert result["trace"]["size"] == 4
    assert result["events"]["size"] == 2
    assert result["metadata"]["module"] == "4126 B"


def test_wait_for_acquisition_read_only_raises(monkeypatch):
    device, _, _ = make_device(monkeypatch, statuses=("READ ONLY",))
    with pytest.raises(mts4000.MTS4000Error, match="READ ONLY"):
        device.wait_for_acquisition(timeout=1.0, poll=0.0)


def test_wait_for_acquisition_times_out(monkeypatch):
    device, _, _ = make_device(monkeypatch, statuses=("IN_PROGRESS",))
    with pytest.raises(TimeoutError):
        device.wait_for_acquisition(timeout=0.01, poll=0.0)


def test_exclusive_otdr_disables_conflicting_functions(monkeypatch):
    device, control, _ = make_device(monkeypatch)
    states = device.exclusive_otdr()
    assert '"OSA",OFF' in " ".join(control.sent)
    assert '"SOURCE",OFF' in " ".join(control.sent)
    assert '"OTDR",ON' in " ".join(control.sent)
    assert states["oppside,slic1"]["OTDR"] is True


def test_save_result_writes_json_trace_and_sor(monkeypatch, tmp_path):
    device, _, _ = make_device(monkeypatch)
    paths = device.save_result(tmp_path, stem="otdr_test")
    assert set(paths) == {"json", "trace_csv", "sor"}
    assert paths["json"].is_file()
    assert paths["trace_csv"].is_file()
    assert paths["sor"].read_bytes() == b"Bellcore-SOR"


def test_identify_parses_the_idn(monkeypatch):
    device, _, _ = make_device(monkeypatch)

    def refuse(*args, **kwargs):
        raise mts4000.urllib.error.URLError("no http")

    monkeypatch.setattr(mts4000.urllib.request, "urlopen", refuse)
    info = device.identify()
    assert info["vendor"] == "VIAVI Solutions"
    assert info["model"] == "MTS 4000 V2"
    assert info["serial"] == "EBAH13125"
    assert info["firmware"] == "23.62"
    assert info["http_info"] is None


def test_close_closes_both_sockets(device):
    device, control, data = device
    device.close()
    assert control.closed and data.closed


# --------------------------------------------------------------------------- review follow-ups (PR #32)
def test_normalise_pulse_width_checks_keywords_against_the_table():
    assert mts4000.normalise_pulse_width("P1US") == "P1US"
    with pytest.raises(ValueError):
        mts4000.normalise_pulse_width("P7NS")


def test_acquisition_time_read_back_can_be_written_again():
    """get_config() reports auto/realtime as -2/-1; configure() accepts them back."""
    assert mts4000._normalise_acquisition_time(-2.0) == "-2"
    assert mts4000._normalise_acquisition_time("-1") == "-1"
    with pytest.raises(ValueError):
        mts4000._normalise_acquisition_time(-3)


def test_decode_curve_block_refuses_non_hex():
    with pytest.raises(mts4000.MTS4000Error):
        mts4000.decode_curve_block(b"12G4")


def test_otdr_config_is_a_plain_value():
    first = mts4000.OTDRConfig(wavelength="L1550", range_km=20.0)
    assert first == mts4000.OTDRConfig(wavelength="L1550", range_km=20.0)
    assert first != mts4000.OTDRConfig(wavelength="L1310", range_km=20.0)
    assert first.as_dict()["wavelength"] == "L1550"
    assert first.as_dict()["available_lasers"] == []
    assert "wavelength='L1550'" in repr(first)


def test_configure_writes_nothing_when_a_value_is_refused(device):
    device, _, data = device
    with pytest.raises(ValueError, match="index_of_refraction"):
        device.configure(wavelength=1550, pulse_width="100ns", index_of_refraction=2.0)
    assert not [command for command in data.sent if command.startswith("OTDSetup:")]


def test_configure_range_sends_manual_range_once(device):
    device, _, data = device
    device.configure(range_km=40, auto_range=False)
    assert data.sent.count("OTDSetup:RAUto NO") == 1


def test_configure_auto_range(device):
    device, _, data = device
    config = device.configure(auto_range=True)
    assert "OTDSetup:RAUto YES" in data.sent
    assert config.auto_range is True


def test_acquire_refuses_a_result_that_is_the_previous_one(monkeypatch):
    """The start was refused: the status stays STOPPED and the result date does not change."""
    device, _, _ = make_device(monkeypatch, statuses=("STOPPED",))
    with pytest.raises(mts4000.MTS4000Error, match="did not start"):
        device.acquire(timeout=5.0, poll=0.0, start_grace=0.0)


def test_acquire_reports_the_instrument_reason_for_a_refused_start(monkeypatch):
    device, _, data = make_device(monkeypatch, statuses=("STOPPED",))

    def responder(command, session):
        if "KEY BEGinacq" in session.sent and command == "*ESR?":
            return "16"
        if command == "STATus:ERR?":
            return "other Fiber functions are active"
        return data_responder(command, session)

    data.responder = responder
    with pytest.raises(mts4000.MTS4000Error, match="exclusive_otdr"):
        device.acquire(timeout=5.0, poll=0.0, start_grace=0.0)


def test_acquire_accepts_a_result_finished_before_the_first_poll(monkeypatch):
    device, _, data = make_device(monkeypatch, statuses=("STOPPED",))

    def responder(command, session):
        if command == "OTDResult:DATE?" and "KEY BEGinacq" in session.sent:
            return "2026-11-06 10:00:00"
        return data_responder(command, session)

    data.responder = responder
    result = device.acquire(timeout=5.0, poll=0.0, start_grace=0.0)
    assert result["metadata"]["date"] == "2026-11-06 10:00:00"


# --------------------------------------------------------------------------- the hardware smoke script
def _smoke_module():
    import importlib.util
    import os

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hardware", "smoke_mts4000.py")
    spec = importlib.util.spec_from_file_location("smoke_mts4000_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_instrument(monkeypatch, statuses=None, transport=FakeScpiTransport, **state):
    """Patches the driver's sockets; returns the list of data-port transports the script opens."""
    opened = []

    def factory(host, port, timeout=10.0):
        if port == 8000:
            return FakeScpiTransport(responder=control_responder)
        data = transport(
            responder=data_responder,
            blocks={"CURve:BUFfer?": curve_payload([100, -200, 300, 0]), "OTDResult:SSOR?": b"SOR"},
            statuses=statuses,
        )
        data.state.update(state)
        opened.append(data)
        return data

    def refuse(*args, **kwargs):
        raise mts4000.urllib.error.URLError("no http")

    monkeypatch.setattr(mts4000, "_SCPISocket", factory)
    monkeypatch.setattr(mts4000.urllib.request, "urlopen", refuse)
    return opened


def test_smoke_script_default_run_writes_no_setting(monkeypatch):
    opened = _fake_instrument(monkeypatch, auto_range="YES", max_time="-2")
    assert _smoke_module().main(["192.0.2.10"]) == 0
    assert len(opened) == 1 and opened[0].closed
    assert not [command for command in opened[0].sent if command.startswith("OTDSetup:")]
    assert (opened[0].state["auto_range"], opened[0].state["max_time"]) == ("YES", "-2")


def test_smoke_script_acquire_puts_auto_range_and_auto_time_back(monkeypatch, tmp_path):
    opened = _fake_instrument(monkeypatch, statuses=("IN_PROGRESS", "STOPPED"), auto_range="YES",
                              max_time="-2")
    argv = ["192.0.2.10", "--acquire", "--range-km", "40", "--time", "10", "--output", str(tmp_path)]
    assert _smoke_module().main(argv) == 0
    data, = opened
    assert data.closed
    assert "OTDSetup:KMRange 40" in data.sent and "OTDSetup:MAXTime 10" in data.sent
    assert (data.state["auto_range"], data.state["max_time"]) == ("YES", "-2")
    assert sorted(path.suffix for path in tmp_path.iterdir()) == [".csv", ".json", ".sor"]


# --------------------------------------------------------------------------- second review (Codex) follow-ups
@pytest.fixture
def scpi_pair():
    """A real ``_SCPISocket`` on one end of a socket pair; the test writes the instrument's side."""
    ours, instrument = socket.socketpair()
    ours.settimeout(1.0)
    scpi = mts4000._SCPISocket("instrument", 8002, timeout=1.0)
    scpi._sock = ours
    yield scpi, instrument
    scpi.close()
    instrument.close()


@pytest.mark.parametrize("after_block", [b"\r\n20\n", b"\n20\n", b"20\n"])
def test_a_block_terminator_arriving_after_the_block_is_not_read_as_the_next_reply(scpi_pair, after_block):
    scpi, instrument = scpi_pair
    instrument.sendall(b"#14ABCD")
    assert scpi.read_block() == b"ABCD"
    instrument.sendall(after_block)  # CRLF, LF or (other firmware) nothing before the next reply
    assert scpi.read_line() == "20"


@pytest.mark.parametrize("chunks", [(b"#14ABCD\r", b"\n20\n"), (b"#14ABCD\r\n", b"20\n")])
def test_a_block_terminator_split_or_whole_is_dropped(scpi_pair, chunks):
    scpi, instrument = scpi_pair
    instrument.sendall(chunks[0])
    assert scpi.read_block() == b"ABCD"
    instrument.sendall(chunks[1])
    assert scpi.read_line() == "20"


def test_a_timeout_closes_the_socket_so_a_late_reply_is_never_read(scpi_pair):
    scpi, instrument = scpi_pair
    scpi._sock.settimeout(0.05)
    with pytest.raises(mts4000.MTS4000TransportError):
        scpi.query("OTDSetup:MAXTime?")
    assert not scpi.connected  # a reply to MAXTime? arriving now can no longer be read
    with pytest.raises(mts4000.MTS4000TransportError):
        scpi.query("*ESR?")


def test_a_query_that_times_out_does_not_shift_the_replies_of_the_next_ones(monkeypatch):
    data_sessions = []

    def factory(host, port, timeout=10.0):
        if port == 8000:
            return FakeScpiTransport(responder=control_responder)
        first = not data_sessions

        def responder(command, session):
            if first and command == "OTDSetup:MAXTime?":
                session.close()  # what _SCPISocket does on a timeout
                raise mts4000.MTS4000TransportError("timeout")
            return data_responder(command, session)

        data = FakeScpiTransport(responder=responder)
        data_sessions.append(data)
        return data

    monkeypatch.setattr(mts4000, "_SCPISocket", factory)
    config = mts4000.MTS4000("192.0.2.10").connect().get_config()
    assert config.acquisition_time_s is None  # the query that timed out
    assert (config.units, config.splice_detection) == ("KM", 50)  # read on a new session, in step
    assert len(data_sessions) == 2 and data_sessions[0].closed


def test_a_failed_connect_closes_what_it_opened_and_a_retry_starts_afresh(monkeypatch):
    opened, failing = [], [True]

    def factory(host, port, timeout=10.0):
        if port == 8000:
            transport = FakeScpiTransport(responder=control_responder)
        else:
            def responder(command, session):
                if command == "*IDN?" and failing[0]:
                    failing[0] = False
                    raise mts4000.MTS4000TransportError("timeout")
                return data_responder(command, session)
            transport = FakeScpiTransport(responder=responder)
        opened.append(transport)
        return transport

    monkeypatch.setattr(mts4000, "_SCPISocket", factory)
    device = mts4000.MTS4000("192.0.2.10")
    with pytest.raises(mts4000.MTS4000TransportError):
        device.connect()
    assert len(opened) == 2 and all(transport.closed for transport in opened)
    assert device._data is None and device._ctl is None
    device.connect()
    assert device.scpi_idn == IDN and len(opened) == 4


class _StatusesUntilHalt(object):
    """``STATus:ACQ?`` answers: the given items (an exception class is raised) until KEY HALTacq is sent."""

    def __init__(self, transport, items, halts=True):
        self.transport, self.items, self.halts = transport, list(items), halts

    def __iter__(self):
        return self

    def __next__(self):
        if self.halts and "KEY HALTacq" in self.transport.sent:
            return "STOPPED"
        item = self.items.pop(0) if len(self.items) > 1 else self.items[0]
        if isinstance(item, type) and issubclass(item, BaseException):
            raise item
        return item


@pytest.mark.parametrize("statuses,error", [
    (["IN_PROGRESS"], TimeoutError),
    (["IN_PROGRESS", KeyboardInterrupt], KeyboardInterrupt),
    (["IN_PROGRESS", mts4000.MTS4000TransportError], mts4000.MTS4000TransportError),
])
def test_acquire_stops_the_acquisition_when_waiting_fails(monkeypatch, capsys, statuses, error):
    device, _, data = make_device(monkeypatch)
    data.statuses = _StatusesUntilHalt(data, statuses)
    with pytest.raises(error):
        device.acquire(timeout=0.05, poll=0.0)
    assert "KEY BEGinacq" in data.sent and "KEY HALTacq" in data.sent
    assert data.sent.index("KEY HALTacq") > data.sent.index("KEY BEGinacq")
    assert capsys.readouterr().err == ""


def test_acquire_reports_a_stop_it_cannot_confirm(monkeypatch, capsys):
    device, _, data = make_device(monkeypatch)
    data.statuses = _StatusesUntilHalt(data, ["IN_PROGRESS"], halts=False)
    with pytest.raises(TimeoutError):
        device.acquire(timeout=0.05, poll=0.0)
    assert "could not be confirmed stopped" in capsys.readouterr().err


@pytest.mark.parametrize("settings", [
    {"range_km": -1}, {"range_km": float("nan")}, {"range_km": 0}, {"resolution": ("manu", 0)},
    {"resolution": -2.0}, {"splice_detection_db": float("inf")}, {"end_of_fiber_db": "high"},
])
def test_configure_refuses_impossible_numbers_before_writing(device, settings):
    device, _, data = device
    with pytest.raises(ValueError):
        device.configure(wavelength=1550, **settings)
    assert not [command for command in data.sent if command.startswith("OTDSetup:")]


class _PerWavelengthPulses(FakeScpiTransport):
    """An instrument whose pulse is set per wavelength, as on the MTS 4000 (LPULse sets one, PULSe the current)."""

    def __init__(self, *args, **kwargs):
        FakeScpiTransport.__init__(self, *args, **kwargs)
        self.pulses = {"L1310": "P100NS", "L1550": "P2US"}
        self.state["laser"] = "L1310"
        self.state["pulse"] = self.pulses["L1310"]

    def _track(self, command):
        text = command.strip()
        if text.startswith("OTDSetup:LASer "):
            self.state["laser"] = text.split(None, 1)[1]
        elif text.startswith("OTDSetup:LPULse "):
            laser, pulse = text.split(None, 1)[1].split(",")
            self.pulses[laser] = pulse
        elif text.startswith("OTDSetup:PULSe "):
            self.pulses[self.state["laser"]] = text.split(None, 1)[1]
        else:
            FakeScpiTransport._track(self, command)
        self.state["pulse"] = self.pulses.get(self.state["laser"])


def test_smoke_script_puts_back_the_pulse_of_each_wavelength_it_changed(monkeypatch, tmp_path):
    opened = _fake_instrument(monkeypatch, statuses=("IN_PROGRESS", "STOPPED"), transport=_PerWavelengthPulses)
    argv = ["192.0.2.10", "--acquire", "--wavelength", "1550", "--pulse", "100ns", "--output", str(tmp_path)]
    assert _smoke_module().main(argv) == 0
    data, = opened
    assert "OTDSetup:LPULse L1550,P100NS" in data.sent
    assert data.state["laser"] == "L1310"
    assert data.pulses == {"L1310": "P100NS", "L1550": "P2US"}


def test_smoke_script_writes_nothing_when_a_setting_it_would_change_cannot_be_read(monkeypatch, tmp_path):
    opened = _fake_instrument(monkeypatch, statuses=("IN_PROGRESS", "STOPPED"), max_time="")
    argv = ["192.0.2.10", "--acquire", "--time", "10", "--output", str(tmp_path)]
    assert _smoke_module().main(argv) == 1
    data, = opened
    assert not [command for command in data.sent if command.startswith("OTDSetup:")]
    assert "KEY BEGinacq" not in data.sent
