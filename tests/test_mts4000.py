"""Software tests of ``tcdona3.mts4000``: parsing, configuration and acquisition over a fake SCPI socket.

No instrument is reached: :class:`fakes.FakeScpiTransport` stands for the two SCPI sockets of the
driver and answers from a scripted responder.
"""

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
