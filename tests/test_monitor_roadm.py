"""``tcdona3.monitor.RoadmMonitor`` builds and records its measurement setup on any host.

It used to read ``/etc/tcdona2/ports.csv`` (a tcdona2 table that only exists on ol02) in its constructor, and
``write_json_data`` used ``roadm_ip_address``, which nothing set: the measurement setup was never recorded.
"""

import pytest

import tcdona3.monitor as monitor_module
from tcdona3.lumentum import ip_map
from tcdona3.monitor import RoadmMonitor


class _Roadm(object):
    """The attributes RoadmMonitor reads from a Lumentum driver at construction."""

    def __init__(self, device_name):
        self.device_name = device_name
        self.mux_additional_attn = 0
        self.demux_additional_attn = 0


@pytest.fixture
def monitor(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("RoadmMonitor must not read a host file: %r" % (args,))

    monkeypatch.setattr(monitor_module.pd, "read_csv", refuse)
    return RoadmMonitor(_Roadm("roadm_1"))


def test_the_constructor_reads_no_host_file_and_knows_the_roadm_address(monitor):
    assert monitor.device_name == "roadm_1"
    assert monitor.roadm_ip_address == ip_map["roadm_1"]


def test_an_unknown_roadm_name_has_no_address():
    assert RoadmMonitor(_Roadm("roadm_unknown")).roadm_ip_address is None


def test_write_json_data_records_the_measurement_setup(monitor, monkeypatch):
    monkeypatch.setattr(RoadmMonitor, "record_monitor_data", lambda self, WAIT_TIME=3: None)
    monkeypatch.setattr(RoadmMonitor, "measurement_sweep", lambda self, debug=True: {"sweep": "done"})
    monitor.start_timer = "now"

    data = monitor.write_json_data(WAIT_TIME=0, get_only=True, index=3, uid="u1")

    setup = data["measurement_setup"]
    assert setup["device_name"] == "roadm_1"
    assert setup["ip_address"] == ip_map["roadm_1"]
    assert setup["index"] == 3
    for removed in ("P1_MUX_port", "P1_DEMUX_port", "Line_Out_port", "Line_In_port"):
        assert removed not in setup
    assert data["measurement_data"] == {"sweep": "done"}
