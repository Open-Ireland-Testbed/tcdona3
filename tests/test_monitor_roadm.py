"""``tcdona3.monitor.RoadmMonitor`` builds and records its measurement setup on any host.

It used to read ``/etc/tcdona2/ports.csv`` (a tcdona2 table that only exists on ol02) in its constructor and
fail without it, and ``write_json_data`` used ``roadm_ip_address``, which nothing set: the measurement setup
was never recorded.
"""

import pandas as pd
import pytest

import tcdona3.monitor as monitor_module
from tcdona3.lumentum import ip_map
from tcdona3.monitor import RoadmMonitor

PORT_KEYS = ("P1_MUX_port", "P1_DEMUX_port", "Line_Out_port", "Line_In_port")


class _Roadm(object):
    """The attributes RoadmMonitor reads from a Lumentum driver at construction."""

    def __init__(self, device_name):
        self.device_name = device_name
        self.mux_additional_attn = 0
        self.demux_additional_attn = 0


def _table(rows):
    """A stand-in for the tcdona2 table: ``rows`` maps a name to its (In, Out) ports."""
    def read_csv(path, sep=None, index_col=None):
        assert path == "/etc/tcdona2/ports.csv" and sep == "\t" and index_col == "Name"
        frame = pd.DataFrame([{"Name": name, "In": ports[0], "Out": ports[1]} for name, ports in rows.items()])
        return frame.set_index("Name")
    return read_csv


@pytest.fixture
def no_table(monkeypatch):
    """A host without the tcdona2 table (ol4)."""
    def missing(*args, **kwargs):
        raise FileNotFoundError("/etc/tcdona2/ports.csv")
    monkeypatch.setattr(monitor_module.pd, "read_csv", missing)


@pytest.fixture
def monitor(no_table):
    return RoadmMonitor(_Roadm("roadm_1"))


def test_without_the_table_the_constructor_works_and_knows_the_roadm_address(monitor):
    assert monitor.device_name == "roadm_1"
    assert monitor.roadm_ip_address == ip_map["roadm_1"]
    assert (monitor.p1_mux_port, monitor.p1_demux_port, monitor.line_out_port, monitor.line_in_port) == \
        (None, None, None, None)


def test_with_the_table_the_ports_are_read_as_before(monkeypatch):
    """The rows of ol02's table; tcdona3 always read those of Lumentum_5."""
    monkeypatch.setattr(monitor_module.pd, "read_csv",
                        _table({"Lumentum_5_line": (98, 418), "Lumentum_5_p1": (114, 434)}))
    monitor = RoadmMonitor(_Roadm("roadm_2"))
    assert (monitor.p1_mux_port, monitor.p1_demux_port, monitor.line_out_port, monitor.line_in_port) == \
        (434, 114, 98, 418)


def test_a_table_without_the_rows_gives_no_ports(monkeypatch):
    monkeypatch.setattr(monitor_module.pd, "read_csv", _table({"Cassini_1_p5": (1, 321)}))
    monitor = RoadmMonitor(_Roadm("roadm_1"))
    assert monitor.p1_mux_port is None and monitor.line_in_port is None


def test_an_unknown_roadm_name_has_no_address(no_table):
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
    assert all(setup[key] is None for key in PORT_KEYS)
    assert data["measurement_data"] == {"sweep": "done"}
