"""Hardware smoke test of ``tcdona3.lumentum.Lumentum`` on one ROADM carrying a line.

Usage (from the parent directory of the tcdona3 package)::

    python3 tcdona3/tests/hardware/smoke_lumentum.py <roadm> [--edfa-writes] [--unblock] [--show-values]

``<roadm>`` is a name the driver knows; the connection goes through the booking check of the driver.

The script saves the running configuration first and exercises the readers (WSS connections, OCM, ports,
EDFAs, line port), the validation, the session options and the WSS writes on one *blocked* DEMUX connection,
which passes no light: attenuation (also as parallel lists), block, output port, delete and add again (one by
one and as a group). Unblocking it is exercised only with ``--unblock``: a blocked connection can still have
light at its input, and unblocking it sends that light to its drop port for about a second, so use it only
when the band of the connection is dark or its drop port is free. It also moves the output VOA of the line port by :data:`STEP_DB` (the line
power changes for a few seconds) and writes the maintenance state and gain switch mode of each EDFA with the
value they hold. With ``--edfa-writes`` it also runs the legacy EDFA setters with the values the EDFAs hold:
each takes its EDFA out of service for about a second, interrupting the line. On a line that loops back to
the ROADM the interruption can latch: with no light at the preamp, the automatic laser shutdown keeps the
booster off, and the amplifiers along the line shut down too.

At the end, also when a check fails or the run is interrupted (Ctrl-C), every element of the running configuration (connection, EDFA, port)
that differs from the saved one is written back as saved, and when the preamp is not in the operational state
it had at the start the automatic laser shutdown is suspended (:meth:`Lumentum.disable_als`) until the line
is back. The script fails when the running configuration or the preamp state is not the initial one. Measured values are printed only with ``--show-values``.
"""

import argparse
import contextlib
import io
import math
import os
import signal
import sys
import time

from lxml import etree

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from tcdona3 import _netconf  # noqa: E402
from tcdona3.lumentum import Lumentum  # noqa: E402

STEP_DB = 0.5
#: How long the automatic laser shutdown is suspended to bring the line back, and the wait for it [s].
ALS_SUSPEND_S, LINE_WAIT_S = 60, 120
#: The containers of the running configuration whose elements are put back, by their ``dn``.
RESTORED = {"connection": "http://www.lumentum.com/lumentum-ote-connection",
            "edfa": "http://www.lumentum.com/lumentum-ote-edfa",
            "physical-port": "http://www.lumentum.com/lumentum-ote-port"}
CONTAINERS = {"connection": "connections", "edfa": "edfas", "physical-port": "physical-ports"}
LINE_PORT_DN = "ne=1;chassis=1;card=1;port=3001"
DEMUX, DEMUX_INPUT = 2, 5101


class Checks:
    def __init__(self, show_values):
        self.show_values, self.failed = show_values, []

    def check(self, name, condition, value=None):
        status = "ok  " if condition else "FAIL"
        shown = " = %r" % (value,) if self.show_values and value is not None else ""
        print("[%s] %s%s" % (status, name, shown))
        if not condition:
            self.failed.append(name)

    def raises(self, name, call, error=ValueError):
        try:
            with quiet():
                call()
        except error:
            self.check(name, True)
        else:
            self.check(name, False)


@contextlib.contextmanager
def quiet():
    """The driver prints its RPC confirmations: keep them out of the report."""
    with contextlib.redirect_stdout(io.StringIO()):
        yield

@contextlib.contextmanager
def interrupts_held():
    """Ctrl-C is held while the device is put back: the restore runs to its end, then the run ends."""
    received = []

    def hold(signum, frame):  # no I/O here: a print during a print raises a reentrant-call error
        received.append(signum)

    previous = signal.signal(signal.SIGINT, hold)
    try:
        yield received
    finally:
        signal.signal(signal.SIGINT, previous)
        if received:
            print("[info] Ctrl-C was held until the device was put back")



def local(element):
    return etree.QName(element).localname


def running(roadm):
    return roadm.m.get_config(source="running").data_xml


def elements(xml):
    """``{(kind, dn): canonical XML}`` of the restored elements of a running configuration."""
    root = etree.fromstring(xml.encode())
    found = {}
    for kind in RESTORED:
        for element in root.iter("{%s}%s" % (RESTORED[kind], kind)):
            dn = element.findtext("{%s}dn" % RESTORED[kind])
            if dn:
                found[(kind, dn)] = etree.tostring(element, method="c14n")
    return found


def connection_config(roadm, dn):
    """The config leaves of one connection, from the running configuration (None when absent)."""
    for (kind, found), xml in elements(running(roadm)).items():
        if kind == "connection" and found == dn:
            config = etree.fromstring(xml).find("{%s}config" % RESTORED["connection"])
            return {local(leaf): (leaf.text or "").strip() for leaf in config}
    return None


def is_number(text):
    try:
        return math.isfinite(float(text))
    except (TypeError, ValueError):
        return False


def demux_dn(number):
    return "ne=1;chassis=1;card=1;module=%d;connection=%d" % (DEMUX, number)


def read(roadm, checks):
    with quiet():
        connections = roadm.wss_get_connections()
    numbers = {k: sorted(c["connection"] for c in connections[k].values()) for k in ("mux", "demux")}
    checks.check("wss_get_connections: every connection carries its number",
                 all(numbers[k] and len(set(numbers[k])) == len(numbers[k]) for k in numbers),
                 {k: len(v) for k, v in numbers.items()})
    with quiet():
        monitored = roadm.wss_get_monitored_channels()
    checks.check("wss_get_monitored_channels: both directions", all(monitored.get(k) for k in ("mux", "demux")),
                 {k: len(v) for k, v in monitored.items()})
    rows = roadm.monitored_channel_rows(6201)
    checks.check("monitored_channel_rows: floats per channel",
                 rows and all(isinstance(r["power"], float) and r["end-freq"] > r["start-freq"] for r in rows),
                 len(rows))
    frequencies = roadm.get_frequency()
    checks.check("get_frequency: Hz", frequencies and all(1.8e14 < f < 2.0e14 for f in frequencies))
    with quiet():
        mux_power = roadm.get_mux_monitored_power()
        connection_powers = [roadm.get_mux_connection_input_power(), roadm.get_demux_connection_input_power(),
                             roadm.get_mux_connection_output_power(), roadm.get_demux_connection_output_power()]
    checks.check("monitored and connection power readers answer",
                 mux_power is not None and all(p is not None for p in connection_powers))
    with quiet():
        ports, legacy_ports = roadm.get_ports_info(), roadm.ports_get_info()
    checks.check("get_ports_info / ports_get_info answer", bool(ports) and bool(legacy_ports),
                 (len(ports), len(legacy_ports)))
    with quiet():
        info = roadm.get_edfa_info()
        legacy_info = roadm.edfa_get_info()
    checks.check("get_edfa_info: booster and preamp", set(info) >= {"booster", "preamp"} and bool(legacy_info))
    with quiet():
        values = [roadm.get_mux_target_gain(), roadm.get_demux_target_gain(), roadm.get_mux_target_power(),
                  roadm.get_demux_target_power(), roadm.get_mux_edfa_input_power(),
                  roadm.get_mux_edfa_output_power(), roadm.get_demux_edfa_input_power(),
                  roadm.get_demux_edfa_output_power()]
    checks.check("EDFA target and power readers return numbers", all(is_number(v) for v in values))
    tilts = [roadm.get_mux_target_tilt(), roadm.get_demux_target_tilt()]
    checks.check("get_mux/demux_target_tilt return floats", all(isinstance(t, float) for t in tilts), tilts)
    for module in (1, 2):
        state = roadm.edfa_state(module)
        checks.check("edfa_state(%d): input and output power" % module,
                     is_number(state.get("input-power")) and is_number(state.get("output-power")))
        checks.check("get_maintenance_state / get_gain_switch_mode(%d)" % module,
                     roadm.get_maintenance_state(module) in ("in-service", "out-of-service") and
                     roadm.get_gain_switch_mode(module) in ("low-gain", "high-gain"))
    line = roadm.line_port()
    checks.check("line_port: state and config", is_number(line["state"].get("outvoa-actual-attenuation")) and
                 is_number(line["config"].get("outvoa-target-attenuation")))


def write(roadm, device, checks, edfa_writes, unblock=False):
    blocked = sorted(int(dn.split("connection=")[1]) for kind, dn in elements(running(roadm))
                     if kind == "connection" and ";module=%d;" % DEMUX in dn and
                     connection_config(roadm, dn)["blocked"] == "true")[:2]
    checks.check("two blocked DEMUX connections to write on", len(blocked) == 2)
    if len(blocked) < 2:
        return
    first, second = blocked
    saved = connection_config(roadm, demux_dn(first))
    port = int(saved["output-port-reference"].split("port=")[1])

    attenuation = float(saved["attenuation"]) + STEP_DB
    with quiet():
        roadm.set_demux_atten(first, attenuation)
    checks.check("set_demux_atten", math.isclose(float(connection_config(roadm, demux_dn(first))["attenuation"]),
                                                 attenuation, abs_tol=0.051))
    targets = [float(connection_config(roadm, demux_dn(n))["attenuation"]) + STEP_DB for n in blocked]
    with quiet():
        roadm.wss_atten(DEMUX, port, blocked, targets)
    checks.check("wss_atten with parallel lists",
                 all(math.isclose(float(connection_config(roadm, demux_dn(n))["attenuation"]), t, abs_tol=0.051)
                     for n, t in zip(blocked, targets)))

    if unblock:
        with quiet():
            roadm.set_demux_unblock(first)
        checks.check("set_demux_unblock", connection_config(roadm, demux_dn(first))["blocked"] == "false")
    else:
        print("[skip] set_demux_unblock: needs --unblock (the band of the connection may carry light)")
    with quiet():
        roadm.set_demux_block(first)
    checks.check("set_demux_block", connection_config(roadm, demux_dn(first))["blocked"] == "true")

    other = 5201 if port != 5201 else 5202
    with quiet():
        roadm.set_demux_port(first, other - 5200)  # it takes the index of the port, 1 to 20
    checks.check("set_demux_port", connection_config(roadm, demux_dn(first))["output-port-reference"]
                 .endswith("port=%d" % other))
    with quiet():
        roadm.set_demux_port(first, port - 5200)

    current = connection_config(roadm, demux_dn(first))
    with quiet():
        roadm.wss_delete_connection(DEMUX, first)
    checks.check("wss_delete_connection", connection_config(roadm, demux_dn(first)) is None)
    with quiet():
        roadm.wss_add_connection(DEMUX, first, current["start-freq"], current["end-freq"], current["attenuation"],
                                 "true", current["maintenance-state"], DEMUX_INPUT, port, current["custom-name"])
    again = connection_config(roadm, demux_dn(first))
    checks.check("wss_add_connection adds it again",
                 again is not None and all(again[k] == current[k] for k in
                                           ("start-freq", "end-freq", "blocked", "custom-name", "maintenance-state",
                                            "output-port-reference")))
    connection = Lumentum.WSSConnection(DEMUX, first, current["maintenance-state"], "true", DEMUX_INPUT, port,
                                        current["start-freq"], current["end-freq"], current["attenuation"],
                                        current["custom-name"])
    with quiet():
        roadm.wss_add_connections([connection])
    checks.check("wss_add_connections applies a group", connection_config(roadm, demux_dn(first)) is not None)

    # line port output VOA, EDFA leaves written with the value they hold
    voa = float(roadm.line_port()["config"]["outvoa-target-attenuation"])
    target = voa + STEP_DB if voa + STEP_DB <= 20 else voa - STEP_DB
    roadm.set_outvoa_attenuation(target)
    checks.check("set_outvoa_attenuation", math.isclose(
        float(roadm.line_port()["config"]["outvoa-target-attenuation"]), target, abs_tol=0.051))
    roadm.set_outvoa_attenuation(voa)
    for module in (1, 2):
        state, mode = roadm.get_maintenance_state(module), roadm.get_gain_switch_mode(module)
        roadm.set_maintenance_state(module, state)
        roadm.set_gain_switch_mode(module, mode)
        checks.check("set_maintenance_state / set_gain_switch_mode(%d) with their value" % module,
                     (roadm.get_maintenance_state(module), roadm.get_gain_switch_mode(module)) == (state, mode))
    checks.check("disable_als reports success", roadm.disable_als(duration=1) is True)

    if edfa_writes:
        with quiet():
            info = roadm.get_edfa_info()
        for name, prefix in (("booster", "mux"), ("preamp", "demux")):
            edfa = info[name]
            tilt = float(edfa["target_gain_tilt"])
            with quiet():
                if edfa["control_mode"] == "constant-gain":
                    getattr(roadm, "set_%s_constant_gain" % prefix)(float(edfa["target_gain"]), tilt)
                else:
                    getattr(roadm, "set_%s_constant_power" % prefix)(float(edfa["target_power"]), tilt)
            time.sleep(2)
            module = 1 if name == "booster" else 2
            checks.check("set_%s_constant_* with its values: back in service" % prefix,
                         roadm.get_maintenance_state(module) == "in-service")

    # validation: nothing is sent
    checks.raises("set_maintenance_state refuses an unknown state", lambda: roadm.set_maintenance_state(1, "maint"))
    checks.raises("set_gain_switch_mode refuses an unknown mode", lambda: roadm.set_gain_switch_mode(1, "mid-gain"))
    checks.raises("set_outvoa_attenuation refuses a negative value", lambda: roadm.set_outvoa_attenuation(-1.0))
    checks.raises("wss_atten refuses lists of different length",
                  lambda: roadm.wss_atten(DEMUX, port, blocked, [1.0]))
    checks.raises("an unknown ROADM is refused", lambda: Lumentum("roadm_unknown"))

    with quiet():
        reconnecting = Lumentum(device, timeout=60, reconnect=True)
    with reconnecting:
        with quiet():
            count = len(reconnecting.wss_get_connections()["demux"])
        checks.check("reconnect=True wraps the session and reads",
                     isinstance(reconnecting.m, _netconf.ReconnectingSession) and count > 0)


def restore(roadm, saved_xml):
    """Write back, as saved, every restored element that differs; the ones still different."""
    saved = elements(saved_xml)
    for attempt in range(2):
        current = elements(running(roadm))
        different = [key for key in saved if current.get(key) != saved[key]]
        extra = [key for key in current if key not in saved]
        if not different and not extra:
            return []
        for kind, dn in extra:
            if kind == "connection":
                module, number = dn.split("module=")[1].split(";connection=")
                with quiet():
                    roadm.wss_delete_connection(int(module), int(number))
        for kind, dn in different:
            body = saved[(kind, dn)].decode()
            config = ('<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0"><%s xmlns="%s">%s</%s>'
                      "</xc:config>" % (CONTAINERS[kind], RESTORED[kind], body, CONTAINERS[kind]))
            try:
                roadm.m.edit_config(target="running", config=config)
            except Exception as error:
                print("[warn] restoring %s: %s" % (kind, type(error).__name__))
        time.sleep(1)
    current = elements(running(roadm))
    return ["%s %s" % key for key in saved if current.get(key) != saved[key]] + \
           ["%s %s" % key for key in current if key not in saved]


def preamp_state(roadm):
    return roadm.edfa_state(2).get("operational-state")


def bring_line_back(roadm, initial):
    """Wait for the preamp to be back in its ``initial`` operational state; when it does not come back by itself,
    suspend the automatic laser shutdown so that the booster lights the line again. Whether it is back."""
    deadline = time.time() + 20
    while time.time() < deadline:
        if preamp_state(roadm) == initial:
            return True
        time.sleep(2)
    print("[info] the preamp is not back by itself: suspending the automatic laser shutdown for %d s"
          % ALS_SUSPEND_S)
    roadm.disable_als(duration=ALS_SUSPEND_S)
    deadline = time.time() + LINE_WAIT_S
    while time.time() < deadline:
        if preamp_state(roadm) == initial:
            return True
        time.sleep(5)
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("roadm", help="a ROADM name the driver knows")
    parser.add_argument("--edfa-writes", action="store_true",
                        help="also run the legacy EDFA setters (each interrupts the line for about a second)")
    parser.add_argument("--unblock", action="store_true",
                        help="also unblock the blocked test connection for a moment (its band may carry light)")
    parser.add_argument("--show-values", action="store_true")
    args = parser.parse_args()

    checks = Checks(args.show_values)
    started = time.time()
    with quiet():
        roadm = Lumentum(args.roadm, timeout=60)
    with roadm:
        initial = running(roadm)
        initial_preamp = preamp_state(roadm)
        checks.check("running configuration read twice the same", initial == running(roadm),
                     "%d chars" % len(initial))
        try:
            read(roadm, checks)
            write(roadm, args.roadm, checks, args.edfa_writes, args.unblock)
        except KeyboardInterrupt:  # Ctrl-C: the configuration and the line are still put back below
            checks.check("the test ran to the end (interrupted)", False)
        except Exception as error:
            checks.check("the test ran to the end (%s: %s)" % (type(error).__name__, error), False)
        with interrupts_held():
            different = restore(roadm, initial)
            checks.check("configuration elements put back", different == [], different)
            checks.check("running configuration back to the initial one", running(roadm) == initial)
            checks.check("preamp back in its initial operational state", bring_line_back(roadm, initial_preamp),
                         initial_preamp)
    print("duration: %.1f s" % (time.time() - started))
    print("%d checks failed%s" % (len(checks.failed), ": " + ", ".join(checks.failed) if checks.failed else ""))
    sys.exit(1 if checks.failed else 0)


if __name__ == "__main__":
    main()
