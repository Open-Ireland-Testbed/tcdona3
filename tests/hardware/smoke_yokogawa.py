"""Hardware smoke test of ``tcdona3.yokogawa.YokogawaOSA`` (AQ6370E).

Usage (from the parent directory of the tcdona3 package)::

    python3 tcdona3/tests/hardware/smoke_yokogawa.py <credentials>.json [--show-values]

``<credentials>.json`` holds ``ip_address`` and optionally ``ip_port``, ``username`` and ``password`` (the
driver's defaults otherwise). The connection goes through the booking check of :meth:`YokogawaOSA.connect`.

The script saves the settings of the instrument (:data:`SETTINGS`) first, exercises the getters, the setters,
the sweeps and the trace readers, and at the end, also when a check fails or the run is interrupted (Ctrl-C), puts every setting back: with the
driver's setters, then by writing the saved answer for what is still different. It exits with an error when a
setting could not be put back; a sweep running at the start is left stopped (an ``[info]`` line says so). The traces of the instrument are overwritten by the sweeps. Not exercised: the
``store_*`` methods (they write files on the instrument), ``set_peak_search`` (it moves the marker, which this
model does not let one read back), ``set_attenuator`` and ``set_sweep_points`` (their nodes are not queryable
on this model). The average count and the FIX trace attribute are only reported: the AQ6370E ignores the
first and queues an error code for the second, although it applies it. Measured values are printed only with ``--show-values``.
"""

import argparse
import contextlib
import io
import json
import math
import os
import signal
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from tcdona3.yokogawa import SENSITIVITY_RESPONSE, SWEEP_MODE_RESPONSE, YokogawaOSA  # noqa: E402

#: The settings the test may change, as queries; each is put back at the end.
SETTINGS = (":SENSe:CORRection:RVELocity:MEDium?", ":SENSe:CORRection:WAVelength:SHIFt?",
            ":SENSe:CORRection:LEVel:SHIFt?", ":SENSe:WAVelength:STARt?", ":SENSe:WAVelength:STOP?",
            ":SENSe:WAVelength:CENTer?", ":SENSe:WAVelength:SPAN?", ":SENSe:BANDwidth:RESolution?",
            ":SENSe:SETTing:CORRection?", ":SENSe:SENSe?", ":SENSe:CHOPper?", ":SENSe:SWEep:SPEed?",
            ":SENSe:AVERage:COUNt?", ":SENSe:SETTing:SMOothing?", ":SENSe:SWEep:POINts:AUTO?",
            ":SENSe:SWEep:POINts?", ":SENSe:SWEep:STEP?", ":DISPlay:TRACe:Y1:SCALe:RLEVel?",
            ":DISPlay:TRACe:Y1:SCALe:PDIVision?", ":DISPlay:TRACe:X:CENTer?", ":DISPlay:TRACe:X:SPAN?",
            ":CALCulate:CATegory?", ":CALCulate:AUTO?", ":TRACe:ATTRibute:TRA?", ":TRACe:STATe:TRA?",
            ":TRACe:ACTive?", ":INITiate:SMODe?")

#: Points of the test sweeps (few, to keep them short) and the limit of one sweep [s].
POINTS, SWEEP_TIMEOUT_S = 501, 180


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
            call()
        except error:
            self.check(name, True)
        else:
            self.check(name, False)


def close(a, b, tolerance):
    return a is not None and b is not None and abs(a - b) <= tolerance


@contextlib.contextmanager
def quiet():
    """The driver prints its replies (the login, the identity): keep them out of the report."""
    with contextlib.redirect_stdout(io.StringIO()):
        yield

@contextlib.contextmanager
def interrupts_held():
    """Ctrl-C is held while the device is put back: the restore runs to its end, then the run ends."""
    received = []

    def hold(signum, frame):
        if not received:
            print("[info] Ctrl-C: the device is being put back first")
        received.append(signum)

    previous = signal.signal(signal.SIGINT, hold)
    try:
        yield received
    finally:
        signal.signal(signal.SIGINT, previous)



def errors(osa):
    """Drain the error queue of the instrument; the codes other than 0."""
    found = []
    for _ in range(20):
        answer = osa._query(":SYSTem:ERRor?")
        if answer.split(",")[0].strip() in ("0", "+0"):
            break
        found.append(answer.split(",")[0].strip())
    return found


def read_settings(osa):
    return {query: osa._query(query) for query in SETTINGS}


def number(text):
    return float(text)


def restore(osa, saved):
    """Put the saved settings back with the driver's setters, then by writing the saved answer of each one
    still different. The queries still different afterwards."""
    s = saved
    steps = [
        lambda: osa.sweep_stop(),
        lambda: osa.osa.write(":SENSe:CORRection:RVELocity:MEDium %s" % s[":SENSe:CORRection:RVELocity:MEDium?"]),
        lambda: osa.set_wavelength_offset(number(s[":SENSe:CORRection:WAVelength:SHIFt?"]) * 1e9),
        lambda: osa.set_level_offset(number(s[":SENSe:CORRection:LEVel:SHIFt?"])),
        lambda: osa.set_start_wavelength(number(s[":SENSe:WAVelength:STARt?"]) * 1e9),
        lambda: osa.set_stop_wavelength(number(s[":SENSe:WAVelength:STOP?"]) * 1e9),
        lambda: osa.set_start_wavelength(number(s[":SENSe:WAVelength:STARt?"]) * 1e9),
        lambda: osa.set_resolution(number(s[":SENSe:BANDwidth:RESolution?"]) * 1e9),
        lambda: osa.set_resolution_correction(s[":SENSe:SETTing:CORRection?"] == "1"),
        lambda: osa.set_sensitivity(SENSITIVITY_RESPONSE[s[":SENSe:SENSe?"]]),
        lambda: osa.osa.write(":SENSe:CHOPper %s" % s[":SENSe:CHOPper?"]),
        lambda: osa.osa.write(":SENSe:SWEep:SPEed %s" % s[":SENSe:SWEep:SPEed?"]),
        lambda: osa.set_average_count(int(number(s[":SENSe:AVERage:COUNt?"]))),
        lambda: osa.set_smoothing(s[":SENSe:SETTing:SMOothing?"] == "1"),
        lambda: osa.set_sampling_points(int(number(s[":SENSe:SWEep:POINts?"]))),
        lambda: osa.set_auto_sweep_points(s[":SENSe:SWEep:POINts:AUTO?"] == "1"),
        lambda: osa.set_reference_level(number(s[":DISPlay:TRACe:Y1:SCALe:RLEVel?"])),
        lambda: osa.osa.write(":DISPlay:TRACe:Y1:SCALe:PDIVision %s" % s[":DISPlay:TRACe:Y1:SCALe:PDIVision?"]),
        lambda: osa.osa.write(":DISPlay:TRACe:X:SPAN %s" % s[":DISPlay:TRACe:X:SPAN?"]),
        lambda: osa.set_center_wavelength(number(s[":DISPlay:TRACe:X:CENTer?"]) * 1e9),
        lambda: osa.osa.write(":CALCulate:CATegory %s" % s[":CALCulate:CATegory?"]),
        lambda: osa.set_auto_measure(s[":CALCulate:AUTO?"] == "1"),
        lambda: osa.osa.write(":TRACe:ATTRibute:TRA %s" % s[":TRACe:ATTRibute:TRA?"]),
        lambda: osa.osa.write(":TRACe:STATe:TRA %s" % s[":TRACe:STATe:TRA?"]),
        lambda: osa.osa.write(":TRACe:ACTive %s" % s[":TRACe:ACTive?"]),
        lambda: osa.osa.write(":INITiate:SMODe %s" % s[":INITiate:SMODe?"]),
    ]
    for step in steps:
        try:
            with quiet():
                step()
        except Exception as error:
            print("[warn] restoring: %s" % type(error).__name__)
    current = read_settings(osa)
    for query in SETTINGS:
        if current[query] != saved[query]:
            osa.osa.write("%s %s" % (query[:-1], saved[query]))
    current = read_settings(osa)
    mode = SWEEP_MODE_RESPONSE.get(str(saved[":INITiate:SMODe?"]).strip().upper())
    if mode in ("repeat", "auto"):
        # the mode is put back, but whether a sweep was running at the start is not known: the test leaves the
        # sweep stopped and says so (starting one could overwrite a trace someone froze)
        print("[info] the instrument was in %s sweep mode at the start; the sweep is left stopped: start it "
              "again (:INITiate) if it was running" % mode)
    return [query for query in SETTINGS if current[query] != saved[query]]


def exercise(osa, checks, workdir):
    with quiet():
        idn = osa.identify()
    checks.check("identify answers YOKOGAWA", "YOKOGAWA" in str(idn).upper())

    # typed getters
    readings = {name: getattr(osa, name)() for name in (
        "get_center_wavelength", "get_span", "get_start_wavelength", "get_stop_wavelength", "get_resolution",
        "get_resolution_correction", "get_sensitivity", "get_chopper", "get_sweep_speed", "get_sweep_mode",
        "get_average_count", "get_sampling_auto", "get_sampling_points", "get_sampling_step", "get_smoothing",
        "get_level_offset", "get_reference_level", "get_wavelength_offset", "get_wavelength_medium",
        "get_trace_attribute", "get_auto_measure")}
    checks.check("typed getters answer", all(value is not None for value in readings.values()))
    checks.check("start + span = stop",
                 close(readings["get_start_wavelength"] + readings["get_span"], readings["get_stop_wavelength"],
                       0.01))

    # wavelength axis
    osa.set_start_wavelength(1545.0)
    osa.set_stop_wavelength(1555.0)
    checks.check("set_start/stop_wavelength", close(osa.get_start_wavelength(), 1545.0, 0.002) and
                 close(osa.get_stop_wavelength(), 1555.0, 0.002))
    checks.check("centre and span follow", close(osa.get_center_wavelength(), 1550.0, 0.002) and
                 close(osa.get_span(), 10.0, 0.002))
    osa.set_span(6.0)
    checks.check("set_span keeps the centre", close(osa.get_span(), 6.0, 0.002) and
                 close(osa.get_center_wavelength(), 1550.0, 0.002))
    with quiet():
        osa.set_wavelength_center(1550.5)
        osa.set_wavelength_span(4.0)
    checks.check("legacy set_wavelength_center/span", close(osa.get_center_wavelength(), 1550.5, 0.002) and
                 close(osa.get_span(), 4.0, 0.002))
    with quiet():
        osa.set_wavelength_range(1546.0, 1554.0)
    checks.check("legacy set_wavelength_range", close(osa.get_start_wavelength(), 1546.0, 0.002) and
                 close(osa.get_stop_wavelength(), 1554.0, 0.002))
    with quiet():
        osa.set_center_wavelength(1551.0)
    checks.check("legacy set_center_wavelength moves the display centre",
                 close(number(osa._query(":DISPlay:TRACe:X:CENTer?")) * 1e9, 1551.0, 0.002))

    # resolution
    osa.set_resolution(0.1)
    checks.check("set_resolution", close(osa.get_resolution(), 0.1, 1e-4))
    with quiet():
        osa.set_resolution_bandwidth("0.05NM")
        rbw = osa.get_resolution_bandwidth()
    checks.check("legacy set/get_resolution_bandwidth (metres)", close(number(rbw) * 1e9, 0.05, 1e-4), rbw)
    osa.set_resolution(0.1)
    correction = osa.get_resolution_correction()
    osa.set_resolution_correction(not correction)
    checks.check("set_resolution_correction", osa.get_resolution_correction() is (not correction))

    # detection and sweep settings
    osa.set_sensitivity("mid")
    checks.check("set_sensitivity", osa.get_sensitivity() == "mid")
    osa.set_chopper(osa.get_chopper())
    checks.check("set_chopper (same mode)", osa.get_chopper() in ("off", "switch"))
    for speed in ("1x", "2x"):
        osa.set_sweep_speed(speed)
        checks.check("set_sweep_speed %s" % speed, osa.get_sweep_speed() == speed)
    # the AQ6370E accepts the command without an error but keeps its count: only reported
    count = osa.get_average_count()
    osa.set_average_count(count + 1)
    applied = osa.get_average_count() == count + 1
    print("[info] set_average_count: %s" % ("applied" if applied else "accepted but not applied by the instrument"))
    osa.set_average_count(count)
    for enabled in (True, False):
        osa.set_smoothing(enabled)
        checks.check("set_smoothing %s" % enabled, osa.get_smoothing() is enabled)

    # sampling
    osa.set_sampling_points(1001)
    checks.check("set_sampling_points turns auto off", osa.get_sampling_points() == 1001 and
                 not osa.get_sampling_auto())
    osa.set_sampling_step(0.016)
    checks.check("set_sampling_step", close(osa.get_sampling_step(), 0.016, 0.002), osa.get_sampling_step())
    with quiet():
        osa.set_auto_sweep_points(True)
    checks.check("legacy set_auto_sweep_points", osa.get_sampling_auto() is True)
    osa.set_sampling_points(POINTS)

    # levels and corrections
    osa.set_level_offset(0.5)
    checks.check("set_level_offset", close(osa.get_level_offset(), 0.5, 1e-3))
    osa.set_level_offset(0.0)
    osa.set_wavelength_offset(0.01)
    checks.check("set_wavelength_offset", close(osa.get_wavelength_offset(), 0.01, 1e-4))
    osa.set_wavelength_offset(0.0)
    osa.set_reference_level(-20.0)
    checks.check("set_reference_level", close(osa.get_reference_level(), -20.0, 1e-3))
    medium = osa.get_wavelength_medium()
    other = "air" if medium == "vacuum" else "vacuum"
    osa.set_wavelength_medium(other)
    checks.check("set_wavelength_medium", osa.get_wavelength_medium() == other)
    osa.set_wavelength_medium(medium)

    # trace and analysis (FIX is applied, but the AQ6370E queues code 101 for it: only reported)
    checks.check("no instrument error after the setters", errors(osa) == [], errors(osa))
    osa.set_trace_attribute("fix")
    checks.check("set_trace_attribute", osa.get_trace_attribute() == "fix")
    print("[info] set_trace_attribute('fix') error codes: %s" % (errors(osa) or "none"))
    osa.set_trace_attribute("write")
    with quiet():
        osa.set_auto_measure(False)
        status = osa.get_auto_measure_status()
    checks.check("legacy set_auto_measure / get_auto_measure_status", osa.get_auto_measure() is False and
                 status.strip() == "0")

    # validation: nothing is written
    checks.raises("set_start_wavelength refuses 2000 nm", lambda: osa.set_start_wavelength(2000.0))
    checks.raises("set_resolution refuses a non preset", lambda: osa.set_resolution(0.3))
    checks.raises("set_average_count refuses 0", lambda: osa.set_average_count(0))
    checks.raises("set_sensitivity refuses an unknown mode", lambda: osa.set_sensitivity("fast"))
    checks.check("no instrument error after the trace setters", errors(osa) == [], errors(osa))

    # sweeps and traces
    started = time.time()
    wavelength, power = osa.read_trace(timeout_s=SWEEP_TIMEOUT_S)
    elapsed = time.time() - started
    checks.check("read_trace: one sample per point", len(wavelength) == len(power) == POINTS, len(wavelength))
    checks.check("read_trace: wavelengths in nm inside the range, increasing",
                 close(wavelength[0], 1546.0, 0.01) and close(wavelength[-1], 1554.0, 0.01) and
                 all(b > a for a, b in zip(wavelength, wavelength[1:])))
    checks.check("read_trace: finite levels in dBm", all(math.isfinite(p) and -220 < p < 30 for p in power),
                 (float(min(power)), float(max(power))))
    checks.check("read_trace left the single sweep mode", osa.get_sweep_mode() == "single")
    print("single sweep: %.1f s" % elapsed)

    with quiet():
        osa.wait_for_sweep_complete(timeout_s=SWEEP_TIMEOUT_S)
        y, x = osa.get_trace_data(), osa.get_wavelength_data()
    checks.check("legacy wait_for_sweep_complete + get_trace/wavelength_data",
                 len(y.split(",")) == len(x.split(",")) == POINTS and close(float(x.split(",")[0]) * 1e9, 1546.0, 0.01))
    with quiet():
        width = osa.measure_spectrum_width()
    checks.check("legacy measure_spectrum_width answers", bool(str(width).strip()))
    json_path, fig_path = os.path.join(workdir, "trace.json"), os.path.join(workdir, "spectrum.png")
    with quiet():
        saved_x, saved_y = osa.save_trace_data(json_path=json_path, fig_path=fig_path)
    checks.check("legacy save_trace_data writes the JSON and the figure",
                 os.path.getsize(json_path) > 0 and os.path.getsize(fig_path) > 0 and len(saved_x) == POINTS)

    with quiet():
        osa.set_sweep_mode("repeat")
    checks.check("legacy set_sweep_mode", osa.get_sweep_mode() == "repeat")
    osa.sweep_repeat()
    time.sleep(2)
    osa.sweep_stop()
    with quiet():
        osa.abort()
    osa.sweep_single()
    deadline = time.time() + SWEEP_TIMEOUT_S
    while not osa._sweep_complete() and time.time() < deadline:
        time.sleep(0.2)
    checks.check("sweep_repeat / sweep_stop / sweep_single", osa.get_sweep_mode() == "single" and
                 time.time() < deadline)
    with quiet():
        osa.osa.write("*CLS")
        osa.sweep()
    deadline = time.time() + SWEEP_TIMEOUT_S
    while not osa._sweep_complete() and time.time() < deadline:
        time.sleep(0.2)
    checks.check("legacy sweep starts a sweep", time.time() < deadline)
    checks.check("no instrument error after the sweeps", errors(osa) == [], errors(osa))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("credentials", help="JSON file with ip_address (and optionally ip_port, username, password)")
    parser.add_argument("--timeout-ms", type=int, default=60000)
    parser.add_argument("--show-values", action="store_true")
    args = parser.parse_args()

    with open(args.credentials) as handle:
        credentials = json.load(handle)
    login = {key: credentials[source] for key, source in (("user", "username"), ("password", "password"))
             if source in credentials}
    checks = Checks(args.show_values)
    osa = YokogawaOSA(credentials["ip_address"], int(credentials.get("ip_port", 10001)),
                      timeout_ms=args.timeout_ms, **login)
    with quiet():
        osa.connect()
    try:
        osa.osa.write("*CLS")
        saved = read_settings(osa)
        checks.check("settings read before the test", all(saved.values()), "%d settings" % len(saved))
        with tempfile.TemporaryDirectory() as workdir:
            try:
                exercise(osa, checks, workdir)
            except KeyboardInterrupt:  # Ctrl-C: the settings are still put back below
                checks.check("the test ran to the end (interrupted)", False)
            except Exception as error:
                checks.check("the test ran to the end (%s: %s)" % (type(error).__name__, error), False)
        with interrupts_held():
            different = restore(osa, saved)
            checks.check("settings put back at the end", different == [], different)
            if different:
                print("Not put back: %s" % ", ".join(different))
    finally:
        with quiet():
            osa.close()

    print("%d checks failed%s" % (len(checks.failed), ": " + ", ".join(checks.failed) if checks.failed else ""))
    sys.exit(1 if checks.failed else 0)


if __name__ == "__main__":
    main()
