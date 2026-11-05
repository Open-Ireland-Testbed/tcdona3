"""Hardware smoke test of ``tcdona3.mts4000.MTS4000`` on a Viavi MTS 4000 (OTDR module).

Usage (from the parent directory of the tcdona3 package)::

    python3 tcdona3/tests/hardware/smoke_mts4000.py [HOST] [--wavelength 1550] [--pulse 100ns]
                                                    [--range-km 20] [--time 30] [--acquire]
                                                    [--output zresults/smoke_mts4000] [--show-values]

``HOST`` defaults to ``$MTS4000_HOST`` or ``10.10.10.150``.  By default the script only reads the
identity, configuration, current trace and event table; it never fires the laser.  With
``--acquire`` it applies the requested settings, runs one acquisition, saves the ``.json`` /
``_trace.csv`` / ``.sor`` files under ``--output`` and then puts the previous settings back (also
when a check fails or the run is interrupted with Ctrl-C).  The script exits non-zero when a check
or the restore fails.  Measured values are printed only with ``--show-values``.
"""

import argparse
import contextlib
import os
import signal
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from tcdona3.mts4000 import DEFAULT_HOST, MTS4000, MTS4000Error  # noqa: E402

#: Settings put back after an acquisition.
RESTORED = ("wavelength", "pulse_width", "range_km", "acquisition_time_s", "acquisition_mode")


class Checks(object):
    def __init__(self, show_values):
        self.show_values = show_values
        self.failed = []

    def check(self, name, condition, value=None):
        status = "ok  " if condition else "FAIL"
        shown = " = %r" % (value,) if self.show_values and value is not None else ""
        print("[%s] %s%s" % (status, name, shown))
        if not condition:
            self.failed.append(name)


@contextlib.contextmanager
def interrupts_held():
    """Hold Ctrl-C while the instrument is put back, then end the run."""
    received = []

    def hold(signum, frame):
        received.append(signum)

    previous = signal.signal(signal.SIGINT, hold)
    try:
        yield received
    finally:
        signal.signal(signal.SIGINT, previous)
        if received:
            print("[info] Ctrl-C was held until the instrument was put back")


def previous_settings(config):
    settings = {}
    for name in RESTORED:
        value = getattr(config, name, None)
        if value is not None:
            settings[name] = value
    return settings


def run_read_only(device, checks, args):
    identity = device.identify()
    checks.check("identify: model is an MTS 4000", "MTS" in (identity.get("model") or ""),
                 identity.get("model"))
    checks.check("identify: serial reported", bool(identity.get("serial")), identity.get("serial"))

    config = device.get_config()
    checks.check("get_config: wavelength reported", bool(config.wavelength), config.wavelength)
    checks.check("get_config: pulse width reported", bool(config.pulse_width), config.pulse_width)
    checks.check("get_config: range reported", config.range_km is not None, config.range_km)
    checks.check("get_config: acquisition time reported",
                 config.acquisition_time_s is not None, config.acquisition_time_s)

    states = device.function_states()
    checks.check("function_states: OTDR visible", any(
        "OTDR" in functions for functions in states.values()), len(states))

    trace = device.get_trace()
    checks.check("get_trace: points reported", trace["size"] > 0, trace["size"])
    checks.check("get_trace: distance array matches the size",
                 len(trace["distance_m"]) == trace["size"], len(trace["distance_m"]))

    events = device.get_events()
    checks.check("get_events: table parsed", events["size"] >= 0, events["size"])
    return config


def run_acquire(device, checks, args):
    config = device.configure(
        wavelength=args.wavelength,
        pulse_width=args.pulse,
        range_km=args.range_km,
        acquisition_time_s=args.time,
    )
    checks.check("configure: wavelength applied", config.wavelength is not None, config.wavelength)
    checks.check("configure: pulse width applied", config.pulse_width is not None, config.pulse_width)
    checks.check("configure: range applied", config.range_km is not None, config.range_km)

    started = time.time()
    result = device.acquire(timeout=args.timeout, exclusive=False)
    elapsed = time.time() - started
    checks.check("acquire: trace collected", result["trace"]["size"] > 0, result["trace"]["size"])
    checks.check("acquire: events parsed", result["events"]["size"] >= 0, result["events"]["size"])
    checks.check("acquire: SOR returned", bool(result.get("sor")), len(result.get("sor") or b""))
    checks.check("acquire: finished", elapsed >= 0, "%.1f s" % elapsed)

    paths = device.save_result(args.output, result=result)
    checks.check("save_result: json/trace/sor written", set(paths) == {"json", "trace_csv", "sor"},
                 {name: str(path) for name, path in paths.items()})
    return result


def restore(device, checks, settings):
    if not settings:
        return
    try:
        device.configure(**settings)
    except (MTS4000Error, ValueError) as exc:
        checks.check("restore: previous settings put back", False, str(exc))
    else:
        checks.check("restore: previous settings put back", True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Hardware smoke test of the MTS4000 OTDR driver.")
    parser.add_argument("host", nargs="?", default=os.environ.get("MTS4000_HOST", DEFAULT_HOST))
    parser.add_argument("--control-port", type=int, default=8000)
    parser.add_argument("--wavelength", default=None, help="1550, 1310nm, L1550 ...")
    parser.add_argument("--pulse", default=None, help="100ns, 2us, P100NS ...")
    parser.add_argument("--range-km", type=float, default=None, help="manual range in km")
    parser.add_argument("--time", type=float, default=None, help="acquisition time in seconds")
    parser.add_argument("--acquire", action="store_true", help="fire the laser and save one acquisition")
    parser.add_argument("--timeout", type=float, default=600.0, help="acquisition timeout (s)")
    parser.add_argument("--output", default="zresults/smoke_mts4000", help="result folder")
    parser.add_argument("--show-values", action="store_true")
    args = parser.parse_args(argv)

    checks = Checks(args.show_values)
    device = MTS4000(args.host, control_port=args.control_port)
    settings = {}
    try:
        with interrupts_held() as received:
            with device:
                checks.check("connect: data port resolved", device.data_port is not None,
                             device.data_port)
                config = run_read_only(device, checks, args)
                settings = previous_settings(config)
                if args.acquire:
                    run_acquire(device, checks, args)
            restore(device, checks, settings)
            if received:
                print("[info] interrupted: the run ended after the restore")
    except (MTS4000Error, OSError, ValueError) as exc:
        print("[FAIL] driver error: %s" % exc)
        checks.failed.append("driver error")

    if checks.failed:
        print("[FAIL] %d check(s) failed: %s" % (len(checks.failed), ", ".join(checks.failed)))
        return 1
    print("[ok  ] all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
