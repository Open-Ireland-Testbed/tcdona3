"""Hardware smoke test of ``tcdona3.mts4000.MTS4000`` on a Viavi MTS 4000 (OTDR module).

Usage (from the parent directory of the tcdona3 package)::

    python3 tcdona3/tests/hardware/smoke_mts4000.py [HOST] [--wavelength 1550] [--pulse 100ns]
                                                    [--range-km 20] [--time 30] [--acquire]
                                                    [--output zresults/smoke_mts4000] [--show-values]

``HOST`` defaults to ``$MTS4000_HOST`` or ``10.10.10.150``.  By default the script only reads the
identity, configuration, result metadata, current trace and event table: it never fires the laser and
writes no setting.  Connecting does switch the OTDR function on and bring the OTDR tab to the front of
the instrument's shared screen, as every :class:`MTS4000` session does.

With ``--acquire`` it applies the requested settings, runs one acquisition, saves the ``.json`` /
``_trace.csv`` / ``.sor`` files under ``--output`` and then, in the same session, stops the acquisition
if it is still running and writes back the settings read at the start: the wavelength and its pulse,
the pulse of the wavelength it selected (pulses are set per wavelength), the range or auto range and
the acquisition time, whatever the flags (an acquisition in auto mode may choose its own pulse and
range).  It then reads them again and fails when they differ from the start.  This also happens when a
check fails or the run is interrupted with Ctrl-C.  Ctrl-C during the acquisition ends it; Ctrl-C during
the restore is held until the restore is done.  When one of these settings cannot be read at the start,
it does not acquire.  The script exits non-zero when a check or the restore fails.  Measured values are
printed only with ``--show-values``.
"""

import argparse
import contextlib
import os
import signal
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from tcdona3.mts4000 import DEFAULT_HOST, MTS4000, MTS4000Error  # noqa: E402



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


def same_wavelength(first, second):
    return (first or "").strip().upper() == (second or "").strip().upper()


def baseline_gaps(config):
    """The settings ``--acquire`` puts back that could not be read at the start."""
    gaps = []
    if not config.wavelength:
        gaps.append("wavelength")
    if not config.pulse_width:
        gaps.append("pulse width")
    if config.auto_range is None or (not config.auto_range and config.range_km is None):
        gaps.append("range")
    if config.acquisition_time_s is None:
        gaps.append("acquisition time")
    return gaps


def restore_plan(config):
    """``configure()`` calls that put the settings of ``config`` back, applied in order.

    All of them, whatever the flags: an acquisition in auto mode may change the pulse and range itself.
    :func:`run_acquire` adds one in front for the pulse of the wavelength it selects."""
    original = {"wavelength": config.wavelength, "pulse_width": config.pulse_width,
                "acquisition_time_s": config.acquisition_time_s}
    if config.auto_range:
        original["auto_range"] = True
    else:
        original["range_km"] = config.range_km
    return [original]


def restored_differences(start, now):
    """The settings of ``restore_plan`` that read differently now than at the start."""
    names = ["wavelength", "pulse_width", "auto_range", "acquisition_time_s"]
    if not start.auto_range:
        names.append("range_km")
    return [name for name in names if getattr(start, name) != getattr(now, name)]


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

    metadata = device.get_result_metadata()
    # acquire() compares this date before and after an acquisition to tell a new result from the old one.
    checks.check("get_result_metadata: result date reported", bool(metadata.get("date")), metadata.get("date"))

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


def run_acquire(device, checks, args, plan):
    if args.wavelength is not None:
        selected = device.configure(wavelength=args.wavelength)
        if not same_wavelength(selected.wavelength, plan[-1]["wavelength"]):
            # The pulse is set per wavelength: note this wavelength's own before changing it.
            if not selected.pulse_width:
                raise MTS4000Error("could not read the pulse of %s, so it would not be put back"
                                   % selected.wavelength)
            plan.insert(0, {"wavelength": selected.wavelength, "pulse_width": selected.pulse_width})
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


def stop_if_running(device, checks):
    """Stop an acquisition left running (after Ctrl-C or an error during it)."""
    try:
        if device.acquisition_status() == "IN_PROGRESS":
            device.stop_acquisition()
            checks.check("acquisition stopped", device.acquisition_status() != "IN_PROGRESS")
    except (MTS4000Error, OSError) as exc:
        checks.check("acquisition stopped", False, str(exc))


def restore(device, checks, plan, start):
    for settings in plan:
        try:
            device.configure(**settings)
        except (MTS4000Error, OSError, ValueError) as exc:
            checks.check("restore: %s written back" % ", ".join(sorted(settings)), False, str(exc))
    try:
        different = restored_differences(start, device.get_config())
    except (MTS4000Error, OSError, ValueError) as exc:
        checks.check("restore: settings read back", False, str(exc))
    else:
        checks.check("restore: settings back to those at the start", different == [], different)


def reopen(device, checks):
    """A reply cut off by Ctrl-C or an error may still be on its way: talk on a new session."""
    try:
        device.close()
        device.connect()
    except (MTS4000Error, OSError) as exc:
        checks.check("session opened again for the restore", False, str(exc))


def acquire_and_restore(device, checks, args, config):
    plan = restore_plan(config)
    cut_off = False
    try:
        run_acquire(device, checks, args, plan)
    except KeyboardInterrupt:  # Ctrl-C ends the acquisition; the instrument is still put back below
        checks.check("the acquisition ran to the end (interrupted)", False)
        cut_off = True
    except (MTS4000Error, OSError, ValueError) as exc:
        checks.check("the acquisition ran to the end (%s: %s)" % (type(exc).__name__, exc), False)
        cut_off = True
    finally:
        with interrupts_held():
            if cut_off:
                reopen(device, checks)
            stop_if_running(device, checks)
            restore(device, checks, plan, config)


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
    try:
        with device:
            checks.check("connect: data port resolved", device.data_port is not None,
                         device.data_port)
            config = run_read_only(device, checks, args)
            if args.acquire:
                gaps = baseline_gaps(config)
                if gaps:
                    checks.check("--acquire: %s could not be read to be put back, so no acquisition"
                                 % ", ".join(gaps), False)
                else:
                    acquire_and_restore(device, checks, args, config)
    except KeyboardInterrupt:  # before any setting was written: nothing to put back
        checks.check("the test ran to the end (interrupted)", False)
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
