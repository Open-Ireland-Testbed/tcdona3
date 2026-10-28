"""Hardware smoke test of ``tcdona3.ila.ILA`` (Juniper TCX-1000) on one in-line amplifier.

Usage (from the parent directory of the tcdona3 package)::

    python3 tcdona3/tests/hardware/smoke_ila.py <device> [--show-values]

``<device>`` is a name the driver knows (``ila_1`` ... ``ila_11``); the connection goes through the booking check
of the driver.

The script saves the running configuration and the set-points of both directions first, exercises the reads,
the write transaction (each set-point moved by :data:`STEP_DB` and read back, the state and the gain range
written with the value they hold, a rejected edit), the options (``setpoint_digits="device"``,
``evoa_numbering="measured"``, ``reconnect=True``) and the validation, and at the end, also when a check fails
or the run is interrupted (Ctrl-C),
writes every saved set-point back as the device returned it. It fails when the running configuration is not
the initial one. The amplifier is never disabled and its gain range never switched, but the gain, tilt and
EVOA moves change the power on the line for a few seconds. Measured values are printed only with
``--show-values``.
"""

import argparse
import contextlib
import io
import math
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from tcdona3 import _netconf  # noqa: E402
from tcdona3.ila import AMP_DIRECTIONS, ILA, OP_RANGES  # noqa: E402

#: How far each set-point is moved by the test [dB].
STEP_DB = 0.5
#: Leaves of each direction's config the test writes, put back at the end.
LEAVES = ("target-gain", "target-gain-tilt", "enabled", "gain-range")
EVOAS = (1, 2)


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


@contextlib.contextmanager
def quiet():
    """The driver prints a banner on connection: keep it out of the report."""
    with contextlib.redirect_stdout(io.StringIO()):
        yield


def connect(device, **options):
    with quiet():
        return ILA(device, timeout=60, **options)


def close(a, b, tolerance):
    return a is not None and b is not None and abs(float(a) - float(b)) <= tolerance


def running(ila):
    return ila.m.get_config(source="running").data_xml


def save(ila):
    leaves = {(amp, leaf): ila._amplifier_leaf(amp, "config", leaf) for amp in AMP_DIRECTIONS for leaf in LEAVES}
    evoas = {number: ila._read_evoa(number) for number in EVOAS}
    return leaves, evoas


def moved(value, low, high):
    """``value`` moved by STEP_DB, inside ``[low, high]``."""
    value = float(value)
    return value + STEP_DB if value + STEP_DB <= high else value - STEP_DB


def candidate_lock_free(ila):
    try:
        with ila.m.locked("candidate"):
            return True
    except Exception:
        return False


def exercise(ila, device, checks):
    limits = OP_RANGES["low"]
    with quiet():
        pm = ila.get_pm_xml()
    checks.check("get_pm_xml: one pretty reply with the state counters",
                 len(pm) == 1 and "actual-gain" in pm[0] and "\n" in pm[0])

    for amp in AMP_DIRECTIONS:
        gain, tilt, state, gain_range = (ila.get_target_gain(amp), ila.get_target_gain_tilt(amp),
                                         ila.get_amp_state(amp), ila.get_gain_range(amp))
        evoa, output_evoa = ila.get_evoa_atten(amp), ila.get_output_evoa_atten(amp)
        checks.check("%s: config getters return the leaf text" % amp,
                     all(isinstance(v, str) for v in (gain, tilt, state, gain_range, evoa, output_evoa)))
        readings = [ila.get_actual_gain(amp), ila.get_actual_gain_tilt(amp), ila.get_input_power(amp),
                    ila.get_output_power(amp)]
        checks.check("%s: state readers return finite floats" % amp,
                     all(isinstance(v, float) and math.isfinite(v) for v in readings), readings)

        # writes: one locked transaction each, read back
        target = moved(gain, limits["min_gain"], limits["max_gain"])
        ila.set_target_gain(amp, target)
        checks.check("%s: set_target_gain" % amp, close(ila.get_target_gain(amp), target, 0.051),
                     (target, ila.get_target_gain(amp)))
        target = moved(tilt, limits["min_tilt"], limits["max_tilt"])
        ila.set_target_gain_tilt(amp, target)
        checks.check("%s: set_target_gain_tilt" % amp, close(ila.get_target_gain_tilt(amp), target, 0.051))
        ila.set_amp_state(amp, state)
        checks.check("%s: set_amp_state with the state it holds" % amp, ila.get_amp_state(amp) == state)
        ila.set_gain_range(amp, gain_range)
        checks.check("%s: set_gain_range with the range it holds" % amp, ila.get_gain_range(amp) == gain_range)
        target = moved(evoa, limits["min_voa_out"], limits["max_voa_out"])
        ila.set_evoa_atten(amp, target)
        checks.check("%s: set_evoa_atten (legacy numbering)" % amp, close(ila.get_evoa_atten(amp), target, 0.051))
        target = moved(ila.get_output_evoa_atten(amp), limits["min_voa_out"], limits["max_voa_out"])
        ila.set_output_evoa_atten(amp, target)
        checks.check("%s: set_output_evoa_atten" % amp, close(ila.get_output_evoa_atten(amp), target, 0.051))

    # a rejected edit is discarded and the lock released
    before = ila.get_target_gain("ab")
    try:
        ila._commit_edit(ila._amplifier_rpc("ab", "target-gain", "not-a-number"))
        rejected = False
    except Exception:
        rejected = True
    checks.check("a rejected edit raises", rejected)
    checks.check("after it the candidate lock is free", candidate_lock_free(ila))
    checks.check("after it the running value is unchanged", ila.get_target_gain("ab") == before)

    # validation: nothing is sent
    checks.raises("set_target_gain refuses NaN", lambda: ila.set_target_gain("ab", float("nan")))
    checks.raises("set_amp_state refuses 'on'", lambda: ila.set_amp_state("ab", "on"))
    checks.raises("set_gain_range refuses an unknown range", lambda: ila.set_gain_range("ab", "ULTRA"))
    checks.raises("an unknown direction is refused", lambda: ila.get_evoa_atten("cd"))

    # options
    digits = connect(device, setpoint_digits="device")
    try:
        checks.check("setpoint_digits='device' reads the YANG decimals", digits.number_of_digits in (1, 2, 3),
                     digits.number_of_digits)
        target = round(float(digits.get_target_gain("ab")) + 0.25, digits.number_of_digits)
        digits.set_target_gain("ab", target)
        checks.check("a set-point with the device decimals", close(digits.get_target_gain("ab"), target, 1e-6),
                     (target, digits.get_target_gain("ab")))
    finally:
        digits.close()
    measured = connect(device, evoa_numbering="measured")
    try:
        checks.check("evoa_numbering='measured' addresses the output EVOA",
                     all(measured.get_evoa_atten(amp) == ila.get_output_evoa_atten(amp) for amp in AMP_DIRECTIONS))
    finally:
        measured.close()
    with connect(device, reconnect=True) as reconnecting:
        checks.check("reconnect=True wraps the session and reads",
                     isinstance(reconnecting.m, _netconf.ReconnectingSession) and
                     reconnecting.get_amp_state("ab") in ("true", "false"))


def restore(ila, saved):
    """Write every saved set-point back as the device returned it; the ones still different."""
    leaves, evoas = saved
    for (amp, leaf), text in leaves.items():
        if ila._amplifier_leaf(amp, "config", leaf) == text:
            continue
        try:
            if leaf == "gain-range":
                ila.set_gain_range(amp, text)
            else:
                ila._commit_edit(ila._amplifier_rpc(amp, leaf, text))
        except Exception as error:
            print("[warn] restoring %s %s: %s" % (amp, leaf, type(error).__name__))
    for number, text in evoas.items():
        if ila._read_evoa(number) != text:
            try:
                ila._write_evoa(number, float(text))
            except Exception as error:
                print("[warn] restoring EVOA %d: %s" % (number, type(error).__name__))
    current = save(ila)
    return ([key for key in leaves if current[0][key] != leaves[key]] +
            ["evoa %d" % n for n in evoas if current[1][n] != evoas[n]])


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("device", help="ila_1 ... ila_11")
    parser.add_argument("--show-values", action="store_true")
    args = parser.parse_args()

    checks = Checks(args.show_values)
    started = time.time()
    with connect(args.device) as ila:
        initial = running(ila)
        checks.check("running configuration read twice the same", initial == running(ila),
                     "%d chars" % len(initial))
        checks.check("the candidate lock is free", candidate_lock_free(ila))
        saved = save(ila)
        try:
            exercise(ila, args.device, checks)
        except KeyboardInterrupt:  # Ctrl-C: the set-points are still put back below
            checks.check("the test ran to the end (interrupted)", False)
        except Exception as error:
            checks.check("the test ran to the end (%s: %s)" % (type(error).__name__, error), False)
        different = restore(ila, saved)
        checks.check("set-points put back", different == [], different)
        checks.check("running configuration back to the initial one", running(ila) == initial)
    print("duration: %.1f s" % (time.time() - started))
    print("%d checks failed%s" % (len(checks.failed), ": " + ", ".join(checks.failed) if checks.failed else ""))
    sys.exit(1 if checks.failed else 0)


if __name__ == "__main__":
    main()
