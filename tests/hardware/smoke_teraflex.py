"""Hardware smoke test of ``tcdona3.teraflex_paramiko.TeraflexSSH`` on a TeraFlex with a live carrier.

Usage (from the parent directory of the tcdona3 package)::

    python3 tcdona3/tests/hardware/smoke_teraflex.py <credentials>.json [--device tf_1] [--logical ot400]
                                                     [--show-values]

``<credentials>.json`` holds ``username`` and ``password`` (and optionally ``ip_address``, which overrides
the address of ``DEVICE_MAP``). The line port must carry a live signal, so that SNR, OSNR and Q-factor are
reported.

The driver only reads: the script saves the configuration first (:meth:`TeraflexSSH.get_current_config`: on
this CLI, without ``show running-config``, the ``show interface`` views of the line port), compares it with
the one at the end and exits with an error when they differ. Measured values are printed only with ``--show-values``.
"""

import argparse
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from tcdona3.teraflex_paramiko import QOT_FIELDS, TeraflexSSH  # noqa: E402

#: The metrics a live carrier must report.
REQUIRED = ("tx_power", "rx_power_total", "snr", "osnr", "q_factor")


class Checks:
    def __init__(self, show_values):
        self.show_values, self.failed = show_values, []

    def check(self, name, condition, value=None):
        status = "ok  " if condition else "FAIL"
        shown = " = %r" % (value,) if self.show_values and value is not None else ""
        print("[%s] %s%s" % (status, name, shown))
        if not condition:
            self.failed.append(name)


def is_number(value):
    return isinstance(value, float) and math.isfinite(value)


def as_float(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def port_configuration(device, logical):
    """The configuration as :meth:`TeraflexSSH.get_current_config` returns it, as a set of stripped lines."""
    return {line.rstrip() for line in device.get_current_config(logical).splitlines() if line.strip()}


def run(device, logical, checks):
    started = time.time()
    initial = port_configuration(device, logical)
    checks.check("get_current_config: the configuration, without a CLI error",
                 bool(initial) and not any("Error(" in line for line in initial), "%d lines" % len(initial))

    commands = device.pm_commands(logical)
    checks.check("pm_commands has the three layers", sorted(commands) == ["opt-phy", "otsi", "otuc"])

    per_layer = {}
    for layer in ("opt-phy", "otsi", "otuc"):
        tables = device.get_pm(layer, logical)
        per_layer[layer] = tables
        live = [t for t in tables if t["interval"] == "live"]
        checks.check("get_pm(%s): tables parsed, one live" % layer, bool(tables) and bool(live),
                     "%d tables" % len(tables))

    all_tables = device.get_all_pm(logical)
    checks.check("get_all_pm covers every layer", {t["layer"] for t in all_tables} == set(per_layer),
                 "%d tables" % len(all_tables))

    snapshot = device.get_qot_snapshot(logical, tables=all_tables)
    checks.check("snapshot port and logical type", (snapshot["port"], snapshot["logical_type"]) ==
                 (device.line_port, logical))
    checks.check("snapshot has every QoT field", set(QOT_FIELDS) <= set(snapshot))
    for key in REQUIRED:
        checks.check("snapshot %s is a live number" % key,
                     is_number(snapshot[key]) and "(live" in snapshot["source"].get(key, ""), snapshot[key])
    checks.check("snapshot units are reported", all(key in snapshot["units"] for key in REQUIRED),
                 {key: snapshot["units"].get(key) for key in REQUIRED})

    fresh = device.get_qot_snapshot(logical)
    checks.check("a second snapshot reads the device again", fresh["timestamp"] > snapshot["timestamp"])

    legacy = device.return_current_config(logical)
    checks.check("return_current_config keeps its keys",
                 {"rx_power", "tx_power", "snr", "osnr", "q_factor", "fec_ber_live", "fec_ber_15min"}
                 <= set(legacy))
    for key in ("rx_power", "tx_power", "snr", "osnr", "q_factor"):
        checks.check("return_current_config %s is a numeric str" % key,
                     isinstance(legacy[key], str) and as_float(legacy[key]) is not None, legacy[key])
    # live values move between two reads: the two APIs must agree within a reasonable margin
    for key, other, margin in (("snr", "snr", 1.0), ("osnr", "osnr", 1.0), ("q_factor", "q_factor", 1.0),
                               ("rx_power", "rx_power_total", 0.5), ("tx_power", "tx_power", 0.5)):
        old, new = as_float(legacy[key]), fresh[other]
        checks.check("return_current_config %s agrees with the snapshot" % key,
                     old is not None and is_number(new) and abs(old - new) <= margin, (old, new))

    pm = device.read_pm_data()
    checks.check("read_pm_data reads rx and tx power",
                 is_number(pm["rx_power"]) and is_number(pm["tx_power"]), (pm["rx_power"], pm["tx_power"]))

    changed = len(initial ^ port_configuration(device, logical))
    checks.check("port configuration unchanged at the end", changed == 0, "%d lines differ" % changed)
    print("duration: %.1f s" % (time.time() - started))
    return changed == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("credentials", help="JSON file with username and password (and optionally ip_address)")
    parser.add_argument("--device", default="tf_1")
    parser.add_argument("--logical", default="ot400")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--show-values", action="store_true")
    args = parser.parse_args()

    with open(args.credentials) as handle:
        credentials = json.load(handle)
    checks = Checks(args.show_values)
    with TeraflexSSH(args.device, credentials["username"], credentials["password"], timeout=args.timeout,
                     host=credentials.get("ip_address")) as device:
        unchanged = run(device, args.logical, checks)

    print("%d checks failed%s" % (len(checks.failed), ": " + ", ".join(checks.failed) if checks.failed else ""))
    if not unchanged:
        print("The port configuration changed during the test: compare it with the initial one.")
    sys.exit(1 if checks.failed else 0)


if __name__ == "__main__":
    main()
