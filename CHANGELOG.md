# Changelog

## 1.2.0

New: a driver for the VIAVI MTS 4000 OTDR, selective Lumentum telemetry, a target-gain check on the Lumentum
EDFAs, a way to skip the booking check outside the lab, and `pyproject.toml`. Existing signatures and
defaults are unchanged.

### What existing scripts may notice

- **Lumentum target gain is checked:** `set_mux_constant_gain`, `set_demux_constant_gain` and
  `edfa_config(..., control_mode="constant-gain")` refuse a target gain outside the device's range (booster
  4-27 dB, preamp 10-30.5 dB) with `ValueError`, before the EDFA is taken out of service. Before, the
  amplifier went out of service and the device's refusal left it there. `edfa_config` in constant-gain mode
  needs a `target_gain` (its default, 0.0, is outside both ranges).
- **TeraFlex (NETCONF `TFlex`):** `delete_logical_interface`, `get_power_and_frequency` and
  `set_power_and_frequency` name the logical channel and the optical-channel component by their keys
  (`index`, `name`) as direct children of the list entry, as NETCONF requires; the delete sends only the key.
- **`import tcdona3` without setuptools:** `utils.py` no longer imports `pkg_resources`. Where setuptools was
  missing (a fresh virtual environment, setuptools 81+), every driver that imports `tcdona3.utils` was
  silently left out of `tcdona3`.

### New

- `MTS4000`, `MTS4000Error`, `OTDRConfig` (`tcdona3.mts4000`): VIAVI MTS/T-BERD 4000 OTDR over its SCPI
  ports. Configure, acquire, trace, event table, `.sor`, save to `.json`/`.csv`/`.sor`, identify. Runs on
  Python 3.6. `acquire()` refuses to return the previous result when the acquisition did not start, and sends
  a stop (and waits for STOPPED) when starting or waiting fails or is interrupted. A timeout or lost connection closes that
  socket (`MTS4000TransportError`) and the next command opens a new session, so a late reply is never read
  as the answer to another command. `configure()` refuses keywords, lasers and numbers it can check before
  writing anything. The OTDR is not in the booking database, so the driver does not check bookings.
  Hardware smoke script: `tests/hardware/smoke_mts4000.py` (read-only by default).
- `Lumentum.fetch_all_telemetry(fetch="all")`: WSS connections, OCM monitored channels and EDFA states in one
  call, or only some of them (one NETCONF get each). Values are those of `wss_get_connections`,
  `wss_get_monitored_channels` and `get_edfa_info`; a part not asked for is `None`.
- `Lumentum(..., gain_limits={"booster": (7.0, 27.0), "preamp": (11.0, 30.0)})`: narrower target-gain limits,
  within the device's ranges.
- Booking check: `TCDONA3_BOOKING_CHECK=0` (or `false`, `no`, `off`) or `check_patch_owners(...,
  enabled=False)` skips it, with a warning on stderr each time (see the README).
- `pyproject.toml` for `pip install -e .` from a checkout (Python 3.7+); the lab hosts still install as in
  `DEPLOY.MD`.

## 1.1.0

New features of the Lumentum, ILA, Yokogawa and TeraFlex SSH drivers, with a backward-compatibility test suite
(`tests/`) and one hardware smoke script per driver (`tests/hardware/`, see `docs/hardware_tests.rst`).
Signatures and defaults of the existing driver methods are unchanged, except the provisioning-database host of
the NETCONF `Polatis` class (below); new options are off by default.

### What existing scripts may notice

- **Lumentum demux setters:** `set_demux_constant_gain`, `set_demux_low_gain_mode` and `set_demux_high_gain_mode`
  took the *booster* out of service and left it there; they now take the preamp out of service, as intended.
  A script that put the booster back by hand after them can stop doing so.
- **Errors instead of exits:** a Lumentum EDFA or port RPC error raises `RuntimeError` instead of ending the
  program with `exit(1)`.
- **ILA writes are transactions:** each write locks the candidate datastore, discards changes left in it,
  edits and commits. A write rejected by the device is discarded and the lock released; before, it left the
  candidate dirty for the next writer. A commit whose reply is lost may still have been applied: read the
  value back after such an error. An ILA setter called inside your own `ila.m.locked("candidate")` now fails
  (the candidate is already locked): call the setters on their own.
- **ILA input checks:** NaN, infinite or non-numeric values, an unknown direction and an unknown state are
  refused before anything is written. Numeric strings (`"15"`) are accepted for gains, tilts and EVOAs (they
  raised `TypeError`); `set_amp_state` accepts `True`/`False` and any case of `'true'`/`'false'`.
- **ILA EVOA numbering:** `get/set_evoa_atten(amp)` keep the legacy numbering (ab → EVOA 1, ba → EVOA 2) and
  show a one-time `UserWarning`. Measured on the line, the output EVOA after ab is EVOA 2 and after ba EVOA 1:
  use `get/set_output_evoa_atten(amp)` for those, or `ILA(..., evoa_numbering="measured")`.
- **Lumentum `set_demux_port` / `wss_outport`** with one connection sent malformed XML and failed; it now
  moves the connection's output port. `wss_atten` and `set_*_atten` also accept a list of connections with a
  list of attenuations (a list of `(connection, attenuation)` pairs works as before).
- **TeraFlex SSH:** SNR, OSNR and Q are read from their own rows (the SNR could come from the OSNR row) and may
  be negative. `get_current_config()` returns the line port's configuration where the CLI has no
  `show running-config`.
- **Yokogawa:** `wait_for_sweep_complete` tests the sweep-complete bit; `set_wavelength_span` writes three
  decimals instead of one.
- **Lumentum `disable_als`** returns whether the device accepted it (it returned nothing).
- **`RoadmMonitor`** no longer fails without `/etc/tcdona2/ports.csv` (its Polatis port attributes are then
  None), and `write_json_data` records the measurement setup with the ROADM's address.
- **Booking check:** `TCDONA3_BOOKING_USER` and `TCDONA3_BOOKING_ALIASES` add booking names (as deployed on
  ol4); names are matched in lower case.
- **Polatis (NETCONF class):** the default `db_host` of the provisioning database is `10.10.10.4` (ol02's
  address, where the database runs) instead of `127.0.0.1`, which only worked on ol02 itself. Patch operations
  run from another host now read the port mappings from that database; an explicit `db_host` is used as before.
- **Package names:** `tcdona3.Yokogawa`, `QuadFlex`, `TeraFlex` and `TeraFlexParamiko` now exist, as aliases of
  `YokogawaOSA`, `QFlex`, `TFlex` and `TeraflexSSH`. `tcdona3.Polatis` is still the legacy class up to Python
  3.12; from 3.13, where `telnetlib` no longer exists, it is the NETCONF class (see issue #29).
- **Logins** of the ROADMs, ILAs and Yokogawa can come from `LUMENTUM_*`, `ILA_*` and `YOKOGAWA_*` environment
  variables (see the README); the defaults are unchanged.

### Options (off by default)

- `Lumentum(..., timeout=, reconnect=True)` and `ILA(..., timeout=, reconnect=True)`: a dropped NETCONF session
  is opened again; reads are repeated, a write only when repeating it is safe (never a delete or
  `disable_als`; an ILA transaction is applied again whole).
- `ILA(..., evoa_numbering="measured")`, `ILA(..., setpoint_digits="device")`.
- New methods: Lumentum EDFA state, maintenance and gain-switch mode, tilt, line port and output VOA
  (`set_outvoa_attenuation`); ILA tilt, gain range, actual gain and powers, output EVOA; typed Yokogawa getters
  and setters in nm and `read_trace()`; TeraFlex PM parser and `get_qot_snapshot()`; `close()` and context
  managers.

### Requirements

Python 3.6 or newer. paramiko older than 5 (3.x and 4.x connect to the ILAs; 5.x does not).
