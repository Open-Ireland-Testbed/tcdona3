# Changelog

## 1.1.0

New features of the Lumentum, ILA, Yokogawa and TeraFlex SSH drivers, with a backward-compatibility test suite
(`tests/`) and one hardware smoke script per driver (`tests/hardware/`, see `docs/hardware_tests.rst`).
Signatures and defaults of the existing methods are unchanged; new options are off by default.

### What existing scripts may notice

- **Lumentum demux setters:** `set_demux_constant_gain`, `set_demux_low_gain_mode` and `set_demux_high_gain_mode`
  took the *booster* out of service and left it there; they now take the preamp out of service, as intended.
  A script that put the booster back by hand after them can stop doing so.
- **Errors instead of exits:** a Lumentum EDFA or port RPC error raises `RuntimeError` instead of ending the
  program with `exit(1)`.
- **ILA writes are transactions:** each write locks the candidate datastore, discards changes left in it,
  edits and commits. A failed write is discarded and the lock released; before, it left the candidate dirty
  for the next writer. An ILA setter called inside your own `ila.m.locked("candidate")` now fails (the
  candidate is already locked): call the setters on their own.
- **ILA input checks:** NaN, infinite or non-numeric values, an unknown direction and an unknown state are
  refused before anything is sent. `set_amp_state` accepts `True`/`False` as well as `'true'`/`'false'`.
- **ILA EVOA numbering:** `get/set_evoa_atten(amp)` keep the legacy numbering (ab → EVOA 1, ba → EVOA 2) and
  show a one-time `UserWarning`. Measured on the line, the output EVOA after ab is EVOA 2 and after ba EVOA 1:
  use `get/set_output_evoa_atten(amp)` for those, or `ILA(..., evoa_numbering="measured")`.
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
- **Package names:** `tcdona3.Yokogawa`, `QuadFlex`, `TeraFlex` and `TeraFlexParamiko` now exist, as aliases of
  `YokogawaOSA`, `QFlex`, `TFlex` and `TeraflexSSH`. `tcdona3.Polatis` is still the legacy class.
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
