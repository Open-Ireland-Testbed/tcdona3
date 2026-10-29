Hardware smoke tests
====================

``tests/hardware/`` holds one script per maintained driver. Each one runs the driver against the real device,
checks what it reads and writes, and leaves the device as it found it. Use them after changing a driver, or
to check a device before an experiment. pytest does not collect them: they are run by hand.

Before running them
-------------------

* Book the device: the Lumentum, ILA and Yokogawa drivers refuse a device booked by someone else.
* Install the drivers' dependencies (see the README). The ILA needs paramiko older than 5 (3.x or 4.x).
* Run them from the directory that contains the ``tcdona3`` package.
* Credentials are never in the scripts: the TeraFlex and Yokogawa scripts read them from a JSON file given as
  argument; the ILA and Lumentum scripts use the driver's.

Every script prints one line per check (``ok`` or ``FAIL``) and exits with 1 when a check failed. Measured
values are hidden unless ``--show-values`` is given; ``[info]`` lines report a device behaviour that is not a
failure.

How each script leaves the device
---------------------------------

Each script saves the configuration first and compares it at the end; it fails when something is different.
The configuration is put back also when a check fails or the run is interrupted with Ctrl-C; while the
device is being put back, Ctrl-C is held until it is done.

.. list-table::
   :header-rows: 1
   :widths: 18 40 42

   * - Script
     - What it changes
     - How it puts it back
   * - ``smoke_teraflex.py``
     - Nothing: the driver only reads. The line port must carry a live signal.
     - Compares ``get_current_config()`` (the ``show interface`` views of the port) before and after.
   * - ``smoke_yokogawa.py``
     - Wavelength range, resolution, sensitivity, sampling, offsets, display, trace attribute, sweep mode; the
       sweeps overwrite the traces.
     - Writes the 27 saved settings back. A sweep running at the start is left stopped: an ``[info]`` line
       says so when the instrument was in repeat or auto mode.
   * - ``smoke_ila.py``
     - Gain, tilt and both EVOAs of each direction, moved by 0.5 dB for a few seconds. The amplifier is never
       disabled and its gain range never switched.
     - Writes every saved set-point back as the device returned it; compares the whole running configuration.
   * - ``smoke_lumentum.py``
     - WSS writes on blocked DEMUX connections only (no light passes); the line output VOA, moved by 0.5 dB.
       With ``--unblock``, one of them is unblocked for about a second: light at its input then reaches its
       drop port, so use it only when its band is dark or its drop port is free. With ``--edfa-writes``, the
       legacy EDFA setters.
     - Writes back every connection, EDFA and port that differs from the saved running configuration, and
       brings the line back (see below).

Usage::

    python3 tcdona3/tests/hardware/smoke_teraflex.py <credentials>.json --device tf_1
    python3 tcdona3/tests/hardware/smoke_yokogawa.py <credentials>.json
    python3 tcdona3/tests/hardware/smoke_ila.py <ila>
    python3 tcdona3/tests/hardware/smoke_lumentum.py <roadm> [--edfa-writes] [--unblock]

``--edfa-writes`` interrupts the line
-------------------------------------

The legacy EDFA setters (``set_mux_constant_gain`` and the like) take their EDFA out of service for about a
second. On a line that starts and ends at the same ROADM this can leave the whole line dark:

1. the booster stops for a moment, so no light comes back to the preamp;
2. with no light at the preamp, the automatic laser shutdown (ALS) of the ROADM keeps the booster off;
3. the in-line amplifiers lose their input and shut down too.

Nothing comes back by itself. ``smoke_lumentum.py`` handles it: when the preamp is not back in the state it
had at the start, it suspends the ALS with :meth:`lumentum.Lumentum.disable_als` until the line is back, and
fails if it does not come back. By hand, the same call brings a line back::

    roadm.disable_als(60)   # the booster lights the line again; the ALS resumes after 60 s

Run ``--edfa-writes`` only when the line may be interrupted for a minute.
