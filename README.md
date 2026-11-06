# tcdona3
Codebase for Open Ireland Optical Testbed v3

## Dependencies

What the maintained drivers need, besides the Python 3.6 standard library:

| Driver | Packages |
|---|---|
| `Lumentum`, `ILA` | `ncclient`, `lxml`, `xmltodict`, `paramiko` < 5 |
| `TeraflexSSH` | `paramiko` < 5 |
| `YokogawaOSA` | `pyvisa` and a VISA backend (`pyvisa-py` when NI-VISA is not installed), `numpy`, `matplotlib` |
| `MTS4000` (VIAVI MTS 4000 OTDR, SCPI over TCP) | `numpy` |
| booking check (`utils.check_patch_owners`) | `mysql-connector-python` |

paramiko 5.x does not connect to the Juniper ILAs ("no acceptable host key"); 3.x (tested with 3.4.1) and 4.x
(4.0.0, the version on ol4) do: the ILAs present an RSA host key (`ssh-rsa`), not the DSA key that 4.0 dropped.
On Python 3.6, pip picks versions that support it.

## Credentials

The logins of the ROADMs, the ILAs and the Yokogawa OSA can be given through environment variables instead of
the defaults in the code: `LUMENTUM_USERNAME`/`LUMENTUM_PASSWORD`, `ILA_USERNAME`/`ILA_PASSWORD`,
`YOKOGAWA_USER`/`YOKOGAWA_PASSWORD` (a Yokogawa login passed to the constructor is used as it is). The booking
database already reads `DB_HOST`, `DB_USER`, `DB_PASSWORD` and `DB_NAME`. Without them, the defaults are used,
as before.

## Booking check

Every driver except the OTDR asks the booking database whether its device is free or booked by you
(`utils.check_patch_owners`). Outside the lab, where the database cannot be reached, the check can be skipped
with `TCDONA3_BOOKING_CHECK=0` (or `false`, `no`, `off`), or with `check_patch_owners(..., enabled=False)` in
code. A skipped check prints a warning on stderr every time, naming the devices nobody checked; do not skip it
on the lab hosts.

## Installing from a checkout

`pip install -e .` (Python 3.7+) installs this checkout with its dependencies (`pyproject.toml`). The lab hosts
install as described in `DEPLOY.MD`.

## Testing

The maintained drivers (Lumentum, ILA, YokogawaOSA, TeraflexSSH, MTS4000) have a test suite in
`tests/` that runs without any device: every transport (NETCONF, VISA, SSH, SCPI) is faked and a network
connection fails the test. It pins their backward compatibility (method signatures in
`tests/data/api_snapshot.json`, the commands sent, what is returned) and tests the features added since.

```bash
pip install -r tests/requirements.txt   # plus the drivers' own dependencies
python -m pytest                        # from the repository root
```

The suite runs on the production interpreter (Python 3.6, pytest 7.0) and on newer ones; it also checks
that the maintained modules need at most Python 3.6 (with `vermin`, when installed).

`tests/hardware/` has one script per driver that checks it on the real device and leaves the device as it
found it; see `docs/hardware_tests.rst` before running them.
