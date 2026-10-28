# tcdona3
Codebase for Open Ireland Optical Testbed v3

## Dependencies

What the maintained drivers need, besides the Python 3.6 standard library:

| Driver | Packages |
|---|---|
| `Lumentum`, `ILA` | `ncclient`, `lxml`, `xmltodict`, `paramiko` < 5 |
| `TeraflexSSH` | `paramiko` < 5 |
| `YokogawaOSA` | `pyvisa` and a VISA backend (`pyvisa-py` when NI-VISA is not installed), `numpy`, `matplotlib` |
| booking check (`utils.check_patch_owners`) | `mysql-connector-python` |

paramiko 5.x does not connect to the Juniper ILAs (it no longer accepts their SSH host key type, `ssh-rsa`);
3.x (tested with 3.4.1) and 4.x (4.0.0, the version on ol4) do. On Python 3.6, pip picks versions that support it.

## Credentials

The logins of the ROADMs, the ILAs and the Yokogawa OSA can be given through environment variables instead of
the defaults in the code: `LUMENTUM_USERNAME`/`LUMENTUM_PASSWORD`, `ILA_USERNAME`/`ILA_PASSWORD`,
`YOKOGAWA_USER`/`YOKOGAWA_PASSWORD` (a Yokogawa login passed to the constructor is used as it is). The booking
database already reads `DB_HOST`, `DB_USER`, `DB_PASSWORD` and `DB_NAME`. Without them, the defaults are used,
as before.

## Testing

The maintained drivers (Lumentum, ILA, YokogawaOSA, TeraflexSSH) have a test suite in
`tests/` that runs without any device: every transport (NETCONF, VISA, SSH) is faked and a network
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
