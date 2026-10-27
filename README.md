# tcdona3
Codebase for Open Ireland Optical Testbed v3

## Testing

The drivers shared with ols-control-app (Lumentum, ILA, YokogawaOSA, TeraflexSSH) have a test suite in
`tests/` that runs without any device: every transport (NETCONF, VISA, SSH) is faked and a network
connection fails the test. It pins their backward compatibility (method signatures in
`tests/data/api_snapshot.json`, the commands sent, what is returned) and tests the features added since.

```bash
pip install -r tests/requirements.txt   # plus the drivers' own dependencies
python -m pytest                        # from the repository root
```

The suite runs on the production interpreter (Python 3.6, pytest 7.0) and on newer ones; it also checks
that the maintained modules need at most Python 3.6 (with `vermin`, when installed).
