# Deploying tcdona3 (ol02 / legacy Python 3.6)

This server is legacy and hardware-bound (e.g., GPIB/VISA). Many workflows require the system Python:

- Production runtime: **/usr/bin/python3 (Python 3.6)**
- Production install location: **/usr/local/lib/python3.6/dist-packages**
- Users typically run: `import tcdona3` (no venv)

Because of this, we deploy **globally** (NOT via a shared venv).

---

## Directory layout on ol02

There is a historical split between git root and packaging root:

- Git root (has `.git`):  
  `/home/ajag/production/tcdona3/tcdona3`

- Packaging root (has `setup.py`, `dist/`, etc.):  
  `/home/ajag/production/tcdona3`

Scripts live here:
- `/home/ajag/production/tcdona3/scripts/`

Artifacts stored here:
- `/home/ajag/deploy_artifacts/tcdona3/<gitsha>/`

---

## Test latest GitHub main (dev lane)

We test `origin/main` without affecting global production:

1) Update dev lane to match GitHub `origin/main`:
```bash
/home/ajag/production/tcdona3/scripts/update_dev_to_origin_main.sh
```

2) In Jupyter, use the kernel:
- `tcdona3-dev (inherits system)`

3) Minimal notebook sanity cell:
```python
import sys, tcdona3
print("python:", sys.executable)
print("tcdona3:", tcdona3.__file__)
```

Note: This dev venv is created with `--system-site-packages` so it can see legacy system deps.

---

## Deploy to production (global install)

**Deploy only after dev testing passes.**

Run:
```bash
/home/ajag/production/tcdona3/scripts/deploy_global.sh
```

This will:
- fast-forward local `main` to `origin/main`
- build a wheel from the current commit
- archive the wheel to `/home/ajag/deploy_artifacts/tcdona3/<sha>/`
- install the wheel into global `/usr/bin/python3` site-packages
- run basic verification

### Same-version deploys
If the package version did not change (e.g., still `1.0.3`), pip may no-op. In that case:
```bash
/home/ajag/production/tcdona3/scripts/deploy_global.sh --force
```

**Preferred**: bump version (e.g., 1.0.3 -> 1.0.4) for meaningful changes to avoid force installs.

---

## Verify production

From a neutral directory:
```bash
cd /tmp
/usr/bin/python3 -m pip show tcdona3 | sed -n '1,12p'
/usr/bin/python3 -c "import tcdona3; print(tcdona3.__file__)"
```

---

## Rollback

List recent deployed artifacts:
```bash
ls -1t /home/ajag/deploy_artifacts/tcdona3/*/*.whl | head
```

Reinstall a previous wheel:
```bash
sudo -H /usr/bin/python3 -m pip install --upgrade /path/to/older.whl
```

---

## Notes / Gotchas

- Do NOT rename wheel files. Pip expects standard wheel filenames.
- Build failures often come from root-owned build folders. If needed:
  - `sudo chown -R $USER:$USER /home/ajag/production/tcdona3`
  - `rm -rf build dist tcdona3.egg-info`
- This process intentionally avoids migrating users to a new venv because system deps require `/usr/bin/python3`.

