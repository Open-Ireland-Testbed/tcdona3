"""Shared test setup for tcdona3.

The tests run on the production interpreter (Python 3.6, pytest <= 7.0) and on newer ones, so
this file and the tests avoid any construct newer than 3.6.

What every test gets:

- ``import tcdona3`` resolves to this checkout (the repository root is the package), whatever the name
  of its directory; the run stops when it would resolve to another copy;
- ``mysql.connector`` is stubbed when it is not installed (``tcdona3.utils`` imports it at load);
- no network: a connect to an IPv4/IPv6 address fails the test, so a driver can never reach a
  device from the test suite;
- ``time.sleep`` does nothing, so the waits of the drivers cost no time.
"""

import os
import socket
import sys
import time
import types

import pytest

os.environ.setdefault("MPLBACKEND", "Agg")

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PARENT = os.path.dirname(_REPO)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

try:
    import mysql.connector  # noqa: F401
except ImportError:
    _mysql = types.ModuleType("mysql")
    _connector = types.ModuleType("mysql.connector")

    def _refuse(*args, **kwargs):
        raise RuntimeError("the booking database is not reachable from the tests")

    _connector.connect = _refuse
    _mysql.connector = _connector
    sys.modules["mysql"] = _mysql
    sys.modules["mysql.connector"] = _connector



def _bind_package_to_this_checkout():
    """``import tcdona3`` is this checkout, whatever its directory is called (a worktree such as
    ``tcdona3_review`` would otherwise import a sibling ``tcdona3`` directory, or an installed copy)."""
    if os.path.basename(_REPO) != "tcdona3" and "tcdona3" not in sys.modules:
        import importlib.util
        spec = importlib.util.spec_from_file_location("tcdona3", os.path.join(_REPO, "__init__.py"),
                                                      submodule_search_locations=[_REPO])
        package = importlib.util.module_from_spec(spec)
        sys.modules["tcdona3"] = package
        spec.loader.exec_module(package)
    import tcdona3
    imported = os.path.dirname(os.path.abspath(tcdona3.__file__))
    if os.path.realpath(imported) != os.path.realpath(_REPO):
        raise ImportError("the tests would run on %s instead of this checkout %s" % (imported, _REPO))


_bind_package_to_this_checkout()

_NETWORK_FAMILIES = (socket.AF_INET, getattr(socket, "AF_INET6", socket.AF_INET))


class NetworkBlocked(AssertionError):
    """A test tried to open a network connection."""


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex

    def guard(original):
        def connect(self, address, *args, **kwargs):
            if self.family in _NETWORK_FAMILIES:
                raise NetworkBlocked("network access attempted in a test: %r" % (address,))
            return original(self, address, *args, **kwargs)
        return connect

    monkeypatch.setattr(socket.socket, "connect", guard(original_connect))
    monkeypatch.setattr(socket.socket, "connect_ex", guard(original_connect_ex))


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
