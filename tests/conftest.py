"""Shared test setup for tcdona3.

The tests run on the production interpreter (Python 3.6, pytest <= 7.0) and on newer ones, so
this file and the tests avoid any construct newer than 3.6.

What every test gets:

- ``import tcdona3`` resolves to this checkout (the repository root is the package);
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
