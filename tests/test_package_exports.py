"""What ``import tcdona3`` exposes, and that the shared drivers stay importable on Python 3.6."""

import ast
import os
import subprocess
import sys

import pytest

import tcdona3

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: Package-level names of the shared drivers -> the class each must be.
SHARED_EXPORTS = {
    "Lumentum": ("tcdona3.lumentum", "Lumentum"),
    "ILA": ("tcdona3.ila", "ILA"),
}

#: Names ``tcdona3/__init__.py`` always tried to export (no module defines them): aliases of these classes.
INTENDED_EXPORTS = {
    "Yokogawa": ("tcdona3.yokogawa", "YokogawaOSA"),
    "TeraFlex": ("tcdona3.teraflex", "TFlex"),
    "QuadFlex": ("tcdona3.quadflex", "QFlex"),
    "TeraFlexParamiko": ("tcdona3.teraflex_paramiko", "TeraflexSSH"),
}

#: The modules this work maintains; they must parse and use nothing newer than Python 3.6.
MAINTAINED = ["lumentum.py", "ila.py", "yokogawa.py", "teraflex_paramiko.py", "__init__.py"]


def _class(module, name):
    return getattr(__import__(module, fromlist=[name]), name)


@pytest.mark.parametrize("name", sorted(SHARED_EXPORTS))
def test_shared_drivers_are_exported(name):
    assert getattr(tcdona3, name) is _class(*SHARED_EXPORTS[name])
    assert name in tcdona3.__all__


def test_every_name_in_all_exists():
    assert all(hasattr(tcdona3, name) for name in tcdona3.__all__)


def test_every_name_in_all_is_listed_once():
    assert len(tcdona3.__all__) == len(set(tcdona3.__all__))


@pytest.mark.xfail(strict=True, reason="out of scope, to decide: polatis_old's Polatis overrides the NETCONF one, "
                                       "which users of tcdona3.Polatis get today")
def test_polatis_is_the_netconf_one():
    assert tcdona3.Polatis is _class("tcdona3.polatis", "Polatis")


@pytest.mark.parametrize("name", sorted(INTENDED_EXPORTS))
def test_intended_exports_exist(name):
    assert getattr(tcdona3, name) is _class(*INTENDED_EXPORTS[name])
    assert name in tcdona3.__all__


@pytest.mark.parametrize("module,name", sorted(INTENDED_EXPORTS.values()))
def test_the_class_names_are_exported_too(module, name):
    assert getattr(tcdona3, name) is _class(module, name)


def test_the_original_class_names_stay_importable():
    for module, name in INTENDED_EXPORTS.values():
        _class(module, name)


@pytest.mark.parametrize("filename", MAINTAINED)
def test_maintained_modules_have_no_postponed_annotations(filename):
    """``from __future__ import annotations`` is a SyntaxError on Python 3.6."""
    with open(os.path.join(REPO, filename)) as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            assert "annotations" not in [alias.name for alias in node.names]


def test_maintained_code_needs_at_most_python_3_6():
    """``vermin`` (when installed) finds the minimum Python version of the maintained code and its tests."""
    vermin = os.path.join(os.path.dirname(sys.executable), "vermin")
    if not os.path.exists(vermin):
        pytest.skip("vermin is not installed")
    tests = [os.path.join("tests", name) for name in os.listdir(os.path.join(REPO, "tests")) if name.endswith(".py")]
    command = [vermin, "-t=3.6-", "--no-tips", "--violations",
               "--eval-annotations", "--no-parse-comments"] + MAINTAINED + sorted(tests)
    result = subprocess.run(command, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            universal_newlines=True)
    assert result.returncode == 0, result.stdout
