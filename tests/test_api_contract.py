"""Every recorded public method of the shared drivers still accepts every call it accepted (see
``api_contract.py``)."""

import importlib

import pytest

import api_contract

RECORDED = api_contract.load_snapshot()
CURRENT = api_contract.describe({name: importlib.import_module("tcdona3." + name)
                                 for name in api_contract.SHARED_CLASSES})


@pytest.mark.parametrize("cls", sorted(RECORDED))
def test_every_recorded_class_still_exists(cls):
    assert cls in CURRENT


@pytest.mark.parametrize("cls,method", sorted((cls, method) for cls in RECORDED for method in RECORDED[cls]))
def test_every_recorded_method_takes_the_same_calls(cls, method):
    assert method in CURRENT[cls], "%s.%s was removed" % (cls, method)
    errors = api_contract.compatibility_errors(RECORDED[cls][method], CURRENT[cls][method])
    assert not errors, "%s.%s: %s" % (cls, method, "; ".join(errors))


def test_the_rule_refuses_a_removed_or_renamed_parameter():
    recorded = [{"name": "self", "kind": "POSITIONAL_OR_KEYWORD", "default": None, "has_default": False},
                {"name": "DEBUG", "kind": "POSITIONAL_OR_KEYWORD", "default": "False", "has_default": True}]
    renamed = [dict(recorded[0]), dict(recorded[1], name="debug")]
    assert api_contract.compatibility_errors(recorded, renamed)
    assert api_contract.compatibility_errors(recorded, recorded[:1])


def test_the_rule_accepts_a_new_parameter_with_a_default_only():
    recorded = [{"name": "self", "kind": "POSITIONAL_OR_KEYWORD", "default": None, "has_default": False}]
    optional = recorded + [{"name": "timeout", "kind": "KEYWORD_ONLY", "default": "None", "has_default": True}]
    required = recorded + [{"name": "host", "kind": "POSITIONAL_OR_KEYWORD", "default": None,
                            "has_default": False}]
    assert api_contract.compatibility_errors(recorded, optional) == []
    assert api_contract.compatibility_errors(recorded, required)
