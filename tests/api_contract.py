"""The public API of the maintained drivers, as a snapshot, and the rule a newer version must follow.

The snapshot (``data/api_snapshot.json``) records, for every public method of every maintained driver
class, its parameters: name, kind and default. A newer version is backward compatible when every
recorded method still exists and accepts every call the recorded one accepted:

- the recorded parameters are kept, in the same order, with the same kind and default;
- a parameter added after them has a default (or is ``*args``/``**kwargs``).

New methods and new classes are allowed. Run ``python tests/api_contract.py`` to print the snapshot
of the current code (used once to record it; never to update it after a change).
"""

import inspect
import json
import os
import sys

#: The maintained driver classes: module of the package -> class names.
DRIVER_CLASSES = {
    "lumentum": ["Lumentum"],
    "ila": ["ILA"],
    "yokogawa": ["YokogawaOSA"],
    "teraflex_paramiko": ["TeraflexSSH"],
}

SNAPSHOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "api_snapshot.json")


def _parameter(parameter):
    default = None if parameter.default is inspect.Parameter.empty else repr(parameter.default)
    return {"name": parameter.name, "kind": str(parameter.kind), "default": default,
            "has_default": parameter.default is not inspect.Parameter.empty}


def describe_class(cls):
    """``{method name: [parameter, ...]}`` of the public methods (and ``__init__``) of ``cls``."""
    methods = {}
    for name, member in sorted(vars(cls).items()):
        if name.startswith("_") and name != "__init__":
            continue
        function = member
        if isinstance(member, (staticmethod, classmethod)):
            function = member.__func__
        if isinstance(member, property) or not callable(function) or inspect.isclass(function):
            continue
        methods[name] = [_parameter(p) for p in inspect.signature(function).parameters.values()]
    return methods


def describe(modules):
    """The snapshot of the maintained classes, ``modules`` mapping a module name to the imported module."""
    return {"%s.%s" % (module, cls): describe_class(getattr(modules[module], cls))
            for module, classes in sorted(DRIVER_CLASSES.items()) for cls in classes}


def compatibility_errors(recorded, current):
    """Why ``current`` (a method's parameters) cannot take every call ``recorded`` could, or ``[]``."""
    errors = []
    for index, old in enumerate(recorded):
        if index >= len(current):
            errors.append("parameter %r removed" % old["name"])
            continue
        new = current[index]
        for key in ("name", "kind", "default", "has_default"):
            if old[key] != new[key]:
                errors.append("parameter %r: %s %r became %r" % (old["name"], key, old[key], new[key]))
    for new in current[len(recorded):]:
        variadic = new["kind"] in ("VAR_POSITIONAL", "VAR_KEYWORD")
        if not (new["has_default"] or variadic):
            errors.append("new parameter %r has no default" % new["name"])
    return errors


def load_snapshot():
    with open(SNAPSHOT) as handle:
        return json.load(handle)


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    import conftest  # noqa: F401 - the mysql stub
    import importlib
    loaded = {name: importlib.import_module("tcdona3." + name) for name in DRIVER_CLASSES}
    print(json.dumps(describe(loaded), indent=1, sort_keys=True))
