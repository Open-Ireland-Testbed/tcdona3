"""Set-point precision as a device declares it in its YANG (Python 3.6 compatible).

A NETCONF device publishes its YANG modules through ``get-schema``. Each set-point leaf (gain, tilt, output
power) is a ``decimal64`` whose ``fraction-digits`` is the number of decimals the device stores: a value
written with more decimals is rounded by the device.
"""

import logging
import re

logger = logging.getLogger("tcdona3.yang")


#: How many typedefs an alias chain may go through (``typedef a { type b; }``) before giving up.
MAX_TYPEDEF_DEPTH = 8


def _statement_body(text, keyword, name):
    """The statements of ``<keyword> <name> { ... }``, braces balanced; names may be quoted."""
    match = re.search(r'\b%s\s+"?%s"?\s*\{' % (keyword, re.escape(name)), text)
    if match is None:
        return None
    depth, index = 1, match.end()
    while depth and index < len(text):
        depth += {"{": 1, "}": -1}.get(text[index], 0)
        index += 1
    return text[match.end():index]


def _leaf_body(text, leaf):
    """The statements of ``leaf <name> { ... }``, braces balanced; names may be quoted."""
    return _statement_body(text, "leaf", leaf)


def _fraction_digits(body):
    match = re.search(r'fraction-digits\s+"?(\d+)', body)
    return int(match.group(1)) if match else None


def _type_name(body):
    match = re.search(r'\btype\s+"?([\w:\-]+)"?', body)
    return match.group(1) if match else None


def _typedef_digits(texts, type_name, depth=0):
    """The ``fraction-digits`` of typedef ``type_name`` (any module prefix), read in its own body only and
    following an alias chain to the typedef that declares them; None when not declared."""
    name = type_name.split(":")[-1]
    if depth > MAX_TYPEDEF_DEPTH:
        return None
    for text in texts:
        body = _statement_body(text, "typedef", name)
        if body is None:
            continue
        digits = _fraction_digits(body)
        if digits is not None:
            return digits
        alias = _type_name(body)
        if alias and alias.split(":")[-1] != name:
            return _typedef_digits(texts, alias, depth + 1)
        return None
    return None


def declared_fraction_digits(schemas, module, leaves):
    """The decimals the set-point ``leaves`` of ``module`` declare (the smallest when they differ), or None.

    ``schemas`` maps module names to YANG text; a leaf typed by a typedef is resolved in any of them.
    """
    text = schemas[module]
    found = []
    for leaf in leaves:
        body = _leaf_body(text, leaf)
        if body is None:
            return None
        digits = _fraction_digits(body)
        if digits is None:
            type_name = _type_name(body)
            digits = _typedef_digits(list(schemas.values()), type_name) if type_name else None
        if digits is None:
            return None
        found.append(digits)
    return min(found) if found else None


def device_fraction_digits(manager, module, leaves, type_modules=()):
    """Ask the device (``get-schema``) the decimals of its set-point leaves; None when it does not answer or
    its YANG does not declare them."""
    try:
        schemas = {name: str(manager.get_schema(name).data) for name in (module,) + tuple(type_modules)}
        return declared_fraction_digits(schemas, module, tuple(leaves))
    except Exception as error:  # any failure means "not declared"
        message = (str(error).splitlines() or [type(error).__name__])[0]
        logger.info("Set-point decimals not readable from %s: %s", module, message[:80])
        return None
