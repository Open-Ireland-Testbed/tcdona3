"""Set-point precision as a device declares it in its YANG (Python 3.6 compatible).

A NETCONF device publishes its YANG modules through ``get-schema``. Each set-point leaf (gain, tilt, output
power) is a ``decimal64`` whose ``fraction-digits`` is the number of decimals the device stores: a value
written with more decimals is rounded by the device.
"""

import logging
import re

logger = logging.getLogger("tcdona3.yang")


def _leaf_body(text, leaf):
    """The statements of ``leaf <name> { ... }``, braces balanced; names may be quoted."""
    match = re.search(r'leaf\s+"?%s"?\s*\{' % re.escape(leaf), text)
    if match is None:
        return None
    depth, index = 1, match.end()
    while depth and index < len(text):
        depth += {"{": 1, "}": -1}.get(text[index], 0)
        index += 1
    return text[match.end():index]


def _fraction_digits(body):
    match = re.search(r'fraction-digits\s+"?(\d+)', body)
    return int(match.group(1)) if match else None


def _typedef_digits(texts, type_name):
    name = re.escape(type_name.split(":")[-1])
    for text in texts:
        match = re.search(r'typedef\s+"?%s"?\s*\{' % name, text)
        if match:
            digits = _fraction_digits(text[match.end():match.end() + 600])
            if digits is not None:
                return digits
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
            type_match = re.search(r'type\s+"?([\w:\-]+)"?', body)
            digits = _typedef_digits(schemas.values(), type_match.group(1)) if type_match else None
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
