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


def _code_positions(text, start=0):
    """The positions of ``text`` from ``start`` that are YANG statements, not inside a quoted string or a
    comment (RFC 7950 6.1: an unquoted ``//`` or ``/*`` always starts a comment)."""
    index, length = start, len(text)
    while index < length:
        char = text[index]
        if char == '"':  # a double-quoted string, with backslash escapes
            index += 1
            while index < length and text[index] != '"':
                index += 2 if text[index] == "\\" else 1
        elif char == "'":  # a single-quoted string, no escapes
            index = text.find("'", index + 1)
            index = length if index < 0 else index
        elif text.startswith("//", index):
            index = text.find("\n", index)
            index = length if index < 0 else index
        elif text.startswith("/*", index):
            index = text.find("*/", index + 2)
            index = length if index < 0 else index + 1
        else:
            yield index
        index += 1


def _body_end(text, start):
    """The index just past the ``}`` closing the block opened before ``start``; braces inside quoted strings
    and comments do not count."""
    depth = 1
    for index in _code_positions(text, start):
        depth += {"{": 1, "}": -1}.get(text[index], 0)
        if depth == 0:
            return index + 1
    return len(text)


def _first_statement(text, pattern):
    """The first match of ``pattern`` that starts in a statement, not inside a quoted string or a comment
    (a description saying "type of amplifier" is not a ``type`` statement); arguments may be quoted."""
    code = None
    for match in re.finditer(pattern, text):
        if code is None:
            code = set(_code_positions(text))
        if match.start() in code:
            return match
    return None


def _statement_body(text, keyword, name):
    """The statements of ``<keyword> <name> { ... }``, braces balanced; names may be quoted."""
    match = _first_statement(text, r'\b%s\s+"?%s"?\s*\{' % (keyword, re.escape(name)))
    if match is None:
        return None
    return text[match.end():_body_end(text, match.end())]


def _leaf_body(text, leaf):
    """The statements of ``leaf <name> { ... }``, braces balanced; names may be quoted."""
    return _statement_body(text, "leaf", leaf)


def _fraction_digits(body):
    match = _first_statement(body, r'\bfraction-digits\s+"?(\d+)')
    return int(match.group(1)) if match else None


def _type_name(body):
    match = _first_statement(body, r'\btype\s+"?([\w:\-]+)"?')
    return match.group(1) if match else None


def _typedef_digits(texts, type_name, visited=()):
    """The ``fraction-digits`` of typedef ``type_name`` (any module prefix), read in its own body only and
    following an alias chain to the typedef that declares them; None when not declared.

    ``visited`` holds the (module index, name) of the typedefs already followed: an alias to a typedef of the
    same name in another module (``typedef gain { type t:gain; }``) is followed there, and a loop ends."""
    name = type_name.split(":")[-1]
    if len(visited) > MAX_TYPEDEF_DEPTH:
        return None
    for index, text in enumerate(texts):
        if (index, name) in visited:
            continue
        body = _statement_body(text, "typedef", name)
        if body is None:
            continue
        digits = _fraction_digits(body)
        if digits is not None:
            return digits
        alias = _type_name(body)
        if alias:
            return _typedef_digits(texts, alias, tuple(visited) + ((index, name),))
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
