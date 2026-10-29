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
    match = _first_statement(text, r'\b%s\s+["\']?%s["\']?\s*\{' % (keyword, re.escape(name)))
    if match is None:
        return None
    return text[match.end():_body_end(text, match.end())]


def _leaf_body(text, leaf):
    """The statements of ``leaf <name> { ... }``, braces balanced; names may be quoted."""
    return _statement_body(text, "leaf", leaf)


def _fraction_digits(body):
    match = _first_statement(body, r'\bfraction-digits\s+["\']?(\d+)')
    return int(match.group(1)) if match else None


def _type_name(body):
    match = _first_statement(body, r'\btype\s+["\']?([\w:\-.]+)["\']?')
    return match.group(1) if match else None


_IDENTIFIER = r"[\w\-.]+"


def _top_level_prefix(text):
    """The prefix a module declares for itself: its ``prefix`` statement at the top level of the module."""
    match = _first_statement(text, r"\b(?:sub)?module\s+[\"']?%s[\"']?\s*\{" % _IDENTIFIER)
    if match is None:
        return None
    depth = 1
    for index in _code_positions(text, match.end()):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return None
        elif depth == 1 and text.startswith("prefix", index) and not re.match(r"[\w\-.:]", text[index - 1]):
            found = re.compile(r"prefix\s+[\"']?(%s)" % _IDENTIFIER).match(text, index)
            if found:
                return found.group(1)
    return None


def _import_prefixes(text):
    """``{prefix: module name}`` of the ``import`` statements of a module."""
    imports, code = {}, None
    for match in re.finditer(r"\bimport\s+[\"']?(%s)[\"']?\s*\{" % _IDENTIFIER, text):
        if code is None:
            code = set(_code_positions(text))
        if match.start() not in code:  # inside a string or a comment
            continue
        body = text[match.end():_body_end(text, match.end())]
        prefix = _first_statement(body, r"\bprefix\s+[\"']?(%s)" % _IDENTIFIER)
        if prefix:
            imports[prefix.group(1)] = match.group(1)
    return imports


def _search_order(names, texts, current, type_name):
    """The module indexes where typedef ``type_name`` is looked for, from ``current`` (the module that uses it):
    the module its prefix binds first (an import of ``current``, or a module declaring that prefix), the
    current module first for an unprefixed name, then all the others."""
    first = []
    if ":" in type_name:
        prefix = type_name.split(":", 1)[0]
        if current is not None and prefix == _top_level_prefix(texts[current]):
            first.append(current)
        elif current is not None and _import_prefixes(texts[current]).get(prefix) in names:
            first.append(names.index(_import_prefixes(texts[current])[prefix]))
        else:
            first.extend(index for index, text in enumerate(texts) if _top_level_prefix(text) == prefix)
    elif current is not None:
        first.append(current)
    return first + [index for index in range(len(texts)) if index not in first]


def _typedef_digits(names, texts, type_name, current=None, visited=()):
    """The ``fraction-digits`` of typedef ``type_name``, read in its own body only and following an alias chain
    to the typedef that declares them; None when not declared.

    The typedef is looked for where its prefix binds it (:func:`_search_order`); ``visited`` holds the
    (module index, name) of the typedefs already followed, so that an alias to a typedef of the same name in
    another module (``typedef gain { type t:gain; }``) is followed there and a loop ends."""
    name = type_name.split(":")[-1]
    if len(visited) > MAX_TYPEDEF_DEPTH:
        return None
    for index in _search_order(names, texts, current, type_name):
        if (index, name) in visited:
            continue
        body = _statement_body(texts[index], "typedef", name)
        if body is None:
            continue
        digits = _fraction_digits(body)
        if digits is not None:
            return digits
        alias = _type_name(body)
        if alias:
            return _typedef_digits(names, texts, alias, index, tuple(visited) + ((index, name),))
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
            names, texts = list(schemas), list(schemas.values())
            digits = _typedef_digits(names, texts, type_name, names.index(module)) if type_name else None
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
