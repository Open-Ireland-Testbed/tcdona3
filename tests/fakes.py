"""Fake transports for the driver tests: no device is ever reached.

- :class:`FakeNetconfManager` stands for an ``ncclient`` manager (Lumentum, ILA);
- :class:`FakeVisaResource` stands for a pyvisa socket resource (Yokogawa);
- :class:`FakeShell` stands for a paramiko interactive channel (TeraFlex SSH).
"""

import contextlib
import xml.etree.ElementTree as ET


class Reply(object):
    """An ncclient reply: ``data_xml`` is the ``<data>`` element, ``xml`` the whole rpc-reply and
    ``str()`` what drivers search ``<ok/>`` in."""

    def __init__(self, data_xml="<data/>", xml=None, ok=True):
        self.data_xml = data_xml
        self.xml = xml if xml is not None else (
            '<nc:rpc-reply xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" message-id="1">%s</nc:rpc-reply>'
            % data_xml)
        self.ok = ok

    def __str__(self):
        if self.ok:
            return '<rpc-reply message-id="1"><ok/></rpc-reply>'
        return self.xml


class Call(object):
    def __init__(self, name, args, kwargs):
        self.name, self.args, self.kwargs = name, args, kwargs

    def __repr__(self):
        return "Call(%r, %r, %r)" % (self.name, self.args, self.kwargs)


def text_of(value):
    """The text of an XML filter or payload, whatever form it was passed in."""
    if isinstance(value, tuple):
        value = value[1]
    if isinstance(value, str):
        return value
    try:
        from lxml import etree
        return etree.tostring(value).decode()
    except Exception:
        return ET.tostring(value, encoding="unicode")


class FakeNetconfManager(object):
    """An ncclient manager answering from ``replies`` and recording every call.

    :param replies: maps an operation (``get``, ``get_config``, ``dispatch``, ``edit_config``, ...) to a
        :class:`Reply` or to a function ``(call) -> Reply``.
    :param fail_on: maps an operation to an exception raised when it is called (once, or always with a list
        holding it repeatedly).
    """

    def __init__(self, replies=None, fail_on=None, capabilities=None, schemas=None):
        self.replies = dict(replies or {})
        self.fail_on = dict(fail_on or {})
        self.calls = []
        self.server_capabilities = list(capabilities if capabilities is not None else
                                        ["urn:ietf:params:netconf:capability:candidate:1.0"])
        self.schemas = dict(schemas or {})
        self.closed = False

    # ------------------------------------------------------------------ helpers
    def names(self):
        return [call.name for call in self.calls]

    def calls_to(self, name):
        return [call for call in self.calls if call.name == name]

    def _answer(self, name, args, kwargs):
        call = Call(name, args, kwargs)
        self.calls.append(call)
        failure = self.fail_on.get(name)
        if isinstance(failure, list):
            if failure:
                raise failure.pop(0)
        elif failure is not None:
            raise failure
        reply = self.replies.get(name, Reply())
        return reply(call) if callable(reply) else reply

    # --------------------------------------------------------------- operations
    def get(self, *args, **kwargs):
        return self._answer("get", args, kwargs)

    def get_config(self, *args, **kwargs):
        return self._answer("get_config", args, kwargs)

    def edit_config(self, *args, **kwargs):
        return self._answer("edit_config", args, kwargs)

    def dispatch(self, *args, **kwargs):
        return self._answer("dispatch", args, kwargs)

    def commit(self, *args, **kwargs):
        return self._answer("commit", args, kwargs)

    def discard_changes(self, *args, **kwargs):
        return self._answer("discard_changes", args, kwargs)

    def get_schema(self, name, *args, **kwargs):
        self._answer("get_schema", (name,) + args, kwargs)
        if name not in self.schemas:
            raise RuntimeError("no schema %s" % name)

        class Schema(object):
            data = self.schemas[name]
        return Schema()

    @contextlib.contextmanager
    def locked(self, target):
        self._answer("lock", (target,), {})
        try:
            yield
        finally:
            self.calls.append(Call("unlock", (target,), {}))

    def close_session(self):
        self.calls.append(Call("close_session", (), {}))
        self.closed = True


def payload(call, name="config"):
    """The XML text a recorded call sent: its ``config``/``filter`` keyword or its first argument."""
    if name in call.kwargs:
        return text_of(call.kwargs[name])
    return text_of(call.args[0])


def leaves(xml_text):
    """``{local tag: text}`` of every leaf of ``xml_text`` (namespaces and prefixes dropped)."""
    root = ET.fromstring(xml_text.strip())
    found = {}
    for element in root.iter():
        if len(element) == 0:
            found[element.tag.split("}")[-1]] = (element.text or "").strip()
    return found


class FakeVisaResource(object):
    """A pyvisa socket resource: ``write`` is recorded, ``query`` answered from ``answers`` (a dict command ->
    answer, or a function)."""

    def __init__(self, answers=None):
        self.answers = answers or {}
        self.writes = []
        self.queries = []
        self.read_termination = None
        self.write_termination = None
        self.timeout = 2000
        self.closed = False
        self._pending = []

    def write(self, command):
        self.writes.append(command)
        if command.strip().endswith("?"):
            self._pending.append(self._lookup(command))

    def read(self):
        return self._pending.pop(0)

    def _lookup(self, command):
        if callable(self.answers):
            return self.answers(command)
        return self.answers.get(command, "0")

    def query(self, command):
        self.queries.append(command)
        return self._lookup(command)

    def close(self):
        self.closed = True


class FakeResourceManager(object):
    def __init__(self, resource):
        self.resource = resource
        self.opened = []
        self.closed = False

    def open_resource(self, name, **kwargs):
        self.opened.append((name, kwargs))
        return self.resource

    def close(self):
        self.closed = True


class FakeShell(object):
    """A paramiko interactive channel answering each command sent with the next queued output.

    :param outputs: a function ``command -> output`` or a dict ``command -> output``; the output is
        delivered in ``chunk`` sized pieces.
    """

    def __init__(self, outputs, chunk=4096, banner="Welcome\r\nadmin@FSP3000C> "):
        self.outputs = outputs
        self.chunk = chunk
        self.sent = []
        self.buffer = banner.encode()
        self.closed = False

    def send(self, text):
        self.sent.append(text)
        command = text.strip()
        if not command or command in (" ", "q"):
            return len(text)
        if callable(self.outputs):
            output = self.outputs(command)
        else:
            output = self.outputs.get(command, "")
        self.buffer += (command + "\r\n" + output).encode()
        return len(text)

    def recv_ready(self):
        return bool(self.buffer)

    def recv(self, size):
        size = min(size, self.chunk)
        data, self.buffer = self.buffer[:size], self.buffer[size:]
        return data

    def exit_status_ready(self):
        return self.closed

    def close(self):
        self.closed = True
