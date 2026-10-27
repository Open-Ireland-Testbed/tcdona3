"""NETCONF sessions that survive a dropped connection (ported from ols-control-app, for Python 3.6).

A driver keeps its ncclient manager in ``self.m``. With ``reconnect=True`` that manager is wrapped in a
:class:`ReconnectingSession`, which behaves like the manager (every attribute and RPC is forwarded) and,
when an operation fails because the session dropped, opens the session again and repeats the operation
when repeating it is safe:

- reads (``get``, ``get_config``, ``get_schema``) always;
- writes that set a value (``edit_config``, a ``dispatch`` that adds or modifies);
- a ``dispatch`` that deletes is never repeated: the session is re-established and :class:`SessionDropped`
  tells the caller to read the device back.

A read whose reply does not come in time is sent again on the same session; a write that timed out is never
repeated (it may have been applied), unless the caller declares it idempotent with
:meth:`ReconnectingSession.idempotent`. Attempts are bounded and logged; no address or credential is logged.
"""

import logging
import time

logger = logging.getLogger("tcdona3.netconf")

#: Times a dropped session is opened again for one operation, and the wait before each [s].
RECONNECT_ATTEMPTS = 3
RECONNECT_WAIT_S = 5.0
#: Times a read without a reply in time is sent again on the same session, and the wait before each [s].
READ_TIMEOUT_REPEATS = 2
READ_TIMEOUT_WAIT_S = 5.0
#: The operations always safe to repeat.
READS = ("get", "get_config", "get_schema")

#: The texts of a dropped session when the error type does not tell it.
DROP_TEXTS = ("not connected", "session closed", "socket is closed", "connection reset", "broken pipe",
              "connection aborted", "channel closed", "invalid session", "resource might be closed")
#: The error types of a reply that did not come in time, by name.
TIMEOUT_ERRORS = ("TimeoutExpiredError",)
#: The error types of a dropped session, by name, wherever they appear in the error's class hierarchy.
DROP_ERRORS = ("TransportError", "SessionCloseError", "SessionError", "SSHError", "SSHException", "EOFError",
               "ConnectionError", "BrokenPipeError", "ConnectionResetError", "InvalidSession")
#: The error types of a refused login, by name.
AUTHENTICATION_ERRORS = ("AuthenticationError", "AuthenticationException", "BadAuthenticationType")


class SessionDropped(ConnectionError):
    """The session of a device dropped: re-established without repeating the operation, or not
    re-established within the attempts."""


def _names(error):
    return {cls.__name__ for cls in type(error).__mro__}


def is_authentication_error(error):
    return type(error).__name__ in AUTHENTICATION_ERRORS


def is_reply_timeout(error):
    """Whether ``error`` is a reply that did not come in time (the session itself still open)."""
    text = str(error).lower()
    return bool(_names(error) & set(TIMEOUT_ERRORS)) or "timed out" in text or "timeout expired" in text


def is_session_drop(error):
    """Whether ``error`` means the session dropped (not a refused login, not a reply timeout)."""
    if is_authentication_error(error) or is_reply_timeout(error):
        return False
    if _names(error) & set(DROP_ERRORS):
        return True
    text = str(error).lower()
    return any(marker in text for marker in DROP_TEXTS)


def repeatable(operation, args):
    """Whether ``operation`` with ``args`` may be applied again after a drop."""
    if operation in READS or operation == "edit_config":
        return True
    if operation == "dispatch":
        try:
            body = args[0] if args else ""
            if not isinstance(body, str):
                from lxml import etree
                body = etree.tostring(body).decode()
        except Exception:  # an unreadable request is not repeated
            return False
        return "delete" not in body.lower() and "remove" not in body.lower()
    return False


def reads_only(operation, args):
    """Only the reads are repeated (a driver that repeats its write transactions whole)."""
    return operation in READS


class ReconnectingSession(object):
    """An ncclient manager opened again by ``reopen`` when its session drops.

    :param manager: the open manager.
    :param reopen: a function opening a new manager.
    :param device: names the device in the logs and errors (no address).
    """

    def __init__(self, manager, reopen, device, attempts=RECONNECT_ATTEMPTS, wait_s=RECONNECT_WAIT_S,
                 repeatable=repeatable, timeout_repeats=READ_TIMEOUT_REPEATS, timeout_wait_s=READ_TIMEOUT_WAIT_S):
        object.__setattr__(self, "_manager", manager)
        object.__setattr__(self, "_reopen", reopen)
        object.__setattr__(self, "_device", device)
        object.__setattr__(self, "_attempts", attempts)
        object.__setattr__(self, "_wait_s", wait_s)
        object.__setattr__(self, "_repeatable", repeatable)
        object.__setattr__(self, "_timeout_repeats", timeout_repeats)
        object.__setattr__(self, "_timeout_wait_s", timeout_wait_s)

    @property
    def manager(self):
        return self._manager

    def close_session(self):
        self._manager.close_session()

    def reopen(self):
        """Open the session again now (a caller that repeats a whole transaction)."""
        self._reconnect("a transaction", ConnectionError("dropped during a transaction"))

    def __setattr__(self, name, value):
        setattr(self._manager, name, value)

    def __getattr__(self, name):
        attribute = getattr(self._manager, name)
        if not callable(attribute):
            return attribute

        def call(*args, **kwargs):
            return self._call(name, args, kwargs)
        return call

    def idempotent(self, operation, *args, **kwargs):
        """``operation`` declared idempotent by the caller: sent again after a reply timeout, like a read
        (``repeats`` times ``wait_s`` apart, keywords default to the session's); a drop is handled as usual."""
        repeats = kwargs.pop("repeats", None)
        wait_s = kwargs.pop("wait_s", None)
        return self._call(operation, args, kwargs, idempotent=True, repeats=repeats, wait_s=wait_s)

    def _reconnect(self, operation, error):
        last = error
        for attempt in range(1, self._attempts + 1):
            logger.warning("%s: session dropped during %s (%s); opening it again (%d of %d)", self._device,
                           operation, last, attempt, self._attempts)
            time.sleep(self._wait_s)
            try:
                object.__setattr__(self, "_manager", self._reopen())
                logger.info("%s: session re-established", self._device)
                return
            except Exception as reopen_error:
                if is_authentication_error(reopen_error):
                    raise SessionDropped("%s: session dropped during %s; the login was refused, not retried: %s"
                                         % (self._device, operation, reopen_error))
                last = reopen_error
        raise SessionDropped("%s: session dropped during %s and not re-established after %d attempts: %s"
                             % (self._device, operation, self._attempts, last))

    def _send(self, operation, args, kwargs, idempotent=False, repeats=None, wait_s=None):
        repeats = self._timeout_repeats if repeats is None else repeats
        wait_s = self._timeout_wait_s if wait_s is None else wait_s
        for repeat in range(repeats + 1):
            try:
                return getattr(self._manager, operation)(*args, **kwargs)
            except Exception as error:
                if not (idempotent or operation in READS) or not is_reply_timeout(error) or repeat == repeats:
                    raise
                logger.warning("%s: no reply in time to %s; sent again in %.0f s (%d of %d)", self._device,
                               operation, wait_s, repeat + 1, repeats)
                time.sleep(wait_s)

    def _call(self, operation, args, kwargs, **send):
        for round_ in range(self._attempts + 1):
            try:
                return self._send(operation, args, kwargs, **send)
            except Exception as error:
                if not is_session_drop(error):
                    raise
                if round_ == self._attempts:
                    raise SessionDropped("%s: %s failed after the session was re-established %d times: %s"
                                         % (self._device, operation, self._attempts, error))
                self._reconnect(operation, error)
                if not self._repeatable(operation, args):
                    raise SessionDropped("%s: session dropped during %s; re-established, the operation is not "
                                         "repeated: read the device back" % (self._device, operation))
                logger.info("%s: %s repeated on the new session", self._device, operation)


def dispatch_idempotent(manager, rpc, repeats=2, wait_s=10.0):
    """Dispatch ``rpc`` (an add or modify that leaves the device as once when sent twice): through
    :meth:`ReconnectingSession.idempotent` when ``manager`` is one, else once."""
    if isinstance(manager, ReconnectingSession):
        return manager.idempotent("dispatch", rpc, repeats=repeats, wait_s=wait_s)
    return manager.dispatch(rpc)


def close_quietly(manager):
    """Close the session of ``manager``; a session already gone is not an error."""
    if manager is None:
        return
    try:
        manager.close_session()
    except Exception as error:
        logger.info("closing the NETCONF session: %s", error)
