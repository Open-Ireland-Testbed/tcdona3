"""The features ``ILA`` gained in 1.1.0: transactional writes, validation, options, new methods."""

import contextlib

import pytest

import tcdona3.ila as ila_module
from tcdona3 import _netconf
from tcdona3.ila import ILA

from fakes import Call, FakeNetconfManager, Reply, leaves, payload

OOD = 'xmlns="http://org/openroadm/device"'
NC = 'xmlns="urn:ietf:params:xml:ns:netconf:base:1.0"'


class TransportError(Exception):
    """Named like ncclient's: a dropped session."""


def amplifier(amp, branch, leaf, value):
    return ("<data %s><open-optical-device %s><optical-amplifier><amplifiers><amplifier><name>%s</name>"
            "<%s><%s>%s</%s></%s></amplifier></amplifiers></optical-amplifier></open-optical-device></data>"
            % (NC, OOD, amp, branch, leaf, value, leaf, branch))


def evoa(number, value):
    return ("<data %s><open-optical-device %s><evoas><evoa-id>%d</evoa-id><evoa><attn-value>%s</attn-value></evoa>"
            "</evoas></open-optical-device></data>" % (NC, OOD, number, value))


YANG_AMP = """module openconfig-optical-amplifier {
  grouping config {
    leaf target-gain { type oc-types:ieeefloat32-3; }
    leaf target-gain-tilt { type decimal64 { fraction-digits 2; } }
    leaf target-output-power { type decimal64 { fraction-digits 2; } }
  }
}"""
YANG_TYPES = "module openconfig-types { typedef ieeefloat32-3 { type decimal64 { fraction-digits 3; } } }"


@pytest.fixture
def managers(monkeypatch):
    queue, calls = [], []

    def connect(**kwargs):
        calls.append(kwargs)
        return queue.pop(0) if queue else FakeNetconfManager()

    monkeypatch.setattr(ila_module, "check_patch_owners", lambda patches: True)
    monkeypatch.setattr(ila_module.manager, "connect", connect)
    return queue, calls


def new(managers, fake=None, **options):
    queue, _ = managers
    fake = fake or FakeNetconfManager()
    queue.append(fake)
    return ILA("ila_1", **options), fake


TRANSACTION = ["lock", "discard_changes", "edit_config", "commit", "unlock"]


# ---------------------------------------------------------------------------------------------- transactions
def test_a_write_is_one_locked_transaction(managers):
    device, fake = new(managers)
    device.set_target_gain("ab", 15.0)
    assert fake.names() == TRANSACTION
    assert fake.calls[0].args == ("candidate",)


def test_a_failed_commit_is_discarded_and_the_lock_released(managers):
    device, fake = new(managers, FakeNetconfManager(fail_on={"commit": RuntimeError("refused")}))
    with pytest.raises(RuntimeError):
        device.set_amp_state("ab", "true")
    assert fake.names() == ["lock", "discard_changes", "edit_config", "commit", "discard_changes", "unlock"]


def test_reconnect_applies_an_interrupted_transaction_again(managers):
    queue, calls = managers
    first = FakeNetconfManager(fail_on={"edit_config": TransportError("session closed")})
    second = FakeNetconfManager()
    queue.extend([first, second])
    device = ILA("ila_1", reconnect=True)
    assert isinstance(device.m, _netconf.ReconnectingSession)
    device.set_evoa_atten("ab", 3.0)
    # the interrupted transaction is cleaned up on its own session; the new one gets the whole transaction
    assert first.names() == ["lock", "discard_changes", "edit_config", "discard_changes", "unlock"]
    assert second.names() == TRANSACTION
    assert len(calls) == 2


class _DiesOnEdit(FakeNetconfManager):
    """A session that drops during the edit: every later operation on it, the unlock included, fails."""

    def __init__(self):
        super(_DiesOnEdit, self).__init__(fail_on={"edit_config": TransportError("session closed")})
        self.dead = False

    def _answer(self, name, args, kwargs):
        if self.dead:
            self.calls.append(Call(name + " (dead)", args, kwargs))
            raise TransportError("Not connected to NETCONF server")
        try:
            return super(_DiesOnEdit, self)._answer(name, args, kwargs)
        except TransportError:
            self.dead = True
            raise

    @contextlib.contextmanager
    def locked(self, target):
        self._answer("lock", (target,), {})
        try:
            yield
        finally:
            self._answer("unlock", (target,), {})


def test_a_drop_mid_transaction_never_sends_its_clean_up_on_the_new_session(managers):
    queue, calls = managers
    first, second = _DiesOnEdit(), FakeNetconfManager()
    queue.extend([first, second])
    device = ILA("ila_1", reconnect=True)
    device.set_target_gain("ab", 15.0)
    assert first.names() == ["lock", "discard_changes", "edit_config", "discard_changes (dead)", "unlock (dead)"]
    assert second.names() == TRANSACTION
    assert len(calls) == 2


def test_a_drop_in_both_attempts_propagates(managers):
    queue, calls = managers
    queue.extend([_DiesOnEdit(), _DiesOnEdit()])
    device = ILA("ila_1", reconnect=True)
    with pytest.raises(Exception) as raised:
        device.set_target_gain("ab", 15.0)
    assert _netconf.is_session_drop(raised.value)
    assert len(calls) == 2


def test_without_reconnect_a_dropped_session_propagates(managers):
    device, fake = new(managers, FakeNetconfManager(fail_on={"edit_config": TransportError("session closed")}))
    with pytest.raises(TransportError):
        device.set_target_gain("ab", 15.0)


def test_reconnect_repeats_a_read(managers):
    queue, _ = managers
    first = FakeNetconfManager(fail_on={"get_config": TransportError("session closed")})
    second = FakeNetconfManager(replies={"get_config": Reply(amplifier("ab", "config", "target-gain", "15.00"))})
    queue.extend([first, second])
    assert ILA("ila_1", reconnect=True).get_target_gain("ab") == "15.00"


def test_timeout_is_passed_to_connect(managers):
    _, calls = managers
    new(managers, timeout=20)
    assert calls[-1]["timeout"] == 20


def test_close_and_context_manager(managers):
    queue, _ = managers
    fake = FakeNetconfManager()
    queue.append(fake)
    with ILA("ila_1") as device:
        pass
    assert fake.closed


# ------------------------------------------------------------------------------------------------ validation
@pytest.mark.parametrize("call", [lambda d: d.set_target_gain("ab", float("inf")),
                                  lambda d: d.set_target_gain("ab", "high"),
                                  lambda d: d.set_target_gain("cd", 15.0),
                                  lambda d: d.set_amp_state("ab", "on"),
                                  lambda d: d.set_amp_state("xy", True),
                                  lambda d: d.set_evoa_atten("ab", float("nan")),
                                  lambda d: d.set_target_gain_tilt("ab", float("nan")),
                                  lambda d: d.set_gain_range("ab", "ULTRA_GAIN_RANGE")])
def test_invalid_values_are_refused_before_any_rpc(managers, call):
    device, fake = new(managers)
    with pytest.raises(ValueError):
        call(device)
    assert fake.calls == []


@pytest.mark.parametrize("state,leaf", [(True, "true"), (False, "false"), ("TRUE", "true"), (" false ", "false")])
def test_states_are_yang_booleans(managers, state, leaf):
    device, fake = new(managers)
    device.set_amp_state("ba", state)
    assert leaves(payload(fake.calls_to("edit_config")[0]))["enabled"] == leaf


def test_a_numeric_string_gain_is_accepted(managers):
    device, fake = new(managers)
    device.set_target_gain("ab", "16.5")
    assert leaves(payload(fake.calls_to("edit_config")[0]))["target-gain"] == "16.5"


@pytest.mark.parametrize("value", [-1, 1.5, "2", True])
def test_invalid_options_are_refused(managers, value):
    with pytest.raises(ValueError):
        new(managers, setpoint_digits=value)
    with pytest.raises(ValueError):
        new(managers, evoa_numbering="other")


# ----------------------------------------------------------------------------------------------------- options
def test_setpoint_digits_as_an_int(managers):
    device, fake = new(managers, setpoint_digits=2)
    device.set_target_gain("ab", 15.123)
    assert leaves(payload(fake.calls_to("edit_config")[0]))["target-gain"] == "15.12"


def test_setpoint_digits_from_the_device_yang(managers):
    fake = FakeNetconfManager(schemas={"openconfig-optical-amplifier": YANG_AMP, "openconfig-types": YANG_TYPES})
    device, fake = new(managers, fake, setpoint_digits="device")
    assert device.number_of_digits == 2
    device.set_target_gain_tilt("ab", -0.456)
    assert leaves(payload(fake.calls_to("edit_config")[0]))["target-gain-tilt"] == "-0.46"
    assert len(fake.calls_to("get_schema")) == 2


def test_setpoint_digits_default_when_the_device_does_not_declare_them(managers):
    device, fake = new(managers, setpoint_digits="device")
    assert device.number_of_digits == ila_module.DEFAULT_DEVICE_DIGITS


@pytest.mark.parametrize("amp,number", [("ab", 2), ("ba", 1)])
def test_measured_evoa_numbering(managers, amp, number):
    device, fake = new(managers, evoa_numbering="measured")
    device.set_evoa_atten(amp, 4.0)
    assert leaves(payload(fake.calls_to("edit_config")[0]))["evoa-id"] == str(number)


@pytest.mark.parametrize("amp,number", [("ab", 2), ("ba", 1)])
def test_output_evoa_is_the_measured_one_whatever_the_numbering(managers, amp, number):
    device, fake = new(managers)
    fake.replies["get_config"] = Reply(evoa(number, "5.0"))
    device.set_output_evoa_atten(amp, 5.0)
    assert device.get_output_evoa_atten(amp) == "5.0"
    assert leaves(payload(fake.calls_to("edit_config")[0]))["evoa-id"] == str(number)
    assert leaves(payload(fake.calls_to("get_config")[0], "filter"))["evoa-id"] == str(number)
    assert device.evoa_map == ila_module.EVOA_LEGACY


# ------------------------------------------------------------------------------------------------- new methods
def test_set_target_gain_tilt(managers):
    device, fake = new(managers)
    device.set_target_gain_tilt("ba", -1.0)
    sent = leaves(payload(fake.calls_to("edit_config")[0]))
    assert (sent["name"], sent["target-gain-tilt"]) == ("ba", "-1.0")


@pytest.mark.parametrize("given", ["high_gain_range", "oc-opt-amp:HIGH_GAIN_RANGE"])
def test_set_gain_range_declares_the_identity_prefix(managers, given):
    device, fake = new(managers)
    device.set_gain_range("ab", given)
    text = payload(fake.calls_to("edit_config")[0])
    assert 'xmlns:oc-opt-amp="http://openconfig.net/yang/optical-amplfier"' in text
    assert leaves(text)["gain-range"] == "oc-opt-amp:HIGH_GAIN_RANGE"


@pytest.mark.parametrize("method,leaf,value", [("get_target_gain_tilt", "target-gain-tilt", "-0.50"),
                                               ("get_gain_range", "gain-range", "oc-opt-amp:LOW_GAIN_RANGE")])
def test_config_readers(managers, method, leaf, value):
    device, fake = new(managers)
    fake.replies["get_config"] = Reply(amplifier("ab", "config", leaf, value))
    assert getattr(device, method)("ab") == value
    assert fake.calls_to("get_config")[0].kwargs["source"] == "running"


@pytest.mark.parametrize("method,leaf", [("get_actual_gain", "actual-gain"),
                                         ("get_actual_gain_tilt", "actual-gain-tilt"),
                                         ("get_input_power", "input-power-total"),
                                         ("get_output_power", "output-power-total")])
@pytest.mark.parametrize("value", ["-12.25", "<instant>-12.25</instant>"], ids=["plain", "instant"])
def test_state_readers_are_floats_from_the_operational_state(managers, method, leaf, value):
    """The TCX-1000 wraps its state counters in ``<instant>``."""
    device, fake = new(managers)
    fake.replies["get"] = Reply(amplifier("ba", "state", leaf, value))
    assert getattr(device, method)("ba") == -12.25
    sent = leaves(payload(fake.calls_to("get")[0], "filter"))
    assert sent["name"] == "ba" and leaf in sent


def test_op_ranges_are_documented():
    assert ila_module.OP_RANGES["low"]["max_gain"] == 20.0
    assert ila_module.OP_RANGES["high"]["min_gain"] == 17.0


def test_the_login_can_come_from_the_environment(managers, monkeypatch):
    _, calls = managers
    monkeypatch.setenv("ILA_USERNAME", "<user>")
    monkeypatch.setenv("ILA_PASSWORD", "<password>")
    new(managers)
    assert (calls[-1]["username"], calls[-1]["password"]) == ("<user>", "<password>")


def test_without_the_environment_the_login_is_the_default(managers, monkeypatch):
    _, calls = managers
    monkeypatch.delenv("ILA_USERNAME", raising=False)
    monkeypatch.delenv("ILA_PASSWORD", raising=False)
    new(managers)
    assert (calls[-1]["username"], calls[-1]["password"]) == (ila_module.user, ila_module.password)


# ----------------------------------------------------------------------------- set-point decimals from YANG
def test_yang_decimals_are_read_in_the_typedef_body_only():
    """An alias typedef without fraction-digits must not borrow those of the next typedef in the text."""
    from tcdona3._yang_precision import declared_fraction_digits
    module = "module m { leaf target-gain { type t:gain-alias; } }"
    types = ("module t { typedef gain-alias { type t:ieeefloat32-3; description \"an alias\"; } "
             "typedef unrelated-1 { type decimal64 { fraction-digits 1; } } "
             "typedef ieeefloat32-3 { type decimal64 { fraction-digits 3; } } }")
    assert declared_fraction_digits({"m": module, "t": types}, "m", ("target-gain",)) == 3


def test_yang_decimals_of_an_undeclared_or_looping_type_are_none():
    from tcdona3._yang_precision import declared_fraction_digits
    module = "module m { leaf target-gain { type t:a; } }"
    looping = "module t { typedef a { type t:b; } typedef b { type t:a; } typedef c { type decimal64 { fraction-digits 1; } } }"
    assert declared_fraction_digits({"m": module, "t": looping}, "m", ("target-gain",)) is None
    plain = "module t { typedef a { type string; } typedef c { type decimal64 { fraction-digits 1; } } }"
    assert declared_fraction_digits({"m": module, "t": plain}, "m", ("target-gain",)) is None


def test_yang_braces_and_keywords_inside_strings_and_comments_are_ignored():
    from tcdona3._yang_precision import declared_fraction_digits
    module = "module m { leaf target-gain { type t:g; } }"
    types = ('module t { typedef g { description "Example } delimiter, type of amplifier { gain";\n'
             "  // a comment with a } brace and fraction-digits 9\n"
             "  /* another { one */ reference 'see } here';\n"
             "  type decimal64 { fraction-digits 3; } } }")
    assert declared_fraction_digits({"m": module, "t": types}, "m", ("target-gain",)) == 3


def test_yang_quoted_arguments_are_read():
    from tcdona3._yang_precision import declared_fraction_digits
    module = 'module m { leaf "target-gain" { type "decimal64" { fraction-digits "2"; } } }'
    assert declared_fraction_digits({"m": module}, "m", ("target-gain",)) == 2


def test_yang_alias_to_a_typedef_of_the_same_name_in_another_module():
    from tcdona3._yang_precision import declared_fraction_digits
    module = "module m { typedef gain { type t:gain; } leaf target-gain { type gain; } }"
    types = "module t { typedef gain { type decimal64 { fraction-digits 3; } } }"
    assert declared_fraction_digits({"m": module, "t": types}, "m", ("target-gain",)) == 3


# ------------------------------------------------------------------------------- legacy EVOA numbering warning
@pytest.fixture
def evoa_warning_not_yet_shown(monkeypatch):
    monkeypatch.setattr(ila_module, "_legacy_evoa_warned", [False])


def test_legacy_evoa_numbering_warns_once_per_process_and_writes_as_before(managers, evoa_warning_not_yet_shown):
    import warnings
    device, fake = new(managers)
    fake.replies["get_config"] = Reply(evoa(1, "3.0"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        device.set_evoa_atten("ab", 3.0)
        device.set_evoa_atten("ba", 4.0)
        device.get_evoa_atten("ab")
    shown = [w for w in caught if "legacy EVOA numbering" in str(w.message)]
    assert len(shown) == 1 and shown[0].category is UserWarning
    assert shown[0].filename == __file__  # points at the caller, not at the driver
    written = [leaves(payload(call))["evoa-id"] for call in fake.calls_to("edit_config")]
    assert written == ["1", "2"]  # legacy: ab -> 1, ba -> 2, unchanged


def test_measured_numbering_and_output_evoa_methods_do_not_warn(managers, evoa_warning_not_yet_shown):
    import warnings
    device, fake = new(managers, evoa_numbering="measured")
    legacy, _ = new(managers)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        device.set_evoa_atten("ab", 3.0)
        legacy.set_output_evoa_atten("ab", 3.0)
    assert not [w for w in caught if "legacy EVOA numbering" in str(w.message)]


def test_yang_a_prefixed_type_resolves_in_the_module_its_prefix_binds():
    """A local typedef of the same name must not shadow the imported one named by the prefix."""
    from tcdona3._yang_precision import declared_fraction_digits
    module = ("module m { prefix m; import t-types { prefix t; } "
              "typedef gain { type decimal64 { fraction-digits 1; } } leaf target-gain { type t:gain; } }")
    types = "module t-types { prefix tt; typedef gain { type decimal64 { fraction-digits 3; } } }"
    assert declared_fraction_digits({"m": module, "t-types": types}, "m", ("target-gain",)) == 3
    # the module's own prefix, and no prefix, resolve locally
    local = module.replace("type t:gain;", "type m:gain;")
    assert declared_fraction_digits({"m": local, "t-types": types}, "m", ("target-gain",)) == 1
    bare = module.replace("type t:gain;", "type gain;")
    assert declared_fraction_digits({"m": bare, "t-types": types}, "m", ("target-gain",)) == 1
    # a prefix not imported resolves in the module that declares it
    unimported = ("module m { prefix m; typedef gain { type decimal64 { fraction-digits 1; } } "
                  "leaf target-gain { type tt:gain; } }")
    assert declared_fraction_digits({"m": unimported, "t-types": types}, "m", ("target-gain",)) == 3


def test_yang_single_quoted_arguments_are_read():
    from tcdona3._yang_precision import declared_fraction_digits
    module = "module m { leaf 'target-gain' { type 'decimal64' { fraction-digits '2'; } } }"
    assert declared_fraction_digits({"m": module}, "m", ("target-gain",)) == 2
    aliased = "module m { prefix m; typedef 'g' { type 'decimal64' { fraction-digits '3'; } } leaf target-gain { type 'm:g'; } }"
    assert declared_fraction_digits({"m": aliased}, "m", ("target-gain",)) == 3
