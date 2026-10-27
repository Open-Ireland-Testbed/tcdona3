"""Backward compatibility of ``tcdona3.ila.ILA`` (Juniper TCX-1000 in-line amplifier, NETCONF)."""

import pytest

import tcdona3.ila as ila_module
from tcdona3.ila import ILA

from fakes import FakeNetconfManager, Reply, leaves, payload

OOD = 'xmlns="http://org/openroadm/device"'
NC = 'xmlns="urn:ietf:params:xml:ns:netconf:base:1.0"'


def amplifier_config(amp, leaf, value):
    return ("<data %s><open-optical-device %s><optical-amplifier><amplifiers><amplifier><name>%s</name>"
            "<config><%s>%s</%s></config></amplifier></amplifiers></optical-amplifier></open-optical-device></data>"
            % (NC, OOD, amp, leaf, value, leaf))


def evoa_config(number, value):
    return ("<data %s><open-optical-device %s><evoas><evoa-id>%d</evoa-id><evoa><attn-value>%s</attn-value></evoa>"
            "</evoas></open-optical-device></data>" % (NC, OOD, number, value))


PM = ("<data %s><open-optical-device %s><optical-amplifier><amplifiers><amplifier><name>ab</name>"
      "<config><target-gain>15.00</target-gain></config><state><actual-gain>14.90</actual-gain></state>"
      "</amplifier></amplifiers></optical-amplifier></open-optical-device></data>" % (NC, OOD))


@pytest.fixture
def connect(monkeypatch):
    fake = FakeNetconfManager()
    calls = []

    def connect(**kwargs):
        calls.append(kwargs)
        return fake

    monkeypatch.setattr(ila_module, "check_patch_owners", lambda patches: True)
    monkeypatch.setattr(ila_module.manager, "connect", connect)
    return fake, calls


@pytest.fixture
def ila(connect):
    fake, _ = connect
    return ILA("ila_1"), fake


# ------------------------------------------------------------------------------------------------ construction
def test_an_unknown_device_is_refused(connect):
    _, calls = connect
    with pytest.raises(ValueError):
        ILA("ila_99")
    with pytest.raises(ValueError):
        ILA(1)
    assert calls == []


def test_an_unauthorised_user_is_refused_before_connecting(connect, monkeypatch):
    _, calls = connect
    checked = []
    monkeypatch.setattr(ila_module, "check_patch_owners", lambda patches: checked.append(patches) and False)
    with pytest.raises(Exception, match="not authorized"):
        ILA("ila_2")
    assert checked == [[("ila_2_fwd", "ila_2_bck")]]
    assert calls == []


@pytest.mark.parametrize("device", ["ila_%d" % n for n in range(1, 12)] + ["dummy_ila_1"])
def test_every_known_device_connects_on_port_830(connect, device):
    fake, calls = connect
    ila = ILA(device)
    assert ila.m is fake
    assert calls[-1]["port"] == 830 and calls[-1]["hostkey_verify"] is False
    assert {"host", "username", "password"} <= set(calls[-1])


def test_the_manager_is_usable_directly(ila):
    """ILAMonitor reads through ``ila.m.get`` and ``ila.m.get_config``."""
    device, fake = ila
    fake.replies["get"] = Reply(PM)
    assert "actual-gain" in str(device.m.get(filter=("subtree", "<x/>")).data_xml)
    device.m.get_config(source="running", filter=("subtree", "<x/>"))
    assert fake.names() == ["get", "get_config"]


# ----------------------------------------------------------------------------------------------------- reads
def test_get_pm_xml_is_a_list_of_one_pretty_rpc_reply(ila):
    device, fake = ila
    fake.replies["get"] = Reply(PM)
    xml = device.get_pm_xml()
    assert isinstance(xml, list) and len(xml) == 1
    assert xml[0].lstrip().startswith("<nc:rpc-reply") and "\n" in xml[0]
    assert "<actual-gain>14.90</actual-gain>" in xml[0]
    sent = payload(fake.calls_to("get")[0], "filter")
    for leaf in ("target-gain", "actual-gain", "input-power-total", "output-power-total", "amp-mode",
                 "gain-range", "operational-state"):
        assert "<%s/>" % leaf in sent


def test_get_pm_xml_raises_on_an_rpc_error(ila):
    device, fake = ila
    fake.fail_on["get"] = RuntimeError("rpc error")
    with pytest.raises(RuntimeError):
        device.get_pm_xml()


@pytest.mark.parametrize("method,leaf,value", [("get_target_gain", "target-gain", "15.00"),
                                               ("get_amp_state", "enabled", "true")])
@pytest.mark.parametrize("amp", ["ab", "ba"])
def test_config_getters_return_the_raw_leaf_text(ila, method, leaf, value, amp):
    device, fake = ila
    fake.replies["get_config"] = Reply(amplifier_config(amp, leaf, value))
    assert getattr(device, method)(amp) == value
    call = fake.calls_to("get_config")[0]
    assert call.kwargs["source"] == "running" and call.kwargs["filter"][0] == "subtree"
    assert leaves(payload(call, "filter"))["name"] == amp


@pytest.mark.parametrize("amp,number", [("ab", 1), ("ba", 2)])
def test_evoa_getter_numbering(ila, amp, number):
    device, fake = ila
    fake.replies["get_config"] = Reply(evoa_config(number, "3.0"))
    assert device.get_evoa_atten(amp) == "3.0"
    assert leaves(payload(fake.calls_to("get_config")[0], "filter"))["evoa-id"] == str(number)


@pytest.mark.parametrize("method,args", [("get_evoa_atten", ("cd",)), ("set_evoa_atten", ("cd", 1.0))])
def test_evoa_refuses_an_unknown_direction(ila, method, args):
    device, fake = ila
    with pytest.raises(ValueError):
        getattr(device, method)(*args)
    assert fake.calls == []


# ---------------------------------------------------------------------------------------------------- writes
def edit_then_commit(fake):
    """The leaves of the one candidate edit, after checking that it was committed."""
    edits = fake.calls_to("edit_config")
    assert len(edits) == 1
    call = edits[0]
    assert call.kwargs["target"] == "candidate"
    names = fake.names()
    assert names.index("commit") > names.index("edit_config")
    return leaves(payload(call))


def test_set_target_gain_edits_the_candidate_and_commits(ila):
    device, fake = ila
    device.set_target_gain("ba", 17.25)
    sent = edit_then_commit(fake)
    assert sent["name"] == "ba" and sent["target-gain"] in ("17.2", "17.3")


def test_set_target_gain_keeps_one_decimal(ila):
    device, fake = ila
    device.set_target_gain("ab", 15)
    assert edit_then_commit(fake)["target-gain"] == "15.0"


@pytest.mark.parametrize("state", ["true", "false"])
def test_set_amp_state(ila, state):
    device, fake = ila
    device.set_amp_state("ab", state)
    assert edit_then_commit(fake)["enabled"] == state


@pytest.mark.parametrize("amp,number", [("ab", 1), ("ba", 2)])
def test_set_evoa_atten_numbering_and_decimals(ila, amp, number):
    device, fake = ila
    device.set_evoa_atten(amp, 2)
    sent = edit_then_commit(fake)
    assert (sent["evoa-id"], sent["attn-value"]) == (str(number), "2.0")


def test_a_commit_error_propagates(ila):
    device, fake = ila
    fake.fail_on["commit"] = RuntimeError("commit refused")
    with pytest.raises(RuntimeError):
        device.set_target_gain("ab", 15.0)


def test_set_amp_state_accepts_a_bool(ila):
    device, fake = ila
    device.set_amp_state("ab", True)
    assert edit_then_commit(fake)["enabled"] == "true"


def test_a_nan_gain_is_refused_before_any_rpc(ila):
    device, fake = ila
    with pytest.raises(ValueError):
        device.set_target_gain("ab", float("nan"))
    assert fake.calls_to("edit_config") == []
