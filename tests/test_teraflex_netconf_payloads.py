"""The OpenConfig payloads of the NETCONF ``TFlex`` driver: the key of a list entry is its direct child.

NETCONF finds a list entry by its key leaves (``index`` of a logical channel, ``name`` of a component), which
must be children of the entry itself (RFC 6241, RFC 7950 7.8.5); a key only inside ``config`` does not
identify the entry. ``TFlex`` is built without its constructor, which reads the device's configuration.
"""

from lxml import etree

from tcdona3.teraflex import TFlex

from fakes import FakeNetconfManager, Reply

OK = Reply(xml='<rpc-reply xmlns="urn:ietf:params:xml:ns:netconf:base:1.0" message-id="1"><ok/></rpc-reply>')


def _transceiver():
    device = TFlex.__new__(TFlex)
    device.conn = FakeNetconfManager(replies={"edit_config": OK, "get_config": Reply()})
    device.line_port = "1/1/n1"
    device._config = {"1/1/n1": {"index": 7, "logical_interface": "ot200"}}
    return device


def _local(element):
    return etree.QName(element).localname


def _children(element):
    return {_local(child): child for child in element if isinstance(child.tag, str)}


def _entries(xml_text, name):
    root = etree.fromstring(xml_text.strip().encode())
    return [element for element in root.iter() if isinstance(element.tag, str) and _local(element) == name]


def test_delete_logical_interface_names_the_channel_by_its_key():
    device = _transceiver()
    device.delete_logical_interface()
    channel, = _entries(device.conn.calls_to("edit_config")[0].kwargs["config"], "channel")
    assert channel.get("{urn:ietf:params:xml:ns:netconf:base:1.0}operation") == "delete"
    assert _children(channel)["index"].text == "7"
    assert "config" not in _children(channel)
    assert device._config["1/1/n1"]["logical_interface"] is None


def test_set_power_and_frequency_names_the_component_by_its_key():
    device = _transceiver()
    device.set_power_and_frequency(-1.0, 193100)
    component, = _entries(device.conn.calls_to("edit_config")[0].kwargs["config"], "component")
    assert _children(component)["name"].text == "optch 1/1/n1"


def test_get_power_and_frequency_selects_the_component_by_its_key():
    device = _transceiver()
    device.get_power_and_frequency()
    kind, subtree = device.conn.calls_to("get_config")[0].kwargs["filter"]
    component, = _entries(subtree, "component")
    assert kind == "subtree" and _children(component)["name"].text == "optch 1/1/n1"
