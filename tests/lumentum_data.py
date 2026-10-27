"""Replies of a Lumentum ROADM, shaped like the device's: values are made up, no device is named."""

NC = 'xmlns="urn:ietf:params:xml:ns:netconf:base:1.0"'
EDFA_NS = 'xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa"'
PORT_NS = ('xmlns="http://www.lumentum.com/lumentum-ote-port" '
           'xmlns:lotepopt="http://www.lumentum.com/lumentum-ote-port-optical"')
CONN_NS = 'xmlns="http://www.lumentum.com/lumentum-ote-connection"'
MON_NS = 'xmlns="http://www.lumentum.com/lumentum-ote-monitored-channel"'


def _edfa(module, mode, power, gain, tilt, pin, pout):
    return """<edfa><dn>ne=1;chassis=1;card=1;edfa=%d</dn>
      <config><lotee:maintenance-state>in-service</lotee:maintenance-state>
        <lotee:control-mode>%s</lotee:control-mode>
        <lotee:gain-switch-mode>low-gain</lotee:gain-switch-mode>
        <lotee:target-power>%s</lotee:target-power><lotee:target-gain>%s</lotee:target-gain>
        <lotee:target-gain-tilt>%s</lotee:target-gain-tilt></config>
      <state><input-power>%s</input-power><output-power>%s</output-power>
        <voas><voa><voa-input-power>%s</voa-input-power><voa-output-power>-1.5</voa-output-power>
        <voa-attentuation>0.5</voa-attentuation></voa></voas></state></edfa>""" % (
        module, mode, power, gain, tilt, pin, pout, pout)


EDFAS = "<data %s><edfas %s>%s%s</edfas></data>" % (
    NC, EDFA_NS,
    _edfa(1, "constant-power", "10.0", "15.0", "-1.0", "-5.0", "10.0"),
    _edfa(2, "constant-gain", "5.0", "20.0", "0.0", "-20.0", "0.0"))


def edfa_config_leaf(module, leaf, value):
    """A ``get_config`` reply holding one config leaf of one EDFA module."""
    return ("<data %s><edfas %s><edfa><dn>ne=1;chassis=1;card=1;edfa=%d</dn><config><lotee:%s>%s</lotee:%s>"
            "</config></edfa></edfas></data>" % (NC, EDFA_NS, module, leaf, value, leaf))


def edfa_state_leaf(module, leaf, value):
    return ("<data %s><edfas %s><edfa><dn>ne=1;chassis=1;card=1;edfa=%d</dn><state><%s>%s</%s></state>"
            "</edfa></edfas></data>" % (NC, EDFA_NS, module, leaf, value, leaf))


def _port(port, state):
    return "<physical-port><dn>ne=1;chassis=1;card=1;port=%d</dn><state>%s</state></physical-port>" % (port, state)


PORTS = "<data %s><physical-ports %s>%s%s%s%s</physical-ports></data>" % (
    NC, PORT_NS,
    _port(3001, "<entity-description>Optical Line</entity-description>"
                "<operational-state>in-service</operational-state>"
                "<lotepopt:input-power>-18.5</lotepopt:input-power>"
                "<lotepopt:output-power>9.5</lotepopt:output-power>"
                "<lotepopt:outvoa-actual-attenuation>1.0</lotepopt:outvoa-actual-attenuation>"),
    _port(4101, "<entity-description>Add 1</entity-description><operational-state>in-service</operational-state>"
                "<lotepopt:input-power>-3.0</lotepopt:input-power>"),
    _port(5201, "<entity-description>Drop 1</entity-description><operational-state>in-service</operational-state>"
                "<lotepopt:output-power>-7.0</lotepopt:output-power>"),
    _port(6201, "<entity-description>Monitor</entity-description><operational-state>in-service</operational-state>"))


def connection(module, number, start, end, attenuation="4.0", blocked="false", pin="-10.0", pout="-14.0"):
    in_port, out_port = (4101, 4201) if module == 1 else (5101, 5201)
    return """<connection><dn>ne=1;chassis=1;card=1;module=%d;connection=%d</dn>
      <config><maintenance-state>in-service</maintenance-state><blocked>%s</blocked>
        <start-freq>%s</start-freq><end-freq>%s</end-freq><attenuation>%s</attenuation>
        <input-port-reference>ne=1;chassis=1;card=1;port=%d</input-port-reference>
        <output-port-reference>ne=1;chassis=1;card=1;port=%d</output-port-reference>
        <custom-name>CH%d</custom-name></config>
      <state><entity-description>CH%d</entity-description><start-freq>%s</start-freq><end-freq>%s</end-freq>
        <attenuation>%s</attenuation><blocked>%s</blocked>
        <input-channel-attributes><power>%s</power><valid-data>true</valid-data></input-channel-attributes>
        <output-channel-attributes><power>%s</power><valid-data>true</valid-data></output-channel-attributes>
      </state></connection>""" % (module, number, blocked, start, end, attenuation, in_port, out_port, number,
                                   number, start, end, attenuation, blocked, pin, pout)


def connections(*items):
    return "<data %s><connections %s>%s</connections></data>" % (NC, CONN_NS, "".join(items))


def monitored(port, number, power, frequency):
    return """<monitored-channel><dn>ne=1;chassis=1;card=1;port=%d;monitored-channel=%d</dn>
      <state><start-freq>%s</start-freq><end-freq>%s</end-freq><power>%s</power>
      <measured-frequency>%s</measured-frequency><valid-data>true</valid-data>
      <entity-description>CH%d</entity-description></state></monitored-channel>""" % (
        port, number, float(frequency) - 25.0, float(frequency) + 25.0, power, frequency, number)


def monitored_channels(*items):
    return "<data %s><monitored-channels %s>%s</monitored-channels></data>" % (NC, MON_NS, "".join(items))
