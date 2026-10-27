from ncclient import manager
import contextlib
import math
import xmltodict
from tcdona3.utils import *
from tcdona3._netconf import ReconnectingSession, SessionDropped, close_quietly, is_session_drop, reads_only
from tcdona3._yang_precision import device_fraction_digits
import traceback
import lxml.etree as et
from ncclient.xml_ import to_ele
from argparse import ArgumentParser
from ncclient.operations import RPCError

user = "fslyne"
password = "password"

#: The EVOA (``evoa-id``) ``get/set_evoa_atten`` address for each direction, as tcdona3 has always numbered it.
EVOA_LEGACY = {"ab": 1, "ba": 2}
#: The output EVOA after each direction, measured on the line (ols-control-app, 2026-10-17): a step of EVOA 2
#: moves the input power of the next amplifier of direction ab, a step of EVOA 1 that of direction ba.
EVOA_AFTER_DIRECTION = {"ab": 2, "ba": 1}
EVOA_NUMBERINGS = {"legacy": EVOA_LEGACY, "measured": EVOA_AFTER_DIRECTION}
#: The amplifier directions of an ILA.
AMP_DIRECTIONS = ("ab", "ba")
#: Namespace of the OpenConfig optical-amplifier identities, exactly as the device declares it (misspelt).
OC_OPT_AMP_NS = "http://openconfig.net/yang/optical-amplfier"
#: Gain ranges ``set_gain_range`` accepts (OpenConfig identities).
GAIN_RANGE_IDENTITIES = ("LOW_GAIN_RANGE", "MID_GAIN_RANGE", "HIGH_GAIN_RANGE", "FIXED_GAIN_RANGE")
#: Operating limits of the amplifier per gain range, measured in ols-control-app (documentation: the driver
#: does not enforce them). Gain, tilt and EVOA in dB, powers in dBm.
OP_RANGES = {
    "low": {"min_gain": 10.0, "max_gain": 20.0, "min_tilt": -3.0, "max_tilt": 1.5, "min_pout": -1.0,
            "max_pout": 20.5, "min_pin": -38.0, "min_voa_out": 0.0, "max_voa_out": 25.0},
    "high": {"min_gain": 17.0, "max_gain": 35.0, "min_tilt": -3.0, "max_tilt": 3.0, "min_pout": -1.0,
             "max_pout": 20.5, "min_pin": -38.0, "min_voa_out": 0.0, "max_voa_out": 25.0},
}
#: The YANG module whose set-point decimals ``setpoint_digits="device"`` reads, and its type modules.
SETPOINT_MODULE = "openconfig-optical-amplifier"
SETPOINT_TYPE_MODULES = ("openconfig-types",)
SETPOINT_LEAVES = ("target-gain", "target-gain-tilt", "target-output-power")
#: Decimals of a set-point when the device does not declare them.
DEFAULT_DEVICE_DIGITS = 2


def _finite(value, name):
    """``value`` as a finite float, or a ValueError naming ``name``."""
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("%s must be a finite number; got %r." % (name, value))
    return number


def _boolean_leaf(state):
    """A YANG boolean leaf value from a bool or from 'true'/'false' (any case)."""
    if isinstance(state, bool):
        return "true" if state else "false"
    text = str(state).strip().lower()
    if text not in ("true", "false"):
        raise ValueError("state must be True, False, 'true' or 'false'; got %r." % (state,))
    return text


def _direction(amp):
    if amp not in AMP_DIRECTIONS:
        raise ValueError("Invalid amp name, please enter ab or ba")
    return amp


class ILA:

    """Class to configure and monitor Juniper TCX-1000 In-Line Amplifiers (ILAs). Each ILA is bi-directions, i.e. it can amplify signals in both directions. There are seperate EDFAs for each direction. The param `amp` used in below API refers to direction of the EDFA for the particular ILA initialized. For example, 'ab' represents the forward direction, and 'ba' represents the reverse direction."""

    # https://codebeautify.org/xmlviewer
    def __init__(self, device, timeout=None, reconnect=False, evoa_numbering="legacy", setpoint_digits=1):
        """Initialize the ILA object. It also checks if the user is authorized to use the device. If the user is not authorized, it raises an Exception and does not connect to the device.

        :param device: The device name, ila_1 to ila_11 or dummy_ila_1.
        :type device: str

        :param timeout: Optional time allowed to open the session and for each RPC reply, in seconds.
        :type timeout: float

        :param reconnect: When True, a session that drops is opened again: reads are repeated, and an
            interrupted write transaction is applied again whole. Default False.
        :type reconnect: bool

        :param evoa_numbering: Which EVOA ``get/set_evoa_atten(amp, ...)`` address: ``"legacy"`` (default: ab
            -> EVOA 1, ba -> EVOA 2, as tcdona3 always did) or ``"measured"`` (the output EVOA after each
            direction, measured on the line: ab -> EVOA 2, ba -> EVOA 1).
        :type evoa_numbering: str

        :param setpoint_digits: Decimals of the gain and tilt set-points written: an int (default 1, as tcdona3
            always did) or ``"device"`` (the ``fraction-digits`` the device's YANG declares, read once).
        :type setpoint_digits: int or str

        :raises ValueError: If the device name or an option is invalid.
        """
        if evoa_numbering not in EVOA_NUMBERINGS:
            raise ValueError("evoa_numbering must be one of %s" % sorted(EVOA_NUMBERINGS))
        if setpoint_digits != "device" and (isinstance(setpoint_digits, bool) or
                                            not isinstance(setpoint_digits, int) or setpoint_digits < 0):
            raise ValueError("setpoint_digits must be a non-negative int or 'device'")
        if device == "ila_1":
            host = "10.10.10.34"
        elif device == "ila_2":
            host = "10.10.10.27"
        elif device == "ila_3":
            host = "10.10.10.26"
        elif device == "ila_4":
            host = "10.10.10.25"
        elif device == "ila_5":
            host = "10.10.10.24"
        elif device == "ila_6":
            host = "10.10.10.23"
        elif device == "ila_7":
            host = "10.10.10.22"
        elif device == "ila_8":
            host = "10.10.10.21"
        elif device == "ila_9":
            host = "10.10.10.20"
        elif device == "ila_10":
            host = "10.10.10.19"
        elif device == "ila_11":
            host = "10.10.10.18"
        elif device == "dummy_ila_1":
            host = "10.10.10.241"

        else:
            raise ValueError("Invalid device name, please enter ila_1, ila_2 or ila_3")

        if not check_patch_owners([(f"{device}_fwd", f"{device}_bck")]):
            raise Exception("You are not authorized to use this device")
        print("ILA Initialised...")
        connect_options = dict(host=host, port=830, username=user, password=password, hostkey_verify=False)
        if timeout is not None:
            connect_options["timeout"] = timeout
        session = manager.connect(**connect_options)
        if reconnect:
            session = ReconnectingSession(session, lambda: manager.connect(**connect_options), "ILA " + device,
                                          repeatable=reads_only)
        self.m = session
        self.device = device
        self.evoa_map = dict(EVOA_NUMBERINGS[evoa_numbering])
        self._setpoint_digits = setpoint_digits
        self._number_of_digits = None

    def close(self):
        """Close the NETCONF session; a session already gone is not an error."""
        close_quietly(self.m)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    @property
    def number_of_digits(self):
        """Decimals of the gain and tilt set-points written (see ``setpoint_digits``)."""
        if self._number_of_digits is None:
            if self._setpoint_digits == "device":
                declared = device_fraction_digits(self.m, SETPOINT_MODULE, SETPOINT_LEAVES, SETPOINT_TYPE_MODULES)
                self._number_of_digits = DEFAULT_DEVICE_DIGITS if declared is None else declared
            else:
                self._number_of_digits = self._setpoint_digits
        return self._number_of_digits

    def _commit_edit(self, rpc):
        """Apply ``rpc`` (an ``<nc:config>``) to the candidate datastore and commit it, as one transaction.

        The candidate is locked, cleared of any change left by another session, edited and committed; on any
        failure the edit is discarded before the error propagates, and the lock is always released. With
        ``reconnect=True``, a transaction interrupted by a dropped session is applied again once.
        """
        try:
            self._commit_edit_once(rpc)
        except Exception as error:
            if not isinstance(self.m, ReconnectingSession) or not (
                    isinstance(error, SessionDropped) or is_session_drop(error)):
                raise
            if not isinstance(error, SessionDropped):
                self.m.reopen()
            self._commit_edit_once(rpc)

    def _commit_edit_once(self, rpc):
        with self.m.locked("candidate"):
            self.m.discard_changes()
            try:
                self.m.edit_config(rpc, target="candidate")
                self.m.commit()
            except BaseException:
                try:
                    self.m.discard_changes()
                except Exception as error:
                    print("Discarding the failed edit also failed: %s" % error)
                raise

    @staticmethod
    def _amplifier_rpc(amp, leaf, value, attributes=""):
        """An ``<nc:config>`` setting one config ``leaf`` of amplifier ``amp``."""
        return """
            <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <open-optical-device xmlns="http://org/openroadm/device">
            <optical-amplifier>
            <amplifiers>
            <amplifier>
            <name>%s</name>
            <config>
            <%s%s>%s</%s>
            </config>
            </amplifier>
            </amplifiers>
            </optical-amplifier>
            </open-optical-device>
            </nc:config>
            """ % (_direction(amp), leaf, attributes, value, leaf)

    def _amplifier_leaf(self, amp, branch, leaf):
        """One leaf of amplifier ``amp``: of its running ``config`` (``get_config``) or of its ``state``
        (``get``), as the device returns it (str). A state counter the device wraps in ``<instant>`` is
        unwrapped."""
        filter = """
                <open-optical-device xmlns="http://org/openroadm/device">
                <optical-amplifier>
                <amplifiers>
                <amplifier>
                <name>%s</name>
                <%s>
                <%s></%s>
                </%s>
                </amplifier>
                </amplifiers>
                </optical-amplifier>
                </open-optical-device>
                """ % (_direction(amp), branch, leaf, leaf, branch)
        if branch == "config":
            reply = self.m.get_config(source="running", filter=("subtree", filter))
        else:
            reply = self.m.get(filter=("subtree", filter))
        amplifier = xmltodict.parse(reply.data_xml)["data"]["open-optical-device"]["optical-amplifier"][
            "amplifiers"]["amplifier"]
        value = amplifier[branch][leaf]
        if isinstance(value, dict) and "instant" in value:
            value = value["instant"]
        return value.get("#text") if isinstance(value, dict) else value

    def _setpoint(self, value, name):
        return "%.*f" % (self.number_of_digits, _finite(value, name))

    def _evoa_number(self, amp):
        if amp not in self.evoa_map:
            raise ValueError("Invalid amp name, please enter ab or ba")
        return self.evoa_map[amp]

    def get_pm_xml(self):
        """
        Run each RPC in `payload`, prettify its XML, and return
        a list of the resulting XML strings.
        :param payload: List[str] of XML RPC payloads
        :return: List[str] of pretty-printed XML replies
        """

        print("[ILA] get_pm_xml() called...")

        payload = '''
        <open-optical-device xmlns="http://org/openroadm/device">
            <optical-amplifier>
                <amplifiers>
                <amplifier>
                    <name/>
                    <config>
                    <name/>
                    <type/>
                    <target-gain/>
                    <target-gain-tilt/>
                    <gain-range/>
                    <amp-mode/>
                    <target-output-power/>
                    <enabled/>
                    <autolos/>
                    <apr-enabled/>
                    <apr-power/>
                    <plim-enabled/>
                    <plim-power/>
                    </config>
                    <state>
                    <name/>
                    <type/>
                    <target-gain/>
                    <target-gain-tilt/>
                    <gain-range/>
                    <amp-mode/>
                    <target-output-power/>
                    <enabled/>
                    <autolos/>
                    <apr-enabled/>
                    <apr-power/>
                    <plim-enabled/>
                    <plim-power/>
                    <operational-state/>
                    <pump-temperature/>
                    <pump1-temperature/>
                    <actual-gain/>
                    <actual-gain-tilt/>
                    <input-power-total/>
                    <input-power-c-band/>
                    <input-power-l-band/>
                    <msa-input-power-c-band/>
                    <output-power-total/>
                    <output-power-c-band/>
                    <output-power-l-band/>
                    <msa-output-power-c-band/>
                    <laser-bias-current/>
                    <laser-bias1-current/>
                    <back-reflection-ratio/>
                    <back-reflection/>
                    </state>
                </amplifier>
                </amplifiers>
            </optical-amplifier>
        </open-optical-device>
        '''
        xml_responses = []
        try:
            resp = self.m.get(filter=('subtree', to_ele(payload)))
            # always pretty-print the returned XML
            root = et.fromstring(resp.xml.encode())
            xml_responses.append(
                et.tostring(root, pretty_print=True).decode()
            )
        except Exception:
            traceback.print_exc()
            raise
        return xml_responses

    def get_target_gain(self, amp):
        """Get the target gain of the amplifier.

        :param: amp: This denotes the direction of the amplifier. 'ab' represents the forward direction, and 'ba' represents the reverse direction.
        :type amp: str

        :return: The target gain of the amplifier.
        :rtype: float
        """
        filter = """
                <open-optical-device xmlns="http://org/openroadm/device">
                <optical-amplifier>
                <amplifiers>
                <amplifier>
                <name>%s</name>
                <config>
                <target-gain></target-gain>
                </config>
                </amplifier>
                </amplifiers>
                </optical-amplifier>
                </open-optical-device>
                """ % (
            amp
        )
        config = self.m.get_config(source="running", filter=("subtree", filter))
        config_details = xmltodict.parse(config.data_xml)
        target_gain = config_details["data"]["open-optical-device"][
            "optical-amplifier"
        ]["amplifiers"]["amplifier"]["config"]["target-gain"]
        return target_gain

    def set_target_gain(self, amp, gain):
        """Set the target gain of the amplifier.

        :param amp: This denotes the direction of the amplifier. 'ab' represents the forward direction, and 'ba' represents the reverse direction.
        :type amp: str
        :param gain: The target gain to be set in dB, written with ``number_of_digits`` decimals (one by default).
        :type gain: float

        :raises ValueError: For an unknown direction or a gain that is not a finite number (before any RPC).
        """
        self._commit_edit(self._amplifier_rpc(amp, "target-gain", self._setpoint(gain, "gain")))

    def get_amp_state(self, amp):
        """Get the state of the amplifier.

        :param amp: This denotes the direction of the amplifier. 'ab' represents the forward direction, and 'ba' represents the reverse direction.
        :type amp: str

        :return: The state of the amplifier.
        :rtype: str
        """

        filter = """
                <open-optical-device xmlns="http://org/openroadm/device">
                <optical-amplifier>
                <amplifiers>
                <amplifier>
                <name>%s</name>
                <config>
                <enabled></enabled>
                </config>
                </amplifier>
                </amplifiers>
                </optical-amplifier>
                </open-optical-device>
                """ % (
            amp
        )
        config = self.m.get_config(source="running", filter=("subtree", filter))
        config_details = xmltodict.parse(config.data_xml)
        target_gain = config_details["data"]["open-optical-device"][
            "optical-amplifier"
        ]["amplifiers"]["amplifier"]["config"]["enabled"]
        return target_gain

    def set_amp_state(self, amp, state):
        """Set the state of the amplifier.

        :param amp: This denotes the direction of the amplifier. 'ab' represents the forward direction, and 'ba' represents the reverse direction.
        :type amp: str
        :param state: The state of the amplifier: 'true' or True for enabled, 'false' or False for disabled.
        :type state: str or bool

        :raises ValueError: For an unknown direction or state (before any RPC).
        """
        self._commit_edit(self._amplifier_rpc(amp, "enabled", _boolean_leaf(state)))

    # def get_amp_autolos(self, amp):
    #     filter = """
    #             <open-optical-device xmlns="http://org/openroadm/device">
    #             <optical-amplifier>
    #             <amplifiers>
    #             <amplifier>
    #             <name>%s</name>
    #             <config>
    #             <autolos></autolos>
    #             </config>
    #             </amplifier>
    #             </amplifiers>
    #             </optical-amplifier>
    #             </open-optical-device>
    #             """ % (
    #         amp
    #     )
    #     config = self.m.get_config(source="running", filter=("subtree", filter))
    #     config_details = xmltodict.parse(config.data_xml)
    #     target_gain = config_details["data"]["open-optical-device"][
    #         "optical-amplifier"
    #     ]["amplifiers"]["amplifier"]["config"]["autolos"]
    #     return target_gain

    # def set_amp_autolos(self, amp, state):
    #     rpc = """
    #         <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
    #         <open-optical-device xmlns="http://org/openroadm/device">
    #         <optical-amplifier>
    #         <amplifiers>
    #         <amplifier>
    #         <name>%s</name>
    #         <config>
    #         <autolos>%s</autolos>
    #         </config>
    #         </amplifier>
    #         </amplifiers>
    #         </optical-amplifier>
    #         </open-optical-device>
    #         </nc:config>
    #         """ % (
    #         amp,
    #         state,
    #     )
    #     reply = self.m.edit_config(rpc, target="candidate")
    #     # print(reply)
    #     reply = self.m.commit()
    #     # print(reply)

    def get_evoa_atten(self, amp):
        """Get the attenuation value of the EDFA VOA.

        :param amp: The direction ('ab' or 'ba'); the EVOA it addresses follows ``evoa_numbering``.
        :type amp: str
        :return: The attenuation value of the EDFA VOA in dB.
        :rtype: str"""
        return self._read_evoa(self._evoa_number(amp))

    def _read_evoa(self, num):
        filter = """
            <open-optical-device xmlns="http://org/openroadm/device">
            <evoas>
            <evoa-id>%d</evoa-id>
            <evoa>
            <attn-value></attn-value>
            </evoa>
            </evoas>
            </open-optical-device>
            """ % (
            num
        )
        config = self.m.get_config(source="running", filter=("subtree", filter))
        config_details = xmltodict.parse(config.data_xml)
        target_gain = config_details["data"]["open-optical-device"]["evoas"]["evoa"][
            "attn-value"
        ]
        return target_gain

    def set_evoa_atten(self, amp, atten):
        """Set the attenuation value of the EDFA VOA.

        :param amp: The direction ('ab' or 'ba'); the EVOA it addresses follows ``evoa_numbering``.
        :type amp: str
        :param atten: The attenuation value to be set in dB (one decimal).
        :type atten: float

        :raises ValueError: For an unknown direction or an attenuation that is not a finite number."""
        self._write_evoa(self._evoa_number(amp), atten)

    def _write_evoa(self, num, atten):
        rpc = """
            <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <open-optical-device xmlns="http://org/openroadm/device">
            <evoas>
            <evoa-id>%d</evoa-id>
            <evoa>
            <attn-value>%.1f</attn-value>
            </evoa>
            </evoas>
            </open-optical-device>
            </nc:config>
            """ % (
            num,
            _finite(atten, "atten"),
        )
        self._commit_edit(rpc)

    def get_output_evoa_atten(self, amp):
        """The attenuation [dB] of the output EVOA after direction ``amp``, measured numbering (ab -> EVOA 2,
        ba -> EVOA 1), whatever ``evoa_numbering`` is."""
        return self._read_evoa(EVOA_AFTER_DIRECTION[_direction(amp)])

    def set_output_evoa_atten(self, amp, atten):
        """Set the output EVOA after direction ``amp`` [dB], measured numbering (ab -> EVOA 2, ba -> EVOA 1),
        whatever ``evoa_numbering`` is."""
        self._write_evoa(EVOA_AFTER_DIRECTION[_direction(amp)], atten)

    def get_target_gain_tilt(self, amp):
        """The target gain tilt of the amplifier [dB], the raw leaf text."""
        return self._amplifier_leaf(amp, "config", "target-gain-tilt")

    def set_target_gain_tilt(self, amp, tilt):
        """Set the target gain tilt of the amplifier [dB], written with ``number_of_digits`` decimals."""
        self._commit_edit(self._amplifier_rpc(amp, "target-gain-tilt", self._setpoint(tilt, "tilt")))

    def get_gain_range(self, amp):
        """The configured gain range of the amplifier, an OpenConfig identity such as
        ``oc-opt-amp:LOW_GAIN_RANGE`` (raw leaf text)."""
        return self._amplifier_leaf(amp, "config", "gain-range")

    def set_gain_range(self, amp, gain_range):
        """Set the gain range of the amplifier: ``LOW_GAIN_RANGE``, ``MID_GAIN_RANGE``, ``HIGH_GAIN_RANGE`` or
        ``FIXED_GAIN_RANGE`` (with or without the ``oc-opt-amp:`` prefix, any case).

        The device runs at the new range's minimum gain after a switch and ignores a write of the value it
        already holds: disable the output first, write the gain target again after (via a different value),
        and read it back.
        """
        name = str(gain_range).split(":")[-1].strip().upper()
        if name not in GAIN_RANGE_IDENTITIES:
            raise ValueError("Unknown gain range %r; choose one of %s." % (gain_range, list(GAIN_RANGE_IDENTITIES)))
        self._commit_edit(self._amplifier_rpc(amp, "gain-range", "oc-opt-amp:" + name,
                                              ' xmlns:oc-opt-amp="%s"' % OC_OPT_AMP_NS))

    def get_actual_gain(self, amp):
        """The gain the amplifier achieves [dB] (``state/actual-gain``), a float."""
        return float(self._amplifier_leaf(amp, "state", "actual-gain"))

    def get_actual_gain_tilt(self, amp):
        """The gain tilt the amplifier achieves [dB] (``state/actual-gain-tilt``); -99.0 means not measured."""
        return float(self._amplifier_leaf(amp, "state", "actual-gain-tilt"))

    def get_input_power(self, amp):
        """The total input power of the amplifier [dBm] (``state/input-power-total``), a float."""
        return float(self._amplifier_leaf(amp, "state", "input-power-total"))

    def get_output_power(self, amp):
        """The total output power of the amplifier [dBm] (``state/output-power-total``), a float."""
        return float(self._amplifier_leaf(amp, "state", "output-power-total"))
