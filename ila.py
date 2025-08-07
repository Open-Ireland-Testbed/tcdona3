from ncclient import manager
import xmltodict
from utils import *
import traceback
import lxml.etree as et
from argparse import ArgumentParser
from ncclient.operations import RPCError

user = "fslyne"
password = "password"


class ILA:

    """Class to configure and monitor Juniper TCX-1000 In-Line Amplifiers (ILAs). Each ILA is bi-directions, i.e. it can amplify signals in both directions. There are seperate EDFAs for each direction. The param `amp` used in below API refers to direction of the EDFA for the particular ILA initialized. For example, 'ab' represents the forward direction, and 'ba' represents the reverse direction."""

    # https://codebeautify.org/xmlviewer
    def __init__(self, device):
        """Initialize the ILA object. It also checks if the user is authorized to use the device. If the user is not authorized, it raises an Exception and does not connect to the device.

        :param device: The device name, either ila_1, ila_2 or ila_3.
        :type device: str

        :raises ValueError: If the device name is invalid.
        """

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

        else:
            raise ValueError("Invalid device name, please enter ila_1, ila_2 or ila_3")

        if not check_patch_owners([(f"{device}_fwd", f"{device}_bck")]):
            raise Exception("You are not authorized to use this device")
        print("ILA Initialised...")
        self.m = manager.connect(
            host=host, port=830, username=user, password=password, hostkey_verify=False
        )

    def get_pm_xml(self):
        """
        Run each RPC in `payload`, prettify its XML, and return
        a list of the resulting XML strings.
        :param payload: List[str] of XML RPC payloads
        :return: List[str] of pretty-printed XML replies
        """

        print("[ILA] get_pm_xml() called...")

        payload = [
        '''
        <get xmlns="urn:ietf:params:xml:ns:netconf:base:1.0">
        <filter>
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
        </filter>
        </get>
        ''',
        ]
        xml_responses = []
        for rpc_str in payload:
            try:
                rpc_elem = et.fromstring(rpc_str)
                resp = self.m.dispatch(rpc_elem)
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

        :param gain: The target gain to be set in dB.
        :type gain: float
        """
        rpc = """
            <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <open-optical-device xmlns="http://org/openroadm/device">
            <optical-amplifier>
            <amplifiers>
            <amplifier>
            <name>%s</name>
            <config>
            <target-gain>%.1f</target-gain>
            </config>
            </amplifier>
            </amplifiers>
            </optical-amplifier>
            </open-optical-device>
            </nc:config>
            """ % (
            amp,
            gain,
        )
        reply = self.m.edit_config(rpc, target="candidate")
        # print(reply)
        reply = self.m.commit()
        # print(reply)

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

        :param state: The state of the amplifier. 'true' for enabled, 'false' for disabled.
        :type state: str
        """

        rpc = """
            <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <open-optical-device xmlns="http://org/openroadm/device">
            <optical-amplifier>
            <amplifiers>
            <amplifier>
            <name>%s</name>
            <config>
            <enabled>%s</enabled>
            </config>
            </amplifier>
            </amplifiers>
            </optical-amplifier>
            </open-optical-device>
            </nc:config>
            """ % (
            amp,
            state,
        )
        reply = self.m.edit_config(rpc, target="candidate")
        # print(reply)
        reply = self.m.commit()
        # print(reply)

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

        :param amp: The number of the EDFA VOA.
        :type amp: str

        :return: The attenuation value of the EDFA VOA in dB.
        :rtype: float"""

        if amp == "ab":
            num = 1
        elif amp == "ba":
            num = 2
        else:
            raise ValueError("Invalid amp name, please enter ab or ba")

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

        :param amp: The number of the EDFA VOA.
        :type amp: str

        :param atten: The attenuation value to be set in dB.
        :type atten: float"""

        if amp == "ab":
            num = 1
        elif amp == "ba":
            num = 2
        else:
            raise ValueError("Invalid amp name, please enter ab or ba")

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
            atten,
        )
        reply = self.m.edit_config(rpc, target="candidate")
        # print(reply)
        reply = self.m.commit()
        # print(reply)
