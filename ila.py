from ncclient import manager
import xmltodict
from utils import check_patch_owners

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

        else:
            raise ValueError("Invalid device name, please enter ila_1, ila_2 or ila_3")

        if not check_patch_owners([(f"{device}_fwd", f"{device}_bck")]):
            raise Exception("You are not authorized to use this device")
        self.m = manager.connect(
            host=host, port=830, username=user, password=password, hostkey_verify=False
        )

    def get_pm_xml(self):
        """Get the performance monitoring XML file from the device. The XML file dumps the current state, configuration, and performance metrics of the ILA. Additional data cleaning is required to extract the relevant information.

        :return: The XML file containing the performance monitoring data.
        :rtype: str
        """

        xml_file = self.m.get().data_xml
        return xml_file

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
