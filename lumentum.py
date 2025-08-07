import time

import xmltodict
from ncclient import manager
from ncclient.xml_ import to_ele
from utils import *
import pprint

pp = pprint.PrettyPrinter(depth=10)

LUMENTUM_USERNAME = "superuser"
LUMENTUM_PASSWORD = "Sup%9User"
LUMENTUM_DEFAULT_WSS_LOSS = "4.0"
LUMENTUM_ZERO_WSS_LOSS = "0.0"
LUMENTUM_MUX = 1
LUMENTUM_DEMUX = 2
LUMENTUM_MUX_OUTPUT_PORT = 4201
LUMENTUM_DEMUX_INPUT_PORT = 5101
LUMENTUM_INSERVICE = "in-service"
LUMENTUM_CHANNEL_QUANTITY = 95
LUMENTUM_CHANNEL_ALL = range(1, LUMENTUM_CHANNEL_QUANTITY + 1)
LUMENTUM_CHANNEL_SHORT = range(4, LUMENTUM_CHANNEL_QUANTITY + 1, 4)
LUMENTUM_POWER_READING_LOWER_LIMIT = -30
LUMENTUM_MAX_CHANNEL_POWER_READING_TRIES = 10
LUMENTUM_CHACEL_OA_LOSS = 13
LUMENTUM_WSS_CHANNEL_FREQ_CENTER_LIST = [
    191350.0 + idx * 50.0 for idx in range(LUMENTUM_CHANNEL_QUANTITY)
]

ip_map = {
    "roadm_1": "10.10.10.38",
    "roadm_2": "10.10.10.37",
    "roadm_3": "10.10.10.33",
    "roadm_4": "10.10.10.32",
    "roadm_5": "10.10.10.31",
    "roadm_6": "10.10.10.30",
    "roadm_7": "10.10.10.29",
    "roadm_8": "10.10.10.17",
    "roadm_9": "10.10.10.16",
    "roadm_10": "10.10.10.15",
    "roadm_11": "10.10.10.14",
    "roadm_dummy_1": "10.10.10.240",
    "roadm_dummy_2": "10.10.10.241",
}


class Lumentum(object):
    """
    A class used to interact with Lumentum ROADMs. The class provides methods to configure EDFA, WSS, and flex-grid connections. It also checks if the user is authorized to use the device. If the user is not authorized, it raises an Exception and does not connect to the device.

    :param roadm_name: The name of the roadm (e.g. roadm_1, roadm_2, etc.)
    :type roadm_name: str
    """

    def __init__(self, roadm_name, DEBUG=False):

        if roadm_name not in ip_map:
            raise ValueError("Invalid roadm_name")

        if not check_patch_owners([(roadm_name + "_p1", roadm_name + "_line")]):
            raise Exception("You are not authorized to use this device")

        self.m = manager.connect(
            host=ip_map[roadm_name],
            port=830,
            username=LUMENTUM_USERNAME,
            password=LUMENTUM_PASSWORD,
            hostkey_verify=False,
        )
        self.device_name = roadm_name
        self.DEBUG = DEBUG
        if self.DEBUG:
            print("ROADM " + roadm_name + " initialized...!")
        self.edfa_info = {"booster": {}, "preamp": {}}
        self.wss_connections = {"mux": {}, "demux": {}}
        self.port_info = {}
        self.mux_additional_attn = 0
        self.demux_additional_attn = 0

    # def __del__(self):
    #     self.m.close_session()

    def disable_als(self, duration=10):
        """
        Automatic Laser Shutdown (ALS) is a safety feature that automatically shuts down the laser if there is no light detected at the Preamp. This method can be used to disable Automatic Laser Shutdown for a specified duration. Do NOT set this to higher than 1000s.

        :param duration: The duration for which ALS should be disabled
        :type duration: int
        """
        module_id = 1

        command = """<disable-als xmlns="http://www.lumentum.com/lumentum-ote-edfa">
                  <dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                  <timeout-period>%s</timeout-period>
                  </disable-als>""" % (
            module_id,
            str(duration),
        )
        try:
            rpc_reply = self.m.dispatch(to_ele(command))
            if "<ok/>" in str(rpc_reply):
                if self.DEBUG:
                    print(
                        "EDFA "
                        + str(module_id)
                        + " ALS disabled for "
                        + str(duration)
                        + " seconds."
                    )

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)

    ### EDFA Operations ###
    def edfa_get_info(self):
        """
        Get the state information of the EDFA modules (booster and preamp), such as target gain/power setting, gain tilt, control mode (such as constant power or constant gain) in the ROADM.

        :return: A dictionary containing the state information of the EDFA modules (booster and preamp)
        :rtype: dict
        """
        command = """<filter><edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa"></edfas></filter>"""

        try:
            edfa_data = self.m.get(command)
            edfa_info_raw = xmltodict.parse(edfa_data.data_xml)["data"]["edfas"]["edfa"]

            # Append booster EDFA info
            self.edfa_info["booster"]["control_mode"] = str(
                edfa_info_raw[0]["config"]["lotee:control-mode"]
            )
            self.edfa_info["booster"]["maintenance-state"] = str(
                edfa_info_raw[0]["config"]["lotee:maintenance-state"]
            )
            self.edfa_info["booster"]["target_power"] = float(
                edfa_info_raw[0]["config"]["lotee:target-power"]
            )
            self.edfa_info["booster"]["target_gain"] = float(
                edfa_info_raw[0]["config"]["lotee:target-gain"]
            )
            self.edfa_info["booster"]["target_gain_tilt"] = float(
                edfa_info_raw[0]["config"]["lotee:target-gain-tilt"]
            )
            self.edfa_info["booster"]["input_power"] = float(
                edfa_info_raw[0]["state"]["input-power"]
            )
            self.edfa_info["booster"]["output_power"] = float(
                edfa_info_raw[0]["state"]["output-power"]
            )
            self.edfa_info["booster"]["voa_input_power"] = float(
                edfa_info_raw[0]["state"]["voas"]["voa"]["voa-input-power"]
            )
            self.edfa_info["booster"]["voa_output_power"] = float(
                edfa_info_raw[0]["state"]["voas"]["voa"]["voa-output-power"]
            )
            self.edfa_info["booster"]["voa_attenuation"] = float(
                edfa_info_raw[0]["state"]["voas"]["voa"]["voa-attentuation"]
            )

            # Append preamp EDFA info
            self.edfa_info["preamp"]["control_mode"] = str(
                edfa_info_raw[1]["config"]["lotee:control-mode"]
            )
            self.edfa_info["preamp"]["maintenance-state"] = str(
                edfa_info_raw[1]["config"]["lotee:maintenance-state"]
            )
            self.edfa_info["preamp"]["target_power"] = float(
                edfa_info_raw[1]["config"]["lotee:target-power"]
            )
            self.edfa_info["preamp"]["target_gain"] = float(
                edfa_info_raw[1]["config"]["lotee:target-gain"]
            )
            self.edfa_info["preamp"]["target_gain_tilt"] = float(
                edfa_info_raw[1]["config"]["lotee:target-gain-tilt"]
            )
            self.edfa_info["preamp"]["input_power"] = float(
                edfa_info_raw[1]["state"]["input-power"]
            )
            self.edfa_info["preamp"]["output_power"] = float(
                edfa_info_raw[1]["state"]["output-power"]
            )
            self.edfa_info["preamp"]["voa_input_power"] = float(
                edfa_info_raw[1]["state"]["voas"]["voa"]["voa-input-power"]
            )
            self.edfa_info["preamp"]["voa_output_power"] = float(
                edfa_info_raw[1]["state"]["voas"]["voa"]["voa-output-power"]
            )
            self.edfa_info["preamp"]["voa_attenuation"] = float(
                edfa_info_raw[1]["state"]["voas"]["voa"]["voa-attentuation"]
            )

            return self.edfa_info

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            exit(0)

    def __edfa_los_mode(self, edfa_module, los_shutdown):

        if los_shutdown not in ["true", "false"]:
            raise ValueError("los_shutdown must be either 'true' or 'false'")

        else:
            if edfa_module != "booster" and edfa_module != "preamp":
                raise Exception(
                    "Invalid edfa_module: Please choose 'booster' or 'preamp'."
                )
            elif edfa_module == "booster":
                target_module_id = 1
            elif edfa_module == "preamp":
                target_module_id = 2
            rpc_reply = 0

            target_edfa_info = self.edfa_get_info()[edfa_module]
            print(target_edfa_info)

            command_out_of_service = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config><maintenance-state>out-of-service</maintenance-state>
                </config></edfa></edfas></xc:config>""" % (
                target_module_id
            )

            try:
                command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <los-shutdown>%s</los-shutdown>
                </config></edfa></edfas></xc:config>""" % (
                    str(target_module_id),
                    str(los_shutdown),
                )

                rpc_reply = self.m.edit_config(
                    target="running", config=command_out_of_service
                )
                time.sleep(0.5)
                rpc_reply = self.m.edit_config(target="running", config=command)

                if "<ok/>" in str(rpc_reply):
                    if self.DEBUG:
                        print(
                            "EDFA "
                            + edfa_module
                            + " LOS shutdown set to "
                            + str(los_shutdown)
                        )
                    pass

            except Exception as e:
                print(e)
                raise SystemError("Encountered the above RPC error!")

    def edfa_config(
        self,
        edfa_module,
        los_shutdown="true",
        maintenance_state="in-service",
        control_mode="constant-power",
        gain_switch_mode="low-gain",
        target_gain=0.0,
        target_power=-10.0,
        target_gain_tilt=0.0,
        optical_loo_threshold=-50.0,
    ):
        """Configure the EDFA module in the ROADM. The method allows the user to set the EDFA module in-service or out-of-service, set the control mode (constant power or constant gain), set the gain switch mode (low-gain or high-gain), set the target gain, target power, target gain tilt, and optical LOO threshold.

        IMPORTANT: This method should be used with caution as it can affect the optical power levels in the network. Do NOT use this method in quick succesion. Ideally, this method should only be used once before setting up the topology.

        :param edfa_module: The EDFA module to be configured (either 'booster' or 'preamp')
        :type edfa_module: str

        :param control_mode: The control mode to be set (either 'constant-power' or 'constant-gain')
        :type control_mode: str

        :param gain_switch_mode: The gain switch mode to be set (either 'low-gain' or 'high-gain'). This setting only applies if setting the EDFA module in 'constant-power' mode.
        :type gain_switch_mode: str

        :param target_gain: The target gain to be set. This setting only applies if setting the EDFA module in 'constant-gain' mode. For boosters, the target gain can be set in the range of [4.0, 27.0] dB. For preamps, the target gain can be set in the range of [10.0, 30.5] dB.
        :type target_gain: float

        :param target_power: The target power to be set. This setting only applies if setting the EDFA module in 'constant-power' mode. For boosters, the target power can be set in the range of [3.0, 23.0] dBm. For preamps, the target power can be set in the range of [-3.0, +22.0] dBm.
        :type target_power: float

        :param target_gain_tilt: Sets the gain tilt applied over the spectrum. This is a a measure in dB of the gain slope from low to high frequency. For boosters, this can be set in the range [-3,+1] dB. The tilt cannot be set for preamps.
        :type target_gain_tilt: float
        """
        # Assert
        if edfa_module != "booster" and edfa_module != "preamp":
            raise Exception("Invalid edfa_module: Please choose 'booster' or 'preamp'.")
            exit(0)
        elif edfa_module == "booster":
            target_module_id = 1
        elif edfa_module == "preamp":
            target_module_id = 2
        rpc_reply = 0

        target_edfa_info = self.edfa_get_info()[edfa_module]
        print(target_edfa_info)

        command_out_of_service = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
            <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
            <config><maintenance-state>out-of-service</maintenance-state>
            </config></edfa></edfas></xc:config>""" % (
            target_module_id
        )

        try:
            if maintenance_state == "out-of-service":
                rpc_reply = self.m.edit_config(
                    target="running", config=command_out_of_service
                )

            if control_mode == "constant-power":
                command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <control-mode>constant-power</control-mode>
                <gain-switch-mode>%s</gain-switch-mode>
                <target-power>%s</target-power>
                <target-gain-tilt>%s</target-gain-tilt>
                </config></edfa></edfas></xc:config>""" % (
                    str(target_module_id),
                    str(gain_switch_mode),
                    str(target_power),
                    str(target_gain_tilt),
                )

                rpc_reply = self.m.edit_config(
                    target="running", config=command_out_of_service
                )
                time.sleep(0.5)
                rpc_reply = self.m.edit_config(target="running", config=command)

            elif control_mode == "constant-gain":
                command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <control-mode>constant-gain</control-mode>
                <target-gain>%s</target-gain>
                <target-gain-tilt>%s</target-gain-tilt>
                </config></edfa></edfas></xc:config>""" % (
                    target_module_id,
                    target_gain,
                    target_gain_tilt,
                )

                rpc_reply = self.m.edit_config(
                    target="running", config=command_out_of_service
                )
                time.sleep(0.5)
                rpc_reply = self.m.edit_config(target="running", config=command)

            if los_shutdown == "false":
                command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <los-shutdown>%s</los-shutdown>
                </config></edfa></edfas></xc:config>""" % (
                    str(target_module_id),
                    str(los_shutdown),
                )

                rpc_reply = self.m.edit_config(
                    target="running", config=command_out_of_service
                )
                time.sleep(0.5)
                rpc_reply = self.m.edit_config(target="running", config=command)

            if optical_loo_threshold != -50:

                command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <optical-loo-threshold>%s</optical-loo-threshold>
                </config></edfa></edfas></xc:config>""" % (
                    str(target_module_id),
                    str(optical_loo_threshold),
                )

                rpc_reply = self.m.edit_config(
                    target="running", config=command_out_of_service
                )
                time.sleep(1.0)
                rpc_reply = self.m.edit_config(target="running", config=command)

            if "<ok/>" in str(rpc_reply):
                if self.DEBUG:
                    print(
                        "EDFA "
                        + edfa_module
                        + " ("
                        + maintenance_state
                        + ") configured in "
                        + control_mode
                        + " mode."
                    )
                pass

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            exit(1)

    def __set_mux_offline(self):
        target_module_id = 1
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
            <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
            <config><maintenance-state>out-of-service</maintenance-state>
            </config></edfa></edfas></xc:config>""" % (
            target_module_id
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def __set_mux_online(self):
        target_module_id = 1
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
            <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
            <config><maintenance-state>in-service</maintenance-state>
            </config></edfa></edfas></xc:config>""" % (
            target_module_id
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def __set_demux_offline(self):
        target_module_id = 2
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
            <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
            <config><maintenance-state>out-of-service</maintenance-state>
            </config></edfa></edfas></xc:config>""" % (
            target_module_id
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def __set_demux_online(self):
        target_module_id = 2
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
            <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
            <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
            <config><maintenance-state>in-service</maintenance-state>
            </config></edfa></edfas></xc:config>""" % (
            target_module_id
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_mux_constant_power(self, target_power, target_gain_tilt=0.0):
        self.__set_mux_offline()
        time.sleep(0.5)
        target_module_id = 1
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <target-power>%s</target-power>
                <target-gain-tilt>%s</target-gain-tilt>
                <control-mode>constant-power</control-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id),
            str(target_power),
            str(target_gain_tilt),
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_demux_constant_power(self, target_power, target_gain_tilt=0.0):
        self.__set_demux_offline()
        time.sleep(0.5)
        target_module_id = 2
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <target-power>%s</target-power>
                <target-gain-tilt>%s</target-gain-tilt>
                <control-mode>constant-power</control-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id),
            str(target_power),
            str(target_gain_tilt),
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_mux_constant_gain(self, target_gain, target_gain_tilt=0.0):
        self.__set_mux_offline()
        time.sleep(0.5)
        target_module_id = 1
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <target-gain>%s</target-gain>
                <target-gain-tilt>%s</target-gain-tilt>
                <control-mode>constant-gain</control-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id),
            str(target_gain),
            str(target_gain_tilt),
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_demux_constant_gain(self, target_gain, target_gain_tilt=0.0):
        self.__set_mux_offline()
        time.sleep(0.5)
        target_module_id = 2
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <target-gain>%s</target-gain>
                <target-gain-tilt>%s</target-gain-tilt>
                <control-mode>constant-gain</control-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id),
            str(target_gain),
            str(target_gain_tilt),
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_mux_low_gain_mode(self):
        self.__set_mux_offline()
        time.sleep(0.5)
        target_module_id = 1
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <control-mode>constant-power</control-mode>
                <gain-switch-mode>low-gain</gain-switch-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id)
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_mux_high_gain_mode(self):
        self.__set_mux_offline()
        time.sleep(0.5)
        target_module_id = 1
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <control-mode>constant-power</control-mode>
                <gain-switch-mode>high-gain</gain-switch-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id)
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_demux_low_gain_mode(self):
        self.__set_mux_offline()
        time.sleep(0.5)
        target_module_id = 2
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <control-mode>constant-power</control-mode>
                <gain-switch-mode>low-gain</gain-switch-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id)
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def set_demux_high_gain_mode(self):
        self.__set_mux_offline()
        time.sleep(0.5)
        target_module_id = 2
        command = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                <config>
                <maintenance-state>in-service</maintenance-state>
                <control-mode>constant-power</control-mode>
                <gain-switch-mode>high-gain</gain-switch-mode>
                </config></edfa></edfas></xc:config>""" % (
            str(target_module_id)
        )
        rpc_reply = self.m.edit_config(target="running", config=command)

    def get_mux_target_gain(self):
        """Get the target gain setting for the booster EDFA module in the ROADM. This only applies if the EDFA module is set in 'constant-gain' mode.

        :return: The target gain setting for the booster EDFA module in the ROADM
        :rtype: float
        """

        target_module_id = 1
        filter = (
            """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                  <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                  <config>
                  <target-gain></target-gain>
                  </config>
                  </edfa>
                  </edfas>"""
            % target_module_id
        )
        config = self.m.get_config(source="running", filter=("subtree", filter))
        config_details = xmltodict.parse(config.data_xml)
        target_gain = config_details["data"]["edfas"]["edfa"]["config"][
            "lotee:target-gain"
        ]
        return target_gain

    def get_demux_target_gain(self):
        """Get the target gain setting for the preamp EDFA module in the ROADM. This only applies if the EDFA module is set in 'constant-gain' mode.

        :return: The target gain setting for the preamp EDFA module in the ROADM
        :rtype: float
        """
        target_module_id = 2
        filter = (
            """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                  <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                  <config>
                  <target-gain></target-gain>
                  </config>
                  </edfa>
                  </edfas>"""
            % target_module_id
        )
        config = self.m.get_config(source="running", filter=("subtree", filter))
        config_details = xmltodict.parse(config.data_xml)
        target_gain = config_details["data"]["edfas"]["edfa"]["config"][
            "lotee:target-gain"
        ]
        return target_gain
        # edfa_info_raw = xmltodict.parse(edfa_data.data_xml)['data']['edfas']['edfa']

    def get_mux_target_power(self):
        """Get the target power setting for the booster EDFA module in the ROADM. This only applies if the EDFA module is set in 'constant-power' mode.

        :return: The target power setting for the booster EDFA module in the ROADM
        :rtype: float
        """
        target_module_id = 1
        filter = (
            """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                  <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                  <config>
                  <target-power></target-power>
                  </config>
                  </edfa>
                  </edfas>"""
            % target_module_id
        )
        config = self.m.get_config(source="running", filter=("subtree", filter))
        config_details = xmltodict.parse(config.data_xml)
        target_power = config_details["data"]["edfas"]["edfa"]["config"][
            "lotee:target-power"
        ]
        return target_power

    def get_demux_target_power(self):
        """Get the target power setting for the preamp EDFA module in the ROADM. This only applies if the EDFA module is set in 'constant-power' mode."""
        target_module_id = 2
        filter = (
            """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa">
                  <edfa><dn>ne=1;chassis=1;card=1;edfa=%s</dn>
                  <config>
                  <target-power></target-power>
                  </config>
                  </edfa>
                  </edfas>"""
            % target_module_id
        )
        config = self.m.get_config(source="running", filter=("subtree", filter))
        config_details = xmltodict.parse(config.data_xml)
        target_power = config_details["data"]["edfas"]["edfa"]["config"][
            "lotee:target-power"
        ]
        return target_power

    def get_mux_edfa_input_power(self):
        """Get the total input power for the booster EDFA module in the ROADM, recorded at the photodiode (PD) present at the Booster EDFA input. The measurement resolution is 0.01 dB.

        :return: The total input power in dBm.
        :rtype: float
        """
        target_module_id = 1
        filter = """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa"></edfas>"""
        edfa_data = self.m.get(filter=('subtree', to_ele(filter)))
        edfa_info_raw = xmltodict.parse(edfa_data.data_xml)["data"]["edfas"]["edfa"]
        input_power = float(edfa_info_raw[0]["state"]["input-power"])
        return input_power

    def get_mux_edfa_output_power(self):
        """Get the total output power for the booster EDFA module in the ROADM, recorded at the photodiode (PD) present at the Booster EDFA output. The measurement resolution is 0.01 dB.

        :return: The total output power in dBm.
        :rtype: float
        """
        target_module_id = 1
        filter = """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa"></edfas>"""
        edfa_data = self.m.get(filter=('subtree', to_ele(filter)))
        edfa_info_raw = xmltodict.parse(edfa_data.data_xml)["data"]["edfas"]["edfa"]
        output_power = float(edfa_info_raw[0]["state"]["output-power"])
        return output_power

    def get_demux_edfa_input_power(self):
        """Get the total input power for the preamp EDFA module in the ROADM, recorded at the photodiode (PD) present at the Preamp EDFA input. The measurement resolution is 0.01 dB.

        :return: The total input power in dBm.
        :rtype: float
        """
        target_module_id = 2
        filter = """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa"></edfas>"""
        edfa_data = self.m.get(filter=('subtree', to_ele(filter)))
        edfa_info_raw = xmltodict.parse(edfa_data.data_xml)["data"]["edfas"]["edfa"]
        input_power = float(edfa_info_raw[1]["state"]["input-power"])
        return input_power

    def get_demux_edfa_output_power(self):
        """Get the total output power for the preamp EDFA module in the ROADM, recorded at the photodiode (PD) present at the Preamp EDFA output. The measurement resolution is 0.01 dB.

        :return: The total output power in dBm.
        :rtype: float
        """
        target_module_id = 2
        filter = """<edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa"></edfas>"""
        edfa_data = self.m.get(filter=('subtree', to_ele(filter)))
        edfa_info_raw = xmltodict.parse(edfa_data.data_xml)["data"]["edfas"]["edfa"]
        output_power = float(edfa_info_raw[1]["state"]["output-power"])
        return output_power

    def debug_edfa(self, DEBUG=False):
        """This method dumps all the EDFA information for the booster and preamp EDFA modules in the ROADM in form of a dictionary, which is parsed from XML data. The method is useful for debugging purposes.

        :param DEBUG: Set to True to enable debugging output
        :type DEBUG: bool

        :return: A dictionary containing the EDFA information for the booster and preamp EDFA modules in the ROADM
        :rtype: dict
        """
        target_module_id = 2
        filter = """<filter><edfas xmlns="http://www.lumentum.com/lumentum-ote-edfa" 
                  xmlns:lotee="http://www.lumentum.com/lumentum-ote-edfa"></edfas></filter>"""
        edfa_data = self.m.get(filter)
        edfa_info_raw = xmltodict.parse(edfa_data.data_xml)
        if DEBUG:
            print(edfa_info_raw)
        return edfa_info_raw

    def __reset_ports_info(self):
        self.port_info = {}

    def get_ports_info(self):
        """
        Get the optical line port information for the ROADM. The method returns a dictionary containing the following information for each port:
        - entity-description: The description of the port
        - operational-state: The operational state of the port
        - input-power: The input power in dBm
        - output-power: The output power in dBm
        - outvoa-actual-attenuation: The actual attenuation in dB

        :return: A dictionary containing the optical line port information for the ROADM
        :rtype:  dict
        """
        command = """<filter><physical-ports xmlns="http://www.lumentum.com/lumentum-ote-port" 
                  xmlns:lotep="http://www.lumentum.com/lumentum-ote-port"></physical-ports></filter>"""
        self.__reset_ports_info()  # clear previous cache

        try:
            rpc_reply = self.m.get(command)
            port_info_raw = xmltodict.parse(rpc_reply.data_xml)["data"][
                "physical-ports"
            ]["physical-port"]
            for port_idx in range(len(port_info_raw)):
                cur_port_info = port_info_raw[port_idx]
                cur_port_id = int(str(cur_port_info["dn"]).split("port=", 4)[1])
                # Optical line port
                if cur_port_id == 3001:
                    self.port_info[str(cur_port_id)] = {}
                    self.port_info[str(cur_port_id)]["entity-description"] = str(
                        cur_port_info["state"]["entity-description"]
                    )
                    self.port_info[str(cur_port_id)]["operational-state"] = str(
                        cur_port_info["state"]["operational-state"]
                    )
                    self.port_info[str(cur_port_id)]["input-power"] = float(
                        cur_port_info["state"]["lotepopt:input-power"]
                    )
                    self.port_info[str(cur_port_id)]["output-power"] = float(
                        cur_port_info["state"]["lotepopt:output-power"]
                    )
                    self.port_info[str(cur_port_id)][
                        "outvoa-actual-attenuation"
                    ] = float(
                        cur_port_info["state"]["lotepopt:outvoa-actual-attenuation"]
                    )
                # MUX ports have only input power info
                elif cur_port_id >= 4101 and cur_port_id <= 4120:
                    self.port_info[str(cur_port_id)] = {}
                    self.port_info[str(cur_port_id)]["entity-description"] = str(
                        cur_port_info["state"]["entity-description"]
                    )
                    self.port_info[str(cur_port_id)]["operational-state"] = str(
                        cur_port_info["state"]["operational-state"]
                    )
                    self.port_info[str(cur_port_id)]["input-power"] = float(
                        cur_port_info["state"]["lotepopt:input-power"]
                    )
                # DEMUX ports have only output power info
                elif cur_port_id >= 5201 and cur_port_id <= 5220:
                    self.port_info[str(cur_port_id)] = {}
                    self.port_info[str(cur_port_id)]["entity-description"] = str(
                        cur_port_info["state"]["entity-description"]
                    )
                    self.port_info[str(cur_port_id)]["operational-state"] = str(
                        cur_port_info["state"]["operational-state"]
                    )
                    self.port_info[str(cur_port_id)]["output-power"] = float(
                        cur_port_info["state"]["lotepopt:output-power"]
                    )

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            exit(1)

        return self.port_info

    ### WSS Operations ###
    class WSSConnection(object):
        def __init__(
            self,
            wss_id,
            connection_id,
            operation,
            blocked,
            input_port,
            output_port,
            start_freq,
            end_freq,
            attenuation,
            name,
        ):
            """
            Args:
                wss_id: 1 (MUX) or 2 (DEMUX)
                connection_id: int
                operation: 'in-service' or 'out-of-service'
                blocked: 'true' or 'false'
                input_port: 4101-4120 for WSS1 (MUX), 5101 for WSS2 (DEMUX)
                output_port: 4201 for WSS1 (MUX), 5201-5220 for WSS2 (DEMUX)
                start_freq: .2f (GHz)
                end_freq: .2f (GHz)
                attenuation: .2f (dB)
                name: 'string'
            """
            # Assert
            if wss_id != 1 and wss_id != 2:
                raise Exception(
                    "Invalid WSS wss_id is not 1 or 2. Please select WSS 1 (MUX) or WSS 2 (DEMUX)"
                )
                exit(0)
            elif wss_id == 1:
                if input_port < 4101 or input_port > 4120 or output_port != 4201:
                    raise Exception(
                        "Invalid WSS1 input_port range (4101-4120) or output_port range (4201)"
                    )
                    exit(0)
            elif wss_id == 2:
                if input_port != 5101 or output_port < 5201 or output_port > 5220:
                    raise Exception(
                        "Invalid WSS1 input_port range (5101) or output_port range (5201-5220)"
                    )
                    exit(0)

            if operation != "in-service" and operation != "out-of-service":
                raise Exception(
                    "Invalid WSS operation: not 'in-service' or 'out-of-service'"
                )
                exit(0)
            if blocked != "true" and blocked != "false":
                raise Exception("Invalid WSS blocked: not 'true' or 'false'")
                exit(0)

            self.wss_id = wss_id
            self.connection_id = connection_id
            self.operation = operation
            self.blocked = blocked
            self.input_port = input_port
            self.output_port = output_port
            self.start_freq = start_freq
            self.end_freq = end_freq
            self.attenuation = attenuation
            self.name = name
         
        def __repr__(self):
            return (
                f"{self.__class__.__name__}("
                f"wss_id={self.wss_id!r}, connection_id={self.connection_id!r}, "
                f"operation={self.operation!r}, blocked={self.blocked!r}, "
                f"input_port={self.input_port!r}, output_port={self.output_port!r}, "
                f"start_freq={self.start_freq!r}, end_freq={self.end_freq!r}, "
                f"attenuation={self.attenuation!r}, name={self.name!r})"
            )

        __str__ = __repr__

    # TODO: adapte this classmethod for better dict parsing
    class WSSConnectionStatus(WSSConnection):
        @classmethod
        def from_connection_details(cls, connection_details):
            return [
                cls(
                    connection_detail["dn"].split(";")[3].split("=")[1],
                    connection_detail["dn"].split(";")[4].split("=")[1],
                    connection_detail["config"]["maintenance-state"],
                    connection_detail["config"]["blocked"],
                    connection_detail["config"]["input-port-reference"].split("port=")[
                        1
                    ],
                    connection_detail["config"]["output-port-reference"].split("port=")[
                        1
                    ],
                    connection_detail["config"]["start-freq"],
                    connection_detail["config"]["end-freq"],
                    connection_detail["config"]["attenuation"],
                    connection_detail["config"]["custom-name"],
                    connection_detail["state"]["input-channel-attributes"]["power"],
                    connection_detail["state"]["output-channel-attributes"]["power"],
                    connection_detail["dn"].split(";")[0].split("=")[1],
                    connection_detail["dn"].split(";")[1].split("=")[1],
                    connection_detail["dn"].split(";")[2].split("=")[1]
                    # ) for connection_detail in connection_details['data']['connections']['connection'] if connection_detail
                )
                for connection_detail in connection_details["data"]["connections"]
                if connection_detail
            ]

        def __init__(
            self,
            wss_id,
            connection_id,
            operation,
            blocked,
            input_port,
            output_port,
            start_freq,
            end_freq,
            attenuation,
            name,
            input_power,
            output_power,
            ne,
            chassis,
            card,
        ):
            super(Lumentum.WSSConnectionStatus, self).__init__(
                wss_id,
                connection_id,
                operation,
                blocked,
                input_port,
                output_port,
                start_freq,
                end_freq,
                attenuation,
                name,
            )
            self.input_power = input_power
            self.output_power = output_power
            self.ne = ne
            self.chassis = chassis
            self.card = card

    def wss_get_connections(self):
        """
        Get the WSS connections for the MUX and DEMUX WSS modules in the ROADM. This is a helper method that parses the XML data and returns a list of WSSConnectionStatus objects. A WSS Connection is the basic building block for the WSS module in the ROADM. It represents the connections between signals coming on a ADD port, and the multiplexed signals going out on a Line Out port. For Demux, it represents the connections between signals coming on a Line In port, and the demultiplexed signals going out on one/multiple DROP ports.

        The method returns a dictionary containing the following information for each connection:
        - id: The connection ID
        - connection-id: The connection ID
        - start-freq: The start frequency in GHz
        - end-freq: The end frequency in GHz
        - attenuation: The attenuation in dB
        - blocked: 'true' or 'false'
        - input-port: The input port number
        - input-power: The input power in dBm
        - input-valid-data: 'true' or 'false'
        - output-port: The output port number
        - output-power: The output power in dBm
        - output-valid-data: 'true' or 'false'

        :return: A dictionary containing the WSS connections for the MUX and DEMUX WSS modules in the ROADM
        :rtype: dict
        """
        command = """
                  <connections xmlns="http://www.lumentum.com/lumentum-ote-connection"></connections>
                  """
        try:
            wss_data = self.m.get(filter=('subtree', command))
            # An ordered dict exported from xml
            wss_details = xmltodict.parse(wss_data.data_xml)
            if "module=1" in str(wss_details) or "module=2" in str(wss_details):
                # A list of connections
                connections = wss_details["data"]["connections"]["connection"]
                # Re-initialize wss_connections
                self.wss_connections = {"mux": {}, "demux": {}}
            else:
                connections = []
                print("No WSS connections exist.")

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            exit(0)

        # With only MUX connections
        if "module=1" in str(connections) and "module=2" not in str(connections):
            if "dn" in connections:
                # print("Only WSS1 (MUX) connections exist, # of connections: 1")
                self.wss_connections["mux"]["conn-1"] = {}
                self.wss_connections["mux"]["conn-1"]["id"] = 1
                self.wss_connections["mux"]["conn-1"]["connection-id"] = str(
                    connections["state"]["entity-description"]
                )
                self.wss_connections["mux"]["conn-1"]["start-freq"] = float(
                    connections["state"]["start-freq"]
                )
                self.wss_connections["mux"]["conn-1"]["end-freq"] = float(
                    connections["state"]["end-freq"]
                )
                self.wss_connections["mux"]["conn-1"]["attenuation"] = float(
                    connections["state"]["attenuation"]
                )
                self.wss_connections["mux"]["conn-1"]["blocked"] = str(
                    connections["state"]["blocked"]
                )
                self.wss_connections["mux"]["conn-1"]["input-port"] = int(
                    str(connections["config"]["input-port-reference"]).split(
                        "port=", 4
                    )[1]
                )
                self.wss_connections["mux"]["conn-1"]["input-power"] = float(
                    connections["state"]["input-channel-attributes"]["power"]
                )
                self.wss_connections["mux"]["conn-1"]["input-valid-data"] = str(
                    connections["state"]["input-channel-attributes"]["valid-data"]
                )
                self.wss_connections["mux"]["conn-1"]["output-port"] = int(
                    str(connections["config"]["output-port-reference"]).split(
                        "port=", 4
                    )[1]
                )
                self.wss_connections["mux"]["conn-1"]["output-power"] = float(
                    connections["state"]["output-channel-attributes"]["power"]
                )
                self.wss_connections["mux"]["conn-1"]["output-valid-data"] = str(
                    connections["state"]["output-channel-attributes"]["valid-data"]
                )
            else:
                num_connections = len(connections)
                # print("Only WSS1 (MUX) connections exist, # of connections: " + str(num_connections))
                for cur_conn_idx in range(num_connections):
                    cur_conn = connections[cur_conn_idx]
                    cur_conn_name = "conn-" + str(cur_conn_idx + 1)
                    self.wss_connections["mux"][cur_conn_name] = {}
                    self.wss_connections["mux"][cur_conn_name]["id"] = cur_conn_idx
                    self.wss_connections["mux"][cur_conn_name]["connection-id"] = str(
                        cur_conn["state"]["entity-description"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["start-freq"] = float(
                        cur_conn["state"]["start-freq"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["end-freq"] = float(
                        cur_conn["state"]["end-freq"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["attenuation"] = float(
                        cur_conn["state"]["attenuation"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["blocked"] = str(
                        cur_conn["state"]["blocked"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["input-port"] = int(
                        str(cur_conn["config"]["input-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["mux"][cur_conn_name]["input-power"] = float(
                        cur_conn["state"]["input-channel-attributes"]["power"]
                    )
                    self.wss_connections["mux"][cur_conn_name][
                        "input-valid-data"
                    ] = str(cur_conn["state"]["input-channel-attributes"]["valid-data"])
                    self.wss_connections["mux"][cur_conn_name]["output-port"] = int(
                        str(cur_conn["config"]["output-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["mux"][cur_conn_name]["output-power"] = float(
                        cur_conn["state"]["output-channel-attributes"]["power"]
                    )
                    self.wss_connections["mux"][cur_conn_name][
                        "output-valid-data"
                    ] = str(
                        cur_conn["state"]["output-channel-attributes"]["valid-data"]
                    )

        # With only DEMUX connections
        elif "module=1" not in str(connections) and "module=2" in str(connections):
            if "dn" in connections:
                # print("Only WSS2 (DEMUX) connections exist, # of connections: 1")
                self.wss_connections["demux"]["conn-1"] = {}
                self.wss_connections["demux"]["conn-1"]["id"] = 1
                self.wss_connections["demux"]["conn-1"]["connection-id"] = str(
                    connections["state"]["entity-description"]
                )
                self.wss_connections["demux"]["conn-1"]["start-freq"] = float(
                    connections["state"]["start-freq"]
                )
                self.wss_connections["demux"]["conn-1"]["end-freq"] = float(
                    connections["state"]["end-freq"]
                )
                self.wss_connections["demux"]["conn-1"]["attenuation"] = float(
                    connections["state"]["attenuation"]
                )
                self.wss_connections["demux"]["conn-1"]["blocked"] = str(
                    connections["state"]["blocked"]
                )
                self.wss_connections["demux"]["conn-1"]["input-port"] = int(
                    str(connections["config"]["input-port-reference"]).split(
                        "port=", 4
                    )[1]
                )
                self.wss_connections["demux"]["conn-1"]["input-power"] = float(
                    connections["state"]["input-channel-attributes"]["power"]
                )
                self.wss_connections["demux"]["conn-1"]["input-valid-data"] = str(
                    connections["state"]["input-channel-attributes"]["valid-data"]
                )
                self.wss_connections["demux"]["conn-1"]["output-port"] = int(
                    str(connections["config"]["output-port-reference"]).split(
                        "port=", 4
                    )[1]
                )
                self.wss_connections["demux"]["conn-1"]["output-power"] = float(
                    connections["state"]["output-channel-attributes"]["power"]
                )
                self.wss_connections["demux"]["conn-1"]["output-valid-data"] = str(
                    connections["state"]["output-channel-attributes"]["valid-data"]
                )
            else:
                num_connections = len(connections)
                # print("Only WSS2 (DEMUX) connections exist, # of connections: " + str(num_connections))
                for cur_conn_idx in range(num_connections):
                    cur_conn = connections[cur_conn_idx]
                    cur_conn_name = "conn-" + str(cur_conn_idx + 1)
                    self.wss_connections["demux"][cur_conn_name] = {}
                    self.wss_connections["demux"][cur_conn_name]["id"] = cur_conn_idx
                    self.wss_connections["demux"][cur_conn_name]["connection-id"] = str(
                        cur_conn["state"]["entity-description"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["start-freq"] = float(
                        cur_conn["state"]["start-freq"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["end-freq"] = float(
                        cur_conn["state"]["end-freq"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["attenuation"] = float(
                        cur_conn["state"]["attenuation"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["blocked"] = str(
                        cur_conn["state"]["blocked"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["input-port"] = int(
                        str(cur_conn["config"]["input-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["demux"][cur_conn_name]["input-power"] = float(
                        cur_conn["state"]["input-channel-attributes"]["power"]
                    )
                    self.wss_connections["demux"][cur_conn_name][
                        "input-valid-data"
                    ] = str(cur_conn["state"]["input-channel-attributes"]["valid-data"])
                    self.wss_connections["demux"][cur_conn_name]["output-port"] = int(
                        str(cur_conn["config"]["output-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["demux"][cur_conn_name][
                        "output-power"
                    ] = float(cur_conn["state"]["output-channel-attributes"]["power"])
                    self.wss_connections["demux"][cur_conn_name][
                        "output-valid-data"
                    ] = str(
                        cur_conn["state"]["output-channel-attributes"]["valid-data"]
                    )

        # With both MUX and DEMUX connections
        elif "module=1" in str(connections) and "module=2" in str(connections):
            num_connections = len(connections)
            # print("Both WSS1 (MUX) and WSS2 (DEMUX) connections exist, # of connections: " + str(num_connections))
            conn_mux_id = 0
            conn_demux_id = 0
            for cur_conn_idx in range(num_connections):
                cur_conn = connections[cur_conn_idx]
                # Append MUX connections
                if "module=1" in str(cur_conn):
                    conn_mux_id += 1
                    cur_conn_name = "conn-" + str(conn_mux_id)
                    self.wss_connections["mux"][cur_conn_name] = {}
                    self.wss_connections["mux"][cur_conn_name]["id"] = conn_mux_id
                    self.wss_connections["mux"][cur_conn_name]["connection-id"] = str(
                        cur_conn["state"]["entity-description"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["start-freq"] = float(
                        cur_conn["state"]["start-freq"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["end-freq"] = float(
                        cur_conn["state"]["end-freq"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["attenuation"] = float(
                        cur_conn["state"]["attenuation"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["blocked"] = str(
                        cur_conn["state"]["blocked"]
                    )
                    self.wss_connections["mux"][cur_conn_name]["input-port"] = int(
                        str(cur_conn["config"]["input-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["mux"][cur_conn_name]["input-power"] = float(
                        cur_conn["state"]["input-channel-attributes"]["power"]
                    )
                    self.wss_connections["mux"][cur_conn_name][
                        "input-valid-data"
                    ] = str(cur_conn["state"]["input-channel-attributes"]["valid-data"])
                    self.wss_connections["mux"][cur_conn_name]["output-port"] = int(
                        str(cur_conn["config"]["output-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["mux"][cur_conn_name]["output-power"] = float(
                        cur_conn["state"]["output-channel-attributes"]["power"]
                    )
                    self.wss_connections["mux"][cur_conn_name][
                        "output-valid-data"
                    ] = str(
                        cur_conn["state"]["output-channel-attributes"]["valid-data"]
                    )
                # Append DEMUX connections
                else:
                    conn_demux_id += 1
                    cur_conn_name = "conn-" + str(conn_demux_id)
                    self.wss_connections["demux"][cur_conn_name] = {}
                    self.wss_connections["demux"][cur_conn_name]["id"] = conn_demux_id
                    self.wss_connections["demux"][cur_conn_name]["connection-id"] = str(
                        cur_conn["state"]["entity-description"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["start-freq"] = float(
                        cur_conn["state"]["start-freq"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["end-freq"] = float(
                        cur_conn["state"]["end-freq"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["attenuation"] = float(
                        cur_conn["state"]["attenuation"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["blocked"] = str(
                        cur_conn["state"]["blocked"]
                    )
                    self.wss_connections["demux"][cur_conn_name]["input-port"] = int(
                        str(cur_conn["config"]["input-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["demux"][cur_conn_name]["input-power"] = float(
                        cur_conn["state"]["input-channel-attributes"]["power"]
                    )
                    self.wss_connections["demux"][cur_conn_name][
                        "input-valid-data"
                    ] = str(cur_conn["state"]["input-channel-attributes"]["valid-data"])
                    self.wss_connections["demux"][cur_conn_name]["output-port"] = int(
                        str(cur_conn["config"]["output-port-reference"]).split(
                            "port=", 4
                        )[1]
                    )
                    self.wss_connections["demux"][cur_conn_name][
                        "output-power"
                    ] = float(cur_conn["state"]["output-channel-attributes"]["power"])
                    self.wss_connections["demux"][cur_conn_name][
                        "output-valid-data"
                    ] = str(
                        cur_conn["state"]["output-channel-attributes"]["valid-data"]
                    )

        return self.wss_connections

    def get_mux_connection_input_power(self):
        """This method returns the input power for each connection defined in the MUX WSS module in the ROADM. The input power is measured by the OCM present at the MUX WSS input. The measurement resolution is 0.01 dB. The power measured is only for each defined channel and not the total power.

        :return: A list of channel power information in the form [(id, input_power)]
        :rtype: list
        """
        return self.wss_get_connections_input_power("mux")

    def get_demux_connection_input_power(self):
        """This method returns the input power for each connection defined in the DEMUX WSS module in the ROADM. The input power is measured by the OCM present at the DEMUX WSS input. The measurement resolution is 0.01 dB. The power measured is only for each defined channel and not the total power.

        Note that this power is measured after the Preamp EDFA module in the ROADM, and before the demultiplexing operation.

        :return: A list of channel power information in the form [(id, input_power)]
        :rtype: list
        """
        return self.wss_get_connections_input_power("demux")

    def get_mux_connection_output_power(self):
        return self.wss_get_connections_output_power("mux")

    def get_demux_connection_output_power(self):
        return self.wss_get_connections_output_power("demux")

    def wss_get_connections_input_power(self, wss_module):
        """
        Obtain WSS connections power information

        Args:
            wss_module: 'mux' or 'demux'

        Returns:
            A list of channel power information in the form [(id, input_power)]
        """
        self.wss_get_connections()
        if wss_module == "mux" or wss_module == "demux":
            return [
                (val["id"], val["input-power"])
                for key, val in self.wss_connections[wss_module].items()
                if "id" in val
            ]
        else:
            print("Invalid wss_module (not 'mux' or 'demux').")

    def wss_get_connections_output_power(self, wss_module):
        """
        Obtain WSS connections power information

        Args:
            wss_module: 'mux' or 'demux'

        Returns:
            A list of channel power information in the form [(id, input_power)]
        """
        self.wss_get_connections()
        if wss_module == "mux" or wss_module == "demux":
            return [
                (val["id"], val["output-power"])
                for key, val in self.wss_connections[wss_module].items()
                if "id" in val
            ]
        else:
            print("Invalid wss_module (not 'mux' or 'demux').")

    def wss_get_monitored_channels(self):
        command = """
            <monitored-channels xmlns="http://www.lumentum.com/lumentum-ote-monitored-channel"
                     xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"></monitored-channels>
        """
        try:
            wss_data = self.m.get(filter=('subtree', to_ele(command)))
            # An ordered dict exported from xml
            wss_details = xmltodict.parse(wss_data.data_xml)
            if "port=3101" in str(wss_details) or "port=6201" in str(wss_details):
                # A list of connections
                monitored_channels = wss_details["data"]["monitored-channels"][
                    "monitored-channel"
                ]
                # Re-initialize wss_connections
                self.monitored_channels = {"mux": {}, "demux": {}}
            else:
                connections = []
                print("No WSS connections exist.")

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            exit(0)

        mon_mux_id = 0
        mon_demux_id = 0
        for cur_monchan_idx in range(len(monitored_channels)):
            cur_monchan = monitored_channels[cur_monchan_idx]
            power = cur_monchan["state"]["power"]
            frequency = cur_monchan["state"]["measured-frequency"]
            if "port=6201" in str(cur_monchan):
                # print('mux')
                mon_mux_id += 1
                cur_mon_name = "mon-" + str(mon_mux_id)
                self.monitored_channels["mux"][cur_mon_name] = {}
                self.monitored_channels["mux"][cur_mon_name]["id"] = mon_mux_id
                self.monitored_channels["mux"][cur_mon_name]["power"] = float(
                    cur_monchan["state"]["power"]
                )
                self.monitored_channels["mux"][cur_mon_name]["frequency"] = float(
                    cur_monchan["state"]["measured-frequency"]
                )
            elif "port=3101" in str(cur_monchan):
                # print('demux')
                mon_demux_id += 1
                cur_mon_name = "mon-" + str(mon_demux_id)
                self.monitored_channels["demux"][cur_mon_name] = {}
                self.monitored_channels["demux"][cur_mon_name]["id"] = mon_demux_id
                self.monitored_channels["demux"][cur_mon_name]["power"] = float(
                    cur_monchan["state"]["power"]
                )
                self.monitored_channels["demux"][cur_mon_name]["frequency"] = float(
                    cur_monchan["state"]["measured-frequency"]
                )

        return self.monitored_channels

    def wss_get_monitored_power(self, wss_module):
        """
        Obtain OCM power information

        Args:
            wss_module: 'mux' or 'demux'

        Returns:
            A list of channel power information in the form [(id, input_power)]
        """
        self.wss_get_monitored_channels()

        if wss_module == "mux" or wss_module == "demux":
            return [
                (val["id"], val["power"])
                for key, val in self.monitored_channels[wss_module].items()
                if "id" in val
            ]
        else:
            print("Invalid wss_module (not 'mux' or 'demux').")

    def get_mux_monitored_power(self):
        return self.wss_get_monitored_power("mux")

    def get_demux_monitored_power(self):
        return self.wss_get_monitored_power("demux")

    def wss_print_connections(self):
        """
        Print WSS connections
        """
        pp.pprint(self.wss_get_connections())

    def wss_add_connection(
        self,
        wss_id,
        connection_id,
        start_freq,
        end_freq,
        attenuation,
        blocked,
        maintenance_state,
        input_port,
        output_port,
        custom_name,
    ):
        """
        Add a single WSS connection
        """
        # Assert
        if wss_id == 1 and output_port != 4201:
            raise Exception("Error: WSS 1 (MUX) output port has to be 4201.")
            exit(0)
        elif wss_id == 2 and input_port != 5101:
            raise Exception("Error: WSS 2 (DEMUX) input port has to be 5101.")
            exit(0)
        elif wss_id != 1 and wss_id != 2:
            raise Exception("Error: Invalid WSS wss_id (not 1 or 2).")
            exit(0)

        command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <start-freq>%s</start-freq>
                    <end-freq>%s</end-freq>
                    <attenuation>%s</attenuation>
                    <blocked>%s</blocked>
                    <maintenance-state>%s</maintenance-state>
                    <input-port-reference>ne=1;chassis=1;card=1;port=%s</input-port-reference>
                    <output-port-reference>ne=1;chassis=1;card=1;port=%s</output-port-reference>
                    <custom-name>%s</custom-name>
                  </add-connection>""" % (
            str(wss_id),
            str(connection_id),
            str(start_freq),
            str(end_freq),
            str(attenuation),
            blocked,
            maintenance_state,
            str(input_port),
            str(output_port),
            custom_name,
        )

        try:
            rpc_reply = self.m.dispatch(to_ele(command))
            if "<ok/>" in str(rpc_reply):
                print(
                    "WSS "
                    + str(wss_id)
                    + ": added connection "
                    + str(connection_id)
                    + ": "
                    + str(input_port)
                    + "->"
                    + str(output_port)
                    + " in ["
                    + str(start_freq)
                    + ", "
                    + str(end_freq)
                    + "] GHz"
                )

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            exit(0)

    def set_mux_block(self, connection_id):
        self.wss_block(1, 4201, connection_id, "true")

    def set_mux_unblock(self, connection_id):
        self.wss_block(1, 4201, connection_id, "false")

    def set_demux_block(self, connection_id):
        self.wss_block(2, 5201, connection_id, "true")

    def set_demux_unblock(self, connection_id):
        self.wss_block(2, 5201, connection_id, "false")

    def wss_block(self, wss_id, output_port, connection_id, blocked):
        if isinstance(connection_id, list):
            for c in connection_id:
                command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <blocked>%s</blocked>
                  </add-connection>
                  """ % (
                    str(wss_id),
                    str(c),
                    blocked,
                )
                try:
                    rpc_reply = self.m.dispatch(to_ele(command))
                    if "<ok/>" in str(rpc_reply):
                        print("WSS " + str(wss_id) + ": modified connection " + str(c))
                except Exception as e:
                    print("Encountered the following RPC error!")
                    print(e)
                    exit(0)

        else:
            command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <blocked>%s</blocked>
                  </add-connection>
                  """ % (
                str(wss_id),
                str(connection_id),
                blocked,
            )
            try:
                rpc_reply = self.m.dispatch(to_ele(command))
                if "<ok/>" in str(rpc_reply):
                    print(
                        "WSS "
                        + str(wss_id)
                        + ": modified connection "
                        + str(connection_id)
                    )

            except Exception as e:
                print("Encountered the following RPC error!")
                print(e)
                exit(0)

    def set_mux_atten(self, connection_id, atten=0.0):
        self.wss_atten(1, 4101, connection_id, atten)

    def set_demux_atten(self, connection_id, atten=0.0):
        self.wss_atten(2, 5201, connection_id, atten)

    def wss_atten(self, wss_id, output_port, connection_id, atten=0.0):
        if isinstance(connection_id, list):
            command = '<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0"><connections xmlns="http://www.lumentum.com/lumentum-ote-connection" xmlns:lotet="http://www.lumentum.com/lumentum-ote-connection">'
            for ch, val in connection_id:
                subcommand = """<connection>
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <config>
                    <attenuation>%s</attenuation>
                    </config>
                    </connection>
                    """ % (
                    str(wss_id),
                    str(ch),
                    str(val),
                )
                command = command + subcommand
            command = command + "</connections></xc:config>"
            try:
                rpc_reply = self.m.edit_config(target="running", config=command)
                if "<ok/>" in str(rpc_reply):
                    if self.DEBUG:
                        print("WSS " + str(wss_id) + ": bulk modified connections")
            except Exception as e:
                print("Encountered the following RPC error!")
                print(e)
                raise ValueError

        else:
            command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <attenuation>%s</attenuation>
                  </add-connection>""" % (
                str(wss_id),
                str(connection_id),
                str(atten),
            )
            try:
                rpc_reply = self.m.dispatch(to_ele(command))
                if "<ok/>" in str(rpc_reply):
                    print(
                        "WSS "
                        + str(wss_id)
                        + ": modified connection "
                        + str(connection_id)
                    )

            except Exception as e:
                print("Encountered the following RPC error!")
                print(e)
                raise ValueError

    def set_mux_block_status(self, channel_list: list):
        self.wss_block_status_config(1, 4201, channel_list)

    def set_demux_block_status(self, channel_list: list):
        self.wss_block_status_config(2, 5201, channel_list)

    def wss_block_status_config(self, wss_id, output_port, connection_id: list):

        command = '<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0"><connections xmlns="http://www.lumentum.com/lumentum-ote-connection" xmlns:lotet="http://www.lumentum.com/lumentum-ote-connection">'
        for ch, is_blocked in connection_id:
            if isinstance(is_blocked, bool):
                is_blocked = str(is_blocked).lower()
            subcommand = """<connection>
                <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                <config>
                <blocked>%s</blocked>
                </config>
                </connection>
                """ % (
                str(wss_id),
                str(ch),
                str(is_blocked),
            )
            command = command + subcommand
        command = command + "</connections></xc:config>"
        try:
            rpc_reply = self.m.edit_config(target="running", config=command)
            if "<ok/>" in str(rpc_reply):
                if self.DEBUG:
                    print("WSS " + str(wss_id) + ": bulk modified connections")
        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            raise ValueError

    def set_mux_port(self, connection_id, port):
        port += 4100
        self.wss_inport(1, connection_id, port)

    def set_demux_port(self, connection_id, port):
        port += 5200
        self.wss_outport(2, connection_id, port)

    def wss_inport(self, wss_id, connection_id, port):
        if isinstance(connection_id, list):
            for c in connection_id:
                command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <input-port-reference>ne=1;chassis=1;card=1;port=%s</input-port-reference>
                  </add-connection>""" % (
                    str(wss_id),
                    str(c),
                    str(port),
                )
                try:
                    rpc_reply = self.m.dispatch(to_ele(command))
                    if "<ok/>" in str(rpc_reply):
                        print("WSS " + str(wss_id) + ": modified connection " + str(c))
                except Exception as e:
                    print("Encountered the following RPC error!")
                    print(e)
                    exit(0)

        else:
            command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <input-port-reference>ne=1;chassis=1;card=1;port=%s</input-port-reference>
                  </add-connection>""" % (
                str(wss_id),
                str(connection_id),
                str(port),
            )
            try:
                rpc_reply = self.m.dispatch(to_ele(command))
                if "<ok/>" in str(rpc_reply):
                    print(
                        "WSS "
                        + str(wss_id)
                        + ": modified connection "
                        + str(connection_id)
                    )

            except Exception as e:
                print("Encountered the following RPC error!")
                print(e)
                exit(0)

    def wss_outport(self, wss_id, connection_id, port):
        if isinstance(connection_id, list):
            for c in connection_id:
                command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <output-port-reference>ne=1;chassis=1;card=1;port=%s</output-port-reference>
                  </add-connection>""" % (
                    str(wss_id),
                    str(c),
                    str(port),
                )
                try:
                    rpc_reply = self.m.dispatch(to_ele(command))
                    if "<ok/>" in str(rpc_reply):
                        print("WSS " + str(wss_id) + ": modified connection " + str(c))
                except Exception as e:
                    print("Encountered the following RPC error!")
                    print(e)
                    exit(0)

        else:
            command = """
                  <add-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                    <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                    <output-reference>ne=1;chassis=1;card=1;port=%s</output-port-reference>
                  </add-connection>""" % (
                str(wss_id),
                str(connection_id),
                str(port),
            )
            try:
                rpc_reply = self.m.dispatch(to_ele(command))
                if "<ok/>" in str(rpc_reply):
                    print(
                        "WSS "
                        + str(wss_id)
                        + ": modified connection "
                        + str(connection_id)
                        + " "
                        + port
                    )

            except Exception as e:
                print("Encountered the following RPC error!")
                print(e)
                exit(0)

    def wss_add_connections(self, connections):
        """
        Add a group of WSS connections (e.g., a DWDM channel)

        Args:
            connections: an array of WSSConnection class objects
        """
        new_line = "\n"
        services = """<xc:config xmlns:xc="urn:ietf:params:xml:ns:netconf:base:1.0">
                   <connections xmlns="http://www.lumentum.com/lumentum-ote-connection" 
                   xmlns:lotet="http://www.lumentum.com/lumentum-ote-connection"> %s
                   </connections>
                   </xc:config>""" % new_line.join(
            [self.wss_get_connection_xml(connection) for connection in connections]
        )
        try:
            rpc_reply = self.m.edit_config(target="running", config=services)
            if "<ok/>" in str(rpc_reply):
                if self.DEBUG:
                    print("Successfully added a set of connections!")
                pass

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            raise ValueError

    def wss_delete_connection(self, wss_id, connection_id):
        """
        Delete WSS connection(s)

        Args:
            wss_id: 1 (MUX) or 2 (DEMUX)
            connection_id: 'all' or 'id'
        """
        try:
            if connection_id == "all":
                command = (
                    """<remove-all-connections
                          xmlns="http://www.lumentum.com/lumentum-ote-connection">
                          <dn>ne=1;chassis=1;card=1;module=%s</dn>
                          </remove-all-connections> """
                    % wss_id
                )
                rpc_reply = self.m.dispatch(to_ele(command))
            else:
                command = """<delete-connection xmlns="http://www.lumentum.com/lumentum-ote-connection">
                          <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
                          </delete-connection>""" % (
                    wss_id,
                    connection_id,
                )
                rpc_reply = self.m.dispatch(to_ele(command))
            if "<ok/>" in str(rpc_reply):
                if self.DEBUG:
                    print(
                        "WSS "
                        + str(wss_id)
                        + " connection "
                        + str(connection_id)
                        + " deleted."
                    )
                pass

        except Exception as e:
            print("Encountered the following RPC error!")
            print(e)
            raise ValueError

    def wss_get_connection_xml(self, wss_connection):
        """
        Generate connections XML

        Args:
            wss_connection: a WSSConnection class object
        """

        return """<connection>
          <dn>ne=1;chassis=1;card=1;module=%s;connection=%s</dn>
          <config>
            <maintenance-state>%s</maintenance-state>
            <blocked>%s</blocked>
            <start-freq>%s</start-freq>
            <end-freq>%s</end-freq>
            <attenuation>%s</attenuation>
            <input-port-reference>ne=1;chassis=1;card=1;port=%s</input-port-reference>
            <output-port-reference>ne=1;chassis=1;card=1;port=%s</output-port-reference>
            <custom-name>%s</custom-name>
          </config></connection>""" % (
            wss_connection.wss_id,
            wss_connection.connection_id,
            wss_connection.operation,
            wss_connection.blocked,
            wss_connection.start_freq,
            wss_connection.end_freq,
            wss_connection.attenuation,
            wss_connection.input_port,
            wss_connection.output_port,
            wss_connection.name,
        )

    def make_grid(
        self,
        device="both",
        open_channels=[],
        rmv=True,
        in_port=4101,
        out_port=5201,
        channel_spacing=50.0,
        channel_width=50.0,
    ):
        if device == "both":
            self.wss_delete_connection(1, "all")
            self.wss_delete_connection(2, "all")
            #           connections=self.wss_gen_connections_dwdm(1,4201,4120)
            #           self.wss_add_connections(connections)
            connections = self.wss_gen_connections_dwdm(
                1,
                in_port,
                4201,
                open_channels=open_channels,
                channel_spacing=channel_spacing,
                channel_width=channel_width,
            )
            self.wss_add_connections(connections)
            connections = self.wss_gen_connections_dwdm(
                2,
                5101,
                out_port,
                open_channels=open_channels,
                channel_spacing=channel_spacing,
                channel_width=channel_width,
            )
            self.wss_add_connections(connections)
        elif device == "mux":
            self.wss_delete_connection(1, "all")
            connections = self.wss_gen_connections_dwdm(
                1,
                in_port,
                4201,
                open_channels=open_channels,
                channel_spacing=channel_spacing,
                channel_width=channel_width,
            )
            self.wss_add_connections(connections)
        elif device == "demux":
            self.wss_delete_connection(2, "all")
            connections = self.wss_gen_connections_dwdm(
                2,
                5101,
                out_port,
                open_channels=open_channels,
                channel_spacing=channel_spacing,
                channel_width=channel_width,
            )
            self.wss_add_connections(connections)

    def wss_gen_connections_dwdm(
        self,
        wss_id,
        input_port,
        output_port,
        channel_spacing=50.0,
        channel_width=50.0,
        central_freq_input=191350.0,
        loss=LUMENTUM_DEFAULT_WSS_LOSS,
        open_channels=[],
        channel_additional_attenuations=None,
    ):

        """
        Generate an array of WSSConnection class objects in a DWDM setting
        between a given pair of input and output ports

        Args:
            wss_id: 1 (MUX) or 2 (DEMUX)
            channel_spacing: in GHz
            channel_width: in GHz
            loss: default loss in dB
            open_channels: open channel index in a list
            channel_additional_attenuations: channel attenuation specified in a list with same length as open_channels

        Returns:
            wss_connections_dwdm: a list of WSSConnection class objects
        """
        wss_connections_dwdm = []
        center_freq = central_freq_input
        for i in range(1, LUMENTUM_CHANNEL_QUANTITY + 1):
            cur_center_freq = center_freq + (i - 1) * channel_spacing
            total_loss = float(loss) + (
                channel_additional_attenuations[i]
                if channel_additional_attenuations is not None
                and i in channel_additional_attenuations
                else 0.0
            )
            cur_conn = Lumentum.WSSConnection(
                wss_id,
                str(i),
                LUMENTUM_INSERVICE,
                "false" if i in open_channels else "true",
                input_port,
                output_port,
                str(cur_center_freq - channel_width / 2.0),
                str(cur_center_freq + channel_width / 2.0),
                "{:.2f}".format(total_loss),
                "CH" + str(i),
            )
            wss_connections_dwdm.append(cur_conn)

        return wss_connections_dwdm

    def apply_mux_grid(self, channel_list):

        mux_conn_list = []

        for i, channel in enumerate(channel_list, start=1):

            port, start_freq, end_freq, attn = channel

            mux_input_port = 4100 + port
            conn = Lumentum.WSSConnection(
                LUMENTUM_MUX,
                str(i),
                "in-service",
                "false",
                mux_input_port,
                LUMENTUM_MUX_OUTPUT_PORT,
                str(start_freq),
                str(end_freq),
                "{:.2f}".format(attn),
                "CH" + str(i),
            )
            mux_conn_list.append(conn)

        self.wss_delete_connection(LUMENTUM_MUX, "all")
        self.wss_add_connections(mux_conn_list)
        print("Done")

    def apply_demux_grid(self, channel_list):

        demux_conn_list = []

        for i, channel in enumerate(channel_list, start=1):

            port, start_freq, end_freq, attn = channel

            demux_output_port = 5200 + port
            conn = Lumentum.WSSConnection(
                LUMENTUM_DEMUX,
                str(i),
                "in-service",
                "false",
                LUMENTUM_DEMUX_INPUT_PORT,
                demux_output_port,
                str(start_freq),
                str(end_freq),
                "{:.2f}".format(attn),
                "CH" + str(i),
            )
            demux_conn_list.append(conn)

        self.wss_delete_connection(LUMENTUM_DEMUX, "all")
        self.wss_add_connections(demux_conn_list)
        print("Done")

    def generate_wide_channel_mux(
        self,
        channel_list,
        input_port,
        wss_id=1,
        output_port=4201,
        loss=4.0,
        open_channels=[],
        channel_additional_attenuations=None,
        blocked=False,
    ):
        # Makes one wide channel mux from multiple channles coming at input port

        if blocked == False:
            blocked_ = "false"
        else:
            blocked_ = "true"

        if isinstance(channel_list, int):
            channel_list = [channel_list]
        if isinstance(channel_list, tuple):
            channel_list = list(channel_list)
        if not isinstance(channel_list, list):
            raise ValueError(
                "channel_list must be a tuple of integers, or an integer for single channels"
            )

        start_freq = float(get_freq_range(channel_list[0])[0])
        end_freq = float(get_freq_range(channel_list[-1])[2])

        total_loss = float(loss) + (
            channel_additional_attenuations
            if channel_additional_attenuations is not None
            else 0.0
        )

        connection_id = str(channel_list[0])
        print(
            wss_id,
            connection_id,
            "in-service",
            blocked_,
            input_port,
            output_port,
            str(start_freq),
            str(end_freq),
            "{:.2f}".format(total_loss),
            "CH" + connection_id,
        )
        cur_conn = Lumentum.WSSConnection(
            wss_id,
            connection_id,
            "in-service",
            blocked_,
            input_port,
            output_port,
            str(start_freq),
            str(end_freq),
            "{:.2f}".format(total_loss),
            "CH" + connection_id,
        )
        return cur_conn

    def operator_flex_grid_mux_connections(
        self,
        add_list,
        wss_id=LUMENTUM_MUX,
        output_port=LUMENTUM_MUX_OUTPUT_PORT,
        channel_spacing=50.0,
        channel_width=50.0,
        central_freq_input=191350.0,
        loss=4.0,
        channel_quantity=95,
        open_channels=[],
        channel_additional_attenuations=None,
        default_port=1,
    ):

        # add_list is dict of form {channels, port}
        wss_connections_dwdm = []

        channel_port_dict = {}

        for channel in range(1, channel_quantity + 1):
            for channel_list, port in add_list.items():
                if isinstance(channel_list, tuple):
                    if channel in channel_list:
                        channel_port_dict[channel_list] = port
                        break
                elif isinstance(channel_list, int):
                    if channel == channel_list:
                        channel_port_dict[channel_list] = port
                        break
            else:
                channel_port_dict[channel] = default_port

        for channel, port in channel_port_dict.items():

            input_port = 4100 + port

            if isinstance(channel, tuple):
                if channel[0] in open_channels:
                    blocked = False
                else:
                    blocked = True
            elif isinstance(channel, int):
                if channel in open_channels:
                    blocked = False
                else:
                    blocked = True
            cur_conn = self.generate_wide_channel_mux(
                channel,
                input_port,
                loss=loss,
                open_channels=open_channels,
                channel_additional_attenuations=channel_additional_attenuations,
                blocked=blocked,
            )
            wss_connections_dwdm.append(cur_conn)

        return wss_connections_dwdm

    def generate_wide_channel_demux(
        self,
        channel_list,
        output_port,
        wss_id=LUMENTUM_DEMUX,
        input_port=LUMENTUM_DEMUX_INPUT_PORT,
        loss=4.0,
        open_channels=[],
        channel_additional_attenuations=None,
        blocked=False,
    ):
        # Makes one wide channel mux from multiple channles coming at input port

        if blocked == False:
            blocked_ = "false"
        else:
            blocked_ = "true"

        if isinstance(channel_list, int):
            channel_list = [channel_list]
        if isinstance(channel_list, tuple):
            channel_list = list(channel_list)
        if not isinstance(channel_list, list):
            raise ValueError(
                "channel_list must be a tuple of integers, or an integer for single channels"
            )

        start_freq = float(get_freq_range(channel_list[0])[0])
        end_freq = float(get_freq_range(channel_list[-1])[2])

        total_loss = float(loss) + (
            channel_additional_attenuations
            if channel_additional_attenuations is not None
            else 0.0
        )

        connection_id = str(channel_list[0])
        cur_conn = Lumentum.WSSConnection(
            wss_id,
            connection_id,
            "in-service",
            blocked_,
            input_port,
            output_port,
            str(start_freq),
            str(end_freq),
            "{:.2f}".format(total_loss),
            "CH" + connection_id,
        )
        return cur_conn

    def operator_flex_grid_demux_connections(
        self,
        drop_list,
        wss_id=LUMENTUM_DEMUX,
        input_port=LUMENTUM_DEMUX_INPUT_PORT,
        channel_spacing=50.0,
        channel_width=50.0,
        central_freq_input=191350.0,
        loss=4.0,
        channel_quantity=95,
        open_channels=[],
        channel_additional_attenuations=None,
        default_port=1,
    ):

        channel_port_dict = {}

        for channel in range(1, channel_quantity + 1):
            for channel_list, port in drop_list.items():
                if isinstance(channel_list, tuple):
                    if channel in channel_list:
                        channel_port_dict[channel_list] = port
                        break
                elif isinstance(channel_list, int):
                    if channel == channel_list:
                        channel_port_dict[channel_list] = port
                        break
            else:
                channel_port_dict[channel] = default_port

        wss_connections_dwdm = []

        for channel, port in channel_port_dict.items():

            output_port = 5200 + port

            if isinstance(channel, tuple):
                if channel[0] in open_channels:
                    blocked = False
                else:
                    blocked = True
            elif isinstance(channel, int):
                if channel in open_channels:
                    blocked = False
                else:
                    blocked = True
            # TODO: Can add channel-specific attenuations here later
            cur_conn = self.generate_wide_channel_demux(
                channel,
                output_port,
                loss=loss,
                open_channels=open_channels,
                channel_additional_attenuations=channel_additional_attenuations,
                blocked=blocked,
            )
            wss_connections_dwdm.append(cur_conn)
        return wss_connections_dwdm
