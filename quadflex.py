from ncclient import manager
from ncclient.xml_ import *
import xmltodict
import logging
import time
from .utils import check_patch_owners


class QFlex:

    """Class to interact with the QuadFlex device using NETCONF protocol. The class provides methods to read the performance monitoring data, change the configuration, and return the current configuration of the Teraflex device. It also checks if the user is authorized to use the device. If the user is not authorized, it raises an Exception and does not connect to the device.

    :param tf_name: The particular transciever name such as 'qf_1' and 'qf_2'.
    :type tf_name: str

    :raise: Exception: If the user is not authorized to use the device
    :raise: Exception: If the Quadflex name is invalid
    """

    def __init__(self, qf_name):

        if qf_name == "qf_1":
            self.line_port = "1/1/n1"
        elif qf_name == "qf_2":
            self.line_port = "1/1/n2"
        else:
            raise Exception("Invalid quadflex name")

        if not check_patch_owners([(qf_name, qf_name)]):
            raise Exception("You are not authorized to use this device")

        self.conn = manager.connect(
            host="10.10.10.120",
            port=830,
            username="admin",
            password="CHGME.1a",
            timeout=60,
            hostkey_verify=False,
        )
        self.conn.raise_mode = 0  # on RPCError, do not throw any exceptions

    def get_params(self, DEBUG=False):
        """Method to get the performance monitoring data of the Quadflex device.
        This method gives the raw output, the cleaned output method is still in development.
        """
        perf_dict = {}

        request_pm_data = f"""
        <get-pm-data xmlns="http://www.advaoptical.com/aos/netconf/aos-core-pm"
                     xmlns:fac="http://www.advaoptical.com/aos/netconf/aos-core-facility"
                     xmlns:me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                     xmlns:otn="http://www.advaoptical.com/aos/netconf/aos-domain-otn">
          <target-entity>/me:managed-element[me:entity-name="1"]/fac:interface[fac:name="{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}"]/fac:logical-interface/otn:otsia/otn:otsi[id="1"]</target-entity>
          <pm-data>
            <pm-current-data/>
          </pm-data>
        </get-pm-data>
        """

        reply_pm_data = self.conn.dispatch(to_ele(request_pm_data))
        perf_details = xmltodict.parse(reply_pm_data.xml)

        return perf_details
        perf_categories = perf_details["nc:rpc-reply"]["pm-data"]["pm-current-data"]

        for perf_cat in perf_categories:
            name = perf_cat["name"]
            interval = perf_cat["bin-interval"].split("-")[2]
            montypemonval = perf_cat["montype-monval"]
            if isinstance(montypemonval, list):
                for mtmv in montypemonval:
                    mt = mtmv["mon-type"].split(":")[1]
                    mv = mtmv["mon-val"]
                    perf_dict["_".join([name, interval, mt])] = mv
                    # print(name,interval,mt,mv)
            else:
                mt = montypemonval["mon-type"].split(":")[1]
                mv = montypemonval["mon-val"]
                perf_dict["_".join([name, interval, mt])] = mv
                # print(name,interval,mt)

        request_fec_ber = f"""
        <get-pm-data xmlns="http://www.advaoptical.com/aos/netconf/aos-core-pm"
                     xmlns:me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                     xmlns:fac="http://www.advaoptical.com/aos/netconf/aos-core-facility"
                     xmlns:adom-oduckpa="http://www.advaoptical.com/aos/netconf/aos-domain-otn-oduckpa"
                     xmlns:otn="http://www.advaoptical.com/aos/netconf/aos-domain-otn">
          <target-entity>/me:managed-element[me:entity-name="1"]/fac:interface[fac:name="{self._config[line_port]['line_port'] + '/' + self._config[line_port]['logical_interface']}"]/fac:logical-interface/adom-oduckpa:otu-c2pa</target-entity>
          <pm-data>
            <pm-current-data/>
          </pm-data>
        </get-pm-data>
        """

        reply_fec_ber = self.conn.dispatch(to_ele(request_fec_ber))

        perf_details = xmltodict.parse(reply_fec_ber.xml)
        if "pm-data" in perf_details["nc:rpc-reply"].keys():
            perf_categories = perf_details["nc:rpc-reply"]["pm-data"]["pm-current-data"]
            for perf_cat in perf_categories:
                name = perf_cat["name"]
                interval = perf_cat["bin-interval"].split("-")[2]
                montypemonval = perf_cat["montype-monval"]
                if isinstance(montypemonval, list):
                    for mtmv in montypemonval:
                        mt = mtmv["mon-type"].split(":")[1]
                        mv = mtmv["mon-val"]
                        perf_dict[":".join([name, interval, mt])] = mv
                else:
                    mt = montypemonval["mon-type"].split(":")[1]
                    mv = montypemonval["mon-val"]
                    perf_dict[":".join([name, interval, mt])] = mv
        else:
            if DEBUG:
                print("No BER reading available!")
        return perf_dict

    def get_pre_fec_ber(self):

        """
        Method to get the pre-FEC BER of the Quadflex device. The pre-FEC BER is the BER before the Forward Error Correction is applied. The method will return 0.0 if the pre-FEC BER is not available, or there is no Rx signal.

        :return: pre-FEC BER
        :rtype: float
        """

        request_fec_ber = f"""
        <get-pm-data xmlns="http://www.advaoptical.com/aos/netconf/aos-core-pm"
                     xmlns:me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                     xmlns:fac="http://www.advaoptical.com/aos/netconf/aos-core-facility"
                     xmlns:adom-oduckpa="http://www.advaoptical.com/aos/netconf/aos-domain-otn-oduckpa"
                     xmlns:otn="http://www.advaoptical.com/aos/netconf/aos-domain-otn">
          <target-entity>/me:managed-element[me:entity-name="1"]/fac:interface[fac:name="{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}"]/fac:logical-interface/adom-oduckpa:otu-c2pa</target-entity>
          <pm-data>
            <pm-current-data/>
          </pm-data>
        </get-pm-data>
        """

        reply_fec_ber = self.conn.dispatch(to_ele(request_fec_ber))
        pre_fec_ber = xmltodict.parse(reply_fec_ber.xml)["nc:rpc-reply"]["pm-data"][
            "pm-current-data"
        ][0]["montype-monval"]["mon-val"]

        try:
            pre_fec_ber = float(pre_fec_ber)
        except:
            pre_fec_ber = 0.0

        return pre_fec_ber

    @property
    def _config(self):

        config_dict = {}

        response = self.get_interface()
        response_details = xmltodict.parse(response.xml)
        print(response)
        config = response_details["nc:rpc-reply"]["data"]["terminal-device"][
            "logical-channels"
        ]["channel"]

        # get line_ports and logical interfaces
        for config_details in config:
            if "odu4" not in config_details["config"]["description"]:
                line_port = config_details["config"]["description"].split("/ot")[0]
                config_dict[line_port] = {}
                config_dict[line_port]["line_port"] = line_port
                config_dict[line_port]["logical_interface"] = config_details["config"][
                    "description"
                ].split(line_port + "/")[1]
                config_dict[line_port]["index"] = config_details["config"]["index"]

        for line_port in config_dict.keys():
            if line_port != self.line_port:
                continue
            # get admin state
            response = self.get_port_admin_state()
            response_details = xmltodict.parse(response.xml)
            config_dict[line_port]["admin_state"] = response_details["nc:rpc-reply"][
                "data"
            ]["managed-element"]["interface"]["physical-interface"]["state"][
                "admin-state"
            ]

            # read power and frequency
            response = self.get_power_and_frequency()
            response_details = xmltodict.parse(response.xml)
            component_details = response_details["nc:rpc-reply"]["data"]["components"][
                "component"
            ]
            if "config" in component_details.keys():
                assert component_details["config"]["name"] == "optch " + line_port
                try:
                    config_dict[line_port]["frequency"] = component_details[
                        "optical-channel"
                    ]["config"]["frequency"]
                    config_dict[line_port]["target-output-power"] = component_details[
                        "optical-channel"
                    ]["config"]["target-output-power"]
                except:
                    config_dict[line_port]["frequency"] = "0"
                    config_dict[line_port]["target-output-power"] = "0"

        return config_dict

    def get_operational_state(self):
        """Method to get the operational state of the QuadFlex device

        :return: operational state of the QuadFlex device
        :rtype: str
        """
        request = f"""
                <components xmlns="http://openconfig.net/yang/platform">
                <component>
                <name>port {self.line_port}</name>
                <state>
                <oper-status/>
                </state>
                </component>
                </components>
                """
        flt = ("subtree", request)
        return self.conn.get(filter=flt)

    def get_interface_state(self):
        """Method to get the interface state of the Quadflex device

        :return: interface state
        :rtype: str
        """
        request = f"""
        <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
        <entity-name>1</entity-name>
        <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">   
        <name>{self.line_port}</name>
        <physical-interface>
        <state xmlns:acor-stt="http://www.advaoptical.com/aos/netconf/aos-core-state-types">
        </state>
        </physical-interface>
        </interface>
        </managed-element>
        """

        flt = ("subtree", request)
        return self.conn.get_config(source="running", filter=flt)

    def get_port_admin_state(self):
        """Method to get the port admin state of the Quadflex device

        :return: port admin state

        :rtype: str"""
        request = f"""
      <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
            <entity-name>1</entity-name>
            <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
                <name>{self.line_port}</name>
                <physical-interface>
                    <state xmlns:acor-stt="http://www.advaoptical.com/aos/netconf/aos-core-state-types">
                        <admin-state/>
                    </state>
                </physical-interface>
            </interface>
        </managed-element>
        """

        flt = ("subtree", request)
        return self.conn.get_config(source="running", filter=flt)

    def get_interface(self):
        """Method to get the interface configuration of the Quadflex device

        :return: interface configuration"""
        request = """
        <terminal-device xmlns="http://openconfig.net/yang/terminal-device">
        <logical-channels>
        <channel>
        <config>
        <index/>
        <description/>
        </config>
        </channel>
        </logical-channels>
        </terminal-device>
        """

        flt = ("subtree", request)

        return self.conn.get_config(source="running", filter=flt)

    def set_interface_on(self):
        """Method to set the interface state of the Quadflex device to 'is' (in-service)"""
        return self.set_interface_state(1)

    def set_interface_off(self):
        """Method to set the interface state of the Teraflex device to 'oos' (out-of-service)"""
        return self.set_interface_state(0)

    def set_interface_state(self, state):
        """Method to set the interface state of the Teraflex device to 'is' (in-service) or 'oos' (out-of-service)

        :param state: 1 for 'is' (in-service) and 0 for 'oos' (out-of-service)
        :type state: int

        :return: response from the Teraflex device
        """
        if state == 0:
            adva_state = "acor-stt:oos"
        else:
            adva_state = "acor-stt:is"

        request = f"""
        <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
        <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
        <entity-name>1</entity-name>
        <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">   
        <name>{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}</name>
        <physical-interface>
        <state xmlns:acor-stt="http://www.advaoptical.com/aos/netconf/aos-core-state-types">
        <admin-state>{adva_state}</admin-state>
        </state>
        </physical-interface>
        </interface>
        </managed-element>
        </nc:config>
        """
        response = self.conn.edit_config(target="running", config=request)
        assert "ok" in xmltodict.parse(response.xml)["nc:rpc-reply"].keys(), print(
            response
        )
        return response

    def __set_admin_maintenance(self, element):
        """

        :param element: line_port or logical interface e.g. 1/2/n1 or 1/2/n1/ot200
        :return:
        """
        request = f"""
     <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
     <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                   xmlns:f8-ne="http://www.advaoptical.com/aos/netconf/adva-f8-ne"
                   xmlns:acor-me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
        <entity-name>1</entity-name>
        <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
          <name>{element}</name>
          <physical-interface xmlns:acor-factt="http://www.advaoptical.com/aos/netconf/aos-core-facility-types">
            <state xmlns:acor-stt="http://www.advaoptical.com/aos/netconf/aos-core-state-types">
              <admin-is-sub-states>acor-stt:mt</admin-is-sub-states>
            </state>
          </physical-interface>
        </interface>
      </managed-element>
      </nc:config>
        """

        return self.conn.edit_config(target="running", config=request)

    def __remove_admin_maintenance(self, element):
        """

        :param element: line_port or logical interface e.g. 1/2/n1 or 1/2/n1/ot200
        :return:
        """
        request = f"""
        <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
          <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                       xmlns:f8-ne="http://www.advaoptical.com/aos/netconf/adva-f8-ne"
                       xmlns:acor-me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
            <entity-name>1</entity-name>
            <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
              <name>{element}</name>
              <physical-interface xmlns:acor-factt="http://www.advaoptical.com/aos/netconf/aos-core-facility-types">
                <state xmlns:acor-stt="http://www.advaoptical.com/aos/netconf/aos-core-state-types">
                  <admin-is-sub-states nc:operation="delete">acor-stt:mt</admin-is-sub-states>
                </state>
              </physical-interface>
            </interface>
          </managed-element>
           </nc:config>
        """

        return self.conn.edit_config(target="running", config=request)

    def get_symbolrate(self):
        """Method to get the symbol rate of the Quadflex device

        :return: symbol rate
        :rtype: str
        """

        request = f"""
                       <components>
                         <component>
                           <name>optch {self.line_port}</name>
                           <optical-channel>
                             <state>
                               <optical-channel-config>
                                 <bits-per-symbol/>
                                 <cdc-range/>
                                 <fec/>
                                 <modulation/>
                                 <symbol-rate/>
                               </optical-channel-config>
                             </state>
                           </optical-channel>
                         </component>
                       </components>
                   """
        flt = ("subtree", request)
        return self.conn.get(filter=flt)

    def delete_logical_interface(self):
        """Method to delete the logical interface of the Quadflex device. This must be used before creating a new logical interface.

        :return: response from the Quadflex device"""
        request = f"""
                <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <terminal-device xmlns="http://openconfig.net/yang/terminal-device">
                    <logical-channels>
                        <channel nc:operation="delete">
                        <index>{self._config[self.line_port]['index']}</index>
                            <config>
                                <index>{self._config[self.line_port]['index']}</index>
                                <description>{self.line_port}</description>
                            </config>
                        </channel>
                    </logical-channels>
                </terminal-device>
                </nc:config>
                """
        response = self.conn.edit_config(target="running", config=request)
        assert "ok" in xmltodict.parse(response.xml)["nc:rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["logical_interface"] = None
        return response

    def create_logical_interface(self, logical_interface):
        """Method to create a new logical interface of the Quadflex device such as 'ot200' for a particular line port.

        :param logical_interface: logical interface number such as 'ot200'. Unlike the Teraflex, thw Quadflex offers only 2 different line rates - 100 Gbps|DP-QPSK  and 200 Gbps|DP-16QAM, which are intrinsically tied to the modulation formats as well.
        :type logical_interface: str

        :return: response from the Quadflex device
        """
        request = f"""
                     <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
                          <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
                              <entity-name>1</entity-name>
                              <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
                              <name>{self._config[self.line_port]['line_port'] + '/' + logical_interface}</name>
                                  <logical-interface/>
                              </interface>
                          </managed-element>
                     </nc:config>
                     """
        response = self.conn.edit_config(target="running", config=request)
        assert "ok" in xmltodict.parse(response.xml)["nc:rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["logical_interface"] = logical_interface
        self._config[self.line_port]["modulation"] = self.get_interface_modulation()
        return response

    def get_power_and_frequency(self):
        """Method to get the power and frequency of the Quadflex device for a particular line port

        :return: power (dBm) and frequency (GHz)"""
        request = f"""
        <oc-platform:components xmlns:oc-platform="http://openconfig.net/yang/platform">
        <oc-platform:component>
        <oc-platform:config>
        <oc-platform:name>optch {self.line_port}</oc-platform:name>
        </oc-platform:config>
        <oc-opt-term:optical-channel xmlns:oc-opt-term="http://openconfig.net/yang/terminal-device">
        <oc-opt-term:config>
        <oc-opt-term:frequency/>
        <oc-opt-term:target-output-power/>
        </oc-opt-term:config>
        </oc-opt-term:optical-channel>
        </oc-platform:component>
        </oc-platform:components>
        """
        flt = ("subtree", request)

        return self.conn.get_config(source="running", filter=flt)

    def set_power_and_frequency(self, power, frequency):
        """Method to set the power and frequency of the Quadflex device for a particular line port

        :param power: target output power in dBm. Should be in the range of [-6.0,6.0] dBm
        :type power: float

        :param frequency: central frequency in THz. Should be in the units of GHz. For example, 193.1 THz should be given as 193100 GHz. The frequency should also be in multiples of 25 GHz.
        :type frequency: float

        :return: response from the Quadflex device
        """
        request = f"""
        <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
        <oc-platform:components xmlns:oc-platform="http://openconfig.net/yang/platform">
        <oc-platform:component>
        <oc-platform:name>optch {self.line_port}</oc-platform:name>
        <oc-platform:config>
        <oc-platform:name>optch {self.line_port}</oc-platform:name>
        </oc-platform:config>
        <oc-opt-term:optical-channel xmlns:oc-opt-term="http://openconfig.net/yang/terminal-device">
        <oc-opt-term:config>
        <oc-opt-term:frequency>{frequency}</oc-opt-term:frequency>
        <oc-opt-term:target-output-power>{power:.1f}</oc-opt-term:target-output-power>
        </oc-opt-term:config>
        </oc-opt-term:optical-channel>
        </oc-platform:component>
        </oc-platform:components>
        </nc:config>
        """
        response = self.conn.edit_config(target="running", config=request)
        assert "nc:ok" in xmltodict.parse(response.xml)["nc:rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["frequency"] = frequency
        self._config[self.line_port]["target-output-power"] = power
        return response

    # def read_pm_data(self, sleep_counter, DEBUG=False):
    #     offline = True
    #     stable = False
    #     localtime = time.localtime()
    #     result = time.strftime("%I:%M:%S %p", localtime)
    #     time.sleep(sleep_counter)
    #     localtime = time.localtime()
    #     result = time.strftime("%I:%M:%S %p", localtime)
    #     counter = 0
    #     while offline:
    #         response = self.get_operational_state()
    #         response_details = xmltodict.parse(response.xml)
    #         status = response_details["nc:rpc-reply"]["data"]["components"][
    #             "component"
    #         ]["state"]["oper-status"]
    #         if DEBUG:
    #             print(status)
    #         offline = None if status == "ACTIVE" else True
    #         time.sleep(5)
    #         counter += 1
    #         if counter > 5:
    #             raise SystemError("Teraflex is offline")

    #     while not stable:
    #         pm_data = self.get_params()
    #         if pm_data["QualityTF_indefinite_q-factor"]:
    #             Q_factor = float(pm_data["QualityTF_indefinite_q-factor"])
    #             time.sleep(5)
    #             pm_data_verification = self.get_params()
    #             stable = (
    #                 abs(
    #                     Q_factor
    #                     - float(pm_data_verification["QualityTF_indefinite_q-factor"])
    #                 )
    #                 < 0.05
    #             )
    #             if DEBUG:
    #                 print(
    #                     Q_factor,
    #                     float(pm_data_verification["QualityTF_indefinite_q-factor"]),
    #                 )
    #         else:
    #             time.sleep(15)
    #     pm_data = self.get_params()
    #     if DEBUG:
    #         print(pm_data["QualityTF_indefinite_q-factor"])
    #     return pm_data
