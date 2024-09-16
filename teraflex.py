from ncclient import manager
from ncclient.xml_ import to_ele
import xmltodict
import logging
import time
import sys
from utils import check_patch_owners


class TFlex:

    """Class to interact with the Teraflex device using NETCONF protocol. The class provides methods to read the performance monitoring data, change the configuration, and return the current configuration of the Teraflex device. It also checks if the user is authorized to use the device. If the user is not authorized, it raises an Exception and does not connect to the device.

    :param tf_name: The particular transciever name such as 'tf_1', 'tf_2', 'tf_3', 'tf_4'
    :type tf_name: str

    :raise: Exception: If the user is not authorized to use the device
    :raise: Exception: If the Teraflex name is invalid
    """

    def __init__(self, tf_name):

        if tf_name == "tf_1":
            self.line_port = "1/1/n1"
        elif tf_name == "tf_2":
            self.line_port = "1/1/n2"
        elif tf_name == "tf_3":
            self.line_port = "1/2/n1"
        elif tf_name == "tf_4":
            self.line_port = "1/2/n2"
        else:
            raise Exception("Invalid Teraflex name")

        if check_patch_owners(
            [(tf_name, tf_name)]
        ):  # We need to map individual line ports to patches and then configure check_patch_list for each set line_port method

            self.conn = manager.connect(
                host="10.10.10.92",
                port=830,
                username="admin",
                password="CHGME.1a",
                timeout=60,
                hostkey_verify=False,
                look_for_keys=False,  # there is a bug in ncclient, which has a temporary workaround here, but ideally should raise a pull request to fix the bug
            )
            self.conn.raise_mode = 0  # on RPCError, do not throw any exceptions
            self._config = {}
            self.__get_config()
        else:
            raise Exception("You are not authorized to use this device")

    def read_pm_data(self, sleep_counter=10, DEBUG=False):
        """Method to read the performance monitoring data from the Teraflex device

        :param sleep_counter: sleep time in seconds, default is 10. Teraflex device needs 10 seconds of time to stabliize the measurements before reading PM data. Increase this value if the device is not stable. Alternatively, decrease this value for testing.
        :type sleep_counter: int

        :param DEBUG: set to True to print the PM data
        :type DEBUG: bool

        :return: dictionary containing the PM data
        :rtype: dict

        :raises: SystemError: if the Teraflex device is offline
        :raises: Exception: If there is no recieving signals to the Rx port

        """

        offline = True
        stable = False
        time.sleep(sleep_counter)
        counter = 0
        while offline:
            response = self.get_operational_state()
            response_details = xmltodict.parse(response.xml)
            status = response_details["rpc-reply"]["data"]["components"]["component"][
                "state"
            ]["oper-status"]
            if DEBUG:
                print(status)
            offline = None if status == "ACTIVE" else True
            time.sleep(5)
            counter += 1
            if counter > 5:
                raise SystemError("Teraflex is offline")

        while not stable:
            pm_data = self.get_params()
            if pm_data["QualityTF_indefinite_q-factor"]:
                Q_factor = float(pm_data["QualityTF_indefinite_q-factor"])
                time.sleep(5)
                pm_data_verification = self.get_params()
                stable = (
                    abs(
                        Q_factor
                        - float(pm_data_verification["QualityTF_indefinite_q-factor"])
                    )
                    < 0.05
                )
                if DEBUG:
                    print(
                        Q_factor,
                        float(pm_data_verification["QualityTF_indefinite_q-factor"]),
                    )
            else:
                time.sleep(15)
        pm_data = self.get_params()
        if DEBUG:
            print(pm_data["QualityTF_indefinite_q-factor"])
        return pm_data

    def change_configuration(
        self,
        logical_interface,
        modulation,
        target_power,
        central_frequency,
        fec="sdfec-acacia15-7iterations",
        rolloff=0.19,
    ):
        """Method to change the configuration of the Teraflex device

        :param logical_interface: logical interface number such as 'ot200'. This depends on the bandwidth and bit rate of the signal chosen. Refer to documentation for available options. For example, 'ot200' is for 200G signal.
        :type logical_interface: str

        :param modulation: modulation scheme such as 'dp-qam16', 'dp-qam64', 'dp-qpsk'. Refer to documentation for available options. This is tied to the logical interface used. Some modulations are not available for all logical interfaces.
        :type modulation: str

        :param target_power: target output power in dBm. Should be in the range of [-2.0,2.0] dBm
        :type target_power: float

        :param central_frequency: central frequency in THz. Should be in the units of GHz. For example, 193.1 THz should be given as 193100 GHz
        :type central_frequency: float

        :param fec: Forward Error Correction algorithm. Default is 'sdfec-acacia15-7iterations'. Refer to documentation for available options.
        :type fec: str

        :param rolloff: filter roll-off factor. Default is 0.19. Refer to documentation for available options.
        :type rolloff: float

        :return: sleep_counter: sleep time in seconds. This is the time required for the Teraflex device to stabilize the configuration changes. Default is 30 seconds.
        :rtype: int

        :raises: SystemError: if the Teraflex device is offline
        :raises Exception: if the configuration changes are not successful
        :raises Exception: if the logical interface is not available for the line port
        :raises Exception: if the modulation is not available for the logical interface
        :raises Exception: if the target power is out of range
        :raises Exception: if the central frequency is out of range
        :raises Exception: if the FEC algorithm is not available
        """

        sleep_counter = 30
        if self._config[self.line_port]["admin_state"] != "acor-stt:is":
            self.set_interface_on()
        if self._config[self.line_port]["logical_interface"] != logical_interface:
            if self._config[self.line_port]["logical_interface"]:
                self.delete_logical_interface()
            self.create_logical_interface(logical_interface)
            sleep_counter = 150
        if self._config[self.line_port]["modulation"] != modulation:
            self.__set_admin_maintenance(self.line_port + "/" + logical_interface)
            self.set_interface_modulation(modulation)
            self.__remove_admin_maintenance(self.line_port + "/" + logical_interface)
            sleep_counter = 150
        # if self._config[line_port]['fec'] != fec:
        #     self.set_fec_algorithm(line_port, fec)
        # if self._config[line_port]['filter-roll-off'] != rolloff:
        #     self.set_filterrolloff(line_port, rolloff)
        self.set_power_and_frequency(power=target_power, frequency=central_frequency)
        return sleep_counter

    def return_current_config(self):
        """Method to return the current configuration of the Teraflex device

        :return: dictionary containing the current configuration
        :rtype: dict
        """

        return self._config

    def __get_config(self):  # This should be done in a more efficient way
        response = self.get_interface()
        response_details = xmltodict.parse(response.xml)
        config = response_details["rpc-reply"]["data"]["terminal-device"][
            "logical-channels"
        ]["channel"]
        self._config[self.line_port] = {}
        self._config[self.line_port]["line_port"] = self.line_port

        for channel in config:

            if (channel["config"]["description"][:6] == self.line_port) and (
                len(channel["config"]["description"].split("/")) == 4
            ):
                self._config[self.line_port]["logical_interface"] = channel["config"][
                    "description"
                ].split("/")[3]
                self._config[self.line_port]["index"] = channel["config"]["index"]

        assert self._config[self.line_port]["logical_interface"] is not None

        response = self.get_port_admin_state()
        response_details = xmltodict.parse(response.xml)
        self._config[self.line_port]["admin_state"] = response_details["rpc-reply"][
            "data"
        ]["managed-element"]["interface"]["physical-interface"]["state"]["admin-state"]

        # get modulation
        response = self.get_interface_modulation()
        response_details = xmltodict.parse(response.xml)
        self._config[self.line_port]["modulation"] = response_details["rpc-reply"][
            "data"
        ]["managed-element"]["interface"]["logical-interface"]["otsia"]["otsi"][
            "optical-channel-configuration"
        ][
            "modulation"
        ]

        # get rolloff
        response = self.get_filterrolloff()
        response_details = xmltodict.parse(response.xml)
        try:
            self._config[self.line_port]["filter-roll-off"] = response_details[
                "rpc-reply"
            ]["data"]["managed-element"]["interface"]["logical-interface"]["otsia"][
                "otsi"
            ][
                "optical-channel-configuration"
            ][
                "filter-roll-off"
            ]
        except:
            self._config[self.line_port]["filter-roll-off"] = "0"

        # read power and frequency
        response = self.get_power_and_frequency()
        response_details = xmltodict.parse(response.xml)
        for component_details in response_details["rpc-reply"]["data"]["components"][
            "component"
        ]:
            if "config" in component_details.keys():
                assert component_details["config"]["name"] == "optch " + self.line_port
                try:
                    self._config[self.line_port]["frequency"] = component_details[
                        "optical-channel"
                    ]["config"]["frequency"]
                    self._config[self.line_port][
                        "target-output-power"
                    ] = component_details["optical-channel"]["config"][
                        "target-output-power"
                    ]
                except:
                    self._config[self.line_port]["frequency"] = "0"
                    self._config[self.line_port]["target-output-power"] = "0"

        # read fec
        response = self.get_fec_algorithm()
        response_details = xmltodict.parse(response.xml)
        for component_details in response_details["rpc-reply"]["data"]["components"][
            "component"
        ]:
            if "config" in component_details.keys():
                assert component_details["config"]["name"] == "optch " + self.line_port
                try:
                    self._config[self.line_port]["fec"] = component_details[
                        "optical-channel"
                    ]["config"]["optical-channel-config"]["fec"]
                except:
                    self._config[self.line_port]["fec"] = "0"

        # get symbolrate
        try:
            self._config[self.line_port]["symbolrate"] = self.get_symbolrate()
        except:
            self._config[self.line_port]["symbolrate"] = "0"

    def get_operational_state(self):
        """Method to get the operational state of the Teraflex device

        :return: operational state of the Teraflex device
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

    def get_interface(self):
        """Method to get the interface configuration of the Teraflex device

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

    def delete_logical_interface(self):
        """Method to delete the logical interface of the Teraflex device. This must be used before creating a new logical interface.

        :return: response from the Teraflex device"""

        request = f"""
                <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
                <terminal-device xmlns="http://openconfig.net/yang/terminal-device">
                    <logical-channels>
                        <channel nc:operation="delete">
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
        assert "ok" in xmltodict.parse(response.xml)["rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["logical_interface"] = None
        return response

    def create_logical_interface(self, logical_interface):
        """Method to create a new logical interface of the Teraflex device such as 'ot200' for a particular line port.

        :param logical_interface: logical interface number such as 'ot200'. This depends on the bandwidth and bit rate of the signal chosen. Refer to documentation for available options. For example, 'ot200' is for 200G signal.
        :type logical_interface: str

        :return: response from the Teraflex device
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
        assert "ok" in xmltodict.parse(response.xml)["rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["logical_interface"] = logical_interface
        self._config[self.line_port]["modulation"] = self.get_interface_modulation()
        return response

    def get_interface_modulation(self):
        """Method to get the modulation scheme of the Teraflex device

        :return: modulation scheme
        :rtype: str
        """

        request = f"""<managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                               xmlns:f8-ne="http://www.advaoptical.com/aos/netconf/adva-f8-ne"
                               xmlns:acor-me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
                                <entity-name>1</entity-name>
                                  <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
                                    <name>{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}</name>
                                    <logical-interface>
                                      <entity-name>{self._config[self.line_port]['logical_interface']}</entity-name>
                                      <otsia xmlns="http://www.advaoptical.com/aos/netconf/aos-domain-otn">
                                        <otsi>
                                          <id>1</id>
                                          <optical-channel-configuration>
                                            <modulation/>
                                          </optical-channel-configuration>
                                        </otsi>
                                      </otsia>
                                    </logical-interface>
                                  </interface>
                                </managed-element>
                                """
        flt = ("subtree", request)
        return self.conn.get_config(source="running", filter=flt)

    def set_interface_modulation(self, modulation):
        """Method to set the modulation scheme of the Teraflex device such as 'dp-16qam', 'dp-64qam', 'dp-qpsk' for a particular line port.

        :param modulation: modulation scheme such as 'dp-qam16', 'dp-qam64', 'dp-qpsk'. Refer to documentation for available options. This is tied to the logical interface used. Some modulations are not available for all logical interfaces.
        :type modulation: str

        :return: response from the Teraflex device
        """

        request = f"""
                    <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
                        <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                               xmlns:f8-ne="http://www.advaoptical.com/aos/netconf/adva-f8-ne"
                               xmlns:acor-me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
                                <entity-name>1</entity-name>
                                  <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
                                    <name>{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}</name>
                                    <logical-interface>
                                      <entity-name>{self._config[self.line_port]['logical_interface']}</entity-name>
                                      <otsia xmlns="http://www.advaoptical.com/aos/netconf/aos-domain-otn">
                                        <otsi>
                                          <id>1</id>
                                          <optical-channel-configuration>
                                            <modulation>{modulation}</modulation>
                                            <state-of-polarization-tracking>normal-tracking</state-of-polarization-tracking>
                                          </optical-channel-configuration>
                                        </otsi>
                                      </otsia>
                                    </logical-interface>
                                  </interface>
                                </managed-element>
                              </nc:config>
                                """
        response = self.conn.edit_config(target="running", config=request)
        assert "ok" in xmltodict.parse(response.xml)["rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["modulation"] = modulation
        return response

    def get_power_and_frequency(self):
        """Method to get the power and frequency of the Teraflex device for a particular line port

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
        """Method to set the power and frequency of the Teraflex device for a particular line port

        :param power: target output power in dBm. Should be in the range of [-2.0,2.0] dBm
        :type power: float

        :param frequency: central frequency in THz. Should be in the units of GHz. For example, 193.1 THz should be given as 193100 GHz
        :type frequency: float

        :return: response from the Teraflex device
        """

        request = f"""
        <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
        <oc-platform:components xmlns:oc-platform="http://openconfig.net/yang/platform">
        <oc-platform:component>
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
        assert "ok" in xmltodict.parse(response.xml)["rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["frequency"] = frequency
        self._config[self.line_port]["target-output-power"] = power
        return response

    def set_interface_on(self):
        """Method to set the interface state of the Teraflex device to 'is' (in-service)"""

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
        assert "ok" in xmltodict.parse(response.xml)["rpc-reply"].keys(), print(
            response
        )
        return response

    def get_interface_state(self):
        """Method to get the interface state of the Teraflex device

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

    def __set_admin_maintenance(self, element):

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

    def get_port_admin_state(self):
        """Method to get the port admin state of the Teraflex device

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

    def get_params(self, DEBUG=False):
        """Method to get the BER historical readings of the Teraflex device for a particular line port.

        CAUTION: This method is not recommended for real-time monitoring. Use read_pm_data method instead.

        :param DEBUG: set to True to print the PM data
        :type DEBUG: bool

        :return: dictionary containing the PM data
        :rtype: dict
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

        perf_categories = perf_details["rpc-reply"]["pm-data"]["pm-current-data"]

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
          <target-entity>/me:managed-element[me:entity-name="1"]/fac:interface[fac:name="{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}"]/fac:logical-interface/adom-oduckpa:otu-c2pa</target-entity>
          <pm-data>
            <pm-current-data/>
          </pm-data>
        </get-pm-data>
        """

        reply_fec_ber = self.conn.dispatch(to_ele(request_fec_ber))

        perf_details = xmltodict.parse(reply_fec_ber.xml)
        if "pm-data" in perf_details["rpc-reply"].keys():
            perf_categories = perf_details["rpc-reply"]["pm-data"]["pm-current-data"]
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

    def get_symbolrate(self):
        """Method to get the symbol rate of the Teraflex device

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

    def set_filterrolloff(self, rolloff):
        """Method to set the filter roll-off factor of the Teraflex device

        :param rolloff: filter roll-off factor. Default is 0.19. Refer to documentation for available options.
        :type rolloff: float

        :return: response from the Teraflex device
        """

        request = f"""
                            <nc:config xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0">
                                <managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                                       xmlns:f8-ne="http://www.advaoptical.com/aos/netconf/adva-f8-ne"
                                       xmlns:acor-me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
                                        <entity-name>1</entity-name>
                                          <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
                                            <name>{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}</name>
                                            <logical-interface>
                                              <entity-name>{self._config[self.line_port]['logical_interface']}</entity-name>
                                              <otsia xmlns="http://www.advaoptical.com/aos/netconf/aos-domain-otn">
                                                <otsi>
                                                  <id>1</id>
                                                  <optical-channel-configuration>
                                                    <filter-roll-off>{rolloff:.2f}</filter-roll-off>
                                                    <state-of-polarization-tracking>normal-tracking</state-of-polarization-tracking>
                                                  </optical-channel-configuration>
                                                </otsi>
                                              </otsia>
                                            </logical-interface>
                                          </interface>
                                        </managed-element>
                                      </nc:config>
                                        """
        response = self.conn.edit_config(target="running", config=request)
        assert "ok" in xmltodict.parse(response.xml)["rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["filter-roll-off"] = rolloff
        return response

    def get_filterrolloff(self):
        """Method to get the filter roll-off factor of the Teraflex device

        :return: filter roll-off factor
        :rtype: str
        """

        request = f"""<managed-element xmlns="http://www.advaoptical.com/aos/netconf/aos-core-managed-element"
                               xmlns:f8-ne="http://www.advaoptical.com/aos/netconf/adva-f8-ne"
                               xmlns:acor-me="http://www.advaoptical.com/aos/netconf/aos-core-managed-element">
                                <entity-name>1</entity-name>
                                  <interface xmlns="http://www.advaoptical.com/aos/netconf/aos-core-facility">
                                    <name>{self._config[self.line_port]['line_port'] + '/' + self._config[self.line_port]['logical_interface']}</name>
                                    <logical-interface>
                                      <entity-name>{self._config[self.line_port]['logical_interface']}</entity-name>
                                      <otsia xmlns="http://www.advaoptical.com/aos/netconf/aos-domain-otn">
                                        <otsi>
                                          <id>1</id>
                                          <optical-channel-configuration>
                                            <filter-roll-off/>
                                          </optical-channel-configuration>
                                        </otsi>
                                      </otsia>
                                    </logical-interface>
                                  </interface>
                                </managed-element>
                                """
        flt = ("subtree", request)
        return self.conn.get_config(source="running", filter=flt)

    def get_fec_algorithm(self):
        """Method to get the FEC algorithm of the Teraflex device

        :return: FEC algorithm
        :rtype: str
        """

        request = f"""<components xmlns="http://openconfig.net/yang/platform">
                      <component>
                        <config>
                          <name>optch {self.line_port}</name>
                        </config>
                        <optical-channel xmlns="http://openconfig.net/yang/terminal-device">
                          <config>
                            <optical-channel-config xmlns="http://www.advaoptical.com/openconfig/terminal-device-dev">
                              <fec/>
                            </optical-channel-config>
                          </config>
                        </optical-channel>
                      </component>
                    </components>
                    """
        flt = ("subtree", request)
        return self.conn.get_config(source="running", filter=flt)

    def set_fec_algorithm(self, fec="sdfec-acacia15-7iterations"):
        """Method to set the FEC algorithm of the Teraflex device

        :param fec: Forward Error Correction algorithm. Default is 'sdfec-acacia15-7iterations'. Refer to documentation for available options.
        :type fec: str

        :return: response from the Teraflex device
        """

        request = f"""<components xmlns="http://openconfig.net/yang/platform">
                      <component>
                        <config>
                          <name>optch {self.line_port}</name>
                        </config>
                        <optical-channel xmlns="http://openconfig.net/yang/terminal-device">
                          <config>
                            <optical-channel-config xmlns="http://www.advaoptical.com/openconfig/terminal-device-dev">
                              <fec>{fec}</fec>
                            </optical-channel-config>
                          </config>
                        </optical-channel>
                      </component>
                    </components>
                    """
        response = self.conn.edit_config(target="running", config=request)
        assert "ok" in xmltodict.parse(response.xml)["rpc-reply"].keys(), print(
            response
        )
        self._config[self.line_port]["fec"] = fec
        return response
