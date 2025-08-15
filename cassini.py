import pandas as pd
import numpy as np
import os
from paramiko import *
import re
import sys
import time
from .utils import *


class Cassini:

    """Initialize the Cassini Transceiver. It also checks if the user is authorized to use the device. If the user is not authorized, it raises an Exception and does not connect to the device.

    :param cassini_num: The Cassini Transsciever ('cassini_1', 'cassini_2', 'cassini_3', 'cassini_4')
    :type cassini_num: str

    :param verbose: Print the output of the commands
    :type verbose: bool
    """

    def __init__(self, cassini_num, verbose=True):

        if not check_patch_owners([(cassini_num, cassini_num)]):
            raise Exception(
                "You are not authorized to use this device! Please contact the administrator."
            )

        if cassini_num == "cassini_1":
            self.module = "/dev/piu1"
        elif cassini_num == "cassini_2":
            self.module = "/dev/piu3"
        elif cassini_num == "cassini_3":
            self.module = "/dev/piu5"
        elif cassini_num == "cassini_4":
            self.module = "/dev/piu7"

        self.client = SSHClient()
        # https://stackoverflow.com/questions/53635843/paramiko-ssh-failing-with-server-not-found-in-known-hosts-when-run-on-we
        self.client.set_missing_host_key_policy(AutoAddPolicy())
        self.client.connect("10.10.10.39", username="root", password="x1")
        command = "kubectl get pods"
        stdin, stdout, stderr = self.client.exec_command(command)
        lines = stdout.read().decode().split("\n")
        for line in lines:
            m = re.match("(tai-\S+)\s*", line)
            if m:
                self.tai_pod = m.group(1)
        if not self.tai_pod:
            raise Exception("No tai pod found")
        self.verbose = verbose

        self.attr_dict = load_csv_with_pandas("cassini_attributes.csv")
        self.attr_list = self.attr_dict["Name"].tolist()

    def get_attributes(self, attr_list=None, debug=True):

        """Get the performance monitoring attributes of the Cassini Transceiver. The attributes can be found in the cassisni_attributes.csv file. If no attributes are provided, it will return all the performance monitoring parameters.

        :param attr_list: List of attributes to monitor
        :type attr_list: list

        :param debug: Print the output of the commands
        :type debug: bool

        :return: Dictionary of the attributes and their values
        :rtype: dict
        """

        attr_state = {}
        attr_state["timestamp"] = time.time()
        if attr_list is None:
            attr_list = self.attr_list
        channel = self.client.invoke_shell()

        for attr in attr_list:
            command = f"get {str(attr)}"
            ret = self.__get_command(command)
            if attr == "current-post-fec-ber":
                for _ in range(5):
                    try:
                        float(ret)
                    except:
                        continue
                    else:
                        break
            attr_state[attr] = ret
            if self.verbose and debug:
                print(f"{attr}: {ret}")

        return attr_state

    def get_current_input_power(self):
        """Get the current input power (dBm) on the Rx of Cassini Transceiver

        :return: Current input power (dBm)
        :rtype: float
        """
        ret = self.__get_command("get current-input-power")
        return ret

    def get_current_output_power(self):
        """Get the current output power (dBm) of the Cassini Transceiver Tx

        :return: Current output power (dBm)
        :rtype: float
        """
        ret = self.__get_command("get current-output-power")
        return ret

    def get_oper_status(self):
        """Get the operational status of the Cassini Transceiver. The output is 'ready' if the transceiver is ready to transmit data.

        :return: Operational status
        :rtype: str"""
        ret = self.__get_command("get oper-status")
        return ret

    def get_dsp_oper_status(self):
        """Get the DSP operational status of the Cassini Transceiver. The output gives more granular status of the transciever status. The output will show 'ready' if the receiver is receiving signals

        :return: DSP operational status
        :rtype: str
        """
        ret = self.__get_command("get dsp-oper-status")
        return ret

    def get_modulation_format(self):
        """Get the modulation format of the Cassini Transceiver. The output will show the modulation

        :return: Modulation format
        :rtype: str"""
        ret = self.__get_command("get modulation-format")
        return ret

    def get_current_post_fec_ber(self):
        """Get the current post-FEC BER of the Cassini Transceiver. The output will show the BER value

        :return: Current post-FEC BER
        :rtype: str
        """
        ret = self.__get_command("get current-post-fec-ber")
        return ret

    def get_current_pre_fec_ber(self):
        """Get the current pre-FEC BER of the Cassini Transceiver. The output will show the BER value

        :return: Current pre-FEC BER
        :rtype: str
        """
        ret = self.__get_command("get current-pre-fec-ber")
        return ret

    def get_current_sd_fec_ber(self):
        """Get the current Soft-Decision(SD) FEC BER of the Cassini Transceiver. The output will show the BER value

        :return: Current SD FEC BER
        :rtype: str
        """
        ret = self.__get_command("get current-sd-fec-ber")
        return ret

    def get_current_hd_fec_ber(self):
        """Get the current Hard-Decision(HD) FEC BER of the Cassini Transceiver. The output will show the BER value

        :return: Current HD FEC BER
        :rtype: str
        """
        ret = self.__get_command("get current-hd-fec-ber")
        return ret

    def set_modulation_format(self, format):
        """Set the modulation format. The format can be 'dp-qpsk' or 'dp-16qam'.

        :param format: Modulation format
        :type format: str
        """
        self.__get_command("set modulation-format %s" % format)
        return

    def set_output_power(self, power):
        """Set the output power of the Cassini Transceiver. The power should be in dBm, and in the limit [-1.0, 1.0] dBm

        :param power: Output power in dBm
        :type power: float"""
        self.__get_command("set output-power %f" % power)
        return

    def get_output_power(self):
        """Get the output power of the Cassini Transceiver.

        :return: Output power in dBm
        :rtype: str"""
        ret = self.__get_command("get output-power")
        return ret

    def get_tx_laser_freq(self):
        """Get the transmitter laser frequency (Hz) of the Cassini Transceiver

        :return: Transmitter laser frequency (Hz)
        :rtype: str"""
        ret = self.__get_command("get tx-laser-freq")
        return ret

    def set_tx_laser_freq(self, freq):
        """Set the transmitter laser frequency (Hz)

        :param freq: Transmitter laser frequency (Hz)
        :type freq: str
        """
        self.__get_command("set tx-laser-freq %s" % freq)
        return

    def __get_command(self, cmd):
        channel = self.client.invoke_shell()
        # channel.get_pty()
        stdin = channel.makefile("w")
        stdout = channel.makefile("r")

        # print("w"," ",channel.recv(9999).decode('utf-8'))
        taish_cmd = "\"echo topper; taish -c 'module %s; netif 0; %s'\"" % (
            self.module,
            cmd,
        )
        command = "kubectl exec %s -- bash -c %s" % (self.tai_pod, taish_cmd)
        stdin.write(command)
        stdin.write("\n")
        for i in range(1, 20):
            ret = channel.recv(9999).decode("utf-8")
            if ret.startswith("topper"):
                ret = channel.recv(9999).decode("utf-8")
                return ret.strip()
        return "NaN"

    # def __set_command(self, cmd):

    #     channel = self.client.invoke_shell()
    #     # channel.get_pty()
    #     stdin = channel.makefile("w")
    #     stdout = channel.makefile("r")

    #     # print("w"," ",channel.recv(9999).decode('utf-8'))
    #     taish_cmd = "\"echo topper; taish -c 'module %s; netif 0; %s'\"" % (
    #         self.module,
    #         cmd,
    #     )
    #     command = "kubectl exec %s -- bash -c %s" % (self.tai_pod, taish_cmd)
    #     stdin.write(command)
    #     stdin.write("\n")
    #     return

    # def get_cas_stat(self, time_stamp):

    #     monitor_list = (
    #         output_power,
    #         current_output_power,
    #         current_input_power,
    #         operation_status,
    #         dsp_operation_status,
    #         modulation_format,
    #         laser_freq,
    #         current_postFEC_BER,
    #         current_preFEC_BER,
    #     ) = [
    #         self.get_output_power(),
    #         self.get_current_output_power(),
    #         self.get_current_input_power(),
    #         self.get_oper_status(),
    #         self.get_dsp_oper_status(),
    #         self.get_modulation_format(),
    #         self.get_tx_laser_freq(),
    #         self.get_current_post_fec_ber(),
    #         self.get_current_pre_fec_ber(),
    #     ]
    #     monitor_list.append(time_stamp)

    #     # self.dframe = self.dframe.append(
    #     #     pd.DataFrame(
    #     #         [monitor_list],
    #     #         columns=[
    #     #             "output_power",
    #     #             "current_output_power",
    #     #             "current_input_power",
    #     #             "operation_status",
    #     #             "dsp_operation_status",
    #     #             "modulation_format",
    #     #             "laser_freq",
    #     #             "current_postFEC_BER",
    #     #             "current_preFEC_BER",
    #     #             "time_stamp",
    #     #         ],
    #     #     ),
    #     #     ignore_index=True,
    #     # )

    #     if self.verbose:

    #         print("Output Power: ", output_power)
    #         print("Current Output Power: ", current_output_power)
    #         print("Current Input Power: ", current_input_power)
    #         print("Operation Status: ", operation_status)
    #         print("DSP Operation Status: ", dsp_operation_status)
    #         print("Modulation Format: ", modulation_format)
    #         print("Transmitter Laser Frequency: ", laser_freq)
    #         print("Current Post-FEC BER: ", current_postFEC_BER)
    #         print("Current Pre-FEC BER: ", current_preFEC_BER)
    #         print("\n")

    #     return monitor_list


########### DEBUGGING ############

# from paramiko import *
# import re

# ip = "10.10.10.39"
# client=SSHClient()
# client.set_missing_host_key_policy(AutoAddPolicy())
# client.connect(ip,username='root', password='x1')

# command='kubectl get pods'
# stdin, stdout, stderr = client.exec_command(command)
# lines=stdout.read().decode().split('\n')
# for line in lines:
#     m=re.match("(tai-\S+)\s*",line)
#     if m:
#         tai_pod=m.group(1)

# ## convert taish command
# # taish_cmd='\"echo topper; taish -c \'module /dev/piu5; netif 0; %s\'\"' % cmd

# def __get_command(taish_cmd):
#     channel = client.invoke_shell()
#     stdin = channel.makefile('w')
#     stdout = channel.makefile('r')
#     command='kubectl exec %s -- bash -c %s' % (tai_pod, taish_cmd)
#     stdin.write(command)
#     stdin.write('\n')

#     for i in range(1,21):
#         ret = channel.recv(9999).decode('utf-8')
#         if ret.startswith('topper'):
#             ret=channel.recv(9999).decode('utf-8')
#             return ret.strip()
#     return "NaN"

## 1. ssh rooat@10.10.10.39
## 2. kubectl get pods
##   "Get the tai pod. e.g. tai-657d7d4647-xhdnj"
## 3. kubectl exec --stdin --tty tai-657d7d4647-xhdnj -- /bin/bash
## 4. taish
##    Taish terminal will open
## 5. module /dev/piu5
##  Can select /dev/piu1, /dev/piu3, /dev/piu5, /dev/piu7
## 6. netif 0
