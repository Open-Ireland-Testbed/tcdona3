import time
import json
from datetime import datetime
import matplotlib
import matplotlib.pyplot as plt
import copy
from collections import OrderedDict
import pandas as pd
from tcdona3.lumentum import (
    Lumentum,
    LUMENTUM_DEFAULT_WSS_LOSS,
    LUMENTUM_CHANNEL_QUANTITY,
    LUMENTUM_WSS_CHANNEL_FREQ_CENTER_LIST,
)
import xmltodict
from tcdona3.polatis import Polatis
from tcdona3.ila import ILA
from tcdona3.osa import OSA

# matplotlib.use('module://drawilleplot')
# matplotlib.use('svg')


class Monitor:
    def __init__(self, device_object):
        """Super Class for Monitoring Devices. Specific device monitoring classes should inherit from this class, and adhere to the defined methods.

        :param device_object: Device object to monitor
        :type device_object: Lumentum, Polatis, ILA, OSA

        :raises ValueError: If device object is not a valid device object
        """

        self.device = device_object

    def measurement_sweep(self):
        """Perform a measurement sweep of the device. This method should be implemented by the specific device monitoring class, and override the default method. The method should return a dictionary of the measurement data."""
        pass

    def measurement_state(self):
        """Get the current state of the device. This method should be implemented by the specific device monitoring class, and override the default method. The method should return a dictionary of the device state."""
        pass

    def write_json_data(self, filename, **kwargs):
        """Write measurement data to a json file. Additional data can be added to the json file by passing keyword arguments.

        :param filename: Name of the file to save the data
        :type filename: str

        :param **kwargs: Additional data to be saved in the json file

        :raises KeyboardInterrupt: If measurement is canceled by user
        :raises RuntimeError: If measurement is canceled by exception
        """

        self.start_timer = datetime.now()

        try:
            measurement_state = self.measurement_state()
            measurement_data = self.measurement_sweep()

        except KeyboardInterrupt:
            print("Measurement canceled by user. Save already collected data.")
            raise KeyboardInterrupt

        except Exception as e:
            print("Measurement canceled by exception. Save already collected data.")
            print(e)
            raise RuntimeError

        finally:
            print("Measurement time:" + str((datetime.now() - self.start_timer)))
            data_output = {
                "measurement_time": str(datetime.now()),
                "config": measurement_state,
                "data": measurement_data,
            }
            for key, value in kwargs.items():
                data_output[key] = value
            data_output = OrderedDict(data_output)
            file_name = f"{filename}.json"
            with open(file_name, "w") as file_output:
                json.dump(data_output, file_output, indent=4)


class RoadmMonitor(Monitor):
    def __init__(self, roadm_object):
        super().__init__(roadm_object)

        self.roadm = self.device
        self.device_name = self.roadm.device_name
        self.device_model = "Lumentum ROADM-20 Whitebox"
        self.roadm_wss_channel_freq_center_start = 191350.0
        self.roadm_wss_channel_spacing = 50.0
        self.roadm_wss_channel_bw = 50.0

        ports = pd.read_csv("/etc/tcdona2/ports.csv", sep="\t", index_col="Name")
        self.p1_mux_port = int(ports.at["Lumentum_5_p1", "Out"])
        self.p1_demux_port = int(ports.at["Lumentum_5_p1", "In"])
        self.line_out_port = int(ports.at["Lumentum_5_line", "In"])
        self.line_in_port = int(ports.at["Lumentum_5_line", "Out"])

        # Loaded from Lumentum class
        self.roadm_wss_channel_attenuation_default = LUMENTUM_DEFAULT_WSS_LOSS
        self.roadm_wss_num_channel = LUMENTUM_CHANNEL_QUANTITY
        self.roadm_wss_channel_freq_center_list = LUMENTUM_WSS_CHANNEL_FREQ_CENTER_LIST
        self.start_timer = "NaN"
        self.monitor_flag = False

        self.wss_data = None

        print("Initializing ROADM Monitoring for %s..." % (self.device_name))

    def record_monitor_data(self, WAIT_TIME=3):

        self.start_timer = datetime.now()
        if WAIT_TIME != 0:
            print(
                "Waiting %s seconds for ROADM measurements to stabilize..."
                % str(WAIT_TIME)
            )
            time.sleep(WAIT_TIME)

        print("\nROADM measurements started at %s" % str(self.start_timer))
        self.ocm_data = self.roadm.wss_get_monitored_channels()
        self.wss_data = self.roadm.wss_get_connections()
        self.ports_info = self.roadm.get_ports_info()
        self.edfa_info = self.roadm.edfa_get_info()

        self.monitor_flag = True

    # Direct functions

    def mux_ocm_power(self):
        if self.ocm_data is None:
            self.record_monitor_data()

        return [
            (val["id"], val["power"])
            for key, val in self.ocm_data["mux"].items()
            if "id" in val
        ]

    def demux_ocm_power(self):
        if self.ocm_data is None:
            self.record_monitor_data()

        return [
            (val["id"], val["power"])
            for key, val in self.ocm_data["demux"].items()
            if "id" in val
        ]

    def decode_wss_return(self, wss_module, attribute, refresh=False):

        if self.wss_data is None or refresh == True:
            self.record_monitor_data()

        return [
            (val["id"], val[attribute])
            for key, val in self.wss_data[wss_module].items()
            if "id" in val
        ]

    def mux_input_power(self):
        return self.decode_wss_return("mux", "input-power")

    def mux_output_power(self):
        return self.decode_wss_return("mux", "output-power")

    def demux_input_power(self):
        return self.decode_wss_return("demux", "input-power")

    def demux_output_power(self):
        return self.decode_wss_return("demux", "output-power")

    def get_mux_atten(self):
        return self.decode_wss_return("mux", "attenuation")

    def get_demux_atten(self, refresh=False):
        return self.decode_wss_return("demux", "attenuation", refresh)

    def get_mux_open_channel_index(self):
        channel_status = self.decode_wss_return("mux", "blocked")
        return [channel for channel, status in channel_status if status == "false"]

    def get_demux_open_channel_index(self):
        channel_status = self.decode_wss_return("demux", "blocked")
        return [channel for channel, status in channel_status if status == "false"]

    def write_json_data(
        self,
        WAIT_TIME=3,
        measurement_label="",
        DATAPREFIX="./",
        debug=True,
        index="",
        repeat_index=1,
        label_measurement_setup=None,
        uid=None,
        get_only=False,
    ):

        print(measurement_label, "\n")
        self.record_monitor_data(WAIT_TIME=WAIT_TIME)
        data_output = OrderedDict()

        try:

            measurement_setup = OrderedDict(
                {
                    "device_model": self.device_model,
                    "date": str(self.start_timer),
                    "index": index,
                    "repeat_index": repeat_index,
                    "device_name": self.device_name,
                    "ip_address": self.roadm_ip_address,
                    "P1_MUX_port": self.p1_mux_port,
                    "P1_DEMUX_port": self.p1_demux_port,
                    "Line_Out_port": self.line_out_port,
                    "Line_In_port": self.line_in_port,
                    "roadm_wss_channel_attenuation_default": self.roadm_wss_channel_attenuation_default,
                    "roadm_wss_num_channel": self.roadm_wss_num_channel,
                    "roadm_wss_channel_freq_center_start": self.roadm_wss_channel_freq_center_start,
                    "roadm_wss_channel_spacing": self.roadm_wss_channel_spacing,
                    "roadm_wss_channel_bw": self.roadm_wss_channel_bw,
                    "roadm_wss_channel_freq_center_list": self.roadm_wss_channel_freq_center_list,
                }
            )

            data_output["uid"] = uid
            data_output["measurement_setup"] = measurement_setup
            data_output["measurement_setup_label"] = label_measurement_setup
            data_output["measurement_label"] = measurement_label

            cur_measurement_data = self.measurement_sweep(debug=debug)
            data_output["measurement_data"] = copy.deepcopy(cur_measurement_data)

        except KeyboardInterrupt:
            print("EDFA measurement canceled by user. Save already collected data.")
            raise KeyboardInterrupt

        except Exception as e:
            print(
                "EDFA measurement canceled by exception. Save already collected data."
            )
            print(e)
            raise RuntimeError

        finally:
            if get_only:
                return data_output
            else:
                file_name = (
                    "monitor_"
                    + str(self.device_name)
                    + "_index"
                    + str(index)
                    + "_uid"
                    + str(uid)
                )
                file_name_json = DATAPREFIX + file_name + ".json"
                # Write to file
                print(
                    "ROADM measurements finished at "
                    + str(datetime.now().strftime("%Y.%m.%d.%H.%M.%S"))
                )
                print(
                    "ROADM measurement time:" + str((datetime.now() - self.start_timer))
                )
                with open(file_name_json, "w") as file_output:
                    json.dump(data_output, file_output, indent=4)

    def measurement_sweep(self, debug=True):

        # Getting readings
        cur_measurement_data = {}

        cur_measurement_data["mux_additional_attn"] = self.roadm.mux_additional_attn
        cur_measurement_data["mux_channel_attenuation"] = self.get_mux_atten()
        cur_measurement_data[
            "mux_open_channel_index"
        ] = self.get_mux_open_channel_index()
        cur_measurement_data["mux_num_open_channels"] = len(
            cur_measurement_data["mux_open_channel_index"]
        )
        cur_measurement_data["mux_input_power"] = self.mux_input_power()
        cur_measurement_data["mux_output_power"] = self.mux_output_power()
        cur_measurement_data["mux_ocm_power"] = self.mux_ocm_power()

        cur_measurement_data["demux_additional_attn"] = self.roadm.demux_additional_attn
        cur_measurement_data["demux_channel_attenuation"] = self.get_demux_atten()
        cur_measurement_data[
            "demux_open_channel_index"
        ] = self.get_demux_open_channel_index()
        cur_measurement_data["demux_num_open_channels"] = len(
            cur_measurement_data["demux_open_channel_index"]
        )
        cur_measurement_data["demux_input_power"] = self.demux_input_power()
        cur_measurement_data["demux_output_power"] = self.demux_output_power()
        cur_measurement_data["demux_ocm_power"] = self.demux_ocm_power()

        booster_monitor, preamp_monitor = self.edfa_monitor().values()
        cur_measurement_data["booster"] = {
            "params": booster_monitor["params"],
            "input-power": booster_monitor["input-power"],
            "output-power": booster_monitor["output-power"],
            "gain-power": booster_monitor["gain-power"],
        }

        cur_measurement_data["preamp"] = {
            "params": preamp_monitor["params"],
            "input-power": preamp_monitor["input-power"],
            "output-power": preamp_monitor["output-power"],
            "gain-power": preamp_monitor["gain-power"],
        }

        cur_measurement_data["line_port"] = self.ports_info["3001"]
        cur_measurement_data["mux_input_port_1"] = self.ports_info["4101"]
        cur_measurement_data["demux_output_port_1"] = self.ports_info["5201"]

        if debug:
            cur_measurement_data["debug"] = self.roadm.debug_edfa()

        return cur_measurement_data

    def edfa_monitor(self):

        output_booster = self.mux_ocm_power()
        input_booster = self.mux_output_power()
        gain_booster = [
            round(outx[1] - inx[1], 2)
            for inx, outx in zip(input_booster, output_booster)
        ]
        params_booster = self.edfa_info["booster"]
        monitor_booster = {
            "params": params_booster,
            "input-power": self.flatten_power_tuple(input_booster),
            "output-power": self.flatten_power_tuple(output_booster),
            "gain-power": gain_booster,
        }

        input_preamp = self.demux_ocm_power()
        output_preamp = self.demux_input_power()
        gain_preamp = [
            round(outx[1] - inx[1], 2) for inx, outx in zip(input_preamp, output_preamp)
        ]
        params_preamp = self.edfa_info["preamp"]
        monitor_preamp = {
            "params": params_preamp,
            "input-power": self.flatten_power_tuple(input_preamp),
            "output-power": self.flatten_power_tuple(output_preamp),
            "gain-power": gain_preamp,
        }

        monitor_edfa = {"booster": monitor_booster, "preamp": monitor_preamp}

        return monitor_edfa

    def wss_monitor(self):

        mux_in = self.mux_input_power()
        mux_out = self.mux_output_power()
        mux_att = self.get_mux_atten()
        mux_ocm = self.mux_ocm_power()
        monitor_mux = {
            "input-power": self.flatten_power_tuple(mux_in),
            "output-power": self.flatten_power_tuple(mux_out),
            "attn-power": self.flatten_power_tuple(mux_att),
            "ocm-power": self.flatten_power_tuple(mux_ocm),
        }

        demux_in = self.demux_input_power()
        demux_out = self.demux_output_power()
        demux_att = self.get_demux_atten()
        demux_ocm = self.demux_ocm_power()
        monitor_demux = {
            "input-power": self.flatten_power_tuple(demux_in),
            "output-power": self.flatten_power_tuple(demux_out),
            "attn-power": self.flatten_power_tuple(demux_att),
            "ocm-power": self.flatten_power_tuple(demux_ocm),
        }

        monitor_wss = {"mux": monitor_mux, "demux": monitor_demux}

        return monitor_wss

    def plot_power(
        self,
        component,
        io="input",
        refresh=True,
        notebook=True,
        save=False,
        savepath=None,
        return_plot=False,
    ):

        if refresh == True or self.monitor_flag == False:
            self.record_monitor_data()

        if save == True:
            pass
        elif notebook == False:
            matplotlib.use("module://drawilleplot")
        assert component in ["all", "booster", "preamp", "mux", "demux"]

        if component in ["preamp", "booster"]:
            assert io in ("input", "output", "gain")
            power_levels = self.edfa_monitor()[component]["%s-power" % str(io)]
            plt.suptitle("%s-%s" % (component, io))
            plt.plot(power_levels)

        elif component in ["mux", "demux"]:
            assert io in ("input", "output", "ocm", "attn")
            power_levels = self.wss_monitor()[component]["%s-power" % str(io)]
            plt.suptitle("%s-%s" % (component, io))
            plt.plot(power_levels)

        elif component in ["all"]:

            monitor_wss = self.wss_monitor()

            fig, axs = plt.subplots(2, 3)
            fig.tight_layout()
            plt.gcf().set_size_inches(6, 4.5)
            axs[0, 0].plot(monitor_wss["mux"]["input-power"])
            axs[0, 0].set_title("Mux Input")
            axs[0, 1].plot(monitor_wss["mux"]["output-power"])
            axs[0, 1].set_title("Mux Output")
            axs[0, 2].plot(monitor_wss["mux"]["ocm-power"])
            axs[0, 2].set_title("Mux OCM")

            axs[1, 0].set_title("DeMux Output")
            axs[1, 0].plot(monitor_wss["demux"]["output-power"])
            axs[1, 1].set_title("DeMux Input")
            axs[1, 1].plot(monitor_wss["demux"]["input-power"])
            axs[1, 2].set_title("Demux OCM")
            axs[1, 2].plot(monitor_wss["demux"]["ocm-power"])
        if return_plot:
            return fig, axs
        elif not save and not return_plot:
            plt.show()
        elif save:
            plt.savefig(savepath)
        plt.close()

    # Auxiliary functions

    def flatten_power_tuple(self, list_of_tuples):
        return [i[1] for i in list_of_tuples]


class PolatisMonitor(Monitor):
    def __init__(self, device_object, patch_list=None):

        self.plts = device_object

        if patch_list is not None:
            self.patch_list = patch_list
            self.patch_set = {component for patch in patch_list for component in patch}

    def measurement_sweep():
        pass

    def measurement_state():
        pass


class ILAMonitor:
    def __init__(self, device=1):

        self.ila = ILA(device)
        print("ILA initialized...")

    @staticmethod
    def iterate_dict(dct, final, prefix=""):

        for key, value in dct.items():
            if prefix != "":
                key = prefix + "-" + key
            if isinstance(value, dict):
                final = ILAMonitor.iterate_dict(value, final, key)
            else:
                final[key] = value
        return final

    def get_edfa_measurement_v1(self, save=False, fname=""):

        filter = """
                <open-optical-device xmlns="http://org/openroadm/device">
                </open-optical-device>
                """

        for attempt in range(10):
            try:
                xml_data = str(self.ila.m.get(filter=("subtree", filter)))
            except Exception as e:
                print(e)
                print("Retrying... Attempt number %s" % str(attempt + 1))
            else:
                break
        else:
            print("All Attempts exhausted.. Please debug RPC")

        if save == True and fname != "":
            with open(fname, "w") as f:
                f.write(xml_data)

        parser = xmltodict.parse(xml_data)

        return parser

    def get_edfa_measurement_v2(self, save=False, fname=""):

        filter = """
                <open-optical-device xmlns="http://org/openroadm/device">
                </open-optical-device>
                """
        for attempt in range(10):
            try:
                xml_data = str(
                    self.ila.m.get_config(source="running", filter=("subtree", filter))
                )

            except Exception as e:
                print(e)
                print("Retrying... Attempt number %s" % str(attempt + 1))
            else:
                break
        else:
            print("All Attempts exhausted.. Please debug RPC")

        if save == True and fname != "":
            with open(fname, "w") as f:
                f.write(xml_data)

        parser = xmltodict.parse(xml_data)["nc:rpc-reply"]

        return parser

    def parse_measurement_v1(self, parser):

        # activeAlarms = parser['rpc-reply']['data']['active-alarm-list']['activeAlarms']
        # currentPmlist_ab = parser['rpc-reply']['data']['currentPmlist'][0]['currentPm']
        # currentPmlist_ba = parser['rpc-reply']['data']['currentPmlist'][1]['currentPm']

        open_optical_device = parser["nc:rpc-reply"]["data"]["open-optical-device"]

        ab_config = open_optical_device["optical-amplifier"]["amplifiers"]["amplifier"][
            0
        ]["config"]
        ab_state = open_optical_device["optical-amplifier"]["amplifiers"]["amplifier"][
            0
        ]["state"]

        ba_config = open_optical_device["optical-amplifier"]["amplifiers"]["amplifier"][
            1
        ]["config"]
        ba_state = open_optical_device["optical-amplifier"]["amplifiers"]["amplifier"][
            1
        ]["state"]

        ab_osc = open_optical_device["oscs"][0]["osc"]["osc-monitor"]
        ba_osc = open_optical_device["oscs"][1]["osc"]["osc-monitor"]

        ab_evoa = open_optical_device["evoas"][0]["evoa"]
        ba_evoa = open_optical_device["evoas"][1]["evoa"]

        state = {
            "operational-state": open_optical_device[0]["state"]["operational-state"]
        }

        ab_port1 = open_optical_device["ports"][1]
        ab_port2 = open_optical_device["ports"][2]

        ba_port1 = open_optical_device["ports"][0]
        ba_port2 = open_optical_device["ports"][3]

        measurement_ab = {}
        measurement_ab = self.iterate_dict(ab_config, measurement_ab)
        measurement_ab = self.iterate_dict(ab_state, measurement_ab)
        measurement_ab = self.iterate_dict(ab_osc, measurement_ab, "osc")
        measurement_ab = self.iterate_dict(ab_evoa, measurement_ab, "evoa")
        measurement_ab = self.iterate_dict(state, measurement_ab)
        measurement_ab = self.iterate_dict(ab_port1, measurement_ab, "ports1")
        measurement_ab = self.iterate_dict(ab_port2, measurement_ab, "ports2")

        measurement_ba = {}
        measurement_ba = self.iterate_dict(ba_config, measurement_ba)
        measurement_ba = self.iterate_dict(ba_state, measurement_ba)
        measurement_ba = self.iterate_dict(ba_osc, measurement_ba, "osc")
        measurement_ba = self.iterate_dict(ba_evoa, measurement_ba, "evoa")
        measurement_ba = self.iterate_dict(state, measurement_ba)
        measurement_ba = self.iterate_dict(ba_port1, measurement_ba, "ports1")
        measurement_ba = self.iterate_dict(ba_port2, measurement_ba, "ports2")

        return measurement_ab, measurement_ba

    def get_edfa_data_v2(self, parser):

        # activeAlarms = parser['rpc-reply']['data']['active-alarm-list']['activeAlarms']
        # currentPmlist_ab = parser['rpc-reply']['data']['currentPmlist'][0]['currentPm']
        # currentPmlist_ba = parser['rpc-reply']['data']['currentPmlist'][1]['currentPm']

        open_optical_device = parser["data"]["open-optical-device"]

        ab_config = open_optical_device["optical-amplifier"]["amplifiers"]["amplifier"][
            0
        ]["config"]
        ba_config = open_optical_device["optical-amplifier"]["amplifiers"]["amplifier"][
            1
        ]["config"]

        ab_osc = open_optical_device["oscs"][0]
        ba_osc = open_optical_device["oscs"][1]

        ab_evoa = open_optical_device["evoas"][0]
        ba_evoa = open_optical_device["evoas"][1]

        ab_port1 = open_optical_device["ports"][1]
        ab_port2 = open_optical_device["ports"][2]

        ba_port1 = open_optical_device["ports"][0]
        ba_port2 = open_optical_device["ports"][3]

        measurement_ab = {}
        measurement_ab = self.iterate_dict(ab_config, measurement_ab)
        measurement_ab = self.iterate_dict(ab_osc, measurement_ab, "osc")
        measurement_ab = self.iterate_dict(ab_evoa, measurement_ab, "evoa")
        measurement_ab = self.iterate_dict(ab_port1, measurement_ab, "ports1")
        measurement_ab = self.iterate_dict(ab_port2, measurement_ab, "ports2")

        measurement_ba = {}
        measurement_ba = self.iterate_dict(ba_config, measurement_ba)
        measurement_ba = self.iterate_dict(ba_osc, measurement_ba, "osc")
        measurement_ba = self.iterate_dict(ba_evoa, measurement_ba, "evoa")
        measurement_ba = self.iterate_dict(ba_port1, measurement_ba, "ports1")
        measurement_ba = self.iterate_dict(ba_port2, measurement_ba, "ports2")

        return measurement_ab, measurement_ba

    def interactive_mode(self, edfa):

        flag = True
        while flag:
            inp = input("Press Enter to nontinue, Any key to exit...")
            if inp == "":
                m_ab, m_ba = self.get_edfa_data()

                if edfa == "ab":
                    m_ = m_ab
                else:
                    m_ = m_ba

                for key, value in m_.items():
                    print(key, " : ", value)
                print("_" * 50)
            else:
                flag = False
                break

    def write_json_data(
        self, measurement_label, DATAPREFIX="", index="", repeat_index=1, uid=None
    ):

        self.start_timer = datetime.now()

        try:
            data_output = self.get_edfa_measurement_v1()
        except Exception as e:
            print("ILA measurement canceled by exception. Save already collected data.")
            print(e)
            raise RuntimeError

        finally:

            data_output["measurement_label"] = measurement_label
            data_output["time"] = str(self.start_timer)
            data_output["index"] = index
            data_output["uid"] = uid
            data_output["repeat_index"] = repeat_index

            file_name = "monitor_ILA_index" + str(index) + "_uid" + str(uid)
            file_name_json = DATAPREFIX + file_name + ".json"
            # Write to file
            print(
                "ILA measurement finished at "
                + str(datetime.now().strftime("%Y.%m.%d.%H.%M.%S"))
            )
            print("Measurement time:" + str((datetime.now() - self.start_timer)))
            with open(file_name_json, "w") as file_output:
                json.dump(data_output, file_output, indent=4)


class OSAMonitor:
    def __init__(self):

        self.osa = OSA()

        print("OSA Initialized...")

    def measurement_sweep(self):

        self.osa.osa_sweep()
        time.sleep(1)
        self.osa.osa_sweep()
        time.sleep(2)

        for attempt in range(3):
            try:
                data = self.osa.query("DQA?")
            except Exception as e:
                print(e)
                print("Attempt %s fail, retry..." % str(attempt + 1))
            else:
                newdata = data.rstrip("\r\n")
                return newdata
        else:
            print("Exiting, all attempts failed")
            raise Exception("OSA Sweep failed")

    def get_component_info(
        self, measurement_label="", index="", repeat_index=1, uid=None
    ):

        data_output = OrderedDict()

        data_output["device_name"] = self.osa.identify()
        data_output["measurement_label"] = measurement_label

        data_output["time"] = str(self.start_timer)
        data_output["index"] = index
        data_output["uid"] = uid
        data_output["repeat_index"] = repeat_index

        data_output["osa_reading"] = self.measurement_sweep()

        print(data_output["osa_reading"])
        return data_output

    def get_image(self, dir="/home/ajag", prefix=""):
        self.osa.get_image(dir=dir, prefix=prefix)

    def write_json_data(
        self, measurement_label, DATAPREFIX="", index="", repeat_index=1, uid=None
    ):

        self.start_timer = datetime.now()

        try:
            data_output = self.get_component_info(
                measurement_label=measurement_label,
                index=index,
                repeat_index=repeat_index,
                uid=uid,
            )

        except Exception as e:
            print("OSA measurement canceled by exception. Save already collected data.")
            print(e)
            raise RuntimeError

        else:
            file_name = "monitor_OSA_index" + str(index) + "_uid" + str(uid)
            file_name_json = DATAPREFIX + file_name + ".json"
            # Write to file
            print(
                "OSA measurement finished at "
                + str(datetime.now().strftime("%Y.%m.%d.%H.%M.%S"))
            )
            print("Measurement time:" + str((datetime.now() - self.start_timer)))
            with open(file_name_json, "w") as file_output:
                json.dump(data_output, file_output, indent=4)
