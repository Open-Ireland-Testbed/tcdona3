import time
import pyvisa
import numpy as np
import matplotlib.pyplot as plt
import json
from tcdona3.utils import *

class YokogawaOSA:
    def __init__(self, ip: str, port: int = 10001):
        self.ip = ip
        self.port = port
        self.osa = None
        self.rm = pyvisa.ResourceManager()

    def connect(self):

        if not check_patch_owners([("Yoko_OSA", "Yoko_OSA")]):
            raise Exception("You are not authorized to use this device")
        
        self.osa = self.rm.open_resource(f"TCPIP::{self.ip}::{self.port}::SOCKET")
        self.osa.read_termination = '\n'
        self.osa.write_termination = '\n'

        # Login sequence
        print(self.osa.query('open "anonymous"'))
        print(self.osa.query("aaa"))
        time.sleep(6)

        # Set command mode
        self.osa.write("CFORM1")
        time.sleep(6)

    def wait_for_sweep_complete(self):
        self.osa.write("*CLS; :init")
        time.sleep(6)
        while True:
            if self.osa.query(":stat:oper:even?").startswith("1"):
                break
        print("Sweep complete")

    def measure_spectrum_width(self):
        result = self.osa.query(":calc:cat swth; :calc; :calc:data?")
        print("Spectrum width:", result)
        return result

    def identify(self):
        idn = self.osa.query("*IDN?")
        print("Instrument ID:", idn)
        return idn

    def set_peak_search(self):
        self.osa.write(":CALCulate:MARKer:MAXimum")
        print("Set peak search complete")

    def get_data_logging_source(self):
        response = self.osa.query(":APPLication:DLOGging:LPARameter:ITEM?")
        print("Data Logging Source:", response)
        return response

    def get_resolution_bandwidth(self):
        rbw = self.osa.query(":SENSe:BANDwidth?")
        print("Current RBW:", rbw)
        return rbw

    def set_resolution_bandwidth(self, value="0.1NM"):
        self.osa.write(f":SENSe:BANDwidth {value}")
        print("Set resolution complete")

    def set_attenuator(self, status: bool):
        self.osa.write(f"ATTenuator {'ON' if status else 'OFF'}")
        print(f"Set attenuator {'ON' if status else 'OFF'}")

    def get_auto_measure_status(self):
        aut = self.osa.query(":CALCULATE:AUTO?")
        print("Auto Measure:", aut)
        return aut

    def set_auto_measure(self, status: bool):
        self.osa.write(f":CALCULATE:AUTO {'ON' if status else 'OFF'}")
        print(f"Set auto measure {'ON' if status else 'OFF'}")

    def set_center_wavelength(self, wavelength_nm: float):
        self.osa.write(f":DISPLAY:TRACE:X:CENTER {wavelength_nm:.3f}NM")
        print("Set X axis center wavelength complete")

    def set_wavelength_center(self, wavelength_nm: float):
        self.osa.write(f":SENSE:WAVELENGTH:CENTER {wavelength_nm:.3f}NM")
        print("Set wavelength center complete")

    def set_wavelength_span(self, span_nm: float):
        self.osa.write(f":SENSE:WAVELENGTH:SPAN {span_nm:.1f}NM")
        print("Set wavelength span complete")

    def set_wavelength_range(self, start_nm: float, stop_nm: float):
        self.osa.write(f":SENSE:WAVELENGTH:START {start_nm:.3f}NM")
        self.osa.write(f":SENSE:WAVELENGTH:STOP {stop_nm:.3f}NM")
        print("Set wavelength range complete")

    def set_auto_sweep_points(self, status: bool):
        self.osa.write(f":SENS:SWEeP:POINTS:AUTO {'ON' if status else 'OFF'}")
        print(f"{'Enable' if status else 'Disable'} automatic sweep points complete")

    def set_sweep_points(self, points: int):
        self.osa.write(f":SENSe:SWEep:SEGMent:POINts {points}")
        print("Set the sweep points for the segment complete")

    def set_sweep_mode(self, mode: str):
        mode = mode.upper()
        if mode not in ['AUTO', 'REPEAT', 'SINGLE']:
            raise ValueError("Invalid sweep mode")
        self.osa.write(f":INITiate:SMODe {mode}")
        print(f"Set sweep mode to {mode}")

    def abort(self):
        self.osa.write(":ABORt")
        print("Aborted current operation")

    def store_memory(self, name="test001", location="INTERNAL"):
        self.osa.write(f':MMEMORY:STORE:MEMORY 1,CSV,"{name}",{location}')
        print("Memory stored")

    def store_trace(self, name="test001", location="INTERNAL"):
        self.osa.write(f':MMEMORY:STORE:TRACE TRA,CSV,"{name}",{location}')
        print("Trace stored")

    def store_graphics(self, name="test001", location="INTERNAL"):
        self.osa.write(f':MMEMORY:STORE:GRAPHICS COLOR,BMP,"{name}",{location}')
        print("Graphics stored")

    def get_trace_data(self,trace_name="TRA"):
        data=self.osa.query(f":TRACE:Y? {trace_name}")
        print(f"Get {trace_name} complete")
        return data
    
    def sweep(self):
        self.osa.write("INIT:IMM")
        print("sweep complete")

    def get_wavelength_data(self,trace_name="TRA"):
        data=self.osa.query(f":TRACE:X? {trace_name}")
        print(f"Get {trace_name} wavelength complete")
        return data

    def save_trace_data(osa,json_path="trace.json",fig_path="spectrum.png"):
        """
        Save trace and wavelength data from YOKOGAWA OSA to a JSON file,
        and save optical spectrum plot as PNG.

        Parameters:
        - osa: PyVISA instrument object, already connected
        - json_path: Path to save JSON file
        - fig_path: Path to save spectrum figure
        """
        # Read trace and wavelength data
        trace_data = osa.get_trace_data(trace_name="TRA")
        print("Get traceA complete")

        wavelength_data = osa.get_wavelength_data(trace_name="TRA")
        print("Get wavelength complete")

        # Parse and save JSON
        data = {
            "wavelength": wavelength_data.split(","),
            "trace": trace_data.split(",")
        }

        with open(json_path, "w") as file:
            json.dump(data, file, indent=4)
        print(f"Save JSON data complete: {json_path}")

        # Convert to float list
        trace_values = [float(x) for x in data["trace"]]
        wavelength_values = [float(x) for x in data["wavelength"]]

        # Convert wavelength to frequency
        # speed_of_light = 2.998e8  # m/s
        # frequency_values = speed_of_light / np.array(wavelength_values) * 1e9  # Convert to GHz
        # interpolated_trace_values = np.interp(frequency_values, np.sort(wavelength_values), trace_values)

        # Plot and save figure
        plt.figure()
        plt.plot(wavelength_values, trace_values)
        # plt.plot(frequency_values, interpolated_trace_values)
        plt.xlabel("Wavelength (nm)")
        plt.ylabel("Power (dBm)")
        plt.title("Optical Spectrum")
        plt.grid(True)
        plt.savefig(fig_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Save spectrum figure complete: {fig_path}")

        return wavelength_values, trace_values
    
if __name__ == '__main__':

    def collect_osaYOKOGAWA_data(i):

        osa = YokogawaOSA(ip="10.10.10.215")
        time.sleep(1)
        osa.connect()
        time.sleep(2)
        osa.identify()
        # osa.set_sweep_mode("SINGLE")  # Sweep remains the same
        time.sleep(6) 
        osa.sweep()
        time.sleep(6)
        # osa.set_wavelength_range(wave_start,wave_stop)
        wavelength, trace = osa.save_trace_data(f"YOKOGAWA_trace_data_{i}.json", f"osaYOKOGAWA_sweep_{i}.png")
        
        return wavelength, trace

    i = 1
    wavelength_YOKOGAWA, trace_YOKOGAWA = collect_osaYOKOGAWA_data(i)
