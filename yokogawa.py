import time
import pyvisa
import numpy as np
import matplotlib.pyplot as plt
import json
from tcdona3.utils import *

#: Preset resolutions of the AQ6370E [nm]; the instrument rounds any other value to one.
RESOLUTIONS_NM = (0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0)
WAVELENGTH_RANGE_NM = (600.0, 1700.0)
SAMPLING_POINTS_RANGE = (101, 100001)
MIN_SAMPLING_STEP_NM = 0.001
AVERAGE_COUNT_RANGE = (1, 999)

#: Vendor labels (lower case) -> command argument, and query answer -> label (AQ6370E manual, section 5.9).
SENSITIVITY = {"nhld": "NHLD", "naut": "NAUT", "normal": "NORMal", "mid": "MID", "high1": "HIGH1",
               "high2": "HIGH2", "high3": "HIGH3", "mid_smsr": "MIDSMSR", "high1_smsr": "HIGH1SMSR"}
SENSITIVITY_RESPONSE = {"0": "nhld", "1": "naut", "2": "mid", "3": "high1", "4": "high2", "5": "high3",
                        "6": "normal", "20": "mid_smsr", "21": "high1_smsr"}
SWEEP_SPEED = {"1x": "1x", "2x": "2x"}
SWEEP_SPEED_RESPONSE = {"0": "1x", "1": "2x"}
CHOPPER = {"off": "OFF", "switch": "SWITch"}
CHOPPER_RESPONSE = {"0": "off", "2": "switch"}
MEDIUM = {"air": "AIR", "vacuum": "VACuum"}
MEDIUM_RESPONSE = {"0": "air", "1": "vacuum"}
TRACE_ATTRIBUTE = {"write": "WRITe", "fix": "FIX", "max": "MAX", "min": "MIN", "ravg": "RAVG", "calc": "CALC"}
TRACE_ATTRIBUTE_RESPONSE = {"0": "write", "1": "fix", "2": "max", "3": "min", "4": "ravg", "5": "calc"}
SWEEP_MODE_RESPONSE = {"1": "single", "2": "repeat", "3": "auto", "SINGLE": "single", "REPEAT": "repeat",
                       "AUTO": "auto"}


def _number(name, value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError("%s must be a number, not %r." % (name, value))
    if number != number or number in (float("inf"), float("-inf")):
        raise ValueError("%s must be a finite number, not %r." % (name, value))
    return number


def _integer(name, value):
    number = _number(name, value)
    if number != int(number):
        raise ValueError("%s must be an integer, not %r." % (name, value))
    return int(number)


def _in_range(name, value, low, high, unit=""):
    if not low <= value <= high:
        raise ValueError("%s %s %s is outside [%s, %s]." % (name, value, unit, low, high))
    return value


def _flag(name, value):
    if not isinstance(value, bool):
        raise ValueError("%s must be True or False, not %r." % (name, value))
    return value


def _label(name, value, labels):
    label = str(value).lower()
    if label not in labels:
        raise ValueError("Invalid %s %r; allowed: %s." % (name, value, ", ".join(labels)))
    return label


def _on_off(value):
    return "ON" if value else "OFF"


class YokogawaOSA:
    """Yokogawa AQ6370E optical spectrum analyser over its remote socket (pyvisa).

    The methods recorded in tcdona3 keep their behaviour: they write the command and print a confirmation,
    and the getters return the raw answer (wavelengths in metres). The typed methods added in 1.1.0
    (``get_*``/``set_*`` below the sweep section, ``read_trace``) validate their value before writing, take
    and return nm, and print nothing.

    Note the two centre wavelengths: :meth:`set_center_wavelength` moves the *display* centre
    (``:DISPLAY:TRACE:X:CENTER``), :meth:`set_wavelength_center` and :meth:`get_center_wavelength` the
    *measurement* centre (``:SENSe:WAVelength:CENTer``).

    :param ip: The instrument address.
    :param port: The remote socket port.
    :param timeout_ms: Optional VISA timeout of every reply [ms] (default: pyvisa's, 2 s); a long trace may need
        more.
    :param user: The remote-socket user of the login.
    :param password: The remote-socket password of the login.
    """

    def __init__(self, ip: str, port: int = 10001, timeout_ms=None, user="anonymous", password="aaa"):
        self.ip = ip
        self.port = port
        self.osa = None
        self.rm = pyvisa.ResourceManager()
        self.timeout_ms = timeout_ms
        self._user = user
        self._password = password

    def connect(self):

        if not check_patch_owners([("Yoko_OSA", "Yoko_OSA")]):
            raise Exception("You are not authorized to use this device")
        
        self.osa = self.rm.open_resource(f"TCPIP::{self.ip}::{self.port}::SOCKET")
        self.osa.read_termination = '\n'
        self.osa.write_termination = '\n'
        if self.timeout_ms is not None:
            self.osa.timeout = self.timeout_ms

        # Login sequence
        print(self.osa.query('open "%s"' % self._user))
        print(self.osa.query(self._password))
        time.sleep(6)

        # Set command mode
        self.osa.write("CFORM1")
        time.sleep(6)

    def close(self):
        """Close the instrument session and the resource manager; closing never raises."""
        for handle in (self.osa, self.rm):
            try:
                if handle is not None:
                    handle.close()
            except Exception as error:
                print("closing the Yokogawa session: %s" % error)
        self.osa = None

    def __enter__(self):
        if self.osa is None:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def _sweep_complete(self):
        """Whether bit 0 (sweep complete) of the operation event register is set."""
        answer = str(self.osa.query(":stat:oper:even?")).strip()
        try:
            return int(float(answer)) & 1 == 1
        except ValueError:
            return False

    def wait_for_sweep_complete(self, timeout_s=None, poll_s=0.2):
        """Start a sweep and wait until the instrument reports it complete (bit 0 of the operation event
        register).

        :param timeout_s: Optional limit [s]; past it a TimeoutError is raised. Default: wait as long as it takes.
        :param poll_s: The wait between two polls [s].
        """
        self.osa.write("*CLS; :init")
        time.sleep(6)
        deadline = None if timeout_s is None else time.time() + timeout_s
        while not self._sweep_complete():
            if deadline is not None and time.time() > deadline:
                raise TimeoutError("the Yokogawa sweep did not complete within %s s" % timeout_s)
            time.sleep(poll_s)
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
        """Kept for compatibility: the AQ6370E documents no attenuator command, so the instrument may ignore it."""
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
        self.osa.write(f":SENSE:WAVELENGTH:SPAN {span_nm:.3f}NM")
        print("Set wavelength span complete")

    def set_wavelength_range(self, start_nm: float, stop_nm: float):
        self.osa.write(f":SENSE:WAVELENGTH:START {start_nm:.3f}NM")
        self.osa.write(f":SENSE:WAVELENGTH:STOP {stop_nm:.3f}NM")
        print("Set wavelength range complete")

    def set_auto_sweep_points(self, status: bool):
        self.osa.write(f":SENS:SWEeP:POINTS:AUTO {'ON' if status else 'OFF'}")
        print(f"{'Enable' if status else 'Disable'} automatic sweep points complete")

    def set_sweep_points(self, points: int):
        """Set the points of a *segment* measurement (``:SENSe:SWEep:SEGMent:POINts``). For the points of a
        normal sweep use :meth:`set_sampling_points`."""
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
        plt.xlabel("Wavelength (m)")
        plt.ylabel("Power (dBm)")
        plt.title("Optical Spectrum")
        plt.grid(True)
        plt.savefig(fig_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Save spectrum figure complete: {fig_path}")

        return wavelength_values, trace_values

    # ------------------------------------------------------------------ typed readers (added in 1.1.0)
    def _query(self, scpi):
        return str(self.osa.query(scpi)).strip()

    def _query_nm(self, scpi):
        """A wavelength node (the instrument answers in metres), in nm."""
        return float(self._query(scpi)) * 1e9

    def _query_flag(self, scpi):
        return self._query(scpi) == "1"

    def _query_label(self, scpi, responses):
        raw = self._query(scpi)
        if raw not in responses:
            raise ValueError("Unexpected response %r to %s." % (raw, scpi))
        return responses[raw]

    def get_center_wavelength(self):
        """The measurement centre wavelength [nm]."""
        return self._query_nm(":SENSe:WAVelength:CENTer?")

    def get_span(self):
        """The measurement span [nm]."""
        return self._query_nm(":SENSe:WAVelength:SPAN?")

    def get_start_wavelength(self):
        return self._query_nm(":SENSe:WAVelength:STARt?")

    def get_stop_wavelength(self):
        return self._query_nm(":SENSe:WAVelength:STOP?")

    def get_resolution(self):
        """The resolution bandwidth [nm]."""
        return self._query_nm(":SENSe:BANDwidth:RESolution?")

    def get_resolution_correction(self):
        return self._query_flag(":SENSe:SETTing:CORRection?")

    def get_sensitivity(self):
        return self._query_label(":SENSe:SENSe?", SENSITIVITY_RESPONSE)

    def get_chopper(self):
        return self._query_label(":SENSe:CHOPper?", CHOPPER_RESPONSE)

    def get_sweep_speed(self):
        return self._query_label(":SENSe:SWEep:SPEed?", SWEEP_SPEED_RESPONSE)

    def get_sweep_mode(self):
        """'single', 'repeat' or 'auto'."""
        return SWEEP_MODE_RESPONSE.get(self._query(":INITiate:SMODe?").upper(), "single")

    def get_average_count(self):
        return int(float(self._query(":SENSe:AVERage:COUNt?")))

    def get_sampling_auto(self):
        return self._query_flag(":SENSe:SWEep:POINts:AUTO?")

    def get_sampling_points(self):
        """The number of points of a sweep, also when the instrument picks it."""
        return int(float(self._query(":SENSe:SWEep:POINts?")))

    def get_sampling_step(self):
        """The sampling interval [nm], ``span / (points - 1)`` (the instrument's step query rounds to 1 pm)."""
        return self.get_span() / (self.get_sampling_points() - 1)

    def get_smoothing(self):
        return self._query_flag(":SENSe:SETTing:SMOothing?")

    def get_level_offset(self):
        """The level offset the instrument adds to every level [dB]."""
        return float(self._query(":SENSe:CORRection:LEVel:SHIFt?"))

    def get_reference_level(self):
        """The reference level [dBm]."""
        return float(self._query(":DISPlay:TRACe:Y1:SCALe:RLEVel?"))

    def get_wavelength_offset(self):
        """The wavelength offset the instrument adds to every wavelength [nm]."""
        return self._query_nm(":SENSe:CORRection:WAVelength:SHIFt?")

    def get_wavelength_medium(self):
        """'air' or 'vacuum', the medium the wavelengths refer to."""
        return self._query_label(":SENSe:CORRection:RVELocity:MEDium?", MEDIUM_RESPONSE)

    def get_trace_attribute(self, trace_name="TRA"):
        return self._query_label(":TRACe:ATTRibute:%s?" % trace_name, TRACE_ATTRIBUTE_RESPONSE)

    def get_auto_measure(self):
        return self._query_flag(":CALCulate:AUTO?")

    def read_trace(self, trace_name="TRA", timeout_s=None, poll_s=0.2):
        """Run a single sweep and read the trace.

        :return: ``(wavelength [nm], power [dBm])``, two NumPy arrays.
        """
        self.osa.write(":init:smode 1")
        self.osa.write("*CLS")
        self.osa.write(":init")
        deadline = None if timeout_s is None else time.time() + timeout_s
        while not self._sweep_complete():
            if deadline is not None and time.time() > deadline:
                raise TimeoutError("the Yokogawa sweep did not complete within %s s" % timeout_s)
            time.sleep(poll_s)
        x = [float(v) for v in str(self.osa.query("TRAC:DATA:X? %s" % trace_name)).split(",")]
        y = [float(v) for v in str(self.osa.query("TRAC:DATA:Y? %s" % trace_name)).split(",")]
        return np.array(x) * 1e9, np.array(y)

    # ------------------------------------------------------------------ typed setters (added in 1.1.0)
    def _wavelength(self, name, value):
        return _in_range(name, _number(name, value), WAVELENGTH_RANGE_NM[0], WAVELENGTH_RANGE_NM[1], "nm")

    def set_span(self, span_nm):
        """Set the measurement span [nm], validated."""
        low, high = WAVELENGTH_RANGE_NM
        value = _in_range("span", _number("span", span_nm), 0.0, high - low, "nm")
        self.osa.write(":SENSe:WAVelength:SPAN %.3fNM" % value)

    def set_start_wavelength(self, wavelength_nm):
        self.osa.write(":SENSe:WAVelength:STARt %.3fNM" % self._wavelength("start wavelength", wavelength_nm))

    def set_stop_wavelength(self, wavelength_nm):
        self.osa.write(":SENSe:WAVelength:STOP %.3fNM" % self._wavelength("stop wavelength", wavelength_nm))

    def set_resolution(self, resolution_nm):
        """Set the resolution bandwidth [nm]: one of the presets (0.02, 0.05, 0.1, 0.2, 0.5, 1, 2)."""
        value = _number("resolution", resolution_nm)
        for preset in RESOLUTIONS_NM:
            if abs(value - preset) <= 1e-6:
                self.osa.write(":SENSe:BANDwidth:RESolution %.3fNM" % preset)
                return
        raise ValueError("resolution %s nm is not a preset of the instrument (%s nm)."
                         % (value, ", ".join("%g" % r for r in RESOLUTIONS_NM)))

    def set_resolution_correction(self, enabled):
        self.osa.write(":SENSe:SETTing:CORRection %s" % _on_off(_flag("resolution correction", enabled)))

    def set_sensitivity(self, mode):
        self.osa.write(":SENSe:SENSe %s" % SENSITIVITY[_label("sensitivity", mode, SENSITIVITY)])

    def set_chopper(self, mode):
        self.osa.write(":SENSe:CHOPper %s" % CHOPPER[_label("chopper", mode, CHOPPER)])

    def set_sweep_speed(self, speed):
        self.osa.write(":SENSe:SWEep:SPEed %s" % SWEEP_SPEED[_label("sweep speed", speed, SWEEP_SPEED)])

    def set_average_count(self, count):
        value = _in_range("average count", _integer("average count", count), *AVERAGE_COUNT_RANGE)
        self.osa.write(":SENSe:AVERage:COUNt %d" % value)

    def set_sampling_points(self, points):
        """Fix the number of points of a sweep; the instrument turns automatic sampling off."""
        value = _in_range("sampling points", _integer("sampling points", points), *SAMPLING_POINTS_RANGE)
        self.osa.write(":SENSe:SWEep:POINts %d" % value)

    def set_sampling_step(self, step_nm):
        """Fix the sampling interval [nm], a multiple of 1 pm; the instrument turns automatic sampling off."""
        step = _number("sampling step", step_nm)
        if step < MIN_SAMPLING_STEP_NM:
            raise ValueError("sampling step %s nm is below the minimum %s nm." % (step, MIN_SAMPLING_STEP_NM))
        quanta = step / MIN_SAMPLING_STEP_NM
        if abs(quanta - round(quanta)) > 1e-6:
            raise ValueError("sampling step %s nm is not a multiple of %s nm." % (step, MIN_SAMPLING_STEP_NM))
        self.osa.write(":SENSe:SWEep:STEP %.4fNM" % (round(quanta) * MIN_SAMPLING_STEP_NM))

    def set_smoothing(self, enabled):
        self.osa.write(":SENSe:SETTing:SMOothing %s" % _on_off(_flag("smoothing", enabled)))

    def set_level_offset(self, offset_db):
        self.osa.write(":SENSe:CORRection:LEVel:SHIFt %.3fDB" % _number("level offset", offset_db))

    def set_reference_level(self, level_dbm):
        self.osa.write(":DISPlay:TRACe:Y1:SCALe:RLEVel %sDBM" % _number("reference level", level_dbm))

    def set_wavelength_offset(self, offset_nm):
        self.osa.write(":SENSe:CORRection:WAVelength:SHIFt %.3fNM" % _number("wavelength offset", offset_nm))

    def set_wavelength_medium(self, medium):
        self.osa.write(":SENSe:CORRection:RVELocity:MEDium %s" % MEDIUM[_label("wavelength medium", medium, MEDIUM)])

    def set_trace_attribute(self, attribute, trace_name="TRA"):
        """Set the attribute of a trace (it also becomes the active trace)."""
        label = _label("trace attribute", attribute, TRACE_ATTRIBUTE)
        self.osa.write(":TRACe:ATTRibute:%s %s" % (trace_name, TRACE_ATTRIBUTE[label]))

    def sweep_single(self):
        self.osa.write(":INITiate:SMODe SINGLE")
        self.osa.write(":init")

    def sweep_repeat(self):
        self.osa.write(":INITiate:SMODe REPEAT")
        self.osa.write(":init")

    def sweep_stop(self):
        self.osa.write(":ABORt")


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
