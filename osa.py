import pyvisa
import matplotlib.pyplot as plt
from tcdona3.utils import check_patch_owners


class OSA:

    """Class to interface with the Anritsu Optical Spectrum Analyzer (OSA). The Anritsu OSA is connected to the testbed by GPIB interface. Hence, the class uses the pyvisa library to interact with the OSA. Initialize the OSA. This method opens the resource manager and connects to the OSA.

    It also checks if the user is authorized to use the device. If the user is not authorized, it raises an Exception and does not connect to the device."""

    def __init__(self):

        if check_patch_owners([("anritsu_osa", "anritsu_osa")]):
            rm = pyvisa.ResourceManager()
            self.osa = rm.open_resource("GPIB0::8::INSTR")
        else:
            raise Exception("You are not authorized to use this device.")

    def query(self, cmd):
        """Query the OSA with the given command and return the response. This command is for GET queries only. All useful queries are already implemented as methods in the class. Please refer the documentation to implement new queries."

        :param cmd: Command to query the OSA
        :type cmd: str

        :return: Response from the OSA
        """
        return self.osa.query(cmd)

    def write(self, cmd):
        """Write the command to the OSA. This command is for SET queries only. All useful queries are already implemented as methods in the class. Please refer the documentation to implement new queries.

        :param cmd: Command to write to the OSA
        :type cmd: str

        :return: None
        """
        return self.osa.write(cmd)

    def identify(self):
        """Get the identification of the OSA.

        :return: Identification name of the OSA
        :rtype: str
        """
        # Information about device. Can also use ("OPT?")
        return self.osa.query("*IDN?")

    def reset_device(self):
        """Reset the device to the default settings at level 3. This will reset all the settings and data in the device."""
        # Resetting of device at level3
        inp = input("Are you sure..?")
        if inp.upper() in ("Y", "YES"):
            self.osa.write("*RST")

    def self_test(self):
        """Perform the self test on the device and return the status of the test."""
        # Self Test and error detection inside the device
        return self.osa.query("*TST?")

    def get_peak_search(self):
        """Detects the spectrum peak and moved the trace marker there.

        :return: PEAK: Detects the main peak whose level is highest, and moves the trace marker there, NEXT: Detects the peak whose level is the second highest compared with the current peak, LAST: Detects the peak whose level is the second lowest compared with the current peak, and moves the trace marker there.
        :rtype: str
        """
        return self.osa.query("PKS?")

    def set_peak_search(self):
        """Detects the spectrum peak and moved the trace marker there."""

        self.osa.write("PKS PEAK")
        return

    def get_num_peaks_multipeak_counter(self):
        """Reads the number of peaks according to the multipeak detection results.

        :return: Number of peaks
        :rtype: str
        """

        # Reads the number of peaks according to the multipeak detection results
        # Output: Returns the number of peaks.
        return self.osa.query("APR? MPKC")

    def get_resolution(self):
        """Reads the current measurement resolution.

        :return: Resolution of OSA in nanometres till third decimal place
        :rtype: str
        """

        # Reads the actual resolution data
        # Output: Resolution of OSA in nm till third decimal place
        return self.osa.query("ARED?")

    def set_resolution(self, resolution):
        """Sets the resolution of the OSA in nm.

        :param resolution: Resolution of OSA in nanometres till third decimal place
        :type resolution: str

        :return: None
        """

        self.osa.write(f"RES {resolution}")
        return

    def get_attn_status(self):
        """Read the status of the internal optical attenuator. Anritsu MS9710C has an internal optical attenuator that can be turned on or off. The VOA must be turned on to measure high power signals."""

        return self.osa.query("ATT?")

    def set_attn_status(self, status):
        """Set the status of the internal optical attenuator. Anritsu MS9710C has an internal optical attenuator that can be turned on or off. The VOA must be turned on to measure high power signals. Setting the attenuator to “ON” will increase the noise floor by 20 dB.

        :param status: 'ON' or 'OFF' -> Turn the internal optical attenuator On/Off
        :type status: str

        :return: None"""

        # Turn the internal optical attenuator On/Off
        if status.upper() == "ON":
            self.osa.write("ATT s")
        else:
            self.osa.write("ATT ")

    def get_auto_measure(self):
        """Carries out auto measurement. The wavelength and resolution are automatically set for the incident light spectrum.

        :return: 0: Measurement has been completed, 1: Measurement is being carried out
        :rtype: str
        """

        return self.osa.query("AUT?")

    def set_auto_measure(self):
        """Carries out auto measurement. The wavelength and resolution are automatically set for the incident light spectrum."""

        # Carries out auto measurement. The wavelength anf resolution are automatically set for the incident light spectrum

        self.osa.write("AUT")
        return

    def get_wavelength_centre(self):
        """Reads the center wavelength (nm) of the OSA.

        :return: Center wavelength in nm
        :rtype: str
        """

        # Output: Returns the center wavelength
        return self.osa.query("CNT?")

    def set_wavelength_centre(self, wavelength=1350.00):
        """Sets the center wavelength (nm) of the OSA.

        :param wavelength: Centre wavelength in nm (upto second decimal place) 600.00<=wavelength<=1750.00. Default = 1350.00 nm
        :type wavelength: str

        :return: None
        """

        cmd = "CNT %s" % str(wavelength)
        self.osa.write(cmd)
        return

    def get_wavelength_span(self):
        """Reads the span of the OSA (nm).

        :return: Span of the OSA in nm
        :rtype: str
        """

        return self.osa.query("SPN?")

    def get_wavelength_start(self):
        """Reads the start wavelength (nm) of the OSA.

        :return: Start wavelength in nm
        :rtype: str
        """

        return self.osa.query("STA?")

    def get_wavelength_stop(self):
        """Reads the stop wavelength (nm) of the OSA.

        :return: Stop wavelength in nm
        :rtype: str
        """

        return self.osa.query("STO?")

    def get_wavelength_mkv(self):
        """Converts the trace marker, the delta marker, and the wavelength values obtained from a part of the analysis into frequencies and then displays them.

        :return: Wavelength in nm
        :rtype: str
        """

        return self.osa.query("MKV?")

    def get_sampling_points(self):
        """Get number of sampling points

        :return: Number of sampling points
        :rtype: str
        """

        return self.osa.query("MPT?")

    def set_sampling_points(self, n):
        """Set number of sampling points. Only following values are supported: [51,101,251,501,1001,2001,5001]

        :param n: Number of sampling points
        :type n: int

        :return: None
        """

        cmd = "MPT %s" % str(int(n))
        self.osa.write(cmd)
        return

    def get_marker_tmk(self):
        """Reads the trace marker value.

        :return: Trace marker value
        :rtype: str
        """

        return self.osa.query("TMK?")

    def set_peak_centre(self):
        """Sets the spectrum peak wavelength as a center wavelength"""

        # Sets the spectrum peak wavelength as a center wavelength
        self.osa.write("PKC")
        return

    def set_tmkr_centre(self):
        """Sets the trace marker value as a center wavelength"""

        self.osa.write("TMC")
        return

    def sweep_single(self):
        """Performs a single sweep of the OSA. This method does not return anything. However, the sweep is stored in the memory of the OSA."""

        self.osa.write("SSI")

    def sweep_repeat(self):
        """Performs a repeated sweep of the OSA. This method does not return anything. However, the sweep is stored in the memory of the OSA."""

        self.osa.write("SRT")

    def sweep_stop(self):
        """Force stops the sweep of the OSA. This method does not return anything."""

        self.osa.write("SST")

    def osa_sweep(self):

        """Perform a sweep of the OSA. This method is used to perform a sweep of the OSA. This method does not return anything. However, the sweep is stored in the memory of the OSA."""

        self.sweep_single()
        self.set_peak_search()
        print("Sweep OSA - start")
        counter = 0
        while True:
            try:
                data = int(self.osa.query("ESR2?"))
                break
            except:
                print("Something went wrong during the sweep. Retry: ", counter + 1)
                counter += 1
                if counter == 3:
                    data = 0
                    break

        counter = 0
        while data != 3:  # more info on page 9-58 and 8-16 of the manual: ms9710c.pdf
            while True:
                try:
                    data = int(self.osa.query("ESR2?"))
                    break
                except:
                    print("Something went wrong during the sweep. Retry: ", counter + 1)
                    counter += 1
                    if counter == 3:
                        data = 0
                        break
            pass
        print("Sweep OSA - end")

    def get_peak_numbers(self):

        """Get the total number of peaks

        :return: Total number of peaks
        :rtype: str

        """

        self.osa_sweep()
        n = self.osa.query("APR? MPKC")
        return n

    def auto_measure(self):

        """Perform the automeasure function of the OSA. This method does not return anything. However, the sweep is stored in the memory of the OSA."""

        # Description: Perform the automeasure function of the OSA.

        print("Automeasure OSA - start")
        # It returns 0 when it ends the automeasure, 1 otherwise.
        while int(self.osa.query("AUT?")) != 0:
            pass
        print("Automeasure OSA - end")

    def get_sweep_data(self, memory="A"):

        """Performs a sweep of the OSA and returns the measurement. The data is returned as a list of power level [dbm] of each sampled data point from start lambda to end lambda. The default memory is A.

        :param memory: 'A' or 'B' -> it reads form memory A or B. (A default)
        :type memory: str

        :return: Power level [dbm] of each sampled data point from start lambda to end lambda. Example: -77.65,-120,-77.01,-120,-78.43, ...
        :rtype: list
        """

        self.osa_sweep()
        self.osa_sweep()
        # time.sleep(0.5)
        counter = 0
        while True:
            try:
                data = self.osa.query("DQ" + memory + "?")
                break
            except:
                print(
                    "Something went wrong during the reading of memory A. Retry: ",
                    counter + 1,
                )
                counter += 1
                if counter == 3:
                    data = 0
                    break
        newdata = data.rstrip("\r\n")
        return newdata.split(",")

    def get_data(self, memory="A"):

        """Get the data from the screen of the OSA stored into the memory. The data is returned as a list of power level [dbm] of each sampled data point from start lambda to end lambda. The default memory is A.

        :param memory: 'A' or 'B' -> it reads form memory A or B. (A default)
        :type memory: str

        :return: Power level [dbm] of each sampled data point from start lambda to end lambda. Example: -77.65,-120,-77.01,-120,-78.43, ...
        :rtype: list
        """

        counter = 0
        while True:
            try:
                data = self.osa.query("DQ" + memory + "?")
                break
            except:
                print(
                    "Something went wrong during the reading of memory A. Retry: ",
                    counter + 1,
                )
                counter += 1
                if counter == 3:
                    data = 0
                    break
        newdata = data.rstrip("\r\n")
        return newdata.split(",")

    # def osa_get_data_screen_dashboard(self, memory="A"):

    #     # Description: Get the data from the screen of the OSA stored into the memory.
    #     # INPUT
    #     # memory (string) = 'A' or 'B' -> it reads form memory A or B. (A default)
    #     # OUTPUT
    #     # data (list) = power level [dbm] of each sampled data point from start lambda to end lambda. Example: -77.65,-120,-77.01,-120,-78.43, ...

    #     # time.sleep(0.5)
    #     counter = 0
    #     while True:
    #         try:
    #             data = self.osa.query("DQ" + memory + "?")
    #             break
    #         except:
    #             print(
    #                 "Something went wrong during the reading of memory A. Retry: ",
    #                 counter + 1,
    #             )
    #             counter += 1
    #             if counter == 3:
    #                 data = 0
    #                 break
    #     newdata = data.rstrip("\r\n")
    #     return newdata.split(",")

    def get_image(self, dir, prefix="image"):

        """Performs a sweep of the OSA and save the measurement as an image. The image is saved as a .png file.

        :param dir: Directory to save the image
        :type dir: str

        :param prefix: Prefix of the image file. Default is 'image'
        :type prefix: str

        :return: None
        """

        self.osa_sweep()
        self.osa_sweep()
        data = self.get_data()
        data = [float(i) for i in data]

        plt.plot(data)
        if dir[-1] != "/":
            dir += "/"
        plt.savefig(f"{dir}/{prefix}.png")
        plt.close()

    def get_csv(self, dir, prefix="data"):

        """Performs a sweep of the OSA and save the measurement as a .csv file.

        :param dir: Directory to save the csv file
        :type dir: str

        :param prefix: Prefix of the csv file. Default is 'data'
        :type prefix: str

        :return: None
        """

        self.osa_sweep()
        self.osa_sweep()
        data = self.get_data()
        data = [float(i) for i in data]

        if dir[-1] != "/":
            dir += "/"
        with open(f"{dir}/{prefix}.csv", "w") as f:
            for item in data:
                f.write("%s\n" % item)
