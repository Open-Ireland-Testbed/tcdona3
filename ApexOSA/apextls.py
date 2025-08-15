from .Common import Send, Receive
import sys
from random import random
from math import log10 as log
from .Errors import *
from .Constantes import *


class TunableLaser:

    def __init__(self, Equipment, Simulation=False):
        """
        Constructor of a TLS embedded in an AP2XXX equipment.
        Equipment is the AP2XXX class of the equipment
        Simulation is a boolean to indicate to the program if it has to run in simulation mode or not
        """
        self.__Connexion = Equipment.Connexion
        self.__Simulation = Simulation
        self.__ID = Equipment.GetID()

        # Variables and constants of the equipment
        self.__Unit = "dBm"
        self.__ValidUnits = ["dbm", "mw"]

        self.__WLUnit = "nm"

        self.__Power = 0.0
        self.__Wavelength = 1550.0
        self.__Status = "OFF"

    def __str__(self):
        """
        Return the equipment type and the AP2XXX ID
        """
        return "TLS of " + str(self.__ID)

    def SetUnit(self, Unit):
        """
        Set the power unit of the TLS equipment
        Unit is a string which could be "dBm" for logaritmic or "mW" for linear power
        """

        try:
            Unit = str(Unit)
        except:
            self.__Connexion.close()
            raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "Unit")
        else:
            if Unit.lower() in self.__ValidUnits:
                self.__Unit = Unit

    def SetPRWUnit(self, iUnit):
        """
        Set the power unit of the TLS equipment
        Unit is a number which could be "0" for logaritmic or "1" for linear power
        """

        if not isinstance(iUnit, int):
            raise TypeError("Set Power Unit Scale : Argument should be int")

        if not self.__Simulation:
            Command = "TLSPWRUNT" + str(iUnit) + "\n"
            Send(self.__Connexion, Command)

    def GetPRWUnit(self):
        """
        Get Power Unit of the TLS equipment
        The Power Unit is expressed in "nm" or "dBm"
        """
        if not self.__Simulation:
            Command = "TLSPWRUNT?\n"
            Send(self.__Connexion, Command)
            PrwUnit = Receive(self.__Connexion)[:-1]

            self.__Unit = PrwUnit

        return self.__Unit

    def SetWLUnit(self, iUnit):
        """
        Set the power unit of the TLS equipment
        Unit is a number which could be "0" for nm or "1" for GHz
        """

        if not isinstance(iUnit, int):
            raise TypeError("Set Power Unit Scale : Argument should be int")

        if not self.__Simulation:
            Command = "TLSWLUNT" + str(iUnit) + "\n"
            Send(self.__Connexion, Command)

    def GetWLUnit(self):
        """
        Get WL Unit of the TLS equipment
        The Wavelength Unit is expressed in "nm" or "GHz"
        """
        if not self.__Simulation:
            Command = "TLSWLUNT?\n"
            Send(self.__Connexion, Command)
            self.__WLUnit = Receive(self.__Connexion)[:-1]

        return self.__WLUnit

    def GetUnit(self):
        """
        Get the power unit of the TLS equipment
        The return unit is a string
        """
        return self.__Unit

    def SetPower(self, Power):
        """
        Set the power of the TLS equipment
        The power is expressed in the unit defined by the GetUnit() method
        """

        if not isinstance(Power, (int, float)):
            self.__Connexion.close()
            raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "Power")

        if self.__Unit.lower() == "mw":
            Power = 10.0 * log(Power)
        self.__Power = Power

        if not self.__Simulation:
            Command = "TLSPWR" + str("%.1f" % self.__Power) + "\n"
            Send(self.__Connexion, Command)

    def GetPower(self):
        """
        Get the power of the TLS equipment
        The returned power is expressed in the unit defined by the GetUnit() method
        """

        if not self.__Simulation:
            Command = "TLSPWR?\n"
            Send(self.__Connexion, Command)
            Power = float(Receive(self.__Connexion)[:-1])

            if self.__Unit.lower() == "mw":
                Power = 10.0 ** (Power / 10.0)

            self.__Power = Power

        return self.__Power

    def SetWavelength(self, Wavelength):
        """
        Set the static wavelength of the TLS equipment
        The wavelength is expressed in nm
        """

        if not isinstance(Wavelength, (int, float)):
            self.__Connexion.close()
            raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "Wavelength")

        self.__Wavelength = Wavelength

        if not self.__Simulation:
            Command = "TLSSWL" + str("%.3f" % self.__Wavelength) + "\n"
            Send(self.__Connexion, Command)

    def GetWavelength(self):
        """
        Get the static wavelength of the TLS equipment
        The wavelength is expressed in nm
        """

        if not self.__Simulation:
            Command = "TLSSWL?\n"
            Send(self.__Connexion, Command)
            Wavelength = float(Receive(self.__Connexion)[:-1])

            self.__Wavelength = Wavelength

        return self.__Wavelength

    def SetFrequency(self, Frequency):
        """
        Set the static frequency of the TLS equipment
        The frequency is expressed in GHz
        """

        if not isinstance(Frequency, (int, float)):
            self.__Connexion.close()
            raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "Frequency")

        self.__Wavelength = VACCUM_LIGHT_SPEED / Frequency

        if not self.__Simulation:
            Command = "TLSSFR" + str("%.3f" % Frequency) + "\n"
            Send(self.__Connexion, Command)

    def GetFrequency(self):
        """
        Get the static frequency of the TLS equipment
        The frequency is expressed in GHz
        """

        if self.__Simulation:
            Frequency = VACCUM_LIGHT_SPEED / self.__Wavelength
        else:
            Command = "TLSSFR?\n"
            Send(self.__Connexion, Command)
            Frequency = float(Receive(self.__Connexion)[:-1])

            self.__Wavelength = VACCUM_LIGHT_SPEED / Frequency

        return Frequency

    def On(self):
        """
        Switch on the output power of TLS equipment
        """

        if not self.__Simulation:
            Command = "TLSOUT1\n"
            Send(self.__Connexion, Command)

        self.__Status = "ON"

    def Off(self):
        """
        Switch off the output power of the TLS equipment
        """

        if not self.__Simulation:
            Command = "TLSOUT0\n"
            Send(self.__Connexion, Command)

        self.__Status = "OFF"

    def GetStatus(self):
        """
        Return the status ("ON" or "OFF") of the TLS equipment
        """

        if not self.__Simulation:
            Command = "TLSOUT?\n"
            Send(self.__Connexion, Command)
            Status = int(Receive(self.__Connexion)[:-1])

            if Status == 1:
                self.__Status = "ON"
            else:
                self.__Status = "OFF"

        return self.__Status

    def SetLaserStatic(self):
        if not self.__Simulation:
            Command = "TLSS\n"
            Send(self.__Connexion, Command)

    def SetLaserSweep(self, Mode):
        if not isinstance(Mode, (int)):
            raise TypeError("Set Sweep mode : Argument should be int")

        if not self.__Simulation:
            Command = "TLSSW" + str(Mode) + "\n"
            Send(self.__Connexion, Command)

    def SetStartWL(self, StartWL):
        if not isinstance(StartWL, float):
            raise TypeError("Set Start WL: Argument should be float")

        if not self.__Simulation:
            Command = "TLSSTAR" + str(StartWL) + "\n"
            Send(self.__Connexion, Command)

    def GetStartWL(self):
        """
        Get the static wavelength of the TLS equipment
        The wavelength is expressed in nm
        """

        if not self.__Simulation:
            Command = "TLSSTAR?\n"
            Send(self.__Connexion, Command)
            StartWL = float(Receive(self.__Connexion)[:-1])

        return StartWL

    def SetStopWL(self, StopWL):
        if not isinstance(StopWL, float):
            raise TypeError("Set Start WL: Argument should be float")

        if not self.__Simulation:
            Command = "TLSSTOP" + str(StopWL) + "\n"
            Send(self.__Connexion, Command)

    def GetStopWL(self):
        """
        Get the static wavelength of the TLS equipment
        The wavelength is expressed in nm
        """

        if not self.__Simulation:
            Command = "TLSSTOP?\n"
            Send(self.__Connexion, Command)
            StartWL = float(Receive(self.__Connexion)[:-1])

        return StartWL

    def SetLaserSpeed(self, Speed):
        if not isinstance(Speed, float):
            raise TypeError("Set Start WL: Argument should be float")

        if not self.__Simulation:
            Command = "TLSSPE" + str(Speed) + "\n"
            Send(self.__Connexion, Command)

    def GetLaserSpeed(self):
        """
        Get the static wavelength of the TLS equipment
        The wavelength is expressed in nm
        """
        if not self.__Simulation:
            Command = "TLSSPE?\n"
            Send(self.__Connexion, Command)
            Speed = float(Receive(self.__Connexion)[:-1])

        return Speed

    def SetContPower(self, ContPower):
        if not isinstance(ContPower, float):
            raise TypeError("Set Start WL: Argument should be float")

        if not self.__Simulation:
            Command = "TLSCONTPWR" + str(ContPower) + "\n"
            Send(self.__Connexion, Command)

    def GetContPower(self):
        """
        Get the static wavelength of the TLS equipment
        The wavelength is expressed in nm
        """
        if not self.__Simulation:
            Command = "TLSCONTPWR?\n"
            Send(self.__Connexion, Command)
            ContPower = float(Receive(self.__Connexion)[:-1])

        return ContPower

    def SeStepDealy(self, StepDelay):
        if not isinstance(StepDelay, float):
            raise TypeError("Set Step Delay: Argument should be float")

        if not self.__Simulation:
            Command = "TLSSTEPDELAY" + str(StepDelay) + "\n"
            Send(self.__Connexion, Command)

    def GetStepDelay(self):
        """
        Get the step delay of the TLS equipment
        The step delay is expressed in s
        """
        if not self.__Simulation:
            Command = "TLSSTEPDELAY?\n"
            Send(self.__Connexion, Command)
            StepDelay = float(Receive(self.__Connexion)[:-1])

        return StepDelay

    def SeStepNum(self, StepNum):
        if not isinstance(StepNum, int):
            raise TypeError("Set Step Delay: Argument should be int")

        if not self.__Simulation:
            Command = "TLSSTEPNUM" + str(StepNum) + "\n"
            Send(self.__Connexion, Command)

    def GetStepNum(self):
        """
        Get the static wavelength of the TLS equipment
        The wavelength is expressed in nm
        """
        if not self.__Simulation:
            Command = "TLSSTEPNUM?\n"
            Send(self.__Connexion, Command)
            StepNum = int(Receive(self.__Connexion)[:-1])

        return StepNum

    def Run(self, Type="single"):
        """
        Runs a laser sweeping and returns the types of sweep (between 1 and 3)
        If Type is
            - "single" or 0, a single sweep is running (default)
            - "repeat" or 1, a repeat sweep is running
        In this function, the connection timeout is disabled and enabled after the
        execution of the function
        """

        if self.__Simulation:
            Type = -1
        else:
            TimeOut = self.__Connexion.gettimeout()
            self.__Connexion.settimeout(None)

            if isinstance(Type, str):
                if Type.lower() == "single":
                    Command = "TLSSWP0\n"
                else:
                    Command = "TLSSWP1\n"  # Type.lower() == "repeat":
            else:
                if Type == 0:
                    Command = "TLSSWP0\n"
                else:
                    Command = "TLSSWP1\n"  # Type == 1: repeat

            Send(self.__Connexion, Command)
            # try:
            #     trace = int(Receive(self.__Connexion))
            # except:
            #     trace = 0

            self.__Connexion.settimeout(TimeOut)

        return Type

    def Stop(self):
        """
        Stops a measurement
        """
        if not self.__Simulation:
            Command = "TLSSWP2\n"
            Send(self.__Connexion, Command)
