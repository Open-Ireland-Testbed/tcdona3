import sys
import telnetlib
import re
from pandas import *
import time
import os, getpass
from datetime import datetime
import sqlite3
import csv
import mysql.connector


def timeStamped(fname, fmt="%Y-%m-%d_{fname}"):
    return datetime.now().strftime(fmt).format(fname=fname)


class Polatis:

    def __init__(self, host="10.10.10.28", port="3082"):
        self.telnet = telnetlib.Telnet(host, port)
        self.eol = ";"
        self.patch = {}
        self.shutter = {}
        self.monmode = {}
        self.wavelength = {}
        self.offset = {}
        self.atime = {}
        self.power = {}
        self.label = {}

    def __del__(self):
        pass

    def login(self):
        return self.__sendcmd("ACT-USER::root:123::root;")

    def logout(self):
        #        print self.telnet.read_all()
        self.__sendcmd("CANC-USER::root:123:;")
        self.telnet.close()

    def __sendcmd(self, line):
        #        print "sending " + line
        cmd_str = "%s\n" % line
        self.telnet.write(cmd_str.encode("ascii"))
        cmd_str = self.eol
        ret = self.telnet.read_until(cmd_str.encode("ascii"))
        ret = ret.decode("utf-8")
        return ret

    def __disable_all(self):
        line = "OPR-PORT-SHUTTER::1&&640:123:;"
        lines = self.__sendcmd(line)

    def __enable_all(self):
        line = "RLS-PORT-SHUTTER::1&&640:123:;"
        lines = self.__sendcmd(line)

    def __settimeout(self, timeout=60):
        return self.__sendcmd("ED-EQPT::TIMEOUT:123:::ADMIN=" + str(timeout) + ";")

    def __clearallconn(self):
        self.__sendcmd("DLT-PATCH::ALL:123:;")

    def get_inport(self, inx):
        """
        Retrieves the 'In' value from the 'ports' table in the 'provdb' database.

        Args:
            inx (str): The name of the port.

        Returns:
            str: The 'In' value associated with the specified port name.
        """
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host="127.0.0.1", user="testbed", password="mypassword", database="provdb"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT `Out_Port` FROM ports_new WHERE Name = %s", (inx,))
        inp = cursor.fetchone()[0]
        return inp

    def get_outport(self, outx):
        """
        Retrieves the output port for a given input port.

        Args:
            outx (str): The name of the input port.

        Returns:
            str: The name of the corresponding output port.

        Raises:
            mysql.connector.Error: If there is an error executing the SQL query.
            IndexError: If no output port is found for the given input port.
        """
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host="127.0.0.1", user="testbed", password="mypassword", database="provdb"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT `In_Port` FROM ports_new WHERE Name = %s", (outx,))
        outp = cursor.fetchone()[0]
        return outp

    def __disable_port(self, port):
        line = "OPR-PORT-SHUTTER::" + str(port) + ":123:;"
        return self.__sendcmd(line)

    def __enable_port(self, port):
        line = "RLS-PORT-SHUTTER::" + str(port) + ":123:;"
        return self.__sendcmd(line)

    def __conn(self, inport, outport):
        line = "ENT-PATCH::" + str(inport) + "," + str(outport) + ":123:;"
        return self.__sendcmd(line)

    def __disconn(self, inport, outport):
        line = "DLT-PATCH::" + str(inport) + ":123:;"
        return self.__sendcmd(line)

    def __fullconn(self, inport, outport):
        self.__conn(inport, outport)

    def __fulldisconn(self, inport, outport):
        self.__disconn(inport, outport)

    def logger(self, message):
        now = datetime.now()
        with open("/tmp/" + timeStamped("polatis.log"), "a") as outf:
            outf.write(
                "%s %s\t%s\t%s\n"
                % (
                    now.strftime("%Y/%m/%d %H:%M:%S"),
                    os.getlogin(),
                    getpass.getuser(),
                    message,
                )
            )

    def apply_patch_list(self, patch_list):
        """
        Applies a list of patches to the Polatis switch.

        Args:
            patch_list (list): A list of tuples representing the patches to be applied. Each tuple should contain two strings,
                               where the first string represents the input component and the second string represents the output component.

        Raises:
            Exception: If the patch_list argument is not a list or if it is empty.
            Exception: If some (or all) ports are not available in the patch_list.

        Returns:
            None
        """
        if not isinstance(patch_list, list):
            raise Exception("Argument patch_list must be a list of tuples of patches")
        if len(patch_list) == 0:
            raise Exception("Argument patch_list must not be empty")

        if not self.check_patch_owners(patch_list):
            print("apply_patch_list failed")
            raise Exception(
                "apply_patch_list failed, some (or all) ports are not available. Please contact admin."
            )

        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host="127.0.0.1", user="testbed", password="mypassword", database="provdb"
        )
        cursor = conn.cursor()

        for patch in patch_list:

            input_comp, output_comp = patch

            # Fetch the 'In' and 'Out' values from the Ports table
            cursor.execute(
                "SELECT `Out_Port` FROM ports_new WHERE Name = %s", (input_comp,)
            )
            inp = cursor.fetchone()[0]

            cursor.execute(
                "SELECT `In_Port` FROM ports_new WHERE Name = %s", (output_comp,)
            )
            outp = cursor.fetchone()[0]

            cursor.execute(
                "SELECT `Max_Inpower` FROM ports_new WHERE Name = %s", (input_comp,)
            )
            max_inpower = cursor.fetchone()
            max_inpower = max_inpower[0] if max_inpower else None

            inpower = self.get_power(int(inp))
            if max_inpower:
                max_inpower_val = float(max_inpower)
            else:
                max_inpower_val = 20.0

            if inpower > max_inpower_val:
                message = "%s (%s): %.2f dBm > %s (%s): %.2f dBm" % (
                    input_comp,
                    inp,
                    inpower,
                    output_comp,
                    outp,
                    max_inpower_val,
                )
                #self.logger("Patch max power exceeded: %s" % message)
                raise Exception("Patch max power exceeded: %s" % message)
            else:
                self.__conn(inp, outp)
                time.sleep(1)
                outpower = self.get_power(int(outp))
                data = "%s (%s): %.2f dBm ---> %s (%s): %.2f dBm < %.2f dBm" % (
                    input_comp,
                    inp,
                    inpower,
                    output_comp,
                    outp,
                    outpower,
                    max_inpower_val,
                )
                print(data)

                now = datetime.now()
                #self.logger("Connect %s" % (data))

        # Close the cursor and connection
        cursor.close()
        conn.close()

    def check_patch_owners(self, patch_list):
        """
        Checks the owners of the ports in the given patch list.

        Args:
            patch_list (list): A list of patches, where each patch is a list of ports.

        Returns:
            bool: True if all ports have the correct owner, False otherwise.
        """

        # Get the Unix user behind sudo
        unix_user = os.getenv("SUDO_USER")
        if not unix_user:
            unix_user = os.getenv("USER")

        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host="127.0.0.1", user="testbed", password="mypassword", database="provdb"
        )
        cursor = conn.cursor()

        nonexistent_ports = []
        other_owners = []

        for patch in patch_list:
            for port in patch:
                # Skip NULL connections
                if port == "NULL":
                    continue

                # Check if the port exists and fetch the owner
                cursor.execute("SELECT Owner FROM ports_new WHERE Name = %s", (port,))
                result = cursor.fetchone()
                if not result:
                    nonexistent_ports.append(port)
                else:
                    #if len(owner) == 0:
                    #    nonexistent_ports.append(port)
                    owner=result[0]
                    if len(owner)!=0 and unix_user not in owner.split(','):
                        other_owners.append(result)
        if (len(nonexistent_ports) > 0) or (len(other_owners) > 0):
            # Close the cursor and connection
            cursor.close()
            conn.close()

            if nonexistent_ports:
                print("Nonexistent ports:", nonexistent_ports)
            if other_owners:
                print("Ports with other owners:", other_owners)
            return False

        else:
            # # If all checks pass, update the owner of the ports to the Unix user
            # for connection in patch_list:
            #     for name in connection:
            #         if name == "NULL":
            #             continue
            #         cursor.execute(
            #             "UPDATE ports SET owner = %s WHERE Name = %s", (unix_user, name)
            #         )

            # # Commit the changes
            # conn.commit()

            # Close the cursor and connection
            cursor.close()
            conn.close()
            return True

    def release_ports(self, patch_list, username):

        with open("/etc/secure_keys/mysql_key.key", "r") as file:
            lines = file.readlines()
            admin_user = lines[0].strip()
            password = lines[1].strip()

        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host="127.0.0.1", user=admin_user, password=password, database="provdb"
        )
        cursor = conn.cursor()

        for patch in patch_list:
            for name in patch:
                if name == "NULL":
                    continue

                # Set owner to NULL where the owner is the Unix user and the name matches
                cursor.execute(
                    f'UPDATE ports_new SET Owner = "{username}" WHERE Name = "{name}"'
                )

        # Commit the changes
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

    def print_patch_table(self, patch_list):
        """
        Prints a patch table based on the given patch list.

        Args:
            patch_list (list): A list of tuples representing the patches.
                Each tuple should contain two elements: the input patch and the output patch.
            csv (bool, optional): If True, returns the patch table as a CSV string.
                Defaults to False.

        Returns:
            None or str: If csv is True, returns the patch table as a CSV string.
                Otherwise, prints the patch table and returns None.

        Raises:
            Exception: If patch_list is not a list or if it is empty.
        """
        if not isinstance(patch_list, list):
            raise Exception("Argument patch_list must be a list of tuples of patches")
        if len(patch_list) == 0:
            raise Exception("Argument patch_list must not be empty")

        field_names = ["#", "Component", "I/O", "Port", "Power (dBm)"]
        while True:
            n = 0
            print('\t'.join(field_names))
            for patch in patch_list:
                inx, outx = patch
                inp = self.get_inport(inx)
                outp = self.get_outport(outx)
                inpower = self.get_power(int(inp))
                outpower = self.get_power(int(outp))
                print('\t'.join([str(i) for i in [n, inx, "Out", inp, inpower]]))
                print('\t'.join([str(i) for i in [n + 1, outx, "In", outp, outpower]]))
                n += 2
            input_txt = input("Enter Y to continue..., otherwise Exit")
            if str(input_txt).lower() != "y":
                break

    def get_patch_table_csv(self, patch_list, filename):
        """
        Generates a CSV file containing the patch table based on the given patch list.

        Args:
            patch_list (list): A list of patches.
            filename (str): The name of the CSV file to be generated.

        Returns:
            None
        """
        if not isinstance(patch_list, list):
            raise Exception("Argument patch_list must be a list of tuples of patches")
        if len(patch_list) == 0:
            raise Exception("Argument patch_list must not be empty")
        data = []
        for n, patch in enumerate(patch_list, start=1):
            inx, outx = patch
            inp = self.get_inport(inx)
            outp = self.get_outport(outx)
            inpower = self.get_power(int(inp))
            outpower = self.get_power(int(outp))
            data.append([inx, "Out", inp, inpower])
            data.append([outx, "In", outp, outpower])
        with open(filename, "w") as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def get_NE_type(self):
        line = "RTRV-NETYPE:::123:;"
        lines = self.__sendcmd(line)
        for line in lines.split("\n"):
            m = re.match(r'\W*"(\S+),(\S+),(\S+),(\S+)"', line)
            if m:
                print(m.group(1), m.group(2), m.group(3), m.group(4))

    def get_all_patch(self):
        line = "RTRV-PATCH:::123:;"
        lines = self.__sendcmd(line)
        ret = ""
        for line in lines.split("\n"):
            m = re.match(r'\W*"(\d+),(\d+)"', line)
            if m:
                self.patch[int(m.group(1))] = int(m.group(2))
        return

    def get_all_atten(self):
        line = "RTRV-PORT-ATTEN::1&&640:123:;"
        lines = self.__sendcmd(line)
        print(lines)

    def get_all_labels(self):
        line = "RTRV-PORT-LABEL::1&&640:123:;"
        lines = self.__sendcmd(line)
        for line in lines.split("\n"):
            m = re.match(r'\W*"(\d+):(\S+)"', line)
            if m:
                self.label[int(m.group(1))] = m.group(2)
        print(self.label)
        return

    def get_all_shutter(self):
        line = "RTRV-PORT-SHUTTER::1&&640:123:;"
        lines = self.__sendcmd(line)
        for line in lines.split("\n"):
            m = re.match(r'\W*"(\d+):(\S+)"', line)
            if m:
                self.shutter[int(m.group(1))] = m.group(2)
        return

    def get_all_pmon(self):
        line = "RTRV-PORT-PMON::1&&640:123:;"
        lines = self.__sendcmd(line)
        for line in lines.split("\n"):
            m = re.match(r'\W*"(\d+):(\S+),(\S+),(\S+)"', line)
            if m:
                self.wavelength[int(m.group(1))] = float(m.group(2))
                self.offset[int(m.group(1))] = float(m.group(3))
                self.atime[int(m.group(1))] = float(m.group(4))
        line = "RTRV-EQPT::PMON:123:::PARAMETER=CONFIG;"
        lines = self.__sendcmd(line)
        for line in lines.split("\n"):
            m = re.match(r'\W*"PMON::PORT=(\d+),MODE=(\S+)"', line)
            if m:
                self.monmode[int(m.group(1))] = m.group(2)
        return

    def get_all_power(self):
        line = "RTRV-PORT-POWER::1&&640:123:;"
        lines = self.__sendcmd(line)
        for line in lines.split("\n"):
            m = re.match(r'\W*"(\d+):(\S+)"', line)
            if m:
                self.power[int(m.group(1))] = float(m.group(2))
        return

    def test_all_power(self):
        self.get_all_power()
        while True:
            ch = input("(dis)connect patch ..")
            line = "RTRV-PORT-POWER::1&&640:123:;"
            lines = self.__sendcmd(line)
            for line in lines.split("\n"):
                m = re.match(r'\W*"(\d+):(\S+)"', line)
                if m:
                    port = int(m.group(1))
                    power = float(m.group(2))
                    if abs(self.power[port] - power) > 3:
                        if power > self.power[port]:
                            transit = "Up"
                        else:
                            transit = "Down"
                        print(
                            "%s: %d %.2f -> %.2f"
                            % (transit, port, self.power[port], power)
                        )
                    self.power[port] = float(power)

    def get_power(self, port):
        line = "RTRV-PORT-POWER::%d:123:;" % port
        lines = self.__sendcmd(line)
        for line in lines.split("\n"):
            m = re.match(r'\W*"(\d+):(\S+)"', line)
            if m:
                return float(m.group(2))
        return -99.99

    def getall(self):
        self.get_all_patch()
        self.get_all_shutter()
        self.get_all_pmon()
        self.get_all_power()

    def report_all(self):
        for i in sorted(self.power.keys()):
            patch = self.patch.get(i, 0)
            shutter = self.shutter.get(i, "")
            monmode = self.monmode.get(i, "")
            wavelength = self.wavelength.get(i, 0.0)
            offset = self.offset.get(i, 0.0)
            atime = self.atime.get(i, 0.0)
            power = self.power.get(i, 0.0)
            print(i, patch, shutter, monmode, wavelength, offset, atime, power)


