import mysql.connector
import os
import math
import pkg_resources
import pandas as pd
import warnings
import functools

FIRST_CENTRAL_FREQ = 191350.0
CHANNEL_SPACING = 50.0
CHANNEL_WIDTH = 50.0
wdm_channel_list = list(range(1, 96))


def get_freq_range(
    channel_num,
    channel_width=CHANNEL_WIDTH,
    channel_spacing=CHANNEL_SPACING,
    first_central_freq=FIRST_CENTRAL_FREQ,
):
    """Get the frequency range of a channel number according to your grid specfied by channel width, channel spacing and first central frequency.
    The default values are set to the 95 x 50 ITU-T G.694.1 grid.

    :param channel_num: Channel number
    :type channel_num: int

    :param channel_width: Channel width in GHz
    :type channel_width: float

    :param channel_spacing: Channel spacing in GHz
    :type channel_spacing: float

    :param first_central_freq: First central frequency in THz
    :type first_central_freq: float

    :return: Start frequency, central frequency, and end frequency of the channel in THz
    :rtype: tuple
    """

    central_freq = first_central_freq + (channel_num - 1) * channel_spacing
    start_freq = central_freq - channel_width / 2.0
    end_freq = central_freq + channel_width / 2.0

    return int(start_freq), int(central_freq), int(end_freq)


def check_patch_owners(patch_list):
    """
    Check if all devices in the patch list are either unbooked or booked by the current user.
    Allows shared bookings (device booked by multiple users including current user).
    
    :param patch_list: A list of patches, where each patch is a tuple/list of device names.
    :type patch_list: list

    :return: True if all devices are unbooked or booked by current user, False otherwise.
    :rtype: bool
    """
    unix_user = os.getenv("SUDO_USER") or os.getenv("USER")
    if not unix_user:
        print("No valid user found in environment.")
        return False

    # Flatten patch_list and filter out "NULL"
    all_devices = {
        device for patch in patch_list for device in patch if device != "NULL"
    }
    if not all_devices:
        return True

    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        user=os.getenv("DB_USER", "testbed"),
        password=os.getenv("DB_PASSWORD", "mypassword"),
        database=os.getenv("DB_NAME", "provdb")
    )
    cursor = conn.cursor()

    try:
        placeholders = ",".join(["%s"] * len(all_devices))

        # Devices returned by this query are TRUE conflicts:
        # - they have at least one non-null/non-empty booking, AND
        # - none of those bookings belong to the current user.
        query = f"""
            SELECT
                device_name,
                GROUP_CONCAT(DISTINCT user_name ORDER BY user_name SEPARATOR ',')
            FROM active_bookings
            WHERE device_name IN ({placeholders})
              AND user_name IS NOT NULL AND user_name <> ''
            GROUP BY device_name
            HAVING SUM(user_name = %s) = 0
        """
        cursor.execute(query, tuple(all_devices) + (unix_user,))
        conflicts = cursor.fetchall()

        if conflicts:
            for device_name, owners_csv in conflicts:
                print(f"Device {device_name} is owned by: {owners_csv}")
            return False

        return True
    finally:
        cursor.close()
        conn.close()

    # Query active_bookings with current user or unallocated devices
    # cursor.execute(
    #     f"""
    #     SELECT device_id FROM active_bookings
    #     WHERE device_id IN (
    #         SELECT id FROM device_table WHERE deviceName IN ({format_strings})
    #     ) AND (user_id = %s OR user_id IS NULL)
    #     """,
    #     tuple(all_devices) + (user_id,),
    # )
    # owned_or_unallocated_device_ids = {row[0] for row in cursor.fetchall()}

    # # Get name to ID mapping (same as earlier)
    # cursor.execute(
    #     "SELECT id, deviceName FROM device_table WHERE deviceName IN (%s)" % format_strings,
    #     tuple(all_devices),
    # )
    # device_rows = cursor.fetchall()
    # name_to_id = {name: dev_id for dev_id, name in device_rows}

    # missing_devices = list(all_devices - set(name_to_id.keys()))
    # conflict_devices = [
    #     name for name, dev_id in name_to_id.items() if dev_id not in owned_or_unallocated_device_ids
    # ]

    # cursor.close()
    # conn.close()

    # if missing_devices:
    #     print("[CHECK_OWNERS] Unallocated devices:", missing_devices)
    # if conflict_devices:
    #     print(
    #         "[CHECK_OWNERS] Devices booked by another user or blocked:",
    #         conflict_devices,
    #     )

    # return not conflict_devices


def db_to_abs(db_value):
    """Function to convert dB to absolute value

    :param db_value
    :type db_value: list or float

    :return: Absolute value in Watts
    """
    absolute_value = 10 ** (db_value / float(10))
    return absolute_value


def abs_to_db(absolute_value):

    """Function to convert absolute value to dB

    :param absolute_value
    :type absolute_value: list or float

    :return: dB value
    """
    db_value = 10 * math.log10(absolute_value)
    return db_value


def abs_to_dbm(absolute_value):
    """Function to convert absolute value to dBm

    :param absolute_value
    :type absolute_value: list or float

    :return: dBm value
    """
    dbm_value = 10 * math.log10(absolute_value / 1e-3)
    return dbm_value


def load_csv_with_pandas(filename):
    # Get the path to the CSV file within the installed package
    csv_path = pkg_resources.resource_filename("tcdona3", filename)

    # Load the CSV file with pandas
    df = pd.read_csv(csv_path)

    return df


def deprecated(reason: str = ""):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            msg = f"Function {func.__name__} is deprecated."
            if reason:
                msg += f" Reason: {reason}"
            warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)
        return wrapper
    return decorator
