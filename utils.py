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


_BOOKING_CHECK_DISABLED_VALUES = {"0", "false", "no", "off"}


def check_patch_owners(patch_list, enabled=None):
    """
    Check if all devices in the patch list are either unbooked or booked by the current user.
    Allows shared bookings (device booked by multiple users including current user).
    Supports username aliasing without changing DB records:
    - TCDONA3_BOOKING_USER: single override username
    - TCDONA3_BOOKING_ALIASES: comma-separated extra usernames

    The check is enabled by default.  Set ``enabled=False`` (or set
    ``TCDONA3_BOOKING_CHECK`` to one of ``0``, ``false``, ``no`` or ``off``)
    to skip the provisioning-database query.

    :param patch_list: A list of patches, where each patch is a tuple/list of device names.
    :type patch_list: list

    :param enabled: Whether to run the booking check.  ``None`` reads
        ``TCDONA3_BOOKING_CHECK`` (default: enabled).
    :type enabled: bool or None

    :return: True if all devices are unbooked or booked by current user, False otherwise.
    :rtype: bool
    """
    if enabled is None:
        enabled = os.getenv("TCDONA3_BOOKING_CHECK", "1").strip().lower() not in (
            _BOOKING_CHECK_DISABLED_VALUES
        )
    if not enabled:
        return True

    allowed_users = set()

    unix_user = os.getenv("SUDO_USER") or os.getenv("USER")
    if unix_user:
        allowed_users.add(unix_user.strip())

    booking_user = os.getenv("TCDONA3_BOOKING_USER", "").strip()
    if booking_user:
        allowed_users.add(booking_user)

    booking_aliases = os.getenv("TCDONA3_BOOKING_ALIASES", "")
    for alias in booking_aliases.split(","):
        alias = alias.strip()
        if alias:
            allowed_users.add(alias)

    allowed_users = sorted(user for user in allowed_users if user)
    if not allowed_users:
        print("No valid user found in environment.")
        return False

    # Flatten patch_list and filter out "NULL"
    all_devices = {
        device for patch in patch_list for device in patch if device != "NULL"
    }
    if not all_devices:
        return True

    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST", "10.10.10.4"),
        user=os.getenv("DB_USER", "testbed"),
        password=os.getenv("DB_PASSWORD", "mypassword"),
        database=os.getenv("DB_NAME", "provdb")
    )
    cursor = conn.cursor()

    try:
        placeholders = ",".join(["%s"] * len(all_devices))
        allowed_user_placeholders = ",".join(["%s"] * len(allowed_users))

        # Devices returned by this query are TRUE conflicts:
        # - they have at least one non-null/non-empty booking, AND
        # - none of those bookings belong to allowed booking usernames.
        query = f"""
            SELECT
                device_name,
                GROUP_CONCAT(DISTINCT user_name ORDER BY user_name SEPARATOR ',')
            FROM active_bookings
            WHERE device_name IN ({placeholders})
              AND user_name IS NOT NULL AND user_name <> ''
            GROUP BY device_name
            HAVING SUM(LOWER(user_name) IN ({allowed_user_placeholders})) = 0
        """
        cursor.execute(
            query,
            tuple(all_devices) + tuple(user.lower() for user in allowed_users),
        )
        conflicts = cursor.fetchall()

        if conflicts:
            print(
                "Allowed booking usernames for this session: "
                + ", ".join(allowed_users)
            )
            for device_name, owners_csv in conflicts:
                print(f"Device {device_name} is owned by: {owners_csv}")
            return False

        return True
    finally:
        cursor.close()
        conn.close()


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
