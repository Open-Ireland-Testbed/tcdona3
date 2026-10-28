"""``tcdona3.utils.check_patch_owners``: who may use a device, and what is asked of the booking database.

The database is faked: a test records the query and its parameters and answers with the conflicts it is given.
"""

import pytest

import tcdona3.utils as utils


class _Cursor(object):
    def __init__(self, conflicts, executed):
        self.conflicts, self.executed = conflicts, executed

    def execute(self, query, params):
        self.executed.append((query, params))

    def fetchall(self):
        return self.conflicts

    def close(self):
        pass


class _Connection(object):
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


@pytest.fixture
def database(monkeypatch):
    """Fakes the booking database; returns ``(executed, connections, set_conflicts)``."""
    executed, connections, conflicts = [], [], []

    def connect(**kwargs):
        connection = _Connection(_Cursor(conflicts, executed))
        connections.append((kwargs, connection))
        return connection

    monkeypatch.setattr(utils.mysql.connector, "connect", connect)
    for name in ("SUDO_USER", "TCDONA3_BOOKING_USER", "TCDONA3_BOOKING_ALIASES"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("USER", "Alice")
    return executed, connections, conflicts


def test_free_or_own_devices_are_allowed(database):
    executed, connections, _ = database
    assert utils.check_patch_owners([("roadm_1_p1", "roadm_1_line")]) is True
    (query, params), = executed
    assert "active_bookings" in query
    assert sorted(params[:2]) == ["roadm_1_line", "roadm_1_p1"]
    assert params[2:] == ("alice",)
    assert connections[0][1].closed


def test_a_device_booked_by_someone_else_is_refused(database, capsys):
    _, _, conflicts = database
    conflicts.append(("roadm_1_p1", "bob"))
    assert utils.check_patch_owners([("roadm_1_p1", "roadm_1_line")]) is False
    assert "roadm_1_p1 is owned by: bob" in capsys.readouterr().out


def test_sudo_user_wins_over_user(database, monkeypatch):
    executed, _, _ = database
    monkeypatch.setenv("SUDO_USER", "carol")
    utils.check_patch_owners([("ila_1_fwd", "ila_1_bck")])
    assert executed[0][1][2:] == ("carol",)


def test_booking_user_and_aliases_extend_the_allowed_users(database, monkeypatch):
    executed, _, _ = database
    monkeypatch.setenv("TCDONA3_BOOKING_USER", " Agastya ")
    monkeypatch.setenv("TCDONA3_BOOKING_ALIASES", "ajag, , alice ,")
    utils.check_patch_owners([("ila_1_fwd", "ila_1_bck")])
    query, params = executed[0]
    # matched in lower case (USER "Alice" and the alias "alice" are the same booking name)
    assert sorted(set(params[2:])) == ["agastya", "ajag", "alice"]
    assert query.count("%s") == len(params)


def test_null_devices_are_ignored_and_no_device_needs_no_database(database):
    executed, connections, _ = database
    assert utils.check_patch_owners([("NULL", "NULL")]) is True
    assert executed == [] and connections == []


def test_no_user_in_the_environment_is_refused_without_a_query(database, monkeypatch):
    executed, connections, _ = database
    monkeypatch.delenv("USER", raising=False)
    assert utils.check_patch_owners([("roadm_1_p1", "roadm_1_line")]) is False
    assert connections == []


def test_the_database_settings_come_from_the_environment(database, monkeypatch):
    _, connections, _ = database
    monkeypatch.setenv("DB_HOST", "db.example")
    utils.check_patch_owners([("roadm_1_p1", "roadm_1_line")])
    assert connections[0][0]["host"] == "db.example"
