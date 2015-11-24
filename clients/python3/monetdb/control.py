# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2008-2015 MonetDB B.V.

import platform
from monetdb import mapi
from monetdb.exceptions import OperationalError, InterfaceError


def parse_statusline(line):
    """
    parses a sabdb format status line. Support v1 and v2.

    """
    if line.startswith("="):
        line = line[1:]
    if not line.startswith('sabdb:'):
        raise OperationalError('wrong result received')

    code, prot_version, rest = line.split(":", 2)

    if prot_version not in ["1", "2"]:
        raise InterfaceError("unsupported sabdb protocol")
    else:
        prot_version = int(prot_version)

    subparts = rest.split(',')
    sub_iter = iter(subparts)

    info = {}

    info['name'] = sub_iter.__next__()
    info['path'] = sub_iter.__next__()
    info['locked'] = sub_iter.__next__() == "1"
    info['state'] = int(sub_iter.__next__())
    info['scenarios'] = sub_iter.__next__().split("'")
    if prot_version == 1:
        sub_iter.__next__()
    info['start_counter'] = int(sub_iter.__next__())
    info['stop_counter'] = int(sub_iter.__next__())
    info['crash_counter'] = int(sub_iter.__next__())
    info['avg_uptime'] = int(sub_iter.__next__())
    info['max_uptime'] = int(sub_iter.__next__())
    info['min_uptime'] = int(sub_iter.__next__())
    info['last_crash'] = int(sub_iter.__next__())
    info['last_start'] = int(sub_iter.__next__())
    if prot_version > 1:
        info['last_stop'] = int(sub_iter.__next__())
    info['crash_avg1'] = sub_iter.__next__() == "1"
    info['crash_avg10'] = float(sub_iter.__next__())
    info['crash_avg30'] = float(sub_iter.__next__())

    return info


def isempty(result):
    """ raises an exception if the result is not empty"""
    if result != "":
        raise OperationalError(result)
    else:
        return True


class Control:
    """
    Use this module to manage your MonetDB databases. You can create, start,
    stop, lock, unlock, destroy your databases and request status information.
    """
    def __init__(self, hostname=None, port=50000, passphrase=None,
                 unix_socket=None):

        if not unix_socket:
            unix_socket = "/tmp/.s.merovingian.%i" % port

        if platform.system() == "Windows" and not hostname:
            hostname = "localhost"

        self.server = mapi.Connection()
        self.hostname = hostname
        self.port = port
        self.passphrase = passphrase
        self.unix_socket = unix_socket

        # check connection
        self.server.connect(hostname=hostname, port=port, username='monetdb',
                            password=passphrase,
                            database='merovingian', language='control',
                            unix_socket=unix_socket)
        self.server.disconnect()

    def _send_command(self, database_name, command):
        self.server.connect(hostname=self.hostname, port=self.port,
                            username='monetdb', password=self.passphrase,
                            database='merovingian', language='control',
                            unix_socket=self.unix_socket)
        try:
            return self.server.cmd("%s %s\n" % (database_name, command))
        finally:
            # always close connection
            self.server.disconnect()

    def create(self, database_name):
        """
        Initialises a new database or multiplexfunnel in the MonetDB Server.
        A database created with this command makes it available  for use,
        however in maintenance mode (see monetdb lock).
        """
        return isempty(self._send_command(database_name, "create"))

    def destroy(self, database_name):
        """
        Removes the given database, including all its data and
        logfiles.  Once destroy has completed, all data is lost.
        Be careful when using this command.
        """
        return isempty(self._send_command(database_name, "destroy"))

    def lock(self, database_name):
        """
        Puts the given database in maintenance mode.  A database
        under maintenance can only be connected to by the DBA.
        A database which is under maintenance is not started
        automatically.  Use the "release" command to bring
        the database back for normal usage.
        """
        return isempty(self._send_command(database_name, "lock"))

    def release(self, database_name):
        """
        Brings back a database from maintenance mode.  A released
        database is available again for normal use.  Use the
        "lock" command to take a database under maintenance.
        """
        return isempty(self._send_command(database_name, "release"))

    def status(self, database_name=False):
        """
        Shows the state of a given glob-style database match, or
        all known if none given.  Instead of the normal mode, a
        long and crash mode control what information is displayed.
        """
        if database_name:
            raw = self._send_command(database_name, "status")
            return parse_statusline(raw)
        else:
            raw = self._send_command("#all", "status")
            return [parse_statusline(line) for line in raw.split("\n")]

    def start(self, database_name):
        """
        Starts the given database, if the MonetDB Database Server
        is running.
        """
        return isempty(self._send_command(database_name, "start"))

    def stop(self, database_name):
        """
        Stops the given database, if the MonetDB Database Server
        is running.
        """
        return isempty(self._send_command(database_name, "stop"))

    def kill(self, database_name):
        """
        Kills the given database, if the MonetDB Database Server
        is running.  Note: killing a database should only be done
        as last resort to stop a database.  A database being
        killed may end up with data loss.
        """
        return isempty(self._send_command(database_name, "kill"))

    def set(self, database_name, property_, value):
        """
        sets property to value for the given database
        for a list of properties, use `monetdb get all`
        """
        return isempty(self._send_command(database_name, "%s=%s" % (property_,
                                                                    value)))

    def get(self, database_name):
        """
        gets value for property for the given database, or
        retrieves all properties for the given database
        """
        properties = self._send_command(database_name, "get")
        values = {}
        for line in properties.split("\n"):
            if line.startswith("="):
                line = line[1:]
            if not line.startswith("#"):
                if "=" in line:
                    split = line.split("=")
                    values[split[0]] = split[1]
        return values

    def inherit(self, database_name, property_):
        """
        unsets property, reverting to its inherited value from
        the default configuration for the given database
        """
        return isempty(self._send_command(database_name, property_ + "="))

    def rename(self, old, new):
        return self.set(old, "name", new)

    def defaults(self):
        return self.get("#defaults")

    def neighbours(self):
        return self._send_command("anelosimus", "eximius")
