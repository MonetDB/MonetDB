
from monetdb import mapi

def parse_statusline(line):
    split = line.split(',')
    info = {}
    info['path'] = split[0]
    info['name'] = info['path'].split("/")[-1]
    info['locked'] = split[1] == ("1")
    info['state'] = int(split[2])
    info['scenarios'] = split[3].split("'")
    info['connections'] = split[4].split("'")
    info['start_counter'] = int(split[5])
    info['stop_counter'] = int(split[6])
    info['crash_counter'] = int(split[7])
    info['avg_uptime'] = int(split[8])
    info['max_uptime'] = int(split[9])
    info['min_uptime'] = int(split[10])
    info['last_crash'] = int(split[11])
    info['lastStart'] = int(split[12])
    info['crash_avg1'] = split[13] == ("1")
    info['crash_avg10'] = float(split[14])
    info['crash_avg30'] = float(split[15])
    return info

class Control:
    """
    Use this module to manage your MonetDB databases. You can create, start, stop,
    lock, unlock, destroy your databases and request status information.
    """
    def __init__(self, hostname, port, passphrase):
        self.server = mapi.Server()
        self.server.connect(hostname, port, 'monetdb', passphrase, 'merovingian', 'control')

    def _send_command(self, database_name, command):
        return self.server.cmd("%s %s\n" % (database_name, command))

    def create(self, database_name):
        """
        Initialises a new database or multiplexfunnel in the MonetDB Server.
        A database created with this command makes it available  for use,
        however in maintenance mode (see monetdb lock).
        """
        return self._send_command(database_name, "create")

    def destroy(self, database_name):
        """
        Removes the given database, including all its data and
        logfiles.  Once destroy has completed, all data is lost.
        Be careful when using this command.
        """
        return self._send_command(database_name, "destroy")

    def lock(self, database_name):
        """
        Puts the given database in maintenance mode.  A database
        under maintenance can only be connected to by the DBA.
        A database which is under maintenance is not started
        automatically.  Use the "release" command to bring
        the database back for normal usage.
        """
        return self._send_command(database_name, "lock")

    def release(self, database_name):
        """
        Brings back a database from maintenance mode.  A released
        database is available again for normal use.  Use the
        "lock" command to take a database under maintenance.
        """
        return self._send_command(database_name, "release")

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
        return self._send_command(database_name, "start")

    def stop(self, database_name):
        """
        Stops the given database, if the MonetDB Database Server
        is running.
        """
        return self._send_command(database_name, "stop")

    def kill(self, database_name):
        """
        Kills the given database, if the MonetDB Database Server
        is running.  Note: killing a database should only be done
        as last resort to stop a database.  A database being
        killed may end up with data loss.
        """
        return self._send_command(database_name, "kill")

    def set(self, database_name, property_, value):
        """
        sets property to value for the given database
        for a list of properties, use `monetdb get all`
        """
        return self._send_command(database_name, "%s=%s" % (property_, value))

    def get(self, database_name):
        """
        gets value for property for the given database, or
        retrieves all properties for the given database
        """
        properties = self._send_command(database_name, "get")
        values = {}
        for dirty_line in properties.split("\n"):
            line = dirty_line[1:]
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
        return self._send_command(database_name, property_ + "=")

    def rename(self, old, new):
        return self.set(old, "name", new)

    def defaults(self):
        return self.get("#defaults")

    def neighbours(self):
        return self._send_command("anelosimus", "eximius")