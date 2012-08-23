
from monetdb import mapi

class Control:
    def __init__(self, hostname, port, passphrase):
        self.server = mapi.Server()
        self.server.connect(hostname, port, 'monetdb', passphrase, 'merovingian', 'control')

    def _send_command(self, database_name, command, has_output=False):
        return self.server.cmd("%s %s\n" % (database_name, command))

    def create(self, database_name):
        return self._send_command(database_name, "create")

    def destroy(self, database_name):
        return self._send_command(database_name, "destroy")

    def lock(self, database_name):
        return self._send_command(database_name, "lock")

    def release(self, database_name):
        return self._send_command(database_name, "release")

    def status(self, database_name):
        return self._send_command(database_name, "status", True)

    def start(self, database_name):
        return self._send_command(database_name, "start")

    def stop(self, database_name):
        return self._send_command(database_name, "stop")

    def kill(self, database_name):
        return self._send_command(database_name, "kill")

    def set(self, database_name, property_, value):
        return self._send_command(database_name, "%s=%s" % (property_, value))

    def get(self, database_name):
        properties = self._send_command(database_name, "get")
        values = {}
        for dirty_line in properties.split("\n")[1:]:
            line = dirty_line[1:]
            if not line.startswith("#"):
                if "=" in line:
                    split = line.split("=")
                    values[split[0]] = split[1]
        return values

    def inherit(self, database_name, property_):
        return self._send_command(database_name, property_ + "=")

    def version(self, database_name):
        self._send_command(database_name, "version")