
from monetdb import mapi

def parse_statusline(line):
    split = line.split(',')
    info = {}
    info['path'] = split[0]
    info['name'] = info['path'].split("/")[-1]
    info['locked'] = True if split[1] == ("1") else False
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
    info['crash_avg1'] = True if split[1] == ("1") else False
    info['crash_avg10'] = float(split[14])
    info['crash_avg30'] = float(split[15])
    return info

class Control:
    def __init__(self, hostname, port, passphrase):
        self.server = mapi.Server()
        self.server.connect(hostname, port, 'monetdb', passphrase, 'merovingian', 'control')

    def _send_command(self, database_name, command):
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
        raw = self._send_command(database_name, "status")
        return parse_statusline(raw)

    def statuses(self):
        raw = self._send_command("#all", "status")
        return [parse_statusline(line) for line in raw.split("\n")]

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
        for dirty_line in properties.split("\n"):
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