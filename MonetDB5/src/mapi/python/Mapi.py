# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2003 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

import string
from socket import socket, AF_INET, SOCK_STREAM
import os, sys

trace=          0
interactive=    0

class server:
    def __init__(self, server, port, user):
        try:
            self.socket = socket(AF_INET, SOCK_STREAM)
            self.socket.connect((server, port))
            self.prompt = u''
            self.buffer = ''
        except IOError:
            print 'server refuses access'

        self.cmd_intern(user+'\n')
        self.result()
        if trace > 0:
            print 'connected ', self.socket

    def cmd_intern(self, cmd):
        # convert to UTF-8 encoding
        if type(cmd) is type(u''):
            cmd = cmd.encode('utf-8')
        try:
            self.socket.send(cmd)
            if trace > 0:
                print 'cmd ', cmd
        except IOError:
            print 'IO error '

    def result(self):
        result = self.getstring()
        if trace > 0:
            print result.encode('utf-8')
	if self.prompt == result :
		return ''
        self.getprompt()
        return result

    def getstring(self):
        try:
            idx = string.find(self.buffer, "\1")
            if trace > 1:
                print self.buffer
            str = ""
            while idx < 0:
                if trace > 1:
                    print self.buffer
                str = str + self.buffer
                self.buffer = self.socket.recv(8096)
                idx = string.find(self.buffer, "\1")

            str = str + self.buffer[0:idx]
            self.buffer = self.buffer[idx+1:]
            if trace > 1:
                print str
            try:
                str = unicode(str, 'utf-8')
            except UnicodeDecodeError:
                print 'Error decoding result'
            return str
        except IOError:
            print 'IO error '
        except OSError:
            print 'end of file'
            sys.exit(1)
        return u''

    def getprompt(self):
        self.prompt = self.getstring()
        if interactive:
            print self.prompt.encode('utf-8')

    def disconnect(self):
        """disconnect()
        Disconnect from the Monet server.
        """
        self.result = self.cmd_intern('quit;\n')
        self.socket.close()
        self.socket = 0

    def cmd(self, cmd):
        """cmd(MIL-command) -> result.
        Main interface to Mapi server.  Sends MIL-command (a Unicode
        or UTF-8-encoded string to the Monet server, waits for the
        result, and returns it, converted to unicode.
        """
        # add linefeed if missing
        if cmd[-1:] != '\n':
            cmd = cmd + '\n'
        self.cmd_intern(cmd)
        return self.result()


if __name__ == '__main__':
    import fileinput

    s = server("localhost" , 50000, os.environ['USER'])
    fi = fileinput.FileInput()
    sys.stdout.write(s.prompt)
    line = fi.readline()
    while line and line != "quit;\n":
        res = s.cmd(line)
        sys.stdout.write(s.prompt)
        line = fi.readline()
    s.disconnect()
