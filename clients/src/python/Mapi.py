# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

try:
    True
except NameError:
    # provide values for old Python versions
    False, True = 0, 1

import string, types
from socket import socket, AF_INET, SOCK_STREAM, error
import os, sys

trace = False

class MapiError(Exception):
    pass

class server:
    language = None
    def __init__(self, server, port, user="monetdb", password="monetdb", language="mil"):
        self.prompt = 0
        self.prompt1 = u'\1\1\n'
        self.prompt2 = u'\1\2\n'

        try:
            self.socket = socket(AF_INET, SOCK_STREAM)
            self.socket.connect((server, port))
        except (IOError, error), e:
            raise MapiError, e.args

        self.cmd_intern(user+':'+password+':'+language+':line\n')
        self.result()

        if trace:
            print 'connected ', self.socket

        self.language = language

    def cmd_intern(self, cmd):
        # convert to UTF-8 encoding
        if type(cmd) is types.UnicodeType:
            cmd = cmd.encode('utf-8')
        try:
            self.socket.send(cmd)
        except (IOError, error), e:
            raise MapiError, e.args

        if trace:
            print 'cmd ', cmd

    def result(self):
        buffer = ""
        while buffer[-len(self.prompt1):] != self.prompt1 and \
                buffer[-len(self.prompt2):] != self.prompt2:
            try:
                buffer += self.socket.recv(8096)
            except (IOError, error), e:
                raise MapiError, e.args
            if trace:
                print buffer

        if buffer[-len(self.prompt1):] == self.prompt1:
            buffer = buffer[:-len(self.prompt1)]
            self.prompt = '> '
        elif buffer[-len(self.prompt2):] == self.prompt2:
            buffer = buffer[:-len(self.prompt2)]
            self.prompt = 'more> '

        if trace:
            print buffer

        try:
            buffer = unicode(buffer, 'utf-8')
        except UnicodeError, e:
            raise MapiError, e.args

        return buffer

    def disconnect(self):
        """disconnect()
        Disconnect from the MonetDB server.
        """
        self.result = self.cmd_intern('quit();\n')
        try:
            self.socket.close()
        except (IOError, error), e:
            raise MapiError, e.args
        self.socket = 0

    def cmd(self, cmd):
        """cmd(MIL-command) -> result.
        Main interface to Mapi server.  Sends MIL-command (a Unicode
        or UTF-8-encoded string to the MonetDB server, waits for the
        result, and returns it, converted to unicode.
        """
        # add linefeed if missing
        if cmd[-1:] != '\n':
            cmd += '\n'
        if (self.language == 'sql'):
            cmd = 's' + cmd
        self.cmd_intern(cmd)
        return self.result()


if __name__ == '__main__':
    s = server("localhost" , 50000)
    sys.stdout.write(s.prompt)
    line = sys.stdin.readline()
    while line and line != "\\q\n" and line != "quit();\n":
        res = s.cmd(line)
        print res.encode('utf-8'),
        sys.stdout.write(s.prompt)
        line = sys.stdin.readline()
    s.disconnect()
