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
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

try:
    True
except NameError:
    # provide values for old Python versions
    False, True = 0, 1

import string, types
from socket import socket, AF_INET, SOCK_STREAM, error
import os, sys, struct

class MapiError(Exception):
    pass

class server:
    def __init__(self, host="localhost", port=50000, user="monetdb",
            passwd="monetdb", lang="mil", db="", trace=0):
        self.prompt = '>'
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.lang = lang
        self.db = db
        self.trace = trace

        try:
            self.socket = socket(AF_INET, SOCK_STREAM)
            self.socket.connect((host, port))
        except (IOError, error), e:
            raise MapiError, e.args

        #block challenge:mserver:8:cypher(s):content_byteorder(BIG/LIT):");
        chal = self.getblock().split(':')
        challenge  = chal[0]
        servertype = chal[1]
        protover   = chal[2]
        cyphers    = chal[3]
        endian     = chal[4]
        self.putblock("LIT:%s:{plain}%s%s:%s:%s:" % (user,passwd,challenge,lang,db))
        if self.trace:
            print "Logged on %s@%s with %s\n" % (user,db,lang)
        prompt = self.getblock()
        if prompt != '':
            print("%s" % prompt)
            sys.exit(-1)

        if self.trace:
            print 'connected ', self.socket

    def getblock(self):
        # now read back the same way
        result = ""
        last_block = 0
        while not last_block:
            flag = self.socket.recv(2)  # read block info
            unpacked = struct.unpack( '<H', flag ) # unpack (little endian short)
            unpacked = unpacked[0]      # get first result from tuple
            len = ( unpacked >> 1 )     # get length
            last_block = unpacked & 1   # get last-block-flag

            if self.trace:
                print("getblock: %d %d\n" % (last_block, len))
            if len > 0:
                data = self.socket.recv(len) # read
                result += data
        if self.trace:
            print("getblock: %s\n" % result)
        return result

    def putblock(self, blk):
        pos        = 0
        last_block = 0
        blocksize  = 0xffff >> 1        # max len per block

        # create blocks of data with max 0xffff length,
        # then loop over the data and send it.
        while not last_block:
            data = blk[pos:blocksize]
            # set last-block-flag
            if len(data) < blocksize:
                last_block = 1
            flag = struct.pack( '<h', ( len(data) << 1 ) + last_block )
            if self.trace:
                print ("putblock: %d %s\n" % (last_block, data))
            self.socket.send(flag)      # len<<1 + last-block-flag
            self.socket.send(data)  # send it
            pos += len(data)        # next block

    def disconnect(self):
        """disconnect()
        Disconnect from the MonetDB server.
        """
        try:
            self.socket.close()
        except (IOError, error), e:
            raise MapiError, e.args
        self.socket = 0

    def cmd(self, cmd):
        """cmd(command) -> result.
        Main interface to Mapi server.  Sends command (a Unicode
        or UTF-8-encoded string to the MonetDB server, waits for the
        result, and returns it, converted to unicode.
        """
        # add linefeed if missing
        if cmd[-1:] != '\n':
            cmd += '\n'
        if (self.lang == 'sql' or self.lang == 'xquery'):
            cmd = 'S' + cmd
        # convert to UTF-8 encoding
        if type(cmd) is types.UnicodeType:
            cmd = cmd.encode('utf-8')
        self.putblock(cmd)
        return self.getblock()


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

# vim: set ts=4 sw=4 expandtab:
