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
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

"""
this is the python 2.* implementation of the mapi API.

the python 3.* implementation is in mapi3.py
"""

import socket
import logging
import struct
import hashlib
import platform

from cStringIO import StringIO

from monetdb.monetdb_exceptions import *

# Windows doesn't support MSG_WAITALL flag for recv
flags = 0
if hasattr(socket, 'MSG_WAITALL'):
    flags = socket.MSG_WAITALL

logger = logging.getLogger("monetdb")

MAX_PACKAGE_LENGTH = (1024*8)-2

MSG_PROMPT = ""
MSG_INFO = "#"
MSG_ERROR = "!"
MSG_Q = "&"
MSG_QTABLE = "&1"
MSG_QUPDATE = "&2"
MSG_QSCHEMA = "&3"
MSG_QTRANS = "&4"
MSG_QPREPARE = "&5"
MSG_QBLOCK = "&6"
MSG_HEADER = "%"
MSG_TUPLE = "["
MSG_REDIRECT = "^"

STATE_INIT = 0
STATE_READY = 1


class Server:
    def __init__(self):
        self.state = STATE_INIT
        self._result = None

    def connect(self, hostname, port, username, password, database, language):
        """ connect to a MonetDB database using the mapi protocol"""

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.language = language

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # For performance, mirror MonetDB/src/common/stream.c socket settings.
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        try:
            self.socket.connect((hostname, port))
        except socket.error, error:
            (error_code, error_str) = error
            raise OperationalError(error_str)

        self.__login()


    def __login(self, iteration=0):
        """ Reads challenge from line, generate response and check if
        everything is okay """

        challenge = self.__getblock()
        response = self.__challenge_response(challenge)
        self.__putblock(response)
        prompt = self.__getblock().strip()

        if len(prompt) == 0:
            # Empty response, server is happy
            pass
        elif prompt.startswith(MSG_INFO):
            logger.info("II %s" % prompt[1:])

        elif prompt.startswith(MSG_ERROR):
            logger.error(prompt[1:])
            raise DatabaseError(prompt[1:])

        elif prompt.startswith(MSG_REDIRECT):
            # a redirect can contain multiple redirects, for now we only use
            # the first
            redirect = prompt.split()[0][1:].split(':')
            if redirect[1] == "merovingian":
                logger.debug("II: merovingian proxy, restarting " +
                        "authenticatiton")
                if iteration <= 10:
                    self.__login(iteration=iteration+1)
                else:
                    raise OperationalError("maximal number of redirects " +
                    "reached (10)")

            elif redirect[1] == "monetdb":
                self.hostname = redirect[2][2:]
                self.port, self.database = redirect[3].split('/')
                self.port = int(self.port)
                logger.info("II: merovingian redirect to monetdb://%s:%s/%s" %
                        (self.hostname, self.port, self.database))
                self.socket.close()
                self.connect(self.hostname, self.port, self.username,
                        self.password, self.database, self.language)

            else:
                logger.error('!' + prompt[0])
                raise ProgrammingError("unknown redirect: %s" % prompt)

        else:
            logger.error('!' + prompt[0])
            raise ProgrammingError("unknown state: %s" % prompt)

        self.state = STATE_READY
        return True


    def disconnect(self):
        """ disconnect from the monetdb server """
        self.state = STATE_INIT
        self.socket.close()


    def cmd(self, operation):
        """ put a mapi command on the line"""
        logger.debug("II: executing command %s" % operation)

        if self.state != STATE_READY:
            raise(ProgrammingError, "Not connected")

        self.__putblock(operation)
        response = self.__getblock()
        if not len(response):
            return
        if response[0] in [MSG_Q, MSG_HEADER, MSG_TUPLE]:
            return response
        elif response[0] == MSG_ERROR:
            raise OperationalError(response[1:])
        else:
            raise ProgrammingError("unknown state: %s" % response)


    def __challenge_response(self, challenge):
        """ generate a response to a mapi login challenge """
        challenges = challenge.split(':')
        salt, identity, protocol, hashes, endian = challenges[:5]

        password = self.password

        if protocol == '9':
            algo = challenges[5]
            if algo == 'SHA512':
                password = hashlib.sha512(password).hexdigest()
            elif algo == 'SHA384':
                password = hashlib.sha384(password).hexdigest()
            elif algo == 'SHA256':
                password = hashlib.sha256(password).hexdigest()
            elif algo == 'SHA224':
                password = hashlib.sha224(password).hexdigest()
            elif algo == 'SHA1':
                password = hashlib.sha1(password).hexdigest()
            elif algo == 'MD5':
                password = hashlib.md5(password).hexdigest()
            else:
                raise NotSupportedError("The %s hash algorithm is not " +
                    "supported" % algo)
        elif protocol != "8":
            raise NotSupportedError("We only speak protocol v8 and v9")

        h = hashes.split(",")
        if "SHA1" in h:
            s = hashlib.sha1()
            s.update(password.encode())
            s.update(salt.encode())
            pwhash = "{SHA1}" + s.hexdigest()
        elif "MD5" in h:
            m = hashlib.md5()
            m.update(password.encode())
            m.update(salt.encode())
            pwhash = "{MD5}" + m.hexdigest()
        elif "crypt" in h:
            import crypt
            pwhash = "{crypt}" + crypt.crypt((password+salt)[:8], salt[-2:])
        else:
            pwhash = "{plain}" + password + salt

        return ":".join(["BIG", self.username, pwhash, self.language,
            self.database]) + ":"


    def __getblock(self):
        """ read one mapi encoded block """
        result = StringIO()
        last = 0
        while not last:
            flag = self.__getbytes(2)
            unpacked = struct.unpack('<H', flag)[0] # unpack little endian short
            length = unpacked >> 1
            last = unpacked & 1
            logger.debug("II: reading %i bytes, last: %s" % (length, bool(last)))
            result.write(self.__getbytes(length))
        logger.debug("RX: %s" % result.getvalue())
        return result.getvalue()


    def __getbytes(self, bytes):
        """Read an amount of bytes from the socket"""
        result = StringIO()
        count = bytes
        while count > 0:
            try:
                recv = self.socket.recv(bytes, flags)
                logger.debug("II: package size: %i payload: %s" % (len(recv), recv))
            except socket.error, error:
                raise OperationalError(error[1])
            count -= len(recv)
            result.write(recv)
        return result.getvalue()


    def __putblock(self, block):
        """ wrap the line in mapi format and put it into the socket """
        pos = 0
        last = 0
        while not last:
            data = block[pos:pos+MAX_PACKAGE_LENGTH]
            length = len(data)
            if length < MAX_PACKAGE_LENGTH:
                last = 1
            flag = struct.pack( '<H', ( length << 1 ) + last )
            logger.debug("II: sending %i bytes, last: %s" % (length, bool(last)))
            logger.debug("TX: %s" % data)
            try:
                self.socket.send(flag)
                self.socket.send(data)
            except socket.error, error:
                raise OperationalError(error[1])
            pos += length

