# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
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

import datetime
import time
import sys
import logging
import decimal

from monetdb.sql import type_codes
from monetdb.monetdb_exceptions import *

logger = logging.getLogger("monetdb")


class Pythonizer:
    """ convert mapi type to python type """

    def __init__(self, use_unicode=False):
        self.mapping = {
            type_codes.CHAR: self.__strip,
            type_codes.VARCHAR: self.__strip,
            type_codes.CLOB: self.__strip,
            type_codes.BLOB: self.__string,
            type_codes.DECIMAL: self.__decimal,
            type_codes.SMALLINT: int,
            type_codes.INT: int,
            type_codes.WRD: int,
            type_codes.BIGINT: int,
            type_codes.SERIAL: int,
            type_codes.REAL: float,
            type_codes.DOUBLE: float,
            type_codes.BOOLEAN: self.__bool,
            type_codes.DATE: self.__date,
            type_codes.TIME: self.__time,
            type_codes.TIMESTAMP: self.__timestamp,
            type_codes.TIMESTAMPTZ: self.__timestamptz,
            type_codes.INTERVAL: self.__strip,
            type_codes.MONTH_INTERVAL: self.__strip,
            type_codes.SEC_INTERVAL: self.__strip,
            type_codes.TINYINT: int,
            type_codes.SHORTINT: int,
            type_codes.MEDIUMINT: int,
            type_codes.LONGINT: int,
            type_codes.FLOAT: float,
        }

        self.use_unicode = use_unicode

    def __string(self, data):
        return str(data)

    def __strip(self, data):
        """ returns a python string, chops off quotes,
        replaces escape characters.
        inverse of escape"""

        c_escapes = {'n':'\n', 't':'\t', 'r':'\r', '"':'\"'}
        a = []
        n = 0
        for c in data:
            if c == '\\':
                n = n + 1
            else:
                if n > 0:
                    if n % 2 == 0:
                        # even number of slashes: '\' '\' 'n' --> '\' 'n'
                        a.extend(['\\'] * int(n/2))
                        a.append(c)
                        n = 0
                    else:
                        # odd number of slashes: '\' '\' '\' 'n' --> '\' '\n'
                        a.extend(['\\'] * int((n - 1)/2))
                        if c in c_escapes.keys():
                            a.append(c_escapes[c])
                        else:
                            logging.warning('unsupported escape character: \\%s' % c)
                        n = 0
                else:
                    a.append(c)
                    n = 0
        if n > 0:
            a.extend(['\\'] * (n/2))
            a.append(c)
        data = ''.join(a)


        if self.use_unicode and sys.version_info[0] < 3:
            return unicode(data[1:-1].decode("UTF-8"))
        return data[1:-1]

    def __decimal(self, data):
        return decimal.Decimal(data)

    def __bool(self, data):
        """ return python boolean """
        return (data == "true")

    def __time(self, data):
        return Time(*[int(float(x)) for x in data.split(':')])

    def __date(self, data):
        return Date(*[int(float(x)) for x in data.split('-')])

    def __timestamp(self, data):
        splitted = data.split(" ")
        date = [int(float(x)) for x in splitted[0].split('-')]
        time = [int(float(x)) for x in splitted[1].split(':')]
        return Timestamp(*date+time)

    def __timestamptz(self, data):
        if data.find('+')!= -1:
            (dt, tz) = data.split("+")
            (tzhour, tzmin) = [int(x) for x in tz.split(':')]
        elif data.find('-')!= -1:
            (dt, tz) = data.split("-")
            (tzhour, tzmin) = [int(x) for x in tz.split(':')]
            tzhour = tzhour * -1
            tzmin = tzmin * -1
        else:
            raise ProgrammingError("no + or - in %s" % data)

        (datestr, timestr) = dt.split(" ")
        date = [int(float(x)) for x in datestr.split('-')]
        time = [int(float(x)) for x in timestr.split(':')]
        year, month, day = date
        hour, minute, second = time
        return Timestamp(year, month, day, hour+tzhour, minute+tzmin, second)


    def __blob(self, x):
        """ Converts a monetdb blob in string representation to a python string.
        The input is a string in the format: '(length: char char char char ... )'
        w/ char in hex representation. The output is the string of chars. """
        #TODO: need to check if blob datatype should use this? otherwise
        # this can be removed
        x = x[x.find(":")+2:-1]
        return ''.join(map(lambda x: chr(int(x, 16)), x.split(" ")))

    def convert(self, data, type_code):
        # null values should always be replaced by None type
        if data == "NULL":
            return None

        try:
            return self.mapping[type_code](data)
        except KeyError:
            raise ProgrammingError("type %s is not supported by Python API" % type_code)


class Monetizer:
    """ convert python type to mapi type """

    def __init__(self):
        self.mapping = {
            type(None): self.__none,
            bool: self.__bool,
            int: self.__string,
            float: self.__string,
            complex: self.__string,
            int: self.__string,
            str: self.__escape,
            datetime.datetime: self.__escape,
            datetime.time: self.__escape,
            decimal.Decimal: self.__string,
            datetime.timedelta: self.__escape,
            datetime.date: self.__escape,
            # I don't think these should be used:
            #list: self.__string,
            #tuple: self.__string,
            #range: self.__string,
            #set: self.__string,
            #frozenset: self.__string,
            #dict: self.__string,
            #Ellipsis: self.__string,
            #self.mapping[bytearray] = self.__string # python2.6 only
        }

        (major, minor, micro, level, serial)  = sys.version_info
        if (major == 3) or (major == 2 and minor == 6):
            # bytes type is only supported by python 2.6 and higher
            self.mapping[bytes] = self.__bytes
        if (major != 3):
            self.mapping[unicode] = self.__unicode

    def convert(self, data):
        try:
            return self.mapping[type(data)](data)
        except KeyError:
            raise ProgrammingError("type %s not supported as value" %
                    type(data))

    def __none(self, data):
        return "NULL"

    def __bool(self, data):
        if data:
            return "true"
        else:
            return "false"

    def __escape(self, data):
        data = str(data).replace( "\\", "\\\\")
        data = data.replace( "\'", "\\\'")
        return "'%s'" % str(data)

    def __string(self, data):
        return str(data)

    def __bytes(self, data):
        return self.__escape(data)

    def __unicode(self, data):
        return self.__escape(data.encode('utf-8'))



# everything below is kind of pointless but required by DB API
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

def Binary(x):
    #return x.encode("hex").upper()
    return ''.join([hex(ord(i))[2:] for i in x]).upper()


class DBAPISet(frozenset):
    """A special type of set for which A == x is true if A is a
    DBAPISet and x is a member of that set."""

    def __ne__(self, other):
        from sets import BaseSet
        if isinstance(other, BaseSet):
            return super(DBAPISet.self).__ne__(self, other)
        else:
            return other not in self

    def __eq__(self, other):
        if isinstance(other, set):
            return super(DBAPISet, self).__eq__(self, other)
        else:
            return other in self


STRING    = DBAPISet([type_codes.VARCHAR])
BINARY    = DBAPISet([type_codes.BLOB])
NUMBER    = DBAPISet([type_codes.DECIMAL, type_codes.DOUBLE, type_codes.REAL,
    type_codes.BIGINT, type_codes.SMALLINT])
DATE      = DBAPISet([type_codes.DATE])
TIME      = DBAPISet([type_codes.TIME])
TIMESTAMP = DBAPISet([type_codes.TIMESTAMP])
DATETIME  = TIMESTAMP
ROWID     = DBAPISet()

