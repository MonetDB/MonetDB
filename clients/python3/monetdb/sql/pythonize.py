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
# Copyright August 2008-2012 MonetDB B.V.
# All Rights Reserved.

import logging
import time
import datetime
from decimal import Decimal
from monetdb.sql import type_codes

logger = logging.getLogger("monetdb")

def strip(data):
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

    return data[1:-1]

def py_bool(data):
    """ return python boolean """
    return (data == "true")

def py_time(data):
    return Time(*[int(float(x)) for x in data.split(':')])

def py_date(data):
    return Date(*[int(float(x)) for x in data.split('-')])

def py_timestamp(data):
    splitted = data.split(" ")
    date = [int(float(x)) for x in splitted[0].split('-')]
    time = [int(float(x)) for x in splitted[1].split(':')]
    return Timestamp(*date+time)

def py_timestamptz(data):
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

def py_blob(x):
    """ Converts a monetdb blob in string representation to a python string.
    The input is a string in the format: '(length: char char char char ... )'
    w/ char in hex representation. The output is the string of chars. """
    #TODO: need to check if blob datatype should use this? otherwise
    # this can be removed
    x = x[x.find(":")+2:-1]
    return ''.join(map(lambda x: chr(int(x, 16)), x.split(" ")))

mapping = {
    type_codes.CHAR: strip,
    type_codes.VARCHAR: strip,
    type_codes.CLOB: strip,
    type_codes.BLOB: str,
    type_codes.DECIMAL: Decimal,
    type_codes.SMALLINT: int,
    type_codes.INT: int,
    type_codes.WRD: int,
    type_codes.BIGINT: int,
    type_codes.SERIAL: int,
    type_codes.REAL: float,
    type_codes.DOUBLE: float,
    type_codes.BOOLEAN: py_bool,
    type_codes.DATE: py_date,
    type_codes.TIME: py_time,
    type_codes.TIMESTAMP: py_timestamp,
    type_codes.TIMESTAMPTZ: py_timestamptz,
    type_codes.INTERVAL: strip,
    type_codes.MONTH_INTERVAL: strip,
    type_codes.SEC_INTERVAL: strip,
    type_codes.TINYINT: int,
    type_codes.SHORTINT: int,
    type_codes.MEDIUMINT: int,
    type_codes.LONGINT: int,
    type_codes.FLOAT: float,
}

def convert(data, type_code):
    # null values should always be replaced by None type
    if data == "NULL":
        return None
    try:
        return mapping[type_code](data)
    except KeyError:
        raise ProgrammingError("type %s is not supported" % type_code)


# below us stuff required by the DBAPI

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime

def Binary(x):
    #return x.encode("hex").upper()
    return ''.join([hex(ord(i))[2:] for i in x]).upper()

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

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
