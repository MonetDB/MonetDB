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
# Copyright August 2008-2013 MonetDB B.V.
# All Rights Reserved.

"""
functions for converting monetdb SQL fields to Python objects
"""

import logging
import time
import datetime
from decimal import Decimal
from monetdb.sql import types
import monetdb.exceptions
import re

logger = logging.getLogger("monetdb")

def strip(data):
    """ returns a python string, chops off quotes,
    replaces escape characters"""
    return ''.join([w.encode('utf-8').decode('unicode_escape') if '\\' in w else w for w in re.split('([\000-\200]+)', data[1:-1])])


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
    types.CHAR: strip,
    types.VARCHAR: strip,
    types.CLOB: strip,
    types.BLOB: str,
    types.DECIMAL: Decimal,
    types.SMALLINT: int,
    types.INT: int,
    types.WRD: int,
    types.BIGINT: int,
    types.SERIAL: int,
    types.REAL: float,
    types.DOUBLE: float,
    types.BOOLEAN: py_bool,
    types.DATE: py_date,
    types.TIME: py_time,
    types.TIMESTAMP: py_timestamp,
    types.TIMESTAMPTZ: py_timestamptz,
    types.INTERVAL: strip,
    types.MONTH_INTERVAL: strip,
    types.SEC_INTERVAL: strip,
    types.TINYINT: int,
    types.SHORTINT: int,
    types.MEDIUMINT: int,
    types.LONGINT: int,
    types.FLOAT: float,
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

STRING    = DBAPISet([types.VARCHAR])
BINARY    = DBAPISet([types.BLOB])
NUMBER    = DBAPISet([types.DECIMAL, types.DOUBLE, types.REAL,
                      types.BIGINT, types.SMALLINT])
DATE      = DBAPISet([types.DATE])
TIME      = DBAPISet([types.TIME])
TIMESTAMP = DBAPISet([types.TIMESTAMP])
DATETIME  = TIMESTAMP
ROWID     = DBAPISet()
