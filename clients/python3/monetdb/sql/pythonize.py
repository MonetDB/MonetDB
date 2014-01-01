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
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

"""
functions for converting monetdb SQL fields to Python objects
"""

import time
import datetime
import re
from decimal import Decimal
from monetdb.sql import types
from monetdb.exceptions import ProgrammingError


def _extract_timezone(data):
    if data.find('+') != -1:
        (dt, tz) = data.split("+")
        (tzhour, tzmin) = [int(x) for x in tz.split(':')]
    elif data.find('-') != -1:
        (dt, tz) = data.split("-")
        (tzhour, tzmin) = [int(x) for x in tz.split(':')]
        tzhour *= -1
        tzmin *= -1
    else:
        raise ProgrammingError("no + or - in %s" % data)

    return dt, tzhour, tzmin


def strip(data):
    """ returns a python string, with chopped off quotes,
    and replaced escape characters"""
    return ''.join([w.encode('utf-8').decode('unicode_escape')
                    if '\\' in w
                    else w
                    for w in re.split('([\000-\200]+)', data[1:-1])])


def py_bool(data):
    """ return python boolean """
    return data == "true"


def py_time(data):
    """ returns a python Time
    """
    return Time(*[int(float(x)) for x in data.split(':')])


def py_timetz(data):
    """ returns a python Time where data contains a tz code
    """
    dt, tzhour, tzmin = _extract_timezone(data)
    hour, minute, second = [int(float(x)) for x in dt.split(':')]
    return Time(hour + tzhour, minute + tzmin, second)


def py_date(data):
    """ Returns a python Date
    """
    return Date(*[int(float(x)) for x in data.split('-')])


def py_timestamp(data):
    """ Returns a python Timestamp
    """
    splitted = data.split(" ")
    date = [int(float(x)) for x in splitted[0].split('-')]
    time = [int(float(x)) for x in splitted[1].split(':')]
    return Timestamp(*(date + time))


def py_timestamptz(data):
    """ Returns a python Timestamp where data contains a tz code
    """
    dt, tzhour, tzmin = _extract_timezone(data)
    (datestr, timestr) = dt.split(" ")
    date = [int(float(x)) for x in datestr.split('-')]
    time = [int(float(x)) for x in timestr.split(':')]
    year, month, day = date
    hour, minute, second = time
    return Timestamp(year, month, day, hour + tzhour, minute + tzmin, second)


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
    types.TIMETZ: py_timetz,
    types.INTERVAL: strip,
    types.MONTH_INTERVAL: strip,
    types.SEC_INTERVAL: strip,
    types.TINYINT: int,
    types.SHORTINT: int,
    types.MEDIUMINT: int,
    types.LONGINT: int,
    types.FLOAT: float,
    types.URL: strip,
    types.INET: str,
}


def convert(data, type_code):
    """
    Calls the appropriate convertion function based upon the python type
    """

    # null values should always be replaced by None type
    if data == "NULL":
        return None
    try:
        return mapping[type_code](data)
    except KeyError:
        raise ProgrammingError("type %s is not supported" % type_code)


# below us stuff required by the DBAPI

def Binary(data):
    """returns binary encoding of data"""
    return ''.join(["%02X" % ord(i) for i in data])


def DateFromTicks(ticks):
    """Convert ticks to python Date"""
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Convert ticks to python Time"""
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Convert ticks to python Timestamp"""
    return Timestamp(*time.localtime(ticks)[:6])

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
STRING = types.VARCHAR
BINARY = types.BLOB
NUMBER = types.DECIMAL
DATE = types.DATE
TIME = types.TIME
DATETIME = types.TIMESTAMP
ROWID = types.INT
