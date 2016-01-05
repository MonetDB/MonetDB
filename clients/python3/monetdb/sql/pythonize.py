# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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
    sign_symbol = data[-6]

    if sign_symbol == '+':
        sign = 1
    elif sign_symbol == '-':
        sign = -1
    else:
        raise ProgrammingError("no + or - in %s" % data)

    return data[:-6], datetime.timedelta(hours=sign * int(data[-5:-3]), minutes=sign * int(data[-2:]))

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
    if '.' in data:
        return datetime.datetime.strptime(data, '%H:%M:%S.%f').time()
    else:
        return datetime.datetime.strptime(data, '%H:%M:%S').time()


def py_timetz(data):
    """ returns a python Time where data contains a tz code
    """
    t, timezone_delta = _extract_timezone(data)
    if '.' in t:
        return (datetime.datetime.strptime(t, '%H:%M:%S.%f') + timezone_delta).time()
    else:
        return (datetime.datetime.strptime(t, '%H:%M:%S') + timezone_delta).time()


def py_date(data):
    """ Returns a python Date
    """
    return datetime.datetime.strptime(data, '%Y-%m-%d').date()


def py_timestamp(data):
    """ Returns a python Timestamp
    """
    if '.' in data:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')
    else:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S')

def py_timestamptz(data):
    """ Returns a python Timestamp where data contains a tz code
    """
    dt, timezone_delta = _extract_timezone(data)
    if '.' in dt:
        return datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S.%f') + timezone_delta
    else:
        return datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') + timezone_delta


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
    types.HUGEINT: int,
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
