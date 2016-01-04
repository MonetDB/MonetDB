# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
functions for converting python objects to monetdb SQL format. If you want
to add support for a specific type you should add a function as a value to
the mapping dict and the datatype as key.
"""

import datetime
import decimal

from monetdb.exceptions import ProgrammingError


def monet_none(data):
    """
    returns a NULL string
    """
    return "NULL"


def monet_bool(data):
    """
    returns "true" or "false"
    """
    return ["false", "true"][bool(data)]


def monet_escape(data):
    """
    returns an escaped string
    """
    data = str(data).replace("\\", "\\\\")
    data = data.replace("\'", "\\\'")
    return "'%s'" % str(data)


def monet_bytes(data):
    """
    converts bytes to string
    """
    return monet_escape(data)


def monet_unicode(data):
    return monet_escape(data.encode('utf-8'))

mapping = (
    (unicode, monet_unicode),
    (str, monet_escape),
    (bytes, monet_bytes),
    (int, str),
    (complex, str),
    (float, str),
    (decimal.Decimal, str),
    (datetime.datetime, monet_escape),
    (datetime.time, monet_escape),
    (datetime.date, monet_escape),
    (datetime.timedelta, monet_escape),
    (bool, monet_bool),
    (type(None), monet_none),
)

mapping_dict = dict(mapping)


def convert(data):
    """
    Return the appropriate convertion function based upon the python type.
    """
    if type(data) in mapping_dict:
        return mapping_dict[type(data)](data)
    else:
        for type_, func in mapping:
            if issubclass(type(data), type_):
                return func(data)
        #if hasattr(data, '__str__'):
        #    return monet_escape
    raise ProgrammingError("type %s not supported as value" % type(data))

