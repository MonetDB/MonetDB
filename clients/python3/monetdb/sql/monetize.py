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
# Copyright August 2008-2015 MonetDB B.V.
# All Rights Reserved.

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


mapping = (
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

