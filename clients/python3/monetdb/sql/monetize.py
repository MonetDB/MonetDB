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

"""
functions for converting python objects to monetdb SQL format
"""

import datetime
import logging
import decimal

logger = logging.getLogger("monetdb")

def monet_none(data):
    return "NULL"

def monet_bool(data):
    if data:
        return "true"
    else:
        return "false"

def monet_escape(data):
    data = str(data).replace( "\\", "\\\\")
    data = data.replace( "\'", "\\\'")
    return "'%s'" % str(data)

def monet_string(data):
    return str(data)

def monet_bytes(data):
    return monet_escape(data)

mapping = {
    type(None): monet_none,
    bool: monet_bool,
    int: monet_string,
    float: monet_string,
    complex: monet_string,
    int: monet_string,
    str: monet_escape,
    datetime.datetime: monet_escape,
    datetime.time: monet_escape,
    decimal.Decimal: monet_string,
    datetime.timedelta: monet_escape,
    datetime.date: monet_escape,
    bytes: monet_bytes,
}

def convert(data):
    try:
        return mapping[type(data)](data)
    except KeyError:
        raise ProgrammingError("type %s not supported as value" % type(data))