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

"""MonetSQLdb type conversion module

This module handles all the type conversions for MonetSQL. If the default
type conversions aren't what you need, you can make your own. The
dictionary conversions maps some kind of type to a conversion function
which returns the corresponding value:

Key: FIELD_TYPE.* (from MonetSQLdb.constants)
Conversion function:
     Arguments: string
     Returns: Python object

Key: Python type object (from types) or class
Conversion function:
     Arguments: Python object of indicated type or class AND
                conversion dictionary
     Returns: SQL literal value
     Notes: Most conversion functions can ignore the dictionary, but
            it is a required parameter. It is necessary for converting
            things like sequences and instances.

Don't modify conversions if you can avoid it. Instead, make copies
(with the copy() method), modify the copies, and then pass them to
MonetSQLdb.connect().

"""

from string import split, replace, join
import times
import types

def string_literal(s, d):
    s = replace(s, "\\", "\\\\")
    s = replace(s, "'", "\\'")
    return "'%s'" % s


def Str2Set(s):
    values = split(s, ',')
    return apply(Set, tuple(values))

def Thing2Str(s, d):
    """Convert something into a string via str()."""
    return str(s)

def Unicode2Str(s, d):
    """Convert a unicode object to a string using utf-8 encoding."""
    return string_literal(s.encode('utf-8'), d)

# Python 1.5.2 compatibility hack
if str(0L)[-1]=='L':
    def Long2Int(l, d):
        """Convert a long integer to a string, chopping the L."""
        return str(l)[:-1]
else:
    Long2Int = Thing2Str

def None2NULL(o, d):
    """Convert None to NULL."""
    return 'NULL' # duh

def Thing2Literal(o, d):
    return string_literal(o, d)


def Instance2Str(o, d):
    """
    Convert an Instance to a string representation.  If the __str__()
    method produces acceptable output, then you don't need to add the
    class to conversions; it will be handled by the default
    converter. If the exact class is not found in d, it will use the
    first class it can find for which o is an instance.
    """

    if d.has_key(o.__class__):
        return d[o.__class__](o, d)
    cl = filter(lambda x,o=o:
                type(x) is types.ClassType
                and isinstance(o, x), d.keys())
    if not cl and hasattr(types, 'ObjectType'):
        cl = filter(lambda x,o=o:
                    type(x) is types.TypeType
                    and isinstance(o, x), d.keys())
    if not cl:
        return d[types.StringType](o,d)
    d[o.__class__] = d[cl[0]]
    return d[cl[0]](o, d)

def array2Str(o, d):
    return Thing2Literal(o.tostring(), d)

def escape_sequence(x, d):
    return Thing2Literal(str(x), d)

def escape_dict(x, d):
    return Thing2Literal(str(x), d)

def str2bool(x):
    if type(x) == types.StringType:
        return x.lower()[0]=='t'
    return bool(x)

def str2blob(x):
    """ Converts a monet blob in string representation to a python string.
    The input is a string in the format: '(length: char char char char ... )'
    w/ char in hex representation. The output is the string of chars. """
    x = x[x.find(":")+2:-1]
    return join(map(lambda x: chr(int(x, 16)), x.split(" ")), '')


conversions = {
    # Python -> Monet
    types.IntType: Thing2Str,
    types.LongType: Long2Int,
    types.FloatType: Thing2Str,
    types.NoneType: None2NULL,
    types.TupleType: escape_sequence,
    types.ListType: escape_sequence,
    types.DictType: escape_dict,
    types.InstanceType: Instance2Str,
    # array.ArrayType: array2Str,
    # FIXME: date/time types
    types.StringType: Thing2Literal, # default

    # Monet SQL -> Python
    "varchar": str,
    "tinyint": int,
    "shortint": int,
    "mediumint": int,
    "bigint": long,
    "longint": long,
    "boolean": str2bool,
    "decimal": float,
    "float": float,
    "double": float,
    "real": float,
    "blob": str2blob,
    # timestamp types
    "date": times.fromDate,
    "time": times.fromTime,
    "timestamp": times.fromTimestamp,

    # Monet MIL -> Python
    "str": str,
    "bte": int,
    "sht": int,
    "int": int,
    "lng": long,
    "wrd": long,
    "bit": bool,
    "chr": str,
    "flt": float,
    "dbl": float,
    "oid": str
    }


if hasattr(types, 'UnicodeType'):
    conversions[types.UnicodeType] = Unicode2Str

if hasattr(types, 'ObjectType'):
    conversions[types.ObjectType] = Instance2Str

def escape(x, conv=None):
    """ Escapes """

    if conv is None: conv = conversions

    if type(x) is types.ListType or type(x) is types.TupleType:
        y = []
        for a in x:
            if conv.has_key(type(a)):
                y.append(conv[type(a)](a, conv))
        if type(x) is types.TupleType:
            y = tuple(y)
        return y
    if type(x) is types.DictType:
        for a in x:
            if conv.has_key(type(x[a])):
                x[a] = conv[type(x[a])](x[a], conv)
        return x

    # single value
    if conv.has_key(type(x)):
        x = conv[type(x)](x, conv)
    return x


def monet2python(value, typestr, conv=None):
    if value is None: return None
    if conv is None: conv = conversions
    if conv.has_key(typestr):
        return conv[typestr](value)
    return value
