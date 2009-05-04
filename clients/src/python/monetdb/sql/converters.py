
import datetime
import types
import time

from monetdb.sql import type_codes





class Pythonizer:
    """ convert mapi type to python type """

    def __init__(self):
        self.mapping = {
            type_codes.CHAR: self.__string,
            type_codes.VARCHAR: self.__string,
            type_codes.CLOB: self.__blob, # TODO: check this
            type_codes.BLOB: self.__string, # TODO: check this
            type_codes.DECIMAL: int,
            type_codes.SMALLINT: int,
            type_codes.INT: int,
            type_codes.BIGINT: int,
            type_codes.SERIAL: int,
            type_codes.REAL: int,
            type_codes.DOUBLE: int,
            type_codes.BOOLEAN: self.__bool,
            type_codes.DATE: self.__date,
            type_codes.TIME: self.__time,
            type_codes.TIMESTAMP: self.__timestamp,
            type_codes.INTERVAL: self.__string, # TODO: check this
            type_codes.TINYINT: int,
            type_codes.SHORTINT: int,
            type_codes.MEDIUMINT: int,
            type_codes.LONGINT: int,
            type_codes.FLOAT: float,

            # Mil Support  TODO: do we need this?
            "str": str,
            "bte": int,
            "sht": int,
            "int": int,
            "lng": int,
            "wrd": int,
            "bit": bool,
            "chr": str,
            "flt": float,
            "dbl": float,
            "oid": str
        }

    def __string(self, data):
        """ returns a python string, chops of quotes """
        #str = str.replace("\\\\", "\\")
        #str = str.replace("\\'", "'")
        #str = str.replace('\\"', '"')
        return data[1:-1]

    def __bool(self, data):
        """ return python boolean """
        return (data == "true")

    def __time(self, x):
        return apply(Time, map(lambda x: int(float(x)), x.split(":")))

    def __date(self, x):
        return apply(Date, map(lambda x: int(float(x)), x.split("-")))

    def __timestamp(self, x):
        x = x.split(" ")
        return apply(Timestamp,
                     map(lambda x: int(float(x)), x[0].split("-")) +
                     map(lambda x: int(float(x)), x[1].split(":"))
                     )

    def __blob(self, x):
        """ Converts a monet blob in string representation to a python string.
        The input is a string in the format: '(length: char char char char ... )'
        w/ char in hex representation. The output is the string of chars. """
        x = x[x.find(":")+2:-1]
        return ''.join(map(lambda x: chr(int(x, 16)), x.split(" ")))

    def convert(self, data, type_code):
        # null values should always be replaced by None type
        if data == "NULL":
            return None

        return self.mapping[type_code](data)


class Monetizer:
    """ convert python type to mapi type """

    def __init__(self):
        self.mapping = {
            None: self.__none,
            bool: self.__bool,
            int: self.__string,
            float: self.__string,
            complex: self.__string,
            int: self.__string,
            str: self.__escape,
            bytes: self.__bytes,
            bytearray: self.__string, # TODO: check this
            list: self.__string, # TODO: check this
            tuple: self.__string, # TODO: check this
            range: self.__string, # TODO: check this
            set: self.__string, # TODO: check this
            frozenset: self.__string, # TODO: check this
            dict: self.__string, # TODO: check this
            Ellipsis: self.__string, # TODO: check this
        }

    def convert(self, data):
        return self.mapping[type(data)](data)

    def __none(self, data):
        return "NULL"

    def __bool(self, data):
        if data:
            return "true"
        else:
            return "false"

    def __escape(self, data):
        data = data.replace( "\\", "\\\\")
        data = data.replace( "'", "\\'")
        return "'%s'" % str(data)

    def __string(self, data):
        return str(data)

    def __bytes(self, data):
        return self.__escape(data.encode())




# required by DB API
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
    return str(x)


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
NUMBER    = DBAPISet([type_codes.DECIMAL, type_codes.DOUBLE, type_codes.REAL, type_codes.BIGINT, type_codes.SMALLINT])
DATE      = DBAPISet([type_codes.DATE])
TIME      = DBAPISet([type_codes.TIME])
TIMESTAMP = DBAPISet([type_codes.TIMESTAMP])
DATETIME  = TIMESTAMP
ROWID     = DBAPISet()



