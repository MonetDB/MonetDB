# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
definition of MonetDB column types, for more info:
http://www.monetdb.org/Documentation/Manuals/SQLreference/Datatypes
"""

CHAR = 'char'                      # (L) character string with length L
VARCHAR = 'varchar'                # (L) string with atmost length L
CLOB = 'clob'
BLOB = 'blob'
DECIMAL = 'decimal'                # (P,S)
SMALLINT = 'smallint'              # 16 bit integer
INT = 'int'                        # 32 bit integer
BIGINT = 'bigint'                  # 64 bit integer
HUGEINT = 'hugeint'                # 128 bit integer
SERIAL = 'serial'                  # special 64 bit integer sequence generator
REAL = 'real'                      # 32 bit floating point
DOUBLE = 'double'                  # 64 bit floating point
BOOLEAN = 'boolean'
DATE = 'date'
TIME = 'time'                      # (T) time of day
TIMESTAMP = 'timestamp'            # (T) date concatenated with unique time
INTERVAL = 'interval'              # (Q) a temporal interval

MONTH_INTERVAL = 'month_interval'
SEC_INTERVAL = 'sec_interval'
WRD = 'wrd'
TINYINT = 'tinyint'

URL = 'url'
INET = 'inet'

# Not on the website:
SHORTINT = 'shortint'
MEDIUMINT = 'mediumint'
LONGINT = 'longint'
FLOAT = 'float'
TIMESTAMPTZ = 'timestamptz'
TIMETZ = 'timetz'


# full names and aliases, spaces are replaced with underscores
CHARACTER = CHAR
CHARACTER_VARYING = VARCHAR
CHARACHTER_LARGE_OBJECT = CLOB
BINARY_LARGE_OBJECT = BLOB
NUMERIC = DECIMAL
DOUBLE_PRECISION = DOUBLE
