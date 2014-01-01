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
