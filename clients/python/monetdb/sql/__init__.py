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
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

from monetdb.monetdb_exceptions import *
from monetdb.sql.connections import Connection
from monetdb.sql.converters import *

apilevel="2.0"
threadsafety=0
paramstyle="pyformat"

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

__all__ = [ 'BINARY', 'Binary', 'connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'MySQLError', 'NULL', 'NUMBER', 'NotSupportedError', 'DBAPISet',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Set', 'Warning', 'apilevel', 'connect', 'connections',
    'constants', 'cursors', 'debug', 'escape', 'escape_dict',
    'escape_sequence', 'escape_string', 'get_client_info',
    'paramstyle', 'string_literal', 'threadsafety', 'version_info']
