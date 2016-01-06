# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from monetdb.sql.connections import Connection
from monetdb.sql.pythonize import *
from monetdb.exceptions import *

apilevel = "2.0"
threadsafety = 0
paramstyle = "pyformat"


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

connect.__doc__ = Connection.__init__.__doc__

__all__ = ['BINARY', 'Binary', 'connect', 'Connection', 'DATE',
           'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
           'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
           'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
           'MySQLError', 'NULL', 'NUMBER', 'NotSupportedError', 'DBAPISet',
           'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
           'TIMESTAMP', 'Set', 'Warning', 'apilevel', 'connect',
           'connections', 'constants', 'cursors', 'debug', 'escape',
           'escape_dict', 'escape_sequence', 'escape_string',
           'get_client_info', 'paramstyle', 'string_literal', 'threadsafety',
           'version_info']
