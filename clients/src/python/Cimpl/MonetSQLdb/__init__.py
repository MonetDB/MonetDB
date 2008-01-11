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
# Portions created by CWI are Copyright (C) 1997-2008 CWI.
# All Rights Reserved.

"""MonetSQLdb - A DB API v2.0 compatible interface to the Monet Database System

This package is a wrapper around Mapi.py, which mostly implements the
Monet C API (Mapi).

connect() -- connects to server

For information on how MonetSQLdb handles type conversion, see the
MonetSQLdb.converters module.

"""

__author__ = "Arjan Scherpenisse <Arjan.Scherpenisse@cwi.nl>"
__revision__ = """$Revision$"""[11:-2]
version_info = (
    0,
    1,
    0,
    "beta",
    1)
if version_info[3] == "final": __version__ = "%d.%d.%d" % version_info[:3]
else: __version__ = "%d.%d.%d%1.1s%d" % version_info[:5]

threadsafety = 1
apilevel = "2.0"
paramstyle = "pyformat"


try:
    from MonetDB.CMapi import *
except ImportError:
    # if run from the build directory, CMapi is not in the MonetDB module
    from CMapi import *

from monetexceptions import *
import cursors, converters

def Connect(**kwargs):
    """Factory function for connections.Connection."""
    return Connection(**kwargs)

connect = Connect

def defaulterrorhandler(connection, cursor, errorclass, errorvalue):
    """

    If cursor is not None, (errorclass, errorvalue) is appended to
    cursor.messages; otherwise it is appended to
    connection.messages. Then errorclass is raised with errorvalue as
    the value.

    You can override this with your own error handler by assigning it
    to the instance.

    """
    error = errorclass, errorvalue
    if cursor:
        cursor.messages.append(error)
    else:
        connection.messages.append(error)
    raise errorclass, errorvalue

class Connection:

    """ Create a connection to the MonetDB database.
    Parameters to pass to the constructor:

    host -- string, host to connect
    user -- string, user to connect as
    password -- string, password to use
    port -- int, port
    lang -- string, language (default SQL)
    unicode -- boolean
    cursorclass -- the class of the cursor to use

    or to create an embedded MonetDB database

    dbfarm -- string, dbfarm location
    dbname -- string, database name
    """

    _mapi = None

    default_cursor = cursors.Cursor
    messages = []
    converter = None

    errorhandler = defaulterrorhandler

    def __init__(self, **kwargs):
        if not kwargs.has_key('dbfarm'):
            if not kwargs.has_key('host'): kwargs['host'] = 'localhost'
            if not kwargs.has_key('port'): kwargs['port'] = 50000
            if not kwargs.has_key('user'): kwargs['user'] = 'monetdb'
            if not kwargs.has_key('password'): kwargs['password'] = 'monetdb'
        else:
            if not kwargs.has_key('version'): kwargs['version'] = 5
        if not kwargs.has_key('dbname'): kwargs['dbname'] = 'demo'
        if not kwargs.has_key('lang'): kwargs['lang'] = 'sql'

        self.lang = kwargs['lang']

        if kwargs.has_key('cursorclass'):
            self.cursorclass = kwargs['cursorclass']
        else:
            self.cursorclass = self.default_cursor

        if kwargs.has_key('converter'):
            self.converter = kwargs['converter']
        else:
            self.converter = converters.conversions

        if not kwargs.has_key('dbfarm'):
            self._mapi = Mapi(kwargs['host'], kwargs['port'], kwargs['user'], kwargs['password'], kwargs['lang'], kwargs['dbname'])
        else:
            self._mapi = Embedded(kwargs['dbfarm'], kwargs['dbname'], kwargs['lang'], kwargs['version'])

    def close(self):
        del self._mapi

    def commit(self):
        """Commit the current transaction."""
        if self.lang == 'mil':
            self._mapi.query('commit();')
        else:
            self._mapi.query("COMMIT")

    def rollback(self):
        """Rollback the current transaction."""
        if self.lang == 'mil':
            self._mapi.query('abort();')
        else:
            self._mapi.query("ROLLBACK")


    def cursor(self, cursorclass=None):
        """
        Create a cursor on which queries may be performed. The
        optional cursorclass parameter is used to create the
        Cursor. By default, self.cursorclass=cursors.Cursor is
        used.
        """
        return (cursorclass or self.cursorclass)(self)

    def literal(self, o):
        """

        If o is a single object, returns an SQL literal as a string.
        If o is a non-string sequence, the items of the sequence are
        converted and returned as a sequence.

        Non-standard.

        """
        return converters.escape(o, self.converter)

    errorhandler = defaulterrorhandler


__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'MySQLError', 'NULL', 'NUMBER', 'NotSupportedError', 'DBAPISet',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Set', 'Warning', 'apilevel', 'connect', 'connections',
    'constants', 'cursors', 'debug', 'escape', 'escape_dict',
    'escape_sequence', 'escape_string', 'get_client_info',
    'paramstyle', 'string_literal', 'threadsafety', 'version_info']
