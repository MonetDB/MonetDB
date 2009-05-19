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
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2009 MonetDB B.V.
# All Rights Reserved.

from monetdb.sql import cursors

try:
    from monetdb import mapi
except SyntaxError:
    # python 2.5 support
    from monetdb import mapi25 as mapi

from monetdb.monetdb_exceptions import *


class Connection:
    """MonetDB Connection Object"""
    default_cursor = cursors.Cursor


    def __init__(self, username="monetdb", password="monetdb", hostname="localhost", port=50000, database="demo"):
        self.mapi = mapi.Server()
        self.mapi.connect(hostname, port, username, password, database, language="sql")


    def close(self):
        """ Close the connection now (rather than whenever __del__ is
        called).  The connection will be unusable from this point
        forward; an Error (or subclass) exception will be raised
        if any operation is attempted with the connection. The
        same applies to all cursor objects trying to use the
        connection.  Note that closing a connection without
        committing the changes first will cause an implicit
        rollback to be performed.
        """
        if self.mapi:
            self.mapi.disconnect()
            self.mapi = None
        else:
            raise Error("already closed")


    def commit(self):
        """
        Commit any pending transaction to the database. Note that
        if the database supports an auto-commit feature, this must
        be initially off. An interface method may be provided to
        turn it back on.

        Database modules that do not support transactions should
        implement this method with void functionality.
        """

        # TODO: implement
        return False
        #self.__mapi_check()
        #return self.execute('COMMIT')



    def rollback(self):
        """
        This method is optional since not all databases provide
        transaction support.

        In case a database does provide transactions this method
        causes the database to roll back to the start of any
        pending transaction.  Closing a connection without
        committing the changes first will cause an implicit
        rollback to be performed.
        """
        # TODO: implement
        return False
        #self.__mapi_check()
        #return self.execute('ROLLBACK')



    def cursor(self):
        """
        Return a new Cursor Object using the connection.  If the
        database does not provide a direct cursor concept, the
        module will have to emulate cursors using other means to
        the extent needed by this specification.
        """
        return cursors.Cursor(self)


    def execute(self, query):
        """ use this for executing SQL queries """
        return self.command('s' + query + ';')


    def command(self, command):
        """ use this to send mapi commands """
        self.__mapi_check()
        return self.mapi.cmd(command)


    def __mapi_check(self):
        if not self.mapi:
            raise Error("connection closed")


    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

