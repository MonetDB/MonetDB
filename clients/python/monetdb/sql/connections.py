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

import sys
import logging

from monetdb.sql import cursors
from monetdb.monetdb_exceptions import *
from monetdb import mapi

logger = logging.getLogger("monetdb")

class Connection:
    """This represents a MonetDB SQL database connection"""
    default_cursor = cursors.Cursor


    def __init__(self, username="monetdb", password="monetdb",
        hostname="localhost", port=50000, database="demo", autocommit=False,
        use_unicode=False, user=None, host=None):
        """ Set up a connection to a MonetDB SQL database.

        user/username -- username for connection (default: monetdb)
        password -- password for connection (default: monetdb)
        host/hostname -- hostname to connect to (default: localhost)
        port -- port to connect to (default: 50000)
        database -- name of the database (default: demo)
        autocommit -- enable/disable auto commit (default: False,
                      required by DBAPI)
        use_unicode -- use unicode for strings or not (default: False,
                       only has effect on python2 since python3 is
                       always unicode)

        If user and username are provided, user overrides the value of username.
        If host and hostname are provided, host overrides the value of hostname.
        """

        if user is not None:
            username = user
        if host is not None:
            hostname = host
        self.mapi = mapi.Server()
        self.autocommit = autocommit
        self.mapi.connect(hostname=hostname, port=int(port), username=username,
            password=password, database=database, language="sql")
        self.set_autocommit(self.autocommit)
        self.use_unicode=use_unicode


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
            if not self.autocommit:
                self.rollback()
            self.mapi.disconnect()
            self.mapi = None
        else:
            raise Error("already closed")


    def set_autocommit(self, autocommit):
        """
        Set auto commit on or off. 'autocommit' must be a boolean
        """
        self.command("Xauto_commit %s" % int(autocommit))
        self.autocommit = autocommit



    def commit(self):
        """
        Commit any pending transaction to the database. Note that
        if the database supports an auto-commit feature, this must
        be initially off. An interface method may be provided to
        turn it back on.

        Database modules that do not support transactions should
        implement this method with void functionality.
        """

        self.__mapi_check()
        return self.cursor().execute('COMMIT')
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

        self.__mapi_check()
        return self.cursor().execute('ROLLBACK')
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
        """ use this function to send low level mapi commands """
        self.__mapi_check()
        return self.mapi.cmd(command)


    def __mapi_check(self):
        """ check if there is a connection with a server """
        if not self.mapi:
            raise Error("connection closed")
        return True


    # these are required by the python DBAPI
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

