# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import logging
import platform

from monetdb.sql import cursors
from monetdb import exceptions
from monetdb import mapi

logger = logging.getLogger("monetdb")


class Connection(object):
    """A MonetDB SQL database connection"""
    default_cursor = cursors.Cursor

    def __init__(self, database, hostname=None, port=50000, username="monetdb",
                 password="monetdb", unix_socket=None, autocommit=False,
                 host=None, user=None):
        """ Set up a connection to a MonetDB SQL database.

        database    -- name of the database
        hostname    -- Hostname where monetDB is running
        port        -- port to connect to (default: 50000)
        username    -- username for connection (default: "monetdb")
        password    -- password for connection (default: "monetdb")
        unix_socket -- socket to connect to. used when hostname not set
                            (default: "/tmp/.s.monetdb.50000")
        autocommit  -- enable/disable auto commit (default: False)

        """

        # The DB API spec is not specific about this
        if host:
            hostname = host
        if user:
            username = user

        if platform.system() == "Windows" and not hostname:
            hostname = "localhost"

        self.mapi = mapi.Connection()
        self.mapi.connect(hostname=hostname, port=int(port), username=username,
                          password=password, database=database, language="sql",
                          unix_socket=unix_socket)
        self.set_autocommit(autocommit)
        self.set_sizeheader(True)
        self.set_replysize(100)

    def close(self):
        """ Close the connection. The connection will be unusable from this
        point forward; an Error  exception will be raised if any operation
        is attempted with the connection. The same applies to all cursor
        objects trying to use the connection.  Note that closing a connection
        without committing the changes first will cause an implicit rollback
        to be performed.
        """
        if self.mapi:
            if not self.autocommit:
                self.rollback()
            self.mapi.disconnect()
            self.mapi = None
        else:
            raise exceptions.Error("already closed")

    def set_autocommit(self, autocommit):
        """
        Set auto commit on or off. 'autocommit' must be a boolean
        """
        self.command("Xauto_commit %s" % int(autocommit))
        self.autocommit = autocommit

    def set_sizeheader(self, sizeheader):
        """
        Set sizeheader on or off. When enabled monetdb will return
        the size a type. 'sizeheader' must be a boolean.
        """
        self.command("Xsizeheader %s" % int(sizeheader))
        self.sizeheader = sizeheader

    def set_replysize(self, replysize):
        self.command("Xreply_size %s" % int(replysize))
        self.replysize = replysize

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
        return self.command('s' + query + '\n;')

    def command(self, command):
        """ use this function to send low level mapi commands """
        self.__mapi_check()
        return self.mapi.cmd(command)

    def __mapi_check(self):
        """ check if there is a connection with a server """
        if not self.mapi:
            raise exceptions.Error("connection closed")
        return True

    def settimeout(self, timeout):
        """ set the amount of time before a connection times out """
        self.mapi.socket.settimeout(timeout)

    def gettimeout(self):
        """ get the amount of time before a connection times out """
        return self.mapi.socket.gettimeout()

    # these are required by the python DBAPI
    Warning = exceptions.Warning
    Error = exceptions.Error
    InterfaceError = exceptions.InterfaceError
    DatabaseError = exceptions.DatabaseError
    DataError = exceptions.DataError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotSupportedError = exceptions.NotSupportedError
