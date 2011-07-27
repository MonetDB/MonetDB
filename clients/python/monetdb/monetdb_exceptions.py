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

# DBAPI states that we should subclass StandardError.
# StandardError is depricated in python 3.0, so we use Exception

class Warning(Exception):
    """Exception raised for important warnings like data
    truncations while inserting, etc. It must be a subclass of
    the Python StandardError (defined in the module
    exceptions)."""
    pass

class Error(Exception):
    """Exception that is the base class of all other error
    exceptions. You can use this to catch all errors with one
    single 'except' statement. Warnings are not considered
    errors and thus should not use this class as base. It must
    be a subclass of the Python StandardError (defined in the
    module exceptions)."""
    pass


class InterfaceError(Error):
    """Exception raised for errors that are related to the
    database interface rather than the database itself.  It
    must be a subclass of Error."""
    pass

class DatabaseError(Error):
    """Exception raised for errors that are related to the
    database.  It must be a subclass of Error."""
    pass

class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with
    the processed data like division by zero, numeric value
    out of range, etc. It must be a subclass of DatabaseError."""
    pass

class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the
    database's operation and not necessarily under the control
    of the programmer, e.g. an unexpected disconnect occurs,
    the data source name is not found, a transaction could not
    be processed, a memory allocation error occurred during
    processing, etc.  It must be a subclass of DatabaseError."""
    pass

class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the
    database is affected, e.g. a foreign key check fails.  It
    must be a subclass of DatabaseError."""
    pass

class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the
    transaction is out of sync, etc.  It must be a subclass of
    DatabaseError."""
    pass

class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not
    found or already exists, syntax error in the SQL
    statement, wrong number of parameters specified, etc.  It
    must be a subclass of DatabaseError."""
    pass

class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which is not
    supported by the database, e.g. requesting a .rollback() on a connection
    that does not support transaction or has transactions turned off.  It must
    be a subclass of DatabaseError."""
    pass


