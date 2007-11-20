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
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

"""MonetSQLdb Cursors

This module implements Cursors of various types for MonetSQLdb. By
default, MonetSQLdb uses the Cursor class.

"""

from monetexceptions import *
import converters

class BaseCursor:

    def __init__(self, connection):
        self.connection = connection
        self.errorhandler = connection.errorhandler
        self._result = None
        self._executed = None
        self.messages = []
        self.rowcount = -1
        self.arraysize = 1

    def close(self):
        self._result = None
        self._executed = None
        self.rowcount = -1
        pass

    def __del__(self):
        self.close()

    def nextset(self):
        if self._result.next_result():
            return True
        if self._result.result_error() != None:
            self.errorhandler(self, DatabaseError, "Query result error: " + self._result.result_error())
        return None

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    def _check_executed(self):
        if not self._executed:
            self.errorhandler(self, ProgrammingError, "execute() first")

    def execute(self, query, args=None):

        """Execute a query.

        query -- string, query to execute on server
        args -- optional sequence or mapping, parameters to use with query.

        Note: If args is a sequence, then %s must be used as the
        parameter placeholder in the query. If a mapping is used,
        %(key)s must be used as the placeholder.

        Returns long integer rows affected, if any

        """
        from types import ListType, TupleType
        from sys import exc_info
        try:
            if args is None:
                r = self._query(query)
            else:
                query = query % self.connection.literal(args)
                r = self._query(query)
        except TypeError, m:
            if m.args[0] in ("not enough arguments for format string",
                             "not all arguments converted"):
                self.errorhandler(self, ProgrammingError, m.args[0])
            else:
                self.errorhandler(self, TypeError, m)
        except:
            exc, value, tb = exc_info()
            del tb
            self.errorhandler(self, exc, value)

        self._executed = query
        return r

    def executemany(self, query, args):

        """Execute a multi-row query.

        query -- string, query to execute on server

        args

            Sequence of sequences or mappings, parameters to use with
            query.

        Returns long integer rows affected, if any.

        This method improves performance on multiple-row INSERT and
        REPLACE. Otherwise it is equivalent to looping over args with
        execute().

        """
        if not args: return
        r = 0
        for a in args:
            r = r + self.execute(query, a)
        return r

    def __do_query(self, q):
        self._result = self.connection._mapi.query(q)
        if self._result.result_error() != None:
            self.errorhandler(self, DatabaseError, "Query result error: " + self._result.result_error())

        self.rownumber = 0
        self.rowcount = self._result.get_row_count()
        self.description = self._describe()
        return self.rowcount

    def _describe(self):
        description = []
        for i in range(self._result.get_field_count()):
            d = (self._result.get_name(i), self._result.get_type(i), None, None, None, None, None)
            description.append(d)
        return description

    # _query = __do_query


    def insert_id(self):
        """Return the last inserted ID on an AUTO_INCREMENT columns.
        DEPRECATED: use lastrowid attribute"""
        self.errorhandler(self, NotSupportedError, "Monet does not support AUTO_INCREMENT columns")

    def _fetch_row(self, size=1):
        if not self._result:
            return ()
        return self._result.fetch_row()

    def __iter__(self):
        return iter(self.fetchone, None)


class CursorStoreResultMixIn:
    """This is a MixIn class which causes the entire result set to be
    stored on the client side.

    In Monet, output comes in one large batch anyway, so there is no
    need to do this iteratively."""

    def _query(self, q):
        rowcount = self._BaseCursor__do_query(q)
        self._cacheResult()
        return rowcount

    def _cacheResult(self):
        self._rows = []
        while self._result.fetch_row() > 0:
            if self._fetch_type == 0: row = ()
            else: row = {}
            for i in range(self._result.get_field_count()):
                val = converters.monet2python(self._result.fetch_field(i), self.description[i][1])

                if self._fetch_type == 0: row = row + (val,)
                else: row[self.description[i][0]] = val
            self._rows.append(row)

    def nextset(self):
        if (self._result.next_result()):
            self._cacheResult()
            return True
        return None

    def fetchone(self):
        """Fetches a single row from the cursor. None indicates that
        no more rows are available."""
        self._check_executed()
        if self.rownumber >= len(self._rows): return None
        result = self._rows[self.rownumber]
        self.rownumber = self.rownumber+1
        return result

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        self._check_executed()
        result = self.rownumber and self._rows[self.rownumber:] or self._rows
        self.rownumber = len(self._rows)
        return result

    def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position according
        to mode.

        If mode is 'relative' (default), value is taken as offset to
        the current position in the result set, if set to 'absolute',
        value states an absolute target position."""
        self._check_executed()
        if mode == 'relative':
            r = self.rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            self.errorhandler(self, ProgrammingError, "unknown scroll mode %s" % `mode`)
        if r < 0 or r >= len(self._rows):
            self.errorhandler(self, IndexError, "out of range")
        self.rownumber = r

    def __iter__(self):
        self._check_executed()
        result = self.rownumber and self._rows[self.rownumber:] or self._rows
        return iter(result)

class CursorTupleRowsMixIn:

    """This is a MixIn class that causes all rows to be returned as tuples,
    which is the standard form required by DB API."""

    _fetch_type = 0


class CursorDictRowsMixIn:

    """This is a MixIn class that causes all rows to be returned as
    dictionaries. This is a non-standard feature."""

    _fetch_type = 1

    def fetchoneDict(self):
        """Fetch a single row as a dictionary. Deprecated:
        Use fetchone() instead."""
        return self.fetchone()

    def fetchmanyDict(self, size=None):
        """Fetch several rows as a list of dictionaries. Deprecated:
        Use fetchmany() instead."""
        return self.fetchmany(size)

    def fetchallDict(self):
        """Fetch all available rows as a list of dictionaries. Deprecated:
        Use fetchall() instead."""
        return self.fetchall()


class Cursor (CursorStoreResultMixIn, CursorTupleRowsMixIn, BaseCursor):
    pass

class DictCursor (CursorStoreResultMixIn, CursorDictRowsMixIn, BaseCursor):
    pass
