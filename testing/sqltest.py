# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
import os
import sys
import unittest
import pymonetdb

TSTDB=os.getenv("TSTDB")
MAPIPORT=int(os.getenv("MAPIPORT"))

class PyMonetDBConnectionContext(object):
    def __init__(self,
            username='monetdb', password='monetdb',
            hostname='localhost', port=MAPIPORT, database=TSTDB, language='sql'):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port
        self.database = database
        self.language = language
        self.dbh = None
        self.crs = None
        self.language = language

    def __enter__(self):
        if self.language == 'sql':
            self.dbh = pymonetdb.connect(
                                     username=self.username,
                                     password=self.password,
                                     hostname=self.hostname,
                                     port=self.port,
                                     database=self.database,
                                     autocommit=True)
            self.crs = self.dbh.cursor()
        else:
            self.dbh = malmapi.Connection()
            self.dbh.connect(
                             username=self.username,
                             password=self.password,
                             hostname=self.hostname,
                             port=self.port,
                             database=self.database,
                             language=self.language)
            self.crs = MapiCursor(self.dbh)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.crs:
            self.crs.close()
            self.crs = None
        if self.dbh:
            self.dbh.close()
            self.dbh = None

class SQLTestResult(object):
    """Holder of sql execution information. Managed by SQLTestCase."""
    query = None
    assertion_errors = [] # holds assertion errors
    query_error = None
    data = []
    rows = []
    rowcount = -1
    description = None
    test_case = None

    def __init__(self, test_case):
        self.test_case = test_case

    def run(self, query:str):
        # ensure runs only once
        if self.query is None:
            self.query = query
            try:
                with self.test_case.conn_ctx as ctx:
                    ctx.crs.execute(query)
                    self.rowcount = ctx.crs.rowcount
                    self.rows = ctx.crs._rows
                    if ctx.crs.description:
                        self.data = ctx.crs.fetchall()
                        self.description = ctx.crs.description
            except (pymonetdb.Error, ValueError) as e:
                self.query_error = e
        return self

    def fail(self, msg, data=None):
        self.assertion_errors.append(AssertionError(msg))
        err_file = self.test_case.err_file
        print(msg, file=err_file)
        if data is not None:
            if len(data) < 100:
                print('query result:', file=err_file)
            else:
                print('truncated query result:', file=err_file)
            for row in data[:100]:
                sep=''
                for col in row:
                    if col is None:
                        print(sep, 'NULL', sep='', end='', file=err_file)
                    else:
                        print(sep, col, sep='', end='', file=err_file)
                    sep = '|'
                print('', file=err_file)


    def transform_data(self):
        pass

    def assertFail(self):
        if self.query_error is None:
            msg = "{}\nwas expected to fail but didn't\n{}!".format(self.query, str(self.query_error))
            self.fail(msg)
        return self

    def assertSucceed(self):
        if self.query_error is not None:
            msg = "{}\nwas expected to succeed but didn't\n{}!".format(self.query, str(self.query_error))
            self.fail(msg)
        return self

    def assertRowCount(self, rowcount):
        if self.rowcount != int(rowcount):
            msg = "{}\nreceived {} rows, expected {} rows".format(self.query, self.rowcount, rowcount)
            self.fail(msg)
        return self

    def assertResultHashTo(self, hash_value):
        raise NotImplementedError()

    def assertValue(self, row, col, val):
        received = None
        row = int(row)
        col = int(col)
        try:
            received = self.data[row][col]
        except IndexError:
            pass
        if type(val) is type(received):
            if val != received:
                msg = '{}\nexpected "{}", received "{}" in row={}, col={}!'.format(self.query, val, received, row, col)
                self.fail(msg, data=self.data)
        else:
            # handle type mismatch
            msg = '{}\nexpected type {} and value "{}", received type {} and value "{}" in row={}, col={}!'.format(self.query, type(val), str(val), type(received), str(received), row, col)
            self.fail(msg, data=self.data)
        return self

class SQLTestCase():
    def __init__(self, out_file=sys.stdout, err_file=sys.stderr):
        self.out_file = out_file
        self.err_file = err_file
        self.test_results = []
        self._conn_ctx = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit()

    def exit(self):
        self._conn_ctx = None
        for res in self.test_results:
            if len(res.assertion_errors) > 0:
                raise SystemExit(1)

    def out(self, data):
        print(data, file=self.out_file)
        print('', file=self.out_file)

    def err(self, msg):
        print(msg, file=self.err_file)
        print('', file=self.err_file)


    def connect(self,
            username='monetdb', password='monetdb',
            hostname='localhost', port=MAPIPORT, database=TSTDB, language='sql'):
            self._conn_ctx = PyMonetDBConnectionContext(
                                 username=username,
                                 password=password,
                                 hostname=hostname,
                                 port=port,
                                 database=database,
                                 language=language)
            return self._conn_ctx

    def default_conn_ctx(self):
        return PyMonetDBConnectionContext()

    @property
    def conn_ctx(self):
        return self._conn_ctx or self.default_conn_ctx()

    def execute(self, query:str):
        res = SQLTestResult(self)
        res.run(query)
        self.test_results.append(res)
        return res

    def drop(self):
        raise NotImplementedError()
