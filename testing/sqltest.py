# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
import os
import sys
import unittest
import pymonetdb
import MonetDBtesting.utils as utils

TSTDB=os.getenv("TSTDB")
MAPIPORT=int(os.getenv("MAPIPORT"))

def equals(a, b) -> bool:
    if type(a) is type(b):
        return a==b
    return False

def sequence_match(left=[], right=[], index=0):
    right = right[index:]
    ll = len(left)
    rl = len(right)
    if ll > rl:
        return False
    if ll == 0 and rl > 0:
        return False
    for i in range(ll):
        if not equals(left[i], right[i]):
            return False
    return True

def piped_representation(data=[]):
    def mapfn(next):
        if type(next) is tuple:
            res=[]
            for v in next:
                res.append(str(v))
            return '|'.join(res)
        else:
            raise TypeError('ERROR: expecting list of tuples!')
    res = list(map(mapfn, data))
    return '\n'.join(res)


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
    test_case = None

    def __init__(self, test_case):
        self.test_case = test_case
        self.query = None
        self.assertion_errors = [] # holds assertion errors
        self.query_error = None
        self.data = []
        self.rows = []
        self.rowcount = -1
        self.description = None

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
        err_file = self.test_case.err_file
        if len(self.assertion_errors) == 0:
            print(self.query, file=err_file)
            print('----', file=err_file)
        self.assertion_errors.append(AssertionError(msg))
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
            print('', file=err_file)

    def assertFailed(self, err_code=None):
        if self.query_error is None:
            msg = "expected to fail but didn't\n{}".format(str(self.query_error))
            self.fail(msg)
        else:
            if err_code:
                err_code_received, err_msg_received = utils.parse_mapi_err_msg(self.query_error.args[0])
                if err_code_received != err_code:
                    msg = "expected to fail with error code {} but failed with error code {}".format(err_code, err_code_received)
                    self.fail(msg)
        return self

    def assertSucceeded(self):
        if self.query_error is not None:
            msg = "expected to succeed but didn't\n{}".format(str(self.query_error))
            self.fail(msg)
        return self

    def assertRowCount(self, rowcount):
        if self.rowcount != int(rowcount):
            msg = "received {} rows, expected {} rows".format(self.rowcount, rowcount)
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
                msg = 'expected "{}", received "{}" in row={}, col={}'.format(val, received, row, col)
                self.fail(msg, data=self.data)
        else:
            # handle type mismatch
            msg = 'expected type {} and value "{}", received type {} and value "{}" in row={}, col={}'.format(type(val), str(val), type(received), str(received), row, col)
            self.fail(msg, data=self.data)
        return self

    def assertDataResultMatch(self, data=[], index=None):
        def mapfn(next):
            if type(next) is list:
                return tuple(next)
            return next
        data = list(map(mapfn, data))
        if index is None:
            if len(data) > 0:
                first = data[0]
                for i, v in enumerate(self.data):
                    if first == v:
                        index = i
                        break
        if not sequence_match(data, self.data, index):
            msg = '{}\nexpected to match query result starting at index={}, but it didn\'t'.format(piped_representation(data), index)
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
