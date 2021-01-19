# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
import os
import sys
import unittest
import pymonetdb
import difflib
from abc import ABCMeta, abstractmethod
import MonetDBtesting.process as process
import inspect

TSTDB=os.getenv("TSTDB")
MAPIPORT=os.getenv("MAPIPORT")

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

def get_index_mismatch(left=[], right=[]):
    ll = len(left)
    rl = len(right)
    index = None
    for i in range(min(ll, rl)):
        if not equals(left[i], right[i]):
            index = i
            break
    return index

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

def filter_junk(s: str):
    """filters empty strings and comments
    """
    s = s.strip()
    if s.startswith('--') or s.startswith('#') or s.startswith('stdout of test'):
        return False
    if s == '':
        return False
    return True

def filter_headers(s: str):
    """filter lines prefixed with % (MAPI headers)"""
    s = s.strip()
    if s.startswith('%'):
        return False
    if s == '':
        return False
    return True

def filter_lines_starting_with(predicates=[]):
    def _fn(line:str):
        line = line.strip()
        if line == '':
            return False
        for p in predicates:
            if line.startswith(p):
                return False
        return True
    return _fn


def filter_matching_blocks(a: [str] = [], b: [str] = []):
    # TODO add some ctx before any mismatch lines
    ptr = 0
    red_a = []
    red_b = []
    min_size = min(len(a), len(b))
    s = difflib.SequenceMatcher()
    for i in range(min_size):
        s.set_seq1(a[i].replace('\t', '').replace(' ', ''))
        s.set_seq2(b[i].replace('\t', '').replace(' ', ''))
        # should be high matching ratio
        if s.quick_ratio() < 0.95:
            red_a.append(a[i])
            red_b.append(b[i])
            # keep track of last mismatch to add some ctx in between
            ptr = i
    # add trailing data if len(a) != len(b)
    red_a+=a[min_size:]
    red_b+=b[min_size:]
    return red_a, red_b

def diff(stable_file, test_file):
    diff = None
    filter_fn = filter_lines_starting_with(['--', '#', 'stdout of test', 'stderr of test', 'MAPI'])
    with open(stable_file) as fstable:
        stable = list(filter(filter_fn, fstable.read().split('\n')))
        with open(test_file) as ftest:
            test = list(filter(filter_fn, ftest.read().split('\n')))
            a, b = filter_matching_blocks(stable, test)
            diff = list(difflib.unified_diff(a, b, fromfile='stable', tofile='test'))
            if len(diff) > 0:
                diff = '\n'.join(diff)
            else:
                diff = None
    return diff

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
        self.language = language

    def connect(self):
        if self.dbh is None:
            if self.language == 'sql':
                self.dbh = pymonetdb.connect(
                                         username=self.username,
                                         password=self.password,
                                         hostname=self.hostname,
                                         port=self.port,
                                         database=self.database,
                                         autocommit=True)
            else:
                self.dbh = malmapi.Connection()
                self.dbh.connect(
                                 username=self.username,
                                 password=self.password,
                                 hostname=self.hostname,
                                 port=self.port,
                                 database=self.database,
                                 language=self.language)
        return self.dbh

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def cursor(self):
        if self.dbh:
            if self.language == 'sql':
                return self.dbh.cursor()
            else:
                return MapiCursor(self.dbh)
        return None

    def close(self):
        if self.dbh:
            self.dbh.close()
            self.dbh = None

class RunnableTestResult(metaclass=ABCMeta):
    """Abstract class for sql result"""
    did_run = False

    @abstractmethod
    def run(self, query:str, *args, stdin=None, lineno=None):
        """Run query with specific client"""
        pass

class TestCaseResult(object):
    """TestCase connected result"""
    test_case = None

    def __init__(self, test_case, **kwargs):
        self.test_case = test_case
        self.assertion_errors = [] # holds assertion errors
        self.query = None
        self.test_run_error = None
        self.err_code = None
        self.err_message = None
        self.data = []
        self.rows = []
        self.rowcount = -1
        self.description = None
        self.id = kwargs.get('id')
        self.lineno = None

    def fail(self, msg, data=None):
        """ logs errors to test case err file"""
        err_file = self.test_case.err_file
        if len(self.assertion_errors) == 0:
            lineno = self.lineno or 'N/A'
            print('', file=err_file)
            if self.query:
                print(f'ln{lineno}:', self.query, file=err_file)
            elif self.id:
                print(f'ln{lineno}:', self.id, file=err_file)
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

    def assertFailed(self, err_code=None, err_message=None):
        """assert on query failed"""
        if self.test_run_error is None:
            msg = "expected to fail but didn't"
            self.fail(msg)
        else:
            msgs = []
            if err_code:
                if self.err_code != err_code:
                    msgs.append( "expected to fail with error code {} but failed with error code {}".format(err_code, self.err_code))
            if err_message:
                if self.err_message:
                    if err_message.lower() != self.err_message.lower():
                        msgs.append("expected to fail with error message '{}' but failed with error message '{}'".format(err_message, self.err_message))
                else:
                    msgs.append("expected to fail with error message '{}' but got '{}'".format(err_message, self.err_message))
            if len(msgs) > 0:
                self.fail('\n'.join(msgs))
        return self

    def assertSucceeded(self):
        """assert on query succeeded"""
        if self.test_run_error is not None:
            msg = "expected to succeed but didn't\n{}".format(str(self.test_run_error).rstrip('\n'))
            self.fail(msg)
        return self

    def assertRowCount(self, rowcount):
        '''Assert on the affected row count'''
        if self.rowcount != int(rowcount):
            msg = "received {} rows, expected {} rows".format(self.rowcount, rowcount)
            self.fail(msg)
        return self

    def assertResultHashTo(self, hash_value):
        raise NotImplementedError

    def assertValue(self, row, col, val):
        """Assert on a value matched against row, col in the result"""
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
        """Assert on a match of a subset of the result. When index is provided it
        starts comparing from that row index onward.
        """
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
        index = index or 0
        # align sequences
        idx_mis = get_index_mismatch(data, self.data[index:])
        if idx_mis is not None:
            exp_v = data[idx_mis]
            exp_t = type(exp_v)
            res_v = self.data[idx_mis]
            res_t = type(res_v)
            msg = 'expected to match query result at index {} but it didn\'t as {} not equals {}'.format(idx_mis, repr(exp_v), repr(res_v))
            self.fail(msg, data=self.data)
        return self

class MclientTestResult(TestCaseResult, RunnableTestResult):
    """Holder of a sql execution result as returned from mclient"""

    def __init__(self, test_case, **kwargs):
        super().__init__(test_case, **kwargs)
        self.did_run = False

    def _parse_error(self, err:str):
        err_code = None
        err_message = None
        for l in err.splitlines():
            l = l.strip()
            if l.startswith('ERROR'):
                err_message = l.split('=').pop().strip()
            if l.startswith('CODE'):
                err_code = l.split('=').pop().strip()
        return err_code, err_message

    def _get_row_count(self, data):
        count = 0
        data = list(filter(filter_junk, data.splitlines()))
        for l in data:
            l = l.strip()
            if l.startswith('[') and l.endswith(']'):
                count+=1
        return count


    def run(self, query:str, *args, stdin=None, lineno=None):
        # ensure runs only once
        if self.did_run is False:
            self.lineno = lineno
            conn_ctx = self.test_case.conn_ctx
            kwargs = dict(
                host = conn_ctx.hostname,
                port = conn_ctx.port,
                dbname = conn_ctx.database,
                user = conn_ctx.username,
                passwd = conn_ctx.password)
            try:
                if query:
                    self.query = query
                    # TODO multiline stmts won't work here ensure single stmt
                    with process.client('sql', **kwargs, \
                            args=list(args), \
                            stdin=process.PIPE, \
                            stdout=process.PIPE, stderr=process.PIPE) as p:
                        out, err = p.communicate(query)
                        if out:
                            self.data = out
                            self.rowcount = self._get_row_count(out)
                        if err:
                            self.test_run_error = err
                            self.err_code, self.err_message = self._parse_error(err)
                elif stdin:
                    with process.client('sql', **kwargs, \
                            args=list(args), \
                            stdin=stdin, \
                            stdout=process.PIPE, stderr=process.PIPE) as p:
                        out, err = p.communicate()
                        if out:
                            self.data = out
                        if err:
                            self.test_run_error = err
                self.did_run = True
            except Exception as e:
                raise SystemExit(e)
        return self

    def assertMatchStableOut(self, fout, ignore_headers=False):
        stable = []
        data = list(filter(filter_junk, self.data.split('\n')))
        with open(fout, 'r') as f:
            stable = list(filter(filter_junk, f.read().split('\n')))
        if ignore_headers:
            stable = list(filter(filter_headers, stable))
            data = list(filter(filter_headers, data))
        a, b = filter_matching_blocks(stable, data)
        if a or b:
            diff = list(difflib.unified_diff(stable, data, fromfile='stable', tofile='test'))
            if len(diff) > 0:
                err_file = self.test_case.err_file
                msg = "expected to match stable output {} but it didnt\'t\n".format(fout)
                msg+='\n'.join(diff)
                self.assertion_errors.append(AssertionError(msg))
                self.fail(msg)
        return self

    def assertMatchStableError(self, ferr, ignore_err_messages=False):
        stable = []
        err = []
        filter_fn = filter_lines_starting_with(['--', '#', 'stderr of test', 'MAPI'])
        if self.test_run_error:
            err = list(filter(filter_fn, self.test_run_error.split('\n')))
        with open(ferr, 'r') as f:
            stable = list(filter(filter_fn, f.read().split('\n')))
        a, b = filter_matching_blocks(stable, err)
        diff = list(difflib.unified_diff(a, b, fromfile='stable', tofile='test'))
        if len(diff) > 0:
            err_file = self.test_case.err_file
            msg = "expected to match stable error {} but it didnt\'t\n".format(ferr)
            msg+='\n'.join(diff)
            self.assertion_errors.append(AssertionError(msg))
            self.fail(msg)
        return self

    def assertDataResultMatch(self, expected):
        data = list(filter(filter_junk, self.data.split('\n')))
        data = list(filter(filter_headers, data))
        a, b = filter_matching_blocks(expected, data)
        diff = list(difflib.unified_diff(a, b, fromfile='expected', tofile='test'))
        if len(diff) > 0:
            err_file = self.test_case.err_file
            exp = '\n'.join(expected)
            got = '\n'.join(data)
            msg = "Output didn't match.\n"
            msg += f"Expected:\n{exp}\n"
            msg += f"Got:\n{got}\n"
            msg += "Diff:\n" + '\n'.join(s.rstrip() for s in diff)
            self.assertion_errors.append(AssertionError(msg))
            self.fail(msg)
        return self

    def assertValue(self, row, col, val):
        raise NotImplementedError

class PyMonetDBTestResult(TestCaseResult, RunnableTestResult):
    """Holder of sql execution information. Managed by SQLTestCase."""
    test_case = None

    def __init__(self, test_case, **kwargs):
        super().__init__(test_case, **kwargs)
        self.did_run = False

    def _parse_error(self, error:str=''):
        """Parse error string and returns (err_code, err_msg) tuple
        """
        err_code = None
        err_msg = None
        tmp = error.split('!')
        if len(tmp) > 1:
            try:
                err_code = tmp[0].strip()
            except (ValueError, TypeError):
                pass
            # reconstruct
            err_msg = ('!'.join(tmp[1:])).strip()
        elif len(tmp) == 1:
            if tmp[0]:
                err_msg = tmp[0].strip()
        return err_code, err_msg

    def run(self, query:str, *args, stdin=None, lineno=None):
        # ensure runs only once
        if self.did_run is False:
            self.lineno = lineno
            if query:
                self.query = query
                try:
                    conn = self.test_case.conn_ctx.connect()
                    crs = conn.cursor()
                    crs.execute(query)
                    self.rowcount = crs.rowcount
                    self.rows = crs._rows
                    if crs.description:
                        self.data = crs.fetchall()
                        self.description = crs.description
                except pymonetdb.Error as e:
                    self.test_run_error = e
                    self.err_code, self.err_message = self._parse_error(e.args[0])
                except (OSError, ValueError) as e:
                    self.test_run_error = e
            self.did_run = True
        return self


class MonetDBeTestResult(TestCaseResult, RunnableTestResult):
    def __init__(self, test_case, **kwargs):
        super().__init__(test_case, **kwargs)
        self.did_run = False

    def _parse_error(self, err: str):
        pass

    def run(self, query:str, *args, stdin=None, lineno=None):
        if self.did_run is False:
            self.lineno = lineno
            if query:
                self.query = query
                try:
                    conn = self.test_case.conn_ctx
                    crs = conn.cursor()
                    crs.execute(query)
                    self.rowcount = int(crs.rowcount)
                    if crs.description:
                        self.description = crs.description
                        self.data = crs.fetchall()
                except (monetdbe.Error, ValueError) as e:
                    self.test_run_error = e
                    # TODO parse error
            self.did_run = True
        return self

class SQLDump():
    def __init__(self, test_case, data=None):
        self.test_case = test_case
        self.data = data
        self.assertion_errors = [] # holds assertion errors

    def assertMatchStableOut(self, fout):
        stable = []
        data = self.data.split('\n') if self.data else []
        dump = list(filter(filter_junk, data))
        with open(fout, 'r') as f:
            stable = list(filter(filter_junk, f.read().split('\n')))
        a, b = filter_matching_blocks(stable, dump)
        diff = list(difflib.unified_diff(a, b, fromfile='stable', tofile='test'))
        if len(diff) > 0:
            err_file = self.test_case.err_file
            msg = "sql dump expected to match stable output {} but it didnt\'t\n".format(fout)
            msg+='\n'.join(diff)
            self.assertion_errors.append(AssertionError(msg))
            print(msg, file=err_file)

class SQLTestCase():
    def __init__(self, out_file=sys.stdout, err_file=sys.stderr):
        self.out_file = out_file
        self.err_file = err_file
        self.test_results = []
        self._conn_ctx = None
        self.in_memory = False
        self.client = 'pymonetdb'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit()

    def close(self):
        if self._conn_ctx:
            self._conn_ctx.close()
        self._conn_ctx = None

    def exit(self):
        self.close()
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
            username='monetdb', password='monetdb', port=MAPIPORT,
            hostname='localhost', database=TSTDB, language='sql'):
        if self._conn_ctx:
            self.close()
        if database == ':memory:':
            import monetdbe
            self.in_memory = True
            # TODO add username, password, port when supported from monetdbe
            self._conn_ctx = monetdbe.connect(':memory:', autocommit=True)
        else:
            self.in_memory = False
            self._conn_ctx = PyMonetDBConnectionContext(
                                 username=username,
                                 password=password,
                                 hostname=hostname,
                                 port=port,
                                 database=database,
                                 language=language)
        return self._conn_ctx

    def default_conn_ctx(self):
        if self.in_memory:
            return  monetdbe.connect(':memory:', autocommit=True)
        ctx = PyMonetDBConnectionContext()
        return ctx

    @property
    def conn_ctx(self):
        return self._conn_ctx or self.default_conn_ctx()

    def execute(self, query:str, *args, client='pymonetdb', stdin=None, result_id=None):
        '''Execute query with specified client. Default client is pymonetbd.'''
        frame = inspect.currentframe().f_back
        lineno = frame.f_lineno
        if client == 'mclient':
            res = MclientTestResult(self, id=result_id)
        elif self.in_memory:
            res = MonetDBeTestResult(self, id=result_id)
        else:
            res = PyMonetDBTestResult(self, id=result_id)
        res.run(query, *args, stdin=stdin, lineno=lineno)
        self.test_results.append(res)
        return res

    def sqldump(self, *args):
        kwargs = dict(
            host = self.conn_ctx.hostname,
            port = self.conn_ctx.port,
            dbname = self.conn_ctx.database,
            user = self.conn_ctx.username,
            passwd = self.conn_ctx.password)
        dump = None
        if '--inserts' in args:
            args = list(args)
            cmd = 'sqldump'
        else:
            cmd = 'sql'
            # TODO should more options be allowed here
            args = ['-lsql', '-D']
        try:
            with process.client(cmd, **kwargs, args=args, stdout=process.PIPE, stderr=process.PIPE) as p:
                dump, err = p.communicate()
        except Exception as e:
            pass
        res = SQLDump(self, data=dump)
        self.test_results.append(res)
        return res

    def drop(self):
        if self.in_memory:
            # TODO
            return
        try:
            with self.conn_ctx as ctx:
                crs = ctx.cursor()
                crs.execute('select s.name, t.name, tt.table_type_name from sys.tables t, sys.schemas s, sys.table_types tt where not t.system and t.schema_id = s.id and t.type = tt.table_type_id')
                for row in crs.fetchall():
                    crs.execute('drop {} "{}"."{}" cascade'.format(row[2], row[0], row[1]))

                crs.execute('select s.name, f.name, ft.function_type_keyword from functions f, schemas s, function_types ft where not f.system and f.schema_id = s.id and f.type = ft.function_type_id')
                for row in crs.fetchall():
                    crs.execute('drop all {} "{}"."{}"'.format(row[2], row[0], row[1]))

                crs.execute('select s.name, q.name from sys.sequences q, schemas s where q.schema_id = s.id')
                for row in crs.fetchall():
                    crs.execute('drop sequence "{}"."{}"'.format(row[0], row[1]))

                crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
                for row in crs.fetchall():
                    crs.execute('alter user "{}" SET SCHEMA "sys"'.format(row[0]))

                crs.execute('select name from sys.schemas where not system')
                for row in crs.fetchall():
                    crs.execute('drop schema "{}" cascade'.format(row[0]))

                crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
                for row in crs.fetchall():
                    crs.execute('drop user "{}"'.format(row[0]))

        except (pymonetdb.Error, ValueError) as e:
            pass
