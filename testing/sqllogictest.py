#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

# skipif <system>
# onlyif <system>

# statement (ok|ok rowcount|error) [arg]
# query (I|T|R)+ (nosort|rowsort|valuesort|python)? [arg]
#       I: integer; T: text (string); R: real (decimal)
#       nosort: do not sort
#       rowsort: sort rows
#       valuesort: sort individual values
#       python some.python.function: run data through function (MonetDB extension)
# hash-threshold number
# halt

# The python function that can be used instead of the various sort
# options should be a simple function that gets a list of lists as
# input and should produce a list of lists as output.  If the name
# contains a period, the last part is the name of the function and
# everything up to the last period is the module.  If the module
# starts with a period, it is searched in the MonetDBtesting module
# (where this file is also).

import pymonetdb
from MonetDBtesting.mapicursor import MapiCursor
import MonetDBtesting.malmapi as malmapi
import hashlib
import re
import sys
import importlib
import MonetDBtesting.utils as utils

skipidx = re.compile(r'create index .* \b(asc|desc)\b', re.I)

class SQLLogicSyntaxError(Exception):
    pass

class SQLLogicConnection(object):
    def __init__(self, conn_id, dbh, crs=None, language='sql'):
        self.conn_id = conn_id
        self.dbh = dbh
        self.crs = crs
        self.language = language

    def cursor(self):
        if self.crs:
            return self.crs
        if self.language == 'sql':
            return self.dbh.cursor()
        return MapiCursor(self.dbh)


def is_copyfrom_stmt(stmt:[str]=[]):
    try:
        index = stmt.index('<COPY_INTO_DATA>')
        return True
    except ValueError:
        pass
    return False

def prepare_copyfrom_stmt(stmt:[str]=[]):
    try:
        index = stmt.index('<COPY_INTO_DATA>')
        head = stmt[:index]
        # check for escape character (single period)
        tail = []
        for l in stmt[index+1:]:
            if l.strip() == '.':
                tail.append('')
            else:
                tail.append(l)
        head = '\n'.join(head) + ';'
        tail='\n'.join(tail)
        return head + '\n' + tail, head
    except ValueError:
        return stmt

def parse_connection_string(s: str) -> dict:
    '''parse strings like @connection(id=con1, ...)
    '''
    res = dict()
    if not (s.startswith('@connection(') and s.endswith(')')):
        raise SQLLogicSyntaxError('invalid connection string!')
    params = s[12:-1].split(',')
    for p in params:
        p = p.strip()
        try:
            k, v = p.split('=')
            if k == 'id':
                k = 'conn_id'
            assert k in ['conn_id', 'username', 'password']
            assert res.get(k) is None
            res[k] = v
        except (ValueError, AssertionError) as e:
            raise SQLLogicSyntaxError('invalid connection paramaters definition!')
    if len(res.keys()) > 1:
        try:
            assert res.get('username')
            assert res.get('password')
        except AssertionError as e:
            raise SQLLogicSyntaxError('invalid connection paramaters definition, username or password missing!')
    return res

class SQLLogic:
    def __init__(self, report=None, out=sys.stdout):
        self.dbh = None
        self.crs = None
        self.out = out
        self.res = None
        self.rpt = report
        self.language = 'sql'
        self.conn_map = dict()
        self.database = None
        self.hostname = None
        self.port = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self, username='monetdb', password='monetdb',
                hostname='localhost', port=None, database='demo', language='sql'):
        self.language = language
        self.hostname = hostname
        self.port = port
        self.database = database
        if language == 'sql':
            self.dbh = pymonetdb.connect(username=username,
                                     password=password,
                                     hostname=hostname,
                                     port=port,
                                     database=database,
                                     autocommit=True)
            self.crs = self.dbh.cursor()
        else:
            dbh = malmapi.Connection()
            dbh.connect(
                                     database=database,
                                     username=username,
                                     password=password,
                                     language=language,
                                     hostname=hostname,
                                     port=port)
            self.crs = MapiCursor(dbh)

    def add_connection(self, conn_id, username='monetdb', password='monetdb'):
        if self.conn_map.get(conn_id, None) is None:
            hostname = self.hostname
            port = self.port
            database = self.database
            language = self.language
            if language == 'sql':
                dbh  = pymonetdb.connect(username=username,
                                     password=password,
                                     hostname=hostname,
                                     port=port,
                                     database=database,
                                     autocommit=True)
                crs = dbh.cursor()
            else:
                dbh = malmapi.Connection()
                dbh.connect(database=database,
                         username=username,
                         password=password,
                         language=language,
                         hostname=hostname,
                         port=port)
                crs = MapiCursor(dbh)
            conn = SQLLogicConnection(conn_id, dbh=dbh, crs=crs, language=language)
            self.conn_map[conn_id] = conn
            return conn

    def get_connection(self, conn_id):
        return self.conn_map.get(conn_id)

    def close(self):
        for k in self.conn_map:
            conn = self.conn_map[k]
            conn.dbh.close()
        self.conn_map.clear()
        if self.crs:
            self.crs.close()
            self.crs = None
        if self.dbh:
            self.dbh.close()
            self.dbh = None


    def drop(self):
        if self.language != 'sql':
            return
        self.crs.execute('select s.name, t.name, tt.table_type_name from sys.tables t, sys.schemas s, sys.table_types tt where not t.system and t.schema_id = s.id and t.type = tt.table_type_id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop {} "{}"."{}" cascade'.format(row[2], row[0], row[1]))
            except pymonetdb.Error:
                # perhaps already dropped because of the cascade
                pass
        self.crs.execute('select s.name, f.name, ft.function_type_keyword from functions f, schemas s, function_types ft where not f.system and f.schema_id = s.id and f.type = ft.function_type_id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop all {} "{}"."{}"'.format(row[2], row[0], row[1]))
            except pymonetdb.Error:
                # perhaps already dropped
                pass
        self.crs.execute('select s.name, q.name from sys.sequences q, schemas s where q.schema_id = s.id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop sequence "{}"."{}"'.format(row[0], row[1]))
            except pymonetdb.Error:
                # perhaps already dropped
                pass
        self.crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
        for row in self.crs.fetchall():
            try:
                self.crs.execute('alter user "{}" SET SCHEMA "sys"'.format(row[0]))
            except pymonetdb.Error:
                pass
        self.crs.execute('select name from sys.schemas where not system')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop schema "{}" cascade'.format(row[0]))
            except pymonetdb.Error:
                pass
        self.crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop user "{}"'.format(row[0]))
            except pymonetdb.Error:
                pass

    def exec_statement(self, statement, expectok, err_stmt=None, expected_err_code=None, expected_err_msg=None, expected_rowcount=None, conn=None):
        crs = conn.cursor() if conn else self.crs
        if skipidx.search(statement) is not None:
            # skip creation of ascending or descending index
            return
        try:
            affected_rowcount = crs.execute(statement)
        except (pymonetdb.Error, ValueError) as e:
            msg = e.args[0]
            if not expectok:
                if expected_err_code or expected_err_msg:
                    # check whether failed as expected
                    err_code_received, err_msg_received = utils.parse_mapi_err_msg(msg)
                    if expected_err_code and expected_err_msg:
                        if expected_err_code == err_code_received and expected_err_msg.lower() == err_msg_received.lower():
                            return
                    else:
                        if expected_err_code:
                            if expected_err_code == err_code_received:
                                return
                        if expected_err_msg:
                            if expected_err_msg.lower() == err_msg_received.lower():
                                return
                    msg = "statement was expected to fail with" \
                            + (" error code {}".format(expected_err_code) if expected_err_code else '')\
                            + (", error message {}".format(str(expected_err_msg)) if expected_err_msg else '')
                    self.query_error(err_stmt or statement, str(msg), str(e))
                return
        except ConnectionError as e:
            self.query_error(err_stmt or statement, 'Server may have crashed', str(e))
            return
        else:
            if expectok:
                if expected_rowcount:
                    if expected_rowcount != affected_rowcount:
                        self.query_error(err_stmt or statement, "statement was expecting to succeed with {} rows but received {} rows!".format(expected_rowcount, affected_rowcount))
                return
            msg = None
        self.query_error(err_stmt or statement, expectok and "statement was expected to succeed but didn't" or "statement was expected to fail but didn't", msg)

    def convertresult(self, query, columns, data):
        ndata = []
        for row in data:
            if len(row) != len(columns):
                self.query_error(query, 'wrong number of columns received')
                return None
            nrow = []
            for i in range(len(columns)):
                if row[i] is None or row[i] == 'NULL':
                    nrow.append('NULL')
                elif columns[i] == 'I':
                    if row[i] == 'true':
                        nrow.append('1')
                    elif row[i] == 'false':
                        nrow.append('0')
                    else:
                        nrow.append('%d' % row[i])
                elif columns[i] == 'T':
                    if row[i] == '' or row[i] == b'':
                        nrow.append('(empty)')
                    else:
                        nval = []
                        if isinstance(row[i], bytes):
                            for c in row[i]:
                                c = '%02X' % c
                                nval.append(c)
                        else:
                            for c in str(row[i]):
                                if ' ' <= c <= '~':
                                    nval.append(c)
                                else:
                                    nval.append('@')
                        nrow.append(''.join(nval))
                elif columns[i] == 'R':
                    nrow.append('%.3f' % row[i])
                else:
                    raise SQLLogicSyntaxError('incorrect column type indicator')
            ndata.append(tuple(nrow))
        return ndata

    def query_error(self, query, message, exception=None, data=None):
        if self.rpt:
            print(self.rpt, file=self.out)
        print(message, file=self.out)
        if exception:
            print(exception.rstrip('\n'), file=self.out)
        print("query started on line %d of file %s" % (self.qline, self.name),
              file=self.out)
        print("query text:", file=self.out)
        print(query, file=self.out)
        print('', file=self.out)
        if data is not None:
            if len(data) < 100:
                print('query result:', file=self.out)
            else:
                print('truncated query result:', file=self.out)
            for row in data[:100]:
                sep=''
                for col in row:
                    if col is None:
                        print(sep, 'NULL', sep='', end='', file=self.out)
                    else:
                        print(sep, col, sep='', end='', file=self.out)
                    sep = '|'
                print('', file=self.out)

    def exec_query(self, query, columns, sorting, pyscript, hashlabel, nresult, hash, expected, conn=None) -> bool:
        err = False
        crs = conn.cursor() if conn else self.crs
        try:
            crs.execute(query)
        except (pymonetdb.Error, ValueError) as e:
            self.query_error(query, 'query failed', e.args[0])
            return False
        data = crs.fetchall()
        if crs.description:
            if len(crs.description) != len(columns):
                self.query_error(query, 'received {} columns, expected {} columns'.format(len(crs.description), len(columns)), data=data)
                return False
        if sorting != 'python' and crs.rowcount * len(columns) != nresult:
            self.query_error(query, 'received {} rows, expected {} rows'.format(crs.rowcount, nresult // len(columns)), data=data)
            return False
        if self.res is not None:
            for row in data:
                sep=''
                for col in row:
                    if col is None:
                        print(sep, 'NULL', sep='', end='', file=self.res)
                    else:
                        print(sep, col, sep='', end='', file=self.res)
                    sep = '|'
                print('', file=self.res)
        data = self.convertresult(query, columns, data)
        if data is None:
            return
        m = hashlib.md5()
        i = 0
        if sorting == 'valuesort':
            ndata = []
            for row in data:
                for col in row:
                    ndata.append(col)
            ndata.sort()
            for col in ndata:
                if expected is not None:
                    if col != expected[i]:
                        self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]), data=data)
                        err = True
                    i += 1
                m.update(bytes(col, encoding='ascii'))
                m.update(b'\n')
        elif sorting == 'python':
            if '.' in pyscript:
                [mod, fnc] = pyscript.rsplit('.', 1)
                try:
                    if mod.startswith('.'):
                        pymod = importlib.import_module(mod, 'MonetDBtesting')
                    else:
                        pymod = importlib.import_module(mod)
                except ModuleNotFoundError:
                    self.query_error(query, 'cannot import filter function module')
                    err = True
                else:
                    try:
                        pyfnc = getattr(pymod, fnc)
                    except AttributeError:
                        self.query_error(query, 'cannot find filter function')
                        err = True
            elif re.match(r'[_a-zA-Z][_a-zA-Z0-9]*$', pyscript) is None:
                self.query_error(query, 'filter function is not an identifier')
                err = True
            else:
                try:
                    pyfnc = eval(pyscript)
                except NameError:
                    self.query_error(query, 'cannot find filter function')
                    err = True
            if not err:
                try:
                    data = pyfnc(data)
                except:
                    self.query_error(query, 'filter function failed')
                    err = True
            ncols = 1
            if (len(data)):
                ncols = len(data[0])
            if len(data)*ncols != nresult:
                self.query_error(query, 'received {} rows, expected {} rows'.format(len(data)*ncols, nresult), data=data)
                return False
            for row in data:
                for col in row:
                    if expected is not None:
                        if col != expected[i]:
                            self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]), data=data)
                            err = True
                        i += 1
                    m.update(bytes(col, encoding='ascii'))
                    m.update(b'\n')
        else:
            if sorting == 'rowsort':
                data.sort()
            err_msg_buff = []
            for row in data:
                for col in row:
                    if expected is not None:
                        if col != expected[i]:
                            err_msg_buff.append('unexpected value;\nreceived "%s"\nexpected "%s"' % (col, expected[i]))
                            #self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]), data=data)
                            err = True
                        i += 1
                    m.update(bytes(col, encoding='ascii'))
                    m.update(b'\n')
            if err:
                self.query_error(query, '\n'.join(err_msg_buff), data=data)
        h = m.hexdigest()
        if not err:
            if hashlabel is not None and hashlabel in self.hashes and self.hashes[hashlabel][0] != h:
                self.query_error(query, 'query hash differs from previous query at line %d' % self.hashes[hashlabel][1], data=data)
                err = True
            elif hash is not None and h != hash:
                self.query_error(query, 'hash mismatch; received: "%s", expected: "%s"' % (h, hash), data=data)
                err = True
        if hashlabel is not None and hashlabel not in self.hashes:
            if hash is not None:
                self.hashes[hashlabel] = (hash, self.qline)
            elif not err:
                self.hashes[hashlabel] = (h, self.qline)
        return False if err else True

    def initfile(self, f):
        self.name = f
        self.file = open(f, 'r', encoding='utf-8', errors='replace')
        self.line = 0
        self.hashes = {}

    def readline(self):
        self.line += 1
        return self.file.readline()

    def parse(self, f):
        self.initfile(f)
        while True:
            skipping = False
            line = self.readline()
            if not line:
                break
            if line[0] == '#': # skip mal comments
                break
            conn = None
            # look for connection string
            if line.startswith('@connection'):
                conn_params = parse_connection_string(line)
                conn = self.get_connection(conn_params.get('conn_id')) or self.add_connection(**conn_params)
                line = self.readline()
            line = line.split()
            if not line:
                continue
            while line[0] == 'skipif' or line[0] == 'onlyif':
                if line[0] == 'skipif' and line[1] == 'MonetDB':
                    skipping = True
                elif line[0] == 'onlyif' and line[1] != 'MonetDB':
                    skipping = True
                line = self.readline().split()
            hashlabel = None
            if line[0] == 'hash-threshold':
                pass
            elif line[0] == 'statement':
                expected_err_code = None
                expected_err_msg = None
                expected_rowcount = None
                expectok = line[1] == 'ok'
                if len(line) > 2:
                    if expectok:
                        if line[2] == 'rowcount':
                            expected_rowcount = int(line[3])
                    else:
                        err_str = " ".join(line[2:])
                        expected_err_code, expected_err_msg = utils.parse_mapi_err_msg(err_str)
                statement = []
                self.qline = self.line + 1
                while True:
                    line = self.readline()
                    if not line or line == '\n':
                        break
                    statement.append(line.rstrip('\n'))
                if not skipping:
                    if is_copyfrom_stmt(statement):
                        stmt, stmt_less_data = prepare_copyfrom_stmt(statement)
                        self.exec_statement(stmt, expectok, err_stmt=stmt_less_data, expected_err_code=expected_err_code, expected_err_msg=expected_err_msg, expected_rowcount=expected_rowcount, conn=conn)
                    else:
                        self.exec_statement('\n'.join(statement), expectok, expected_err_code=expected_err_code, expected_err_msg=expected_err_msg, expected_rowcount=expected_rowcount, conn=conn)
            elif line[0] == 'query':
                columns = line[1]
                pyscript = None
                if len(line) > 2:
                    sorting = line[2]  # nosort,rowsort,valuesort
                    if sorting == 'python':
                        pyscript = line[3]
                        if len(line) > 4:
                            hashlabel = line[4]
                    elif len(line) > 3:
                        hashlabel = line[3]
                else:
                    sorting = 'nosort'
                query = []
                self.qline = self.line + 1
                while True:
                    line = self.readline()
                    if not line or line == '\n' or line.startswith('----'):
                        break
                    query.append(line.rstrip('\n'))
                if not line.startswith('----'):
                    raise SQLLogicSyntaxError('---- expected')
                line = self.readline()
                if not line:
                    line = '\n'
                if 'values hashing to' in line:
                    line = line.split()
                    hash = line[4]
                    expected = None
                    nresult = int(line[0])
                else:
                    hash = None
                    expected = []
                    while line and line != '\n':
                        expected.append(line.rstrip('\n'))
                        line = self.readline()
                    nresult = len(expected)
                if not skipping:
                    self.exec_query('\n'.join(query), columns, sorting, pyscript, hashlabel, nresult, hash, expected, conn=conn)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a Sqllogictest')
    parser.add_argument('--host', action='store', default='localhost',
                        help='hostname where the server runs')
    parser.add_argument('--port', action='store', type=int, default=50000,
                        help='port the server listens on')
    parser.add_argument('--database', action='store', default='demo',
                        help='name of the database')
    parser.add_argument('--nodrop', action='store_true',
                        help='do not drop tables at start of test')
    parser.add_argument('--verbose', action='store_true',
                        help='be a bit more verbose')
    parser.add_argument('--results', action='store',
                        type=argparse.FileType('w'),
                        help='file to store results of queries')
    parser.add_argument('--report', action='store', default='',
                        help='information to add to any error messages')
    parser.add_argument('tests', nargs='*', help='tests to be run')
    opts = parser.parse_args()
    args = opts.tests
    sql = SQLLogic(report=opts.report)
    sql.res = opts.results
    sql.connect(hostname=opts.host, port=opts.port, database=opts.database)
    for test in args:
        try:
            if not opts.nodrop:
                sql.drop()
            if opts.verbose:
                print('now testing {}'. format(test))
            try:
                sql.parse(test)
            except SQLLogicSyntaxError:
                pass
        except BrokenPipeError:
            break
    sql.close()
