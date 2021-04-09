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
        self.approve = None
        self.threshold = 100
        self.seenerr = False

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
                self.crs.execute('drop {} "{}"."{}" cascade'.format(row[2], row[0].replace('"', '""'), row[1].replace('"', '""')))
            except pymonetdb.Error:
                # perhaps already dropped because of the cascade
                pass
        self.crs.execute('select s.name, f.name, ft.function_type_keyword from functions f, schemas s, function_types ft where not f.system and f.schema_id = s.id and f.type = ft.function_type_id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop all {} "{}"."{}"'.format(row[2], row[0].replace('"', '""'), row[1].replace('"', '""')))
            except pymonetdb.Error:
                # perhaps already dropped
                pass
        self.crs.execute('select s.name, q.name from sys.sequences q, schemas s where q.schema_id = s.id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop sequence "{}"."{}"'.format(row[0].replace('"', '""'), row[1].replace('"', '""')))
            except pymonetdb.Error:
                # perhaps already dropped
                pass
        self.crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
        for row in self.crs.fetchall():
            try:
                self.crs.execute('alter user "{}" SET SCHEMA "sys"'.format(row[0].replace('"', '""')))
            except pymonetdb.Error:
                pass
        self.crs.execute('select name from sys.schemas where not system')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop schema "{}" cascade'.format(row[0].replace('"', '""')))
            except pymonetdb.Error:
                pass
        self.crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop user "{}"'.format(row[0].replace('"', '""')))
            except pymonetdb.Error:
                pass

    def exec_statement(self, statement, expectok,
                       err_stmt=None,
                       expected_err_code=None,
                       expected_err_msg=None,
                       expected_rowcount=None,
                       conn=None):
        crs = conn.cursor() if conn else self.crs
        if skipidx.search(statement) is not None:
            # skip creation of ascending or descending index
            return ['statement', 'ok']
        try:
            affected_rowcount = crs.execute(statement)
        except (pymonetdb.Error, ValueError) as e:
            msg = e.args[0]
            if not expectok:
                result = ['statement', 'error']
                if expected_err_code or expected_err_msg:
                    # check whether failed as expected
                    err_code_received, err_msg_received = utils.parse_mapi_err_msg(msg)
                    if expected_err_code and expected_err_msg:
                        result.append(err_code_received + '!' + err_msg_received)
                        if expected_err_code == err_code_received and expected_err_msg.lower() == err_msg_received.lower():
                            return result
                    else:
                        if expected_err_code:
                            result.append(err_code_received + '!')
                            if expected_err_code == err_code_received:
                                return result
                        if expected_err_msg:
                            result.append(err_msg_received)
                            if expected_err_msg.lower() == err_msg_received.lower():
                                return result
                    msg = "statement was expected to fail with" \
                            + (" error code {}".format(expected_err_code) if expected_err_code else '')\
                            + (", error message {}".format(str(expected_err_msg)) if expected_err_msg else '')
                    self.query_error(err_stmt or statement, str(msg), str(e))
                return result
        except ConnectionError as e:
            self.query_error(err_stmt or statement, 'Server may have crashed', str(e))
            return ['statenent', 'crash'] # should never be approved
        except KeyboardInterrupt:
            raise
        except:
            type, value, traceback = sys.exc_info()
            self.query_error(statement, 'unexpected error from pymonetdb', str(value))
            return ['statement', 'error']
        else:
            result = ['statement', 'ok']
            if expectok:
                if expected_rowcount:
                    result.append('rowcount')
                    result.append('{}'.format(affected_rowcount))
                    if expected_rowcount != affected_rowcount:
                        self.query_error(err_stmt or statement, "statement was expecting to succeed with {} rows but received {} rows!".format(expected_rowcount, affected_rowcount))
                return result
            msg = None
        self.query_error(err_stmt or statement, expectok and "statement was expected to succeed but didn't" or "statement was expected to fail but didn't", msg)
        return ['statement', 'error']

    def convertresult(self, query, columns, data):
        ndata = []
        for row in data:
            if len(row) != len(columns):
                self.query_error(query, 'wrong number of columns received')
                return None
            nrow = []
            for i in range(len(columns)):
                if row[i] is None:
                    nrow.append('NULL')
                elif self.language == 'sql' and row[i] == 'NULL':
                    nrow.append('NULL')
                elif self.language == 'mal' and row[i] == 'nil':
                    nrow.append('NULL')
                elif columns[i] == 'I':
                    if row[i] in ('true', 'True'):
                        nrow.append('1')
                    elif row[i] in ('false', 'False'):
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
                    self.raise_error('incorrect column type indicator')
            ndata.append(tuple(nrow))
        return ndata

    def raise_error(self, message):
        print('Syntax error in test file, line {}:'.format(self.qline), file=self.out)
        print(message, file=self.out)
        raise SQLLogicSyntaxError(message)

    def query_error(self, query, message, exception=None, data=None):
        self.seenerr = True
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
            return ['statement', 'error'], []
        except KeyboardInterrupt:
            raise
        except:
            type, value, traceback = sys.exc_info()
            self.query_error(query, 'unexpected error from pymonetdb', str(value))
            return ['statement', 'error'], []
        try:
            data = crs.fetchall()
        except KeyboardInterrupt:
            raise
        except:
            type, value, traceback = sys.exc_info()
            self.query_error(query, 'unexpected error from pymonetdb', str(value))
            return ['statement', 'error'], []
        if crs.description:
            rescols = []
            for desc in crs.description:
                if desc.type_code in ('boolean', 'tinyint', 'smallint', 'int', 'bigint', 'hugeint', 'bit', 'sht', 'lng', 'hge', 'oid', 'void'):
                    rescols.append('I')
                elif desc.type_code in ('decimal', 'double', 'real', 'flt', 'dbl'):
                    rescols.append('R')
                else:
                    rescols.append('T')
            rescols = ''.join(rescols)
            if len(crs.description) != len(columns):
                self.query_error(query, 'received {} columns, expected {} columns'.format(len(crs.description), len(columns)), data=data)
                columns = rescols
                err = True
        else:
            # how can this be?
            #self.query_error(query, 'no crs.description')
            rescols = 'T'
        if sorting != 'python' and crs.rowcount * len(columns) != nresult:
            if not err:
                self.query_error(query, 'received {} rows, expected {} rows'.format(crs.rowcount, nresult // len(columns)), data=data)
                err = True
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
        if columns != rescols and self.approve:
            resdata = self.convertresult(query, rescols, data)
        else:
            resdata = None
        data = self.convertresult(query, columns, data)
        if data is None:
            return ['statement', 'error'], []
        m = hashlib.md5()
        resm = hashlib.md5()
        i = 0
        result = []
        if sorting == 'valuesort':
            ndata = []
            for row in data:
                for col in row:
                    ndata.append(col)
            ndata.sort()
            for col in ndata:
                if expected is not None:
                    if i < len(expected) and col != expected[i]:
                        self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]), data=data)
                        err = True
                    i += 1
                m.update(bytes(col, encoding='ascii'))
                m.update(b'\n')
                result.append(col)
            if resdata is not None:
                result = []
                ndata = []
                for row in resdata:
                    for col in row:
                        ndata.append(col)
                ndata.sort()
                for col in ndata:
                    resm.update(bytes(col, encoding='ascii'))
                    resm.update(b'\n')
                    result.append(col)
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
                if resdata is not None:
                    try:
                        resdata = pyfnc(resdata)
                    except:
                        resdata = None
            ncols = 1
            if (len(data)):
                ncols = len(data[0])
            if len(data)*ncols != nresult:
                self.query_error(query, 'received {} rows, expected {} rows'.format(len(data)*ncols, nresult), data=data)
                err = True
            for row in data:
                for col in row:
                    if expected is not None:
                        if i < len(expected) and col != expected[i]:
                            self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]), data=data)
                            err = True
                        i += 1
                    m.update(bytes(col, encoding='ascii'))
                    m.update(b'\n')
                    result.append(col)
            if resdata is not None:
                result = []
                for row in resdata:
                    for col in row:
                        resm.update(bytes(col, encoding='ascii'))
                        resm.update(b'\n')
                        result.append(col)
        else:
            if sorting == 'rowsort':
                data.sort()
            err_msg_buff = []
            for row in data:
                for col in row:
                    if expected is not None:
                        if i < len(expected) and col != expected[i]:
                            err_msg_buff.append('unexpected value;\nreceived "%s"\nexpected "%s"' % (col, expected[i]))
                            #self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]), data=data)
                            err = True
                        i += 1
                    m.update(bytes(col, encoding='ascii'))
                    m.update(b'\n')
                    result.append(col)
            if err:
                self.query_error(query, '\n'.join(err_msg_buff), data=data)
            if resdata is not None:
                if sorting == 'rowsort':
                    resdata.sort()
                result = []
                for row in resdata:
                    for col in row:
                        resm.update(bytes(col, encoding='ascii'))
                        resm.update(b'\n')
                        result.append(col)
        h = m.hexdigest()
        if resdata is not None:
            resh = resm.hexdigest()
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
        result1 = ['query', rescols, sorting]
        if sorting == 'python':
            result1.append(pyscript)
        if hashlabel:
            result1.append(hashlabel)
        if len(result) > self.threshold:
            result2 = ['{} values hashing to {}'.format(len(result), h if resdata is None else resh)]
        else:
            result2 = result
        return result1, result2

    def initfile(self, f):
        self.name = f
        self.file = open(f, 'r', encoding='utf-8', errors='replace')
        self.line = 0
        self.hashes = {}

    def readline(self):
        self.line += 1
        return self.file.readline()

    def writeline(self, line=''):
        if self.approve:
            self.approve.write(line)
            if not line.endswith('\n'):
                self.approve.write('\n')

    def parse_connection_string(self, s: str) -> dict:
        '''parse strings like @connection(id=con1, ...)
        '''
        res = dict()
        if not (s.startswith('@connection(') and s.endswith(')')):
            self.raise_error('invalid connection string!')
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
                self.raise_error('invalid connection parameters definition!')
        if len(res.keys()) > 1:
            try:
                assert res.get('username')
                assert res.get('password')
            except AssertionError as e:
                self.raise_error('invalid connection parameters definition, username or password missing!')
        return res

    def parse(self, f, approve=None):
        self.approve = approve
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
                self.writeline(line)
                line = self.readline()
            words = line.split()
            if not words:
                continue
            while words[0] == 'skipif' or words[0] == 'onlyif':
                if words[0] == 'skipif' and words[1] == 'MonetDB':
                    skipping = True
                elif words[0] == 'onlyif' and words[1] != 'MonetDB':
                    skipping = True
                self.writeline(line)
                line = self.readline()
                words = line.split()
            hashlabel = None
            if words[0] == 'hash-threshold':
                self.threshold = int(words[1])
                self.writeline(line)
            elif words[0] == 'statement':
                expected_err_code = None
                expected_err_msg = None
                expected_rowcount = None
                expectok = words[1] == 'ok'
                if len(words) > 2:
                    if expectok:
                        if words[2] == 'rowcount':
                            expected_rowcount = int(words[3])
                    else:
                        err_str = " ".join(words[2:])
                        expected_err_code, expected_err_msg = utils.parse_mapi_err_msg(err_str)
                statement = []
                self.qline = self.line + 1
                stline = line
                while True:
                    line = self.readline()
                    if not line or line == '\n':
                        break
                    statement.append(line.rstrip('\n'))
                if not skipping:
                    if is_copyfrom_stmt(statement):
                        stmt, stmt_less_data = prepare_copyfrom_stmt(statement)
                        result = self.exec_statement(stmt, expectok, err_stmt=stmt_less_data, expected_err_code=expected_err_code, expected_err_msg=expected_err_msg, expected_rowcount=expected_rowcount, conn=conn)
                    else:
                        result = self.exec_statement('\n'.join(statement), expectok, expected_err_code=expected_err_code, expected_err_msg=expected_err_msg, expected_rowcount=expected_rowcount, conn=conn)
                    self.writeline(' '.join(result))
                else:
                    self.writeline(stline)
                for line in statement:
                    self.writeline(line)
                self.writeline()
            elif words[0] == 'query':
                columns = words[1]
                pyscript = None
                if len(words) > 2:
                    sorting = words[2]  # nosort,rowsort,valuesort
                    if sorting == 'python':
                        pyscript = words[3]
                        if len(words) > 4:
                            hashlabel = words[4]
                    elif len(words) > 3:
                        hashlabel = words[3]
                else:
                    sorting = 'nosort'
                query = []
                self.qline = self.line + 1
                qrline = line
                while True:
                    line = self.readline()
                    if not line or line == '\n' or line.startswith('----'):
                        break
                    query.append(line.rstrip('\n'))
                if not line.startswith('----'):
                    self.raise_error('---- expected')
                line = self.readline()
                if not line:
                    line = '\n'
                if 'values hashing to' in line:
                    words = line.split()
                    hash = words[4]
                    expected = None
                    nresult = int(words[0])
                else:
                    hash = None
                    expected = []
                    while line and line != '\n':
                        expected.append(line.rstrip('\n'))
                        line = self.readline()
                    nresult = len(expected)
                if not skipping:
                    result1, result2 = self.exec_query('\n'.join(query), columns, sorting, pyscript, hashlabel, nresult, hash, expected, conn=conn)
                    self.writeline(' '.join(result1))
                    for line in query:
                        self.writeline(line)
                    self.writeline('----')
                    for line in result2:
                        self.writeline(line)
                else:
                    self.writeline(qrline)
                    for line in query:
                        self.writeline(line)
                    self.writeline('----')
                    if hash:
                        self.writeline('{} values hashing to {}'.format(
                            nresult, hash))
                    else:
                        for line in expected:
                            self.writeline(line)
                self.writeline()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a Sqllogictest')
    parser.add_argument('--host', action='store', default='localhost',
                        help='hostname where the server runs')
    parser.add_argument('--port', action='store', type=int, default=50000,
                        help='port the server listens on')
    parser.add_argument('--database', action='store', default='demo',
                        help='name of the database')
    parser.add_argument('--language', action='store', default='sql',
                        help='language to use for testing')
    parser.add_argument('--nodrop', action='store_true',
                        help='do not drop tables at start of test')
    parser.add_argument('--verbose', action='store_true',
                        help='be a bit more verbose')
    parser.add_argument('--results', action='store',
                        type=argparse.FileType('w'),
                        help='file to store results of queries')
    parser.add_argument('--report', action='store', default='',
                        help='information to add to any error messages')
    parser.add_argument('--approve', action='store',
                        type=argparse.FileType('w'),
                        help='file in which to produce a new .test file with updated results')
    parser.add_argument('tests', nargs='*', help='tests to be run')
    opts = parser.parse_args()
    args = opts.tests
    sql = SQLLogic(report=opts.report)
    sql.res = opts.results
    sql.connect(hostname=opts.host, port=opts.port, database=opts.database, language=opts.language)
    for test in args:
        try:
            if not opts.nodrop:
                sql.drop()
            if opts.verbose:
                print('now testing {}'. format(test))
            try:
                sql.parse(test, approve=opts.approve)
            except SQLLogicSyntaxError:
                pass
        except BrokenPipeError:
            break
    sql.close()
    if sql.seenerr:
        sys.exit(1)
