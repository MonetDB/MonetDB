#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

# skipif <system>
# onlyif <system>

# statement (ok|error)
# query (I|T|R)+ (nosort|rowsort|valuesort)? [arg]
#       I: integer; T: text (string); R: real (decimal)
#       nosort: do not sort
#       rowsort: sort rows
#       valuesort: sort individual values
# hash-threshold number
# halt

import pymonetdb
import hashlib
import re
import sys

skipidx = re.compile(r'create index .* \b(asc|desc)\b', re.I)

class SQLLogicSyntaxError(Exception):
    pass

class SQLLogic:
    def __init__(self, report=None, out=sys.stdout):
        self.dbh = None
        self.crs = None
        self.out = out
        self.res = None
        self.rpt = report

    def connect(self, username='monetdb', password='monetdb',
                hostname='localhost', port=None, database='demo'):
        self.dbh = pymonetdb.connect(username=username,
                                     password=password,
                                     hostname=hostname,
                                     port=port,
                                     database=database,
                                     autocommit=True)
        self.crs = self.dbh.cursor()

    def close(self):
        if self.crs:
            self.crs.close()
            self.crs = None
        if self.dbh:
            self.dbh.close()
            self.dbh = None

    def drop(self):
        self.crs.execute('select name from tables where not system')
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop table "%s" cascade' % row[0].replace('"', '""'))
            except:
                pass

    def exec_statement(self, statement, expectok):
        if skipidx.search(statement) is not None:
            # skip creation of ascending or descending index
            return
        try:
            self.crs.execute(statement)
        except pymonetdb.DatabaseError:
            if not expectok:
                return
        except:
            type, value, traceback = sys.exc_info()
            self.query_error(statement, 'unexpected error from pymonetdb', str(value))
            return
        else:
            if expectok:
                return
        self.query_error(statement, "statement didn't give expected result", expectok and "statement was expected to succeed but didn't" or "statement was expected to fail bat didn't")

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
                    if row[i] == '':
                        nrow.append('(empty)')
                    else:
                        nval = []
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

    def query_error(self, query, message, exception=None):
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

    def exec_query(self, query, columns, sorting, hashlabel, nresult, hash, expected):
        err = False
        try:
            self.crs.execute(query)
        except pymonetdb.DatabaseError as e:
            self.query_error(query, 'query failed', e.args[0])
            return
        except:
            type, value, traceback = sys.exc_info()
            self.query_error(query, 'unexpected error from pymonetdb', str(value))
            return
        if len(self.crs.description) != len(columns):
            self.query_error(query, 'received {} columns, expected {} columns'.format(len(self.crs.description), len(columns)))
            return
        if self.crs.rowcount * len(columns) != nresult:
            self.query_error(query, 'received {} rows, expected {} rows'.format(self.crs.rowcount, nresult // len(columns)))
            return
        try:
            data = self.crs.fetchall()
        except:
            type, value, traceback = sys.exc_info()
            self.query_error(query, 'unexpected error from pymonetdb', str(value))
            return
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
                        self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]))
                        err = True
                    i += 1
                m.update(bytes(col, encoding='ascii'))
                m.update(b'\n')
        else:
            if sorting == 'rowsort':
                data.sort()
            for row in data:
                for col in row:
                    if expected is not None:
                        if col != expected[i]:
                            self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]))
                            err = True
                        i += 1
                    m.update(bytes(col, encoding='ascii'))
                    m.update(b'\n')
        h = m.hexdigest()
        if not err:
            if hashlabel is not None and hashlabel in self.hashes and self.hashes[hashlabel][0] != h:
                self.query_error(query, 'query hash differs from previous query at line %d' % self.hashes[hashlabel][1])
                err = True
            elif hash is not None and h != hash:
                self.query_error(query, 'hash mismatch; received: "%s", expected: "%s"' % (h, hash))
                err = True
        if hashlabel is not None and hashlabel not in self.hashes:
            if hash is not None:
                self.hashes[hashlabel] = (hash, self.qline)
            elif not err:
                self.hashes[hashlabel] = (h, self.qline)

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
                expectok = line[1] == 'ok'
                statement = []
                self.qline = self.line + 1
                while True:
                    line = self.readline()
                    if not line or line == '\n':
                        break
                    statement.append(line.rstrip('\n'))
                if not skipping:
                    self.exec_statement('\n'.join(statement), expectok)
            elif line[0] == 'query':
                columns = line[1]
                if len(line) > 2:
                    sorting = line[2]  # nosort,rowsort,valuesort
                    if len(line) > 3:
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
                    self.exec_query('\n'.join(query), columns, sorting, hashlabel, nresult, hash, expected)

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
