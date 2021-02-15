#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import pymonetdb
from MonetDBtesting.mapicursor import MapiCursor
import MonetDBtesting.malmapi as malmapi
import hashlib
import re
import sys
import argparse

def is_copyfrom_stmt(stmt:str):
    # TODO fix need stronger regex
    return re.match(r'copy\b[^;]*?\bfrom\s+stdin\b', stmt, re.IGNORECASE) is not None

def is_complete_stmt(query, line:str):
    stmt = query + [line]
    stmt = '\n'.join(stmt)
    if opts.language == 'sql':
        res = re.match(r'create(\s+or\s+replace)?\s+(function|procedure|aggregate|filter|window|loader)\b[^;]*\blanguage\s+\S+\s*\{', stmt, re.IGNORECASE)
        if res is not None:
            q = None
            skip = False
            n = 0
            hash = False
            for c in stmt:
                if skip:
                    skip = False
                elif hash:
                    if c == '\n':
                        hash = False
                elif c == q:
                    q = None
                elif q is not None:
                    if c == '\\':
                        skip = True
                elif c == "'" or c == '"':
                    q = c
                elif c == '{':
                    n += 1
                elif c == '}':
                    n -= 1
                elif c == ';' and n == 0:
                    return True
                elif c == '#':
                    hash = True
            return False
        if re.match(r'create(\s+or\s+replace)?\s+(function|procedure|aggregate|filter|window|loader|trigger)\b[^;]*\bbegin\b.*?\bend\b\s*;', stmt, re.DOTALL|re.IGNORECASE) is not None:
            return True
        if re.match(r'create(\s+or\s+replace)?\s+(function|procedure|aggregate|filter|window|loader|trigger)\b[^;]*\bbegin\b', stmt, re.IGNORECASE) is not None:
            # we need an "end"
            return False
        if re.match(r'create(\s+or\s+replace)?\s+(function|procedure|aggregate|filter|window|loader|trigger)\b[^;]*;', stmt, re.IGNORECASE) is not None:
            return True
        if re.match(r'create(\s+or\s+replace)?\s+(function|procedure|aggregate|filter|window|loader|trigger)\b', stmt, re.IGNORECASE) is not None:
            return False
    else:
        if re.match(r'\s*function\s', stmt) is not None:
            return re.match(r'\s*function\s.*\bend(\s+\w+)?\s*;', stmt, re.DOTALL) is not None
        if re.match(r'\s*barrier\s', stmt) is not None:
            return re.match(r'\s*barrier\s.*\bexit\s(\w+|\(\w+(,\w+)+\)\s*);', stmt, re.DOTALL) is not None
    return re.match(r'[^;]*;', stmt) is not None

def convertresult(columns, data):
    ndata = []
    for row in data:
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
        ndata.append(tuple(nrow))
    return ndata

def to_sqllogic_test(query, copy_into_data=None):
    data = []
    try:
        q = query
        if copy_into_data is not None:
            q += ';\n' + '\n'.join(copy_into_data)
        crs.execute(q)
        if crs.description:
            data = crs.fetchall()
    except (pymonetdb.Error, ValueError) as e:
        print('statement error')
        print(query)
        if copy_into_data is not None:
            print('<COPY_INTO_DATA>')
            print('\n'.join(copy_into_data))
        print('')
    else:
        if crs.description is None:
            print('statement ok')
            print(query)
            if copy_into_data is not None:
                print('<COPY_INTO_DATA>')
                print('\n'.join(copy_into_data))
            print('')
        else:
            args = ''
            for arg in crs.description:
                if arg.type_code.endswith('int'):
                    args += 'I'
                elif arg.type_code in ('real', 'double', 'decimal'):
                    args += 'R'
                else:
                    args += 'T'
            sorting = opts.sort
            if re.search(r'\sorder\s+by\s', query, re.IGNORECASE) is not None:
                sorting = 'nosort'
            print('query {} {}'.format(args, sorting))
            print(query)
            print('----')
            if opts.results:
                for row in data:
                    sep=''
                    for col in row:
                        if col is None:
                            print(sep, 'NULL', sep='', end='', file=opts.results)
                        else:
                            print(sep, col, sep='', end='', file=opts.results)
                        sep = '|'
                    print('', file=opts.results)
            data = convertresult(args, data)
            nvalues = len(args) * len(data)
            if sorting == 'valuesort':
                ndata = []
                for row in data:
                    for col in row:
                        ndata.append(col)
                ndata.sort()
                data = [ndata]
            elif sorting == 'rowsort':
                data.sort()
            if nvalues < opts.hashlimit:
                for row in data:
                    for col in row:
                        print(col)
            else:
                m = hashlib.md5()
                for row in data:
                    for col in row:
                        m.update(bytes(col, encoding='ascii'))
                        m.update(b'\n')
                h = m.hexdigest()
                print('{} values hashing to {}'.format(len(args) * crs.rowcount, h))
            print('')

opts = None
dbh = None
crs = None
def main():
    parser = argparse.ArgumentParser(description='Create a Sqllogictest')
    parser.add_argument('--host', action='store', default='localhost',
                        help='hostname where the server runs')
    parser.add_argument('--port', action='store', type=int, default=50000,
                        help='port the server listens on')
    parser.add_argument('--database', action='store', default='demo',
                        help='name of the database')
    parser.add_argument('--user', action='store', default='monetdb',
                        help='user')
    parser.add_argument('--password', action='store', default='monetdb',
                        help='password')
    parser.add_argument('--language', action='store', default='sql',
                        help='language to connect to the database')
    parser.add_argument('--sort', action='store', default='rowsort',
                        choices=['nosort','rowsort','valuesort'],
                        help='how to sort the values')
    parser.add_argument('--hashlimit', action='store', type=int, default=100,
                        help='hash limit')
    parser.add_argument('--results', action='store', type=argparse.FileType('w'),
                        help='file to store results of queries')
    global opts
    opts = parser.parse_args()

    global dbh
    global crs
    if opts.language == 'sql':
        dbh = pymonetdb.connect(username=opts.user,
                            password=opts.password,
                            hostname=opts.host,
                            port=opts.port,
                            database=opts.database,
                            autocommit=True)
        crs = dbh.cursor()
    else:
        dbh = malmapi.Connection()
        dbh.connect(
                            database=opts.database,
                            username=opts.user,
                            password=opts.password,
                            language='mal',
                            hostname=opts.host,
                            port=opts.port)
        crs = MapiCursor(dbh)

    query = []
    incomment = False
    incopy = False
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        if incomment:
            if '*/' in line:
                line = line[line.find('*/') + 2:]
                incomment = False
            else:
                continue
        line = re.sub('/\*.*?\*/', ' ', line)
        if '/*' in line:
            line = line[:line.find('/*')]
            incomment = True
        line = line.rstrip()
        if not line:
            continue
        if not query:
            res = re.match('[^\'"]*((\'[^\']*\'|"[^"]*")[^\'"]*)*#', line)
            if res is not None:
                line = res.group(0)[:-1].rstrip()
            res = re.match('[^\'"]*((\'[^\']*\'|"[^"]*")[^\'"]*)*--', line)
            if res is not None:
                line = res.group(0)[:-2].rstrip()
            if not line:
                continue
        if is_complete_stmt(query, line):
            if opts.language == 'sql':
                l = line.rstrip(';')
            else:
                l = line
            query.append(l)
            data = None
            stmt = '\n'.join(query)
            if is_copyfrom_stmt('\n'.join(query)):
                data = []
                while True:
                    line = sys.stdin.readline()
                    if not line or line == '\n':
                        break
                    data.append(line.rstrip('\n'))
            to_sqllogic_test(stmt, data)
            query = []
        else:
            query.append(line)

if __name__ == '__main__':
    main()
