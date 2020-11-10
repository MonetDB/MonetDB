#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

import pymonetdb
from MonetDBtesting.mapicursor import MapiCursor
import MonetDBtesting.malmapi as malmapi
import hashlib
import re
import sys
import argparse

parser = argparse.ArgumentParser(description='Create a Sqllogictest')
parser.add_argument('--host', action='store', default='localhost',
                    help='hostname where the server runs')
parser.add_argument('--port', action='store', type=int, default=50000,
                    help='port the server listens on')
parser.add_argument('--database', action='store', default='demo',
                    help='name of the database')
parser.add_argument('--language', action='store', default='sql',
                    help='language to connect to the database')
parser.add_argument('--sort', action='store', default='rowsort',
                    choices=['nosort','rowsort','valuesort'],
                    help='how to sort the values')
parser.add_argument('--hashlimit', action='store', type=int, default=10,
                    help='hash limit')
parser.add_argument('--results', action='store', type=argparse.FileType('w'),
                    help='file to store results of queries')
opts = parser.parse_args()

if opts.language == 'sql':
    dbh = pymonetdb.connect(username='monetdb',
                        password='monetdb',
                        hostname=opts.host,
                        port=opts.port,
                        database=opts.database,
                        autocommit=True)
    crs = dbh.cursor()
else:
    dbh = malmapi.Connection()
    dbh.connect(
                        database=opts.database,
                        username='monetdb',
                        password='monetdb',
                        language='mal',
                        hostname=opts.host,
                        port=opts.port)
    crs = MapiCursor(dbh)

query = []

def is_psm_stmt(stmt:str):
    if opts.language == 'sql':
        rgx = re.compile(r'(create|create\s+or\s+replace)\s+(function|procedure)', re.I)
        return rgx.match(stmt) is not None
    else:
        return re.match(r'\s*function\s', stmt) is not None

def is_psm_stmt_end(stmt:str):
    if opts.language == 'sql':
        return re.search(r'end;$', stmt, re.I) is not None
    else:
        return re.match(r'\s*end(\s+\w+)?\s*;', stmt) is not None

def is_complete_psm_stmt(stmt:str):
    if opts.language == 'sql':
        rgx = re.compile(r'(create|create\s+or\s+replace)\s+(function|procedure)[\S\s]*(end;|external\s+name\s+.*;)$', re.I)
        return rgx.match(stmt) is not None
    else:
        return re.match(r'\s*function\s.*\bend(\s+\w+)?\s*;', stmt, re.DOTALL) is not None

def is_copyfrom_stmt(stmt:str):
    # TODO fix need stronger regex
    rgx = re.compile('copy.*from.*stdin.*', re.I)
    return rgx.match(stmt) is not None

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
        ndata.append(tuple(nrow))
    return ndata

def to_sqllogic_test(query, copy_into_stmt=None, copy_into_data=[]):
    try:
        crs.execute(query)
    except pymonetdb.Error as e:
        print('statement error')
        if copy_into_stmt:
            print(copy_into_stmt)
            print('<COPY_INTO_DATA>')
            print('\n'.join(copy_into_data))
        else:
            print(query)
        print('')
    else:
        if crs.description is None:
            print('statement ok')
            if copy_into_stmt:
                print(copy_into_stmt)
                print('<COPY_INTO_DATA>')
                print('\n'.join(copy_into_data))
            else:
                print(query)
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
            print('query {} {}'.format(args, sorting))
            print(query)
            print('----')
            data = crs.fetchall()
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

def monet_escape(data):
    """
    returns an escaped string
    """
    data = str(data).replace("\\", "\\\\")
    data = data.replace("\'", "\\\'")
    return "%s" % str(data)

def process_copyfrom_stmt(query):
    index = 0
    for i, n in enumerate(query):
        if n.strip().endswith(';'):
            index = i
            break
    index+=1
    copy_into_stmt = '\n'.join(query[:index]).rstrip(';')
    rest_ = query[index:]
    # escape stuff
    copy_into_data = list(map(lambda x: monet_escape(x), rest_))
    query = '\n'.join(query)
    to_sqllogic_test(query, copy_into_stmt=copy_into_stmt, copy_into_data=copy_into_data)


incomment = False
inpsm = False
while True:
    line = sys.stdin.readline()
    if not line:
        break
    line = line.rstrip()
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
    if not line or line.lstrip().startswith('--') or line.lstrip().startswith('#'):
        if len(query) > 0:
            if is_copyfrom_stmt('\n'.join(query)):
                process_copyfrom_stmt(query)
                query = []
        continue
    # when copyfrom stmt from stdin skip because data may contain --
    if opts.language == 'sql' and '--' in line and not is_copyfrom_stmt('\n'.join(query)):
        line = line[:line.index('--')].rstrip()
    res = re.match('[^\'"]*((\'[^\']*\'|"[^"]*")[^\'"]*)*#', line)
    if res is not None:
        line = res.group(0)[:-1].rstrip()
    if (inpsm and is_psm_stmt_end(line)) or (not inpsm and line.endswith(';')):
        inpsm = False
        tmp = ([] + query)
        tmp.append(line)
        stmt = '\n'.join(tmp)
        if is_copyfrom_stmt(stmt):
            query.append(line)
            continue
        if is_psm_stmt(stmt):
            if is_complete_psm_stmt(stmt):
                stmt = stmt.rstrip(';')
                to_sqllogic_test(stmt)
                query = []
            else:
                query.append(line)
                inpsm = True
            continue
        stripped = line.rstrip(';')
        query.append(stripped)
        query = '\n'.join(query)
        to_sqllogic_test(query)
        query = []
    else:
        query.append(line)
