# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

import pymonetdb
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
parser.add_argument('--sort', action='store', default='rowsort',
                    choices=['nosort','rowsort','valuesort'],
                    help='how to sort the values')
parser.add_argument('--hashlimit', action='store', type=int, default=10,
                    help='hash limit')
parser.add_argument('--results', action='store', type=argparse.FileType('w'),
                    help='file to store results of queries')
opts = parser.parse_args()

dbh = pymonetdb.connect(username='monetdb',
                        password='monetdb',
                        hostname=opts.host,
                        port=opts.port,
                        database=opts.database,
                        autocommit=True)
crs = dbh.cursor()

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

query = []
while True:
    line = sys.stdin.readline()
    if not line:
        break
    line = line.rstrip()
    if not line or line.lstrip().startswith('--') or line.lstrip().startswith('#'):
        continue
    if '--' in line:
        line = line[:line.index('--')].rstrip()
    if line.endswith(';'):
        query.append(line.rstrip(';'))
        query = '\n'.join(query)
        try:
            crs.execute(query)
        except pymonetdb.DatabaseError:
            print('statement error')
            print(query)
            print('')
        except pymonetdb.Error:
            print('exception raised on query "{}"'.format(query), file=sys.stderr)
            sys.exit(1)
        else:
            if crs.description is None:
                print('statement ok')
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
        query = []
    else:
        query.append(line)
