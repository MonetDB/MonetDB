# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

import pymonetdb
import hashlib
import re
import sys
import getopt

port = 50000
db = 'demo'
hostname = 'localhost'
defsorting = 'rowsort'
hashlimit = 10
opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'port=', 'database=', 'sort=', 'hashlimit='])
for o, a in opts:
    if o == '--host':
        hostname = a
    elif o == '--port':
        port = int(a)
    elif o == '--database':
        db = a
    elif o == '--sort':
        if a in ('nosort', 'valuesort', 'rowsort'):
            defsorting = a
        else:
            print('unknown sort option', out=sys.stderr)
            sys.exit(1)
    elif o == '--hashlimit':
        hashlimit = int(a)

dbh = pymonetdb.connect(username='monetdb', password='monetdb', hostname=hostname, port=port, database=db, autocommit=True)
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
                if 'order by' in query or 'ORDER BY' in query:
                    sorting = 'nosort'
                else:
                    sorting = defsorting
                print('query {} {}'.format(args, sorting))
                print(query)
                print('----')
                data = crs.fetchall()
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
                if nvalues < hashlimit:
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
