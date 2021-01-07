#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import hashlib
import re
import sys
import argparse

parser = argparse.ArgumentParser(description='produce sqllogic test results from .stable.out')
parser.add_argument('--hashlimit', action='store', type=int, default=10,
                    help='hash limit')
parser.add_argument(
        'files', metavar='str', nargs='+', type=str, help='files to be processed')
opts = parser.parse_args()

def parse_header(line:str):
    col_types=''
    cols = line.split(',')
    for col in cols:
        col = col.replace('%', '').replace('#', '').replace('type', '').strip()
        if col =='int':
            col_types+='I'
        elif col in ('real', 'double', 'decimal'):
            col_types+='R'
        else:
            col_types+='T'
    txt = 'query {} nosort'.format(col_types)
    return {'col_types': col_types, 'txt': txt}

def parse_row(columns, line: str):
    def map_fn(x):
        x = x.strip()
        if x.startswith('\"') and x.endswith('\"'):
            x = x[1:-1]
        return x
    row = list(map(map_fn, line.split(',')))
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
                nrow.append('%d' % int(row[i]))
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
            nrow.append('%.3f' % float(row[i]))
    return tuple(nrow)

def print_result(header, values):
    if len(values) > 0:
        cols = len(values[0])
        rows = len(values)
        nvalues = cols*rows
        print('---------------------')
        print(header['txt'])
        if nvalues < opts.hashlimit:
            for row in values:
                for col in row:
                    print(col)
        else:
            m = hashlib.md5()
            for row in values:
                for col in row:
                    m.update(bytes(col, encoding='ascii'))
                    m.update(b'\n')
            h = m.hexdigest()
            print('{} values hashing to {}'.format(nvalues, h))

def work(fpath):
    hdr_rgx = re.compile('^%.*\#\s*type$')
    values_rgx = re.compile('^\[.*\]$')
    with open(fpath, 'r') as f:
        header = None
        values = []
        for line in f:
            line = line.strip()
            if not line or line.startswith('--') or line.startswith('#'):
                continue
            if hdr_rgx.match(line):
                if header is not None and len(values) > 0:
                    print_result(header, values)
                    values = []
                header = parse_header(line)
                continue
            if values_rgx.match(line) and header:
                row = parse_row(header['col_types'], line[1:-1])
                values.append(row)
                continue
        if header and len(values) > 0:
            print_result(header, values)

if __name__ == '__main__':
    for f in opts.files:
        work(f)
