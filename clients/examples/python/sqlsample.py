#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

from __future__ import print_function

import pymonetdb
import sys

dbh = pymonetdb.connect(port=int(sys.argv[1]),database=sys.argv[2],hostname=sys.argv[3],autocommit=True)

cursor = dbh.cursor();
cursor.execute('select 1;')
print(cursor.fetchall())

cursor = dbh.cursor();
cursor.execute('select 2;')
print(cursor.fetchone())

# deliberately executing a wrong SQL statement:
try:
    cursor.execute('( xyz 1);')
except pymonetdb.OperationalError as e:
    print(e)

cursor.execute('create table python_table (i smallint,s string);');
cursor.execute('insert into python_table values ( 3, \'three\');');
cursor.execute('insert into python_table values ( 7, \'seven\');');
cursor.execute('select * from python_table;');
print(cursor.fetchall())

s = ((0, 'row1'), (1, 'row2'))
x = cursor.executemany("insert into python_table VALUES (%s, %s);", s)
print(x);

cursor.execute('drop table python_table;');
