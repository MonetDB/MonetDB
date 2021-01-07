#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import pymonetdb
import sys

dbh = pymonetdb.connect(port=int(sys.argv[1]),database=sys.argv[2],hostname=sys.argv[3],autocommit=True)

cursor = dbh.cursor()
cursor.execute('select 1;')
if cursor.fetchall() != [(1,)]:
    sys.stderr.write("Just row (1,) expected")

cursor = dbh.cursor()
cursor.execute('select 2;')
if cursor.fetchall() != [(2,)]:
    sys.stderr.write("Just row (2,) expected")

# deliberately executing a wrong SQL statement:
try:
    cursor.execute('select commit_action, access from tables group by access;')
    sys.stderr.write("Grouping error expected")
except pymonetdb.OperationalError as e:
    if 'SELECT: cannot use non GROUP BY column \'commit_action\' in query results without an aggregate function' not in str(e):
        raise e

cursor.execute('create table python_table (i smallint,s string);')
cursor.execute('insert into python_table values ( 3, \'three\');')
cursor.execute('insert into python_table values ( 7, \'seven\');')
cursor.execute('select * from python_table;')
if cursor.fetchall() != [(3, 'three'), (7, 'seven')]:
    sys.stderr.write("Result set [(3, 'three'), (7, 'seven')] expected")

s = ((0, 'row1'), (1, 'row2'))
if cursor.executemany("insert into python_table VALUES (%s, %s);", s) != 2:
    sys.stderr.write("2 rows inserted expected")

cursor.execute('drop table python_table;')
