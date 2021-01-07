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

cursor.execute('CREATE TABLE python_int128 (i HUGEINT);')
if cursor.execute('INSERT INTO python_int128 VALUES (123456789098765432101234567890987654321);') != 1:
    sys.stderr.write("1 row inserted expected")
cursor.execute('SELECT * FROM python_int128;')
if cursor.fetchall() != [(123456789098765432101234567890987654321,)]:
    sys.stderr.write("Result set [(123456789098765432101234567890987654321,)] expected")

cursor.execute('DROP TABLE python_int128;')
