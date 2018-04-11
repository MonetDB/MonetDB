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

cursor = dbh.cursor()

cursor.execute('CREATE TABLE python_int128 (i HUGEINT);')
cursor.execute('INSERT INTO python_int128 VALUES (123456789098765432101234567890987654321);')
cursor.execute('SELECT * FROM python_int128;')
result = cursor.fetchall()
print(result)
print(result[0])
print(result[0][0])

cursor.execute('DROP TABLE python_int128;')
