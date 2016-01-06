#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import monetdb.sql
import sys

dbh = monetdb.sql.Connection(port=int(sys.argv[1]),database=sys.argv[2],hostname=sys.argv[3],autocommit=True)

cursor = dbh.cursor()

cursor.execute('CREATE TABLE python_dec38 (d38_0 DECIMAL(38,0), d38_19 DECIMAL(38,19), d38_38 DECIMAL(38,38));')
cursor.execute('INSERT INTO python_dec38 VALUES (12345678901234567899876543210987654321, 1234567890123456789.9876543210987654321, .12345678901234567899876543210987654321);')
cursor.execute('SELECT * FROM python_dec38;')
result = cursor.fetchall()
print(result)
print(result[0])
print(result[0][0])
print(result[0][1])
print(result[0][2])

cursor.execute('DROP TABLE python_dec38;')
