#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import pymonetdb
import sys
from decimal import Decimal

dbh = pymonetdb.connect(port=int(sys.argv[1]),database=sys.argv[2],hostname=sys.argv[3],autocommit=True)

cursor = dbh.cursor()

cursor.execute('CREATE TABLE python_dec38 (d38_0 DECIMAL(38,0), d38_19 DECIMAL(38,19), d38_38 DECIMAL(38,38));')
if cursor.execute('INSERT INTO python_dec38 VALUES (12345678901234567899876543210987654321, 1234567890123456789.9876543210987654321, .12345678901234567899876543210987654321);') != 1:
    sys.stderr.write("1 row inserted expected")
cursor.execute('SELECT * FROM python_dec38;')
if cursor.fetchall() != [(Decimal('12345678901234567899876543210987654321'), Decimal('1234567890123456789.9876543210987654321'), Decimal('0.12345678901234567899876543210987654321'))]:
    sys.stderr.write("Result set [(Decimal('12345678901234567899876543210987654321'), Decimal('1234567890123456789.9876543210987654321'), Decimal('0.12345678901234567899876543210987654321'))] expected")

cursor.execute('DROP TABLE python_dec38;')
