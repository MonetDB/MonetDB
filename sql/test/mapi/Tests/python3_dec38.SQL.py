#!/usr/bin/env python3

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

import pymonetdb
import sys, os
from decimal import Decimal

dbh = pymonetdb.connect(port=int(os.getenv('MAPIPORT')),database=os.getenv('TSTDB'),hostname=os.getenv('MAPIHOST'),autocommit=True)

cursor = dbh.cursor()

cursor.execute('CREATE TABLE python_dec38 (d38_0 DECIMAL(38,0), d38_19 DECIMAL(38,19), d38_38 DECIMAL(38,38));')
if cursor.execute('INSERT INTO python_dec38 VALUES (12345678901234567899876543210987654321, 1234567890123456789.9876543210987654321, .12345678901234567899876543210987654321);') != 1:
    sys.stderr.write("1 row inserted expected")
cursor.execute('SELECT * FROM python_dec38;')
if cursor.fetchall() != [(Decimal('12345678901234567899876543210987654321'), Decimal('1234567890123456789.9876543210987654321'), Decimal('0.12345678901234567899876543210987654321'))]:
    sys.stderr.write("Result set [(Decimal('12345678901234567899876543210987654321'), Decimal('1234567890123456789.9876543210987654321'), Decimal('0.12345678901234567899876543210987654321'))] expected")

cursor.execute('DROP TABLE python_dec38;')
cursor.close()
dbh.close()
