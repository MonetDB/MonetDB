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

dbh = pymonetdb.connect(port=int(os.getenv('MAPIPORT')),database=os.getenv('TSTDB'),hostname=os.getenv('MAPIHOST'),autocommit=True)

cursor = dbh.cursor()

cursor.execute('CREATE TABLE python_int128 (i HUGEINT);')
if cursor.execute('INSERT INTO python_int128 VALUES (123456789098765432101234567890987654321);') != 1:
    sys.stderr.write("1 row inserted expected")
cursor.execute('SELECT * FROM python_int128;')
if cursor.fetchall() != [(123456789098765432101234567890987654321,)]:
    sys.stderr.write("Result set [(123456789098765432101234567890987654321,)] expected")

cursor.execute('DROP TABLE python_int128;')
cursor.close()
dbh.close()
