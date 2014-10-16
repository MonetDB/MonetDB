#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

import monetdb.sql
import sys

dbh = monetdb.sql.Connection(port=int(sys.argv[1]),database=sys.argv[2],hostname=sys.argv[3],autocommit=True)

cursor = dbh.cursor()

cursor.execute('CREATE TABLE python_int128 (i HUGEINT);')
cursor.execute('INSERT INTO python_int128 VALUES (123456789098765432101234567890987654321);')
cursor.execute('SELECT * FROM python_int128;')
result = cursor.fetchall()
print(result)
print(result[0])
print(result[0][0])

cursor.execute('DROP TABLE python_int128;')
