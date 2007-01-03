#!/bin/sh

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

PWD=`pwd`
echo $PWD

SQL='MapiClient -lsql -umonetdb -Pmonetdb'
if [ $# -eq 1 ]; then
	SQL="$1"
fi

$SQL < sql99/create_table.sql
cat input/copy.source \
	| sed -r \
		-e s+@abs_srcdir@+$PWD+ig \
	      	-e 's/.*@abs_builddir@.*//ig' \
	      	-e "s/COPY (.*);/COPY INTO \1 USING DELIMITERS '\\\\t\', '\\\\n';/ig" \
	| $SQL
