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
# Portions created by CWI are Copyright (C) 1997-2005 CWI.
# All Rights Reserved.

# converts PostgreSQL specific SQL into SQL99 equivalent (if possible)

dirout="sql99"
dirin="sql"
# create a directory to output our converted files to
mkdir $dirout >& /dev/null

files=`cd ${dirin}; ls *.sql`
for file in $files;
do
	sed -r \
		-e 's/\bint8\b/bigint/ig' \
		-e 's/\bint4\b/integer/ig' \
		-e 's/\bint2\b/smallint/ig' \
		-e 's/\s+([^\s]+)::float[248]\b/ cast(\1 as double)/ig' \
		-e 's/\s+([^\s]+)::int2\b/ cast(\1 as smallint)/ig' \
		-e 's/\s+([^\s]+)::int4\b/ cast(\1 as integer)/ig' \
		-e 's/\s+([^\s]+)::int8\b/ cast(\1 as bigint)/ig' \
		-e 's/\s+([^\s]+)::(\w+(\([0-9]+(,[0-9]+))\)?)\b/ cast(\1 as \2)/ig' \
		${dirin}/${file} > ${dirout}/${file}
done
