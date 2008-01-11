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
# Portions created by CWI are Copyright (C) 1997-2008 CWI.
# All Rights Reserved.

# runs a test by the given name and compares the output to the PostgreSQL
# output in the expected directory

if [ -n $INSTALL_DIR -a -d $INSTALL_DIR ];
then
	monetdir=${INSTALL_DIR}
elif [ -n $MONETDB_PREFIX -a -d $MONETDB_PREFIX ];
then
	monetdir=${MONETDB_PREFIX}
else
	echo "MonetDB installation not found, please set MONETDB_PREFIX"
	exit -1
fi

jdbcclient="java -jar ${monetdir}/share/MonetDB/lib/jdbcclient.jar -e"

if [ "$1" == "" ];
then
	echo "Usage: $0 <script.sql> [...]"
	exit -1
fi

for script in $*;
do
	$jdbcclient -f $script 2>&1 \
		| grep -v "affected row" \
		| grep -v "Operation successful" \
		| sed -r \
			-e "s/^\+([-=]+\+)+$//g" \
			-e "s/^\|(.+)\|$/\1/g" \
			-e "/^$/d" \
			-e "s/([0-9]+) row(s?)/(\1 row\2)\n/g" \
		| diff -ubBw expected/`basename $script .sql`.out -
done
