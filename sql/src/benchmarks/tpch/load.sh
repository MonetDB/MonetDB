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

set -x

PWD=`pwd`
echo $PWD

SQL='MapiClient -lsql -umonetdb -Pmonetdb'
if [ $# -eq 1 ]; then
	SQL="$1"
fi

#$SQL < c.sql-dec
#SF='sf-0.01'
SF='sf-1'
#SF='sf-2'
#SF='sf-5'

dir=`echo $SF | tr '[a-z]' '[A-Z]'`
#$SQL < c.sql-dec-primary-foreign
$SQL -T < c.sql-dec
cat load-$SF.sql | sed -e s+PWD+$PWD/$dir+ | $SQL -T
$SQL -T < alter.sql
