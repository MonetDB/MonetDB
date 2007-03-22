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
SQL="MapiClient -lsql -umonetdb -Pmonetdb -h $HOST -p $MAPIPORT"
SQL1="MapiClient -lsql -uskyserver -Pskyserver -h $HOST -p $MAPIPORT"

#Generate Data...

#Bulk-Load into SQL server...

echo Create User
$SQL < ${RELSRCDIR}/../create_user.sql

echo tables
$SQL1 < $RELSRCDIR/../../../sql/math.sql
$SQL1 < $RELSRCDIR/../../../sql/cache.sql
$SQL1 < $RELSRCDIR/../../../sql/ms_functions.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_tables.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_constraints.sql

echo views
$SQL1 < ${RELSRCDIR}/../Skyserver_views.sql

echo functions
$SQL1 < ${RELSRCDIR}/../Skyserver_functions.sql

echo Cleanup
$SQL1 < ${RELSRCDIR}/../Skyserver_dropFunctions.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_dropMs_functions.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_dropMath.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_dropCache.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_dropViews.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_dropConstraints.sql
$SQL1 < ${RELSRCDIR}/../Skyserver_dropTables.sql

echo Remove User
$SQL < ${RELSRCDIR}/../drop_user.sql
