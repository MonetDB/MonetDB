#!/bin/sh

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

set -x

PWD=`pwd`
echo $PWD

SQL='mclient -lsql -umonetdb -Pmonetdb'
if [ $# -eq 1 ]; then
	SQL="$1"
fi

$SQL < VOCschema.sql
cat load.sql | sed -e s+PWD+$PWD+ | $SQL 
