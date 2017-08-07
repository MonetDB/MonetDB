#!/bin/sh

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

set -x

PWD=`pwd`
echo $PWD

SQL='mclient -lsql'
if [ $# -eq 1 ]; then
	SQL="$1"
fi

#T=-
T=
SF='sf-0.01'
#SF='sf-1'
#SF='sf-2'
#SF='sf-5'

dir=`echo $SF | tr '[a-z]' '[A-Z]'`
#$SQL < c.sql
$SQL < c.sql-primary-foreign
#cat load-$SF-LOCKED.sql$T | sed -e s+PWD+$PWD/$dir+ | $SQL 
cat load-$SF.sql$T | sed -e s+PWD+$PWD/$dir+ | $SQL
#$SQL < alter.sql
