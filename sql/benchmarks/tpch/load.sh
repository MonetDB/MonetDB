#!/bin/sh

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
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

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
#$SQL < c.sql-dec-primary-foreign
$SQL < c.sql-dec
cat load-$SF-LOCKED.sql$T | sed -e s+PWD+$PWD/$dir+ | $SQL 
#cat load-$SF.sql$T | sed -e s+PWD+$PWD/$dir+ 
$SQL < alter.sql
