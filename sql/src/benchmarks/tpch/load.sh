#!/bin/sh
set -x

PWD=`pwd`
echo $PWD

SQL='Msql -u monetdb -P monetdb'
if [ $# -eq 1 ]; then
	SQL="$1"
fi

$SQL < c.sql-dec
cat load.sql | sed -e s+PWD+$PWD/SF-0.01+ | $SQL

