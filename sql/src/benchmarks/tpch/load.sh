#!/bin/sh
set -x

PWD=`pwd`
echo $PWD

if [ $# -lt 1 ]; then
	echo "usage load directory with data"
	exit 1
fi

sql_client < ~/data/sql/src/benchmarks/tpch/c.sql
cat load.sql | sed -e s+PWD+$PWD+ | sql_client 

