#!/bin/sh
PWD=`pwd`

rm -rf /ufs/niels/data/dbfarm/sql 
Mcreatedb -db sql
Mserver -db sql ../parser/catalog.mil ../parser/sql.mil <<EOF
sql( "$PWD/as3ap.sql", "-", 1);
quit;
EOF
