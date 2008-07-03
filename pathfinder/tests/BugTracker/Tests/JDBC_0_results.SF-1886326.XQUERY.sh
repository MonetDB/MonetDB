#!/bin/bash

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog -x "java nl.cwi.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -l xquery -f $TST.0.xq"

rm -f .monetdb
