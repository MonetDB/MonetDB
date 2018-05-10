#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog -x "java nl.cwi.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d ${TSTDB} -f \"$RELSRCDIR/jdbc_no_debug.SF-1739356-data.sql\""

rm -f .monetdb
