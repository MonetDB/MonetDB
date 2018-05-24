#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog -x "java nl.cwi.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f \"$RELSRCDIR/../except-union-intersect-bug-sf-1146079.sql\""

rm -f .monetdb
