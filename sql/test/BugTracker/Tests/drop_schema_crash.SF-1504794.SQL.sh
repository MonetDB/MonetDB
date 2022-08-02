#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog -x "java nl.cwi.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d ${TSTDB} -f \"$RELSRCDIR/drop_schema_crash.SF-1504794-data.sql\""
