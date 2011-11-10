#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog -x "$MTIMEOUT java nl.cwi.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f \"$TSTSRCDIR/alter_table_describe.SF-1146092-src.sql\""

rm -f .monetdb
