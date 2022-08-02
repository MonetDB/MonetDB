#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog -x "java nl.cwi.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f \"$TSTSRCDIR/type_dump_test.SF-989257-src.sql\""

rm -f .monetdb
