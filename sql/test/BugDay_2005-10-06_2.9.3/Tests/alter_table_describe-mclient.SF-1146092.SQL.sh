#!/usr/bin/env bash

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog -x "$SQLCLIENT -h$HOST -p$MAPIPORT -d$TSTDB -i -e < \"$TSTSRCDIR/alter_table_describe.SF-1146092-src.sql\""

rm -f .monetdb
