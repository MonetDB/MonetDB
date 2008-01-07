#!/bin/bash

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog -x "$MTIMEOUT mclient -lsql -h $HOST -p $MAPIPORT -d ${TSTDB} \"$RELSRCDIR/local_temp_table_data.SF-1865953.sql\""
Mlog -x "$MTIMEOUT mclient -lsql -h $HOST -p $MAPIPORT -d ${TSTDB}"

