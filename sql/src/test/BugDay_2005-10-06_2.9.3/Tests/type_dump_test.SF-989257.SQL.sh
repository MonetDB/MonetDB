#!/bin/bash

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog -x "$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d $TSTDB -f \"$RELSRCDIR/type_dump_test.SF-989257-src.sql\""

rm -f .monetdb
