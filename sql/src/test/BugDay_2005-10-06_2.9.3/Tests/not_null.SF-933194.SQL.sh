#!/bin/bash

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog   "$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d $TSTDB -f $RELSRCDIR/not_null.SF-933194-src.sql"
	$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d $TSTDB -f $RELSRCDIR/not_null.SF-933194-src.sql 


rm -f .monetdb
