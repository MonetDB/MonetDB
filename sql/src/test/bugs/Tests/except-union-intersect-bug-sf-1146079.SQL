#!/bin/bash

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog   "$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../except-union-intersect-bug-sf-1146079.sql"
	$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../except-union-intersect-bug-sf-1146079.sql 

rm -f .monetdb
