#!/bin/bash

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog   "$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d ${TSTDB} -f $RELSRCDIR/authorization.SF-1430616-data.sql"
	$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d ${TSTDB} -f $RELSRCDIR/authorization.SF-1430616-data.sql 

cat << EOF > .monetdb
user=voc
password=voc
EOF

Mlog   "$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d ${TSTDB} -q" 
	$MTIMEOUT mjclient -h $HOST -p $MAPIPORT -d ${TSTDB} -q

rm -f .monetdb
