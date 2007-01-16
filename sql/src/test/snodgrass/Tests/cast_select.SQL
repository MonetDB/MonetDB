#!/bin/bash

pdd="`monetdb-clients-config --pkgdatadir`"
JAR="`ls $pdd/lib/jdbcclient-*.*.jar | head -n1`"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_ALL="`locale -a | grep -i en_us | grep -i utf | head -n 1`"
export LC_ALL

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../cast_select.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../cast_select.sql 2>&1

rm -f .monetdb
