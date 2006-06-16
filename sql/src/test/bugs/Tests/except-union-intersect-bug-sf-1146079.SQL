#!/bin/bash

# necessary for version number
source ${TSTSRCBASE}/src/jdbc/build.properties

JAR="${TSTBLDBASE}/src/jdbc/jdbcclient-${JDBC_MAJOR}.${JDBC_MINOR}.jar"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../except-union-intersect-bug-sf-1146079.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../except-union-intersect-bug-sf-1146079.sql 

rm -f .monetdb
