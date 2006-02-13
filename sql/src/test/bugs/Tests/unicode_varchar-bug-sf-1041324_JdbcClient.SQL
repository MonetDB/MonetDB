#!/bin/sh

# necessary for version number
source ${TSTSRCBASE}/src/jdbc/build.properties

JAR="${TSTBLDBASE}/src/jdbc/jdbcclient-${JDBC_MAJOR}.${JDBC_MINOR}.jar"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -b${TSTDB} -f $RELSRCDIR/../bug-sf-1041324_weird-char.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -b${TSTDB} -f $RELSRCDIR/../unicode_varchar-bug-sf-1041324.sql 

rm -f .monetdb
