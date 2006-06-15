#!/bin/bash

# necessary for version number
source ${TSTSRCBASE}/src/jdbc/build.properties

JAR="${TSTBLDBASE}/src/jdbc/jdbcclient-${JDBC_MAJOR}.${JDBC_MINOR}.jar"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -f $RELSRCDIR/authorization.SF-1430616-data.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -f $RELSRCDIR/authorization.SF-1430616-data.sql 

cat << EOF > .monetdb
user=voc
password=voc
EOF

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -q" 
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -q

rm -f .monetdb
