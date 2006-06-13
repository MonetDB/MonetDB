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

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -f $RELSRCDIR/drop_schema_crash.SF-1504794-data.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -f $RELSRCDIR/drop_schema_crash.SF-1504794-data.sql
