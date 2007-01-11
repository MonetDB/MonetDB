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

Mlog -x "java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d $TSTDB -f $RELSRCDIR/../JdbcClient_create_tables.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d $TSTDB -f $RELSRCDIR/../JdbcClient_inserts_selects.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d $TSTDB -D"

rm -f .monetdb
