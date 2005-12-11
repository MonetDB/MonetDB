#!/bin/sh

JAR="`find ${TSTBLDDIR}/../../ -name 'jdbcclient*.jar'`"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -f $RELSRCDIR/../JdbcClient_create_tables.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -f $RELSRCDIR/../JdbcClient_inserts_selects.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -d"

rm -f .monetdb
