#!/bin/bash

pdd="`monetdb-clients-config --pkgdatadir`"
JAR="`ls $pdd/lib/jdbcclient-*.*.jar | head -n1`"

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
