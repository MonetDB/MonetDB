#!/bin/bash

pdd="`monetdb-clients-config --pkgdatadir`"
JAR="`ls $pdd/lib/jdbcclient-*.*.jar | head -n1`"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog -x "java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d $TSTDB -l xquery -f $TST.250.xq"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d $TSTDB -l xquery -f $TST.251.xq"

rm -f .monetdb
