#!/bin/bash

pdd="`monetdb-java-config --pkgdatadir`"
JAR="$pdd/lib/jdbcclient.jar"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog -x "java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d $TSTDB -l xquery -f $TST.250.xq"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d $TSTDB -l xquery -f $TST.251.xq"

rm -f .monetdb
