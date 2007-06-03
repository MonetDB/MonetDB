#!/bin/sh

pdd="`monetdb-clients-config --pkgdatadir`"
JAR="`ls $pdd/lib/monetdb-*.*-jdbc.jar | head -n1`"
URL="jdbc:monetdb://${HOST}:${MAPIPORT}/${TSTDB}?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

Mlog -x "java -classpath $JAR:$pdd/Tests ${TST} \"${URL}\""
