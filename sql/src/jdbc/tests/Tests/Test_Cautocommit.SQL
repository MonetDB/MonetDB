#!/bin/sh

URL="jdbc:monetdb://${HOST}:${SQLPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

Mlog -x "java -classpath $MONET_PREFIX/lib/sql/Tests:$MONET_PREFIX/lib/MonetDB/java/MonetJDBC.jar Test_Cautocommit \"${URL}\""
