#!/bin/sh

URL="jdbc:monetdb://${HOST}:${SQLPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

if [ -f $SQL_BUILD/src/jdbc/MonetJDBC.jar ] ; then
	CLASSPATH=$SQL_BUILD/src/jdbc/MonetJDBC.jar:$CLASSPATH ; export CLASSPATH
  else
	CLASSPATH=$SQL_PREFIX/lib/MonetDB/java/MonetJDBC.jar:$CLASSPATH ; export CLASSPATH
fi
if [ -f $SQL_BUILD/src/jdbc/tests/${TST}.class ] ; then
	CLASSPATH=$SQL_BUILD/src/jdbc/tests:$CLASSPATH ; export CLASSPATH
  else
	CLASSPATH=$SQL_PREFIX/lib/sql/Tests:$CLASSPATH ; export CLASSPATH
fi

Mlog -x "java ${TST} \"${URL}\""
