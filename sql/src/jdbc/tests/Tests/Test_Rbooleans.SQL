#!/bin/sh

URL="jdbc:monetdb://${HOST}:${SQLPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

if [ -f $SQL_BUILD/src/jdbc/MonetJDBC.jar ] ; then
	CLASSPATH=$SQL_BUILD/src/jdbc/MonetJDBC.jar:$CLASSPATH
  elif [ -f $TSTBLDBASE/src/jdbc/MonetJDBC.jar ] ; then
	CLASSPATH=$TSTBLDBASE/src/jdbc/MonetJDBC.jar:$CLASSPATH
  elif [ -f $SQL_PREFIX/lib/MonetDB/java/MonetJDBC.jar ] ; then
	CLASSPATH=$SQL_PREFIX/lib/MonetDB/java/MonetJDBC.jar:$CLASSPATH
  elif [ -f $MONET_PREFIX/lib/MonetDB/java/MonetJDBC.jar ] ; then
	CLASSPATH=$MONET_PREFIX/lib/MonetDB/java/MonetJDBC.jar:$CLASSPATH
  else
	CLASSPATH=$TSTTRGBASE/lib/MonetDB/java/MonetJDBC.jar:$CLASSPATH
fi
if [ -f $SQL_BUILD/src/jdbc/tests/${TST}.class ] ; then
	CLASSPATH=$SQL_BUILD/src/jdbc/tests:$CLASSPATH
  elif [ -f $TSTBLDBASE/src/jdbc/tests/${TST}.class ] ; then
	CLASSPATH=$TSTBLDBASE/src/jdbc/tests:$CLASSPATH
  elif [ -f $SQL_PREFIX/lib/sql/Tests/${TST}.class ] ; then
	CLASSPATH=$SQL_PREFIX/lib/sql/Tests:$CLASSPATH
  elif [ -f $MONET_PREFIX/lib/sql/Tests/${TST}.class ] ; then
	CLASSPATH=$MONET_PREFIX/lib/sql/Tests:$CLASSPATH
  else
	CLASSPATH=$TSTTRGBASE/lib/sql/Tests:$CLASSPATH
fi
export CLASSPATH

Mlog -x "java ${TST} \"${URL}\""
