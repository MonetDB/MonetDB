#!/bin/sh

URL="jdbc:monetdb://${HOST}:${SQLPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

if [ -f $SQL_BUILD/src/jdbc/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_BUILD/src/jdbc/MonetDB_JDBC.jar:$CLASSPATH"`"
  elif [ -f $TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar:$CLASSPATH"`"
  elif [ -f $SQL_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar:$CLASSPATH"`"
  elif [ -f $MONETDB_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$MONETDB_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar:$CLASSPATH"`"
  else
	CLASSPATH="`$CYGPATH_WP "$TSTTRGBASE/lib/MonetDB/java/MonetDB_JDBC.jar:$CLASSPATH"`"
fi
if [ -f $SQL_BUILD/src/jdbc/tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_BUILD/src/jdbc/tests:$CLASSPATH"`"
  elif [ -f $TSTBLDBASE/src/jdbc/tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$TSTBLDBASE/src/jdbc/tests:$CLASSPATH"`"
  elif [ -f $SQL_PREFIX/lib/sql/Tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_PREFIX/lib/sql/Tests:$CLASSPATH"`"
  elif [ -f $MONETDB_PREFIX/lib/sql/Tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$MONETDB_PREFIX/lib/sql/Tests:$CLASSPATH"`"
  else
	CLASSPATH="`$CYGPATH_WP "$TSTTRGBASE/lib/sql/Tests:$CLASSPATH"`"
fi
export CLASSPATH

Mlog -x "java ${TST} \"${URL}\""
