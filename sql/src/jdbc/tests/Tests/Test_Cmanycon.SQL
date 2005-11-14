#!/bin/sh

URL="jdbc:monetdb://${HOST}:${SQLPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

if [ -f $SQL_BUILD/src/jdbc/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_BUILD/src/jdbc/MonetDB_JDBC.jar:$CLASSPATH"`"
  elif [ -f $TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar:$CLASSPATH"`"
  elif [ -f $SQL_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar:$CLASSPATH"`"
  elif [ -f $MONETDB_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar ] ; then
	CLASSPATH="`$CYGPATH_WP "$MONETDB_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar:$CLASSPATH"`"
  else
	CLASSPATH="`$CYGPATH_WP "$TSTTRGBASE/share/MonetDB/lib/MonetDB_JDBC.jar:$CLASSPATH"`"
fi
if [ -f $SQL_BUILD/src/jdbc/tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_BUILD/src/jdbc/tests:$CLASSPATH"`"
  elif [ -f $TSTBLDBASE/src/jdbc/tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$TSTBLDBASE/src/jdbc/tests:$CLASSPATH"`"
  elif [ -f $SQL_PREFIX/share/sql/Tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$SQL_PREFIX/share/sql/Tests:$CLASSPATH"`"
  elif [ -f $MONETDB_PREFIX/share/sql/Tests/${TST}.class ] ; then
	CLASSPATH="`$CYGPATH_WP "$MONETDB_PREFIX/share/sql/Tests:$CLASSPATH"`"
  else
	CLASSPATH="`$CYGPATH_WP "$TSTTRGBASE/share/sql/Tests:$CLASSPATH"`"
fi
export CLASSPATH

Mlog -x "java ${TST} \"${URL}\""
