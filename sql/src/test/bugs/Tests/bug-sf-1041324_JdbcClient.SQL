#!/bin/sh

if [ -f $SQL_BUILD/src/jdbc/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$SQL_BUILD/src/jdbc/MonetDB_JDBC.jar"`"
  elif [ -f $TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar"`"
  elif [ -f $SQL_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$SQL_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar"`"
  elif [ -f $MONETDB_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$MONETDB_PREFIX/lib/MonetDB/java/MonetDB_JDBC.jar"`"
  else
	JAR="`$CYGPATH_WP "$TSTTRGBASE/lib/MonetDB/java/MonetDB_JDBC.jar"`"
fi

# JdcsClient is not really made for non-interactive usage ... ;-)
Mlog   "\{ echo monetdb \; cat $RELSRCDIR/../bug-sf-1041324_weird-char.sql \; \} \| $MTIMEOUT java -jar \"$JAR\" -p $SQLPORT -u monetdb"
	 { echo monetdb  ; cat $RELSRCDIR/../bug-sf-1041324_weird-char.sql  ;  }  | $MTIMEOUT java -jar  "$JAR"  -p $SQLPORT -u monetdb

