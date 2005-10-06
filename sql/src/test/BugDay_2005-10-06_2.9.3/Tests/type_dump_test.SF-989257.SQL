#!/bin/sh

if [ -f $SQL_BUILD/src/jdbc/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$SQL_BUILD/src/jdbc/MonetDB_JDBC.jar"`"
  elif [ -f $TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$TSTBLDBASE/src/jdbc/MonetDB_JDBC.jar"`"
  elif [ -f $SQL_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$SQL_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar"`"
  elif [ -f $MONETDB_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar ] ; then
	JAR="`$CYGPATH_WP "$MONETDB_PREFIX/share/MonetDB/lib/MonetDB_JDBC.jar"`"
  else
	JAR="`$CYGPATH_WP "$TSTTRGBASE/share/MonetDB/lib/MonetDB_JDBC.jar"`"
fi

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LANG="en_US.UTF-8"
export LANG

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $SQLPORT -f $RELSRCDIR/type_dump_test.SF-989257-src.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $SQLPORT -f $RELSRCDIR/type_dump_test.SF-989257-src.sql 

rm -f .monetdb
