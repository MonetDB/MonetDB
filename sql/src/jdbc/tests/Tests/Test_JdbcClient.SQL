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
export JAR

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -f $RELSRCDIR/../JdbcClient_create_tables.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -f $RELSRCDIR/../JdbcClient_inserts_selects.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -d"

rm -f .monetdb
