#!/bin/sh

ARGS="-h${HOST} -p${SQLPORT} -umonetdb"

if [ -f $SQL_BUILD/src/jdbc/MonetJDBC.jar ] ; then
	MonetJDBC=$SQL_BUILD/src/jdbc/MonetJDBC.jar
  else
	MonetJDBC=$SQL_PREFIX/lib/MonetDB/java/MonetJDBC.jar
fi

Mlog -x "java -jar ${MonetJDBC} ${ARGS} -f $RELSRCDIR/../JdbcClient_create_tables.sql < $RELSRCDIR/../JdbcClient_monetdb"
Mlog -x "java -jar ${MonetJDBC} ${ARGS} -f $RELSRCDIR/../JdbcClient_inserts_selects.sql < $RELSRCDIR/../JdbcClient_monetdb"
Mlog -x "java -jar ${MonetJDBC} ${ARGS} -D < $RELSRCDIR/../JdbcClient_monetdb"
