#!/bin/sh

ARGS="-h${HOST} -p${SQLPORT} -umonetdb"

Mlog -x "java -jar $MONET_PREFIX/lib/MonetDB/java/MonetJDBC.jar ${ARGS} -f $RELSRCDIR/../JdbcClient_create_tables.sql < $RELSRCDIR/../JdbcClient_monetdb"
Mlog -x "java -jar $MONET_PREFIX/lib/MonetDB/java/MonetJDBC.jar ${ARGS} -f $RELSRCDIR/../JdbcClient_inserts_selects.sql < $RELSRCDIR/../JdbcClient_monetdb"
Mlog -x "java -jar $MONET_PREFIX/lib/MonetDB/java/MonetJDBC.jar ${ARGS} -D < $RELSRCDIR/../JdbcClient_monetdb"
