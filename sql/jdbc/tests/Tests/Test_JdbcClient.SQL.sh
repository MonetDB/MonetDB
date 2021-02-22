#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB --help

java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_create_tables.sql
java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_inserts_selects.sql
java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -D
java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_drop_tables.sql

rm -f .monetdb
