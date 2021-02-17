#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog -x "java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB --help"

Mlog -x "java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_create_tables.sql"
Mlog -x "java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_inserts_selects.sql"
Mlog -x "java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -D"
Mlog -x "java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_drop_tables.sql"

rm -f .monetdb
