#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

java nl.cwi.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $TSTSRCBASE/$TSTDIR/Tests/ValidateSystemCatalogTables.sql

rm -f .monetdb
