#!/bin/sh

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog -x "java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $TSTSRCBASE/$TSTDIR/Tests/ValidateSystemCatalogTables.sql"

rm -f .monetdb
