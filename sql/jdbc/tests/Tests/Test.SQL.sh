#!/bin/sh

URL="jdbc:monetdb://${HOST}:${MAPIPORT}/${TSTDB}?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"
TST=$1

java ${TST} "${URL}"
