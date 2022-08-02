#!/bin/sh

URL="jdbc:monetdb://${HOST}:${MAPIPORT}/${TSTDB}?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

Mlog -x "java ${TST} \"${URL}\""
