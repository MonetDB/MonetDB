#!/bin/sh

URL="jdbc:monetdb://${HOST}:${SQLPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

Mlog -x "ant -e -buildfile ${TSTSRCDIR}/../build.xml -Dbuilddir=${TSTBLDDIR}/../ -Djdbc_url=\"${URL}\" -Djardir=${TSTBLDDIR}/../../ ${TST}"
