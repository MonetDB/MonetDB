#!/bin/sh

URL="jdbc:monetdb://${HOST}:${MAPIPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

Mlog -x "ant -emacs -buildfile ${TSTSRCDIR}/../build.xml -Dbuilddir=${TSTBLDDIR}/../ -Djdbc_url=\"${URL}\" -Djardir=${TSTBLDDIR}/../../ ${TST}"
