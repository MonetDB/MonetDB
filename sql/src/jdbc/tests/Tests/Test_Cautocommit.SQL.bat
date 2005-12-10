@echo off

URL="jdbc:monetdb://${HOST}:${SQLPORT}/database?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"

call Mlog.bat -x "ant -e -buildfile ${TSTSRCDIR}/../build.xml -Dbuilddir=${PWD} -Djdbc_url=\"${URL}\" -Djardir=${TSTBLDDIR}/../../ ${TST}"
