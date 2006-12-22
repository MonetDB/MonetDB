@echo off

call Mlog.bat -x "ant -emacs -buildfile ${TSTSRCDIR}/../build.xml -Dbuilddir=${TSTBLDDIR}/../ -Dhost=\"${HOST}\" -Dport=${MAPIPORT} -Dmapi_jar=${TSTBLDDIR}/../../ ${TST}"
