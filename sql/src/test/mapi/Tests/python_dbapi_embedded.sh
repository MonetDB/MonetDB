#!/bin/sh

PATH="`monetdb-clients-config --pkglibdir`/Tests:$PATH"
export PATH

PYTHONPATH="`monetdb-config --pythonlibdir`:`monetdb-clients-config --pythonlibdir`:`monetdb-clients-config --pythonlibdir`/MonetDB:`monetdb-sql-config --pythonlibdir`:$PYTHONPATH"
export PYTHONPATH

if [ -n "$TST_FIVE" ] ; then
	Mlog -x "sqlsample.py $GDK_DBFARM $TSTDB 5"
else
	Mlog -x "sqlsample.py $GDK_DBFARM $TSTDB 4"
fi
