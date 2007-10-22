#!/bin/sh

PATH="`monetdb-clients-config --pkglibdir`/Tests:$PATH"
export PATH

PYTHONPATH="`monetdb-clients-config --pythonlibdir`"
export PYTHONPATH

if [ -n "$TST_FIVE" ] ; then
	Mlog -x sqlsample.py $GDK_DBFARM $TSTDB 5
else
	Mlog -x sqlsample.py $GDK_DBFARM $TSTDB 4
fi
