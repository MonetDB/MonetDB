#!/bin/sh

PATH="`monetdb-clients-config --pkglibdir`/Tests:$PATH"
export PATH

PYTHONPATH="`monetdb-config --pythonlibdir`:`monetdb-clients-config --pythonlibdir`:`monetdb-clients-config --pythonlibdir`/MonetDB:$PYTHONPATH"
export PYTHONPATH

if [ -n "$TST_FIVE" ] ; then
	PYTHONPATH="$TSTBLDBASE/src/backends/monet5/:$PYTHONPATH"
	PYTHONPATH="$TSTBLDBASE/src/backends/monet5/.libs:$PYTHONPATH"
	export PYTHONPATH
	Mlog -x "sqlsample.py $GDK_DBFARM $TSTDB 5"
else
	PYTHONPATH="$TSTBLDBASE/src/backends/monet4/:$PYTHONPATH"
	PYTHONPATH="$TSTBLDBASE/src/backends/monet4/.libs:$PYTHONPATH"
	export PYTHONPATH
	Mlog -x "sqlsample.py $GDK_DBFARM $TSTDB 4"
fi
