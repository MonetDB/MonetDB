#!/bin/sh

for i in $SQL_PREFIX/lib/sql/Tests $SQL_PREFIX/lib64/sql/Tests $TSTBLDBASE/src/odbc/samples ; do
	if [ -d "$i" ] ; then
		PATH="$i:$PATH"
	fi
done
export PATH
                                
Mlog -x odbcsample1
