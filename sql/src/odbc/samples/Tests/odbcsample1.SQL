#!/bin/sh

PATH=$TSTBLDBASE/src/odbc/samples:$MONETDB_PREFIX/lib/sql/Tests:$PATH ; export PATH

Mlog -x odbcsample1
