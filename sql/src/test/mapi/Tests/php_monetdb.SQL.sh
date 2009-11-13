#!/bin/sh

path="`monetdb-clients-config --pkglibdir`/Tests"
dir="`monetdb-clients-config --datadir`/php"

Mlog -x "php -n -d include_path=$dir -f $path/sqlsample.php $MAPIPORT $TSTDB"
