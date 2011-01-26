#!/bin/sh

Mlog -x "php -n -d include_path=$PHP_INCPATH -f $BINDIR/sqlsample.php $MAPIPORT $TSTDB"
