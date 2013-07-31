#!/bin/sh

Mlog -x "php -d include_path=$PHP_INCPATH -f $TSTSRCBASE/clients/examples/php/sqlsample.php $MAPIPORT $TSTDB"
