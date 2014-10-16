#!/bin/sh

Mlog -x "php -d include_path=$PHP_INCPATH -f $TSTSRCDIR/php_int64_dec18.php $MAPIPORT $TSTDB"
