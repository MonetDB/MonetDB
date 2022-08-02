#!/bin/sh

Mlog -x "php -d include_path=$PHP_INCPATH -f $TSTSRCDIR/php_int128.php $MAPIPORT $TSTDB"
