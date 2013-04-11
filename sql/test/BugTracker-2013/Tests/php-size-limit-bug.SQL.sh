#!/bin/sh

Mlog -x "php -n -d include_path=$PHP_INCPATH -f $TSTSRCDIR/php-size-limit-bug.php $MAPIPORT $TSTDB"
