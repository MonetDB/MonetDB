#!/bin/sh

path="`type -P sqlsample.php`"
dir="`dirname $path`"
dir="`dirname $dir`"
dir="$dir/share/php"

Mlog -x "php -n -d include_path=$dir -f $path $MAPIPORT $TSTDB"
