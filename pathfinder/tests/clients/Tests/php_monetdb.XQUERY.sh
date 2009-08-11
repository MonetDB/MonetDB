#!/bin/sh

path="`monetdb-clients-config --pkglibdir`/Tests"
dir="`monetdb-clients-config --phpextensiondir`"

Mlog -x "php -n -d extension_dir=$dir -f $path/xquerysample.php $MAPIPORT"
