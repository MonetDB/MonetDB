#!/bin/sh

eval "`perl -V:installvendorarch`"
eval "`perl -V:installprefix`"
inspfxlen=`echo $installprefix | wc -c`

prefix="`type -P sqlsample.pl`"
prefix="`dirname $prefix`"
prefix="`dirname $prefix`"

PERLLIB="$prefix`echo $installvendorarch | cut -c${inspfxlen}-`"
export PERLLIB

Mlog -x "sqlsample.pl $MAPIPORT $TSTDB"
