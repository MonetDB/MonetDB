#!/bin/sh

PATH=$MONETDB_BUILD/src/mapi/examples/C:$MONETDB_PREFIX/lib/MonetDB/Tests:$PATH ; export PATH

Mlog -x sample4 $HOST $MAPIPORT sql
