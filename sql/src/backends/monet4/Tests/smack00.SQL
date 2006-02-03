#!/bin/sh

PATH=$MONETDB_BUILD/src/mapi/examples/C:$MONETDB_PREFIX/lib/MonetDB/Tests:$PATH ; export PATH

Mlog -x smack00 $MAPIPORT sql
