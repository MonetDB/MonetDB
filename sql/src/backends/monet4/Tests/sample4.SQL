#!/bin/sh

PATH=$MONET_BUILD/src/mapi/examples/C:$MONET_PREFIX/lib/MonetDB/Tests:$PATH ; export PATH

Mlog -x sample4 $HOST $SQLPORT sql
