#!/bin/sh

export PATH=$MONET_BUILD/src/mapi/examples/C:$MONET_PREFIX/lib/MonetDB/Tests:$PATH

Mlog -x sample3 $HOST $SQLPORT sql
