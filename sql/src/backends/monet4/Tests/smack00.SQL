#!/bin/sh

PATH=$MONET_BUILD/src/mapi/examples/C:$MONET_PREFIX/lib/MonetDB/Tests:$PATH ; export PATH

Mlog -x smack00 $SQLPORT sql
