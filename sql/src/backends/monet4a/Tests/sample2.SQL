#!/usr/bin/env bash

export PATH=$MONET_BUILD/src/mapi/examples/C:$MONET_PREFIX/lib/MonetDB/Tests:$PATH

Mlog -x sample2 $HOST $SQLPORT sql
