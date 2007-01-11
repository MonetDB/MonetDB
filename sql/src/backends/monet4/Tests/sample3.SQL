#!/bin/sh

PATH="`monetdb-clients-config --pkglibdir`/Tests:$PATH"
export PATH

Mlog -x sample3 $HOST $MAPIPORT sql
