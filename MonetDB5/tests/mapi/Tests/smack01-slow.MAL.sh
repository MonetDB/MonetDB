#!/bin/sh

PATH="`monetdb-clients-config --pkglibdir`/Tests:$PATH"
export PATH

Mlog -x smack01 $MAPIPORT mal
