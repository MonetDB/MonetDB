#!/bin/sh

PATH="`monetdb-clients-config --pkglibdir`/Tests:$PATH"
export PATH

PYTHONPATH="`monetdb-clients-config --pythonlibdir`"
export PYTHONPATH

Mlog -x sqlsample.py $MAPIPORT $TSTDB
