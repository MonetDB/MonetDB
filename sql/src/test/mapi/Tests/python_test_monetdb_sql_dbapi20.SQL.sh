#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/src/python/test/Makefile.ag
testpath="`monetdb-clients-config --pkglibdir`/Tests"
PYTHONPATH=$testpath:$PYTHONPATH
export PYTHONPATH

Mlog -x "python $testpath/dbapi20_monetdb.py"
