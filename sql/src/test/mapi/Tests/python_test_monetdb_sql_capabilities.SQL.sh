#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/src/python/test/Makefile.ag
testpath="`monetdb-clients-config --pkglibdir`/Tests"
export PYTHONPATH=$testpath:$PYTHONPATH

Mlog -x "python $testpath/capabilities_monetdb.py"
