#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/python/test/Makefile.ag
testpath="$TSTSRCDIR"

${PYTHON} $testpath/python_dec38.py $MAPIPORT $TSTDB $MAPIHOST
