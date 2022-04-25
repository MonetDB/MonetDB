#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/python/test/Makefile.ag
testpath="$TSTSRCDIR"

${PYTHON} $testpath/python_int128.py $MAPIPORT $TSTDB $MAPIHOST
