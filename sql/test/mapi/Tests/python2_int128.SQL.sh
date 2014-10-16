#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/python/test/Makefile.ag
testpath="$TSTSRCDIR"
# ignore PYTHONPATH from Mtest, it is hardcoded to the dirs for the
# Python that runs Mtest (currently always Python 2)
PYTHONPATH=$testpath:${PYTHON2PATH}
export PYTHONPATH

Mlog -x "${PYTHON2} $testpath/python_int128.py $MAPIPORT $TSTDB $MAPIHOST"
