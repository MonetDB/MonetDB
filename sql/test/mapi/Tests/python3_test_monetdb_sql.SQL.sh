#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/python/test/Makefile.ag
testpath="$TSTSRCBASE/clients/python3/test"
# ignore PYTHONPATH from Mtest, it is hardcoded to the dirs for the
# Python that runs Mtest (currently always Python 2)
PYTHONPATH=$testpath:${PYTHON3PATH}
export PYTHONPATH

Mlog -x "${PYTHON3} $testpath/runtests.py"
