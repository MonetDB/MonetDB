#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/python/test/Makefile.ag
testpath="$TSTSRCBASE/../clients/python2/test"
PYTHONPATH=$testpath:$PYTHONPATH
export PYTHONPATH

Mlog -x "${PYTHON2} $testpath/runtests.py"
