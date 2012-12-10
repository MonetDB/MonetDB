#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/python/test/Makefile.ag
testpath="$TSTSRCBASE/../clients/python3/test"
PYTHONPATH=$testpath:$PYTHONPATH
export PYTHONPATH

Mlog -x "${PYTHON3} $testpath/runtests.py"
