#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/python/test/Makefile.ag
testpath="$TSTSRCBASE/../clients/python/test"
PYTHONPATH=$testpath:$PYTHONPATH
export PYTHONPATH

Mlog -x "python $testpath/runtests.py"
