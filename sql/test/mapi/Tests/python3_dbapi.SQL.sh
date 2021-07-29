#!/bin/sh

# must be aligned with the installation directory chosen in
# clients/examples/python
testpath="$TSTSRCBASE/clients/examples/python"

${PYTHON} ${testpath}/sqlsample.py $MAPIPORT $TSTDB $MAPIHOST
