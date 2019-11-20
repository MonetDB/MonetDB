#!/bin/sh

python3 $TSTSRCBASE/$TSTDIR/sqllogictest.py --host=localhost --port=$MAPIPORT --database=$TSTDB $TSTSRCBASE/$TSTDIR/select2.test
