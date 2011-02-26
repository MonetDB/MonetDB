#!/bin/sh

#PERLLIB="`monetdb-clients-config --perllibdir`"
#export PERLLIB

Mlog -x "milsample.pl $MAPIPORT $TSTDB"
