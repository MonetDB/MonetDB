#!/bin/sh
# install local version
RLIBDIR=$TSTTRGDIR/rlibdir
mkdir -p $RLIBDIR
out1=$(R CMD INSTALL $TSTSRCDIR/../MonetDB.R -l $RLIBDIR 2>&1)

# try loading the required packages
out2=$(R --vanilla --quiet -e "library(MonetDB.R,lib.loc=\"$RLIBDIR\"); library(dplyr); library(sqlsurvey)" 2>&1)
RRET=$?


if [ $RRET -eq 0 ];
then
    echo "All right, packages installed"
else
    echo "Something went wrong"
    echo "$out1"
    echo
    echo "$out2"
fi
