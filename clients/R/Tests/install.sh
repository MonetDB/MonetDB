#!/bin/sh
# install local version
RLIBDIR=$TSTTRGDIR/rlibdir
mkdir -p $RLIBDIR
R CMD INSTALL $TSTSRCDIR/../MonetDB.R -l $RLIBDIR >> /dev/null 2>&1

# try loading the required packages
R --vanilla --quiet -e "library(MonetDB.R,lib.loc=\"$RLIBDIR\"); library(dplyr); library(sqlsurvey)" >> /dev/null 2>&1
RRET=$?


if [ $RRET -eq 0 ];
then
    echo "All right, packages installed"
else
	echo "Something went wrong"
fi