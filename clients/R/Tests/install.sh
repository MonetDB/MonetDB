#!/bin/sh
# install dependencies first
# R --vanilla --quiet -e "install.packages(c('MonetDB.R','dplyr','sqlsurvey'),repos=c('http://r-forge.r-project.org','http://cran.rstudio.com/'),type='source')"

# install local version
R CMD INSTALL $TSTSRCDIR/../MonetDB.R >> /dev/null 2>&1

# try loading the required packages
R --vanilla --quiet -e "library(MonetDB.R); library(dplyr); library(sqlsurvey)" >> /dev/null 2>&1
RRET=$?


if [ $RRET -eq 0 ];
then
    echo "All right, packages installed"
else
	echo "Something went wrong"
fi