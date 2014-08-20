#!/bin/sh
# install dependencies first
# R --vanilla --quiet -e "install.packages(c('MonetDB.R','dplyr','sqlsurvey'),repos=c('http://r-forge.r-project.org','http://cran.rstudio.com/'),type='source')"

echo "#~BeginVariableOutput~#"
>&2 echo "#~BeginVariableOutput~#"

# install local version
R CMD INSTALL $TSTSRCDIR/../MonetDB.R

# try loading the required packages
R --vanilla --quiet -e "library(MonetDB.R); library(dplyr); library(sqlsurvey)"
RRET=$?

echo "#~EndVariableOutput~#"
>&2 echo "#~EndVariableOutput~#"

if [ $RRET -eq 0 ];
then
    echo "All right, packages installed"
else
	echo "Something went wrong"
fi