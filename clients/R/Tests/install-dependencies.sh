#!/bin/sh
R --vanilla --quiet -e "install.packages(c('MonetDB.R','dplyr','sqlsurvey'),repos=c('http://r-forge.r-project.org','http://cran.rstudio.com/'),type='source')"
