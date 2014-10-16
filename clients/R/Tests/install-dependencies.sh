#!/bin/sh
R --vanilla --quiet -e "install.packages(c('dplyr','sqlsurvey','DBI','digest'),repos=c('http://r-forge.r-project.org','http://cran.rstudio.com/'),type='source')"
