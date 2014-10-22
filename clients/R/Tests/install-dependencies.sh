#!/bin/sh
R --vanilla --quiet -e "install.packages(c('dplyr','survey','sqlsurvey','DBI','digest','Lahman'),repos=c('http://download.r-forge.r-project.org','http://cran.rstudio.com/'),type='source')"
