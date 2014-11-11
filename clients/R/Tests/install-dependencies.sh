#!/bin/sh
R --vanilla --quiet -e 'dir.create(unlist(strsplit(Sys.getenv("R_LIBS_USER"), .Platform$path.sep))[1L], recursive = TRUE, showWarnings=F)'
R --vanilla --quiet -e "install.packages(c('dplyr','survey','sqlsurvey','DBI','digest','Lahman'),repos=c('http://download.r-forge.r-project.org','http://cran.rstudio.com/'),type='source')"
