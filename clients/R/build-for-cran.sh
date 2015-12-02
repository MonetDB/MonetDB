#!/bin/bash

# set this to a path where the latest svn version or R was installed. 
# CRAN policies state that R CMD check must use the latest version
R=~/r-trunk-install/R.framework/Resources/bin/R

# can't have these in a R package, it wont build
find MonetDB.R -name "Makefile*" -exec rm {} \;
find MonetDB.R -name ".DS_Store" -exec rm {} \;

# for now, no profiler
rm MonetDB.R/src/profiler*
rm MonetDB.R/src/Makevars.win

# binary leftovers
rm MonetDB.R/src/*.o
rm MonetDB.R/src/*.so

# remove windows artefacts
rm -r MonetDB.R/src-*

rm MonetDB.R_*.tar.gz
mkdir -p /tmp/rcheck

$R CMD build MonetDB.R
$R CMD check -o /tmp/rcheck --as-cran MonetDB.R_*.tar.gz

#hgid=`hg id -i | sed s/+//`
#newname=`basename MonetDB.R_*.tar.gz .tar.gz`-$hgid.tar.gz
#mv MonetDB.R_*.tar.gz $newname
