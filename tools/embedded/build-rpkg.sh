#!/bin/sh
set -x

STAGEDIR=/tmp/monetdb-embedded-stage
RPKG=MonetDB_1.0.0.tar.gz

rm -rf $STAGEDIR

hg archive $STAGEDIR/sourcetree
cd $STAGEDIR/sourcetree

# patch build system to only include things we need
echo "SUBDIRS = embedded" > tools/Makefile.ag
echo "SUBDIRS = mapilib" > clients/Makefile.ag
echo "SUBDIRS = mal modules optimizer scheduler tools" > monetdb5/Makefile.ag
echo "SUBDIRS = buildtools common clients gdk monetdb5 sql tools\nEXTRA_DIST = bootstrap configure configure.ac configure.ag libversions rpm.mk.in\nheaders_config = {\nDIR = includedir/monetdb\nHEADERS = h\nSOURCES = monetdb_config.h\n}\n" > Makefile.ag

./bootstrap
# we need this directory since sql/server depends on it

cd ..
mv sourcetree/tools/embedded/rpackage .
rsync -av --exclude-from sourcetree/tools/embedded/pkg-excludes sourcetree/ rpackage/src
mkdir -p rpackage/src/monetdb5/extras/rapi
touch rpackage/src/monetdb5/extras/rapi/placeholder
R CMD build rpackage

scp $RPKG lyon.ins.cwi.nl:/cwi/www/homepages/hannes/WWW/R

# install.packages("MonetDB", repos="http://homepages.cwi.nl/~hannes/R/", type="source")

# OSX 10.10 check (check vanilla!)
# ubuntu 14.04.02: needs bison libssl-dev libxml2-dev
# fedora 22: bison openssl-devel libxml2-devel
