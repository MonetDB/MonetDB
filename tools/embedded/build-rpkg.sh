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

# generate sql_parser.tab.c/h to remove our dependency on bison on Windows.
cd sourcetree
./configure
make sql/server/sql_parser.tab.h
make sql/server/sql_parser.tab.c
cd ..
cpsh
cp sourcetree/sql/server/sql_parser.tab.* rpackage/src/tools/embedded/windows/

# bundle pcre for windows (TODO: also iconv/zlib/ ...?)
wget http://dev.monetdb.org/downloads/Windows/Libraries/libs-win64.zip
umask 666
unzip libs-win64.zip
umask 644
cp -r pcre-8.37.win64 rpackage/src/tools/embedded/windows/

mkdir -p rpackage/src/monetdb5/extras/rapi
touch rpackage/src/monetdb5/extras/rapi/placeholder
R CMD build rpackage

scp $RPKG lyon.ins.cwi.nl:/cwi/www/homepages/hannes/WWW/R

# install.packages("MonetDB", repos="http://homepages.cwi.nl/~hannes/R/", type="source")

# OSX 10.10 check (check vanilla!)
# ubuntu 14.04.02: needs bison libssl-dev libxml2-dev
# fedora 22: bison openssl-devel libxml2-devel
