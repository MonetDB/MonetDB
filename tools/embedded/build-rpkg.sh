#!/bin/sh
#set -x

STAGEDIR=/tmp/monetdb-embedded-stage
RPKG=MonetDBLite_1.0.0.tar.gz

rm -rf $STAGEDIR

hg archive $STAGEDIR/sourcetree
cd $STAGEDIR/sourcetree

# patch build system to only include things we need
echo "SUBDIRS = embedded" > tools/Makefile.ag
echo "SUBDIRS = mapilib" > clients/Makefile.ag
echo "SUBDIRS = mal modules optimizer scheduler tools" > monetdb5/Makefile.ag
echo "SUBDIRS = buildtools common clients gdk monetdb5 sql tools\nEXTRA_DIST = bootstrap configure configure.ac configure.ag libversions rpm.mk.in\nheaders_config = {\nDIR = includedir/monetdb\nHEADERS = h\nSOURCES = monetdb_config.h\n}\n" > Makefile.ag
sed -i "/^SUBDIRS = .*/d" sql/backends/Makefile.ag

./bootstrap
# we need this directory since sql/server depends on it
cd ..
mv sourcetree/tools/embedded/rpackage .
rsync -av --exclude-from sourcetree/tools/embedded/pkg-excludes sourcetree/ rpackage/src

# generate sql_parser.tab.c/h to remove our dependency on bison.
cd sourcetree
./configure
cd sql/server/
make sql_parser.tab.h
make sql_parser.tab.c
cd ../../../
cp sourcetree/sql/server/sql_parser.tab.* rpackage/src/sql/server/

# bundle pcre for windows (TODO: also iconv/zlib/ ...?)
# wget http://dev.monetdb.org/downloads/Windows/Libraries/libs-win64.zip
# 7z x libs-win64.zip
# cp -r pcre-8.37.win64 rpackage/src/tools/embedded/windows/

mkdir -p rpackage/src/monetdb5/extras/rapi
touch rpackage/src/monetdb5/extras/rapi/placeholder
R CMD build rpackage

#scp $RPKG cwi:WWW/R
scp $RPKG release@dev.monetdb.org:/var/www/html/Assets/R/

echo
echo 'install.packages("MonetDBLite", repos="http://homepages.cwi.nl/~hannes/R/", type="source")'
echo

# OSX 10.10 check (check vanilla!)
