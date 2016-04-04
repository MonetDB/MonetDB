#!/bin/sh
if [ -z $STAGEDIR ]; then
	STAGEDIR=/tmp/monetdb-embedded-stage
fi

RPKG=MonetDBLite_0.2.2.tar.gz
OSXPKG=MonetDBLite_0.2.2.tgz
WINPKG=MonetDBLite_0.2.2.zip

rm -rf $STAGEDIR

hg archive $STAGEDIR/sourcetree

# generate mal/sql scripts and sql_parser.tab.c/h to remove our dependency on bison.
rm -f ../../sql/server/sql_parser.tab.*
cd ../../sql/server/
make sql_parser.tab.c 
make sql_parser.tab.h
cp sql_parser.tab.c sql_parser.tab.h $STAGEDIR/sourcetree/sql/server/
cd $STAGEDIR/sourcetree

# patch build system to only include things we need
printf "SUBDIRS = embedded" > tools/Makefile.ag
printf "SUBDIRS = mapilib" > clients/Makefile.ag
printf "SUBDIRS = mal modules optimizer scheduler tools" > monetdb5/Makefile.ag
printf "SUBDIRS = buildtools common clients gdk monetdb5 sql tools\nEXTRA_DIST = configure configure.ac configure.ag libversions rpm.mk.in\nheaders_config = {\nDIR = includedir/monetdb\nHEADERS = h\nSOURCES = monetdb_config.h\n}\n" > Makefile.ag
sed -i -e '/^SUBDIRS = .*$/d' sql/backends/monet5/Makefile.ag
sed -i -e 's/sql_parser\.y/sql_parser.tab.h sql_parser.tab.c/' sql/server/Makefile.ag

./bootstrap
# buildtools/conf/lt~obsolete.m4 file name violates CRAN policies, rename and replace references
mv "buildtools/conf/lt~obsolete.m4" buildtools/conf/lt-obsolete.m4
find . -name "Makefile.in" -exec sed -i -e "s/lt~obsolete\.m4/lt-obsolete\.m4/g" {} \;

cd ..
mv sourcetree/tools/embedded/rpackage .
rsync -av --exclude-from sourcetree/tools/embedded/pkg-excludes sourcetree/ rpackage/src
cp sourcetree/NT/monetdb_config.h.in rpackage/src/tools/embedded/windows/
 
export R_INCLUDE_DIR=`R CMD config --cppflags | sed s/^-I//`

# # inline mal/sql scripts
# python sourcetree/tools/embedded/inline.py dummytarget/libs/monetdb5/ rpackage/src/monetdb5/mal/mal_init_inline.h

mkdir -p rpackage/src/monetdb5/extras/rapi
touch rpackage/src/monetdb5/extras/rapi/placeholder

R CMD build rpackage

read -p "Install and test? [y]" -r
echo    # (optional) move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

export MAKEFLAGS="-j"

R CMD INSTALL --build $STAGEDIR/$RPKG && \
R CMD INSTALL $STAGEDIR/$OSXPKG && \
R -f $STAGEDIR/sourcetree/tools/embedded/Tests/lowlevel.R && \
R -f $STAGEDIR/sourcetree/tools/embedded/Tests/dbi.R

read -p "Upload? [y]" -r
echo    # (optional) move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

scp $STAGEDIR/$RPKG release@dev.monetdb.org:/var/www/html/Assets/R/
scp $STAGEDIR/$OSXPKG release@dev.monetdb.org:/var/www/html/Assets/R/

# OSX 10.10/10.11 needs pkg-config!
