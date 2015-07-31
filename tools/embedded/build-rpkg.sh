#!/bin/sh
set -x

STAGEDIR=/tmp/monetdb-embedded-stage
RPKG=MonetDB_1.0.0.tar.gz

rm -rf $TARBALL
rm -rf $STAGEDIR

hg archive $STAGEDIR/sourcetree

cd $STAGEDIR/sourcetree

./bootstrap

#tar --exclude-from=tools/embedded/tar-excludes -zcvf $TARBALL .

cd ..
mv sourcetree/tools/reverserapi .
rsync -av --exclude-from sourcetree/tools/embedded/pkg-excludes sourcetree/ reverserapi/src

R CMD build reverserapi
scp $RPKG lyon.ins.cwi.nl:/cwi/www/homepages/hannes/WWW/R

# install.packages("MonetDB", repos="http://homepages.cwi.nl/~hannes/R/", type="source")

# OSX 10.10 check (check vanilla!)
# ubuntu 14.04.02: needs bison libssl-dev libxml2-dev
# fedora 22: bison openssl-devel libxml2-devel
