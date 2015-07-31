#!/bin/sh
set -x

TARBALL=/tmp/embedded.tgz
STAGEDIR=/tmp/monetdb-embedded-tarball
RPKG=MonetDB_1.0.0.tar.gz

rm -rf $TARBALL
rm -rf $STAGEDIR

hg archive $STAGEDIR

cd $STAGEDIR

./bootstrap

tar --exclude-from=tools/embedded/tar-excludes -zcvf $TARBALL .

scp $TARBALL lyon.ins.cwi.nl:/cwi/www/homepages/hannes/WWW/R

R CMD build tools/reverserapi 
scp $RPKG lyon.ins.cwi.nl:/cwi/www/homepages/hannes/WWW/R

rm -rf $TARBALL
rm -rf $STAGEDIR

# install.packages("MonetDB", repos="http://homepages.cwi.nl/~hannes/R/", type="source")

# OSX 10.10 check (check vanilla!)
# ubuntu 14.04.02: needs bison libssl-dev libxml2-dev
# fedora 22: bison openssl-devel libxml2-devel
