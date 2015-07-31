#!/bin/sh
set -x

TARBALL=/tmp/embedded.tgz
STAGEDIR=/tmp/monetdb-embedded-tarball

rm -rf $TARBALL
rm -rf $STAGEDIR
hg archive $STAGEDIR
cd $STAGEDIR

./bootstrap
tar --exclude-from=tools/embedded/tar-excludes -zcvf $TARBALL .

scp $TARBALL lyon.ins.cwi.nl:/cwi/www/homepages/hannes/WWW/R

