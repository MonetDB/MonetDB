#!/bin/sh

PREFIX=/tmp/monetdb-embedded-dummytarget
rm -r $PREFIX
mkdir -p $PREFIX

cd ../../
./configure --prefix=$PREFIX \
--disable-fits --disable-geom --disable-rintegration --disable-gsl --disable-netcdf \
--disable-jdbc --disable-merocontrol --disable-odbc --disable-console --disable-microhttpd \
--without-perl --without-python2 --without-python3 --without-unixodbc --disable-mapi \
--without-samtools --without-sphinxclient --without-geos --without-samtools --without-readline \
--enable-debug --enable-silent-rules --disable-assert --disable-strict --disable-int128 --without-openssl

make -j clean install

python tools/embedded/inlined_scripts.py $PREFIX/lib/monetdb5/ .

cd tools/embedded
