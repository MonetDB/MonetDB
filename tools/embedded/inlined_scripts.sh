#!/bin/sh

PREFIX=/tmp/monetdb-embedded-dummytarget
rm -r $PREFIX

cd ../../
./configure --prefix=$PREFIX --enable-embedded \
--disable-fits --disable-geom --disable-rintegration --disable-gsl --disable-netcdf \
--disable-jdbc --disable-merocontrol --disable-odbc --disable-console --disable-microhttpd \
--without-perl --without-python2 --without-python3 --without-rubygem --without-unixodbc \
--without-samtools --without-sphinxclient --without-geos --without-samtools --without-readline \
--enable-optimize --enable-silent-rules --disable-assert --enable-strict --disable-int128

make -j clean install

python tools/embedded/inlined_scripts.py $PREFIX/lib/monetdb5/ .

cd tools/embedded
