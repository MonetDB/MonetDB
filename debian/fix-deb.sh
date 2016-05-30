#!/bin/bash
if [ -f /etc/lsb-release ]; then
    source /etc/lsb-release
    ID=${DISTRIB_ID,,}
    VERSION_ID=$DISTRIB_RELEASE
elif [ -f /etc/os-release ]; then
    source /etc/os-release
else
    echo "don't know what distro this is"
    exit 1
fi

if [[ ( $ID == debian && $VERSION_ID -lt 8 ) || ( $ID == ubuntu && $VERSION_ID < 15.10 ) ]]; then
    # fix control file because these systems don't have liblas and a
    # too old version of libgeos
    sed -i 's/, libgeos-dev[^,]*//;s/, liblas-c-dev[^,]*//' debian/control
    sed -i '/^Package:.*lidar/,/^$/d' debian/control
    sed -i '/^Package:.*geom/,/^$/d' debian/control
    rm debian/libmonetdb5-server-lidar.install debian/libmonetdb5-server-geom.install
    sed -i '/geo[ms]=yes/s/yes/no/;/gdal=yes/s/yes/no/;/lidar=yes/s/yes/no/;/liblas=yes/s/yes/no/' debian/rules
fi
