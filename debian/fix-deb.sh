#!/bin/bash
case $# in
0)
    # figure out which distribution this is
    if [ -f /etc/lsb-release ]; then
	source /etc/lsb-release
	SUITE=$DISTRIB_CODENAME
    elif [ -f /etc/os-release ]; then
	source /etc/os-release
	if [[ $ID == debian ]]; then
	    SUITE="${VERSION#*(}"
	    SUITE="${SUITE%)*}"
	else
	    case $VERSION in
	    *\(*\)*)
		SUITE="${VERSION#*(}"
		SUITE="${SUITE% *)*}"
		;;
	    *)
		SUITE="${VERSION#*, }"
		SUITE="${SUITE% *)*}"
		;;
	    esac
	    SUITE=${SUITE,,}
	fi
    else
	echo "don't know what distro this is"
	exit 1
    fi
    ARCH=$(arch)
    ;;
1)
    # we're passed the name of the distribution
    SUITE=$1
    ARCH=$(arch)
    ;;
2)
    SUITE=$1
    ARCH=$2
    ;;
esac

if [[ $ARCH == i?86 ]]; then
    ARCH=i386			# normalize
fi

case $SUITE in
trusty)
    # fix control file because this system doesn't have liblas and a
    # too old version of libgeos
    sed -i -e 's/, libgeos-dev[^,]*//;s/, liblas-c-dev[^,]*//' \
	-e 's/libcfitsio-dev/libcfitsio3-dev/' \
	-e '/^Package:.*lidar/,/^$/d' \
	-e '/^Package:.*geom/,/^$/d' debian/control
    rm debian/libmonetdb5-server-lidar.install debian/libmonetdb5-server-geom.install
    sed -i '/geo[ms]=yes/s/yes/no/;/gdal=yes/s/yes/no/;/lidar=yes/s/yes/no/;/liblas=yes/s/yes/no/' debian/rules
    ;;
esac

case $SUITE in
jessie | trusty)
    # Xenial Xerus (and presumably newer releases) uses php-cli,
    # all others still have php5-cli and don't have php*-sockets
    sed -i 's/php-cli/php5-cli/;s/, *php-sockets//' debian/control
    ;;
esac

case $SUITE in
trusty)
    # the trusty linker produces unresolved references to openSSL functions
    sed -i '/openssl_LIBS/s/WIN32?//' clients/mapilib/Makefile.ag
    sed -i '/^libmapi_la_LIBADD/s/$/ $(openssl_LIBS)/' clients/mapilib/Makefile.am clients/mapilib/Makefile.in
    ;;
esac

case $SUITE in
jessie | trusty)
    # The Python 3 version is too old for py3integration.
    sed -i '/^Package: monetdb-python3/,/^$/d' debian/control
    # There is a separate line for the Python3 dependencies: delete it
    sed -i '/python3/d' debian/control
    rm debian/monetdb-python3.install
    sed -i -e 's/py3integration=yes/py3integration=no/' \
	-e 's/python3=yes/python3=no/' debian/rules
;;
esac

case $SUITE in
cosmic)
    # libbam is not available as a shared object (also true for older
    # version) and this means that on 18.10 the libmonetdb5-server-bam
    # package cannot be compiled on amd64
    sed -i -e 's/libbam-dev, //' \
	-e '/^Package: libmonetdb5-server-bam/,/^$/d' debian/control
    sed -i '/samtools=yes/s/yes/no/' debian/rules
    rm debian/libmonetdb5-server-bam.install
    ;;
esac

# debhelper compatibility 9 and later support multiarch by using
# architecture-specific subdirectories in /usr/lib (and /lib); our
# code base is set up for amd64 (x86_64), so change to whatever
# architecture we're compiling for
case $ARCH in
x86_64 | amd64)
    ;;
*)
    sed -i "s/x86_64/$ARCH/g" debian/*.install
    ;;
esac
