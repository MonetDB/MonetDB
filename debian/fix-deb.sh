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
    ;;
1)
    # we're passed the name of the distribution
    SUITE=$1
    ;;
esac

case $SUITE in
wheezy | precise | trusty)
    # fix control file because these systems don't have liblas and a
    # too old version of libgeos
    sed -i -e 's/, libgeos-dev[^,]*//;s/, liblas-c-dev[^,]*//' \
	-e '/^Package:.*lidar/,/^$/d' \
	-e '/^Package:.*geom/,/^$/d' debian/control
    rm debian/libmonetdb5-server-lidar.install debian/libmonetdb5-server-geom.install
    sed -i '/geo[ms]=yes/s/yes/no/;/gdal=yes/s/yes/no/;/lidar=yes/s/yes/no/;/liblas=yes/s/yes/no/' debian/rules
    ;;
esac

case $SUITE in
wheezy | jessie | precise | trusty | wily)
    # Xenial Xerus (and presumably newer releases) uses php-common,
    # all others still have php5-common
    sed -i 's/php-common/php5-common/' debian/control
    ;;
esac
