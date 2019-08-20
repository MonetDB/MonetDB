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
jessie)
    # Debian 8 still have php5-cli and doesn't have php*-sockets;
    # newer ones use php-cli and do have php-sockets.
    sed -i 's/php-cli/php5-cli/;s/, *php-sockets//' debian/control
    ;;
esac

case $SUITE in
jessie)
    # The Python 3 version is too old for py3integration.
    sed -i '/^Package: monetdb-python3/,/^$/d' debian/control
    sed -i 's/ python3-dev, python3-numpy,//' debian/control
    rm debian/monetdb-python3.install
    sed -i 's/py3integration=yes/py3integration=no/' debian/rules
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
