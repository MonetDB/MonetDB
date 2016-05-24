if [ -f /etc/lsb-release ]; then
    source /etc/lsb-release
    CODENAME=${DISTRIB_CODENAME}
elif [ -f /etc/os-release ]; then
    source /etc/os-release
    if [[ ${UBUNTU_CODENAME:+defined} == defined ]]; then
	CODENAME=$UBUNTU_CODENAME
    else
	CODENAME="${VERSION#*(}"
	CODENAME="${CODENAME%)}"
    fi
else
    echo "don't know what distro this is"
    exit 1
fi

case $CODENAME in
trusty | wheezy)
    # fix control file because these systems don't have liblas and a
    # too old version of libgeos
    sed -i 's/, libgeos-dev[^,]*//;s/, liblas-c-dev[^,]*//' debian/control
    sed -i '/^Package:.*lidar/,/^$/d' debian/control
    sed -i '/^Package:.*geom/,/^$/d' debian/control
    rm debian/libmonetdb5-server-lidar.install debian/libmonetdb5-server-geom.install
    ;;
esac
