#
# This script is supposed to be "sourced" in order to set your (architecture
# dependent) environment to be able to compile Monet 4.2.
#
# To select your desired compiler ("GNU" or "ntv" (native)) and your desired
# binary type (32bit or 64bit), set the following environment variables
# before "sourcing" this script:
#
#	COMP="GNU"	or	COMP="ntv"
#	BITS="32"	or	BITS="64"
#
# (If not or wrongly set, "GNU32" is used as default.)
#

os="`uname`"
base="${PWD}"
conf_opts="--enable-shared --disable-static"

if [ ! -x bootstrap ] ; then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out Monet source tree.'
	echo ''
  else
	if [ "${PATH}" ] ; then
		binpath=":${PATH}"
	  else	binpath=""
	fi
	if [ "${LD_LIBRARY_PATH}" ] ; then
		libpath=":${LD_LIBRARY_PATH}"
	  else	libpath=""
	fi

	if [ ! "${PREFIX}" ] ; then
		echo ''
		echo 'PREFIX not set to specify desired target directory.'
		echo 'Using PREFIX="'${base}/${os}'" (default).'
		PREFIX="${base}/${os}"
	fi
	if [ "${COMP}" != "GNU"  -a  "${COMP}" != "ntv" ] ; then
		echo ''
		echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
		echo 'Using COMP="GNU" (default).'
		COMP="GNU"
	fi
	if [ "${BITS}" != "32"   -a  "${BITS}" != "64" ]  ; then
		echo ''
		echo 'BITS not set to either "32" or "64" to select the desired binary type.'
		echo 'Using BITS="32" (default).'
		BITS="32"
	fi

	if [ "${os}" = "Linux" ] ; then
		if [ "${COMP}" = "ntv" ] ; then
			echo ''
			echo 'Linux'\''s "native" compilier is "GNU"; hence, using COMP=GNU.'
			COMP="GNU"
		fi
		if [ "${BITS}" = "64" ] ; then
			echo ''
			echo 'Linux doesn'\''t support 64 bit, yep; hence, using BITS=32.'
			BITS="32"
		fi
	fi

	if [ "${COMP}" = "GNU" ] ; then
		cc="gcc"
		cxx="g++"
	fi
	if [ "${COMP}" = "ntv" ] ; then
		cc="cc"
		cxx="CC"
	fi

	if [ "${os}" = "SunOS" ] ; then
		binpath=":/var/tmp/local/bin:/opt/SUNWspro/bin:/usr/local/bin${binpath}"
		libpath=":/usr/local/lib${libpath}"
		if [ "${BITS}" = "64" ] ; then
			libpath=":/usr/lib/sparcv9:/usr/ucblib/sparcv9${libpath}"
		fi
		if [ "${COMP}" = "GNU" ] ; then
			cc="${cc} -m$BITS"
			cxx="${cxx} -m$BITS"
		fi
		if [ "${COMP}${BITS}" = "ntv64" ] ; then
			cc="${cc} -xarch=v9"
			cxx="${cxx} -xarch=v9"
		fi
	fi

	if [ "${os}" = "IRIX64" ] ; then
		binpath=":/dbfarm/local/bin:/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/local/bin:/usr/java/bin${binpath}"
		if [ "${COMP}${BITS}" = "GNU64" ] ; then
			cc="${cc} -mabi=64"
			cxx="${cxx} -mabi=64"
		fi
		if [ "${COMP}${BITS}" = "ntv64" ] ; then
			cc="${cc} -64"
			cxx="${cxx} -64"
		fi
		conf_opts="${conf_opts} --with-readline=/ufs/monet"
	fi

	export CC="${cc}"
	export CXX="${cxx}"
	export PATH="${PREFIX}/bin${binpath}"
	export LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet${libpath}"

	export MONETDIST="${PREFIX}"
	export MONET_MOD_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet"

	export CONFIGURE="${base}/configure ${conf_opts} --prefix=${PREFIX}"

	echo ""
	echo "To compile Monet, just execute:"
	echo -e "\t./bootstrap"
	echo -e "\tcd ${PREFIX}"
	echo -e "\t${CONFIGURE}"
	echo -e "\tmake"
	echo -e "\tmake install"
	echo ""
fi
