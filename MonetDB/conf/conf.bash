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
	binpath=""
	libpath=""

	if [ ! "${BUILD}" ] ; then
		echo ''
		echo 'BUILD not set to specify desired compilation directory.'
		echo 'Using BUILD="'${base}/${os}'" (default).'
		BUILD="${base}/${os}"
	fi
	if [ ! "${PREFIX}" ] ; then
		echo ''
		echo 'PREFIX not set to specify desired target directory.'
		echo 'Using PREFIX="'${BUILD}'" (default).'
		PREFIX="${BUILD}"
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
			echo 'Linux doesn'\''t support 64 bit, yet; hence, using BITS=32.'
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
		binpath="/var/tmp/local/bin:/opt/SUNWspro/bin:/usr/local/bin:${binpath}"
		libpath="/usr/local/lib:${libpath}"
		if [ "${BITS}" = "64" ] ; then
			libpath="/usr/lib/sparcv9:/usr/ucblib/sparcv9:${libpath}"
		fi
		if [ "${COMP}" = "GNU" ] ; then
			cc="${cc} -m$BITS"
			cxx="${cxx} -m$BITS"
		fi
		if [ "${COMP}${BITS}" = "ntv64" ] ; then
			cc="${cc} -xarch=v9"
			cxx="${cxx} -xarch=v9"
		fi
		libpath="/var/tmp/local/lib:${libpath}"
	fi

	if [ "${os}" = "IRIX64" ] ; then
		binpath="/soft64/local/bin:/soft/local/bin:/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/local/bin:/usr/java/bin:${binpath}"
		if [ "${COMP}${BITS}" = "GNU32" ] ; then
			libpath="/soft/local/lib:${libpath}"
		fi
		if [ "${COMP}${BITS}" = "GNU64" ] ; then
			libpath="/soft/local/lib/mabi=64:${libpath}"
			cc="${cc} -mabi=64"
			cxx="${cxx} -mabi=64"
		fi
		if [ "${COMP}${BITS}" = "ntv64" ] ; then
			cc="${cc} -64"
			cxx="${cxx} -64"
		fi
		conf_opts="${conf_opts} --with-readline=/ufs/monet"
	fi

	binpath="${PREFIX}/bin:${binpath}"
	binpath=`echo "${binpath}" | sed 's|:$||'`
	libpath=`echo "${libpath}" | sed 's|:$||'`

	echo ""
	echo "Setting..."
	export CC="${cc}"
	echo " CC=${CC}"
	export CXX="${cxx}"
	echo " CXX=${CXX}"
	if [ "${binpath}" ] ; then
		if [ "${PATH}" ] ; then
			if [ "`echo ":${PATH}:" | sed "s|:${binpath}:|:|"`" = ":${PATH}:" ] ; then
				export PATH="${binpath}:${PATH}"
			fi
		  else
			export PATH="${binpath}"
		fi
		echo " PATH=${PATH}"
	fi
	if [ "${libpath}" ] ; then
		if [ "${LD_LIBRARY_PATH}" ] ; then
			if [ "`echo ":${LD_LIBRARY_PATH}:" | sed "s|:${libpath}:|:|"`" = ":${LD_LIBRARY_PATH}:" ] ; then
				export LD_LIBRARY_PATH="${libpath}:${LD_LIBRARY_PATH}"
			fi
		  else
			export LD_LIBRARY_PATH="${libpath}"
		fi
		echo " LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
	fi
#	if [ "${LD_LIBRARY_PATH}" ] ; then
#		export LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet:${LD_LIBRARY_PATH}"
#	  else	export LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet"
#	fi

#	export MONETDIST="${PREFIX}"
#	export MONET_MOD_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet"

	export CONFIGURE="${base}/configure ${conf_opts} --prefix=${PREFIX}"
	echo " CONFIGURE=${CONFIGURE}"

	mkdir -p ${BUILD}

	echo ""
	echo "To compile Monet, just execute:"
	echo -e "\t./bootstrap"
	echo -e "\tcd ${BUILD}"
	echo -e "\t${CONFIGURE}"
	echo -e "\tmake"
	echo -e "\tmake install"
	echo ""
fi
