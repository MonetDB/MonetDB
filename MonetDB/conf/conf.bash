#
# This script is supposed to be "sourced" in order to set your (architecture
# dependent) environment to be able to compile Monet 4.2.
#
# To select your desired compiler ("GNU" or "ntv" (native)), your desired
# binary type (32bit or 64bit), and whether binarys should be linked
# dynamically or statically, set the following environment variables before
# "sourcing" this script:
#
#	COMP="GNU"	or	COMP="ntv"
#	BITS="32"	or	BITS="64"
#	LINK="dynamic"	or	LINK="static"
#
# (If not or wrongly set, "GNU32dynamic" is used as default.)
#

os="`uname`"
base="${PWD}"

if [ ! -x bootstrap ] ; then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out Monet source tree.'
	echo ''
  else
	binpath=""
	libpath=""

	# check for not or incorrectly set variables (BUILD, PREFIX, COMP, BITS, LINK)

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
	if [ "${BITS}" != "32"   -a  "${BITS}" != "64"  ]  ; then
		echo ''
		echo 'BITS not set to either "32" or "64" to select the desired binary type.'
		echo 'Using BITS="32" (default).'
		BITS="32"
	fi
	LINK="`echo "${LINK}" | cut -c1`"
	if [ "${LINK}" != "d"    -a  "${LINK}" != "s"   ]  ; then
		echo ''
		echo 'LINK not set to either "dynamic" or "static" to select the desired way of linking.'
		if [ "${os}${COMP}" = "Linuxntv" ] ; then
			echo 'Using LINK="static" (default for Intel compiler on Linux).'
			LINK="s"
		  else
			echo 'Using LINK="dynamic" (default).'
			LINK="d"
		fi
	fi

	# exclude "illegal" combinations

	if [ "${os}" = "Linux" ] ; then
		if [ "${BITS}" = "64" ] ; then
			echo ''
			echo 'Linux doesn'\''t support 64 bit, yet; hence, using BITS="32".'
			BITS="32"
		fi
		if [ "${COMP}" = "ntv" -a "${LINK}" = "d" ] ; then
			echo ''
			echo 'Intel compiler on Linux doesn'\''t support dynamic linking, yet; hence, using LINK="static".'
			LINK="s"
		fi
	fi

	# set default compilers & configure options

	if [ "${COMP}" = "GNU" ] ; then
		# standard GNU compilers are gcc/g++
		cc="gcc"
		cxx="g++"
	fi
	if [ "${COMP}" = "ntv" ] ; then
		# standard native compilrs are cc/CC
		cc="cc"
		cxx="CC"
	fi
	if [ "${LINK}" = "d"   ] ; then
		# dynamic/shared linking
		conf_opts="--enable-shared --disable-static"
	  else
		# static linking
	  	conf_opts="--disable-shared --enable-static"
	fi

	# (additional) system-specific settings

	if [ "${os}" = "Linux" ] ; then
		if [ "${COMP}" = "ntv" ] ; then
			# "ntv" on Linux means IntelC++-5.0.1-beta ("icc")
			export IA32ROOT=/soft/IntelC++-5.0.1-beta/ia32
			export INTEL_FLEXLM_LICENSE=/soft/IntelC++-5.0.1-beta/licenses
			libpath="/soft/IntelC++-5.0.1-beta/ia32/lib"
			cc="icc"
			cxx="icc"
		fi
	fi

	if [ "${os}" = "SunOS" ] ; then
		# "standard: SunOS paths
		binpath="/opt/SUNWspro/bin:/usr/local/bin:${binpath}"
		libpath="/usr/local/lib:${libpath}"
		if [ "${BITS}" = "64" ] ; then
			# propper/extended LD_LIBRAY_PATH for 64bit on SunOS
			libpath="/usr/lib/sparcv9:/usr/ucblib/sparcv9:${libpath}"
			# GNU ar in /usr/local/bin doesn't support 64bit
			export AR='/usr/ccs/bin/ar'
			export AR_FLAGS='-r -cu'
		fi
		if [ "${COMP}" = "GNU" ] ; then
			# required GNU gcc/g++ options for 32 & 64 bit
			cc="${cc} -m$BITS"
			cxx="${cxx} -m$BITS"
		fi
		if [ "${COMP}${BITS}" = "ntv64" ] ; then
			# required SUNWspro cc/CC options for 64bit
			cc="${cc} -xarch=v9"
			cxx="${cxx} -xarch=v9"
		fi
		# our "fake" /soft/local/bin on apps
		binpath="/var/tmp/local/bin:${binpath}"
		libpath="/var/tmp/local/lib:${libpath}"
	fi

	if [ "${os}" = "IRIX64" ] ; then
		# propper/extended paths on medusa
		binpath="/soft64/local/bin:/soft/local/bin:/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/local/bin:/usr/java/bin:${binpath}"
		if [ "${COMP}${BITS}" = "GNU32" ] ; then
			# propper/extended paths on medusa
			libpath="/soft/local/lib:${libpath}"
		fi
		if [ "${COMP}${BITS}" = "GNU64" ] ; then
			# propper/extended paths on medusa
			libpath="/soft/local/lib/mabi=64:${libpath}"
			# required GNU gcc/g++ options for 64bit
			cc="${cc} -mabi=64"
			cxx="${cxx} -mabi=64"
		fi
		if [ "${COMP}${BITS}" = "ntv64" ] ; then
			# required MIPSpro cc/CC options for 64bit
			cc="${cc} -64"
			cxx="${cxx} -64"
		fi
		# 32 & 64 bit libreadline for IRIX64 are in /ufs/monet/lib
		conf_opts="${conf_opts} --with-readline=/ufs/monet"
	fi

	# prepend target bin-dir to PATH
	binpath="${PREFIX}/bin:${binpath}"
	# remove trailing ':'
	binpath=`echo "${binpath}" | sed 's|:$||'`
	libpath=`echo "${libpath}" | sed 's|:$||'`

	# export new settings
	echo ""
	echo "Setting..."
	export CC="${cc}"
	echo " CC=${CC}"
	export CXX="${cxx}"
	echo " CXX=${CXX}"
	export CFLAGS=""
	echo " CFLAGS=${CFLAGS}"
	export CXXFLAGS=""
	echo " CXXFLAGS=${CXXFLAGS}"
	if [ "${binpath}" ] ; then
		if [ "${PATH}" ] ; then
			# prepend new binpath to existing PATH, if PATH doesn't contain binpath, yet
			if [ "`echo ":${PATH}:" | sed "s|:${binpath}:|:|"`" = ":${PATH}:" ] ; then
				export PATH="${binpath}:${PATH}"
			fi
		  else
		  	# set PATH as binpath
			export PATH="${binpath}"
		fi
		echo " PATH=${PATH}"
	fi
	if [ "${libpath}" ] ; then
		if [ "${LD_LIBRARY_PATH}" ] ; then
			# prepend new libpath to existing LD_LIBRARY_PATH, if LD_LIBRARY_PATH doesn't contain libpath, yet
			if [ "`echo ":${LD_LIBRARY_PATH}:" | sed "s|:${libpath}:|:|"`" = ":${LD_LIBRARY_PATH}:" ] ; then
				export LD_LIBRARY_PATH="${libpath}:${LD_LIBRARY_PATH}"
			fi
		  else
		  	# set LD_LIBRARY_PATH as libpath
			export LD_LIBRARY_PATH="${libpath}"
		fi
		echo " LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
	fi
	
#	# we shouldn't need this
#	if [ "${LD_LIBRARY_PATH}" ] ; then
#		export LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet:${LD_LIBRARY_PATH}"
#	  else	export LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet"
#	fi

#	# this is obsolete
#	export MONETDIST="${PREFIX}"
#	export MONET_MOD_PATH="${PREFIX}/lib:${PREFIX}/lib/Monet"

	# for convenience: store the complete configure-call in CONFIGURE
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
