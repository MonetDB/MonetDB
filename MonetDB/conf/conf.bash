#
# This script is supposed to be "sourced" in the top-level directory of the
# checked-out Monet source tree (referred to as BASE in the remainder). 
# While sourced, this script sets your (architecture dependent) environment
# as required to compile Monet.
#
# By default, compilation will take place in MONET_BUILD=BASE/`uname` and
# Monet will be installed in MONET_PREFIX=MONET_BUILD. You can change either
# directory by setting the enviroment variables
#	MONET_BUILD
# and/or
#	MONET_PREFIX
# appropiately before "sourcing" this script.
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
# (If not or wrongly set, "GNU 32 dynamic" is used as default.)
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
	modpath=""

	# check for not or incorrectly set variables (MONET_BUILD, MONET_PREFIX, COMP, BITS, LINK)

	if [ ! "${MONET_BUILD}" ] ; then
		echo ''
		echo 'MONET_BUILD not set to specify desired compilation directory.'
		echo 'Using MONET_BUILD="'${base}/${os}'" (default).'
		export MONET_BUILD="${base}/${os}"
	fi
	if [ ! "${MONET_PREFIX}" ] ; then
		echo ''
		echo 'MONET_PREFIX not set to specify desired target directory.'
		echo 'Using MONET_PREFIX="'${MONET_BUILD}'" (default).'
		export MONET_PREFIX="${MONET_BUILD}"
	fi

	if [ "${COMP}" != "GNU"  -a  "${COMP}" != "ntv" ] ; then
		echo ''
		echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
		echo 'Using COMP="GNU" (default).'
		COMP="GNU"
	fi
	if [ "${BITS}" != "32"   -a  "${BITS}" != "64"  ] ; then
		echo ''
		echo 'BITS not set to either "32" or "64" to select the desired binary type.'
		echo 'Using BITS="32" (default).'
		BITS="32"
	fi
	LINK="`echo "${LINK}" | cut -c1`"
	if [ "${LINK}" != "d"    -a  "${LINK}" != "s"   ] ; then
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
	fi

	# set default compilers, configure options & paths

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
	# "standard" local paths
	binpath="/usr/local/bin:${binpath}"
	libpath="/usr/local/lib:${libpath}"
	# "our" /soft[64] path
	soft32=/soft/local
	soft64=/soft64/local
	if [ "${BITS}" = "32" ] ; then
		softpath=${soft32}
	  else
		softpath=${soft64}
	fi

	# (additional) system-specific settings

	if [ "${os}" = "Linux" ] ; then
		if [ "${COMP}" = "ntv" ] ; then
			if [ -f /opt/intel/licenses/l_cpp.lic  -a  -f /opt/intel/compiler50/ia32/bin/iccvars.sh ] ; then
				# "ntv" on Linux means IntelC++-5.0.1 ("icc")
				# source /opt/intel/compiler50/ia32/bin/iccvars.sh
				export IA32ROOT=/opt/intel/compiler50/ia32
				export INTEL_FLEXLM_LICENSE=/opt/intel/licenses
				libpath="${IA32ROOT}/lib:${libpath}"
				binpath="${IA32ROOT}/bin:${binpath}"
			  else
				# "ntv" on Linux means IntelC++-5.0.1 ("icc")
				# source /soft/IntelC++-5.0.1/bin/iccvars.sh
				export IA32ROOT=/soft/IntelC++-5.0.1
				export INTEL_FLEXLM_LICENSE=/soft/IntelC++-5.0.1/licenses
				libpath="${IA32ROOT}/lib:${libpath}"
			fi
			cc="icc"
			cxx="icc"
			conf_opts="${conf_opts} --with-hwcounters=${softpath}"
		fi
	fi

	if [ "${os}" = "SunOS" ] ; then
		# "our" /soft[64] path on apps
		soft32="/var/tmp${soft32}"
		soft64="/var/tmp${soft64}"
		softpath="/var/tmp${softpath}"
		# "standard" SunOS paths
		binpath="/opt/SUNWspro/bin:/sw/SunOS/5.8/bin:/usr/java/bin:${binpath}"
		libpath="/sw/SunOS/5.8/lib:${libpath}"
		if [ "${BITS}" = "64" ] ; then
			# propper/extended LD_LIBRAY_PATH for 64bit on SunOS
			libpath="/usr/lib/sparcv9:/usr/ucblib/sparcv9:${libpath}"
			# GNU ar in /usr/local/bin doesn't support 64bit
			export AR='/usr/ccs/bin/ar'
			export AR_FLAGS='-r -cu'
		fi
		if [ "${COMP}${BITS}${LINK}" = "ntv32d" ] ; then
			# propper/extended LD_LIBRAY_PATH for native 32bit shared libs on SunOS
			libpath="/usr/ucblib:${libpath}"
		fi
		if [ "${COMP}${BITS}" = "GNU64" ] ; then
			# our gcc/g++ on apps is in ${soft32} (also for 64 bit)
			binpath="${soft32}/bin:${binpath}"
			libpath="${soft32}/lib/sparcv9:${soft32}/lib:${libpath}"
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
	fi

	if [ "${os}" = "IRIX64" ] ; then
		# propper/extended paths on medusa
		binpath="/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/java/bin:${binpath}"
		if [ "${BITS}" = "64" ] ; then
			# some tools are not in ${soft64} on medusa
			binpath="${soft32}/bin:${binpath}"
		fi
		if [ "${COMP}${BITS}" = "GNU64" ] ; then
			# our gcc/g++ on medusa is in ${soft32} (also for 64 bit)
			libpath="${soft32}/lib/mabi=64:${libpath}"
			# required GNU gcc/g++ options for 64bit
			cc="${cc} -mabi=64"
			cxx="${cxx} -mabi=64"
		fi
		if [ "${COMP}${BITS}" = "ntv64" ] ; then
			# required MIPSpro cc/CC options for 64bit
			cc="${cc} -64"
			cxx="${cxx} -64"
		fi
	fi

#	# gathered from old scripts, but not used anymore/yet
#	if [ "${os}" = "AIX" ] ; then
#		# rs6000.ddi.nl
#		# gcc/g++ only?
#		cc="${cc} -mthreads"
#		cxx="${cxx} -mthreads"
#	fi
#	if [ "${os}" = "CYGWIN32_NT" ] ; then
#		# yalite.ddi.nl
#		# gcc/g++ only!
#		cc="${cc} -mno-cygwin"   # ?
#		cxx="${cxx} -mno-cygwin" # ?
#		conf_opts="${conf_opts} --with-pthread=/MonetDS/PthreadsWin32"
#	fi
#	if [ "${os}" = "CYGWIN_NT-4.0" ] ; then
#		# VMware
#		# gcc/g++ only!
#		cc="${cc} -mno-cygwin"   # ?
#		cxx="${cxx} -mno-cygwin" # ?
#		conf_opts="${conf_opts} --with-pthread=/tmp"
#	fi

	if [ "${os}" != "Linux" ] ; then
		# on Linux, /soft/local is identical with /usr/local
		# prepend ${softpath} to ${binpath} & ${libpath}
		binpath="${softpath}/bin:${binpath}"
		libpath="${softpath}/lib:${libpath}"
		# "our" libs/tools in ${softpath}
		conf_opts="${conf_opts} --with-readline=${softpath}"
		conf_opts="${conf_opts} --with-getopt=${softpath}"
		conf_opts="${conf_opts} --with-z=${softpath}"
		conf_opts="${conf_opts} --with-bz2=${softpath}"
	fi

	# prepend target bin-dir to PATH
	binpath="${MONET_PREFIX}/bin:${binpath}"

#	# the following is nolonger needed for Monet
#	# set MONET_MOD_PATH and prepend it to LD_LIBRARY_PATH
#	modpath="${MONET_PREFIX}/lib:${MONET_PREFIX}/lib/Monet"
#	libpath="${modpath}:${libpath}"

	# remove trailing ':'
	binpath=`echo "${binpath}" | sed 's|:$||'`
	libpath=`echo "${libpath}" | sed 's|:$||'`
	modpath=`echo "${modpath}" | sed 's|:$||'`

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
	if [ "${modpath}" ] ; then
		if [ "${MONET_MOD_PATH}" ] ; then
			# prepend new modpath to existing MONET_MOD_PATH, if MONET_MOD_PATH doesn't contain modpath, yet
			if [ "`echo ":${MONET_MOD_PATH}:" | sed "s|:${modpath}:|:|"`" = ":${MONET_MOD_PATH}:" ] ; then
				export MONET_MOD_PATH="${modpath}:${MONET_MOD_PATH}"
			fi
		  else
			# set MONET_MOD_PATH as modpath
			export MONET_MOD_PATH="${modpath}"
		fi
		echo " MONET_MOD_PATH=${MONET_MOD_PATH}"
	fi

	# for convenience: store the complete configure-call in MONET_CONFIGURE
	export MONET_CONFIGURE="${base}/configure ${conf_opts} --prefix=${MONET_PREFIX}"
	echo " MONET_CONFIGURE=${MONET_CONFIGURE}"

	mkdir -p ${MONET_BUILD}

	echo ""
	echo "To compile Monet, just execute:"
	echo -e "\t./bootstrap"
	echo -e "\tcd ${MONET_BUILD}"
	echo -e "\t${CONFIGURE}"
	echo -e "\tmake"
	echo -e "\tmake install"
	echo ""
fi
