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

set os = "`uname`"
set base = "${PWD}"

if ( ! -x bootstrap ) then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out Monet source tree.'
	echo ''
  else
	set binpath = ""
	set libpath = ""

	# check for not set variables (BUILD, PREFIX, COMP, BITS, LINK)

	if ( ! ${?BUILD} ) then
		echo ''
		echo 'BUILD not set to specify desired compilation directory.'
		echo 'Using BUILD="'${base}/${os}'" (default).'
		set BUILD = "${base}/${os}"
	endif
	if ( ! ${?PREFIX} ) then
		echo ''
		echo 'PREFIX not set to specify desired target directory.'
		echo 'Using PREFIX="'${BUILD}'" (default).'
		set PREFIX = "${BUILD}"
	endif

	if ( ! ${?COMP} ) then
		echo ''
		echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
		echo 'Using COMP="GNU" (default).'
		set COMP = "GNU"
	endif
	if ( ! ${?BITS} ) then
		echo ''
		echo 'BITS not set to either "32" or "64" to select the desired binary type.'
		echo 'Using BITS="32" (default).'
		set BITS = "32"
	endif

	if ( ! ${?LINK} ) then
		echo ''
		echo 'LINK not set to either "dynamic" or "static" to select the desired way of linking.'
		if ( "${os}${COMP}" == "Linuxntv" ) then
			echo 'Using LINK="static" (default for Intel compiler on Linux).'
			set LINK = "s"
		  else
			echo 'Using LINK="dynamic" (default).'
			set LINK = "d"
		endif
	endif

	# check for incorrectly set variables (BUILD, PREFIX, COMP, BITS, LINK)

	if ( "${COMP}" != "GNU"  &&  "${COMP}" != "ntv" ) then
		echo ''
		echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
		echo 'Using COMP="GNU" (default).'
		set COMP = "GNU"
	endif
	if ( "${BITS}" != "32"   &&  "${BITS}" != "64"  ) then
		echo ''
		echo 'BITS not set to either "32" or "64" to select the desired binary type.'
		echo 'Using BITS="32" (default).'
		set BITS = "32"
	endif
	set LINK = "`echo "${LINK}" | cut -c1`"
	if ( "${LINK}" != "d"    &&  "${LINK}" != "s"   ) then
		echo ''
		echo 'LINK not set to either "dynamic" or "static" to select the desired way of linking.'
		if ( "${os}${COMP}" == "Linuxntv" ) then
			echo 'Using LINK="static" (default for Intel compiler on Linux).'
			set LINK = "s"
		  else
			echo 'Using LINK="dynamic" (default).'
			set LINK = "d"
		endif
	endif

	# exclude "illegal" combinations

	if ( "${os}" == "Linux" ) then
		if ( "${BITS}" == "64" ) then
			echo ''
			echo 'Linux doesn'\''t support 64 bit, yet; hence, using BITS="32".'
			set BITS = "32"
		endif
	endif

	# set default compilers & configure options

	if ( "${COMP}" == "GNU" ) then
		# standard GNU compilers are gcc/g++
		set cc = "gcc"
		set cxx = "g++"
	endif
	if ( "${COMP}" == "ntv" ) then
		# standard native compilrs are cc/CC
		set cc = "cc"
		set cxx = "CC"
	endif
	if ( "${LINK}" == "d"   ) then
		# dynamic/shared linking
		set conf_opts = "--enable-shared --disable-static"
	  else
		# static linking
		set conf_opts = "--disable-shared --enable-static"
	endif

	# (additional) system-specific settings

	if ( "${os}" == "Linux" ) then
		if ( "${COMP}" == "ntv" ) then
			# "ntv" on Linux means IntelC++-5.0.1-beta ("icc")
			setenv IA32ROOT /soft/IntelC++-5.0.1-beta/ia32
			setenv INTEL_FLEXLM_LICENSE /soft/IntelC++-5.0.1-beta/licenses
			set libpath = "/soft/IntelC++-5.0.1-beta/ia32/lib"
			set cc = "icc"
			set cxx = "icc"
			set conf_opts = "${conf_opts} --with-hwcounters=/soft/local"
			if ( "${LINK}" == "d" ) then
				# otherwise, Mserver crashes due to the "alloca(3)"-problem
				set conf_opts = "${conf_opts} --enable-debug"
			endif
		endif
	endif

	if ( "${os}" == "SunOS" ) then
		# "standard: SunOS paths
		set binpath = "/opt/SUNWspro/bin:/usr/local/bin:${binpath}"
		set libpath = "/usr/local/lib:${libpath}"
		if ( "${BITS}" == "64" ) then
			# propper/extended LD_LIBRAY_PATH for 64bit on SunOS
			set libpath = "/usr/lib/sparcv9:/usr/ucblib/sparcv9:${libpath}"
			# GNU ar in /usr/local/bin doesn't support 64bit
			setenv AR '/usr/ccs/bin/ar'
			setenv AR_FLAGS '-r -cu'
		endif
		if ( "${COMP}" == "GNU" ) then
			# required GNU gcc/g++ options for 32 & 64 bit
			set cc = "${cc} -m$BITS"
			set cxx = "${cxx} -m$BITS"
		endif
		if ( "${COMP}${BITS}" == "ntv64" ) then
			# required SUNWspro cc/CC options for 64bit
			set cc = "${cc} -xarch=v9"
			set cxx = "${cxx} -xarch=v9"
		endif
		# our "fake" /soft/local/bin on apps
		set binpath = "/var/tmp/local/bin:${binpath}"
		set libpath = "/var/tmp/local/lib:${libpath}"
		if ( "${BITS}" == "64" ) then
			set conf_opts = "${conf_opts} --with-readline=/var/tmp/soft64/local"
			set conf_opts = "${conf_opts} --with-getopt=/var/tmp/soft64/local"
		else
			set conf_opts = "${conf_opts} --with-readline=/var/tmp/soft/local"
			set conf_opts = "${conf_opts} --with-getopt=/var/tmp/soft/local"
		endif
	endif

	if ( "${os}" == "IRIX64" ) then
		# propper/extended paths on medusa
		set binpath = "/soft64/local/bin:/soft/local/bin:/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/local/bin:/usr/java/bin:${binpath}"
		if ( "${COMP}${BITS}" == "GNU32" ) then
			# propper/extended paths on medusa
			set libpath = "/soft/local/lib:${libpath}"
		endif
		if ( "${COMP}${BITS}" == "GNU64" ) then
			# propper/extended paths on medusa
			set libpath = "/soft/local/lib/mabi=64:${libpath}"
			# required GNU gcc/g++ options for 64bit
			set cc = "${cc} -mabi=64"
			set cxx = "${cxx} -mabi=64"
		endif
		if ( "${COMP}${BITS}" == "ntv64" ) then
			# required MIPSpro cc/CC options for 64bit
			set cc = "${cc} -64"
			set cxx = "${cxx} -64"
		endif
		# 32 & 64 bit libreadline for IRIX64 are in /ufs/monet/lib
		if ( "${BITS}" == "64" ) then
			set conf_opts = "${conf_opts} --with-readline=/soft64/local"
			set conf_opts = "${conf_opts} --with-getopt=/soft64/local"
		else
			set conf_opts = "${conf_opts} --with-readline=/soft/local"
			set conf_opts = "${conf_opts} --with-getopt=/soft/local"
		endif
	endif

	# prepend target bin-dir to PATH
	set binpath = "${PREFIX}/bin:${binpath}"
	# remove trailing ':'
	set binpath = `echo "${binpath}" | sed 's|:$||'`
	set libpath = `echo "${libpath}" | sed 's|:$||'`

	# export new settings
	echo ""
	echo "Setting..."
	setenv CC "${cc}"
	echo " CC=${CC}"
	setenv CXX "${cxx}"
	echo " CXX=${CXX}"
	setenv CFLAGS ""
	echo " CFLAGS=${CFLAGS}"
	setenv CXXFLAGS ""
	echo " CXXFLAGS=${CXXFLAGS}"
	if ( ${%binpath} ) then
		if ( ${?PATH} ) then
			# prepend new binpath to existing PATH, if PATH doesn't contain binpath, yet
			if ( "`echo ':${PATH}:' | sed 's|:${binpath}:|:|'`" == ":${PATH}:" ) then
				setenv PATH "${binpath}:${PATH}"
			endif
		  else
		  	# set PATH as binpath
			setenv PATH "${binpath}"
		endif
		echo " PATH=${PATH}"
	endif
	if ( ${%libpath} ) then
		if ( ${?LD_LIBRARY_PATH} ) then
			# prepend new libpath to existing LD_LIBRARY_PATH, if LD_LIBRARY_PATH doesn't contain libpath, yet
			if ( "`echo ':${LD_LIBRARY_PATH}:' | sed 's|:${libpath}:|:|'`" == ":${LD_LIBRARY_PATH}:" ) then
				setenv LD_LIBRARY_PATH "${libpath}:${LD_LIBRARY_PATH}"
			endif
		  else
		  	# set LD_LIBRARY_PATH as libpath
			setenv LD_LIBRARY_PATH "${libpath}"
		endif
		echo " LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
	endif

#	# we shouldn't need this
#	if ( ${?LD_LIBRARY_PATH} ) then
#		setenv LD_LIBRARY_PATH "${PREFIX}/lib:${PREFIX}/lib/Monet:${LD_LIBRARY_PATH}"
#	  else	setenv LD_LIBRARY_PATH "${PREFIX}/lib:${PREFIX}/lib/Monet"
#	endif

#	# this is obsolete
#	setenv MONETDIST "${PREFIX}"
#	setenv MONET_MOD_PATH "${PREFIX}/lib:${PREFIX}/lib/Monet"

	# for convenience: store the complete configure-call in CONFIGURE
	setenv CONFIGURE "${base}/configure ${conf_opts} --prefix=${PREFIX}"
	echo " CONFIGURE=${CONFIGURE}"

	mkdir -p ${BUILD}

	echo ""
	echo "To compile Monet, just execute:"
	echo "\t./bootstrap"
	echo "\tcd ${BUILD}"
	echo "\t${CONFIGURE}"
	echo "\tmake"
	echo "\tmake install"
	echo ""
endif
