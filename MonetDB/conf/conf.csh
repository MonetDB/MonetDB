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

set os = "`uname`"
set base = "${PWD}"
set conf_opts = "--enable-shared --disable-static"

if ( ! -x bootstrap ) then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out Monet source tree.'
	echo ''
  else
	set binpath = ""
	set libpath = ""

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

	if ( "${COMP}" != "GNU"  &&  "${COMP}" != "ntv" ) then
		echo ''
		echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
		echo 'Using COMP="GNU" (default).'
		set COMP = "GNU"
	endif
	if ( "${BITS}" != "32"   &&  "${BITS}" != "64" ) then
		echo ''
		echo 'BITS not set to either "32" or "64" to select the desired binary type.'
		echo 'Using BITS="32" (default).'
		set BITS = "32"
	endif

	if ( "${os}" == "Linux" ) then
		if ( "${COMP}" == "ntv" ) then
			echo ''
			echo 'Linux'\''s "native" compilier is "GNU"; hence, using COMP=GNU.'
			set COMP = "GNU"
		endif
		if ( "${BITS}" == "64" ) then
			echo ''
			echo 'Linux doesn'\''t support 64 bit, yet; hence, using BITS=32.'
			set BITS = "32"
		endif
	endif

	if ( "${COMP}" == "GNU" ) then
		set cc = "gcc"
		set cxx = "g++"
	endif
	if ( "${COMP}" == "ntv" ) then
		set cc = "cc"
		set cxx = "CC"
	endif

	if ( "${os}" == "SunOS" ) then
		set binpath = "/var/tmp/local/bin:/opt/SUNWspro/bin:/usr/local/bin:${binpath}"
		set libpath = "/usr/local/lib:${libpath}"
		if ( "${BITS}" == "64" ) then
			set libpath = "/usr/lib/sparcv9:/usr/ucblib/sparcv9:${libpath}"
		endif
		if ( "${COMP}" == "GNU" ) then
			set cc = "${cc} -m$BITS"
			set cxx = "${cxx} -m$BITS"
		endif
		if ( "${COMP}${BITS}" == "ntv64" ) then
			set cc = "${cc} -xarch=v9"
			set cxx = "${cxx} -xarch=v9"
		endif
		set libpath = "/var/tmp/local/lib:${libpath}"
	endif

	if ( "${os}" == "IRIX64" ) then
		set binpath = "/soft64/local/bin:/soft/local/bin:/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/local/bin:/usr/java/bin:${binpath}"
		if ( "${COMP}${BITS}" == "GNU32" ) then
			set libpath = "/soft/local/lib:${libpath}"
		endif
		if ( "${COMP}${BITS}" == "GNU64" ) then
			set libpath = "/soft/local/lib/mabi=64:${libpath}"
			set cc = "${cc} -mabi=64"
			set cxx = "${cxx} -mabi=64"
		endif
		if ( "${COMP}${BITS}" == "ntv64" ) then
			set cc = "${cc} -64"
			set cxx = "${cxx} -64"
		endif
		set conf_opts = "${conf_opts} --with-readline=/ufs/monet"
	endif

	set binpath = "${PREFIX}/bin:${binpath}"
	set binpath = `echo "${binpath}" | sed 's|:$||'`
	set libpath = `echo "${libpath}" | sed 's|:$||'`

	echo ""
	echo "Setting..."
	setenv CC "${cc}"
	echo " CC=${CC}"
	setenv CXX "${cxx}"
	echo " CXX=${CXX}"
	if ( ${%binpath} ) then
		if ( ${?PATH} ) then
			if ( "`echo ':${PATH}:' | sed 's|:${binpath}:|:|'`" == ":${PATH}:" ) then
				setenv PATH "${binpath}:${PATH}"
			endif
		  else
			setenv PATH "${binpath}"
		endif
		echo " PATH=${PATH}"
	endif
	if ( ${%libpath} ) then
		if ( ${?LD_LIBRARY_PATH} ) then
			if ( "`echo ':${LD_LIBRARY_PATH}:' | sed 's|:${libpath}:|:|'`" == ":${LD_LIBRARY_PATH}:" ) then
				setenv LD_LIBRARY_PATH "${libpath}:${LD_LIBRARY_PATH}"
			endif
		  else
			setenv LD_LIBRARY_PATH "${libpath}"
		endif
		echo " LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
	endif
#	if ( ${?LD_LIBRARY_PATH} ) then
#		setenv LD_LIBRARY_PATH "${PREFIX}/lib:${PREFIX}/lib/Monet:${LD_LIBRARY_PATH}"
#	  else	setenv LD_LIBRARY_PATH "${PREFIX}/lib:${PREFIX}/lib/Monet"
#	endif

#	setenv MONETDIST "${PREFIX}"
#	setenv MONET_MOD_PATH "${PREFIX}/lib:${PREFIX}/lib/Monet"

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
