#
# This script is supposed to be "sourced" in order to set your (architecture
# dependent) environment to be able to compile sq.
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

source `monet-config --prefix`/share/Monet/conf/conf.csh

set os = "`uname`"
set base = "${PWD}"

if ( ! -x bootstrap ) then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out sql source tree.'
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
		if ( "${COMP}" = "ntv" && "${LINK}" == "d" ) then
			echo ''
			echo 'Intel compiler on Linux doesn'\''t support dynamic linking, yet; hence, using LINK="static".'
			set LINK = "s"
		endif
	endif

	# set default compilers & configure options

	if ( "${os}" == "SunOS" ) then
		if ( "${BITS}" == "64" ) then
			set conf_opts = "${conf_opts} --with-odbc=/var/tmp/soft64/local"
		else
			set conf_opts = "${conf_opts} --with-odbc=/var/tmp/soft/local"
		endif
	endif

	if ( "${os}" == "IRIX64" ) then
		if ( "${BITS}" == "64" ) then
			set conf_opts = "${conf_opts} --with-odbc=/soft64/local"
		else
			set conf_opts = "${conf_opts} --with-odbc=/soft/local"
		endif
	endif

	# prepend target bin-dir to PATH
	set binpath = "${PREFIX}/bin:${binpath}"
	# remove trailing ':'
	set binpath = `echo "${binpath}" | sed 's|:$||'`
	set libpath = `echo "${libpath}" | sed 's|:$||'`

	# export new settings
	echo ""
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
	echo "To compile SQL, just execute:"
	echo "\t./bootstrap"
	echo "\tcd ${BUILD}"
	echo "\t${CONFIGURE}"
	echo "\tmake"
	echo "\tmake install"
	echo ""
endif
