#
# This script is supposed to be "sourced" in the top-level directory of the
# checked-out SQL source tree (referred to as BASE in the remainder). 
# While sourced, this script sets your (architecture dependent) environment
# as required to compile SQL.
#
# In order to find your Monet installation, this script requires
# 'monet-config` to be in your PATH.
#
# By default, compilation will take place in SQL_BUILD=BASE/`uname` and SQL
# will be installed in PREFIX=SQL_BUILD. You can change either directory by
# setting the enviroment variables
#	SQL_BUILD
# and/or
#	PREFIX
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

set os = "`uname`"
set base = "${PWD}"

if ( ! -x `monet-config --prefix`/bin/monet-config ) then
	echo ''
	echo 'Could not find Monet installation.'
	echo ''
	return 1
endif

set MONET_PREFIX = `monet-config --prefix`


if ( ! -x bootstrap ) then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out sql source tree.'
	echo ''
  else
	set binpath = ""
	set libpath = ""

	# check for not set variables (SQL_BUILD, PREFIX, COMP, BITS, LINK)

	if ( ! ${?SQL_BUILD} ) then
		echo ''
		echo 'SQL_BUILD not set to specify desired compilation directory.'
		echo 'Using SQL_BUILD="'${base}/${os}'" (default).'
		set SQL_BUILD = "${base}/${os}"
	endif
	if ( ! ${?PREFIX} ) then
		echo ''
		echo 'PREFIX not set to specify desired target directory.'
		echo 'Using PREFIX="'${SQL_BUILD}'" (default).'
		set PREFIX = "${SQL_BUILD}"
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

	# check for incorrectly set variables (SQL_BUILD, PREFIX, COMP, BITS, LINK)

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

	# set default compilers, configure options & paths

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
	# "standard" local paths
	set binpath = "/usr/local/bin:${binpath}"
	set libpath = "/usr/local/lib:${libpath}"
	# "our" /soft[64] path
	set soft32 = /soft/local
	set soft64 = /soft64/local
	if ( "${BITS}" == "32" ) then
		set softpath = ${soft32}
	  else
		set softpath = ${soft64}
	endif

	# (additional) system-specific settings

	if ( "${os}" == "Linux" ) then
		if ( "${COMP}" == "ntv" ) then
			# "ntv" on Linux means IntelC++-5.0.1-beta ("icc")
			# source /soft/IntelC++-5.0.1-beta/bin/iccvars.csh
			setenv IA32ROOT /soft/IntelC++-5.0.1-beta/ia32
			setenv INTEL_FLEXLM_LICENSE /soft/IntelC++-5.0.1-beta/licenses
			set libpath = "/soft/IntelC++-5.0.1-beta/ia32/lib"
			set cc = "icc"
			set cxx = "icc"
			if ( "${LINK}" == "d" ) then
				# otherwise, Mserver crashes due to the "alloca(3)"-problem
				set conf_opts = "${conf_opts} --enable-debug"
			endif
		endif
	endif

	if ( "${os}" == "SunOS" ) then
		# "our" /soft[64] path on apps
		set soft32 = "/var/tmp${soft32}"
		set soft64 = "/var/tmp${soft64}"
		set softpath = "/var/tmp${softpath}"
		# "standard" SunOS paths
		set binpath = "/opt/SUNWspro/bin:/sw/SunOS/5.8/bin:/usr/java/bin:${binpath}"
		set libpath = "/sw/SunOS/5.8/lib:${libpath}"
		if ( "${BITS}" == "64" ) then
			# propper/extended LD_LIBRAY_PATH for 64bit on SunOS
			set libpath = "/usr/lib/sparcv9:/usr/ucblib/sparcv9:${libpath}"
			# GNU ar in /usr/local/bin doesn't support 64bit
			setenv AR '/usr/ccs/bin/ar'
			setenv AR_FLAGS '-r -cu'
		endif
		if ( "${COMP}${BITS}${LINK}" == "ntv32d" ) then
			# propper/extended LD_LIBRAY_PATH for native 32bit shared libs on SunOS
			set libpath = "/usr/ucblib:${libpath}"
		endif
		if ( "${COMP}${BITS}" == "GNU64" ) then
			# our gcc/g++ on apps is in ${soft32} (also for 64 bit)
			set binpath = "${soft32}/bin:${binpath}"
			set libpath = "${soft32}/lib:${libpath}"
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
	endif

	if ( "${os}" == "IRIX64" ) then
		# propper/extended paths on medusa
		set binpath = "/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/java/bin:${binpath}"
		if ( "${BITS}" == "64" ) then
			# some tools are not in ${soft64} on medusa
			set binpath = "${soft32}/bin:${binpath}"
		endif
		if ( "${COMP}${BITS}" == "GNU64" ) then
			# our gcc/g++ on medusa is in ${soft32} (also for 64 bit)
			set libpath = "${soft32}/lib/mabi=64:${libpath}"
			# required GNU gcc/g++ options for 64bit
			set cc = "${cc} -mabi=64"
			set cxx = "${cxx} -mabi=64"
		endif
		if ( "${COMP}${BITS}" == "ntv64" ) then
			# required MIPSpro cc/CC options for 64bit
			set cc = "${cc} -64"
			set cxx = "${cxx} -64"
		endif
	endif

	if ( "${os}" != "Linux" ) then
		# on Linux, /soft/local is identical with /usr/local
		# prepend ${softpath} to ${binpath} & ${libpath}
		set binpath = "${softpath}/bin:${binpath}"
		set libpath = "${softpath}/lib:${libpath}"
		# "our" libs/tools in ${softpath}
		set conf_opts = "${conf_opts} --with-readline=${softpath}"
		set conf_opts = "${conf_opts} --with-getopt=${softpath}"
		set conf_opts = "${conf_opts} --with-odbc=${softpath}"
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

#	# this still needed for SQL
#	setenv MONETDIST "${PREFIX}"
	setenv MONET_MOD_PATH "${PREFIX}/lib:${PREFIX}/lib/sql:${MONET_PREFIX}/lib:${MONET_PREFIX}/lib/Monet"
	echo " MONET_MOD_PATH=${MONET_MOD_PATH}"

	# for convenience: store the complete configure-call in CONFIGURE
	setenv CONFIGURE "${base}/configure ${conf_opts} --with-monet=${MONET_PREFIX} --prefix=${PREFIX}"
	echo " CONFIGURE=${CONFIGURE}"

	mkdir -p ${SQL_BUILD}

	echo ""
	echo "To compile SQL, just execute:"
	echo "\t./bootstrap"
	echo "\tcd ${SQL_BUILD}"
	echo "\t${CONFIGURE}"
	echo "\tmake"
	echo "\tmake install"
	echo ""
endif
