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
set prefix = "${PWD}"
set conf_opts = "--enable-shared --disable-static"

if ( ${?PATH} ) then
	set binpath = ":${PATH}"
  else	set binpath = ""
endif
if ( ${?LD_LIBRARY_PATH} ) then
	set libpath = ":${LD_LIBRARY_PATH}"
  else	set libpath = ""
endif

if ( ! ${?COMP} ) then
	echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
	echo 'Using COMP=GNU (default).'
	set COMP = "GNU"
endif
if ( ! ${?BITS} ) then
	echo 'BITS not set to either "32" or "64" to select the desired binary type.'
	echo 'Using BITS=32 (default).'
	set BITS = "32"
endif

if ( "${COMP}" != "GNU"  &&  "${COMP}" != "ntv" ) then
	echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
	echo 'Using COMP=GNU (default).'
	set COMP = "GNU"
endif
if ( "${BITS}" != "32"   &&  "${BITS}" != "64" ) then
	echo 'BITS not set to either "32" or "64" to select the desired binary type.'
	echo 'Using BITS=32 (default).'
	set BITS = "32"
endif

if ( "${os}" == "Linux" ) then
	if ( "${COMP}" == "ntv" ) then
		echo 'Linux'\''s "native" compilier is "GNU"; hence, using COMP=GNU.'
		set COMP = "GNU"
	endif
	if ( "${BITS}" == "64" ) then
		echo 'Linux doesn'\''t support 64 bit, yep; hence, using BITS=32.'
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
	set binpath = ":/var/tmp/local/bin:/opt/SUNWspro/bin:/usr/local/bin${binpath}"
	set libpath = ":/usr/local/lib${libpath}"
	if ( "${BITS}" == "64" ) then
		set libpath = ":/usr/lib/sparcv9:/usr/ucblib/sparcv9${libpath}"
	endif
	if ( "${COMP}" == "GNU" ) then
		set cc = "${cc} -m$BITS"
		set cxx = "${cxx} -m$BITS"
	endif
	if ( "${COMP}${BITS}" == "ntv64" ) then
		set cc = "${cc} -xarch=v9"
		set cxx = "${cxx} -xarch=v9"
	endif
endif

if ( "${os}" == "IRIX64" ) then
	set binpath = ":/dbfarm/local/bin:/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/local/bin:/usr/java/bin${binpath}"
	if ( "${COMP}${BITS}" == "GNU64" ) then
		set cc = "${cc} -mabi=64"
		set cxx = "${cxx} -mabi=64"
	endif
	if ( "${COMP}${BITS}" == "ntv64" ) then
		set cc = "${cc} -64"
		set cxx = "${cxx} -64"
	endif
	set conf_opts = "${conf_opts} --with-readline=/ufs/monet"
endif

setenv CC "${cc}"
setenv CXX "${cxx}"
setenv PATH "${prefix}/bin${binpath}"
setenv LD_LIBRARY_PATH "${prefix}/lib:${prefix}/lib/Monet${libpath}"

setenv MONETDIST "${prefix}"
setenv MONET_MOD_PATH "${prefix}/lib:${prefix}/lib/Monet"

setenv CONFIGURE "configure ${conf_opts} --prefix=${prefix}"

echo ""
echo "To configure Monet, just execute:"
echo -e "\t${CONFIGURE}"
echo ""

