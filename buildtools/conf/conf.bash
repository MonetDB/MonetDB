# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

# In the following, ${what} is one of monet, sql, xml, acoi. 
# It is automatically derived from the current directory name.
#
# This script is supposed to be "sourced" in the top-level directory of the
# checked-out ${what} source tree (referred to as BASE in the remainder). 
# While sourced, this script sets your (architecture dependent) environment
# as required to compile ${what}.
#
# For everything else but Monet, this script requires MONETDB_PREFIX to be
# set, or 'monetdb-config` to be in your PATH, in order to find your Monet
# installation.
#
# By default, compilation will take place in ${what}_BUILD=BASE/`uname` and ${what}
# will be installed in ${what}_PREFIX=${what}_BUILD. You can change either directory by
# setting the enviroment variables
#	${what}_BUILD
# and/or
#	${what}_PREFIX
# appropiately before "sourcing" this script.
#
# To select your desired compiler ("GNU" or "ntv" (native)), your desired
# binary type (32bit or 64bit), and whether binarys should be linked
# dynamically or statically, set the following environment variables before
# "sourcing" this script:
#
#	COMP="GNU"	or	COMP="ntv"
#	BITS="32"	or	BITS="64"
#	OIDS="32"	or	OIDS="64"
#	LINK="dynamic"	or	LINK="static"
#	DEBUG="somevalue" 	or not set
#	WARNING="somevalue" 	or not set
#	PROFILE="somevalue"	or not set
#	OPTIMIZE="somevalue"	or not set
#	INSTRUMENT="somevalue"	or not set
#
# (If not or wrongly set, "GNU 32 dynamic" is used as default.)
#

base="`pwd`"
# first, try the CVS repository name to guess the value for $what
if [ -f CVS/Repository ]; then
	wh_t="`cat CVS/Repository`"
	wh_t="`basename $wh_t`"
else
	# otherwise, use the current directory's (base)name to guess, 
	# which of monet, sql, xml, acoi, template, ... we are.
	wh_t="`basename $base`"
fi
what="`echo ${wh_t} | tr '[a-z]' '[A-Z]' | tr '.-' '_'`"

if [ "${what}" != "BUILDTOOLS" -a ! -f configure.ag ] ; then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out ${what} source tree.'
	echo ''
	return 1
fi
if [ "${what}" = "BUILDTOOLS" -a ! -f configure.ac ] ; then
	echo ''
	echo 'conf/conf.bash has to be "sourced" in the top-level directory of the checked-out ${what} source tree.'
	echo ''
	return 1
fi

if [ "${what}" != "BUILDTOOLS" ] ; then
	if [ "${what}" = "MONET5" ] ; then
		pkgdir="MonetDB5"
	  else
		pkgdir="MonetDB4"
	fi

	if [ "${what}" != "MONETDB" ] ; then
		if [ ! "${MONETDB_PREFIX}" ] ; then
			MONETDB_PREFIX=`monetdb-config --prefix`
		fi
		if [ ! -x ${MONETDB_PREFIX}/bin/monetdb-config ] ; then
			echo ''
			echo 'Could not find Monet installation.'
			echo ''
			wh_t='' ; unset wh_t
			what='' ; unset what
			return 1
		fi
	fi
fi

# set generic variables
eval WHAT_BUILD="\${${what}_BUILD}"
eval WHAT_PREFIX="\${${what}_PREFIX}"

binpath=""
libpath=""
pytpath=""
modpath=""
mtest_modpath=""
conf_opts=""
libdir="lib"

os="`uname | sed 's|_NT-.*$||'`"
hw="`uname -m`"
host="`hostname`"
domain="`domainname`"

# check for not or incorrectly set variables (${what}_BUILD, ${what}_PREFIX, COMP, BITS, OIDS, LINK)

if [ ! "${WHAT_BUILD}" ] ; then
	echo ''
	echo ${what}'_BUILD not set to specify desired compilation directory.'
	echo 'Using '${what}'_BUILD="'${base}/${os}'" (default).'
	WHAT_BUILD="${base}/${os}"
fi
if [ ! "${WHAT_PREFIX}" ] ; then
	echo ''
	echo ${what}'_PREFIX not set to specify desired target directory.'
	echo 'Using '${what}'_PREFIX="'${WHAT_BUILD}'" (default).'
	WHAT_PREFIX="${WHAT_BUILD}"
fi

if [ "${COMP}" != "GNU"  -a  "${COMP}" != "ntv"  -a  "${os}${COMP}" != "LinuxPGI" ] ; then
	COMP="GNU"
	echo ''
	echo 'COMP not set to either "GNU" or "ntv" (native) to select the desired compiler.'
	echo 'Using COMP="'${COMP}'" (default).'
fi
case "${LINK}" in
d*)	LINK="d";;
s*)	LINK="s";;
*)
	echo ''
	echo 'LINK not set to either "dynamic" or "static" to select the desired way of linking.'
	echo 'Using LINK="dynamic" (default).'
	LINK="d"
	;;
esac

if [ "${BITS}" != "32"   -a  "${BITS}" != "64"  ] ; then
	echo ''
	echo 'BITS not set to either "32" or "64" to select the desired binary type.'
	case "${os}-${hw}" in
	Linux-*64|IRIX64-IP27|SunOS-sun4u)
		BITS="64"
		DFT="default";;
	*)
		case "${OIDS}" in
		32|64)
			BITS="${OIDS}"
			DFT="=OIDS";;
		*)
			BITS="32"
			DFT="default";;
		esac
	esac
	echo 'Using BITS="'${BITS}'" ('${DFT}').'
fi

# exclude "illegal" combinations

if [ "${os}" = "Linux" ] ; then
	if [ "${BITS}" = "64"  -a  "${hw}" != "ia64"  -a  "${hw}" != "x86_64" ] ; then
		echo ''
		echo "${hw}"' is 32-bit, only; hence, using BITS="32".'
		BITS="32"
	  elif [ "${BITS}" = "32"  -a  "${hw}" = "ia64" ] ; then
		echo ''
		echo 'Currently, we do not support 32-bit on '"${hw}"'; hence, using BITS="64".'
		BITS="64"
	  elif [ "${BITS}" = "32"  -a  "${hw}" = "x86_64"  -a  \( -s /etc/redhat-release  -o  -s /etc/fedora-release \) ] ; then
		echo ''
		echo 'Currently, we do not support 32-bit with RedHat/Fedora Linux on '"${hw}"'; hence, using BITS="64".'
		BITS="64"
	fi
fi

if [ "${OIDS}" != "32"   -a  "${OIDS}" != "64"  ] ; then
	OIDS="${BITS}"
	echo ''
	echo 'OIDS not set to either "32" or "64" to select the desired binary type.'
	echo 'Using OIDS="'${OIDS}'" (=BITS).'
fi
if [ "${BITS}${OIDS}" == "3264" ] ; then
	echo ''
	echo 'Using 64-bit OIDS with 32-bit compilation is not possible.'
	echo 'Using OIDS="'${BITS}'", instead.'
	OIDS="${BITS}"
fi

# set default compilers, configure options & paths

cc=''
cxx=''
if [ "${LINK}" = "d"   ] ; then
	# dynamic/shared linking
	conf_opts="${conf_opts} --enable-shared --disable-static"
  else
	# static linking
	conf_opts="${conf_opts} --disable-shared --enable-static"
fi
# "standard" local paths
if [ -d /usr/local ] ; then
	binpath="/usr/local/bin:${binpath}"
	libpath="/usr/local/lib:${libpath}"
fi
# "our" /soft[64] path
soft32=/soft/local
soft64=/soft64/local
if [ "${BITS}" = "32"  -o  "${os}" = "Linux" ] ; then
	softpath=${soft32}
  else
	softpath=${soft64}
fi
# "our" libdir
if [ "${os}${BITS}" = "Linux64" ] ; then
	libdir=lib64
  else
  	libdir=lib
fi

# (additional) system-specific settings

if [ "${os}" = "Linux" ] ; then
	if [ "${COMP}" = "ntv"  -a  -d "${softpath}" ] ; then
		# the Intel compiler doesn't find headers/libs in /usr/local without help
		case ${what} in
		MONETDB)
			conf_opts="${conf_opts} --with-hwcounters=${softpath}"
			conf_opts="${conf_opts} --with-pcl=${softpath}"
			;;
		esac
	fi
	if [ "${COMP}" = "ntv" ] ; then
		d="`ls -d /soft/${BITS}/icc-* 2>/dev/null | tail -n1`"
		if [ ! "$d" ] ; then
			d="`ls -d /soft/${BITS}/IntelC* 2>/dev/null | tail -n1`"
		fi
		if [ "$d"  -a  -d "$d/bin" ] ; then
			binpath="$d/bin:${binpath}"
		fi
		if [ "$d"  -a  -d "$d/lib" ] ; then
			libpath="$d/lib:${libpath}"
		fi
		if [ "$d"  -a  -d "$d/lib${BITS}" ] ; then
			libpath="$d/lib${BITS}:${libpath}"
		fi
	fi
	if [ "${hw}${COMP}${BITS}${host%-*}${domain}" = "x86_64ntv64singlebeo-cluster" ] ; then
		# the combination of icc 8.1 & libtool 1.5.14 fail to find/use
		# the proper libbz2 in /usr/lib64/ on our Athlon64 running SuSE 9.3
		conf_opts="${conf_opts} --with-bz2=no"
	fi
	if [ "${COMP}" = "PGI" ] ; then
		# Portland Group compiler on spin
		cc='pgcc'
		cxx='pgCC'
	fi
	if [ "${hw}" = "ia64" ] ; then
		if [ "${host%.ins.cwi.nl}" = "titan" ] ; then
			# specific settings for our Itanium2 "titan" system
			binpath="/soft/python-2.3/bin:/soft/automake-1.5/bin:${binpath}"
			if [ "${COMP}" = "GNU" ] ; then
				binpath="/soft/gcc-3.3/bin:${binpath}"
				libpath="/soft/gcc-3.3/lib:${libpath}"
			fi
		  elif [ "${host%.sara.nl}" = "theo" ] ; then
			# specific settings for Sara's Itanium system
			binpath="/home/niels/soft/local/bin:${binpath}"
			libpath="/home/niels/soft/local/lib:${libpath}"
		fi
	fi
	# Java support on Gentoo systems
	if [ -x /usr/bin/java-config ]; then
		binpath="`/usr/bin/java-config -O`/bin:${binpath}"
	fi
	if [ -x /net/lin_local/java/j2sdk1.4.2/bin/javac  -a  -x /net/lin_local/java/j2sdk1.4.2/bin/jar ] ; then
		# java in Konstanz
		binpath="/net/lin_local/java/j2sdk1.4.2/bin:${binpath}"
	fi
	if [ -x /usr/java/jdk1.5.0_09/bin/javac -a -x /usr/java/jdk1.5.0_09/bin/jar ]; then
		# java in muenchen
		binpath="/usr/java/jdk1.5.0_09/bin:${binpath}"
	fi
	if [ -x /usr/local/bin/swig ]; then
		conf_opts="${conf_opts} --with-swig=/usr/local/bin/swig"
	fi
	if [ -x /var/tmp/soft/local/bin/swig ]; then
		conf_opts="${conf_opts} --with-swig=/var/tmp/soft/local/bin/swig"
	fi
	if [ -s /etc/debian_version -a -x /soft/${BITS}/flex-2.5.33/bin/flex ] ; then
		# Debian's default flex 2.5.31 does not like the Pathfinder code;
		# hence, we use our own flex 2.5.33 instead.
		binpath="/soft/${BITS}/flex-2.5.33/bin:${binpath}"
	fi
fi

if [ "${os}" = "CYGWIN" ] ; then
	if [ -x /cygdrive/c/j2ee14/jdk/bin/javac  -a  -x /cygdrive/c/j2ee14/jdk/bin/jar ] ; then
		# java on winxp
		binpath="/cygdrive/c/j2ee14/jdk/bin:${binpath}"
	fi
	if [ -x /cygdrive/c/Sun/AppServer/jdk/bin/javac  -a  -x /cygdrive/c/Sun/AppServer/jdk/bin/jar ] ; then
		# java on winxp01
		binpath="/cygdrive/c/Sun/AppServer/jdk/bin:${binpath}"
	fi
fi

if [ "${os}" = "Darwin" ] ; then
	# "our" autoconf on sap & monet
	mypath=""
	for d in "/Users/manegold/soft/local/bin" "/Users/monet/soft/local/bin" "/usr/local/bin" "/sw/bin" "/usr/bin" ; do
		if [ -d ${d} ] ; then
			mypath="${mypath}${d}:"
		fi
	done
	binpath="${mypath}:${binpath}"
	unset mypath
	if [ "${BITS}" = 64 ] ; then
		if [ -f /Users/monet/soft/local/lib/libz.dylib ] ; then
			conf_opts="${conf_opts} --with-z=/Users/monet/soft/local"
		fi
		if [ -f /Users/monet/soft/local/lib/libiconv.dylib ] ; then
			conf_opts="${conf_opts} --with-iconv=/Users/monet/soft/local"
		fi
	fi
fi

if [ "${os}" = "SunOS" -a "${hw}" = "i86pc" ] ; then
	# pkg-get software 
	softpath="/opt/csw"
	binpath="$softpath/bin:${binpath}"
	libpath="$softpath/lib:${libpath}"
	# ld & ar
	binpath="/opt/sfw/lib/bin:${binpath}"
	# php 5.1.2 & gcc 3.4.5
	binpath="/opt/csw/php5/bin:/opt/csw/gcc3/bin:${binpath}"
fi

if [ "${os}" = "SunOS" -a "${hw}" != "i86pc" ] ; then
	# "our" /soft[64] path on apps
	soft32="/var/tmp${soft32}"
	soft64="/var/tmp${soft64}"
	softpath="/var/tmp${softpath}"
	# "standard" SunOS paths
	binpath="/opt/SUNWspro/bin:/cwi/bin:/usr/java/bin:/usr/ccs/bin:${binpath}"
	libpath="/cwi/lib:${libpath}"
	if [ -x /var/tmp/soft/j2ee_sdk-1.4/jdk/bin/javac  -a  -x /var/tmp/soft/j2ee_sdk-1.4/jdk/bin/jar ] ; then
		# java 1.4 on apps
		binpath="/var/tmp/soft/j2ee_sdk-1.4/jdk/bin:${binpath}"
	fi
	if [ "${BITS}" = "64" ] ; then
		# propper/extended LD_LIBRARY_PATH for 64bit on SunOS
		libpath="/usr/lib/sparcv9:/usr/ucblib/sparcv9:${libpath}"
		if [ -d "${soft32}" ] ; then
			# libraries compiled with gcc may need the gcc libs, so
			# at them to the LD_LIBRARY_PATH 
			libpath="${soft32}/lib/sparcv9:${soft32}/lib:${libpath}"
			# some tools are not in ${soft64} on apps
			binpath="${soft32}/bin:${binpath}"
		fi
	fi
	if [ "${COMP}${BITS}${LINK}" = "ntv32d" ] ; then
		# propper/extended LD_LIBRARY_PATH for native 32bit shared libs on SunOS
		libpath="/usr/ucblib:${libpath}"
	fi
fi

if [ "${os}" = "IRIX64" ] ; then
	# propper/extended paths on medusa
	binpath="/usr/local/egcs/bin:/usr/local/gnu/bin:/usr/java/bin:/usr/local/teTeX/bin:${binpath}"
	if [ -d "${soft32}"  -a  "${BITS}" = "64" ] ; then
		# some tools are not in ${soft64} on medusa
		binpath="${soft32}/bin:${binpath}"
		libpath="${soft32}/lib:${libpath}"
	fi
	if [ -d "${soft32}"  -a  "${COMP}${BITS}" = "GNU64" ] ; then
		# our gcc/g++ on medusa is in ${soft32} (also for 64 bit)
		libpath="${soft32}/lib/mabi=64:${libpath}"
	fi
fi

if [ "${os}" = "AIX" ] ; then
	# paths on beluga & solo
	mypath=""
	for d in "/san/sara/nielsnes/soft/local/bin" "/usr/java131/jre/bin" "/usr/java131/bin" "/usr/java130/jre/bin" "/usr/java130/bin" "/opt/freeware/bin" "/usr/bin" "/usr/local/bin" "/usr/local/tgcc-2.95.3/bin" "/usr/vac/bin" "/usr/vacpp/bin" "/usr/ccs/bin" "/usr/ucb" "/usr/dt/bin" "/usr/bin/X11" "/usr/lpp/X11/bin" ; do
		if [ -d ${d} ] ; then
			mypath="${mypath}${d}:"
		fi
	done
	binpath="${mypath}:${binpath}"
	unset mypath
	# required to make mmap() work on AIX !
	export XPG_SUS_ENV=ON
fi

## gathered from old scripts, but not used anymore/yet
#if [ "${os}" = "CYGWIN32_NT" ] ; then
#	# yalite.ddi.nl
#	# gcc/g++ only!
#	cc="${cc} -mno-cygwin"   # ?
#	cxx="${cxx} -mno-cygwin" # ?
#	conf_opts="${conf_opts} --with-pthread=/${what}DS/PthreadsWin32"
#fi
#if [ "${os}" = "CYGWIN_NT-4.0" ] ; then
#	# VMware
#	# gcc/g++ only!
#	cc="${cc} -mno-cygwin"   # ?
#	cxx="${cxx} -mno-cygwin" # ?
#	conf_opts="${conf_opts} --with-pthread=/tmp"
#fi

if [ "${os}" != "Linux"  -a  "${os}" != "CYGWIN"  -a  "${os}" != "Darwin" ] ; then
	# on Linux, CYGWIN, & Darwin, /soft/local is identical with /usr/local
	if [ -d "${softpath}" ] ; then
		# prepend ${softpath} to ${binpath} & ${libpath}
		binpath="${softpath}/bin:${binpath}"
		libpath="${softpath}/lib:${libpath}"
		# "our" libs/tools in ${softpath}
		conf_opts="${conf_opts} --with-readline=${softpath}"
		conf_opts="${conf_opts} --with-z=${softpath}"
		conf_opts="${conf_opts} --with-bz2=${softpath}"
		case ${what} in
		ACOI)
			conf_opts="${conf_opts} --with-getopt=${softpath}"
			conf_opts="${conf_opts} --with-tcl=${softpath}"
			;;
		SQL)
			conf_opts="${conf_opts} --with-odbc=${softpath}"
			;;
		XML)
			conf_opts="${conf_opts} --with-expat=${softpath}"
			conf_opts="${conf_opts} --with-pcre=${softpath}"
			;;
		esac
	fi
fi

# CWI specific additional package settings
if [ -f conf/local.bash ]; then
	source conf/local.bash
fi

# tell configure about chosen compiler and bits
if [ "${what}" != "BUILDTOOLS" ] ; then
	if [ "${cc}" ] ; then
		conf_opts="${conf_opts} CC='${cc}'"
	fi
	if [ "${cxx}" ] ; then
		conf_opts="${conf_opts} CXX='${cxx}'"
	fi
	if [ "${cc}${cxx}${COMP}" = "ntv" ] ; then
		case "${os}" in
		Linux)	conf_opts="${conf_opts} CC='icc' CXX='icpc'";;
		AIX)	conf_opts="${conf_opts} CC='xlc_r' CXX='xlc_r'";;
		*)	conf_opts="${conf_opts} CC='cc' CXX='CC'";;
		esac
	fi
fi
conf_opts="${conf_opts} --enable-bits=${BITS}"
if [ "${BITS}${OIDS}" = "6432" ] ; then
	conf_opts="${conf_opts} --enable-oid32"
fi

if [ "${DEBUG}" ] ; then
	conf_opts="${conf_opts} --enable-debug"
fi

if [ "${WARNING}" ] ; then
	conf_opts="${conf_opts} --enable-warning"
fi

if [ "${PROFILE}" ] ; then
	conf_opts="${conf_opts} --enable-profile"
fi

if [ "${OPTIMIZE}" ] ; then
	conf_opts="${conf_opts} --enable-optimize"
fi

if [ "${INSTRUMENT}" ] ; then
	conf_opts="${conf_opts} --enable-instrument"
fi

if [ "${what}" != "BUILDTOOLS"  -a  "${what}" != "MONETDB" ] ; then
	# tell configure where to find MonetDB
	conf_opts="${conf_opts} --with-monet=${MONETDB_PREFIX}"
	if [ "${what}" != "MONET5"  -a  "${MONET5_PREFIX}" ] ; then
		# tell configure where to find MonetDB5
		conf_opts="${conf_opts} --with-monet5=${MONET5_PREFIX}"
	fi
fi

# prepend target bin-dir to PATH
binpath="${WHAT_PREFIX}/bin:${binpath}"

if [ "${what}" != "BUILDTOOLS" ] ; then
	# the following is nolonger needed for Monet,
	# but still needed for the rest:
	if [ "${what}" != "MONETDB"  -a  "${WHAT_PREFIX}" != "${MONETDB_PREFIX}" ] ; then
		# set MONETDB_MOD_PATH and prepend it to LD_LIBRARY_PATH
		modpath="${WHAT_PREFIX}/${libdir}/${pkgdir}:${WHAT_PREFIX}/${libdir}/${pkgdir}/lib"
		libpath="${WHAT_PREFIX}/${libdir}:${WHAT_PREFIX}/${libdir}/${pkgdir}/lib:${libpath}"
		if [ "${what}" != "MONET5" ] ; then
			mtest_modpath="--monet_mod_path=${modpath}:`${MONETDB4_PREFIX}/bin/monetdb4-config --modpath`"
		fi
	fi
	if [ "${os}" = "CYGWIN" ] ; then
		# CYGWIN finds dlls using the PATH variable 
		if [ "${what}" = "MONETDB" ] ; then
			binpath="${WHAT_PREFIX}/${libdir}/${pkgdir}/bin:${binpath}"
		fi
	fi
	if [ "${os}" = "IRIX64" ] ; then
		# IRIX64 requires this to find dependend modules
		if [ "${what}" = "MONETDB" ] ; then
			libpath="${WHAT_PREFIX}/${libdir}/${pkgdir}/lib:${libpath}"
		  else
			libpath="${MONETDB_PREFIX}/${libdir}/${pkgdir}/lib:${libpath}"
		fi
	fi
	if [ "${os}${COMP}${BITS}" = "SunOSntv64" ] ; then
		# native 64-bit version on SunOS needs this to find libmonet
		if [ "${what}" = "MONETDB" ] ; then
			libpath="${WHAT_PREFIX}/${libdir}:${libpath}"
		  else
			libpath="${MONETDB_PREFIX}/${libdir}:${libpath}"
		fi
	fi
  else
	if [ "`type -p python`" ] ; then
		pytpath="${WHAT_PREFIX}/`python -c 'import distutils.sysconfig; print distutils.sysconfig.get_python_lib(0,0,"")'`"
	fi
fi

# remove trailing ':'
binpath=`echo "${binpath}" | sed 's|:$||'`
libpath=`echo "${libpath}" | sed 's|:$||'`
modpath=`echo "${modpath}" | sed 's|:$||'`

# export new settings
echo ""
echo "Setting..."
if [ "${binpath}" ] ; then
	if [ "${PATH}" ] ; then
		# prepend new binpath to existing PATH, if PATH doesn't contain binpath, yet
		case ":${PATH}:" in
		*:${binpath}:*)
			;;
		*)
			PATH="${binpath}:${PATH}" ; export PATH
			;;
		esac
	  else
		# set PATH as binpath
		PATH="${binpath}" ; export PATH
	fi
	echo " PATH=${PATH}"
fi
if [ "${libpath}" ] ; then
	if [ "${LD_LIBRARY_PATH}" ] ; then
		# prepend new libpath to existing LD_LIBRARY_PATH, if LD_LIBRARY_PATH doesn't contain libpath, yet
		case ":${LD_LIBRARY_PATH}:" in
		*:${libpath}:*)
			;;
		*)
			LD_LIBRARY_PATH="${libpath}:${LD_LIBRARY_PATH}" ; export LD_LIBRARY_PATH
			;;
		esac
	  else
		# set LD_LIBRARY_PATH as libpath
		LD_LIBRARY_PATH="${libpath}" ; export LD_LIBRARY_PATH
	fi
	echo " LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
	dylibpath="`echo "${libpath}" | perl -pe 's|:?/usr/local/lib$||'`"
	if [ "${os}" = "Darwin" ] ; then
		if [ "${DYLD_LIBRARY_PATH}" ] ; then
			# prepend new dylibpath to existing DYLD_LIBRARY_PATH, if DYLD_LIBRARY_PATH doesn't contain dylibpath, yet
			case ":${DYLD_LIBRARY_PATH}:" in
			*:${dylibpath}:*)
				;;
			*)
				DYLD_LIBRARY_PATH="${dylibpath}:${DYLD_LIBRARY_PATH}" ; export DYLD_LIBRARY_PATH
				;;
			esac
		  else
			# set DYLD_LIBRARY_PATH as dylibpath
			DYLD_LIBRARY_PATH="${dylibpath}" ; export DYLD_LIBRARY_PATH
		fi
		echo " DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}"
	fi
	if [ "${os}" = "AIX"  -a  "${host%.ddi.nl}" = "beluga" ] ; then
		if [ "${LIBPATH}" ] ; then
			# prepend new libpath to existing LIBPATH, if LIBPATH doesn't contain libpath, yet
			case ":${LIBPATH}:" in
			*:${libpath}:*)
				;;
			*)
				LIBPATH="${libpath}:${LIBPATH}" ; export LIBPATH
				;;
			esac
		  else
			# set LIBPATH as libpath
			LIBPATH="${libpath}" ; export LIBPATH
		fi
		echo " LIBPATH=${LIBPATH}"
	fi
fi
if [ "${pytpath}" ] ; then
	if [ "${PYTHONPATH}" ] ; then
		# prepend new pytpath to existing PYTHONPATH, if PYTHONPATH doesn't contain pytpath, yet
		case ":${PYTHONPATH}:" in
		*:${pytpath}:*)
			;;
		*)
			PYTHONPATH="${pytpath}:${PYTHONPATH}" ; export PYTHONPATH
			;;
		esac
	  else
		# set PYTHONPATH as pytpath
		PYTHONPATH="${pytpath}" ; export PYTHONPATH
	fi
	echo " PYTHONPATH=${PYTHONPATH}"
fi
if [ "${modpath}" ] ; then
	if [ "${MONETDB_MOD_PATH}" ] ; then
		# prepend new modpath to existing MONETDB_MOD_PATH, if MONETDB_MOD_PATH doesn't contain modpath, yet
		case ":${MONETDB_MOD_PATH}:" in
		*:${modpath}:*)
			;;
		*)
			MONETDB_MOD_PATH="${modpath}:${MONETDB_MOD_PATH}" ; export MONETDB_MOD_PATH
			;;
		esac
	  else
		# set MONETDB_MOD_PATH as modpath
		MONETDB_MOD_PATH="${modpath}" ; export MONETDB_MOD_PATH
	fi
	echo " MONETDB_MOD_PATH=${MONETDB_MOD_PATH}"
fi

if [ "${what}" = "MONET5" ] ; then
	monet5_config="-5 --config=${WHAT_PREFIX}/etc/monetdb5.conf"
  elif [ "${MONET5_PREFIX}" ] ; then
	monet5_config="-5 --config=${MONET5_PREFIX}/etc/monetdb5.conf"
  elif [ "${what}" = "MONETDB4" ] ; then
	monet4_config="-4 --config=${WHAT_PREFIX}/etc/MonetDB.conf"
  else
	monet4_config="-4 --config=${MONETDB4_PREFIX}/etc/MonetDB.conf"
fi

if [ "${what}" != "BUILDTOOLS" ] ; then
	if [ "${what}" = "MONET5" ] ; then
		mtest_config="${monet5_config}"
	  else
		mtest_config="${monet4_config}"
	fi
fi

# for convenience: store the complete configure-call in ${what}_CONFIGURE
WHAT_CONFIGURE="${base}/configure ${conf_opts} --prefix=${WHAT_PREFIX}"
if [ "${os}${BITS}" = "Linux64" ] ; then
	WHAT_CONFIGURE="$WHAT_CONFIGURE --libdir='"'${prefix}'"/${libdir}'"
fi
echo " ${what}_CONFIGURE=${WHAT_CONFIGURE}"
eval "alias configure_${wh_t}=\"`echo "${WHAT_CONFIGURE}" | sed -e 's|\\$|\\\\\$|g'`\""
eval "alias configure_${wh_t}"
if [ "${what}" != "BUILDTOOLS" ] ; then
	MTEST_WHAT="Mtest.py ${mtest_config} --TSTSRCBASE=${base} --TSTBLDBASE=${WHAT_BUILD} --TSTTRGBASE=${WHAT_PREFIX} ${mtest_modpath}"
	echo " MTEST_${what}=${MTEST_WHAT}"
	eval "MTEST_${what}='${MTEST_WHAT}'; export MTEST_${what}"
	eval "alias Mtest_${wh_t}='${MTEST_WHAT}'"
	eval "alias Mtest_${wh_t}"
	if [ "${what}" = "SQL"  -a  "${MONET5_PREFIX}" ] ; then
		MTEST_WHAT="Mtest.py ${monet5_config} --TSTSRCBASE=${base} --TSTBLDBASE=${WHAT_BUILD} --TSTTRGBASE=${WHAT_PREFIX} --monet_mod_path=${WHAT_PREFIX}/${libdir}/MonetDB5:${WHAT_PREFIX}/${libdir}/MonetDB5/lib:`${MONET5_PREFIX}/bin/monetdb5-config --modpath`"
		echo " MTEST_${what}5=${MTEST_WHAT}"
		eval "MTEST_${what}5='${MTEST_WHAT}'; export MTEST_${what}5"
		eval "alias Mtest_${wh_t}5='${MTEST_WHAT}'"
		eval "alias Mtest_${wh_t}5"
	fi
	MTEST_WHAT='' ; unset MTEST_WHAT
	eval "alias Mapprove_${wh_t}='Mapprove.py ${mtest_config} --TSTSRCBASE=${base} --TSTBLDBASE=${WHAT_BUILD} --TSTTRGBASE=${WHAT_PREFIX}'"
	eval "alias Mapprove_${wh_t}"
	if [ "${what}" = "SQL"  -a  "${MONET5_PREFIX}" ] ; then
		eval "alias Mapprove_${wh_t}5='Mapprove.py ${monet5_config} --TSTSRCBASE=${base} --TSTBLDBASE=${WHAT_BUILD} --TSTTRGBASE=${WHAT_PREFIX}'"
		eval "alias Mapprove_${wh_t}5"
	fi
fi

mkdir -p ${WHAT_BUILD}

cat <<EOF

To compile ${what}, just execute:
	./bootstrap
	cd ${WHAT_BUILD}
	configure_${wh_t}
	make
	make install

Then, to test ${what}, just execute:
	cd ${base}
	Mtest_${wh_t} -r

EOF

# set specific variables
eval "${what}_BUILD=\"$WHAT_BUILD\" ; export ${what}_BUILD"
eval "${what}_PREFIX=\"$WHAT_PREFIX\" ; export ${what}_PREFIX"
eval "${what}_CONFIGURE=\"`echo "$WHAT_CONFIGURE" | sed -e 's|\\$|\\\\$|g'`\" ; export ${what}_CONFIGURE"

# clean-up temporary variables
wh_t='' ; unset wh_t
what='' ; unset what
WHAT_BUILD='' ; unset WHAT_BUILD
WHAT_PREFIX='' ; unset WHAT_PREFIX
WHAT_CONFIGURE='' ; unset WHAT_CONFIGURE

