# The contents of this file are subject to the Pathfinder Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
# the License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the Pathfinder system.
#
# The Original Code has initially been developed by the Database &
# Information Systems Group at the University of Konstanz, Germany and
# is now maintained by the Database Systems Group at the Technische
# Universitaet Muenchen, Germany.  Portions created by the University of
# Konstanz and the Technische Universitaet Muenchen are Copyright (C)
# 2000-2005 University of Konstanz and (C) 2005-2007 Technische
# Universitaet Muenchen, respectively.  All Rights Reserved.

# CWI & Pathfinder specific additional package settings

acoi_soft="/ufs/acoi/local"

xml=""

for d in /Users/monet/soft/local /usr /usr/local ${softpath} ${acoi_soft}/${os}/${BITS} \
		`ls -d /soft/${BITS}/libxml2* 2>/dev/null | tail -n1` ; do
	if [ ! "${xml}"  -a  -x "${d}/bin/xml2-config"  -a  "${host%.ins.cwi.nl}${d}" != "titan/usr" ] ; then
		xml="${d}"
		break
	fi
done

if [ "${xml}" ] ; then
	conf_opts="${conf_opts} --with-libxml2=${xml}"
	if [ -d "${xml}/lib64" ]
	  then	xmllib="${xml}/lib64"
	  else	xmllib="${xml}/lib"
	fi
	if [ "${xml#/usr}" = "${xml}" ] ; then
		binpath="${xml}/bin:${binpath}"
		libpath="${xmllib}:${libpath}"
	  elif [ "${xml}" != "/usr" ] ; then
		binpath="${binpath}:${xml}/bin"
		libpath="${libpath}:${xmllib}"
	fi
fi

for d in "${MONETDB_PREFIX}" "${PATHFINDER_PREFIX}" "${softpath}" "${xml}" ; do
	ddd="${d}/lib${BITS}/pkgconfig"
	dd="${d}/lib/pkgconfig"
	if [ "${d}"  -a  -d "${ddd}" ] ; then
		export PKG_CONFIG_PATH="${ddd}:${PKG_CONFIG_PATH}"
	elif [ "${d}"  -a  -d "${dd}" ] ; then
		export PKG_CONFIG_PATH="${dd}:${PKG_CONFIG_PATH}"
	fi
done

