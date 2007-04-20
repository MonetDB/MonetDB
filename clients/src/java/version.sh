#!/usr/bin/env bash

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
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

if [[ -z $1 ]] ; then
	echo "Usage: $0 [-w] <(major|minor|suffix|mmajor|mminor)=newversion> [...]"
	echo "where -w activates actual write of changes"
	exit -1
fi

PROPERTIES='build.properties'

get_value() {
	local tmp=$(grep -E "^$*=" ${PROPERTIES})
	echo ${tmp#*=}
}

escape_value() {
	echo "$*" | sed -e 's/\+/\\\\+/g' | sed -e 's/\*/\\\\*/g' | sed -e 's/\./\\\\./g'
}

MCL_MAJOR=$(get_value 'MCL_MAJOR')
MCL_MINOR=$(get_value 'MCL_MINOR')

CUR_MAJOR=$(get_value 'JDBC_MAJOR')
CUR_MINOR=$(get_value 'JDBC_MINOR')
CUR_SUFFIX=$(get_value 'JDBC_VER_SUFFIX')

NEW_MAJOR=${CUR_MAJOR}
NEW_MINOR=${CUR_MINOR}
NEW_SUFFIX=${CUR_SUFFIX}

ESC_MMAJOR=$(escape_value ${MCL_MAJOR})
ESC_MMINOR=$(escape_value ${MCL_MINOR})

ESC_MAJOR=$(escape_value ${CUR_MAJOR})
ESC_MINOR=$(escape_value ${CUR_MINOR})
ESC_SUFFIX=$(escape_value ${CUR_SUFFIX})

patch="cat"

for param in $* ; do
	arg=${param%%=*}
	val=${param#*=}
	num=$(echo ${val} | grep -E '[0-9]+' -o | head -n1)
	case ${arg} in
	major)
		if [[ -z ${num} ]] ; then
			echo "major needs a numeric argument!";
			exit -1
		fi
		NEW_MAJOR=${num}
		;;
	minor)
		if [[ -z ${num} ]] ; then
			echo "minor needs a numeric argument!";
			exit -1
		fi
		NEW_MINOR=${num}
		;;
	mmajor)
		if [[ -z ${num} ]] ; then
			echo "mmajor needs a numeric argument!";
			exit -1
		fi
		MCL_MAJOR=${num}
		;;
	mminor)
		if [[ -z ${num} ]] ; then
			echo "mminor needs a numeric argument!";
			exit -1
		fi
		MCL_MINOR=${num}
		;;
	suffix)
		NEW_SUFFIX=${val}
		;;
	-w)
		patch="patch -p0";
		;;
	esac
done

[[ ${NEW_SUFFIX} == ${CUR_SUFFIX} ]] && \
	NEW_SUFFIX=${NEW_SUFFIX}/MCL-${MCL_MAJOR}.${MCL_MINOR}

echo "Current version: ${CUR_MAJOR}.${CUR_MINOR} (${CUR_SUFFIX})"
echo "New version:     ${NEW_MAJOR}.${NEW_MINOR} (${NEW_SUFFIX})"

diff="diff -Naur"

file="release.txt"
sed \
	-e "s|version ${ESC_MAJOR}\.${ESC_MINOR} (${ESC_SUFFIX})|version ${NEW_MAJOR}.${NEW_MINOR} \(${NEW_SUFFIX}\)|g" \
	-e "s|Release date: 20[0-9][0-9]-[01][0-9]-[0-3][0-9]|Release date: `date +%F`|" \
	${file} | ${diff} ${file} - | ${patch}

file="Makefile.ag"
sed \
	-e "s|monetdb-${ESC_MMAJOR}\.${ESC_MMINOR}-jdbc\.jar|monetdb-${MCL_MAJOR}.${MCL_MINOR}-mcl.jar|g" \
	-e "s|monetdb-${ESC_MAJOR}\.${ESC_MINOR}-jdbc\.jar|monetdb-${NEW_MAJOR}.${NEW_MINOR}-jdbc.jar|g" \
	-e "s|jdbcclient-${ESC_MAJOR}\.${ESC_MINOR}\.jar|jdbcclient-${NEW_MAJOR}.${NEW_MINOR}.jar|g" \
	${file} | ${diff} ${file} - | ${patch}

file="build.properties"
sed \
	-e "s|MCL_MAJOR=${ESC_MMAJOR}|MCL_MAJOR=${MCL_MAJOR}|g" \
	-e "s|MCL_MINOR=${ESC_MMINOR}|MCL_MINOR=${MCL_MINOR}|g" \
	-e "s|JDBC_MAJOR=${ESC_MAJOR}|JDBC_MAJOR=${NEW_MAJOR}|g" \
	-e "s|JDBC_MINOR=${ESC_MINOR}|JDBC_MINOR=${NEW_MINOR}|g" \
	-e "s|JDBC_VER_SUFFIX=${ESC_SUFFIX}|JDBC_VER_SUFFIX=${NEW_SUFFIX}|g" \
	${file} | ${diff} ${file} - | ${patch}
