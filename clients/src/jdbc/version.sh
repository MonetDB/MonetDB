#!/bin/sh

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

if [ "$1" == "" ];
then
	echo "Usage: $0 [-w] <(major|minor|suffix)=newversion> [...]"
	echo "where -w activates actual write of changes"
	exit -1
fi

PROPERTIES='build.properties'

CUR_MAJOR=`grep -E 'JDBC_MAJOR=[0-9]+' -o ${PROPERTIES} | cut -d'=' -f2`
CUR_MINOR=`grep -E 'JDBC_MINOR=[0-9]+' -o ${PROPERTIES} | cut -d'=' -f2`
CUR_SUFFIX=`grep 'JDBC_VER_SUFFIX' ${PROPERTIES} | cut -d'=' -f2`

NEW_MAJOR=${CUR_MAJOR}
NEW_MINOR=${CUR_MINOR}
NEW_SUFFIX=${CUR_SUFFIX}

ESC_MAJOR=`echo "$CUR_MAJOR" | sed -e 's/\+/\\\\+/g' | sed -e 's/\*/\\\\*/g' | sed -e 's/\./\\\\./g'`
ESC_MINOR=`echo "$CUR_MINOR" | sed -e 's/\+/\\\\+/g' | sed -e 's/\*/\\\\*/g' | sed -e 's/\./\\\\./g'`
ESC_SUFFIX=`echo "$CUR_SUFFIX" | sed -e 's/\+/\\\\+/g' | sed -e 's/\*/\\\\*/g' | sed -e 's/\./\\\\./g'`

patch="cat"

for param in $*;
do
	arg=`echo ${param} | cut -d'=' -f1`
	val=`echo ${param} | cut -d'=' -f2`
	num=`echo ${val} | grep -E '[0-9]+' -o | head -n1`
	case ${arg} in
	major)
		if [ "${num}" = "" ];
		then
			echo "major needs a numeric argument!";
			exit -1
		fi
		NEW_MAJOR=${num}
		;;
	minor)
		if [ "${num}" = "" ];
		then
			echo "minor needs a numeric argument!";
			exit -1
		fi
		NEW_MINOR=${num}
		;;
	suffix)
		NEW_SUFFIX=${val}
		;;
	-w)
		patch="patch -p0";
		;;
	esac
done

echo "Current version: ${CUR_MAJOR}.${CUR_MINOR} (${CUR_SUFFIX})"
echo "New version:     ${NEW_MAJOR}.${NEW_MINOR} (${NEW_SUFFIX})"

diff="diff -Naur"

file="release.txt"
sed \
	-e "s/version ${ESC_MAJOR}\.${ESC_MINOR} (${ESC_SUFFIX})/version ${NEW_MAJOR}.${NEW_MINOR} \(${NEW_SUFFIX}\)/g" \
	-e "s/Release date: 20[0-9][0-9]-[01][0-9]-[0-3][0-9]/Release date: `date +%F`/" \
	${file} | ${diff} ${file} - | ${patch}

file="Makefile.ag"
sed \
	-e "s/monetdb-${ESC_MAJOR}\.${ESC_MINOR}-jdbc\.jar/monetdb-${NEW_MAJOR}.${NEW_MINOR}-jdbc.jar/g" \
	-e "s/jdbcclient-${ESC_MAJOR}\.${ESC_MINOR}\.jar/jdbcclient-${NEW_MAJOR}.${NEW_MINOR}.jar/g" \
	${file} | ${diff} ${file} - | ${patch}

file="build.properties"
sed \
	-e "s/JDBC_MAJOR=${ESC_MAJOR}/JDBC_MAJOR=${NEW_MAJOR}/g" \
	-e "s/JDBC_MINOR=${ESC_MINOR}/JDBC_MINOR=${NEW_MINOR}/g" \
	-e "s/JDBC_VER_SUFFIX=${ESC_SUFFIX}/JDBC_VER_SUFFIX=${NEW_SUFFIX}/g" \
	${file} | ${diff} ${file} - | ${patch}
