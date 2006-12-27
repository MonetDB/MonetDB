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
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

# "Meta-configure[.ac]" for buildtools

THIS="$0"

SRC="$PWD"
if [ "${THIS#/}" != "$THIS" ] ; then
	SRC="${THIS%/*}"
elif [ "${THIS%/*}" != "$THIS" ] ; then
	SRC="$PWD/${THIS%/*}"
fi
BUILD="$PWD"
PREFIX="/usr/local"
for i; do
    case "$i" in
    --prefix=*)
	PREFIX="${i#--prefix=}"
	;;
    esac
done

# check for python
if [ ! "`type -p python`" ] ; then
	echo "'python' is required to compile 'autogen',"
	echo "but no 'python' is found in your PATH ($PATH)."
	echo
	echo "Please make sure that 'python' is found in your PATH and re-try."
	echo "Aborting $0."
	exit 1
fi

# Create "Meta-Makefile" ...
cat $SRC/Makefile.in \
 | sed -e 's|@SRC@|'"$SRC"'|g' \
       -e 's|@BUILD@|'"$BUILD"'|g' \
       -e 's|@PREFIX@|'"$PREFIX"'|g' \
 > Makefile || exit 1

# Copy python cruft to the build dir, so it can do it's thing, since it
# stubbornly persists on polluting the src tree.
cp -R "${SRC}"/autogen "${BUILD}"/_autogen

# ... and run individual "configure"s
( test -d _MX || mkdir _MX ) && ( cd _MX && $SRC/Mx/configure "$@" ) || exit 1
( test -d _MEL || mkdir _MEL ) && ( cd _MEL && $SRC/mel/configure "$@" --with-mx=$BUILD/_MX/Mx ) || exit 1
( test -d _BURG || mkdir _BURG ) && ( cd _BURG && $SRC/burg/configure "$@" ) || exit 1
