#!/usr/bin/env bash
# "Meta-configure[.ac]" for buildtools

THIS="$0"

SRC="$PWD"
if [ "${THIS#/}" != "$THIS" ] ; then
	SRC="${THIS%/*}"
elif [ "${THIS%/*}" != "$THIS" ] ; then
	SRC="$PWD/${THIS%/*}"
fi
BUILD="$PWD"
PREFIX="/usr"
for i; do
    case "$i" in
    --prefix=*)
	PREFIX="${i#--prefix=}"
	break
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

set -x

# Create "Meta-Makefile" ...
cat $SRC/Makefile.in \
 | sed -e 's|@SRC@|'"$SRC"'|g' \
       -e 's|@BUILD@|'"$BUILD"'|g' \
       -e 's|@PREFIX@|'"$PREFIX"'|g' \
 > Makefile || exit 1

# ... and run individual "configure"s
( test -d _MX || mkdir _MX ) && ( cd _MX && $SRC/Mx/configure "$@" ) || exit 1
( test -d _MEL || mkdir _MEL ) && ( cd _MEL && $SRC/mel/configure "$@" --with-mx=$BUILD/_MX/Mx ) || exit 1
( test -d _BURG || mkdir _BURG ) && ( cd _BURG && $SRC/burg/configure "$@" ) || exit 1
