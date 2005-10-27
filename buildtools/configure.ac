#!/usr/bin/env bash
# "Meta-configure[.ac]" for buildtools

THIS="$0"
ARGS="$*"

SRC="$PWD"
if [ "${THIS%/*}" != "$THIS" ] ; then
	SRC="${THIS%/*}"
fi
BUILD="$PWD"
PREFIX="/usr"
while [ "$1" ] ; do
	a="$1"
	p="${a#--prefix=}"
	if [ "$p" != "$a" ] ; then
		PREFIX="$p"
	fi
	shift
done

set -x

# Create "Meta-Makefile" ...
cat $SRC/Makefile.in \
 | sed -e 's|@SRC@|'"$SRC"'|g' \
       -e 's|@BUILD@|'"$BUILD"'|g' \
       -e 's|@PREFIX@|'"$PREFIX"'|g' \
 > Makefile || exit 1

# ... and run individual "configure"s
mkdir MX && ( cd MX && $SRC/Mx/configure $ARGS ) || exit 1
mkdir MEL && ( cd MEL && $SRC/mel/configure $ARGS --with-mx=$BUILD/MX/Mx ) || exit 1
mkdir BURG && ( cd BURG && $SRC/burg/configure $ARGS ) || exit 1

