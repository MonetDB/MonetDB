#!/bin/sh

XQTS_DIR='tests/XQTS'
XQTS_SRC='/var/tmp/XQTS'

if [ ! "$1" ] ; then
	echo 'Usage: '"$0"' <TSTSRCBASE>'
	exit 1
fi

THISFILE="${0##*/}"
THISDIR="${0%/*}"
if [ "$THISDIR" = "$THISFILE" ] ; then
	THISDIR="$PWD"
elif [ "${THISDIR#/}" = "$THISDIR" ] ; then
	THISDIR="$PWD/$THISDIR"
fi
	
TSTSRCBASE="$1"
XQTS_DST="$TSTSRCBASE/$XQTS_DIR"

if [ "$TSTSRCBASE" = "$XQTS_SRC" ] ; then
	XQTS_SRC="${XQTS_SRC}_"
fi

type -p MapiClient || {
	echo 'MapiClient not found!'
	echo 'Please make sure that MapiClient is found in your $PATH.'
	exit 1
}

echo '1+2' | MapiClient -lx -sxml || {
	echo 'MonetDB/XQuery server not running?'
	echo 'Please start it with (e.g.) `Mserver --dbinit="module(pathfinder);"`.'
	exit 1
}

set -x

rm -rf "$XQTS_SRC"
mkdir -p "$XQTS_SRC" || exit 1
(
cd "$XQTS_SRC" || exit 1
wget http://www.w3.org/XML/Query/test-suite/XQTS_0_9_4.zip || exit 1
unzip XQTS_0_9_4.zip >/dev/null || exit 1
) || exit 1

rm -rf "$XQTS_DST"
mkdir -p "$XQTS_DST" || exit 1

cat "$THISDIR/XQTS.xq" \
 | sed -e 's|doc("[^")]*XQTSCatalog.xml")|doc("'"$XQTS_SRC"'/XQTSCatalog.xml")|g' \
       -e 's|doc("[^")]*XQTS.Errors.xml")|doc("'"$THISDIR"'/XQTS.Errors.xml")|g' \
       -e 's|doc("[^")]*XQTS.missing.xml")|doc("'"$THISDIR"'/XQTS.missing.xml")|g' \
       -e 's|doc("[^")]*XQTS.supported.xml")|doc("'"$THISDIR"'/XQTS.supported.xml")|g' \
 | MapiClient -lx -sxml \
 | grep -v '^<.*>$' \
 | sed -e 's|&lt;|<|g' -e 's|&gt;|>|g' \
 > "$XQTS_DST/.XQTS.sh" || exit 1

/bin/sh "$XQTS_DST/.XQTS.sh" "$TSTSRCBASE" "$XQTS_DIR" "$XQTS_SRC" || exit 1

rm -rf "$XQTS_SRC"

set +x

echo -e ""
echo -e "Now you can run Mtest"
echo -e "via"
echo -e "\tMtest_pathfinder -r --TSTSRCBASE=\"$TSTSRCBASE\" \"$XQTS_DIR\""
echo -e "or"
echo -e "\tMtest.py -r --TSTSRCBASE=\"$TSTSRCBASE\" \"$XQTS_DIR\""
echo -e ""

