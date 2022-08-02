#!/bin/sh

if [ ! "$1" ] ; then
	echo "Usage: $0 <n>" 2>&1
	exit 1
fi

perl -i -p -e 's|^n:=[0-9]+;$|n:='"$1"';|' fastcrack_*.mal
grep '^n:=' fastcrack_*.mal
