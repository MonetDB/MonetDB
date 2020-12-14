#!/bin/sh

if [ ! "$1" ] ; then
	echo "Usage: $0 <#tuples>" 2>&1
	exit 1
fi

perl -i -p -e 's|^tuples:=[0-9]+;$|tuples:='"$1"';|' fastcrack_*.mal
grep '^tuples:=' fastcrack_*.mal
