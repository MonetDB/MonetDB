#!/bin/sh

for i in $CLIENTS_PREFIX/lib/MonetDB/Tests $CLIENTS_PREFIX/lib64/MonetDB/Tests $CLIENTS_BUILD/src/examples/C ; do
	if [ -d "$i" ] ; then
		PATH="$i:$PATH"
	fi
done
export PATH

Mlog -x sample2 $HOST $MAPIPORT sql
