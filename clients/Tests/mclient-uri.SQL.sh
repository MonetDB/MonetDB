#!/bin/sh

# test the URI parsing capabilities of the MAPI library

mclient -d "mapi:monetdb://$HOST:$MAPIPORT/$TSTDB?language=sql&user=monetdb" -f csv -t none -E utf-8 -s 'select 1' | grep -v '^1$'
if [ $? -eq 2 ]; then
	return 2;
fi

mclient -d "mapi:monetdb://$MAPIHOST/.s.monetdb.$MAPIPORT?database=$TSTDB&language=sql&user=monetdb" -f csv -t none -E utf-8 -s 'select 1' | grep -v '^1$'
if [ $? -eq 2 ]; then
	return 2;
fi
