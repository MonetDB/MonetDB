#!/bin/sh

# test the URI parsing capabilities of the MAPI library

Mlog "mclient -d mapi:monetdb://$HOST:$MAPIPORT/$TSTDB?language=sql&user=monetdb -f test -E utf-8 -s select 1"
mclient -d "mapi:monetdb://$HOST:$MAPIPORT/$TSTDB?language=sql&user=monetdb" -f test -E utf-8 -s 'select 1'

Mlog "mclient -d mapi:monetdb://$MAPIHOST/.s.monetdb.$MAPIPORT?database=$TSTDB&language=sql&user=monetdb -f test -E utf-8 -s select 1"
mclient -d "mapi:monetdb://$MAPIHOST/.s.monetdb.$MAPIPORT?database=$TSTDB&language=sql&user=monetdb" -f test -E utf-8 -s 'select 1'
