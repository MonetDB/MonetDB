#!/bin/sh

cat $TSTSRCDIR/ID.1204965-a.xq | sed 's|<TSTSRCDIR>|'"$TSTSRCDIR"'|' > $TSTTRGDIR/ID.1204965-a.xquery
$XQUERY_CLIENT -lxquery -sxml $TSTTRGDIR/ID.1204965-a.xquery
