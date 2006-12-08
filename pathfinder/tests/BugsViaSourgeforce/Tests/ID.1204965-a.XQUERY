#!/bin/sh

sed 's|<TSTSRCDIR>|'$TSTSRCDIR'|' $TSTSRCDIR/ID.1204965-a.xq > $TSTTRGDIR/ID.1204965-a.xquery
Mlog -x $XQUERY_CLIENT -lxquery -oxml $TSTTRGDIR/ID.1204965-a.xquery
