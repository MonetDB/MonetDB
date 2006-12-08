#!/bin/sh

sed 's|<TSTSRCDIR>|'$TSTSRCDIR'|' $TSTSRCDIR/ID.1600704-b.xq > $TSTTRGDIR/ID.1600704-b.xquery
Mlog -x $XQUERY_CLIENT -lxquery -oxml $TSTTRGDIR/ID.1600704-b.xquery
