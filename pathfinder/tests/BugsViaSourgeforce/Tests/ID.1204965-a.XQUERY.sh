#!/bin/sh

sed 's|<TSTSRCDIR>|'$TSTSRCDIR'|' $TSTSRCDIR/ID.1204965-a.xq > $TSTTRGDIR/ID.1204965-a.xquery
Mlog "$XQUERY_CLIENT -lxquery -fxml $TSTTRGDIR/ID.1204965-a.xquery"
      $XQUERY_CLIENT -lxquery -fxml $TSTTRGDIR/ID.1204965-a.xquery
