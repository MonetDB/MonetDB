#!/bin/sh

sed 's|<TSTSRCDIR>|'$TSTSRCDIR'|' $TSTSRCDIR/ID.1600704-b.xq > $TSTTRGDIR/ID.1600704-b.xquery
Mlog "$XQUERY_CLIENT -lxquery -fxml $TSTTRGDIR/ID.1600704-b.xquery"
      $XQUERY_CLIENT -lxquery -fxml $TSTTRGDIR/ID.1600704-b.xquery
