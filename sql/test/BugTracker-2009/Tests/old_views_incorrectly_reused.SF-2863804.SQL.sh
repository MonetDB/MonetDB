#!/bin/sh

Mlog -x "$SQL_CLIENT $TSTTRGDIR/$1.def.sql"
Mlog -x "$SQL_CLIENT $TSTTRGDIR/$1.view1.sql"
Mlog -x "$SQL_CLIENT $TSTTRGDIR/$1.view2.sql"
