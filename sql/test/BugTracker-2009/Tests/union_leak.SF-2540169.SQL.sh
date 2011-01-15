#!/bin/sh

Mlog -x "$SQL_CLIENT $TSTTRGDIR/$1.1.sql"
Mlog -x "$MAL_CLIENT $TSTTRGDIR/$1.2.mal"
Mlog -x "$SQL_CLIENT $TSTTRGDIR/$1.3.sql"
Mlog -x "$MAL_CLIENT $TSTTRGDIR/$1.2.mal"
Mlog -x "$SQL_CLIENT $TSTTRGDIR/$1.3.sql"
Mlog -x "$MAL_CLIENT $TSTTRGDIR/$1.2.mal"
Mlog -x "$SQL_CLIENT $TSTTRGDIR/$1.4.sql"
