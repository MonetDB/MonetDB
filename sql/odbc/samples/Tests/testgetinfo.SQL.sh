#!/bin/sh

ODBCINI=$PWD/odbc.ini
trap "rm $ODBCINI" 0 15
cat > $ODBCINI <<EOF
[MonetDB-Test]
Description	= Test MonetDB Data Source
Driver		= $LIBDIR/libMonetODBC.so
Setup		= $LIBDIR/libMonetODBCs.so
Host		= localhost
Port		= $MAPIPORT
Database	= $TSTDB
User		= monetdb
Password	= monetdb
Debug		= 

EOF
export ODBCINI
eval testgetinfo MonetDB-Test
