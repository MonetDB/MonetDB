#!/bin/sh

# check1
Mlog	"$SQL_CLIENT < $TSTSRCDIR/check0.sql"
	 $SQL_CLIENT < $TSTSRCDIR/check0.sql

# # check1a
# Mlog	"$MIL_CLIENT -prelude $TSTSRCDIR/check0a.prelude < $TSTSRCDIR/check0a.milM"
# 	 $MIL_CLIENT -prelude $TSTSRCDIR/check0a.prelude < $TSTSRCDIR/check0a.milM

# queries
Mlog	"$SQL_CLIENT < $TSTSRCBASE/$TSTDIR/queries.sql"
	 $SQL_CLIENT < $TSTSRCBASE/$TSTDIR/queries.sql
