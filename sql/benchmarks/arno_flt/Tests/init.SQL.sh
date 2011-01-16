#!/bin/sh

# create
Mlog	"$SQL_CLIENT < $TSTSRCBASE/$TSTDIR/create_tables.flt.sql"
	 $SQL_CLIENT < $TSTSRCBASE/$TSTDIR/create_tables.flt.sql

# check0
Mlog	"$SQL_CLIENT < $TSTSRCDIR/check0.sql"
	 $SQL_CLIENT < $TSTSRCDIR/check0.sql

# # check0a
# Mlog	"$MIL_CLIENT -prelude $TSTSRCDIR/check0a.prelude < $TSTSRCDIR/check0a.milM"
# 	 $MIL_CLIENT -prelude $TSTSRCDIR/check0a.prelude < $TSTSRCDIR/check0a.milM

# insert_MODEL
Mlog	"$SQL_CLIENT < $TSTSRCBASE/$TSTDIR/insert_MODEL.flt.sql"
	 $SQL_CLIENT < $TSTSRCBASE/$TSTDIR/insert_MODEL.flt.sql

# insert_ATOM
Mlog	"$SQL_CLIENT < $TSTSRCBASE/$TSTDIR/insert_ATOM.flt.sql"
	 $SQL_CLIENT < $TSTSRCBASE/$TSTDIR/insert_ATOM.flt.sql

# insert_BOND
Mlog	"$SQL_CLIENT < $TSTSRCBASE/$TSTDIR/insert_BOND.flt.sql"
	 $SQL_CLIENT < $TSTSRCBASE/$TSTDIR/insert_BOND.flt.sql

# check1
Mlog	"$SQL_CLIENT < $TSTSRCDIR/check0.sql"
	 $SQL_CLIENT < $TSTSRCDIR/check0.sql

# # check1a
# Mlog	"$MIL_CLIENT -prelude $TSTSRCDIR/check0a.prelude < $TSTSRCDIR/check0a.milM"
# 	 $MIL_CLIENT -prelude $TSTSRCDIR/check0a.prelude < $TSTSRCDIR/check0a.milM

