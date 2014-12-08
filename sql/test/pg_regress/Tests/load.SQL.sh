#!/usr/bin/env bash

Mlog -x "$SQL_CLIENT < $TSTSRCDIR/create_table.sql"

Mlog "loading data with sed-foo"

# don't load stud_emp, as it's the only data file with \N entries
# (PostgreSQL NULL notation)
sed \
	-e "s+@abs_srcdir@+$TSTSRCBASE/$TSTDIR+g" \
	-e '/@abs_builddir@/d' \
	-e '/DELETE FROM/d' \
	-e "s/[Cc][Oo][Pp][Yy] \\(.*\\);/COPY INTO \\1 USING DELIMITERS '\\\\t', '\\\\n';/" \
	-e '/stud_emp/d' \
	$TSTSRCDIR/../input/copy.source \
	| $SQL_CLIENT

Mlog -x "$SQL_CLIENT -s 'select count(*) from aggtest;'"
