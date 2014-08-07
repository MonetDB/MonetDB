#!/bin/sh

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

# converts PostgreSQL specific SQL into SQL99 equivalent (if possible)

# Note: when bug 3520 has been implemented, remove the substitution rule:  -e 's/LN(/LOG(/ig' \

sed -r \
	-e 's/\bAS true/AS "true"/Ig' \
	-e 's/\bAS false/AS "false"/Ig' \
	-e 's/\bAS year/AS "year"/Ig' \
	-e 's/\bAS month/AS "month"/Ig' \
	-e 's/\bAS day/AS "day"/Ig' \
	-e 's/\bAS hour/AS "hour"/Ig' \
	-e 's/\bAS minute/AS "minute"/Ig' \
	-e 's/\bAS second/AS "second"/Ig' \
	-e 's/\bIS TRUE/= TRUE/Ig' \
	-e 's/\bIS FALSE/= FALSE/Ig' \
	-e 's/\bIS NOT TRUE/= NOT TRUE/Ig' \
	-e 's/\bIS NOT FALSE/= NOT FALSE/Ig' \
	-e 's/\bbool '*'\b/cast('\1' as boolean)/Ig' \
	-e 's/\bchar 'c'/cast('c' as char)/Ig' \
	-e 's/\bint2 '0'/cast('0' as smallint)/Ig' \
	-e 's/\bint2 '1'/cast('1' as smallint)/Ig' \
	-e 's/\bint2 '2'/cast('2' as smallint)/Ig' \
	-e 's/\bint2 '4'/cast('4' as smallint)/Ig' \
	-e 's/\bint2 '16'/cast('16' as smallint)/Ig' \
	-e 's/\bint4 '0'/cast('0' as integer)/Ig' \
	-e 's/\bint4 '1'/cast('1' as integer)/Ig' \
	-e 's/\bint4 '2'/cast('2' as integer)/Ig' \
	-e 's/\bint4 '4'/cast('4' as integer)/Ig' \
	-e 's/\bint4 '16'/cast('16' as integer)/Ig' \
	-e 's/\bint4 '999'/cast('999' as integer)/Ig' \
	-e 's/\bint4 '1000'/cast('1000' as integer)/Ig' \
	-e 's/\bint8 '0'/cast('0' as bigint)/Ig' \
	-e 's/\bint2\b/smallint/Ig' \
	-e 's/\bint4\b/integer/Ig' \
	-e 's/\bint8\b/bigint/Ig' \
	-e 's/\bfloat4\b/real/Ig' \
	-e 's/\bfloat8\b/double/Ig' \
	-e 's/\bnumeric(210,10)\b/numeric(18,10)/Ig' \
	-e 's/\bfloat8 (*)/cast(\1 as double)/Ig' \
	-e 's/\bbytea\b/blob/Ig' \
	-e 's/\bpath\b/string/Ig' \
	-e 's/\bpoint\b/string/Ig' \
	-e 's/\bbox\b/string/Ig' \
	-e 's/\bpolygon\b/string/Ig' \
	-e 's/\bcity_budget\b/decimal(7,2)/Ig' \
	-e 's/\bdate 'yesterday'/sql_sub(current_date, 24*60*60.0)/Ig' \
	-e 's/\bdate 'today'/current_date/Ig' \
	-e 's/\bdate 'tomorrow'/sql_add((current_date, 24*60*60.0)/Ig' \
	-e 's/\bname,/string,/Ig' \
	-e 's/\bname$/string/Ig' \
	-e 's/\btimestamp with time zone 'now'/cast(now as timestamptz)/Ig' \
	-e 's/\btimestamp with time zone 'yesterday'/cast(sql_sub(current_date, 24*60*60.0)as timestamptz)/Ig' \
	-e 's/\btimestamp with time zone 'today'/cast(current_date as timestamptz)/Ig' \
	-e 's/\btimestamp with time zone 'tomorrow'/cast(sql_add((current_date, 24*60*60.0)as timestamptz)/Ig' \
	-e 's/\btimestamp with time zone '*'/cast('\1' as timestamptz)/Ig' \
	-e 's/\btimestamp without time zone 'now'/cast(now as timestamp)/Ig' \
	-e 's/\btimestamp without time zone 'yesterday'/cast(sql_sub(current_date, 24*60*60.0)as timestamp)/Ig' \
	-e 's/\btimestamp without time zone 'today'/cast(current_date as timestamp)/Ig' \
	-e 's/\btimestamp without time zone 'tomorrow'/cast(sql_add((current_date, 24*60*60.0)as timestamp)/Ig' \
	-e 's/\btimestamp without time zone '*'/cast('\1' as timestamp)/Ig' \
	-e 's/\btimestamp(2) without time zone/timestamp(2)/Ig' \
	-e 's/LOG(numeric '10',/LOG10(/Ig' \
	-e 's/LOG(/LOG10(/Ig' \
	-e 's/LN(/LOG(/Ig' \
	-e 's/substr(/substring(/Ig' \
	-e 's/strpos(*,*)/locate(\2,\1)/Ig' \
	-e 's/TRIM(BOTH FROM *)/TRIM(\1)/Ig' \
	-e 's/TRIM(LEADING FROM *)/LTRIM(\1)/Ig' \
	-e 's/TRIM(TRAILING FROM *)/RTRIM(\1)/Ig' \
	-e 's/TRIM(BOTH * FROM *)/TRIM(replace(\2,\1,' '))/Ig' \
	-e 's/\bnumeric '10'/cast('10.0' as numeric(2,0))/Ig' \
	-e 's/\btext 'text'/cast('text' as text)/Ig' \
	-e 's/\bchar(20) 'characters'/cast('characters' as char(20))/Ig' \
	-e 's/\b!= /<> /Ig' \
	-e 's/(.*)\bFROM ONLY (.*)/\1 FROM \2/Ig' \
	-e 's/BEGIN TRANSACTION;/START TRANSACTION;/Ig' \
	-e 's/BEGIN;/START TRANSACTION;/Ig' \
	-e 's/END;/COMMIT;/Ig' \
	-e 's/COMMIT TRANSACTION;/COMMIT;/Ig' \
	-e 's/^COMMENT.*;$//Ig' \
	-e 's/\) (INHERITS.*);/\); -- \1/Ig' \
	-e 's/VACUUM ANALYZE *;/\/* VACUUM ANALYZE \1; *\//Ig' \
	-e 's/alter table * alter column * set storage external;/\/* alter table \1 alter column \2 set storage external; *\//Ig' \
	-e 's/SET datestyle TO *;/\/* SET datestyle TO \1; *\//Ig' \
	-e 's/SET geqo TO *;/\/* SET geqo TO \1; *\//Ig' \
	-e 's/RESET datestyle;/\/* RESET datestyle; *\//Ig' \
	-e 's/RESET geqo;/\/* RESET geqo; *\//Ig' \
	-e 's/\s+([^\s]+)::float[248]\b/ cast(\1 as double)/Ig' \
	-e 's/\s+([^\s]+)::int2\b/ cast(\1 as smallint)/Ig' \
	-e 's/\s+([^\s]+)::int4\b/ cast(\1 as integer)/Ig' \
	-e 's/\s+([^\s]+)::int8\b/ cast(\1 as bigint)/Ig' \
	-e 's/\s+([^\s]+)::float4\b/ cast(\1 as real)/Ig' \
	-e 's/\s+([^\s]+)::float8\b/ cast(\1 as double)/Ig' \
	-e 's/\s+([^\s]+)::text\b/ cast(\1 as string)/Ig' \
	-e 's/\s+([^\s]+)::(\w+(\([0-9]+(,[0-9]+))\)?)\b/ cast(\1 as \2)/Ig'
