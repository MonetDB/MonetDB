#!/bin/sh

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

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
	-e 's/\b'f'::bool\b/cast('false' as boolean)/Ig' \
	-e 's/\b't'::bool\b/cast('true' as boolean)/Ig' \
	-e 's/\bbool 'f'\b/cast('false' as boolean)/Ig' \
	-e 's/\bbool 't'\b/cast('true' as boolean)/Ig' \
	-e 's/\bbool ''*''\b/cast('\1' as boolean)/Ig' \
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
	-e 's/\bserial8\b/bigint GENERATED ALWAYS AS IDENTITY/Ig' \
	-e 's/\bnumeric(210,10)\b/numeric(18,10)/Ig' \
	-e 's/\bfloat8 (*)/cast(\1 as double)/Ig' \
	-e 's/\bbytea\b/blob/Ig' \
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
	-e 's/\btimestamp with time zone ''*''/cast('\1' as timestamptz)/Ig' \
	-e 's/\btimestamp without time zone 'now'/cast(now as timestamp)/Ig' \
	-e 's/\btimestamp without time zone 'yesterday'/cast(sql_sub(current_date, 24*60*60.0)as timestamp)/Ig' \
	-e 's/\btimestamp without time zone 'today'/cast(current_date as timestamp)/Ig' \
	-e 's/\btimestamp without time zone 'tomorrow'/cast(sql_add((current_date, 24*60*60.0)as timestamp)/Ig' \
	-e 's/\btimestamp without time zone ''*''/cast('\1' as timestamp)/Ig' \
	-e 's/\btimestamp(2) without time zone/timestamp(2)/Ig' \
	-e 's/\btimeofday()::timestamp/current_timestamp/Ig' \
	-e 's/\b(f1 interval)/(f1 interval second)/Ig' \
	-e 's/\binterval ''*''/cast('\1' as interval second)/Ig' \
	-e 's/\b(f1 reltime)/(f1 interval second)/Ig' \
	-e 's/\breltime ''*''/cast('\1' as interval second)/Ig' \
	-e 's/\b(f1 box)/(f1 mbr)/Ig' \
	-e 's/\bbox ''*,*,*,*''/mbr(''linestring('\1' '\2', '\3' '\4')'')/Ig' \
	-e 's/\b''(2.0,0.0),(2.0,4.0),(0.0,0.0)''/''polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))''/Ig' \
	-e 's/\b''(3.0,1.0),(3.0,3.0),(1.0,0.0)''/''polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))''/Ig' \
	-e 's/\bpoint ''(*,*)''/point('\1', '\2'))/Ig' \
	-e 's/LOG(numeric '10',/LOG10(/Ig' \
	-e 's/LOG(/LOG10(/Ig' \
	-e 's/LN(/LOG(/Ig' \
	-e 's/substr(/substring(/Ig' \
	-e 's/strpos(*,*)/locate(\2,\1)/Ig' \
	-e 's/TRIM(BOTH FROM *)/TRIM(\1)/Ig' \
	-e 's/TRIM(LEADING FROM *)/LTRIM(\1)/Ig' \
	-e 's/TRIM(TRAILING FROM *)/RTRIM(\1)/Ig' \
	-e 's/TRIM(BOTH * FROM *)/TRIM(replace(\2,\1,' '))/Ig' \
	-e 's/\bcidr,/inet,/Ig' \
	-e 's/\bcidr(*)/cast(\1 as inet)/Ig' \
	-e 's/\bdouble '64'/cast('64' as double)/Ig' \
	-e 's/\bdouble '144'/cast('144' as double)/Ig' \
	-e 's/\bdouble '0.5'/cast('0.5' as double)/Ig' \
	-e 's/\bdouble '27'/cast('27' as double)/Ig' \
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
	-e 's/SELECT * INTO TABLE * FROM *;/\CREATE TABLE \2 AS SELECT \1 FROM \3 WITH DATA;/Ig' \
	-e 's/VACUUM ANALYZE *;/call vacuum('sys', '\1'); call analyze('sys', '\1');/Ig' \
	-e 's/VACUUM FULL *;/call vacuum('sys', '\1');/Ig' \
	-e 's/VACUUM *;/call vacuum('sys', '\1');/Ig' \
	-e 's/alter table only\b/alter table \/* only *\/ /Ig' \
	-e 's/alter table * alter column * set storage external;/\/* alter table \1 alter column \2 set storage external; *\//Ig' \
	-e 's/SET datestyle TO *;/\/* SET datestyle TO \1; *\//Ig' \
	-e 's/SET geqo TO *;/\/* SET geqo TO \1; *\//Ig' \
	-e 's/RESET datestyle;/\/* RESET datestyle; *\//Ig' \
	-e 's/RESET geqo;/\/* RESET geqo; *\//Ig' \
	-e 's/ WITH OIDS;/\/* WITH OIDS; *\//Ig' \
	-e 's/ WITHOUT OIDS;/\/* WITHOUT OIDS; *\//Ig' \
	-e 's/\s+([^\s]+)::bool\b/ cast(\1 as boolean)/Ig' \
	-e 's/\s+([^\s]+)::cidr\b/ cast(\1 as inet)/Ig' \
	-e 's/\s+([^\s]+)::float[248]\b/ cast(\1 as double)/Ig' \
	-e 's/\s+([^\s]+)::int2\b/ cast(\1 as smallint)/Ig' \
	-e 's/\s+([^\s]+)::int4\b/ cast(\1 as integer)/Ig' \
	-e 's/\s+([^\s]+)::int8\b/ cast(\1 as bigint)/Ig' \
	-e 's/\s+([^\s]+)::float4\b/ cast(\1 as real)/Ig' \
	-e 's/\s+([^\s]+)::float8\b/ cast(\1 as double)/Ig' \
	-e 's/\s+([^\s]+)::name\b/ cast(\1 as string)/Ig' \
	-e 's/\s+([^\s]+)::string\b/ cast(\1 as string)/Ig' \
	-e 's/\s+([^\s]+)::text\b/ cast(\1 as text)/Ig' \
	-e 's/\s+([^\s]+)::(\w+(\([0-9]+(,[0-9]+))\)?)\b/ cast(\1 as \2)/Ig'
