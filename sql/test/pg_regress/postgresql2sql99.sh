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

sed -r \
	-e 's/\bAS true/AS "true"/ig' \
	-e 's/\bAS false/AS "false"/ig' \
	-e 's/\bIS TRUE/= TRUE/ig' \
	-e 's/\bIS FALSE/= FALSE/ig' \
	-e 's/\bIS NOT TRUE/= NOT TRUE/ig' \
	-e 's/\bIS NOT FALSE/= NOT FALSE/ig' \
	-e 's/\bbool '*'\b/cast('\1' as boolean)/ig' \
	-e 's/\bint2 '0'/cast('0' as smallint)/ig' \
	-e 's/\bint2 '1'/cast('1' as smallint)/ig' \
	-e 's/\bint2 '2'/cast('2' as smallint)/ig' \
	-e 's/\bint2 '4'/cast('4' as smallint)/ig' \
	-e 's/\bint2 '16'/cast('16' as smallint)/ig' \
	-e 's/\bint4 '0'/cast('0' as integer)/ig' \
	-e 's/\bint4 '1'/cast('1' as integer)/ig' \
	-e 's/\bint4 '2'/cast('2' as integer)/ig' \
	-e 's/\bint4 '4'/cast('4' as integer)/ig' \
	-e 's/\bint4 '16'/cast('16' as integer)/ig' \
	-e 's/\bint4 '999'/cast('999' as integer)/ig' \
	-e 's/\bint4 '1000'/cast('1000' as integer)/ig' \
	-e 's/\bint8 '0'/cast('0' as bigint)/ig' \
	-e 's/\bint2\b/smallint/ig' \
	-e 's/\bint4\b/integer/ig' \
	-e 's/\bint8\b/bigint/ig' \
	-e 's/\bfloat4\b/real/ig' \
	-e 's/\bfloat8\b/double/ig' \
	-e 's/\bfloat8 (*)/cast(\1 as double)/ig' \
	-e 's/\bpath\b/string/ig' \
	-e 's/\bpoint\b/string/ig' \
	-e 's/\bbox\b/string/ig' \
	-e 's/\bpolygon\b/string/ig' \
	-e 's/\bcity_budget\b/decimal(7,2)/ig' \
	-e 's/\bname,/string,/ig' \
	-e 's/\bname$/string/ig' \
	-e 's/(.*)\bFROM ONLY (.*)/\1 FROM \2/ig' \
	-e 's/BEGIN;/START TRANSACTION;/ig' \
	-e 's/END;/COMMIT;/ig' \
	-e 's/^COMMENT.*;$//ig' \
	-e 's/\) (INHERITS.*);/\); -- \1/ig' \
	-e 's/\s+([^\s]+)::float[248]\b/ cast(\1 as double)/ig' \
	-e 's/\s+([^\s]+)::int2\b/ cast(\1 as smallint)/ig' \
	-e 's/\s+([^\s]+)::int4\b/ cast(\1 as integer)/ig' \
	-e 's/\s+([^\s]+)::int8\b/ cast(\1 as bigint)/ig' \
	-e 's/\s+([^\s]+)::(\w+(\([0-9]+(,[0-9]+))\)?)\b/ cast(\1 as \2)/ig'
