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
	-e 's/\bint8\b/bigint/Ig' \
	-e 's/\bint4\b/integer/Ig' \
	-e 's/\bint2\b/smallint/Ig' \
	-e 's/\bfloat4\b/float/Ig' \
	-e 's/\bfloat8\b/double/Ig' \
	-e 's/\bpath\b/string/Ig' \
	-e 's/\bpoint\b/string/Ig' \
	-e 's/\bbox\b/string/Ig' \
	-e 's/\bpolygon\b/string/Ig' \
	-e 's/\bcity_budget\b/decimal(7,2)/Ig' \
	-e 's/\bname,/string,/Ig' \
	-e 's/\bname$/string/Ig' \
	-e 's/(.*)\bFROM ONLY (.*)/\1 FROM \2/Ig' \
	-e 's/BEGIN;/START TRANSACTION;/Ig' \
	-e 's/END;/COMMIT;/Ig' \
	-e 's/COMMENT.*;//Ig' \
	-e '/CREATE TABLE.*INHERITS.*;/s/INHERITS (.*)//' \
	-e 's/\) INHERITS.*;/\);/Ig' \
	-e 's/\s+([^\s]+)::float[248]\b/ cast(\1 as double)/Ig' \
	-e 's/\s+([^\s]+)::int2\b/ cast(\1 as smallint)/Ig' \
	-e 's/\s+([^\s]+)::int4\b/ cast(\1 as integer)/Ig' \
	-e 's/\s+([^\s]+)::int8\b/ cast(\1 as bigint)/Ig' \
	-e 's/\s+([^\s]+)::(\w+(\([0-9]+(,[0-9]+))\)?)\b/ cast(\1 as \2)/Ig'
