-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

CREATE TYPE inet EXTERNAL NAME inet;

CREATE FUNCTION "broadcast" (p inet) RETURNS inet
	EXTERNAL NAME inet."broadcast";
CREATE FUNCTION "host" (p inet) RETURNS clob
	EXTERNAL NAME inet."host";
CREATE FUNCTION "masklen" (p inet) RETURNS int
	EXTERNAL NAME inet."masklen";
CREATE FUNCTION "setmasklen" (p inet, mask int) RETURNS inet
	EXTERNAL NAME inet."setmasklen";
CREATE FUNCTION "netmask" (p inet) RETURNS inet
	EXTERNAL NAME inet."netmask";
CREATE FUNCTION "hostmask" (p inet) RETURNS inet
	EXTERNAL NAME inet."hostmask";
CREATE FUNCTION "network" (p inet) RETURNS inet
	EXTERNAL NAME inet."network";
CREATE FUNCTION "text" (p inet) RETURNS clob
	EXTERNAL NAME inet."text";
CREATE FUNCTION "abbrev" (p inet) RETURNS clob
	EXTERNAL NAME inet."abbrev";

CREATE FUNCTION "left_shift"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet."<<";
CREATE FUNCTION "right_shift"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet.">>";

CREATE FUNCTION "left_shift_assign"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet."<<=";
CREATE FUNCTION "right_shift_assign"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet.">>=";
