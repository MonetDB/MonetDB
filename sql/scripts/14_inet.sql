-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

CREATE TYPE inet EXTERNAL NAME inet;

CREATE FUNCTION "broadcast" (p inet) RETURNS inet
	EXTERNAL NAME inet."broadcast";
GRANT EXECUTE ON FUNCTION "broadcast"(inet) TO PUBLIC;
CREATE FUNCTION "host" (p inet) RETURNS clob
	EXTERNAL NAME inet."host";
GRANT EXECUTE ON FUNCTION "host"(inet) TO PUBLIC;
CREATE FUNCTION "masklen" (p inet) RETURNS int
	EXTERNAL NAME inet."masklen";
GRANT EXECUTE ON FUNCTION "masklen"(inet) TO PUBLIC;
CREATE FUNCTION "setmasklen" (p inet, mask int) RETURNS inet
	EXTERNAL NAME inet."setmasklen";
GRANT EXECUTE ON FUNCTION "setmasklen"(inet, int) TO PUBLIC;
CREATE FUNCTION "netmask" (p inet) RETURNS inet
	EXTERNAL NAME inet."netmask";
GRANT EXECUTE ON FUNCTION "netmask"(inet) TO PUBLIC;
CREATE FUNCTION "hostmask" (p inet) RETURNS inet
	EXTERNAL NAME inet."hostmask";
GRANT EXECUTE ON FUNCTION "hostmask"(inet) TO PUBLIC;
CREATE FUNCTION "network" (p inet) RETURNS inet
	EXTERNAL NAME inet."network";
GRANT EXECUTE ON FUNCTION "network"(inet) TO PUBLIC;
CREATE FUNCTION "text" (p inet) RETURNS clob
	EXTERNAL NAME inet."text";
GRANT EXECUTE ON FUNCTION "text"(inet) TO PUBLIC;
CREATE FUNCTION "abbrev" (p inet) RETURNS clob
	EXTERNAL NAME inet."abbrev";
GRANT EXECUTE ON FUNCTION "abbrev"(inet) TO PUBLIC;

CREATE FUNCTION "left_shift"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet."<<";
GRANT EXECUTE ON FUNCTION "left_shift"(inet, inet) TO PUBLIC;
CREATE FUNCTION "right_shift"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet.">>";
GRANT EXECUTE ON FUNCTION "right_shift"(inet, inet) TO PUBLIC;

CREATE FUNCTION "left_shift_assign"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet."<<=";
GRANT EXECUTE ON FUNCTION "left_shift_assign"(inet, inet) TO PUBLIC;
CREATE FUNCTION "right_shift_assign"(i1 inet, i2 inet) RETURNS boolean
	EXTERNAL NAME inet.">>=";
GRANT EXECUTE ON FUNCTION "right_shift_assign"(inet, inet) TO PUBLIC;
