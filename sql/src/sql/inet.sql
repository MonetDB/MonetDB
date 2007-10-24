
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
