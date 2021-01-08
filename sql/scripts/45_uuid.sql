-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

-- (co) Martin Kersten
-- The UUID type comes with a few operators.

create type uuid external name uuid;

-- generate a new uuid
create function sys.uuid()
returns uuid external name uuid."new";
GRANT EXECUTE ON FUNCTION sys.uuid() TO PUBLIC;

-- generate a new uuid with a dummy parameter, so it can be called for a column
create function sys.uuid(d int)
returns uuid external name uuid."new";
GRANT EXECUTE ON FUNCTION sys.uuid(int) TO PUBLIC;

create function sys.isaUUID(s string)
returns boolean external name uuid."isaUUID";
GRANT EXECUTE ON FUNCTION sys.isaUUID(string) TO PUBLIC;
