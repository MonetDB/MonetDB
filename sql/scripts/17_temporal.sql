-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- assume milliseconds when converted to TIMESTAMP
create function sys."epoch"(sec BIGINT) returns TIMESTAMP
	external name timestamp."epoch";

create function sys."epoch"(sec INT) returns TIMESTAMP
	external name timestamp."epoch";

create function sys."epoch"(ts TIMESTAMP) returns INT
	external name timestamp."epoch";

create function sys."epoch"(ts TIMESTAMP WITH TIME ZONE) returns INT
	external name timestamp."epoch";

grant execute on function sys."epoch" (BIGINT) to public;
grant execute on function sys."epoch" (INT) to public;
grant execute on function sys."epoch" (TIMESTAMP) to public;
grant execute on function sys."epoch" (TIMESTAMP WITH TIME ZONE) to public;
