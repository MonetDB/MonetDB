-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

-- assume milliseconds when converted to TIMESTAMP
create function sys.epoch(sec BIGINT) returns TIMESTAMP WITH TIME ZONE
	external name mtime.epoch;

create function sys.epoch(sec INT) returns TIMESTAMP WITH TIME ZONE
	external name mtime.epoch;

create function sys.epoch(ts TIMESTAMP WITH TIME ZONE) returns INT
	external name mtime.epoch;

grant execute on function sys.epoch (BIGINT) to public;
grant execute on function sys.epoch (INT) to public;
grant execute on function sys.epoch (TIMESTAMP WITH TIME ZONE) to public;

create function sys.date_trunc(txt string, t timestamp)
returns timestamp
external name sql.date_trunc;
grant execute on function sys.date_trunc(string, timestamp) to public;

create function sys.date_trunc(txt string, t timestamp with time zone)
returns timestamp with time zone
external name sql.date_trunc;
grant execute on function sys.date_trunc(string, timestamp with time zone) to public;
