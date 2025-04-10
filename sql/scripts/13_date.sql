-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024, 2025 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

create function str_to_date(s string, format string) returns date
	external name mtime."str_to_date";

create function date_to_str(d date, format string) returns string
	external name mtime."date_to_str";

create function str_to_time(s string, format string) returns time with time zone
	external name mtime."str_to_time";

create function time_to_str(d time, format string) returns string
	external name mtime."time_to_str";

create function time_to_str(d time with time zone, format string) returns string
	external name mtime."timetz_to_str";

create function str_to_timestamp(s string, format string) returns timestamp with time zone
	external name mtime."str_to_timestamp";

create function timestamp_to_str(d timestamp, format string) returns string
	external name mtime."timestamp_to_str";

create function timestamp_to_str(d timestamp with time zone, format string) returns string
	external name mtime."timestamptz_to_str";

create function dayname(d date) returns varchar(10) return date_to_str(d, '%A');
create function monthname(d date) returns varchar(10) return date_to_str(d, '%B');

grant execute on function str_to_date to public;
grant execute on function date_to_str to public;
grant execute on function str_to_time to public;
grant execute on function time_to_str(time, string) to public;
grant execute on function time_to_str(time with time zone, string) to public;
grant execute on function str_to_timestamp to public;
grant execute on function timestamp_to_str(timestamp, string) to public;
grant execute on function timestamp_to_str(timestamp with time zone, string) to public;
grant execute on function dayname(date) to public;
grant execute on function monthname(date) to public;
