-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

create function str_to_date(s string, format string) returns date
	external name mtime."str_to_date";

create function date_to_str(d date, format string) returns string
	external name mtime."date_to_str";
