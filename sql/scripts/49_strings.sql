-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024, 2025 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

create function asciify(x string)
returns string external name str.asciify;
grant execute on function asciify(string) to public;

create function sys.startswith(x string, y string)
returns boolean external name str.startswith;
grant execute on function startswith(string, string) to public;

create function sys.startswith(x string, y string, icase boolean)
returns boolean external name str.startswith;
grant execute on function startswith(string, string, boolean) to public;

create filter function sys.startswith(x string, y string)
external name str.startswith;
grant execute on filter function startswith(string, string) to public;

create filter function sys.startswith(x string, y string, icase boolean)
external name str.startswith;
grant execute on filter function startswith(string, string, boolean) to public;

create function sys.endswith(x string, y string)
returns boolean external name str.endswith;
grant execute on function endswith(string, string) to public;

create function sys.endswith(x string, y string, icase boolean)
returns boolean external name str.endswith;
grant execute on function endswith(string, string, boolean) to public;

create filter function sys.endswith(x string, y string)
external name str.endswith;
grant execute on filter function endswith(string, string) to public;

create filter function sys.endswith(x string, y string, icase boolean)
external name str.endswith;
grant execute on filter function endswith(string, string, boolean) to public;

create function sys.contains(x string, y string)
returns boolean external name str.contains;
grant execute on function contains(string, string) to public;

create function sys.contains(x string, y string, icase boolean)
returns boolean external name str.contains;
grant execute on function contains(string, string, boolean) to public;

create filter function sys.contains(x string, y string)
external name str.contains;
grant execute on filter function contains(string, string) to public;

create filter function sys.contains(x string, y string, icase boolean)
external name str.contains;
grant execute on filter function contains(string, string, boolean) to public;

create aggregate sys.group_concat(str string) returns string with order
	external name "aggr"."str_group_concat";
GRANT EXECUTE ON AGGREGATE sys.group_concat(string) TO PUBLIC;
create aggregate sys.group_concat(str string, sep string) returns string with order
	external name "aggr"."str_group_concat";
GRANT EXECUTE ON AGGREGATE sys.group_concat(string, string) TO PUBLIC;

create function sys.normalize_monetdb_url(u string)
returns string external name sql.normalize_monetdb_url;
grant execute on function sys.normalize_monetdb_url(string) to public;

-- NGRAMS
create filter function c(h varchar, n varchar) external name ngrams.contains;
create filter function c(h varchar, n varchar, ng tinyint) external name ngrams.contains;
create filter function s(h varchar, n varchar) external name ngrams.startswith;
create filter function s(h varchar, n varchar, ng tinyint) external name ngrams.startswith;
create filter function e(h varchar, n varchar) external name ngrams.endswith;
create filter function e(h varchar, n varchar, ng tinyint) external name ngrams.endswith;
