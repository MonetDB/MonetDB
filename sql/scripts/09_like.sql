-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

create filter function "like"(val string, pat string, esc string) external name algebra."like";
create filter function "ilike"(val string, pat string, esc string) external name algebra."ilike";
create filter function "like"(val string, pat string) external name algebra."like";
create filter function "ilike"(val string, pat string) external name algebra."ilike";

grant execute on filter function "like" (string, string, string) to public;
grant execute on filter function "ilike" (string, string, string) to public;
grant execute on filter function "like" (string, string) to public;
grant execute on filter function "ilike" (string, string) to public;
