-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

create filter function "like"(val string, pat string, esc string) external name algebra."like";
create filter function "ilike"(val string, pat string, esc string) external name algebra."ilike";
create filter function "like"(val string, pat string) external name algebra."like";
create filter function "ilike"(val string, pat string) external name algebra."ilike";
