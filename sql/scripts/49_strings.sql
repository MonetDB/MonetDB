-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

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
