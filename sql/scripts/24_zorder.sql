-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

create function zorder_encode(x integer, y integer) returns oid
    external name zorder.encode;

create function zorder_decode_x(z oid) returns integer
    external name zorder.decode_x;

create function zorder_decode_y(z oid) returns integer
    external name zorder.decode_y;

grant execute on function zorder_encode to public;
grant execute on function zorder_decode_x to public;
grant execute on function zorder_decode_y to public;

