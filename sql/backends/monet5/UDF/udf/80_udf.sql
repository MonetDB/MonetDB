-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- add function signatures to SQL catalog


-- Reverse a string
create function reverse(src string)
returns string external name udf.reverse;


-- fuse two (1-byte) tinyint values into one (2-byte) smallint value
create function fuse(one tinyint, two tinyint)
returns smallint external name udf.fuse;

-- fuse two (2-byte) smallint values into one (4-byte) integer value
create function fuse(one smallint, two smallint)
returns integer external name udf.fuse;

-- fuse two (4-byte) integer values into one (8-byte) bigint value
create function fuse(one integer, two integer)
returns bigint external name udf.fuse;
