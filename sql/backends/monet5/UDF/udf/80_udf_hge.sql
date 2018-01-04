-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- add function signatures to SQL catalog


-- fuse two (8-byte) integer values into one (16-byte) bigint value
create function fuse(one bigint, two bigint)
returns hugeint external name udf.fuse;
