-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

-- assume milliseconds when converted to TIMESTAMP
create function "epoch"(sec BIGINT) returns TIMESTAMP
    external name timestamp."epoch";

create function "epoch"(sec INT) returns TIMESTAMP
	external name timestamp."epoch";

create function "epoch"(ts TIMESTAMP) returns INT
	external name timestamp."epoch";
