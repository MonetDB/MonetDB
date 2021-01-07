-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

-- make the offline tracing table available for inspection
create function sys.tracelog()
	returns table (
		ticks bigint,		-- time in microseconds
		stmt string		-- actual statement executed
	)
	external name sql.dump_trace;

create view sys.tracelog as select * from sys.tracelog();
