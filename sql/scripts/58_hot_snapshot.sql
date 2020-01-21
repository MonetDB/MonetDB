-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

-- Hot snapshot

-- Main command to create a hot snapshot
create procedure hot_snapshot(tarfile string)
	external name sql.hot_snapshot;

-- We intentionally don't GRANT EXECUTE ON sys.hot_snapshot TO PUBLIC!
