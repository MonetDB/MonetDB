-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- Workload Capture and Replay

declare replaythreshold integer;
set replaythreshold = -1; -- don't replay

create procedure drift(duration int)
external name wlcr.drift;

create procedure master()
external name wlcr.master;

create procedure stopmaster()
external name wlcr.stopmaster;

create procedure replicate(dbname string)
external name wlcr.replicate;

create procedure replicate(dbname string, waitforsync bool)
external name wlcr.replicate;

