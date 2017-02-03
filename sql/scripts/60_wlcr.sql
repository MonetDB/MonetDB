-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- Workload Capture and Replay

-- Master commands
create procedure logthreshold(duration int)
external name wlcr.logthreshold;

create procedure logrollback(flag int)
external name wlcr.logrollback;

create procedure drift(duration int)
external name wlcr.drift;

create procedure master()
external name wlcr.master;

create procedure stopmaster()
external name wlcr.stopmaster;

-- Replica commands
create procedure replicate(dbname string)
external name wlcr.replicate;

create procedure replaythreshold(duration int)
external name wlr.replaythreshold;

