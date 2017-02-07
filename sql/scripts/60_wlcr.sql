-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- Workload Capture and Replay

-- Master commands
create procedure master()
external name wlcr.master;

create procedure master(path string)
external name wlcr.master;

create procedure pausemaster()
external name wlcr.pausemaster;

create procedure resumemaster()
external name wlcr.resumemaster;

create procedure stopmaster()
external name wlcr.stopmaster;

create procedure logthreshold(duration int)
external name wlcr.logthreshold;

create procedure logrollback(flag int)
external name wlcr.logrollback;

create procedure drift(duration int)
external name wlcr.drift;

create procedure master(role integer)
external name wlcr.master;

-- Replica commands
create procedure replicate(dbname string)
external name wlr.replicate;

create procedure pausereplicate()
external name wlr.pausereplicate;

create procedure resumereplicate()
external name wlr.resumereplicate;

create procedure waitformaster()
external name wlr.waitformaster;

create procedure replaythreshold(duration int)
external name wlr.replaythreshold;

