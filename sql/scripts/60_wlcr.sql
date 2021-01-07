-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

-- Workload Capture and Replay

-- Master commands
create schema wlc;

create procedure wlc.master()
external name wlc.master;

create procedure wlc.master(path string)
external name wlc.master;

create procedure wlc.stop()
external name wlc.stop;

create procedure wlc.flush()
external name wlc.flush;

create procedure wlc.beat( duration int)
external name wlc."setbeat";

create function wlc.clock() returns string
external name wlc."getclock";

create function wlc.tick() returns bigint
external name wlc."gettick";

