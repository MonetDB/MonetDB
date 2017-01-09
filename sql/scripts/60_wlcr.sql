-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- Workload Capture and Replay

create schema wlcr;

create procedure wlcr.master()
external name wlcr.master;

create procedure wlcr.master(threshold integer)
external name wlcr.master;

create procedure wlcr.replay()
external name wlcr.replay;

create procedure wlcr.replay(threshold int)
external name wlcr.replay;

create procedure wlcr.replay(dbname string)
external name wlcr.replay;

create procedure wlcr.replay(dbname string, threshold int)
external name wlcr.replay;

create procedure wlcr.clone(dbname string)
external name wlcr.clone;

