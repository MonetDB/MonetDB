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

create procedure stopmaster()
external name wlcr.stopmaster;

create procedure masterdrift( duration int)
external name wlcr."setmasterdrift";

create function masterClock() returns string
external name wlcr."getmasterclock";

create function masterTick() returns integer
external name wlcr."getmastertick";

-- Replica commands
create procedure replicate()
external name wlr.replicate;

create procedure replicate(dbname string)
external name wlr.replicate;

create procedure replicate(dbname string, pit timestamp)
external name wlr.replicate;

create procedure replicate(dbname string, id tinyint)
external name wlr.replicate;

create procedure replicate(dbname string, id smallint)
external name wlr.replicate;

create procedure replicate(dbname string, id integer)
external name wlr.replicate;

create procedure replicate(dbname string, id bigint)
external name wlr.replicate;

create procedure replicadrift(duration integer)
external name wlr."setreplicadrift";

create function replicaClock() returns string
external name wlr."getreplicaclock";

create function replicaTick() returns integer
external name wlr."getreplicatick";

