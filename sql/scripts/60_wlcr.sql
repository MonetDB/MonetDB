-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.

-- Workload Capture and Replay

-- Master commands
create procedure master()
external name wlc.master;

create procedure master(path string)
external name wlc.master;

create procedure stopmaster()
external name wlc.stopmaster;

create procedure flush()
external name wlc.flush;

create procedure masterbeat( duration int)
external name wlc."setmasterbeat";

create function masterClock() returns string
external name wlc."getmasterclock";

create function masterTick() returns bigint
external name wlc."getmastertick";

-- Replica commands

create procedure replicate(dbname string)
external name wlr.replicate;

create procedure stopreplicate()
external name wlr.stopreplicate;

-- run it forever
create procedure replicate()
external name wlr.replicate;

-- run replicator until condition is met
create procedure replicate(pointintime timestamp)
external name wlr.replicate;

create procedure replicate(id tinyint)
external name wlr.replicate;

create procedure replicate(id smallint)
external name wlr.replicate;

create procedure replicate(id integer)
external name wlr.replicate;

create procedure replicate(id bigint)
external name wlr.replicate;

-- control the interval for replication 
create procedure replicabeat(duration integer)
external name wlr."setreplicabeat";

create function replicaClock() returns string
external name wlr."getreplicaclock";

create function replicaTick() returns bigint
external name wlr."getreplicatick";

